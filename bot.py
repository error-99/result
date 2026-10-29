#!/usr/bin/env python3
"""
Telegram control bot (aiogram 3) for the result service.

You do NOT run this file directly - main.py starts it in a background thread when BOT_TOKEN is set:
    python main.py

It talks to the running service through the `svc` module that main.py hands over, so it sees the
same live key pool, queues and job store (no HTTP round-trips, no second process).

Commands (all also reachable from the inline buttons under /menu):
    /start /menu      main menu
    /status           live status (keys, queues, jobs, threads)
    /health           active probe of result sites + every Gemini key
    /sites            probe result sites only
    /keys             list / test / reset / remove Gemini keys
    /addkey           guided form to add one or many Gemini keys (tested before saving)
    /testkey [key]    test a key without adding it
    /queue  /jobs     queue depths / recent jobs
    /digest           the hourly digest, on demand
    /cancel           leave a form
    /id               show your Telegram id (works for anyone - put it in ADMIN_IDS)
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

log = logging.getLogger("bot")

# Gemini keys: old "AIza..." and new "AQ.xxxx" (contains a dot!)
KEY_RE = re.compile(r"[A-Za-z0-9_\-.]{20,}")
MAX_KEYS_PER_MESSAGE = 10

STATE_ICON = {"valid": "✅", "rate_limited": "⚠️", "invalid": "❌", "model_unavailable": "❓",
              "server_error": "🛠", "network_error": "📡", "error": "❓"}
STATE_TEXT = {"valid": "valid", "rate_limited": "valid, but rate limited right now",
              "invalid": "rejected by Google", "model_unavailable": "model not available for this key",
              "server_error": "Google server error, try again", "network_error": "network error",
              "error": "unexpected response"}


class AddKey(StatesGroup):
    waiting = State()   # step 1: waiting for key(s)
    confirm = State()   # step 2: tested, waiting for the Add button


class TestKey(StatesGroup):
    waiting = State()


# --------------------------------------------------------------------------- #
# small UI helpers
# --------------------------------------------------------------------------- #
def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def kb(*rows: List[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])


BACK = [btn("🏠 Menu", "m:menu")]


async def safe_edit(msg: Message, text: str, markup: Optional[InlineKeyboardMarkup] = None) -> Message:
    try:
        await msg.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as e:
        if "not modified" not in str(e).lower():
            return await msg.answer(text, reply_markup=markup)
    return msg


async def show(ev: Any, text: str, markup: Optional[InlineKeyboardMarkup] = None) -> Message:
    """Edit the message a button was pressed on, or send a new message for a command."""
    if isinstance(ev, CallbackQuery):
        return await safe_edit(ev.message, text, markup)
    return await ev.answer(text, reply_markup=markup)


async def ack(ev: Any, text: str = "") -> None:
    if isinstance(ev, CallbackQuery):
        try:
            await ev.answer(text)
        except TelegramBadRequest:
            pass


def fmt_probe(res: Dict[str, Any]) -> str:
    st = res.get("state", "error")
    parts = [f"{STATE_ICON.get(st, '❓')} <code>{res['tag']}</code> — {STATE_TEXT.get(st, st)}"]
    meta = []
    if res.get("http"):
        meta.append(f"HTTP {res['http']}")
    if res.get("ms") is not None:
        meta.append(f"{res['ms']}ms")
    if meta:
        parts.append("   <i>" + " · ".join(meta) + "</i>")
    if res.get("detail") and st != "valid":
        parts.append(f"   <i>{_e(res['detail'])}</i>")
    return "\n".join(parts)


def _e(x: Any) -> str:
    return html.escape(str(x), quote=False)


def ago(ts: float) -> str:
    s = int(max(0, time.time() - ts))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    return f"{s // 3600}h"


def build(svc: Any) -> Tuple[Bot, Dispatcher]:
    bot = Bot(token=svc.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML,
                                                                link_preview_is_disabled=True))
    dp = Dispatcher(storage=MemoryStorage())

    pub = Router(name="public")
    adm = Router(name="admin")
    fb = Router(name="fallback")
    admin_ids = set(svc.ADMIN_IDS)
    adm.message.filter(F.from_user.id.in_(admin_ids))
    adm.callback_query.filter(F.from_user.id.in_(admin_ids))
    dp.include_routers(pub, adm, fb)

    async def tp(fn: Callable[..., Any], *a: Any) -> Any:
        return await asyncio.to_thread(fn, *a)

    # ------------------------------ public ---------------------------------- #
    @pub.message(Command("id"))
    async def cmd_id(m: Message) -> None:
        await m.answer(f"Your Telegram id: <code>{m.from_user.id}</code>\n"
                       f"Chat id: <code>{m.chat.id}</code>\n\nPut the id in <code>ADMIN_IDS</code> in .env.")

    # ------------------------------ screens --------------------------------- #
    def menu_markup() -> InlineKeyboardMarkup:
        return kb(
            [btn("📊 Status", "m:status"), btn("❤️ Health", "m:health")],
            [btn("🔑 Gemini keys", "m:keys"), btn("➕ Add key", "m:addkey")],
            [btn("🧪 Test a key", "m:testkey"), btn("🌐 Test sites", "m:sites")],
            [btn("📥 Queues", "m:queue"), btn("🗂 Recent jobs", "m:jobs")],
            [btn("📈 Digest now", "m:digest")],
        )

    async def screen_menu(ev: Any) -> None:
        await ack(ev)
        await show(ev, "🎛 <b>Result service control</b>\nChoose an action:", menu_markup())

    async def screen_status(ev: Any) -> None:
        await ack(ev)
        await show(ev, svc.build_digest("📊 <b>Status</b>"),
                   kb([btn("🔄 Refresh", "m:status"), btn("❤️ Health", "m:health")], BACK))

    async def screen_digest(ev: Any) -> None:
        await ack(ev)
        await show(ev, svc.build_digest("📈 <b>Digest</b>"), kb([btn("🔄 Refresh", "m:digest")], BACK))

    async def screen_health(ev: Any) -> None:
        await ack(ev, "Probing…")
        msg = await show(ev, "⏳ Probing result sites and Gemini keys…")
        text = await tp(svc.build_health_text, True)
        await safe_edit(msg, text, kb([btn("🔄 Probe again", "m:health")], BACK))

    async def screen_sites(ev: Any) -> None:
        await ack(ev, "Probing…")
        msg = await show(ev, "⏳ Opening a session on each result site…")
        res = await tp(svc.probe_sites)
        lines = ["🌐 <b>Result sites</b>", ""]
        for s in res:
            ms = f" · {s['ms']}ms" if s.get("ms") is not None else ""
            lines.append(f"{'✅' if s['up'] else '❌'} {_e(s['site'])}{ms}"
                         + ("" if s["up"] else f"\n   <i>{_e(s['detail'])}</i>"))
        lines.append("\n<i>“up” = home page loaded and the anti-bot challenge passed.</i>")
        await safe_edit(msg, "\n".join(lines), kb([btn("🔄 Again", "m:sites")], BACK))

    async def screen_queue(ev: Any) -> None:
        await ack(ev)
        jc = svc.JOBS.counts()
        text = ("📥 <b>Queues</b>\n\n"
                f"JOB_Q: <b>{svc.JOB_Q.qsize()}</b> / {svc.QUEUE_MAX}\n"
                f"CB_Q: <b>{svc.CB_Q.qsize()}</b>\n\n"
                f"Jobs tracked: {svc.JOBS.size()}\n"
                f"queued {jc.get('queued', 0)} · running {jc.get('running', 0)} · "
                f"done {jc.get('done', 0)} · failed {jc.get('failed', 0)}")
        await show(ev, text, kb([btn("🔄 Refresh", "m:queue"), btn("🗂 Jobs", "m:jobs")], BACK))

    async def screen_jobs(ev: Any) -> None:
        await ack(ev)
        rows = svc.JOBS.recent(8)
        lines = ["🗂 <b>Recent jobs</b>", ""]
        icon = {"queued": "🕒", "running": "⚙️", "done": "✅", "failed": "❌"}
        if not rows:
            lines.append("<i>no jobs yet</i>")
        for j in rows:
            extra = j.get("source") or j.get("msg") or ""
            lines.append(f"{icon.get(j['state'], '•')} <code>…{j['job_id'][-6:]}</code> {j['state']}"
                         f" · {ago(j['created'])} ago · cb {j.get('callback')}"
                         + (f"\n   <i>{_e(str(extra)[:80])}</i>" if extra else ""))
        await show(ev, "\n".join(lines), kb([btn("🔄 Refresh", "m:jobs")], BACK))

    # ---- keys screen -------------------------------------------------------- #
    def keys_view(note: str = "") -> Tuple[str, InlineKeyboardMarkup]:
        ks = svc.POOL.status()
        ready = sum(1 for k in ks if k["available"])
        lines = [f"🔑 <b>Gemini keys</b> — {ready}/{len(ks)} ready", f"<i>model: {_e(svc.MODEL)}</i>", ""]
        rows: List[List[InlineKeyboardButton]] = []
        if not ks:
            lines.append("<i>No keys. Tap ➕ Add key.</i>")
        for i, k in enumerate(ks, 1):
            if k["available"]:
                state = "✅ ready"
            else:
                state = f"⏳ cooling {int(k['cool_left'])}s — {_e(k['why'] or 'cooldown')}"
            lines.append(f"{i}. <code>{k['tag']}</code>  {state}\n"
                         f"    ok {k['ok']} · fail {k['fail']}"
                         + (f" · last: {_e(k['last_err'])}" if k["last_err"] else ""))
            rows.append([btn(f"🧪 {k['tag']}", f"k:test:{k['id']}"),
                         btn("♻️ Reset", f"k:reset:{k['id']}"),
                         btn("🗑 Remove", f"k:rm:{k['id']}")])
        if ks:
            rows.append([btn("🧪 Test all", "k:testall"), btn("♻️ Reset all", "k:resetall")])
        rows.append([btn("➕ Add key", "m:addkey"), btn("🔄 Refresh", "m:keys")])
        rows.append(BACK)
        if note:
            lines += ["", note]
        return "\n".join(lines), kb(*rows)

    async def screen_keys(ev: Any, note: str = "") -> None:
        await ack(ev)
        text, markup = keys_view(note)
        await show(ev, text, markup)

    # ---- forms ------------------------------------------------------------------ #
    ADD_FORM = ("➕ <b>Add Gemini API key</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "<b>Step 1 of 3</b> · Send your key\n\n"
                "Paste one key, or several separated by spaces, commas or new lines "
                f"(max {MAX_KEYS_PER_MESSAGE}).\n"
                "Get a free key at https://aistudio.google.com/apikey\n\n"
                "🔒 I delete your message right after reading it, and I test every key "
                "with a real request before saving.")
    TEST_FORM = ("🧪 <b>Test a Gemini API key</b>\n"
                 "━━━━━━━━━━━━━━━━━━\n"
                 "Paste a key (or several). I'll run a real request against "
                 f"<code>{_e(svc.MODEL)}</code> and tell you if it works.\n"
                 "Nothing is saved.\n\n🔒 Your message is deleted after reading.")

    async def start_add(ev: Any, state: FSMContext) -> None:
        await ack(ev)
        await state.set_state(AddKey.waiting)
        await show(ev, ADD_FORM, kb([btn("✖ Cancel", "f:cancel")]))

    async def start_test(ev: Any, state: FSMContext) -> None:
        await ack(ev)
        await state.set_state(TestKey.waiting)
        await show(ev, TEST_FORM, kb([btn("✖ Cancel", "f:cancel")]))

    def parse_keys(text: str) -> List[str]:
        seen: List[str] = []
        for k in KEY_RE.findall(text or ""):
            k = k.strip(".")            # drop a full stop that ended a sentence
            if len(k) >= 20 and k not in seen:
                seen.append(k)
        return seen

    async def test_many(keys: List[str]) -> List[Dict[str, Any]]:
        return await asyncio.gather(*[tp(svc.probe_key, k, True) for k in keys])

    async def drop_secret(m: Message) -> None:
        try:
            await m.delete()
        except Exception:
            pass  # e.g. no permission in a group; nothing else we can do

    async def handle_key_input(m: Message, state: FSMContext, adding: bool, text: str) -> None:
        keys = parse_keys(text)
        await drop_secret(m)
        if not keys:
            await m.answer("🤔 I couldn't find a key in that. Send the key text "
                           "(it looks like <code>AIza…</code> or <code>AQ.…</code>), or /cancel.",
                           reply_markup=kb([btn("✖ Cancel", "f:cancel")]))
            return
        extra = max(0, len(keys) - MAX_KEYS_PER_MESSAGE)
        keys = keys[:MAX_KEYS_PER_MESSAGE]
        title = "<b>Step 2 of 3</b> · Testing" if adding else "<b>Testing</b>"
        status = await m.answer(f"{'➕' if adding else '🧪'} {title} {len(keys)} key(s)… ⏳")
        results = await test_many(keys)

        lines = [("➕ <b>Add Gemini API key</b>\n━━━━━━━━━━━━━━━━━━\n<b>Step 2 of 3</b> · Results"
                  if adding else "🧪 <b>Key test results</b>\n━━━━━━━━━━━━━━━━━━"), ""]
        good: List[str] = []
        for k, res in zip(keys, results):
            if adding and svc.POOL.has(k):
                lines.append(f"♻️ <code>{res['tag']}</code> — already in the pool")
                continue
            lines.append(fmt_probe(res))
            if svc.key_usable(res["state"]):
                good.append(k)
        if extra:
            lines.append(f"\n<i>{extra} extra key(s) ignored (limit {MAX_KEYS_PER_MESSAGE}).</i>")

        if not adding:
            await state.clear()
            await safe_edit(status, "\n".join(lines),
                            kb([btn("🧪 Test another", "m:testkey"), btn("➕ Add key", "m:addkey")], BACK))
            return

        if good:
            await state.set_state(AddKey.confirm)
            await state.update_data(pending=good)
            lines.append(f"\n<b>Step 3 of 3</b> · Save {len(good)} working key(s) to the pool?")
            markup = kb([btn(f"✅ Add {len(good)} key(s)", "f:add")],
                        [btn("🔁 Try again", "m:addkey"), btn("✖ Cancel", "f:cancel")])
        else:
            await state.set_state(AddKey.waiting)
            lines.append("\nNothing usable to add. Send another key, or cancel.")
            markup = kb([btn("✖ Cancel", "f:cancel")])
        await safe_edit(status, "\n".join(lines), markup)

    # ------------------------------ admin: commands -------------------------- #
    def command(name: str, screen: Callable[[Any], Awaitable[None]]) -> None:
        @adm.message(Command(name))
        async def _h(m: Message, state: FSMContext) -> None:
            await state.clear()
            await screen(m)

    command("start", screen_menu)
    command("menu", screen_menu)
    command("help", screen_menu)
    command("status", screen_status)
    command("health", screen_health)
    command("sites", screen_sites)
    command("keys", screen_keys)
    command("queue", screen_queue)
    command("jobs", screen_jobs)
    command("digest", screen_digest)

    @adm.message(Command("addkey"))
    async def cmd_addkey(m: Message, command: CommandObject, state: FSMContext) -> None:
        await state.clear()
        if command.args and parse_keys(command.args):
            await state.set_state(AddKey.waiting)
            await handle_key_input(m, state, True, command.args)
        else:
            await start_add(m, state)

    @adm.message(Command("testkey"))
    async def cmd_testkey(m: Message, command: CommandObject, state: FSMContext) -> None:
        await state.clear()
        if command.args and parse_keys(command.args):
            await handle_key_input(m, state, False, command.args)
        else:
            await start_test(m, state)

    @adm.message(Command("cancel"))
    async def cmd_cancel(m: Message, state: FSMContext) -> None:
        await state.clear()
        await m.answer("✖ Cancelled.", reply_markup=kb(BACK))

    # ------------------------------ admin: form input ------------------------ #
    @adm.message(AddKey.waiting, F.text, ~F.text.startswith("/"))
    async def add_input(m: Message, state: FSMContext) -> None:
        await handle_key_input(m, state, True, m.text or "")

    @adm.message(AddKey.confirm, F.text, ~F.text.startswith("/"))
    async def add_input_again(m: Message, state: FSMContext) -> None:
        # user pasted more keys instead of pressing the button: start the test over with these
        await handle_key_input(m, state, True, m.text or "")

    @adm.message(TestKey.waiting, F.text, ~F.text.startswith("/"))
    async def test_input(m: Message, state: FSMContext) -> None:
        await handle_key_input(m, state, False, m.text or "")

    # ------------------------------ admin: buttons --------------------------- #
    MENU: Dict[str, Callable[[Any], Awaitable[None]]] = {
        "menu": screen_menu, "status": screen_status, "health": screen_health,
        "keys": screen_keys, "sites": screen_sites, "queue": screen_queue,
        "jobs": screen_jobs, "digest": screen_digest,
    }

    @adm.callback_query(F.data.startswith("m:"))
    async def cb_menu(cb: CallbackQuery, state: FSMContext) -> None:
        action = (cb.data or "")[2:]
        if action == "addkey":
            await start_add(cb, state)
            return
        if action == "testkey":
            await start_test(cb, state)
            return
        await state.clear()
        screen = MENU.get(action)
        if screen:
            await screen(cb)
        else:
            await ack(cb, "Unknown action")

    @adm.callback_query(F.data == "f:cancel")
    async def cb_cancel(cb: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await screen_menu(cb)

    @adm.callback_query(F.data == "f:add")
    async def cb_add(cb: CallbackQuery, state: FSMContext) -> None:
        data = await state.get_data()
        pending: List[str] = data.get("pending") or []
        await state.clear()
        if not pending:
            await ack(cb, "Nothing pending")
            await screen_menu(cb)
            return
        n = await tp(svc.POOL.add_keys, pending)
        await ack(cb, f"Added {n}")
        tags = ", ".join(f"<code>…{k[-4:]}</code>" for k in pending)
        await show(cb, "➕ <b>Add Gemini API key</b>\n━━━━━━━━━━━━━━━━━━\n"
                       f"<b>Step 3 of 3</b> · Done ✅\n\nAdded {n} key(s): {tags}\n"
                       f"Pool now has <b>{len(svc.POOL.keys)}</b> key(s), "
                       f"<b>{svc.POOL.available()}</b> ready.",
                   kb([btn("🔑 Keys", "m:keys"), btn("➕ Add more", "m:addkey")], BACK))

    @adm.callback_query(F.data.startswith("k:"))
    async def cb_key(cb: CallbackQuery) -> None:
        parts = (cb.data or "").split(":")
        action = parts[1] if len(parts) > 1 else ""
        kid = parts[2] if len(parts) > 2 else ""

        if action == "test":
            key = svc.POOL.find(kid)
            if not key:
                await ack(cb, "Key no longer exists")
                await screen_keys(cb)
                return
            await ack(cb, "Testing…")
            res = await tp(svc.POOL.probe, key, True)
            text, markup = keys_view("🧪 <b>Test result</b>\n" + fmt_probe(res))
            await safe_edit(cb.message, text, markup)
        elif action == "testall":
            await ack(cb, "Testing all…")
            with_keys = [svc.POOL.find(k["id"]) for k in svc.POOL.status()]
            results = await asyncio.gather(*[tp(svc.POOL.probe, k, True) for k in with_keys if k])
            text, markup = keys_view("🧪 <b>Test results</b>\n" + "\n".join(fmt_probe(r) for r in results))
            await safe_edit(cb.message, text, markup)
        elif action == "reset":
            n = svc.POOL.reset(kid)
            await ack(cb, "Cooldown cleared" if n else "Key not found")
            await screen_keys(cb)
        elif action == "resetall":
            n = svc.POOL.reset(None)
            await ack(cb, f"Cleared {n} cooldown(s)")
            await screen_keys(cb)
        elif action == "rm":
            key = svc.POOL.find(kid)
            if not key:
                await ack(cb, "Key no longer exists")
                await screen_keys(cb)
                return
            await ack(cb)
            warn = "\n⚠️ <b>This is your last key</b> — captcha solving stops until you add another." \
                if len(svc.POOL.keys) == 1 else ""
            await show(cb, f"🗑 Remove <code>…{key[-4:]}</code> from the pool?{warn}",
                       kb([btn("✅ Yes, remove", f"k:rmy:{kid}"), btn("✖ No", "m:keys")]))
        elif action == "rmy":
            ok = await tp(svc.POOL.remove, kid)
            await ack(cb, "Removed" if ok else "Key not found")
            await screen_keys(cb, "🗑 Key removed." if ok else "")
        else:
            await ack(cb, "Unknown action")

    # ------------------------------ everyone else ---------------------------- #
    @fb.message()
    async def deny_msg(m: Message) -> None:
        uid = m.from_user.id if m.from_user else 0
        if uid in admin_ids:  # an admin typed something that is not a command or form input
            await m.answer("Use the buttons below or /menu.", reply_markup=menu_markup())
            return
        await m.answer(f"⛔ Not authorized.\nYour Telegram id is <code>{uid}</code> — "
                       "ask the owner to add it to <code>ADMIN_IDS</code>.")

    @fb.callback_query()
    async def deny_cb(cb: CallbackQuery) -> None:
        await cb.answer("⛔ Not authorized", show_alert=True)

    return bot, dp


COMMANDS = [
    ("menu", "Main menu with buttons"), ("status", "Live status"), ("health", "Probe sites + Gemini keys"),
    ("keys", "Manage Gemini keys"), ("addkey", "Add Gemini key(s) — guided form"),
    ("testkey", "Test a Gemini key"), ("sites", "Probe result sites"), ("queue", "Queue depths"),
    ("jobs", "Recent jobs"), ("digest", "Digest now"), ("cancel", "Cancel current form"),
    ("id", "Show my Telegram id"),
]


async def _amain(svc: Any) -> None:
    bot, dp = build(svc)
    try:
        await bot.set_my_commands([BotCommand(command=c, description=d) for c, d in COMMANDS])
    except Exception as e:  # cosmetic only
        log.warning("could not set bot commands (%s)", type(e).__name__)

    async def watcher() -> None:
        while not svc.STOP.is_set():
            await asyncio.sleep(1)
        try:
            await dp.stop_polling()
        except Exception:
            pass

    wt = asyncio.create_task(watcher())
    try:
        log.info("telegram bot polling (admins: %d)", len(svc.ADMIN_IDS))
        await dp.start_polling(bot, handle_signals=False,
                               allowed_updates=dp.resolve_used_update_types())
    finally:
        wt.cancel()
        await bot.session.close()


def run_bot(svc: Any) -> None:
    """Thread target (called by main.py). Restarts itself if polling ever dies."""
    backoff = 5
    while not svc.STOP.is_set():
        try:
            asyncio.run(_amain(svc))
            if svc.STOP.is_set():
                break
            log.warning("bot polling ended unexpectedly, restarting")
        except Exception as e:
            # message only: aiogram errors can embed the request URL which contains the token
            log.error("bot crashed (%s), restarting in %ss", type(e).__name__, backoff)
        if svc.STOP.wait(backoff):
            break
        backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    print("bot.py is started by main.py - run:  python main.py")
