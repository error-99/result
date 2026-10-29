#!/usr/bin/env python3
"""
Result checker + Flask queue + callback to PHP + Telegram control bot (production build)

Files:
    main.py   this file - the service (API, queue, workers, alerts, digest, probes)
    bot.py    aiogram Telegram bot (menu, buttons, add/test Gemini keys). Started by main.py.

Run (recommended):
    pip install -r requirements.txt
    python main.py                                # uses waitress if installed
  or
    gunicorn -w 1 --threads 8 -b 0.0.0.0:5000 main:APP

NOTE: the job queue is in memory, so run exactly ONE process (threads are fine).
      Run only ONE copy of the bot too (Telegram allows one polling client per token).

Environment / .env:
    GEMINI_KEYS=k1,k2,k3            (or GEMINI_API_KEY=k1)  - may be empty if BOT_TOKEN is set
                                    (then add keys from Telegram with /addkey)
    KEYS_FILE=gemini_keys.json      keys added/removed from Telegram are stored here
    API_TOKEN=long-random-string    clients send  Authorization: Bearer <token>  or  X-API-Key
    CALLBACK_SECRET=another-secret  sent as X-Callback-Secret + HMAC in X-Callback-Signature
    CALLBACK_ALLOWED_HOSTS=example.com,panel.example.com   (optional allow-list)
    ALLOW_PRIVATE_CALLBACKS=0       set 1 only if your PHP lives on localhost / private IP
    API_HOST=0.0.0.0  API_PORT=5000  WORKERS=2  CALLBACK_WORKERS=2  QUEUE_MAX=200
    JOB_TIMEOUT=240  JOB_TTL=3600  CAPTCHA_ATTEMPTS=3  CALLBACK_ATTEMPTS=5
    DEAD_LETTER_PATH=failed_callbacks.jsonl  GEMINI_MODEL=gemini-flash-lite-latest
    LOG_LEVEL=INFO

Telegram (all optional - without BOT_TOKEN the service runs exactly as before):
    BOT_TOKEN=123456:ABC...         from @BotFather
    ADMIN_IDS=111111,222222         Telegram user ids allowed to use the bot (get yours with /id)
    ALERT_CHAT_ID=                  where alerts/digests go (default: every admin)
    ALERT_ON_JOB_FAIL=1             alert on every failed job
    ALERT_MASK_PAYLOAD=0            1 = mask roll/reg inside alerts
    ALERT_MAX_PER_MIN=20            flood guard for alerts
    ALERT_ON_REQUEST=1              send every incoming /result request + the HTTP reply to Telegram
    ALERT_ON_RESPONSE=1             send every finished job's JSON (or error) + callback delivery status
    TRAFFIC_MAX_PER_MIN=60          flood guard for the two traffic feeds above

Request flow tuning:
    PREFLIGHT_TIMEOUT=10            max seconds to wait for the per-request check of all sites + keys
    PREFLIGHT_KEY_TTL=10            re-check all Gemini keys at most every N seconds (0 = every request)
    ALERT_KEYS_COOLDOWN=600         min seconds between "all keys cooling down" alerts
    DIGEST_ENABLED=1  DIGEST_INTERVAL=3600
    HEALTH_CACHE_TTL=60  PROBE_TIMEOUT=25
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import html
import ipaddress
import json
import logging
import os
import queue
import re
import signal
import socket
import sys
import threading
import time
from collections import OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests
from flask import Flask, jsonify, request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- #
# Logging + configuration
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("result")


def load_env_file(path: Optional[str] = None) -> None:
    """Load KEY=VALUE lines into os.environ (real environment wins)."""
    path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                if k.startswith("export "):
                    k = k[7:].strip()
                v = v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                    v = v[1:-1]
                os.environ.setdefault(k, v)
    except OSError as e:
        log.warning("could not read %s: %s", path, e)


load_env_file()


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        log.warning("invalid %s=%r, using %s", name, raw, default)
        return default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def parse_ids(raw: str) -> List[int]:
    out: List[int] = []
    for part in re.split(r"[,\s]+", raw or ""):
        part = part.strip()
        if part and re.fullmatch(r"-?\d+", part) and int(part) not in out:
            out.append(int(part))
    return out


MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-lite-latest")
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = env_int("API_PORT", env_int("PORT", 5000))  # Railway sets PORT
API_TOKEN = os.environ.get("API_TOKEN", "").strip()
CALLBACK_SECRET = os.environ.get("CALLBACK_SECRET", "").strip()
WORKERS = env_int("WORKERS", 2)
CB_WORKERS = env_int("CALLBACK_WORKERS", 2)
QUEUE_MAX = env_int("QUEUE_MAX", 200)
JOB_TIMEOUT = env_int("JOB_TIMEOUT", 240, 10)
JOB_TTL = env_int("JOB_TTL", 3600, 60)
JOB_STORE_MAX = env_int("JOB_STORE_MAX", 5000, 100)
CAPTCHA_ATTEMPTS = env_int("CAPTCHA_ATTEMPTS", 3)
PREFLIGHT_TIMEOUT = env_int("PREFLIGHT_TIMEOUT", 10)
PREFLIGHT_KEY_TTL = env_int("PREFLIGHT_KEY_TTL", 10, 0)
CB_ATTEMPTS = env_int("CALLBACK_ATTEMPTS", 5)
SHUTDOWN_GRACE = env_int("SHUTDOWN_GRACE", 30, 0)
DEAD_LETTER_PATH = os.environ.get("DEAD_LETTER_PATH", "failed_callbacks.jsonl")
ALLOW_PRIVATE_CB = env_bool("ALLOW_PRIVATE_CALLBACKS", False)
ALLOWED_CB_HOSTS = {
    h.strip().lower() for h in os.environ.get("CALLBACK_ALLOWED_HOSTS", "").split(",") if h.strip()
}
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# Telegram / alerts / digest / probes
KEYS_FILE = os.environ.get("KEYS_FILE", "gemini_keys.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_IDS = frozenset(parse_ids(os.environ.get("ADMIN_IDS", "")))
ALERT_CHAT_IDS = parse_ids(os.environ.get("ALERT_CHAT_ID", "")) or sorted(ADMIN_IDS)
ALERT_ON_JOB_FAIL = env_bool("ALERT_ON_JOB_FAIL", True)
ALERT_MASK_PAYLOAD = env_bool("ALERT_MASK_PAYLOAD", False)
ALERT_MAX_PER_MIN = env_int("ALERT_MAX_PER_MIN", 20)
ALERT_KEYS_COOLDOWN = env_int("ALERT_KEYS_COOLDOWN", 600, 0)
ALERT_ON_REQUEST = env_bool("ALERT_ON_REQUEST", True)
ALERT_ON_RESPONSE = env_bool("ALERT_ON_RESPONSE", True)
TRAFFIC_MAX_PER_MIN = env_int("TRAFFIC_MAX_PER_MIN", 60)
DIGEST_ENABLED = env_bool("DIGEST_ENABLED", True)
DIGEST_INTERVAL = env_int("DIGEST_INTERVAL", 3600, 60)
PROBE_TTL = env_int("HEALTH_CACHE_TTL", 60, 0)
PROBE_TIMEOUT = env_int("PROBE_TIMEOUT", 25, 5)

STOP = threading.Event()
START_TIME = time.time()


def mask(value: Any, keep: int = 3) -> str:
    s = str(value)
    return "*" * max(0, len(s) - keep) + s[-keep:]


def esc(value: Any) -> str:
    """HTML-escape for Telegram HTML parse mode."""
    return html.escape(str(value), quote=False)


def fmt_dur(sec: float) -> str:
    sec = int(max(0, sec))
    d, r = divmod(sec, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    if d:
        return f"{d}d {h:02d}h"
    if h:
        return f"{h}h {m:02d}m"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def key_id(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:8]


def key_tag(key: str) -> str:
    return f"…{key[-4:]}"


# --------------------------------------------------------------------------- #
# Stats (lifetime counters since process start)
# --------------------------------------------------------------------------- #
class Stats:
    def __init__(self) -> None:
        self._l = threading.Lock()
        self._c: Dict[str, int] = defaultdict(int)
        self.since = time.time()

    def inc(self, name: str, n: int = 1) -> None:
        with self._l:
            self._c[name] += n

    def snap(self) -> Dict[str, int]:
        with self._l:
            return dict(self._c)


STATS = Stats()


# --------------------------------------------------------------------------- #
# Telegram notifier (non-blocking alerts; works without aiogram)
# --------------------------------------------------------------------------- #
class Notifier:
    """Queue + one sender thread, so request/worker threads are never blocked by Telegram."""

    def __init__(self, token: str, chats: List[int], max_per_min: int):
        self.token = token
        self.chats = list(chats)
        self.enabled = bool(token and chats)
        self.max_per_min = max_per_min
        self._q: "queue.Queue[str]" = queue.Queue(maxsize=100)
        self._sent: "deque[float]" = deque()
        self._suppressed = 0
        self._once: Dict[str, float] = {}
        self._lock = threading.Lock()

    def send(self, text: str, force: bool = False) -> bool:
        """Queue a message (HTML). Returns False if disabled, rate-limited or queue full."""
        if not self.enabled:
            return False
        with self._lock:
            now = time.monotonic()
            while self._sent and now - self._sent[0] > 60:
                self._sent.popleft()
            if not force and len(self._sent) >= self.max_per_min:
                self._suppressed += 1
                return False
            self._sent.append(now)
            if self._suppressed:
                text += f"\n\n<i>(+{self._suppressed} alert(s) suppressed by the rate limit)</i>"
                self._suppressed = 0
        try:
            self._q.put_nowait(text)
            return True
        except queue.Full:
            return False

    def once(self, key: str, cooldown: float, text: str) -> bool:
        """Send at most one alert per `cooldown` seconds for this key."""
        with self._lock:
            now = time.monotonic()
            last = self._once.get(key)
            if last is not None and now - last < cooldown:
                return False
            self._once[key] = now
        return self.send(text)

    def _post(self, chat: int, text: str, html_mode: bool) -> requests.Response:
        payload: Dict[str, Any] = {"chat_id": chat, "text": text[:4000],
                                   "disable_web_page_preview": True}
        if html_mode:
            payload["parse_mode"] = "HTML"
        return requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                             json=payload, timeout=(5, 15))

    def loop(self) -> None:
        while not STOP.is_set():
            try:
                text = self._q.get(timeout=1)
            except queue.Empty:
                continue
            for chat in self.chats:
                try:
                    r = self._post(chat, text, True)
                    if r.status_code == 429:
                        try:
                            wait_s = int(r.json().get("parameters", {}).get("retry_after", 5))
                        except (ValueError, TypeError, AttributeError):
                            wait_s = 5
                        STOP.wait(min(wait_s, 30))
                        r = self._post(chat, text, True)
                    if r.status_code == 400:  # bad HTML (e.g. truncated tag): resend as plain text
                        r = self._post(chat, re.sub(r"<[^>]+>", "", html.unescape(text)), False)
                    if not r.ok:
                        log.warning("telegram alert to %s failed: HTTP %s", chat, r.status_code)
                except requests.RequestException as e:
                    # never log the exception text: it contains the bot token in the URL
                    log.warning("telegram alert to %s failed (%s)", chat, type(e).__name__)
                except Exception:
                    log.exception("telegram alert crashed")


ALERTS = Notifier(BOT_TOKEN, ALERT_CHAT_IDS, ALERT_MAX_PER_MIN)


def clip(text: Any, n: int = 900) -> str:
    s = str(text)
    return s if len(s) <= n else s[:n] + "…"


def payload_for_alert(p: Dict[str, Any]) -> str:
    d = dict(p)
    cb = d.get("callback_url")
    if cb:
        try:
            u = urlparse(cb)
            d["callback_url"] = urlunparse((u.scheme, u.netloc, u.path, "", "", ""))  # drop ?tokens
        except ValueError:
            d["callback_url"] = "<unparseable>"
    if ALERT_MASK_PAYLOAD:
        for f in ("roll", "reg"):
            if d.get(f):
                d[f] = mask(d[f])
    return json.dumps(d, ensure_ascii=False, default=str)


def alert_job_failed(job_id: str, p: Dict[str, Any], res: Dict[str, Any], secs: float) -> None:
    if not ALERT_ON_JOB_FAIL or not ALERTS.enabled:
        return
    errs = res.get("errors") or []
    text = (f"🚨 <b>Job failed</b>\n"
            f"job: <code>{esc(job_id)}</code>  ·  took {secs:.1f}s\n"
            f"reason: {esc(clip(res.get('msg') or 'unknown', 300))}\n")
    if errs:
        text += "details:\n" + "\n".join(f"  • {esc(clip(e, 200))}" for e in errs[:6]) + "\n"
    text += f"payload:\n<pre>{esc(clip(payload_for_alert(p), 900))}</pre>"
    ALERTS.send(text)


# --------------------------------------------------------------------------- #
# Gemini key pool
# --------------------------------------------------------------------------- #
class SolveError(Exception):
    """Captcha could not be solved. fatal=True means Gemini itself is unusable right now
    (no keys / all keys exhausted) so trying another result site would not help."""

    def __init__(self, msg: str = "", fatal: bool = False):
        super().__init__(msg)
        self.fatal = fatal


class JobTimeout(Exception):
    """Overall per-job deadline exceeded."""


def check_deadline(deadline: Optional[float]) -> None:
    if deadline is not None and time.monotonic() > deadline:
        raise JobTimeout("job deadline exceeded")


def load_keys() -> List[str]:
    raw = os.environ.get("GEMINI_KEYS", "") or os.environ.get("GEMINI_API_KEY", "")
    keys: List[str] = []
    for k in raw.split(","):
        k = k.strip()
        if k and k not in keys:
            keys.append(k)
    return keys


def sniff_mime(data: bytes) -> Optional[str]:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def retry_after(resp: requests.Response, default: int) -> int:
    try:
        return max(1, min(300, int(resp.headers.get("Retry-After", default))))
    except (TypeError, ValueError):
        return default


GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models"


_GENERATE_ONLY_KEYS: set = set()   # keys the metadata check rejects but generateContent accepts


def probe_key(key: str, deep: bool = False) -> Dict[str, Any]:
    """Check one Gemini key. If the cheap metadata check says "invalid", confirm it with ONE tiny real
    generateContent call before condemning the key (protects newer key formats from a false "invalid")."""
    if not deep and key in _GENERATE_ONLY_KEYS:
        return {"id": key_id(key), "tag": key_tag(key), "deep": False, "state": "valid", "http": 200,
                "ms": 0, "detail": "quick check skipped (key verified by a real call)"}
    out = _probe_key_once(key, deep)
    if not deep and out["state"] == "invalid":
        real = _probe_key_once(key, True)
        if real["state"] in ("valid", "rate_limited"):
            _GENERATE_ONLY_KEYS.add(key)
            log.info("key%s fails the metadata check but works for generateContent - accepted", key_tag(key))
            real["deep"] = False
            real["detail"] = "works for generateContent (metadata check not supported by this key)"
            return real
        return real if real["state"] != "error" else out
    return out


def _probe_key_once(key: str, deep: bool = False) -> Dict[str, Any]:
    """
    Check one Gemini key.
      quick (deep=False): GET model metadata - validates the key without using generation quota.
      deep  (deep=True):  a tiny real generateContent call - proves the key can actually generate.
    state: valid | rate_limited | invalid | model_unavailable | server_error | network_error | error
    (valid and rate_limited both mean "the key itself is good").
    """
    t0 = time.monotonic()
    base = f"{GEMINI_BASE}/{MODEL}"
    headers = {"x-goog-api-key": key}
    out: Dict[str, Any] = {"id": key_id(key), "tag": key_tag(key), "deep": deep,
                           "state": "error", "http": None, "ms": None, "detail": ""}
    try:
        if deep:
            r = requests.post(
                f"{base}:generateContent", headers=headers, timeout=(5, 25),
                json={"contents": [{"parts": [{"text": "Reply with the single word OK."}]}],
                      "generationConfig": {"temperature": 0, "maxOutputTokens": 16}})
        else:
            r = requests.get(base, headers=headers, timeout=(5, 15))
    except requests.RequestException as e:
        out.update(state="network_error", detail=type(e).__name__,
                   ms=int((time.monotonic() - t0) * 1000))
        return out

    out["ms"] = int((time.monotonic() - t0) * 1000)
    sc = r.status_code
    out["http"] = sc
    try:
        detail = str((r.json().get("error") or {}).get("message") or "")
    except (ValueError, AttributeError):
        detail = ""
    out["detail"] = clip(detail.replace(key, "***"), 160)
    low = (r.text or "").lower()
    if sc == 200:
        out["state"] = "valid"
    elif sc == 429:
        out["state"] = "rate_limited"
    elif sc in (401, 403) or (sc == 400 and ("api key" in low or "api_key_invalid" in low)):
        out["state"] = "invalid"
    elif sc == 404:
        out["state"] = "model_unavailable"
    elif sc >= 500:
        out["state"] = "server_error"
    return out


def key_usable(state: str) -> bool:
    return state in ("valid", "rate_limited")


def run_parallel(fns: List[Callable[[], Any]], timeout: float) -> List[Any]:
    """Run callables concurrently; a result is None if it raised or did not finish in time."""
    if not fns:
        return []
    ex = ThreadPoolExecutor(max_workers=len(fns), thread_name_prefix="probe")
    futs = [ex.submit(f) for f in fns]
    done, _ = wait(futs, timeout=timeout)
    out: List[Any] = []
    for f in futs:
        out.append(f.result() if f in done and f.exception() is None else None)
    ex.shutdown(wait=False, cancel_futures=True)
    return out


class KeyPool:
    def __init__(self, keys: List[str], model: str, env_keys: Optional[List[str]] = None,
                 allow_empty: bool = False):
        if not keys and not allow_empty:
            raise RuntimeError("no Gemini keys: set GEMINI_KEYS=k1,k2,... in .env or the environment "
                               "(or set BOT_TOKEN and add keys from Telegram)")
        self.keys = list(keys)
        self.model = model
        self.env_keys = list(env_keys if env_keys is not None else keys)
        self._cool = {k: 0.0 for k in self.keys}
        self._busy: Dict[str, int] = {k: 0 for k in self.keys}   # requests using the key right now
        self._last_check = 0.0
        self._why: Dict[str, str] = {}
        self._stat: Dict[str, Dict[str, Any]] = {k: {"ok": 0, "fail": 0} for k in self.keys}
        self._rr = 0
        self._lock = threading.Lock()

    # ---- selection / state ------------------------------------------------ #
    def _pick(self, exclude: Tuple[str, ...] = ()) -> Tuple[Optional[str], float]:
        """Pick a live key and mark it busy (call _release afterwards).
        Prefers a key nobody is using right now, then round-robin; keys that already failed for
        this captcha (`exclude`) are skipped unless nothing else is live."""
        now = time.monotonic()
        with self._lock:
            n = len(self.keys)
            if n == 0:
                return None, 0.0
            live: List[Tuple[int, int, str]] = []
            soonest: Optional[float] = None
            for j in range(n):
                k = self.keys[(self._rr + j) % n]
                until = self._cool[k]
                if until <= now:
                    live.append((self._busy.get(k, 0), j, k))
                else:
                    soonest = until if soonest is None else min(soonest, until)
            if live:
                fresh = [c for c in live if c[2] not in exclude] or live
                _, _, k = min(fresh)
                self._rr = (self.keys.index(k) + 1) % n
                self._busy[k] = self._busy.get(k, 0) + 1
                return k, 0.0
            return None, max(0.0, (soonest or now) - now)

    def _release(self, key: str) -> None:
        with self._lock:
            if key in self._busy:
                self._busy[key] = max(0, self._busy[key] - 1)

    def preflight(self, ttl: float) -> Optional[List[Dict[str, Any]]]:
        """Check every key (cheap metadata call). Results feed the cooldown state, so dead keys are
        skipped by _pick. Throttled by `ttl` seconds so a burst of requests does not hammer Google."""
        now = time.monotonic()
        with self._lock:
            if ttl > 0 and now - self._last_check < ttl:
                return None
            self._last_check = now
        return self.probe_all(False)

    def _cooldown(self, key: str, secs: float, why: str = "") -> None:
        with self._lock:
            if key not in self._cool:  # removed meanwhile
                return
            self._cool[key] = max(self._cool[key], time.monotonic() + secs)
            if why:
                self._why[key] = why

    def _note(self, key: str, ok: bool, err: str = "") -> None:
        with self._lock:
            st = self._stat.get(key)
            if st is None:
                return
            st["ok" if ok else "fail"] += 1
            if err:
                st["last_err"] = err

    def available(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for v in self._cool.values() if v <= now)

    def status(self) -> List[Dict[str, Any]]:
        now = time.monotonic()
        with self._lock:
            out = []
            for k in self.keys:
                left = max(0.0, self._cool[k] - now)
                st = self._stat.get(k, {})
                out.append({"id": key_id(k), "tag": key_tag(k), "available": left <= 0,
                            "cool_left": left, "why": self._why.get(k, "") if left > 0 else "",
                            "ok": st.get("ok", 0), "fail": st.get("fail", 0),
                            "last_err": st.get("last_err", "")})
            return out

    def find(self, kid: str) -> Optional[str]:
        with self._lock:
            return next((k for k in self.keys if key_id(k) == kid), None)

    def has(self, key: str) -> bool:
        with self._lock:
            return key in self._cool

    def reset(self, kid: Optional[str] = None) -> int:
        """Clear cooldowns (one key by id, or all). Returns how many keys were reset."""
        n = 0
        with self._lock:
            for k in self.keys:
                if kid is None or key_id(k) == kid:
                    self._cool[k] = 0.0
                    self._why.pop(k, None)
                    n += 1
        return n

    # ---- runtime management (used by the Telegram bot) ---------------------- #
    def add_keys(self, new: List[str]) -> int:
        added = 0
        with self._lock:
            for k in new:
                k = k.strip()
                if k and k not in self._cool:
                    self.keys.append(k)
                    self._cool[k] = 0.0
                    self._busy[k] = 0
                    self._stat[k] = {"ok": 0, "fail": 0}
                    added += 1
        if added:
            self._persist()
            log.info("keys: added %d key(s) via bot, now %d", added, len(self.keys))
        return added

    def remove(self, kid: str) -> bool:
        with self._lock:
            key = next((k for k in self.keys if key_id(k) == kid), None)
            if key is None:
                return False
            self.keys.remove(key)
            self._cool.pop(key, None)
            self._busy.pop(key, None)
            self._why.pop(key, None)
            self._stat.pop(key, None)
            self._rr = 0
        self._persist()
        log.info("keys: removed key %s via bot, now %d", key_tag(key), len(self.keys))
        return True

    def _persist(self) -> None:
        with self._lock:
            added = [k for k in self.keys if k not in self.env_keys]
            removed = [k for k in self.env_keys if k not in self.keys]
        try:
            tmp = KEYS_FILE + ".tmp"
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"added": added, "removed": removed}, fh)
            os.replace(tmp, KEYS_FILE)
        except OSError as e:
            log.error("could not save %s: %s (keys will be lost on restart)", KEYS_FILE, e)

    # ---- probing -------------------------------------------------------------- #
    def probe(self, key: str, deep: bool = False) -> Dict[str, Any]:
        """Probe one key and fold the result back into the pool's cooldown state."""
        res = probe_key(key, deep)
        if self.has(key):
            st = res["state"]
            if st == "invalid":
                self._cooldown(key, 3600, f"key rejected (HTTP {res['http']})")
            elif st == "rate_limited":
                self._cooldown(key, 60, "rate limited")
            elif st == "valid" and deep:  # a real generation just worked: lift any cooldown
                self.reset(res["id"])
        return res

    def probe_all(self, deep: bool = False) -> List[Dict[str, Any]]:
        with self._lock:
            keys = list(self.keys)
        results = run_parallel([lambda k=k: self.probe(k, deep) for k in keys], PROBE_TIMEOUT)
        return [r if r is not None else
                {"id": key_id(k), "tag": key_tag(k), "state": "error", "http": None, "ms": None,
                 "detail": "probe timed out", "deep": deep}
                for k, r in zip(keys, results)]

    # ---- solving -------------------------------------------------------------- #
    def _alert_exhausted(self, reason: str) -> None:
        if not ALERTS.enabled:
            return
        lines = [f"🔴 <b>All Gemini keys unavailable</b>", f"reason: {esc(reason)}"]
        for k in self.status():
            if k["available"]:
                lines.append(f"  • <code>{k['tag']}</code> ready")
            else:
                lines.append(f"  • <code>{k['tag']}</code> cooling {int(k['cool_left'])}s "
                             f"— {esc(k['why'] or 'cooldown')}")
        lines.append("Add a key with /addkey or reset with /keys.")
        ALERTS.once("keys_exhausted", ALERT_KEYS_COOLDOWN, "\n".join(lines))

    def solve(self, img: bytes, mime: str, deadline: Optional[float] = None) -> str:
        url = f"{GEMINI_BASE}/{self.model}:generateContent"
        body = {
            "contents": [{"parts": [
                {"text": "This is a captcha image. Reply with ONLY the characters shown, nothing else."},
                {"inline_data": {"mime_type": mime, "data": base64.b64encode(img).decode()}},
            ]}],
            "generationConfig": {"temperature": 0, "maxOutputTokens": 64},
        }
        if not self.keys:
            self._alert_exhausted("no keys configured")
            raise SolveError("no Gemini keys configured", fatal=True)
        last = "no attempt made"
        failed: List[str] = []          # keys that already failed for THIS captcha -> try another one
        for _ in range(len(self.keys) * 2 + 1):
            check_deadline(deadline)
            key, wait_s = self._pick(tuple(failed))
            if key is None:
                if not self.keys:
                    raise SolveError("no Gemini keys configured", fatal=True)
                if wait_s > 15:
                    self._alert_exhausted(f"all keys cooling down ({int(wait_s)}s)")
                    raise SolveError(f"all Gemini keys cooling down ({int(wait_s)}s)", fatal=True)
                time.sleep(wait_s + 0.1)
                continue
            tag = f"key{key_tag(key)}"
            try:
                try:
                    # key goes in a header, never in the URL, so it can't leak via exception text
                    r = requests.post(url, json=body, timeout=(5, 30),
                                      headers={"x-goog-api-key": key})
                except requests.RequestException as e:
                    self._cooldown(key, 5, "network error")
                    last = f"network error ({type(e).__name__})"
                    self._note(key, False, last)
                    failed.append(key)
                    log.warning("gemini %s: %s -> retrying with another key", tag, last)
                    continue

                sc = r.status_code
                if sc == 429:
                    self._cooldown(key, retry_after(r, 60), "rate limited (429)")
                    last = "rate limited"
                    self._note(key, False, last)
                    failed.append(key)
                    log.warning("gemini %s: 429 rate limited -> retrying with another key", tag)
                    continue
                if sc in (401, 403) or (sc == 400 and "api key" in r.text.lower()):
                    self._cooldown(key, 3600, f"key rejected (HTTP {sc})")
                    last = f"key rejected (HTTP {sc})"
                    self._note(key, False, last)
                    failed.append(key)
                    log.error("gemini %s: %s - disabled for 1h", tag, last)
                    continue
                if sc >= 500:
                    self._cooldown(key, 5, f"server error (HTTP {sc})")
                    last = f"gemini server error (HTTP {sc})"
                    self._note(key, False, last)
                    failed.append(key)
                    log.warning("gemini %s: %s -> retrying with another key", tag, last)
                    continue
                if sc != 200:
                    self._note(key, False, f"HTTP {sc}")
                    raise SolveError(f"gemini HTTP {sc}")

                try:
                    data = r.json()
                    cands = data.get("candidates") or []
                    parts = ((cands[0].get("content") or {}).get("parts") or []) if cands else []
                    text = "".join(p.get("text", "") for p in parts)
                except (ValueError, AttributeError, IndexError, TypeError):
                    raise SolveError("unparseable Gemini response")
                code = re.sub(r"[^A-Za-z0-9]", "", text)
                if not code:
                    raise SolveError("Gemini returned no characters")
                self._note(key, True)
                return code
            finally:
                self._release(key)
        # every attempt was spent on failing keys
        if self.available() == 0:
            self._alert_exhausted(f"all keys exhausted (last: {last})")
        raise SolveError(f"all Gemini keys exhausted (last: {last})", fatal=True)


def read_keys_file() -> Tuple[List[str], List[str]]:
    try:
        with open(KEYS_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
        return ([str(k) for k in d.get("added", []) if k], [str(k) for k in d.get("removed", []) if k])
    except FileNotFoundError:
        return [], []
    except (OSError, ValueError, AttributeError) as e:
        log.warning("could not read %s: %s", KEYS_FILE, e)
        return [], []


def build_pool() -> KeyPool:
    base = load_keys()
    added, removed = read_keys_file()
    keys = [k for k in base if k not in removed] + [k for k in added if k not in base]
    return KeyPool(keys, MODEL, env_keys=base, allow_empty=bool(BOT_TOKEN))


POOL = build_pool()


# --------------------------------------------------------------------------- #
# Result sites
# --------------------------------------------------------------------------- #
class ChallengeError(Exception):
    pass


def parse_challenge(html_text: str) -> Optional[Dict[str, Any]]:
    """Return {seed, salt, token} if the page carries a browser challenge, else None."""
    if '"seed"' not in html_text:
        return None
    m = re.search(r"challenge\s*=\s*(\{.*?\})\s*;", html_text, re.S)
    if m:
        try:
            ch = json.loads(m.group(1))
            if all(k in ch for k in ("seed", "salt", "token")):
                return ch
        except ValueError:
            pass
    seed = re.search(r'"seed"\s*:\s*"?(\d+)"?', html_text)
    salt = re.search(r'"salt"\s*:\s*"([^"]+)"', html_text)
    token = re.search(r'"token"\s*:\s*"([^"]+)"', html_text)
    if seed and salt and token:
        return {"seed": seed.group(1), "salt": salt.group(1), "token": token.group(1)}
    raise ChallengeError("unrecognised challenge format")


class Site:
    def __init__(self, name: str, base: str):
        self.name, self.base = name, base

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = UA
        retry = Retry(total=2, connect=2, read=0, backoff_factor=0.5,
                      status_forcelist=(502, 503, 504), allowed_methods=frozenset({"GET"}),
                      raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def open_session(self) -> Optional[requests.Session]:
        """Return a ready session, or None if the site is down / challenge failed."""
        s = self._session()
        try:
            r = s.get(f"{self.base}/v2/home", timeout=(5, 20))
            ch = parse_challenge(r.text)
            if ch is None:
                if r.ok:
                    return s
                log.info("%s: HTTP %s on home page", self.name, r.status_code)
                return None
            proof = hashlib.sha256(
                f"browser-proof:{ch['seed']}:{ch['salt']}".encode()).hexdigest()
            v = s.post(f"{self.base}/_challenge-verify",
                       json={"token": ch["token"], "answer": "hash:" + proof}, timeout=(5, 20))
            if v.ok and v.json().get("ok"):
                return s
            log.info("%s: challenge verification rejected (HTTP %s)", self.name, v.status_code)
        except (requests.RequestException, ValueError, ChallengeError) as e:
            log.info("%s: unavailable (%s: %s)", self.name, type(e).__name__, e)
        return None

    def fetch_result(self, s: requests.Session, roll: str, reg: str, exam: str,
                     board: str, year: str, deadline: Optional[float]) -> Dict[str, Any]:
        """Returns the site's JSON on success, or {"status": 1, "msg": ..., "kind": K} where K is
             definitive - the site answered (invalid registration, result not published, ...):
                          report it to the caller, never retry, never switch server
             captcha    - the site kept rejecting our captcha: try the next server
             server     - the site misbehaved (5xx, not JSON, no captcha image): try the next server
        Gemini problems are retried here with a fresh captcha; if Gemini itself is unusable a fatal
        SolveError is raised."""
        last, kind = "captcha failed after retries", "captcha"
        solver_fails = 0
        for attempt in range(1, CAPTCHA_ATTEMPTS + 1):
            check_deadline(deadline)
            cr = s.get(f"{self.base}/v2/captcha?t={int(time.time() * 1000)}", timeout=(5, 30))
            cr.raise_for_status()
            img = cr.content
            mime = sniff_mime(img)
            if not mime:
                last, kind = "captcha endpoint did not return an image", "server"
                log.warning("%s: %s (attempt %d)", self.name, last, attempt)
                time.sleep(1)
                continue
            try:
                code = POOL.solve(img, mime, deadline)
            except SolveError as e:
                if e.fatal:
                    raise
                solver_fails += 1
                last, kind = f"captcha solver: {e}", "captcha"
                log.warning("%s: %s (attempt %d) -> new captcha", self.name, last, attempt)
                continue
            resp = s.post(f"{self.base}/v2/getres", data={
                "exam": exam, "board": board, "year": year,
                "roll": roll, "reg": reg, "result_type": "1", "captcha": code,
            }, timeout=(5, 60))
            if resp.status_code >= 500:
                last, kind = f"server error (HTTP {resp.status_code})", "server"
                log.warning("%s: %s (attempt %d)", self.name, last, attempt)
                continue
            try:
                j = resp.json()
            except ValueError:
                last, kind = f"non-JSON response (HTTP {resp.status_code})", "server"
                log.warning("%s: %s (attempt %d)", self.name, last, attempt)
                continue
            if not isinstance(j, dict):
                last, kind = "unexpected response shape", "server"
                continue
            if j.get("status") == 0 and j.get("res"):
                return j
            msg = str(j.get("msg") or j.get("message") or "")
            if "captcha" in msg.lower() or not msg:
                last, kind = msg or "captcha rejected", "captcha"
                log.info("%s: captcha rejected (attempt %d)", self.name, attempt)
                continue
            # a definite answer ("invalid registration", "result not published", ...):
            # no point burning more captchas or asking another server
            return {"status": 1, "msg": msg, "kind": "definitive"}
        if solver_fails >= CAPTCHA_ATTEMPTS:   # every attempt died in Gemini, not on the site
            raise SolveError(last, fatal=True)
        return {"status": 1, "msg": last, "kind": kind}


SITES = [
    Site("eboardresults.com", "https://eboardresults.com"),
    Site("educationboardresults.gov.bd", "https://educationboardresults.gov.bd"),
]

class SiteBalancer:
    """Spreads requests over the live result servers.
    A server that is busy with another request is skipped while a free one exists; among equally
    busy servers it goes round-robin (request 1 -> server 1, request 2 -> server 2, ...)."""

    def __init__(self, sites: List[Site]):
        self._sites = sites
        self._busy: Dict[str, int] = {x.name: 0 for x in sites}
        self._rr = 0
        self._lock = threading.Lock()

    def choose(self, live: List[Site]) -> List[Site]:
        """Order `live` (best first) and mark the first one busy - atomically, so two requests
        arriving together cannot both grab the same server."""
        with self._lock:
            n = len(self._sites)
            idx = {x.name: i for i, x in enumerate(self._sites)}
            order = sorted(live, key=lambda x: (self._busy.get(x.name, 0),
                                                (idx[x.name] - self._rr) % n))
            if order:
                self._busy[order[0].name] += 1
                self._rr = (idx[order[0].name] + 1) % n
            return order

    def acquire(self, site: Site) -> None:
        with self._lock:
            self._busy[site.name] = self._busy.get(site.name, 0) + 1

    def release(self, site: Site) -> None:
        with self._lock:
            self._busy[site.name] = max(0, self._busy.get(site.name, 0) - 1)

    def busy(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._busy)


BALANCER = SiteBalancer(SITES)

_DEFINITIVE_TYPES = (
    ("result_not_published", ("not published", "not yet published", "yet to be published", "publish")),
    ("invalid_registration", ("registration", "reg no", "reg. no", "invalid reg")),
    ("invalid_roll", ("invalid roll", "roll no", "roll number")),
    ("result_not_found", ("not found", "no result", "not available", "no data", "does not exist")),
)


def classify_site_msg(msg: str) -> str:
    """Machine-readable error_type for a definitive answer from the result site."""
    low = (msg or "").lower()
    for etype, words in _DEFINITIVE_TYPES:
        if any(w in low for w in words):
            return etype
    return "result_error"


def fail(msg: str, etype: str, retryable: bool, **extra: Any) -> Dict[str, Any]:
    return {"ok": False, "status": 1, "msg": msg, "error_type": etype, "retryable": retryable, **extra}


def preflight() -> Dict[str, Optional[requests.Session]]:
    """Step 1 of every request: check ALL result servers and ALL Gemini keys at the same time.
    Returns {site name: ready session or None if the server is down}; the sessions are reused for
    the real lookup so nothing is wasted."""
    fns: List[Callable[[], Any]] = [lambda x=x: x.open_session() for x in SITES]
    fns.append(lambda: POOL.preflight(PREFLIGHT_KEY_TTL))
    res = run_parallel(fns, PREFLIGHT_TIMEOUT)
    return {x.name: res[i] for i, x in enumerate(SITES)}


def check(roll: str, reg: str, exam: str, board: str, year: str,
          deadline: Optional[float] = None) -> Dict[str, Any]:
    """
    1. check every result server + every Gemini key (parallel)
    2. no server live -> error to the caller
    3. pick the least-busy live server (request 1 -> server 1, request 2 -> server 2 ...)
    4. solve the captcha with a live, free Gemini key (another key / new captcha on Gemini errors)
    5. outcome:
         result            -> returned
         definitive error  -> returned as an error right away (invalid registration, result not
                              published, not found ...): NO retry, NO other server
         server unreachable / misbehaving / keeps rejecting captcha -> next live server
         Gemini unusable   -> returned as an error (another server would not help)
    """
    sessions = preflight()
    live = [x for x in SITES if sessions.get(x.name) is not None]
    down = [f"{x.name}: down" for x in SITES if sessions.get(x.name) is None]
    log.info("preflight: live=%s down=%s keys_ready=%d/%d", [x.name for x in live],
             [x.name for x in SITES if sessions.get(x.name) is None], POOL.available(), len(POOL.keys))
    held: Optional[Site] = None
    try:
        if not live:
            return fail("no server is live", "no_server_live", True, errors=down)
        order = BALANCER.choose(live)
        held = order[0]
        errors: List[str] = list(down)
        for n, site in enumerate(order):
            check_deadline(deadline)
            if n:                                   # failing over: move the "busy" mark along
                BALANCER.release(held)
                BALANCER.acquire(site)
                held = site
            log.info("trying %s for roll=%s", site.name, mask(roll))
            try:
                j = site.fetch_result(sessions[site.name], roll, reg, exam, board, year, deadline)
            except JobTimeout:
                raise
            except SolveError as e:
                if e.fatal:
                    log.error("gemini unusable (%s) - not trying other servers", e)
                    return fail(f"captcha solver unavailable: {e}", "captcha_solver_failed", True,
                                errors=errors + [f"{site.name}: {e}"])
                msg = f"captcha solver: {e}"
            except requests.RequestException as e:
                msg = f"server not reachable ({type(e).__name__})"
            except Exception:
                log.exception("%s: unexpected error", site.name)
                msg = "unexpected error"
            else:
                if j.get("status") == 0 and j.get("res"):
                    j["source"] = site.name
                    j["ok"] = True
                    return j
                msg = str(j.get("msg") or "no result")
                if j.get("kind") == "definitive":
                    log.info("%s answered: %s (final, no other server tried)", site.name, msg)
                    return fail(msg, classify_site_msg(msg), False, source=site.name)
            log.info("%s failed (%s), switching to next live server", site.name, msg)
            errors.append(f"{site.name}: {msg}")
        return fail("all live servers failed", "all_servers_failed", True, errors=errors)
    finally:
        if held is not None:
            BALANCER.release(held)
        for sess in sessions.values():
            if sess is not None:
                try:
                    sess.close()
                except Exception:
                    pass


def _probe_site(site: Site) -> Dict[str, Any]:
    t0 = time.monotonic()
    s = None
    try:
        s = site.open_session()
    except Exception as e:  # probing must never raise
        log.info("probe %s: %s", site.name, type(e).__name__)
    finally:
        ms = int((time.monotonic() - t0) * 1000)
        if s is not None:
            s.close()
    return {"site": site.name, "up": s is not None, "ms": ms,
            "detail": "ok" if s is not None else "down or challenge failed"}


def probe_sites() -> List[Dict[str, Any]]:
    """Actively open a session (home page + anti-bot challenge) on every result site."""
    results = run_parallel([lambda s=s: _probe_site(s) for s in SITES], PROBE_TIMEOUT)
    return [r if r is not None else
            {"site": s.name, "up": False, "ms": None, "detail": "probe timed out"}
            for s, r in zip(SITES, results)]


_probe_lock = threading.Lock()
_probe_cache: Dict[str, Any] = {"ts": 0.0, "sites": [], "keys": []}


def get_probe(fresh: bool = False) -> Dict[str, Any]:
    """Cached active probe of result sites + Gemini keys (quick, no generation quota used)."""
    with _probe_lock:
        age = time.time() - _probe_cache["ts"]
        if fresh or not _probe_cache["ts"] or age >= PROBE_TTL:
            with ThreadPoolExecutor(max_workers=2) as ex:
                fs = ex.submit(probe_sites)
                fk = ex.submit(POOL.probe_all, False)
                _probe_cache.update(ts=time.time(), sites=fs.result(), keys=fk.result())
        return dict(_probe_cache)


# --------------------------------------------------------------------------- #
# Traffic feed: what came in, and exactly what went back (JSON or error)
# --------------------------------------------------------------------------- #
TRAFFIC = Notifier(BOT_TOKEN, ALERT_CHAT_IDS, TRAFFIC_MAX_PER_MIN)  # own queue + own flood guard
_MASK_FIELDS = ("roll", "reg")


def _mask_obj(o: Any) -> Any:
    """Recursively mask roll/reg values (only used when ALERT_MASK_PAYLOAD=1)."""
    if isinstance(o, dict):
        return {k: (mask(v) if k in _MASK_FIELDS and v not in (None, "") else _mask_obj(v))
                for k, v in o.items()}
    if isinstance(o, list):
        return [_mask_obj(v) for v in o]
    return o


def _strip_secret_url(o: Any) -> Any:
    """Drop ?query tokens from callback_url so secrets never reach Telegram."""
    if isinstance(o, dict) and isinstance(o.get("callback_url"), str):
        o = dict(o)
        try:
            u = urlparse(o["callback_url"])
            o["callback_url"] = urlunparse((u.scheme, u.netloc, u.path, "", "", ""))
        except ValueError:
            o["callback_url"] = "<unparseable>"
    return o


def pretty_json(obj: Any, limit: int) -> str:
    """Safe, readable JSON for a Telegram <pre> block (masked + clipped + escaped)."""
    if isinstance(obj, (dict, list)):
        obj = _strip_secret_url(obj)
        if ALERT_MASK_PAYLOAD:
            obj = _mask_obj(obj)
        try:
            txt = json.dumps(obj, ensure_ascii=False, indent=1, default=str)
        except (TypeError, ValueError):
            txt = str(obj)
    else:
        txt = str(obj)
    return esc(clip(txt, limit))


def report_http(path: str, method: str, ip: str, raw_body: bytes, status: int, reply: Any) -> None:
    """One Telegram message: the request that arrived + the HTTP reply the client got."""
    if not (ALERT_ON_REQUEST and TRAFFIC.enabled):
        return
    try:
        body: Any = json.loads(raw_body.decode("utf-8")) if raw_body else None
    except (ValueError, UnicodeDecodeError):
        body = (raw_body or b"")[:300].decode("utf-8", "replace") or None
    ok = status < 400
    text = (f"{'📩' if ok else '⚠️'} <b>Request</b>  <code>{esc(method)} {esc(path)}</code>\n"
            f"from: <code>{esc(ip)}</code>\n"
            f"<b>incoming:</b>\n<pre>{pretty_json(body if body is not None else '(empty)', 600)}</pre>\n"
            f"{'↩️' if ok else '❌'} <b>reply sent: HTTP {status}</b>\n"
            f"<pre>{pretty_json(reply, 1500)}</pre>")
    TRAFFIC.send(text)


def report_result(job_id: str, p: Dict[str, Any], res: Dict[str, Any], secs: float) -> None:
    """One Telegram message: the finished job's JSON (success) or the error (failure)."""
    if not (ALERT_ON_RESPONSE and TRAFFIC.enabled):
        return
    ok = bool(res.get("ok"))
    roll = mask(p["roll"]) if ALERT_MASK_PAYLOAD else p["roll"]
    reg = mask(p["reg"]) if ALERT_MASK_PAYLOAD else p["reg"]
    head = (f"{'✅' if ok else '❌'} <b>Result {'ready' if ok else 'FAILED'}</b>  ·  {secs:.1f}s\n"
            f"job: <code>{esc(job_id)}</code>\n"
            f"roll: <code>{esc(roll)}</code>  reg: <code>{esc(reg)}</code>\n"
            f"{esc(p.get('exam'))} · {esc(p.get('board'))} · {esc(p.get('year'))}"
            + (f"  ·  request_id: <code>{esc(clip(p['request_id'], 60))}</code>" if p.get("request_id") else "")
            + "\n")
    if not ok:
        head += (f"<b>error:</b> {esc(clip(res.get('msg') or 'unknown', 300))}\n"
                 f"type: <code>{esc(res.get('error_type') or 'unknown')}</code> · "
                 f"{'retryable' if res.get('retryable', True) else 'final answer from the result site'}\n")
    TRAFFIC.send(head + f"<b>JSON:</b>\n<pre>{pretty_json(res, 2300)}</pre>")


def report_callback(job_id: str, url: str, ok: bool, reason: str = "") -> None:
    """Did the JSON actually reach the caller's callback_url?"""
    if not (ALERT_ON_RESPONSE and TRAFFIC.enabled):
        return
    try:
        u = urlparse(url)
        safe = urlunparse((u.scheme, u.netloc, u.path, "", "", ""))
    except ValueError:
        safe = "<unparseable>"
    TRAFFIC.send(f"{'📬' if ok else '📪'} <b>Callback {'delivered' if ok else 'FAILED'}</b>\n"
                 f"job: <code>{esc(job_id)}</code>\nurl: {esc(clip(safe, 200))}"
                 + (f"\nreason: {esc(reason)}" if reason and not ok else ""))


# --------------------------------------------------------------------------- #
# Job store (in memory, TTL + size bounded)
# --------------------------------------------------------------------------- #
TERMINAL = ("done", "failed")


class JobStore:
    def __init__(self, ttl: int, cap: int):
        self._d: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._rid: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._ttl, self._cap = ttl, cap
        self._last_purge = 0.0

    def _purge(self) -> None:
        now = time.time()
        if now - self._last_purge < 30 and len(self._d) <= self._cap:
            return
        self._last_purge = now
        for jid, j in list(self._d.items()):
            if j["state"] in TERMINAL and (now - j["updated"] > self._ttl or len(self._d) > self._cap):
                self._drop(jid)

    def _drop(self, jid: str) -> None:
        j = self._d.pop(jid, None)
        if j and j.get("_rid") and self._rid.get(j["_rid"]) == jid:
            del self._rid[j["_rid"]]

    def create(self, request_id: Optional[str]) -> Tuple[str, bool]:
        """Returns (job_id, is_duplicate). Duplicate = same request_id still queued/running."""
        with self._lock:
            self._purge()
            if request_id:
                existing = self._rid.get(request_id)
                if existing and self._d.get(existing, {}).get("state") in ("queued", "running"):
                    return existing, True
            jid = str(time.time_ns())
            while jid in self._d:
                jid = str(int(jid) + 1)
            now = time.time()
            self._d[jid] = {"job_id": jid, "state": "queued", "created": now, "updated": now,
                            "request_id": request_id, "callback": "none", "_rid": request_id}
            if request_id:
                self._rid[request_id] = jid
            return jid, False

    def update(self, jid: str, **fields: Any) -> None:
        with self._lock:
            j = self._d.get(jid)
            if j:
                j.update(fields)
                j["updated"] = time.time()

    def discard(self, jid: str) -> None:
        with self._lock:
            self._drop(jid)

    def get(self, jid: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            j = self._d.get(jid)
            return {k: v for k, v in j.items() if not k.startswith("_")} if j else None

    def size(self) -> int:
        with self._lock:
            return len(self._d)

    def counts(self) -> Dict[str, int]:
        with self._lock:
            out: Dict[str, int] = defaultdict(int)
            for j in self._d.values():
                out[j["state"]] += 1
            return dict(out)

    def recent(self, n: int = 8) -> List[Dict[str, Any]]:
        """Newest jobs first, compact (no result bodies)."""
        with self._lock:
            out = []
            for j in reversed(self._d.values()):
                res = j.get("result") or {}
                out.append({"job_id": j["job_id"], "state": j["state"], "callback": j.get("callback"),
                            "created": j["created"], "updated": j["updated"],
                            "msg": res.get("msg"), "source": res.get("source")})
                if len(out) >= n:
                    break
            return out


JOBS = JobStore(JOB_TTL, JOB_STORE_MAX)
JOB_Q: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue(maxsize=QUEUE_MAX)
CB_Q: "queue.Queue[Tuple[str, str, Dict[str, Any]]]" = queue.Queue()


# --------------------------------------------------------------------------- #
# Callback delivery
# --------------------------------------------------------------------------- #
_dl_lock = threading.Lock()
_CB_REASON: Dict[str, str] = {}  # job_id -> why the last callback failed (consumed by callback_loop)


def dead_letter(job_id: str, url: str, body: Dict[str, Any], reason: str) -> None:
    rec = {"ts": time.time(), "job_id": job_id, "url": url, "reason": reason, "body": body}
    try:
        with _dl_lock:
            fd = os.open(DEAD_LETTER_PATH, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        log.error("callback for job %s dead-lettered to %s (%s)", job_id, DEAD_LETTER_PATH, reason)
    except OSError as e:
        log.error("could not write dead letter for job %s: %s", job_id, e)
    try:
        u = urlparse(url)
        safe_url = urlunparse((u.scheme, u.netloc, u.path, "", "", ""))
    except ValueError:
        safe_url = "<unparseable>"
    ALERTS.send(f"📪 <b>Callback undeliverable</b>\njob: <code>{esc(job_id)}</code>\n"
                f"url: {esc(clip(safe_url, 200))}\nreason: {esc(reason)}\n"
                f"saved to <code>{esc(DEAD_LETTER_PATH)}</code>")


def send_callback(job_id: str, url: str, body: Dict[str, Any]) -> bool:
    raw = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
    headers = {"Content-Type": "application/json", "User-Agent": "result-service/1.0"}
    if CALLBACK_SECRET:
        headers["X-Callback-Secret"] = CALLBACK_SECRET
        headers["X-Callback-Signature"] = hmac.new(
            CALLBACK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    reason = "unknown"
    for attempt in range(1, CB_ATTEMPTS + 1):
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=(5, 30),
                              allow_redirects=False)
            sc = r.status_code
            if 200 <= sc < 300:
                log.info("callback OK job=%s -> HTTP %s", job_id, sc)
                return True
            reason = f"HTTP {sc}"
            permanent = 300 <= sc < 500 and sc not in (408, 425, 429)
            log.warning("callback attempt %d/%d job=%s: %s", attempt, CB_ATTEMPTS, job_id, reason)
            if permanent:
                break
        except requests.RequestException as e:
            reason = f"{type(e).__name__}"
            log.warning("callback attempt %d/%d job=%s: %s", attempt, CB_ATTEMPTS, job_id, reason)
        if attempt < CB_ATTEMPTS and STOP.wait(min(2 ** attempt, 60)):
            reason += " (shutdown)"
            break
    dead_letter(job_id, url, body, reason)
    _CB_REASON[job_id] = reason
    return False


def callback_loop() -> None:
    while not STOP.is_set():
        try:
            job_id, url, body = CB_Q.get(timeout=1)
        except queue.Empty:
            continue
        try:
            ok = send_callback(job_id, url, body)
            STATS.inc("callbacks_sent" if ok else "callbacks_failed")
            report_callback(job_id, url, ok, _CB_REASON.pop(job_id, ""))
            JOBS.update(job_id, callback="sent" if ok else "failed")
        except Exception:
            log.exception("callback loop crashed on job %s", job_id)
            STATS.inc("callbacks_failed")
            JOBS.update(job_id, callback="failed")
        finally:
            CB_Q.task_done()


# --------------------------------------------------------------------------- #
# Workers
# --------------------------------------------------------------------------- #
def process_job(job_id: str, p: Dict[str, Any]) -> None:
    JOBS.update(job_id, state="running")
    started = time.monotonic()
    try:
        res = check(p["roll"], p["reg"], p["exam"], p["board"], p["year"],
                    deadline=started + JOB_TIMEOUT)
    except JobTimeout:
        res = fail("job timed out", "timeout", True)
    except Exception:
        log.exception("job %s crashed", job_id)
        res = fail("internal error", "internal_error", True)

    res["job_id"] = job_id
    res["request_id"] = p.get("request_id")
    res["ok"] = bool(res.get("ok"))
    cb = p.get("callback_url")
    elapsed = time.monotonic() - started
    JOBS.update(job_id, state="done" if res["ok"] else "failed", result=res,
                callback="pending" if cb else "none")
    log.info("job %s finished ok=%s in %.1fs", job_id, res["ok"], elapsed)
    if res["ok"]:
        STATS.inc("done")
    else:
        STATS.inc("failed")
    report_result(job_id, p, res, elapsed)
    if not res["ok"] and res.get("retryable", True):   # infrastructure problem -> 🚨 alert
        alert_job_failed(job_id, p, res, elapsed)
    if cb:
        CB_Q.put((job_id, cb, res))


def worker_loop() -> None:
    while not STOP.is_set():
        try:
            job_id, payload = JOB_Q.get(timeout=1)
        except queue.Empty:
            continue
        try:
            process_job(job_id, payload)
        except Exception:  # never let the worker thread die
            log.exception("worker crashed on job %s", job_id)
            res = fail("internal error", "internal_error", True, job_id=job_id)
            JOBS.update(job_id, state="failed", result=res)
            STATS.inc("failed")
            report_result(job_id, payload, res, 0.0)
            alert_job_failed(job_id, payload, res, 0.0)
        finally:
            JOB_Q.task_done()


# --------------------------------------------------------------------------- #
# Health, status text, hourly digest
# --------------------------------------------------------------------------- #
_THREADS: List[threading.Thread] = []      # workers + callback workers (used by /ready)
_AUX_THREADS: List[threading.Thread] = []  # alerts, digest, telegram bot
_started = False
_start_lock = threading.Lock()


def thread_health() -> Dict[str, Any]:
    w = sum(1 for t in _THREADS if t.name.startswith("worker") and t.is_alive())
    c = sum(1 for t in _THREADS if t.name.startswith("callback") and t.is_alive())
    aux = {t.name: t.is_alive() for t in _AUX_THREADS}
    return {"workers": w, "callbacks": c, "aux": aux}


def build_digest(title: str = "📊 <b>Hourly digest</b>") -> str:
    st = STATS.snap()
    jc = JOBS.counts()
    ks = POOL.status()
    ready = sum(1 for k in ks if k["available"])
    th = thread_health()
    lines = [title,
             f"<i>uptime {fmt_dur(time.time() - START_TIME)} · model {esc(MODEL)}</i>", ""]
    lines.append(f"🔑 <b>Gemini keys</b>: {ready}/{len(ks)} available")
    for k in ks:
        if not k["available"]:
            lines.append(f"   • <code>{k['tag']}</code> cooling {int(k['cool_left'])}s"
                         f" — {esc(k['why'] or 'cooldown')}")
    lines.append(f"📥 <b>Queues</b>: JOB_Q {JOB_Q.qsize()}/{QUEUE_MAX} · CB_Q {CB_Q.qsize()}")
    lines.append(f"🧾 <b>Jobs tracked</b>: queued {jc.get('queued', 0)} · running {jc.get('running', 0)}"
                 f" · done {jc.get('done', 0)} · failed {jc.get('failed', 0)}")
    lines.append(f"📈 <b>Since start</b>: submitted {st.get('submitted', 0)} · ok {st.get('done', 0)}"
                 f" · failed {st.get('failed', 0)} · callbacks {st.get('callbacks_sent', 0)} sent"
                 f"/{st.get('callbacks_failed', 0)} failed")
    w_ok = th["workers"] == WORKERS and th["callbacks"] == CB_WORKERS
    aux_txt = " · ".join(f"{n} {'✅' if ok else '❌'}" for n, ok in th["aux"].items())
    lines.append(f"{'🧵' if w_ok else '⚠️'} <b>Threads</b>: workers {th['workers']}/{WORKERS}"
                 f" · callbacks {th['callbacks']}/{CB_WORKERS}" + (f" · {aux_txt}" if aux_txt else ""))
    return "\n".join(lines)


def health_snapshot(probe: bool = True, fresh: bool = False, detail: bool = True) -> Dict[str, Any]:
    """detail=False (public view): counts and site up/down only - no key tags or key details."""
    alive = sum(1 for t in _THREADS if t.is_alive())
    data: Dict[str, Any] = {
        "status": "ok", "model": MODEL, "uptime_s": int(time.time() - START_TIME),
        "queue_depth": JOB_Q.qsize(), "callback_depth": CB_Q.qsize(),
        "keys": len(POOL.keys), "keys_available": POOL.available(),
        "threads_alive": alive, "jobs_tracked": JOBS.size()}
    degraded = alive < WORKERS + CB_WORKERS or POOL.available() == 0
    down = False
    if probe:
        pr = get_probe(fresh)
        cool = {k["id"]: k for k in POOL.status()}
        keys = []
        for k in pr["keys"]:
            c = cool.get(k["id"], {})
            keys.append({"key": k["tag"], "state": k["state"], "http": k.get("http"), "ms": k.get("ms"),
                         "detail": k.get("detail", ""), "in_cooldown": not c.get("available", True),
                         "cooldown_s": int(c.get("cool_left", 0))})
        sites = pr["sites"]
        n_up = sum(1 for x in sites if x["up"])
        states = [k["state"] for k in pr["keys"]]
        n_valid = states.count("valid")
        n_rate = states.count("rate_limited")
        n_invalid = states.count("invalid")
        data["sites"] = sites
        data["sites_total"], data["sites_up"], data["sites_down"] = len(sites), n_up, len(sites) - n_up
        data["keys_total"] = len(states)
        data["keys_alive"] = n_valid                    # work right now
        data["keys_rate_limited"] = n_rate              # good key, Google is throttling it for a moment
        data["keys_invalid"] = n_invalid                # rejected by Google
        data["keys_unreachable"] = len(states) - n_valid - n_rate - n_invalid   # network/server/model errors
        if detail:
            data["gemini_keys"] = keys
        data["probed_age_s"] = int(time.time() - pr["ts"])
        if n_up < len(sites) or n_valid + n_rate < len(states):
            degraded = True
        if n_up == 0 or n_valid + n_rate == 0:
            down = True
    if down:
        data["status"] = "down"
    elif degraded:
        data["status"] = "degraded"
    return data


def render_health_html(h: Dict[str, Any], detail: bool) -> str:
    """Small self-contained status page for browsers (light/dark aware, auto-refresh)."""
    status = h["status"]
    color = {"ok": "#1a7f37", "degraded": "#b26a00", "down": "#c62828"}.get(status, "#555")
    rows = ""
    for x in h.get("sites", []):
        ms = f"{x['ms']} ms" if x.get("ms") is not None else "-"
        rows += (f"<tr><td>{'🟢' if x['up'] else '🔴'} {esc(x['site'])}</td>"
                 f"<td>{'UP' if x['up'] else 'DOWN'}</td><td>{ms}</td><td>{esc(x.get('detail', ''))}</td></tr>")
    krows = ""
    for k in h.get("gemini_keys", []):
        good = key_usable(k["state"])
        cool = f"cooling {k['cooldown_s']}s" if k["in_cooldown"] else ""
        krows += (f"<tr><td>{'🟢' if good else '🔴'} <code>{esc(k['key'])}</code></td><td>{esc(k['state'])}</td>"
                  f"<td>{k['ms'] if k.get('ms') is not None else '-'} ms</td><td>{esc(cool or k.get('detail', ''))}</td></tr>")
    up_h = f"{h['uptime_s'] // 3600}h {h['uptime_s'] % 3600 // 60}m"
    keys_block = ""
    if "keys_total" in h:
        keys_block = (
            f"<h2>Gemini keys: {h['keys_alive']} alive / {h['keys_total']} total</h2>"
            f"<p class=m>working now <b>{h['keys_alive']}</b> · rate-limited (good, throttled) <b>{h['keys_rate_limited']}</b>"
            f" · rejected <b>{h['keys_invalid']}</b> · unreachable <b>{h['keys_unreachable']}</b>"
            f" · free to use right now <b>{h['keys_available']}</b></p>"
            + (f"<table><tr><th>key</th><th>state</th><th>speed</th><th>note</th></tr>{krows}</table>"
               if krows else "") 
            + ("" if detail else "<p class=m>Key details are hidden. Add <code>?token=YOUR_API_TOKEN</code> to see them.</p>"))
    sites_block = ""
    if "sites_total" in h:
        sites_block = (f"<h2>Result servers: {h['sites_up']} up / {h['sites_total']} total</h2>"
                       f"<table><tr><th>server</th><th>state</th><th>speed</th><th>note</th></tr>{rows}</table>")
    age = f" · checked {h['probed_age_s']}s ago" if "probed_age_s" in h else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>Result service - {esc(status)}</title>
<style>
:root{{--bg:#f6f7f9;--card:#fff;--tx:#1b1f24;--mu:#5b6570;--bd:#dde1e6}}
@media (prefers-color-scheme:dark){{:root{{--bg:#12151a;--card:#1b2027;--tx:#e8ebef;--mu:#9aa5b1;--bd:#2c333c}}}}
body{{margin:0;background:var(--bg);color:var(--tx);font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}}
main{{max-width:760px;margin:0 auto;padding:20px 14px}}
.card{{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:16px 18px;margin-bottom:14px}}
.badge{{display:inline-block;padding:3px 12px;border-radius:99px;color:#fff;font-weight:700;background:{color}}}
h1{{font-size:20px;margin:0 0 6px}} h2{{font-size:16px;margin:0 0 8px}}
.m{{color:var(--mu);margin:4px 0 10px;font-size:13px}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
th,td{{text-align:left;padding:6px 8px;border-top:1px solid var(--bd)}} th{{color:var(--mu);font-weight:600;border-top:0}}
.wrap{{overflow-x:auto}} code{{font-size:13px}}
</style></head><body><main>
<div class="card"><h1>Result service <span class="badge">{esc(status.upper())}</span></h1>
<p class="m">model {esc(h.get('model', ''))} · uptime {up_h}{age} · auto-refresh 30s</p>
<p class="m">queue {h['queue_depth']}/{QUEUE_MAX} · callbacks waiting {h['callback_depth']}
 · threads {h['threads_alive']}/{WORKERS + CB_WORKERS} · jobs tracked {h['jobs_tracked']}</p></div>
<div class="card wrap">{sites_block}</div>
<div class="card wrap">{keys_block}</div>
</main></body></html>"""


def build_health_text(fresh: bool = True) -> str:
    h = health_snapshot(True, fresh)
    icon = "🟢" if h["status"] == "ok" else "🟠"
    lines = [f"{icon} <b>Health: {h['status'].upper()}</b>", "", "🌐 <b>Result sites</b>"]
    for s in h.get("sites", []):
        ms = f" · {s['ms']}ms" if s.get("ms") is not None else ""
        lines.append(f"  {'✅' if s['up'] else '❌'} {esc(s['site'])}{ms}"
                     + ("" if s["up"] else f" — {esc(s['detail'])}"))
    lines += ["", "🔑 <b>Gemini keys</b>"]
    if not h.get("gemini_keys"):
        lines.append("  (none configured — /addkey)")
    for k in h.get("gemini_keys", []):
        ok = key_usable(k["state"])
        extra = f" · cooling {k['cooldown_s']}s" if k["in_cooldown"] else ""
        lines.append(f"  {'✅' if ok else '❌'} <code>{k['key']}</code> {esc(k['state'])}"
                     f"{' · ' + str(k['ms']) + 'ms' if k.get('ms') is not None else ''}{extra}")
    lines += ["", f"📥 JOB_Q {h['queue_depth']}/{QUEUE_MAX} · CB_Q {h['callback_depth']}"
                  f" · threads {h['threads_alive']}/{WORKERS + CB_WORKERS} · jobs {h['jobs_tracked']}"]
    return "\n".join(lines)


def digest_loop() -> None:
    while not STOP.wait(DIGEST_INTERVAL):
        try:
            ALERTS.send(build_digest(), force=True)
        except Exception:
            log.exception("digest failed")


def _start_thread(target: Callable[..., Any], name: str, aux: bool = False,
                  args: Tuple[Any, ...] = ()) -> None:
    t = threading.Thread(target=target, args=args, name=name, daemon=True)
    t.start()
    (_AUX_THREADS if aux else _THREADS).append(t)


def start_background() -> None:
    global _started
    if _started:
        return
    with _start_lock:
        if _started:
            return
        for i in range(WORKERS):
            _start_thread(worker_loop, f"worker-{i}")
        for i in range(CB_WORKERS):
            _start_thread(callback_loop, f"callback-{i}")
        if ALERTS.enabled:
            _start_thread(ALERTS.loop, "alerts", aux=True)
            _start_thread(TRAFFIC.loop, "traffic", aux=True)
            if DIGEST_ENABLED:
                _start_thread(digest_loop, "digest", aux=True)
        elif BOT_TOKEN:
            log.warning("BOT_TOKEN set but no ADMIN_IDS/ALERT_CHAT_ID: alerts and digests are off")
        if BOT_TOKEN:
            if not ADMIN_IDS:
                log.warning("ADMIN_IDS is empty: the bot will refuse everyone (use /id to find your id)")
            try:
                import bot as _bot  # imported lazily so the service runs without aiogram
            except ImportError as e:
                log.error("Telegram bot disabled (%s). Run: pip install -r requirements.txt", e)
            else:
                _start_thread(_bot.run_bot, "telegram-bot", aux=True, args=(sys.modules[__name__],))
        _started = True
        if not API_TOKEN:
            log.warning("API_TOKEN is not set: /result is UNAUTHENTICATED and spends your Gemini quota")
        log.info("started: %d workers, %d callback workers, %d Gemini keys, model=%s, telegram=%s",
                 WORKERS, CB_WORKERS, len(POOL.keys), MODEL, "on" if BOT_TOKEN else "off")
        ALERTS.send(f"🟢 <b>result-service started</b>\n{len(POOL.keys)} Gemini key(s) · "
                    f"{WORKERS} workers · model {esc(MODEL)}", force=True)


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
NUM_RE = re.compile(r"\d{1,15}")
WORD_RE = re.compile(r"[a-z][a-z_]{1,24}")
YEAR_RE = re.compile(r"(19|20)\d{2}")


def validate_callback(url: str) -> Optional[str]:
    if len(url) > 2048:
        return "callback_url too long"
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        return "callback_url must be an absolute http(s) URL"
    if u.username or u.password:
        return "credentials in callback_url are not allowed"
    host = u.hostname.lower()
    if ALLOWED_CB_HOSTS:
        return None if host in ALLOWED_CB_HOSTS else "callback_url host is not in CALLBACK_ALLOWED_HOSTS"
    if ALLOW_PRIVATE_CB:
        return None
    try:
        port = u.port or (443 if u.scheme == "https" else 80)
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, ValueError):
        return "callback_url host does not resolve"
    for info in infos:
        if not ipaddress.ip_address(info[4][0].split("%")[0]).is_global:
            return ("callback_url resolves to a non-public address "
                    "(set ALLOW_PRIVATE_CALLBACKS=1 or CALLBACK_ALLOWED_HOSTS to permit)")
    return None


def validate(p: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    def txt(name: str, default: Optional[str] = None) -> Optional[str]:
        v = p.get(name, default)
        if v is None or isinstance(v, (dict, list, bool)):
            return default if v is None else ""
        return str(v).strip()

    roll, reg = txt("roll"), txt("reg")
    if not roll or not reg:
        return None, "roll and reg are both required"
    if not NUM_RE.fullmatch(roll) or not NUM_RE.fullmatch(reg):
        return None, "roll and reg must be numeric (max 15 digits)"
    exam = (txt("exam", "hsc") or "").lower()
    board = (txt("board", "dhaka") or "").lower()
    year = txt("year", "2024") or ""
    if not WORD_RE.fullmatch(exam):
        return None, "invalid exam"
    if not WORD_RE.fullmatch(board):
        return None, "invalid board"
    if not YEAR_RE.fullmatch(year):
        return None, "invalid year"

    rid = p.get("request_id")
    if rid is not None and (isinstance(rid, (dict, list, bool)) or len(str(rid)) > 128):
        return None, "invalid request_id"

    cb = p.get("callback_url")
    if cb:
        if not isinstance(cb, str):
            return None, "callback_url must be a string"
        err = validate_callback(cb.strip())
        if err:
            return None, err
        cb = cb.strip()
    return {"roll": roll, "reg": reg, "exam": exam, "board": board, "year": year,
            "request_id": rid, "callback_url": cb or None}, None


# --------------------------------------------------------------------------- #
# Flask app
# --------------------------------------------------------------------------- #
APP = Flask(__name__)
APP.config["MAX_CONTENT_LENGTH"] = 16 * 1024


@APP.before_request
def _boot() -> None:
    start_background()


@APP.after_request
def _traffic_hook(resp):
    """Mirror each /result request and the exact reply to Telegram (never breaks the response)."""
    try:
        if ALERT_ON_REQUEST and TRAFFIC.enabled and (request.path == "/result" or resp.status_code >= 500):
            try:
                raw = request.get_data(cache=True)[:4096]
            except Exception:  # e.g. 413 body too large
                raw = b""
            reply = resp.get_json(silent=True) if resp.is_json else resp.get_data(as_text=True)
            report_http(request.path, request.method, request.remote_addr or "?", raw,
                        resp.status_code, reply)
    except Exception:
        log.exception("traffic report failed")
    return resp


def authorized() -> bool:
    if not API_TOKEN:
        return True
    supplied = request.headers.get("X-API-Key", "")
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        supplied = auth[7:].strip()
    return hmac.compare_digest(supplied.encode(), API_TOKEN.encode())


@APP.post("/result")
def submit():
    if not authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    p = request.get_json(force=True, silent=True)
    if not isinstance(p, dict):
        return jsonify({"ok": False, "error": "body must be a JSON object"}), 400
    clean, err = validate(p)
    if err or clean is None:
        return jsonify({"ok": False, "error": err}), 400

    rid = clean["request_id"]
    job_id, dup = JOBS.create(str(rid) if rid is not None else None)
    if dup:
        return jsonify({"ok": True, "queued": True, "duplicate": True, "job_id": job_id}), 202
    try:
        JOB_Q.put_nowait((job_id, clean))
    except queue.Full:
        JOBS.discard(job_id)
        resp = jsonify({"ok": False, "error": "queue is full, retry later"})
        resp.headers["Retry-After"] = "30"
        return resp, 503
    STATS.inc("submitted")
    log.info("queued job=%s roll=%s request_id=%s", job_id, mask(clean["roll"]), rid)
    return jsonify({"ok": True, "queued": True, "job_id": job_id}), 202


@APP.get("/job/<job_id>")
def job_status(job_id: str):
    if not authorized():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    j = JOBS.get(job_id)
    if not j:
        return jsonify({"ok": False, "error": "unknown or expired job"}), 404
    return jsonify({"ok": True, **j})


def wants_html() -> bool:
    """Is this a browser opening the page (a document), or a script/fetch/XHR wanting JSON?"""
    fmt = request.args.get("format", "").lower()
    if fmt in ("html", "json"):
        return fmt == "html"
    dest = request.headers.get("Sec-Fetch-Dest", "")      # sent by all modern browsers
    if dest:
        return dest in ("document", "iframe", "frame")     # "empty" = fetch()/XHR -> JSON
    return "text/html" in request.headers.get("Accept", "")   # older browsers; curl sends */*


def _health_response(force_html: bool = False):
    supplied = request.args.get("token", "")
    detail = authorized() or bool(API_TOKEN and supplied
                                  and hmac.compare_digest(supplied.encode(), API_TOKEN.encode()))
    do_probe = request.args.get("probe", "1") != "0"
    fresh = request.args.get("fresh") == "1" and detail
    data = health_snapshot(do_probe, fresh, detail)
    if force_html or wants_html():
        resp = APP.response_class(render_health_html(data, detail), mimetype="text/html")
    else:
        resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Vary"] = "Accept, Sec-Fetch-Dest"        # proxies must not mix the two formats
    return resp


@APP.get("/health")
def health():
    """
    Browser (address bar) -> status page; script / fetch / curl -> JSON. Force one with ?format=html|json.
      public            server up/down + key COUNTS
      with API token    also each key's tag/state/speed (header X-API-Key / Authorization, or ?token=)
    ?probe=0 counters only, ?fresh=1 force a new probe (token required).
    Always HTTP 200 - read "status" (ok / degraded / down); /ready is the 503 gate.
    """
    return _health_response()


@APP.get("/status")
@APP.get("/")
def status_page():
    """Always the HTML status page (a normal document), whatever the client sends."""
    return _health_response(force_html=True)


@APP.get("/ready")
def ready():
    alive = sum(1 for t in _THREADS if t.is_alive())
    ok = _started and alive == WORKERS + CB_WORKERS and POOL.available() > 0
    return jsonify({"ready": ok}), (200 if ok else 503)


@APP.errorhandler(404)
def _404(_e):
    return jsonify({"ok": False, "error": "not found"}), 404


@APP.errorhandler(405)
def _405(_e):
    return jsonify({"ok": False, "error": "method not allowed"}), 405


@APP.errorhandler(413)
def _413(_e):
    return jsonify({"ok": False, "error": "payload too large"}), 413


@APP.errorhandler(Exception)
def _500(e):
    log.exception("unhandled error: %s", e)
    return jsonify({"ok": False, "error": "internal server error"}), 500


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def shutdown(grace: int) -> None:
    """Let in-flight jobs and callbacks finish (up to `grace` seconds), then stop threads."""
    log.info("shutting down, draining queues (max %ss) ...", grace)
    end = time.monotonic() + grace
    while time.monotonic() < end and (JOB_Q.unfinished_tasks or CB_Q.unfinished_tasks):
        time.sleep(0.5)
    left = JOB_Q.unfinished_tasks + CB_Q.unfinished_tasks
    if left:
        log.warning("%d task(s) abandoned at shutdown", left)
    STOP.set()


def main() -> None:
    start_background()

    def _term(signum, _frame):
        log.info("signal %s received", signum)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _term)
    try:
        try:
            from waitress import serve
        except ImportError:
            log.warning("waitress not installed (pip install waitress); using Flask's built-in server")
            APP.run(host=API_HOST, port=API_PORT, threaded=True)
        else:
            serve(APP, host=API_HOST, port=API_PORT, threads=8,
                  max_request_body_size=16 * 1024, ident="result-service")
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        shutdown(SHUTDOWN_GRACE)


if __name__ == "__main__":
    main()
