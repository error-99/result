# Result Service

A small API that looks up Bangladesh board exam results for you.

You send a **roll** and **reg** number. The service replies right away with a `job_id`, finds the result in the background (solving the captcha with Gemini), and **posts the JSON result or the error to your `callback_url`**. A Telegram bot shows every request and every response live, and lets you manage the Gemini keys.

```
Your app / PHP  ──POST /result──►  Result Service  ──►  result websites  (captcha solved by Gemini)
       ▲                                  │
       └────── callback (JSON / error) ───┘──►  Telegram bot (live feed + control)
```

---

## Contents

1. [Features](#1-features)
2. [Files](#2-files)
3. [Quick start](#3-quick-start)
4. [Configuration (.env)](#4-configuration-env)
5. [API reference](#5-api-reference)
6. [Workflow: what happens to a request](#6-workflow-what-happens-to-a-request)
7. [Callback: what your server receives](#7-callback-what-your-server-receives)
8. [Error types](#8-error-types)
9. [Telegram bot](#9-telegram-bot)
10. [Deploying](#10-deploying)
11. [Troubleshooting](#11-troubleshooting)
12. [Security notes](#12-security-notes)

---

## 1. Features

- **Async API**: `POST /result` answers instantly (`202`); the result arrives later by callback.
- **Checks everything first**: for every request it checks all result sites and all Gemini keys before starting.
- **Load balancing**: request 1 goes to site 1, request 2 to site 2. A site or Gemini key that is busy with another request is skipped.
- **Automatic failover**: if a site is not reachable, the next live site is used.
- **Gemini retry**: if a Gemini key fails (rate limit, rejected, server error), the same captcha is retried with another key. A wrongly read captcha is retried with a new captcha.
- **Final answers are not retried**: "invalid registration", "result not published", "not found" are sent straight to the callback as errors.
- **Reliable callbacks**: signed (HMAC), retried with backoff, and saved to a file if they can never be delivered.
- **Telegram bot**: live feed of every request, reply, result, and callback; manage Gemini keys; health checks; hourly digest.

---

## 2. Files

| File | What it is |
|---|---|
| `main.py` | The service: API, queue, workers, site logic, Gemini key pool, callbacks, alerts |
| `bot.py` | Telegram control bot. Started automatically by `main.py`. Do not run it by itself |
| `.env` | Your settings and secrets (keep it private) |
| `requirements.txt` | Python packages |
| `gemini_keys.json` | Created automatically. Keys added/removed from Telegram are saved here |
| `failed_callbacks.jsonl` | Created automatically. Callbacks that could not be delivered |

---

## 3. Quick start

**Requirements:** Python 3.9 or newer.

```bash
cd result_service
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**Fill in `.env`** (at minimum):

```ini
GEMINI_KEYS=your_key_1,your_key_2        # free keys: https://aistudio.google.com/apikey
API_TOKEN=a-long-random-string           # your app must send this
CALLBACK_SECRET=another-long-random-string
BOT_TOKEN=123456:ABC...                  # from @BotFather
ADMIN_IDS=111111111                      # your Telegram id (send /id to the bot to find it)
```

**Run:**

```bash
python main.py
```

You should see `started: 2 workers, 2 callback workers, N Gemini keys...` in the log, and the bot sends **🟢 result-service started** to the admins.

**Test it:**

```bash
curl -X POST http://localhost:5000/result \
  -H "Authorization: Bearer a-long-random-string" \
  -H "Content-Type: application/json" \
  -d '{"roll":"123456","reg":"1234567890","exam":"hsc","board":"dhaka","year":"2024"}'
# -> {"ok":true,"queued":true,"job_id":"1793066033755550017"}
```

Watch your Telegram bot: you will see the request, then the result.

> **Run exactly ONE copy.** The queue is in memory, and Telegram allows only one bot polling client per token.

---

## 4. Configuration (.env)

### Required / important

| Variable | Default | Meaning |
|---|---|---|
| `GEMINI_KEYS` | (empty) | Comma-separated Gemini API keys. Can be empty if you add keys from Telegram with `/addkey` |
| `GEMINI_MODEL` | `gemini-flash-lite-latest` | Model used to read the captcha |
| `API_TOKEN` | (empty) | Clients must send it. **If empty, the API is open to everyone** and spends your Gemini quota |
| `CALLBACK_SECRET` | (empty) | Used to sign callbacks (see [section 7](#7-callback-what-your-server-receives)) |
| `BOT_TOKEN` | (empty) | Telegram bot token. Empty = no Telegram at all, service still works |
| `ADMIN_IDS` | (empty) | Comma-separated Telegram user ids allowed to use the bot and receive alerts |

### Server

| Variable | Default | Meaning |
|---|---|---|
| `API_HOST` | `0.0.0.0` | Listen address |
| `API_PORT` | `5000` | Port (`PORT` is also read, for Railway-style hosts) |
| `WORKERS` | `2` | Jobs processed at the same time |
| `CALLBACK_WORKERS` | `2` | Callbacks delivered at the same time |
| `QUEUE_MAX` | `200` | Queue size. When full, the API replies `503` |
| `JOB_TIMEOUT` | `240` | Max seconds per job, then `timeout` error |
| `JOB_TTL` | `3600` | How long finished jobs stay available at `GET /job/<id>` |
| `JOB_STORE_MAX` | `5000` | Max jobs kept in memory |
| `CAPTCHA_ATTEMPTS` | `3` | Captcha tries per site |
| `CALLBACK_ATTEMPTS` | `5` | Delivery tries per callback |
| `DEAD_LETTER_PATH` | `failed_callbacks.jsonl` | Where undeliverable callbacks are saved |
| `KEYS_FILE` | `gemini_keys.json` | Where keys added from Telegram are saved |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `SHUTDOWN_GRACE` | `30` | Seconds to finish running jobs on shutdown |

### Callback safety

| Variable | Default | Meaning |
|---|---|---|
| `CALLBACK_ALLOWED_HOSTS` | (empty) | Optional allow-list, e.g. `example.com,panel.example.com` |
| `ALLOW_PRIVATE_CALLBACKS` | `0` | Set `1` only if your callback server is on localhost or a private IP |

### Request flow

| Variable | Default | Meaning |
|---|---|---|
| `PREFLIGHT_TIMEOUT` | `10` | Max seconds to wait for the per-request check of all sites and keys |
| `PREFLIGHT_KEY_TTL` | `10` | Re-check all Gemini keys at most every N seconds. `0` = check on every request |

### Telegram

| Variable | Default | Meaning |
|---|---|---|
| `ALERT_CHAT_ID` | (all admins) | Send alerts to this chat/group instead |
| `ALERT_ON_REQUEST` | `1` | Send every incoming request and the reply we gave |
| `ALERT_ON_RESPONSE` | `1` | Send every finished job's JSON/error and callback status |
| `ALERT_ON_JOB_FAIL` | `1` | 🚨 alert on failures caused by servers or Gemini |
| `ALERT_MASK_PAYLOAD` | `0` | `1` = mask roll/reg in all Telegram messages |
| `ALERT_MAX_PER_MIN` | `20` | Flood guard for alerts |
| `TRAFFIC_MAX_PER_MIN` | `60` | Flood guard for the request/response feed |
| `ALERT_KEYS_COOLDOWN` | `600` | Min seconds between "all keys down" alerts |
| `DIGEST_ENABLED` | `1` | Send a periodic status digest |
| `DIGEST_INTERVAL` | `3600` | Seconds between digests |
| `HEALTH_CACHE_TTL` | `60` | How long `/health` probe results are cached |
| `PROBE_TIMEOUT` | `25` | Timeout for health probes |

---

## 5. API reference

Authentication: send your `API_TOKEN` as `Authorization: Bearer <token>` **or** `X-API-Key: <token>`.

### `POST /result`: submit a lookup

Body (JSON, max 16 KB):

| Field | Required | Notes |
|---|---|---|
| `roll` | yes | Digits only, up to 15 |
| `reg` | yes | Digits only, up to 15 |
| `exam` | no | Default `hsc` |
| `board` | no | Default `dhaka` |
| `year` | no | Default `2024` (4 digits, 19xx or 20xx) |
| `request_id` | no | Your own id, up to 128 chars. Echoed back in the callback. Sending the same `request_id` while it is still queued or running returns the same job (`"duplicate": true`) |
| `callback_url` | no | Absolute `http(s)` URL that receives the result. Without it, poll `GET /job/<id>` |

Replies:

| HTTP | Body | Meaning |
|---|---|---|
| `202` | `{"ok":true,"queued":true,"job_id":"..."}` | Accepted |
| `202` | `{"ok":true,"queued":true,"duplicate":true,"job_id":"..."}` | Same `request_id` already in progress |
| `400` | `{"ok":false,"error":"..."}` | Invalid input (for example `roll and reg must be numeric`) |
| `401` | `{"ok":false,"error":"unauthorized"}` | Missing or wrong token |
| `413` | `{"ok":false,"error":"payload too large"}` | Body over 16 KB |
| `503` | `{"ok":false,"error":"queue is full, retry later"}` | Queue full. Header `Retry-After: 30` |

### `GET /job/<job_id>`: check a job

```json
{
  "ok": true,
  "job_id": "1793066033755550017",
  "state": "done",              // queued | running | done | failed
  "callback": "sent",           // none | pending | sent | failed
  "request_id": "order-42",
  "result": { "...": "same JSON as the callback body" }
}
```

`404` if the job is unknown or older than `JOB_TTL`.

### `GET /health`: is everything alive?

Open it in a **browser** and you get a status page; scripts (`curl`) get JSON. It checks every result server and every Gemini key (results are cached `HEALTH_CACHE_TTL` seconds, so refreshing is cheap) and shows:

- **Result servers: X up / Y total**, with each server's state and response time
- **Gemini keys: X alive / Y total**, split into *working*, *rate-limited* (good key, Google is throttling it for a moment), *rejected*, and *unreachable*
- Overall `status`: `ok`, `degraded` (something is down, but jobs still work), or `down` (no server or no key usable)
- Queue sizes, worker threads, uptime, model

Example JSON (shortened):

```json
{
  "status": "degraded",
  "sites_up": 1, "sites_total": 2,
  "keys_alive": 3, "keys_rate_limited": 1, "keys_invalid": 0, "keys_unreachable": 0, "keys_total": 4,
  "sites": [{"site": "...", "up": true, "ms": 310}, {"site": "...", "up": false, "detail": "down or challenge failed"}],
  "queue_depth": 0, "callback_depth": 0
}
```

| Who | What they see |
|---|---|
| Anyone (no token) | Server up/down and **counts** of keys. Never any key text |
| With your `API_TOKEN` | Also a list of each key's last characters, state, speed, and cooldown. Send the token as header `X-API-Key`, or in a browser use `/health?token=YOUR_API_TOKEN` |

Options: `?format=json` or `?format=html` to force a format, `?probe=0` for counters only, `?fresh=1` to force a new check (token required). Always returns HTTP 200: read `"status"`. The page refreshes itself every 30 seconds.

> `?token=` puts the token in the URL (browser history, proxy logs). Use it only on your own devices.

### `GET /ready`

`200 {"ready":true}` when workers are running and at least one Gemini key is available, otherwise `503`. Use it as a load-balancer or uptime check.

---

## 6. Workflow: what happens to a request

```mermaid
flowchart TD
    A[Your app: POST /result] --> B{Token OK?}
    B -- no --> B1[401]
    B -- yes --> C{Input valid?}
    C -- no --> C1[400 + error message]
    C -- yes --> D{Queue full?}
    D -- yes --> D1[503 retry later]
    D -- no --> E[202 + job_id returned immediately<br/>job goes into the queue]

    E --> F[Worker picks the job]
    F --> G[STEP 1 - Check everything in parallel<br/>open a session on ALL result sites<br/>check ALL Gemini keys]
    G --> H{Any site live?}
    H -- no --> H1[/Callback ERROR<br/>no_server_live/]
    H -- yes --> I[STEP 2 - Pick the least busy live site<br/>req 1 = site 1, req 2 = site 2<br/>skip a site that is in use]

    I --> J[STEP 3 - Get captcha image from the site]
    J --> K[STEP 4 - Gemini reads the captcha<br/>pick a live, free key]
    K --> L{Gemini OK?}
    L -- "error: 429 / rejected / 5xx" --> L1[retry with ANOTHER key]
    L1 --> L
    L -- "all keys unusable" --> L2[/Callback ERROR<br/>captcha_solver_failed/]
    L -- yes --> M[STEP 5 - Submit roll + reg + captcha to the site]

    M --> N{What did the site answer?}
    N -- "captcha wrong" --> N1[new captcha, try again<br/>up to CAPTCHA_ATTEMPTS]
    N1 --> J
    N -- "RESULT FOUND" --> O[/Callback SUCCESS<br/>result JSON/]
    N -- "FINAL ANSWER<br/>invalid registration<br/>result not published<br/>not found" --> P[/Callback ERROR<br/>sent immediately<br/>NO retry, NO other site/]
    N -- "server problem<br/>unreachable, 5xx, not JSON,<br/>captcha keeps failing" --> Q{Another live site?}
    Q -- yes --> R[Switch to the next site] --> J
    Q -- no --> S[/Callback ERROR<br/>all_servers_failed/]
```

### Step by step

| # | Step | Details |
|---|---|---|
| 1 | **Accept** | The API checks the token and the fields, creates a `job_id`, queues the job, and replies `202`. Your app does not wait. |
| 2 | **Check everything** | A worker opens a session (home page and anti-bot challenge) on **every** result site and quick-checks **every** Gemini key, all at once. Dead keys go on cooldown and are skipped. These sessions are reused for the real lookup. |
| 3 | **No site live?** | The callback gets `no_server_live` right away. |
| 4 | **Choose a site** | The least busy live site is chosen. With everything idle it alternates: request 1 uses site 1, request 2 uses site 2. A site that is currently serving another request is skipped while a free one exists. |
| 5 | **Solve the captcha** | The captcha image goes to Gemini. A live key that nobody is using is chosen first. If Gemini fails (rate limit, rejected key, server error, network), the same image is retried with a different key. |
| 6 | **Submit** | roll, reg, exam, board, year, and the captcha code are sent to the site. |
| 7 | **Handle the answer** | See the table below. |
| 8 | **Deliver** | The JSON (success or error) is stored in the job, sent to `callback_url`, and shown in Telegram. |

### How each outcome is handled

| What happened | Action |
|---|---|
| Result found | Send result JSON to the callback. |
| Site says: invalid registration / result not published / not found | **Final.** Send the error to the callback immediately. No retry, no second site. |
| Site rejected the captcha | Get a new captcha and retry (up to `CAPTCHA_ATTEMPTS`). If it keeps failing, move to the next site. |
| Site not reachable, 5xx, or non-JSON reply | Move to the next live site. |
| Gemini key fails | Retry with another key. |
| Every Gemini key unusable | Send `captcha_solver_failed`. The other site is **not** tried, since it would fail the same way. |
| All sites failed | Send `all_servers_failed` with the reasons in `errors`. |
| Job takes longer than `JOB_TIMEOUT` | Send `timeout`. |

### Callback delivery

The worker puts the JSON in a callback queue. A callback worker POSTs it to your `callback_url`:

- **2xx** = delivered.
- **5xx, timeout, network error** = retried with growing delays, up to `CALLBACK_ATTEMPTS`.
- **Other 3xx/4xx** (except 408, 425, 429) = treated as permanent, not retried.
- After the last failure the callback is saved to `failed_callbacks.jsonl` and you get a Telegram alert **📪 Callback FAILED**.

---

## 7. Callback: what your server receives

`POST <callback_url>` with `Content-Type: application/json`.

### Success

```json
{
  "ok": true,
  "status": 0,
  "res": { "...": "the result data from the board website" },
  "source": "eboardresults.com",
  "job_id": "1793066033755550017",
  "request_id": "order-42"
}
```

### Error

```json
{
  "ok": false,
  "status": 1,
  "msg": "Invalid Registration Number",
  "error_type": "invalid_registration",
  "retryable": false,
  "source": "eboardresults.com",
  "job_id": "1793066033755550017",
  "request_id": "order-42"
}
```

- `msg` is the human-readable message (for final answers, the site's own text).
- `error_type` is stable and safe to branch on in code (see [section 8](#8-error-types)).
- `retryable: true` means it is a temporary problem and sending the same request again later may work. `false` means the answer is final.
- Infrastructure errors (`all_servers_failed`, `no_server_live`) also include an `errors` array with one reason per site.

### Verifying the callback (important)

If `CALLBACK_SECRET` is set, each callback carries:

- `X-Callback-Secret`: the secret itself
- `X-Callback-Signature`: HMAC-SHA256 (hex) of the **raw request body**, keyed with the secret

**PHP**

```php
<?php
$secret = 'your CALLBACK_SECRET';
$raw    = file_get_contents('php://input');
$sig    = $_SERVER['HTTP_X_CALLBACK_SIGNATURE'] ?? '';

if (!hash_equals(hash_hmac('sha256', $raw, $secret), $sig)) {
    http_response_code(401);
    exit;
}

$data = json_decode($raw, true);

if ($data['ok']) {
    // save $data['res'] for $data['request_id']
} else {
    // $data['error_type'], $data['msg'], $data['retryable']
}

http_response_code(200);   // any 2xx tells the service it was delivered
echo 'ok';
```

**Python**

```python
import hmac, hashlib
expected = hmac.new(SECRET.encode(), request.get_data(), hashlib.sha256).hexdigest()
if not hmac.compare_digest(expected, request.headers.get("X-Callback-Signature", "")):
    abort(401)
```

Tip: callbacks can arrive more than once (after a retry). Use `job_id` or `request_id` to ignore duplicates.

---

## 8. Error types

### Final answers from the result site (`retryable: false`)

Do not resend the same roll/reg; show the message to the user.

| `error_type` | Meaning |
|---|---|
| `invalid_registration` | Registration number is wrong for this roll |
| `invalid_roll` | Roll number is invalid |
| `result_not_published` | The result is not published yet |
| `result_not_found` | No result exists for these details |
| `result_error` | Any other answer from the site (read `msg`) |

The type is chosen from keywords in the site's message. If a site words its message differently you get `result_error`, and the original text is still in `msg`.

### Temporary problems (`retryable: true`)

Safe to retry later.

| `error_type` | Meaning |
|---|---|
| `no_server_live` | Every result site is down or blocked |
| `all_servers_failed` | Live sites all failed during the lookup (see `errors`) |
| `captcha_solver_failed` | Gemini could not be used (no key, all keys rejected or rate limited) |
| `timeout` | The job exceeded `JOB_TIMEOUT` |
| `internal_error` | Unexpected bug in the service (check the logs) |

---

## 9. Telegram bot

Create a bot with **@BotFather**, put the token in `BOT_TOKEN`, start a chat with it, send `/id`, and put the number it shows in `ADMIN_IDS`. Restart the service. Only admins can use the bot.

### What it sends automatically

| Message | When |
|---|---|
| 🟢 **result-service started** | On startup |
| 📩 **Request** | Every `POST /result`: the JSON that came in and the exact HTTP reply (✅ 202 or ❌ 400/401/503) |
| ✅ **Result ready** | A job finished: job id, roll, reg, and the full result JSON |
| ❌ **Result FAILED** | A job failed: error text, `error_type`, retryable or final, and the JSON |
| 📬 / 📪 **Callback delivered / FAILED** | Whether your server received the result |
| 🚨 **Job failed** | A temporary failure (servers or Gemini), not for normal "invalid registration" answers |
| 🔴 **All Gemini keys unavailable** | No usable key left |
| 📈 **Digest** | Hourly status summary |

### Commands

| Command | What it does |
|---|---|
| `/menu` | Main menu with buttons |
| `/status` | Live status: keys, queues, jobs, threads |
| `/health` | Probe all result sites and all Gemini keys |
| `/sites` | Probe result sites only |
| `/keys` | List keys; test, reset cooldown, or remove each one |
| `/addkey` | Guided form to add key(s). Keys are tested before saving, and your message is deleted |
| `/testkey` | Test a key without adding it |
| `/queue` | Queue depths |
| `/jobs` | Recent jobs |
| `/digest` | Digest on demand |
| `/cancel` | Leave a form |
| `/id` | Show your Telegram id (works for anyone) |

> The request/result messages contain students' data. Keep the bot chat private, or set `ALERT_MASK_PAYLOAD=1` to mask roll and reg.

---

## 10. Deploying

### Linux server with systemd

`/etc/systemd/system/result-service.service`

```ini
[Unit]
Description=Result Service
After=network-online.target

[Service]
WorkingDirectory=/opt/result_service
ExecStart=/opt/result_service/venv/bin/python main.py
Restart=always
RestartSec=5
User=www-data

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now result-service
journalctl -u result-service -f      # live logs
```

### Platforms like Railway / Render

Set the variables from `.env` in the platform's settings. The service reads the platform's `PORT` automatically. Use `/ready` as the health check. Keep **one instance only**.

### Alternative: gunicorn

```bash
gunicorn -w 1 --threads 8 -b 0.0.0.0:5000 main:APP
```

Always `-w 1` (one process).

### Render specifics

- **Keys added with `/addkey` are lost on redeploy** because Render's disk is temporary. Put your keys in the `GEMINI_KEYS` environment variable (comma-separated), or attach a persistent disk and set `KEYS_FILE` to a path on it.
- During a deploy, Render may briefly run the old and new instance together. The bot then logs `TelegramConflictError` for a few seconds and recovers on its own.
- Use `/ready` as the health check path. Keep one instance only.

### Good to know

- The queue and job list are **in memory**. If the service restarts, jobs that were still waiting are lost, so your app should retry requests that never produced a callback (use `request_id` to avoid duplicates).
- On a stop signal the service waits up to `SHUTDOWN_GRACE` seconds for running jobs.
- Put it behind HTTPS (nginx, Caddy, or the platform's proxy) if it is reachable from the internet.

---

## 11. Troubleshooting

| Problem | What to check |
|---|---|
| Bot does not answer | `BOT_TOKEN` correct? Your id in `ADMIN_IDS`? Only one copy running? Check the log for `bot crashed` |
| "⛔ Not authorized" in the bot | Send `/id`, put the number in `ADMIN_IDS`, restart |
| No Telegram messages but the service works | `ADMIN_IDS` or `ALERT_CHAT_ID` is empty. The log says `alerts and digests are off` |
| `401 unauthorized` from the API | Token mismatch. Send `Authorization: Bearer <API_TOKEN>` |
| `no_server_live` | Result sites are down, or the anti-bot challenge changed. Use `/sites` in the bot |
| `captcha_solver_failed` | No usable Gemini key. Use `/keys`, test, reset, or `/addkey` |
| `/addkey` says "API key not valid" | Both key formats are supported: `AIza...` and the newer `AQ.xxxx...` (it contains a dot). Paste the **whole** key. In older versions of this bot the dot cut `AQ.` off the key; that is fixed |
| Gemini keys keep "cooling" | Free-tier rate limit. Add more keys |
| `callback_url host does not resolve` / `non-public address` | Callback to localhost/private IP needs `ALLOW_PRIVATE_CALLBACKS=1`, or add the host to `CALLBACK_ALLOWED_HOSTS` |
| Callbacks fail | Open `failed_callbacks.jsonl`. Your server must answer 2xx quickly. Check the signature code |
| `503 queue is full` | Raise `QUEUE_MAX` or `WORKERS`, or send fewer requests at once |
| Everything slow | Raise `WORKERS` (also needs enough Gemini keys). Each request opens a session on every site first, which adds a little time |

Set `LOG_LEVEL=DEBUG` for more detail. The log shows each decision: `preflight: live=[...]`, `trying <site>`, `switching to next live server`, Gemini key retries.

---

## 12. Security notes

- Set a strong `API_TOKEN`. With it empty, anyone who finds the URL can spend your Gemini quota.
- Always verify `X-Callback-Signature` in your callback endpoint.
- Never commit `.env` or `gemini_keys.json`. They hold secrets.
- Telegram messages never include your API token, Gemini keys, or the query part (`?token=...`) of the callback URL.
- Callbacks to private or local addresses are blocked by default (protection against server-side request forgery). Enable them only when you need them.
