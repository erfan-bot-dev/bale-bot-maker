import os
import re
import ast
import json
import time
import html
import threading
import multiprocessing
import sqlite3
import urllib.request
import urllib.parse
import secrets
import hashlib
import inspect
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# =========================================================
# CONFIG
# =========================================================

# بهتر است توکن را در متغیر محیطی MAIN_TOKEN قرار بده؛ هرگز توکن واقعی را داخل فایل عمومی نگذار.
MAIN_TOKEN = os.environ.get("MAIN_TOKEN", "توکن_جدید_ربات_اصلی_را_اینجا_قرار_بده").strip()
DB_FILE = "bot_maker_v6.db"
CHILDREN_DIR = "children"
POLL_TIMEOUT = 20

# برای Webhook روی VPS: دامنه‌ای که HTTPS معتبر دارد، مانند https://bot.example.com
# اگر خالی باشد، ربات اصلی با getUpdates (Polling) اجرا می‌شود؛ مناسب تست محلی/Pydroid.
WEBHOOK_BASE_URL = (os.environ.get("WEBHOOK_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")
# Render provides PORT and requires binding to 0.0.0.0. Outside Render, WEBHOOK_PORT can be used.
WEBHOOK_PORT = int(os.environ.get("PORT") or os.environ.get("WEBHOOK_PORT", "10000"))
# مسیر محرمانهٔ endpoint. برای ثبات بین ری‌استارت‌ها می‌توان WEBHOOK_SECRET را در محیط تنظیم کرد.
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", secrets.token_urlsafe(32)).strip()

# 🛑 محدودیت‌های سرور
MAX_BOTS_PER_USER = 2           # محدودیت ۲ ربات
MAX_WEEKLY_UPTIME = 90 * 3600   # ۹۰ ساعت در هفته (به ثانیه)
MAX_CODE_PREVIEW = 3500         # حداکثر خطوط نمایش کد

# 💎 پلن‌های نمونه اشتراک ویژه (قیمت به تومان؛ قبل از انتشار قابل تغییر است)
SUBSCRIPTION_PLANS = {
    30: 300_000,
    60: 550_000,
    90: 750_000,
    120: 900_000,
    180: 1_250_000,
    300: 1_800_000,
    360: 2_000_000,
}
PAYMENT_CONTACT = "@sadboytoxk"

# ⭐ آیدی عددی خودت (مالک ربات) رو اینجا بذار:
OWNER_ID = 2082625569  # مثال: 2086123456

active_processes = {}
active_webhook_queues = {}
webhook_id_to_token = {}
webhook_server = None


# =========================================================
# HELPERS
# =========================================================

def esc(s):
    return html.escape(str(s) if s else "")


def mask_token(token):
    if not token or ":" not in token: return "—"
    parts = token.split(":", 1)
    prefix = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    if len(rest) <= 8:
        return f"{prefix}:{rest[:2]}...{rest[-2:]}"
    return f"{prefix}:{rest[:4]}...{rest[-4:]}"


def fmt_duration(seconds):
    if seconds < 0: seconds = 0
    if seconds < 60: return f"{seconds} ثانیه"
    if seconds < 3600:
        m = seconds // 60
        return f"{m} دقیقه"
    if seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h} ساعت و {m} دقیقه"
    d = seconds // 86400
    return f"{d} روز"


def fmt_ago(timestamp):
    if not timestamp: return "—"
    return fmt_duration(int(time.time()) - int(timestamp))


def fmt_date(timestamp):
    if not timestamp: return "—"
    from datetime import datetime
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y/%m/%d - %H:%M")


# =========================================================
# DATABASE
# =========================================================

db = sqlite3.connect(DB_FILE, check_same_thread=False)
db_lock = threading.Lock()


def db_execute(query, params=(), fetchone=False, fetchall=False):
    with db_lock:
        cur = db.cursor()
        cur.execute(query, params)
        r = None
        if fetchone: r = cur.fetchone()
        elif fetchall: r = cur.fetchall()
        db.commit()
        return r


def init_db():
    with db_lock:
        cur = db.cursor()

        # جدول کاربران با سیستم محاسبه زمان
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            username TEXT,
            uptime_this_week INTEGER DEFAULT 0,
            week_start INTEGER DEFAULT 0
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS bots (
            token TEXT PRIMARY KEY,
            owner_id INTEGER,
            bot_username TEXT,
            bot_name TEXT,
            code TEXT,
            file_path TEXT,
            created_at INTEGER,
            is_running INTEGER DEFAULT 0,
            started_at INTEGER DEFAULT 0
        )""")

        # Per-bot runtime mode: polling (free-compatible) or webhook (premium only).
        cur.execute("PRAGMA table_info(bots)")
        existing_columns = {row[1] for row in cur.fetchall()}
        if "runtime_mode" not in existing_columns:
            cur.execute("ALTER TABLE bots ADD COLUMN runtime_mode TEXT DEFAULT 'polling'")
        cur.execute("UPDATE bots SET runtime_mode='polling' WHERE runtime_mode IS NULL OR runtime_mode=''")

        cur.execute("""CREATE TABLE IF NOT EXISTS user_states (
            user_id INTEGER PRIMARY KEY,
            state TEXT,
            temp_data TEXT,
            updated_at INTEGER
        )""")

        # وضعیت اشتراک ویژه هر کاربر؛ last_expiry_handled مانع اجرای دوباره روال انقضاست.
        cur.execute("""CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY,
            expires_at INTEGER DEFAULT 0,
            last_expiry_handled INTEGER DEFAULT 0
        )""")

        # درخواست‌های پرداخت دستی اشتراک
        cur.execute("""CREATE TABLE IF NOT EXISTS subscription_requests (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            days INTEGER NOT NULL,
            price INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            reviewed_at INTEGER DEFAULT 0,
            reviewed_by INTEGER DEFAULT 0
        )""")

        db.commit()

init_db()
os.makedirs(CHILDREN_DIR, exist_ok=True)


# =========================================================
# API & UTILS
# =========================================================

def api(token, method, data=None):
    url = f"https://tapi.bale.ai/bot{token}/{method}"
    try:
        if data is None: data = {}
        encoded = urllib.parse.urlencode(data).encode("utf-8")
        req = urllib.request.Request(url, data=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=40) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"API ERROR [{method}]:", e)
        return {"ok": False, "error": str(e)}


def send_message(token, chat_id, text, reply_markup=None, parse_mode=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    if parse_mode:
        data["parse_mode"] = parse_mode
    return api(token, "sendMessage", data)


def edit_message(token, chat_id, mid, text, reply_markup=None, parse_mode=None):
    data = {"chat_id": chat_id, "message_id": mid, "text": text}
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    if parse_mode:
        data["parse_mode"] = parse_mode
    return api(token, "editMessageText", data)


def answer_callback(token, cid, text=None, show_alert=False):
    data = {"callback_query_id": cid, "show_alert": "true" if show_alert else "false"}
    if text: data["text"] = text
    return api(token, "answerCallbackQuery", data)


def get_me(token):
    r = api(token, "getMe")
    return r.get("result") if r.get("ok") else None


def ibtn(text, callback_data=None, url=None):
    b = {"text": text}
    if callback_data is not None: b["callback_data"] = callback_data
    if url is not None: b["url"] = url
    return b


def download_file(file_id):
    try:
        r = api(MAIN_TOKEN, "getFile", {"file_id": file_id})
        if not r.get("ok"): return None
        file_path = r["result"]["file_path"]
        url = f"https://tapi.bale.ai/file/bot{MAIN_TOKEN}/{file_path}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"DOWNLOAD ERR: {e}")
        return None


# =========================================================
# STATE & DB HELPERS
# =========================================================

def save_user_info(uid, name, username):
    db_execute("""INSERT INTO users (user_id, first_name, username, week_start)
        VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET
        first_name=excluded.first_name, username=excluded.username""",
        (int(uid), name or "", username or "", int(time.time())))


def get_user_info(uid):
    return db_execute("SELECT first_name, username, uptime_this_week, week_start FROM users WHERE user_id=?",
        (int(uid),), fetchone=True)


def set_user_state(uid, state, temp_data=""):
    db_execute("""INSERT INTO user_states (user_id, state, temp_data, updated_at)
        VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET
        state=excluded.state, temp_data=excluded.temp_data,
        updated_at=excluded.updated_at""",
        (int(uid), state, temp_data, int(time.time())))


def get_user_state(uid):
    return db_execute("SELECT state, temp_data FROM user_states WHERE user_id=?",
        (int(uid),), fetchone=True)


def clear_user_state(uid):
    db_execute("DELETE FROM user_states WHERE user_id=?", (int(uid),))


def save_bot(token, owner_id, bot_username, bot_name, code, file_path, runtime_mode="polling"):
    now = int(time.time())
    db_execute("""INSERT INTO bots
        (token, owner_id, bot_username, bot_name, code, file_path,
         created_at, is_running, started_at, runtime_mode)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(token) DO UPDATE SET
        code=excluded.code, file_path=excluded.file_path,
        started_at=excluded.started_at, is_running=1,
        runtime_mode=excluded.runtime_mode""",
        (token, int(owner_id), bot_username or "", bot_name or "",
         code, file_path, now, now, runtime_mode))


def get_bot_runtime_mode(token):
    row = db_execute("SELECT runtime_mode FROM bots WHERE token=?", (token,), fetchone=True)
    return (row[0] or "polling") if row else "polling"


def set_bot_runtime_mode(token, mode):
    if mode not in ("polling", "webhook"):
        raise ValueError("runtime mode must be polling or webhook")
    db_execute("UPDATE bots SET runtime_mode=? WHERE token=?", (mode, token))


def webhook_id(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:32]


def get_bot(token):
    return db_execute("""SELECT token, owner_id, bot_username, bot_name,
        code, file_path, created_at, is_running, started_at
        FROM bots WHERE token=?""", (token,), fetchone=True)


def get_my_bots(uid):
    return db_execute("""SELECT token, bot_username, bot_name, is_running,
        created_at, started_at
        FROM bots WHERE owner_id=? ORDER BY created_at DESC""",
        (int(uid),), fetchall=True)


def get_all_bots():
    return db_execute("""SELECT token, bot_username, owner_id, is_running
        FROM bots ORDER BY created_at DESC""", fetchall=True)


def count_user_bots(uid):
    r = db_execute("SELECT COUNT(*) FROM bots WHERE owner_id=?",
        (int(uid),), fetchone=True)
    return r[0] if r else 0


def ensure_week_window(uid, now=None):
    """Reset the user's rolling 7-day quota window when it expires."""
    now = int(now or time.time())
    info = get_user_info(uid)
    if not info:
        return
    week_start = int(info[3] or 0)
    if week_start <= 0:
        db_execute("UPDATE users SET week_start=? WHERE user_id=?", (now, int(uid)))
        return
    if now - week_start >= 7 * 86400:
        # Running projects start a fresh accounting session; downtime is never charged.
        db_execute("UPDATE users SET uptime_this_week=0, week_start=? WHERE user_id=?",
                   (now, int(uid)))
        db_execute("UPDATE bots SET started_at=? WHERE owner_id=? AND is_running=1",
                   (now, int(uid)))


def get_user_used_uptime(uid, now=None):
    """Return accumulated plus currently-running time for all of a user's bots."""
    now = int(now or time.time())
    ensure_week_window(uid, now)
    info = get_user_info(uid)
    if not info:
        return 0
    accumulated = max(0, int(info[2] or 0))
    rows = db_execute(
        "SELECT started_at FROM bots WHERE owner_id=? AND is_running=1",
        (int(uid),), fetchall=True
    ) or []
    live_seconds = sum(max(0, now - int(row[0] or now)) for row in rows)
    return accumulated + live_seconds


def set_bot_stopped(token):
    bot = get_bot(token)
    if bot:
        ensure_week_window(bot[1])
        bot = get_bot(token)
    if bot and bot[7] == 1 and bot[8] > 0 and not is_premium(bot[1]):
        up_time = max(0, int(time.time()) - int(bot[8]))
        db_execute("UPDATE users SET uptime_this_week = uptime_this_week + ? WHERE user_id=?",
                   (up_time, bot[1]))
    if get_bot_runtime_mode(token) == "webhook":
        try:
            api(token, "deleteWebhook", {"drop_pending_updates": "true"})
        except Exception as e:
            print("DELETE WEBHOOK ERROR:", e)
    db_execute("UPDATE bots SET is_running=0, started_at=0 WHERE token=?", (token,))


def delete_bot(token):
    db_execute("DELETE FROM bots WHERE token=?", (token,))


# =========================================================
# PREMIUM SUBSCRIPTIONS
# =========================================================

def get_subscription(uid):
    row = db_execute("SELECT expires_at, last_expiry_handled FROM subscriptions WHERE user_id=?",
                     (int(uid),), fetchone=True)
    return row if row else (0, 0)


def is_premium(uid, now=None):
    now = int(now or time.time())
    expires_at, _ = get_subscription(uid)
    return int(expires_at or 0) > now


def format_price(amount):
    return f"{int(amount):,}".replace(",", "٬") + " تومان"


def premium_status_text(uid, now=None):
    now = int(now or time.time())
    expires_at, _ = get_subscription(uid)
    if int(expires_at or 0) > now:
        return f"💎 ویژه فعال تا <b>{fmt_date(expires_at)}</b>"
    return "🎁 پلن رایگان فعال است"


def create_subscription_request(uid, days):
    if days not in SUBSCRIPTION_PLANS:
        return None, "پلن نامعتبر است."
    pending = db_execute("SELECT request_id, days, price FROM subscription_requests WHERE user_id=? AND status='pending' ORDER BY request_id DESC LIMIT 1",
                         (int(uid),), fetchone=True)
    if pending:
        return pending[0], "pending"
    price = SUBSCRIPTION_PLANS[days]
    now = int(time.time())
    # INSERT و گرفتن شناسه در یک قفل تا هیچ ترد دیگری نتواند شناسه را عوض کند.
    with db_lock:
        cur = db.cursor()
        cur.execute("INSERT INTO subscription_requests (user_id, days, price, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
                    (int(uid), int(days), int(price), now))
        request_id = cur.lastrowid
        db.commit()
    return request_id, "created"


def subscription_request_details(request_id):
    return db_execute("""SELECT request_id, user_id, days, price, status, created_at, reviewed_at, reviewed_by
        FROM subscription_requests WHERE request_id=?""", (int(request_id),), fetchone=True)


def approve_subscription_request(request_id, owner_id):
    if int(owner_id) != OWNER_ID:
        return False, "دسترسی غیرمجاز"
    req = subscription_request_details(request_id)
    if not req or req[4] != "pending":
        return False, "این درخواست وجود ندارد یا قبلاً بررسی شده است."
    _, uid, days, price, _, _, _, _ = req
    now = int(time.time())
    expires_at, _ = get_subscription(uid)
    start_at = max(now, int(expires_at or 0))
    new_expiry = start_at + int(days) * 86400
    db_execute("""INSERT INTO subscriptions (user_id, expires_at, last_expiry_handled)
        VALUES (?, ?, 0) ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at""",
        (int(uid), int(new_expiry)))
    db_execute("UPDATE subscription_requests SET status='approved', reviewed_at=?, reviewed_by=? WHERE request_id=? AND status='pending'",
               (now, int(owner_id), int(request_id)))
    return True, (uid, days, price, new_expiry)


def reject_subscription_request(request_id, owner_id):
    if int(owner_id) != OWNER_ID:
        return False
    now = int(time.time())
    changed = db_execute("UPDATE subscription_requests SET status='rejected', reviewed_at=?, reviewed_by=? WHERE request_id=? AND status='pending'",
                         (now, int(owner_id), int(request_id)))
    # db_execute returns None for writes, so verify the resulting status.
    req = subscription_request_details(request_id)
    return bool(req and req[4] == "rejected" and req[7] == int(owner_id))


def show_subscription_plans(chat_id, uid, message_id=None):
    current = premium_status_text(uid)
    text = (
        "<b>💎 اشتراک ویژه</b>\n━━━━━━━━━━━━━━━━━━\n\n"
        "با اشتراک ویژه، محدودیت ۹۰ ساعت اجرای هفتگی برداشته می‌شود و برای کدهای سازگار، روش Webhook هم در دسترس است.\n"
        "Webhook به VPS با دامنه و HTTPS نیاز دارد؛ کد باید تابع handle_update(update) داشته باشد و Polling fallback هم حفظ شده باشد.\n\n"
        f"{current}\n\n"
        "یکی از پلن‌ها را انتخاب کن. تمدید اشتراک فعال به انتهای زمان فعلی اضافه می‌شود:"
    )
    rows = []
    plan_items = list(SUBSCRIPTION_PLANS.items())
    for i in range(0, len(plan_items), 2):
        row = []
        for days, price in plan_items[i:i+2]:
            row.append(ibtn(f"{days} روز | {format_price(price)}", f"buyplan:{days}"))
        rows.append(row)
    rows.append([ibtn("🔙 بازگشت", "back")])
    markup = {"inline_keyboard": rows}
    if message_id:
        try:
            edit_message(MAIN_TOKEN, chat_id, message_id, text, markup, parse_mode="HTML")
            return
        except Exception:
            pass
    send_message(MAIN_TOKEN, chat_id, text, markup, parse_mode="HTML")


def show_subscription_requests(chat_id, uid, message_id=None):
    if int(uid) != OWNER_ID:
        send_message(MAIN_TOKEN, chat_id, "❌ فقط مالک می‌تواند درخواست‌ها را ببیند.")
        return
    rows_data = db_execute("SELECT request_id, user_id, days, price, created_at FROM subscription_requests WHERE status='pending' ORDER BY created_at ASC", fetchall=True) or []
    if not rows_data:
        text = "<b>💳 درخواست‌های اشتراک</b>\n━━━━━━━━━━━━━━━━━━\n\nدرخواست پرداختِ در انتظار بررسی وجود ندارد."
        markup = {"inline_keyboard": [[ibtn("🔙 منوی اصلی", "back")]]}
    else:
        text = f"<b>💳 درخواست‌های اشتراک</b>\n━━━━━━━━━━━━━━━━━━\n\nدرخواست‌های در انتظار: <b>{len(rows_data)}</b>\nبرای بررسی، روی یک درخواست بزن:"
        rows = []
        for rid, user_id, days, price, created_at in rows_data:
            info = get_user_info(user_id)
            name = info[0] if info and info[0] else "کاربر"
            rows.append([ibtn(f"#{rid} | {name} | {days} روز", f"subreq:{rid}")])
        rows.append([ibtn("🔙 منوی اصلی", "back")])
        markup = {"inline_keyboard": rows}
    if message_id:
        try:
            edit_message(MAIN_TOKEN, chat_id, message_id, text, markup, parse_mode="HTML")
            return
        except Exception:
            pass
    send_message(MAIN_TOKEN, chat_id, text, markup, parse_mode="HTML")


# =========================================================
# TOKEN INJECTION
# =========================================================

def inject_token(code, token):
    patterns = [
        r'(BOT_TOKEN)\s*=\s*["\'][^"\']*["\']',
        r'(MAIN_TOKEN)\s*=\s*["\'][^"\']*["\']',
        r'(\bbot_token\b)\s*=\s*["\'][^"\']*["\']',
        r'(TOKEN)\s*=\s*["\'][^"\']*["\']',
        r'(API_TOKEN)\s*=\s*["\'][^"\']*["\']',
    ]
    for pattern in patterns:
        new_code, count = re.subn(pattern,
            lambda m: f'{m.group(1)} = "{token}"', code, count=1)
        if count > 0:
            return new_code
    return f'BOT_TOKEN = "{token}"\n' + code


# =========================================================
# RUN / STOP
# =========================================================

def _bot_process_entry(token, code, file_path, owner_id):
    """Top-level child target; required for multiprocessing on Windows."""
    try:
        print(f"🚀 RUNNING: {token[:12]}...")
        exec_globals = {"__name__": "__main__", "__file__": file_path}
        exec(code, exec_globals)
    except SystemExit:
        pass
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"❌ ERROR [{token[:12]}]: {e}")
        import traceback
        traceback.print_exc()
        try:
            send_message(MAIN_TOKEN, owner_id,
                f"❌ <b>رباتت با خطا متوقف شد</b>\n\n"
                f"🐛 خطا:\n<code>{esc(str(e)[:200])}</code>",
                parse_mode="HTML")
        except Exception:
            pass
    finally:
        try:
            set_bot_stopped(token)
        except Exception as e:
            print(f"DB STOP ERROR [{token[:12]}]: {e}")
        print(f"🛑 STOPPED: {token[:12]}")


def run_user_bot(token, code, file_path, owner_id):
    """Start a user bot in its original polling mode."""
    old = active_processes.get(token)
    if old and old.is_alive():
        print(f"⚠️ Process already running: {token[:12]}...")
        return
    q = active_webhook_queues.pop(token, None)
    if q:
        try: q.close()
        except Exception: pass
    set_bot_runtime_mode(token, "polling")
    p = multiprocessing.Process(target=_bot_process_entry,
        args=(token, code, file_path, int(owner_id)), daemon=True)
    p.start()
    active_processes[token] = p
    db_execute("UPDATE bots SET is_running=1, started_at=? WHERE token=?", (int(time.time()), token))


def _strip_polling_entrypoints(code):
    """Remove common top-level polling launch calls only in the webhook worker copy."""
    tree = ast.parse(code)
    startup_attrs = {"run", "run_polling", "start_polling", "polling", "infinity_polling",
                     "start_webhook", "run_webhook", "run_until_disconnected"}
    class StripLaunch(ast.NodeTransformer):
        def visit_Expr(self, node):
            value = node.value
            if isinstance(value, ast.Call):
                f = value.func
                if isinstance(f, ast.Attribute) and f.attr in startup_attrs:
                    return ast.copy_location(ast.Pass(), node)
                if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id == "asyncio" and f.attr == "run":
                    return ast.copy_location(ast.Pass(), node)
                if isinstance(f, ast.Name) and f.id in {"run_polling", "start_polling", "run_webhook", "start_webhook"}:
                    return ast.copy_location(ast.Pass(), node)
            return self.generic_visit(node)
    tree = StripLaunch().visit(tree)
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)


def _webhook_worker_entry(token, code, file_path, owner_id, update_queue):
    """Execute webhook-compatible user code once and call handle_update for each Update."""
    try:
        webhook_code = _strip_polling_entrypoints(code)
        g = {"__name__": "__main__", "__file__": file_path,
             "BOT_TOKEN": token, "BOT_RUNTIME_MODE": "webhook", "WEBHOOK_MODE": True}
        exec(compile(webhook_code, file_path, "exec"), g)
        handler = g.get("handle_update") or g.get("handle_webhook_update")
        if not callable(handler):
            raise RuntimeError("Webhook mode requires a handle_update(update) or handle_webhook_update(update) function")
        print(f"🌐 WEBHOOK WORKER READY: {token[:12]}...")
        while True:
            update = update_queue.get()
            if update is None:
                break
            try:
                try:
                    param_count = len(inspect.signature(handler).parameters)
                except Exception:
                    param_count = 1
                result = handler(update, token) if param_count >= 2 else handler(update)
                if inspect.isawaitable(result):
                    asyncio.run(result)
            except Exception as e:
                print(f"WEBHOOK UPDATE ERROR [{token[:12]}]: {e}")
                import traceback; traceback.print_exc()
    except Exception as e:
        print(f"WEBHOOK BOT ERROR [{token[:12]}]: {e}")
        import traceback; traceback.print_exc()
        try:
            send_message(MAIN_TOKEN, owner_id,
                f"❌ خطای اجرای Webhook در رباتت: <code>{esc(str(e)[:250])}</code>", parse_mode="HTML")
        except Exception:
            pass
    finally:
        try: set_bot_stopped(token)
        except Exception: pass


def code_has_polling_entrypoint(code):
    """Heuristic used for downgrade checks; not a guarantee that the code runs successfully."""
    patterns = (
        r"\b(?:bot|client|app|application|dispatcher|dp|updater)\.run\s*\(",
        r"\b(?:bot|client|app|application|dispatcher|dp)\.(?:start_polling|run_polling)\s*\(",
        r"\b(?:bot|client|app)\.(?:polling|infinity_polling|run_until_disconnected)\s*\(",
        r"\bupdater\.start_polling\s*\(",
        r"\b(?:run_polling|start_polling)\s*\(",
        r"\bgetUpdates\b|\bget_updates\s*\(",
        r"\basyncio\.run\s*\("
    )
    return any(re.search(pattern, code, flags=re.IGNORECASE) for pattern in patterns)


def code_supports_webhook(code):
    """Webhook mode requires a raw-update handler and a polling fallback for the free plan."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False, "کد از نظر سینتکس معتبر نیست."
    handler = any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and
                  n.name in ("handle_update", "handle_webhook_update") for n in ast.walk(tree))
    if not handler:
        return False, "تابع handle_update(update) یا handle_webhook_update(update) در کد پیدا نشد."
    if not code_has_polling_entrypoint(code):
        return False, "برای امکان برگشت به پلن رایگان، کد باید علاوه بر handler وب‌هوک، نقطه شروع Polling هم داشته باشد (مثلاً bot.run())."
    return True, ""


def start_webhook_bot(token, code, file_path, owner_id):
    if not is_premium(owner_id):
        return False, "Webhook فقط در اشتراک ویژه قابل استفاده است."
    if not WEBHOOK_BASE_URL or not WEBHOOK_BASE_URL.lower().startswith("https://"):
        return False, "برای Webhook باید روی VPS دامنه و HTTPS تنظیم شود؛ فعلاً Polling را انتخاب کن."
    if webhook_server is None:
        return False, "Listener وب‌هوک روی سرور بالا نیامده؛ تنظیمات VPS و WEBHOOK_PORT را بررسی کن."
    ok, reason = code_supports_webhook(code)
    if not ok:
        return False, reason

    wid = webhook_id(token)
    endpoint = f"{WEBHOOK_BASE_URL}/user-bot/{wid}/{WEBHOOK_SECRET}"
    old = active_processes.get(token)
    if old and old.is_alive():
        old.terminate(); old.join(timeout=3)
    old_q = active_webhook_queues.pop(token, None)
    if old_q:
        try: old_q.close()
        except Exception: pass

    # Start the worker and register the route before telling Bale to deliver updates.
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_webhook_worker_entry,
        args=(token, code, file_path, int(owner_id), q), daemon=True)
    p.start()
    active_processes[token] = p
    active_webhook_queues[token] = q
    webhook_id_to_token[wid] = token
    set_bot_runtime_mode(token, "webhook")
    db_execute("UPDATE bots SET is_running=1, started_at=? WHERE token=?", (int(time.time()), token))

    # Give Windows spawn/imports a moment, but don't wait indefinitely on a broken script.
    for _ in range(30):
        time.sleep(0.1)
        if not p.is_alive():
            break
    if not p.is_alive():
        active_processes.pop(token, None); active_webhook_queues.pop(token, None)
        webhook_id_to_token.pop(wid, None)
        set_bot_runtime_mode(token, "polling")
        set_bot_stopped(token)
        return False, "پردازش Webhook هنگام بارگذاری کد متوقف شد؛ لاگ VPS را بررسی کن."

    result = api(token, "setWebhook", {"url": endpoint})
    if not isinstance(result, dict) or not result.get("ok") or result.get("result") is False:
        try: q.put(None)
        except Exception: pass
        if p.is_alive():
            p.terminate(); p.join(timeout=3)
        active_processes.pop(token, None); active_webhook_queues.pop(token, None)
        webhook_id_to_token.pop(wid, None)
        try: api(token, "deleteWebhook", {"drop_pending_updates": "true"})
        except Exception: pass
        set_bot_runtime_mode(token, "polling")
        set_bot_stopped(token)
        return False, f"بله Webhook را نپذیرفت: {str(result)[:250]}"
    print(f"✅ User Webhook active for {token[:12]}...")
    return True, ""


def stop_bot_process(token):
    p = active_processes.get(token)
    q = active_webhook_queues.pop(token, None)
    if q:
        try: q.put(None)
        except Exception: pass
    if p and p.is_alive():
        try:
            p.terminate()
            p.join(timeout=3)
        except Exception as e:
            print(f"STOP ERROR [{token[:12]}]: {e}")
    active_processes.pop(token, None)
    if get_bot_runtime_mode(token) == "webhook":
        try: api(token, "deleteWebhook", {"drop_pending_updates": "true"})
        except Exception as e: print("DELETE WEBHOOK ERROR:", e)
    set_bot_stopped(token)


# =========================================================
# UPTIME TRACKER THREAD (نظارت بر ۹۰ ساعت)
# =========================================================

def uptime_checker():
    """Enforce a combined 90-hour weekly limit across all a user's projects."""
    while True:
        try:
            now = int(time.time())
            # وقتی اشتراک منقضی می‌شود، شمارش پلن رایگان از همان لحظه شروع می‌شود.
            expired = db_execute("SELECT user_id, expires_at FROM subscriptions WHERE expires_at>0 AND expires_at<=? AND last_expiry_handled<expires_at",
                                 (now,), fetchall=True) or []
            for exp_row in expired:
                uid, expiry = int(exp_row[0]), int(exp_row[1])
                # Stop all premium-running bots first; the user explicitly chooses renew or free.
                running = db_execute("SELECT token FROM bots WHERE owner_id=? AND is_running=1", (uid,), fetchall=True) or []
                for item in running:
                    stop_bot_process(item[0])
                db_execute("UPDATE subscriptions SET last_expiry_handled=? WHERE user_id=?", (expiry, uid))
                db_execute("UPDATE users SET uptime_this_week=0, week_start=? WHERE user_id=?", (now, uid))
                send_message(MAIN_TOKEN, uid,
                    "⌛ <b>اشتراک ویژه‌ات به پایان رسید.</b>\n\n"
                    "همه ربات‌ها متوقف شدند. برای ادامه یکی از گزینه‌ها را انتخاب کن:",
                    {"inline_keyboard": [
                        [ibtn("💎 تمدید اشتراک ویژه", "premium")],
                        [ibtn("🎁 ادامه با پلن رایگان (Polling)", "continue_free")],
                        [ibtn("📊 پروژه‌های من", "my_bots")]
                    ]}, parse_mode="HTML")

            users = db_execute("SELECT user_id FROM users", fetchall=True) or []
            for row in users:
                uid = int(row[0])
                if is_premium(uid, now):
                    continue
                ensure_week_window(uid, now)
                used = get_user_used_uptime(uid, now)
                if used < MAX_WEEKLY_UPTIME:
                    continue

                running = db_execute(
                    "SELECT token FROM bots WHERE owner_id=? AND is_running=1",
                    (uid,), fetchall=True
                ) or []
                if not running:
                    continue

                # Stop every project belonging to this user once the combined quota is used.
                for item in running:
                    stop_bot_process(item[0])

                send_message(MAIN_TOKEN, uid,
                    "⏰ <b>سهمیه رایگان این هفته تمام شد!</b>\n\n"
                    "به سقف <b>۹۰ ساعت اجرای هفتگی</b> رسیدی.\n"
                    "برای جلوگیری از مصرف بیشتر منابع، همه پروژه‌های روشن خاموش شدند.\n\n"
                    "🔄 سهمیه در شروع چرخه ۷روزه بعدی دوباره شارژ می‌شود.",
                    parse_mode="HTML")
        except Exception as e:
            print(f"UPTIME CHECK ERROR: {e}")
        time.sleep(30)


# =========================================================
# UI / MENUS
# =========================================================

def main_menu(uid):
    count = count_user_bots(uid)
    premium_label = "💎 اشتراک ویژه (فعال)" if is_premium(uid) else "💎 خرید اشتراک ویژه"
    kb = [
        [ibtn(f"🚀 افزودن پروژه ({count}/{MAX_BOTS_PER_USER})", "create")],
        [ibtn("📊 پروژه‌های من", "my_bots")],
        [ibtn(premium_label, "premium")],
    ]
    if uid == OWNER_ID:
        kb.insert(2, [ibtn("👑 مدیریت سرور (مالک)", "admin_panel")])
        kb.insert(3, [ibtn("💳 درخواست‌های اشتراک", "sub_requests")])
    kb.append([ibtn("ℹ️ راهنما", "help")])
    return {"inline_keyboard": kb}


def main_text(name="کاربر"):
    # The caller supplies the display name; usage is shown in the project list/card.
    return (
        f"👋 سلام <b>{esc(name)}</b>!\n\n"
        f"<b>🚀 مرکز مدیریت پروژه‌های رباتی</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"اینجا می‌تونی کد رباتت رو ثبت کنی و پروژه‌ات رو اجرا کنی.\n\n"
        f"<b>🎁 پلن رایگان</b>\n"
        f"├ حداکثر <b>{MAX_BOTS_PER_USER} پروژه</b> برای هر کاربر\n"
        f"├ مجموعاً <b>{MAX_WEEKLY_UPTIME // 3600} ساعت اجرا در هر چرخه ۷روزه</b>\n"
        f"└ اجرای پروژه‌ها با روش <b>Polling</b>\n\n"
        f"💎 کاربران ویژه محدودیت ۹۰ ساعت هفتگی ندارند.\n"
        f"🌐 Webhook فقط برای ربات‌های ساخته‌شده توسط کاربران ویژه و روی VPS دارای HTTPS است.\n\n"
        f"👇 از منوی زیر شروع کن."
    )


def bot_info_card(token, chat_id, uid, is_admin=False):
    bot = get_bot(token)
    if not bot: return None
    
    (tk, owner, uname, fname, code, path,
     created_at, is_running, started_at) = bot
    runtime_mode = get_bot_runtime_mode(tk)
    
    display = f"@{uname}" if uname else (fname or "ربات")
    status_text = "🟢 <b>روشن</b>" if is_running else "🔴 <b>خاموش</b>"
    
    uptime_text = "—"
    if is_running and started_at:
        uptime_text = fmt_ago(started_at) + " پیش"
    
    creator_info = get_user_info(owner)
    c_name = creator_info[0] if creator_info else "نامشخص"
    c_uname = f"@{creator_info[1]}" if creator_info and creator_info[1] else "—"
    
    text = (
        f"<b>🤖 کارت ربات</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"📛 <b>نام:</b> {esc(display)}\n"
        f"🔑 <b>توکن:</b> <code>{esc(mask_token(tk))}</code>\n\n"
        f"👤 <b>سازنده:</b> {esc(c_name)}\n"
        f"🔗 <b>یوزرنیم سازنده:</b> {esc(c_uname)}\n"
        f"🆔 <b>آیدی سازنده:</b> <code>{esc(owner)}</code>\n\n"
        f"📊 <b>وضعیت:</b> {status_text}\n"
        f"⚙️ <b>روش اجرا:</b> {'Webhook' if runtime_mode == 'webhook' else 'Polling'}\n"
        f"⏱ <b>آپتایم:</b> {uptime_text}\n"
        f"📅 <b>ساخته شده:</b> {fmt_date(created_at)}\n"
    )
    
    kb = []
    if is_admin:
        kb.append([ibtn("🗑 حذف ربات (ادمین)", f"adm_del:{tk}")])
        kb.append([ibtn("🔙 بازگشت به لیست ادمین", "admin_panel")])
    else:
        if owner == uid:
            kb.append([ibtn("🔄 ریستارت", f"restart:{tk}"),
                       ibtn("📄 دیدن کد", f"code:{tk}")])
            kb.append([ibtn("⚙️ تغییر روش اجرا", f"mode:{tk}")])
            kb.append([ibtn("🗑 حذف ربات", f"del_confirm:{tk}")])
        kb.append([ibtn("🔙 بازگشت به لیست من", "my_bots")])
        
    return text, {"inline_keyboard": kb}


def show_admin_panel(chat_id, uid):
    if uid != OWNER_ID:
        send_message(MAIN_TOKEN, chat_id, "❌ شما دسترسی به این بخش ندارید.")
        return

    bots = get_all_bots()
    if not bots:
        send_message(MAIN_TOKEN, chat_id, "📭 هیچ رباتی در سیستم وجود ندارد.")
        return

    text = (
        f"<b>👑 پنل مدیریت مالک</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"تعداد کل ربات‌ها: <b>{len(bots)}</b>\n\n"
        f"برای مشاهده جزئیات و حذف، روی هر ربات بزنید:"
    )

    rows = []
    for b in bots:
        token, uname, owner_id, is_run = b
        name = f"@{uname}" if uname else "بدون نام"
        icon = "🟢" if is_run else "🔴"
        rows.append([ibtn(f"{icon} {name} (ID: {owner_id})", f"adm_bot:{token}")])

    rows.append([ibtn("🔙 بازگشت به منوی اصلی", "back")])
    send_message(MAIN_TOKEN, chat_id, text, {"inline_keyboard": rows}, parse_mode="HTML")


# =========================================================
# MESSAGE HANDLER
# =========================================================

def handle_message(message):
    uid = message.get("from", {}).get("id")
    chat_id = message.get("chat", {}).get("id")
    name = message.get("from", {}).get("first_name", "کاربر")
    username = message.get("from", {}).get("username", "")
    text = (message.get("text", "") or "").strip()
    document = message.get("document")

    if not uid: return
    save_user_info(uid, name, username)
    state = get_user_state(uid)

    if text == "مدیریت سرور" and uid == OWNER_ID:
        show_admin_panel(chat_id, uid)
        return

    if text.startswith("/start"):
        clear_user_state(uid)
        send_message(MAIN_TOKEN, chat_id, main_text(name), main_menu(uid), parse_mode="HTML")
        return

    # ---------- دریافت توکن ----------
    if state and state[0] == "waiting_token":
        bot_token = text

        if ":" not in bot_token or len(bot_token) < 20:
            send_message(MAIN_TOKEN, chat_id,
                "❌ <b>توکن نامعتبره!</b>\n\n"
                "فرمت درست:\n<code>123456789:ABCdefGHIjkl...</code>",
                parse_mode="HTML")
            return

        existing = get_bot(bot_token)
        if existing:
            existing_owner = get_user_info(existing[1])
            e_name = existing_owner[0] if existing_owner else "کاربر دیگر"
            clear_user_state(uid)
            send_message(MAIN_TOKEN, chat_id,
                f"❌ <b>این توکن قبلاً ثبت شده است!</b>\n\n"
                f"🤖 نام ربات: @{esc(existing[2])}\n"
                f"👤 سازنده: {esc(e_name)}\n\n"
                f"هر توکن فقط یکبار می‌تونه فعال باشه. اگه ربات خودته، به منوی «پروژه‌های من» برو.",
                {"inline_keyboard": [[ibtn("📊 پروژه‌های من", "my_bots")]]},
                parse_mode="HTML")
            return

        if count_user_bots(uid) >= MAX_BOTS_PER_USER:
            clear_user_state(uid)
            send_message(MAIN_TOKEN, chat_id,
                f"🛑 <b>محدودیت پلن رایگان</b>\n\n"
                f"شما به حداکثر تعداد پروژه ({MAX_BOTS_PER_USER} ربات) رسیدید.\n\n"
                f"🗑 یکی از ربات‌هاتون رو حذف کنید تا بتونید ربات جدید بسازید.",
                {"inline_keyboard": [
                    [ibtn("📊 پروژه‌های من", "my_bots")],
                    [ibtn("🏠 منوی اصلی", "back")],
                ]}, parse_mode="HTML")
            return

        send_message(MAIN_TOKEN, chat_id, "⏳ در حال بررسی توکن...")
        me = get_me(bot_token)
        if not me:
            send_message(MAIN_TOKEN, chat_id,
                "❌ <b>توکن کار نمی‌کنه!</b>\n\n"
                "دلایل احتمالی:\n"
                "• توکن اشتباهه\n"
                "• ربات هنوز ساخته نشده\n"
                "• توکن باطل شده",
                parse_mode="HTML")
            return

        set_user_state(uid, "waiting_code", bot_token)

        send_message(MAIN_TOKEN, chat_id,
            f"✅ <b>توکن معتبره!</b>\n\n"
            f"🤖 ربات: <b>{esc(me.get('username', me.get('first_name', '')))}</b>\n"
            f"🆔 آیدی: <code>{me.get('id')}</code>\n\n"
            f"<b>📝 حالا کد رباتت رو بفرست:</b>\n\n"
            f"• به صورت متن\n"
            f"• یا فایل <code>.py</code>\n\n"
            f"⚠️ کدت باید نقطه شروع معتبر داشته باشه؛ الگوهای رایج polling و اجرای async بررسی می‌شن.",
            {"inline_keyboard": [
                [ibtn("❌ لغو", "cancel")],
            ]}, parse_mode="HTML")
        return

    # ---------- دریافت کد ----------
    if state and state[0] == "waiting_code":
        bot_token = state[1]

        if document:
            file_name = document.get("file_name", "")
            if not file_name.endswith(".py"):
                send_message(MAIN_TOKEN, chat_id, "❌ فقط فایل <code>.py</code> قبول می‌کنم!", parse_mode="HTML")
                return

            send_message(MAIN_TOKEN, chat_id, "⏳ در حال دانلود فایل...")
            code = download_file(document.get("file_id"))
            if not code:
                send_message(MAIN_TOKEN, chat_id, "❌ دانلود ناموفق. دوباره بفرست.")
                return
            process_code(uid, chat_id, bot_token, code)
            return

        if text and (not text.startswith("/") or "=" in text):
            if len(text) < 30:
                send_message(MAIN_TOKEN, chat_id, "❌ حداقل ۳۰ کاراکتر باشه.")
                return
            process_code(uid, chat_id, bot_token, text)
            return

        send_message(MAIN_TOKEN, chat_id, "❌ کد رو بفرست!\n\nمتن یا فایل <code>.py</code>", parse_mode="HTML")
        return

    send_message(MAIN_TOKEN, chat_id, main_text(name), main_menu(uid), parse_mode="HTML")


# =========================================================
# STARTUP COMPATIBILITY DETECTOR
# =========================================================

# این فهرست برای تشخیص الگوهای رایج شروع ربات است، نه تضمین سازگاری
# تمام نسخه‌ها/کتابخانه‌ها. کد کاربر همچنان در محیط خودش اجرا می‌شود.
STARTUP_PATTERNS = [
    # Bale / Telegram-style wrappers
    r"\b(?:bot|client|app|application|dispatcher|dp|updater)\.run\s*\(",
    r"\b(?:bot|client|app|application|dispatcher|dp)\.start_polling\s*\(",
    r"\b(?:bot|client|app|application|dispatcher|dp)\.run_polling\s*\(",
    r"\b(?:bot|client|app|application)\.(?:polling|infinity_polling|start|run_until_disconnected)\s*\(",
    r"\b(?:client|app|bot)\.loop\.run_until_disconnected\s*\(",
    r"\bupdater\.start_(?:polling|webhook)\s*\(",
    r"\b(?:bot|client|app|application)\.start_webhook\s*\(",
    r"\b(?:run_polling|start_polling|run_webhook|start_webhook)\s*\(",
    # asyncio-based applications
    r"\basyncio\.run\s*\(",
    r"\b(?:loop)\.run_until_complete\s*\(",
    # Common raw Bot API / HTTP polling implementations
    r"\bgetUpdates\b",
    r"\bget_updates\s*\(",
    r"\bwhile\s+True\s*:",
    # Framework entry points (may need library-specific setup)
    r"\b(?:executor\.start_polling|start_polling)\s*\(",
    r"\b(?:uvicorn\.run|hypercorn\.run|web\.run_app)\s*\(",
    r"\b(?:app|server)\.run\s*\(",
    r"\b(?:Flask|FastAPI|Application)\s*\(",
]


def detect_startup_style(code):
    """Return likely startup mode and matched clue; heuristic only."""
    lower = code.lower()
    webhook_clues = (
        "setwebhook", "set_webhook", "run_webhook", "start_webhook",
        "webhookhandler", "webhook_handler", "@app.post", "@app.route",
        "web.run_app", "uvicorn.run", "fastapi(", "flask(__name__)",
    )
    polling_clues = (
        "getupdates", "get_updates", "start_polling", "run_polling",
        "startpolling", "bot.run(", "client.run(", "updater.start_polling",
        "dispatcher.start_polling", "dp.start_polling", "bot.polling(",
        "bot.infinity_polling(", "run_until_disconnected(", "while true:",
    )
    for pattern in STARTUP_PATTERNS:
        if re.search(pattern, code, flags=re.IGNORECASE):
            if any(clue in lower for clue in webhook_clues):
                return ("webhook_or_web_server", pattern)
            if any(clue in lower for clue in polling_clues):
                return ("polling", pattern)
            return ("possible_entrypoint", pattern)
    # Common Python entrypoint: asyncio.run(main()) is useful only when main()
    # actually starts the bot; the detector cannot prove that statically.
    if re.search(r"if\s+__name__\s*==\s*['\"]__main__['\"]", code):
        return ("python_entrypoint_only", "__main__")
    return ("unknown", None)


# =========================================================
# PROCESS CODE
# =========================================================

def process_code(uid, chat_id, bot_token, code):
    try:
        ast.parse(code)
    except SyntaxError as e:
        send_message(MAIN_TOKEN, chat_id,
            f"❌ <b>خطای سینتکس</b>\n\n"
            f"📍 خط {e.lineno}، ستون {e.offset}\n"
            f"🐛 {esc(e.msg)}\n\n"
            f"🔧 متن خط:\n<code>{esc((e.text or '').strip()[:100])}</code>\n\n"
            f"کدت رو اصلاح کن و دوباره بفرست.",
            {"inline_keyboard": [[ibtn("❌ لغو", "cancel")]]},
            parse_mode="HTML")
        return

    existing_code = db_execute("SELECT bot_username, owner_id FROM bots WHERE code=?", 
                               (code,), fetchone=True)
    if existing_code:
        e_uname, e_owner = existing_code
        e_owner_info = get_user_info(e_owner)
        e_name = e_owner_info[0] if e_owner_info else "کاربر دیگر"
        clear_user_state(uid)
        send_message(MAIN_TOKEN, chat_id,
            f"❌ <b>این کد قبلاً استفاده شده است!</b>\n\n"
            f"🤖 ربات فعال با این کد: @{esc(e_uname)}\n"
            f"👤 سازنده: {esc(e_name)}\n\n"
            f"کد شما باید حداقل یک تغییر کوچیک داشته باشه تا بتونید ربات جدیدی بسازید.",
            {"inline_keyboard": [[ibtn("🏠 منوی اصلی", "back")]]},
            parse_mode="HTML")
        return

    startup_mode, startup_clue = detect_startup_style(code)
    if startup_mode == "unknown":
        send_message(MAIN_TOKEN, chat_id,
            "⚠️ <b>الگوی شروع ربات پیدا نشد</b>\n\n"
            "سامانه چندین الگوی رایج را بررسی کرد، اما نتوانست نقطه شروع را تشخیص دهد.\n"
            "اگر کدت با تابع اصلی یا روش خاصی اجرا می‌شود، می‌توانی ادامه بدهی؛ ولی ممکن است ربات فوراً متوقف شود.\n\n"
            "Webhook هم فقط با تشخیص اسم تابع فعال نمی‌شود؛ به آدرس HTTPS و سازگاری خود کتابخانه نیاز دارد.\n\n"
            "ادامه می‌دهی؟",
            {"inline_keyboard": [
                [ibtn("✅ ادامه بده", "force_run")],
                [ibtn("❌ لغو", "cancel")],
            ]}, parse_mode="HTML")
        set_user_state(uid, "waiting_confirmation",
                       json.dumps({"token": bot_token, "code": code}))
        return

    # تشخیص استاتیک قطعی نیست؛ خطاهای import، توکن و API هنگام اجرا مشخص می‌شوند.
    if is_premium(uid):
        ask_runtime_mode(uid, chat_id, bot_token, code)
    else:
        finalize_run(uid, chat_id, bot_token, code, "polling")


def ask_runtime_mode(uid, chat_id, bot_token, code):
    set_user_state(uid, "waiting_runtime_mode", json.dumps({"token": bot_token, "code": code}))
    rows = [[ibtn("🔁 Polling", "runtime:polling")],
            [ibtn("🌐 Webhook (ویژه)", "runtime:webhook")],
            [ibtn("❌ لغو", "cancel")]]
    note = ""
    if not WEBHOOK_BASE_URL:
        note = "\n\n⚠️ آدرس HTTPS هنوز تنظیم نشده؛ Webhook بعد از آماده‌شدن VPS قابل اجراست."
    send_message(MAIN_TOKEN, chat_id,
        "<b>⚙️ روش اجرای ربات را انتخاب کن</b>\n\n"
        "<b>Polling:</b> روش معمول و سازگار با کدهای فعلی.\n"
        "<b>Webhook:</b> فقط برای کد دارای handle_update(update)، روی VPS با HTTPS.\n"
        "برای امکان برگشت به پلن رایگان، کد باید Polling هم داشته باشد." + note,
        {"inline_keyboard": rows}, parse_mode="HTML")


def finalize_run(uid, chat_id, bot_token, code, runtime_mode="polling"):
    try:
        if not is_premium(uid) and get_user_used_uptime(uid) >= MAX_WEEKLY_UPTIME:
            clear_user_state(uid)
            send_message(MAIN_TOKEN, chat_id,
                "⏰ <b>سهمیه رایگان این چرخه تمام شده</b>\n\n"
                "برای اجرای پروژه جدید باید منتظر شارژ دوباره سهمیه بمانی.",
                {"inline_keyboard": [[ibtn("🏠 منوی اصلی", "back")]]},
                parse_mode="HTML")
            return
        if runtime_mode == "webhook":
            if not is_premium(uid):
                send_message(MAIN_TOKEN, chat_id, "❌ Webhook فقط در اشتراک ویژه فعال است.")
                return
            if not WEBHOOK_BASE_URL or not WEBHOOK_BASE_URL.lower().startswith("https://"):
                set_user_state(uid, "waiting_runtime_mode", json.dumps({"token": bot_token, "code": code}))
                send_message(MAIN_TOKEN, chat_id,
                    "❌ برای اجرای Webhook باید VPS و دامنه HTTPS تنظیم شده باشد.\n"
                    "فعلاً Polling را انتخاب کن یا پس از راه‌اندازی VPS دوباره تلاش کن.",
                    {"inline_keyboard": [[ibtn("🔁 اجرای Polling", "retry_polling")], [ibtn("🏠 منو", "back")]]}, parse_mode="HTML")
                return
        code_with_token = inject_token(code, bot_token)
        if runtime_mode == "webhook":
            ok, reason = code_supports_webhook(code_with_token)
            if not ok:
                set_user_state(uid, "waiting_runtime_mode", json.dumps({"token": bot_token, "code": code}))
                send_message(MAIN_TOKEN, chat_id,
                    "❌ این کد برای Webhook آماده نیست.\n\n" + esc(reason) +
                    "\n\nکد را اصلاح کن یا Polling را انتخاب کن.",
                    {"inline_keyboard": [[ibtn("🔁 اجرای Polling", "retry_polling")], [ibtn("❌ لغو", "cancel")]]}, parse_mode="HTML")
                return

        safe_name = bot_token.split(":")[0]
        file_path = os.path.join(CHILDREN_DIR, f"bot_{safe_name}.py")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(code_with_token)

        try:
            ast.parse(code_with_token)
        except SyntaxError as e:
            send_message(MAIN_TOKEN, chat_id,
                f"❌ بعد از جایگزینی توکن خطا داد!\n\n"
                f"<code>{esc(e.msg)}</code> (خط {e.lineno})",
                parse_mode="HTML")
            return

        me = get_me(bot_token)
        if not me:
            send_message(MAIN_TOKEN, chat_id, "❌ توکن الان کار نمی‌کنه.")
            return

        save_bot(bot_token, uid, me.get("username", ""),
                 me.get("first_name", ""), code_with_token, file_path, runtime_mode)

        if runtime_mode == "webhook":
            ok, reason = start_webhook_bot(bot_token, code_with_token, file_path, uid)
            if not ok:
                db_execute("UPDATE bots SET is_running=0, started_at=0, runtime_mode='polling' WHERE token=?", (bot_token,))
                set_user_state(uid, "waiting_runtime_mode", json.dumps({"token": bot_token, "code": code}))
                send_message(MAIN_TOKEN, chat_id, "❌ راه‌اندازی Webhook ناموفق بود.\n\n" + esc(reason),
                    {"inline_keyboard": [[ibtn("🔁 تلاش با Polling", "retry_polling")], [ibtn("🏠 منو", "back")]]}, parse_mode="HTML")
                return
        else:
            run_user_bot(bot_token, code_with_token, file_path, uid)

        clear_user_state(uid)
        send_message(MAIN_TOKEN, chat_id,
            f"<b>🎉 پروژه با موفقیت روشن شد!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"🤖 <b>@{esc(me.get('username', ''))}</b>\n"
            f"🔑 <code>{esc(mask_token(bot_token))}</code>\n"
            f"⏱ روشن از همین الان\n"
            f"🚀 روش اجرا: {'Webhook' if runtime_mode == 'webhook' else 'Polling'}\n\n"
            f"💡 هر وقت خواستی ببینیش: 📊 پروژه‌های من",
            main_menu(uid), parse_mode="HTML")

    except Exception as e:
        send_message(MAIN_TOKEN, chat_id,
            f"❌ خطا در اجرا:\n<code>{esc(str(e)[:300])}</code>",
            parse_mode="HTML")
        import traceback
        traceback.print_exc()


# =========================================================
# CALLBACK HANDLER
# =========================================================

def handle_callback(callback):
    uid = callback.get("from", {}).get("id")
    chat_id = callback.get("message", {}).get("chat", {}).get("id")
    mid = callback.get("message", {}).get("message_id")
    data = callback.get("data", "")
    cid = callback.get("id")

    if not uid: return
    answer_callback(MAIN_TOKEN, cid)

    # ---------- اشتراک ویژه ----------
    if data == "premium":
        show_subscription_plans(chat_id, uid, mid)
        return

    if data == "runtime:polling" or data == "runtime:webhook":
        state = get_user_state(uid)
        if not state or state[0] != "waiting_runtime_mode":
            answer_callback(MAIN_TOKEN, cid, "مرحله انتخاب روش اجرا منقضی شده است.", True)
            return
        try:
            payload = json.loads(state[1]); bot_token = payload["token"]; code = payload["code"]
        except Exception:
            answer_callback(MAIN_TOKEN, cid, "اطلاعات نامعتبر است.", True); return
        mode = "webhook" if data.endswith("webhook") else "polling"
        clear_user_state(uid)
        finalize_run(uid, chat_id, bot_token, code, mode)
        return

    if data == "retry_polling":
        state = get_user_state(uid)
        if state and state[0] == "waiting_runtime_mode":
            try:
                payload = json.loads(state[1]); clear_user_state(uid)
                finalize_run(uid, chat_id, payload["token"], payload["code"], "polling")
            except Exception:
                send_message(MAIN_TOKEN, chat_id, "❌ اطلاعات مرحله قبل در دسترس نیست.")
        else:
            # If creation had already reached the save step, this button refers to the latest bot owned by the user.
            rows = db_execute("SELECT token, code, file_path FROM bots WHERE owner_id=? ORDER BY created_at DESC LIMIT 1", (uid,), fetchone=True)
            if rows:
                token, code, path = rows
                stop_bot_process(token)
                set_bot_runtime_mode(token, "polling")
                run_user_bot(token, code, path, uid)
                send_message(MAIN_TOKEN, chat_id, "✅ ربات با Polling روشن شد.", {"inline_keyboard": [[ibtn("📊 پروژه‌های من", "my_bots")]]})
            else:
                send_message(MAIN_TOKEN, chat_id, "❌ رباتی برای اجرا پیدا نشد.")
        return

    if data == "continue_free":
        if is_premium(uid):
            answer_callback(MAIN_TOKEN, cid, "اشتراک ویژه هنوز فعال است.", True); return
        bots = db_execute("SELECT token, code, file_path FROM bots WHERE owner_id=?", (uid,), fetchall=True) or []
        started, failed = 0, []
        for token, code, path in bots:
            stop_bot_process(token)
            if not code_has_polling_entrypoint(code or ""):
                failed.append(token); continue
            set_bot_runtime_mode(token, "polling")
            run_user_bot(token, code, path, uid); started += 1
        send_message(MAIN_TOKEN, chat_id,
            f"🎁 <b>پلن رایگان فعال شد.</b>\n\n"
            f"🟢 ربات‌های روشن‌شده با Polling: <b>{started}</b>\n"
            f"⏱ سقف اجرا: <b>۹۰ ساعت در هر چرخه ۷روزه</b>\n"
            + (f"\n⚠️ {len(failed)} ربات به Polling سازگار نبود و روشن نشد؛ کدش باید اصلاح شود." if failed else ""),
            {"inline_keyboard": [[ibtn("📊 پروژه‌های من", "my_bots")], [ibtn("💎 تمدید ویژه", "premium")]]}, parse_mode="HTML")
        return

    if data.startswith("mode:"):
        token = data[5:]
        bot = get_bot(token)
        if not bot or bot[1] != uid:
            answer_callback(MAIN_TOKEN, cid, "ربات پیدا نشد.", True); return
        if not is_premium(uid):
            send_message(MAIN_TOKEN, chat_id,
                "🌐 تغییر به Webhook فقط با اشتراک ویژه ممکن است. پلن رایگان از Polling استفاده می‌کند.",
                {"inline_keyboard": [[ibtn("💎 خرید اشتراک", "premium")], [ibtn("🔙 کارت ربات", f"bot:{token}")]]}, parse_mode="HTML")
            return
        kb = {"inline_keyboard": [[ibtn("🔁 Polling", f"setmode:p:{token}")],
                                   [ibtn("🌐 Webhook", f"setmode:w:{token}")],
                                   [ibtn("🔙 کارت ربات", f"bot:{token}")]]}
        edit_message(MAIN_TOKEN, chat_id, mid, "<b>⚙️ روش اجرای ربات</b>\n\nPolling برای اجرای معمولی؛ Webhook برای کد سازگار و VPS دارای HTTPS.", kb, parse_mode="HTML")
        return

    if data.startswith("setmode:"):
        parts = data.split(":", 2)
        if len(parts) != 3: return
        mode, token = parts[1], parts[2]
        mode = {"p": "polling", "w": "webhook"}.get(mode, mode)
        bot = get_bot(token)
        if not bot or bot[1] != uid:
            answer_callback(MAIN_TOKEN, cid, "ربات پیدا نشد.", True); return
        if mode == "webhook":
            if not is_premium(uid):
                answer_callback(MAIN_TOKEN, cid, "فقط پلن ویژه.", True); return
            # Stop polling first so Bale does not receive getUpdates and Webhook at once.
            stop_bot_process(token)
            ok, reason = start_webhook_bot(token, bot[4], bot[5], uid)
            if not ok:
                send_message(MAIN_TOKEN, chat_id, "❌ " + esc(reason), {"inline_keyboard": [[ibtn("🔙 کارت ربات", f"bot:{token}")]]}, parse_mode="HTML")
                return
        elif mode == "polling":
            stop_bot_process(token)
            set_bot_runtime_mode(token, "polling")
            run_user_bot(token, bot[4], bot[5], uid)
        else:
            return
        result = bot_info_card(token, chat_id, uid)
        if result:
            text, kb = result
            edit_message(MAIN_TOKEN, chat_id, mid, text, kb, parse_mode="HTML")
        return

    if data.startswith("buyplan:"):
        try:
            days = int(data.split(":", 1)[1])
        except Exception:
            answer_callback(MAIN_TOKEN, cid, "پلن نامعتبر است.", True)
            return
        if days not in SUBSCRIPTION_PLANS:
            answer_callback(MAIN_TOKEN, cid, "پلن نامعتبر است.", True)
            return
        req_id, status = create_subscription_request(uid, days)
        if status == "pending":
            req = subscription_request_details(req_id)
            existing_days = req[2] if req else days
            existing_price = req[3] if req else SUBSCRIPTION_PLANS[days]
            send_message(MAIN_TOKEN, chat_id,
                f"⏳ <b>درخواست پرداخت قبلی هنوز بررسی نشده.</b>\n\n"
                f"درخواست شماره <b>#{req_id}</b> برای پلن {existing_days} روزه ({format_price(existing_price)}) در انتظار بررسی است.\n"
                f"برای هماهنگی پرداخت به {PAYMENT_CONTACT} پیام بده و شماره درخواست را هم بفرست.",
                {"inline_keyboard": [[ibtn("🔙 بازگشت به پلن‌ها", "premium")]]}, parse_mode="HTML")
            return
        send_message(MAIN_TOKEN, chat_id,
            f"<b>💳 درخواست اشتراک ثبت شد!</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            f"🧾 شماره درخواست: <code>#{req_id}</code>\n"
            f"📅 مدت: <b>{days} روز</b>\n"
            f"💰 مبلغ: <b>{format_price(SUBSCRIPTION_PLANS[days])}</b>\n\n"
            f"حالا برای هماهنگی پرداخت به {PAYMENT_CONTACT} پیام بده و شماره درخواست <code>#{req_id}</code> را بفرست.\n"
            f"اشتراک فقط پس از بررسی و تأیید دستی مالک فعال می‌شود؛ این ربات پرداخت را خودکار تشخیص نمی‌دهد.",
            {"inline_keyboard": [[ibtn("💎 دیدن پلن‌ها", "premium")], [ibtn("🏠 منوی اصلی", "back")]]}, parse_mode="HTML")
        return

    if data == "sub_requests":
        if uid != OWNER_ID:
            answer_callback(MAIN_TOKEN, cid, "فقط مالک دسترسی دارد.", True)
            return
        show_subscription_requests(chat_id, uid, mid)
        return

    if data.startswith("subreq:"):
        if uid != OWNER_ID:
            answer_callback(MAIN_TOKEN, cid, "فقط مالک دسترسی دارد.", True)
            return
        try:
            req_id = int(data.split(":", 1)[1])
        except Exception:
            return
        req = subscription_request_details(req_id)
        if not req or req[4] != "pending":
            answer_callback(MAIN_TOKEN, cid, "درخواست قبلاً بررسی شده یا وجود ندارد.", True)
            show_subscription_requests(chat_id, uid, mid)
            return
        _, user_id, days, price, _, created_at, _, _ = req
        info = get_user_info(user_id)
        name = info[0] if info and info[0] else "نامشخص"
        username = ("@" + info[1]) if info and info[1] else "ندارد"
        active_until, _ = get_subscription(user_id)
        details = (f"<b>💳 بررسی درخواست اشتراک #{req_id}</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                   f"👤 نام: <b>{esc(name)}</b>\n🆔 آیدی: <code>{user_id}</code>\n"
                   f"🔗 یوزرنیم: {esc(username)}\n📅 پلن: <b>{days} روز</b>\n"
                   f"💰 مبلغ: <b>{format_price(price)}</b>\n🕒 ثبت: {fmt_date(created_at)}\n"
                   f"💎 اشتراک فعلی تا: {fmt_date(active_until) if active_until > int(time.time()) else 'فعال نیست'}\n\n"
                   "بعد از اینکه پرداخت را بیرون از ربات بررسی کردی، یکی از گزینه‌ها را بزن.")
        markup = {"inline_keyboard": [
            [ibtn("✅ تأیید پرداخت و فعال‌سازی", f"subapprove:{req_id}")],
            [ibtn("❌ رد درخواست", f"subreject:{req_id}")],
            [ibtn("🔙 فهرست درخواست‌ها", "sub_requests")]
        ]}
        try: edit_message(MAIN_TOKEN, chat_id, mid, details, markup, parse_mode="HTML")
        except Exception: send_message(MAIN_TOKEN, chat_id, details, markup, parse_mode="HTML")
        return

    if data.startswith("subapprove:"):
        if uid != OWNER_ID:
            answer_callback(MAIN_TOKEN, cid, "فقط مالک دسترسی دارد.", True)
            return
        try: req_id = int(data.split(":", 1)[1])
        except Exception: return
        ok, result = approve_subscription_request(req_id, uid)
        if not ok:
            answer_callback(MAIN_TOKEN, cid, result, True)
            show_subscription_requests(chat_id, uid, mid)
            return
        user_id, days, price, expiry = result
        send_message(MAIN_TOKEN, user_id,
            f"🎉 <b>پرداختت تأیید شد و اشتراک ویژه فعال شد!</b>\n\n"
            f"📅 مدت افزوده‌شده: <b>{days} روز</b>\n"
            f"💎 اعتبار اشتراک تا: <b>{fmt_date(expiry)}</b>\n\n"
            "محدودیت ۹۰ ساعت هفتگی تا پایان اشتراک برداشته شد.",
            {"inline_keyboard": [[ibtn("💎 وضعیت اشتراک", "premium")], [ibtn("🏠 منوی اصلی", "back")]]}, parse_mode="HTML")
        answer_callback(MAIN_TOKEN, cid, "پرداخت تأیید و اشتراک فعال شد.", True)
        show_subscription_requests(chat_id, uid, mid)
        return

    if data.startswith("subreject:"):
        if uid != OWNER_ID:
            answer_callback(MAIN_TOKEN, cid, "فقط مالک دسترسی دارد.", True)
            return
        try: req_id = int(data.split(":", 1)[1])
        except Exception: return
        req = subscription_request_details(req_id)
        if not req or req[4] != "pending":
            answer_callback(MAIN_TOKEN, cid, "درخواست قبلاً بررسی شده یا وجود ندارد.", True)
            show_subscription_requests(chat_id, uid, mid)
            return
        user_id = int(req[1])
        ok = reject_subscription_request(req_id, uid)
        if ok:
            send_message(MAIN_TOKEN, user_id,
                f"❌ درخواست اشتراک شماره <code>#{req_id}</code> تأیید نشد.\nاگر فکر می‌کنی اشتباهی شده، با {PAYMENT_CONTACT} هماهنگ کن.",
                parse_mode="HTML")
            answer_callback(MAIN_TOKEN, cid, "درخواست رد شد.", True)
        show_subscription_requests(chat_id, uid, mid)
        return

    # ---------- ادمین پنل ----------
    if data == "admin_panel":
        if uid != OWNER_ID:
            answer_callback(MAIN_TOKEN, cid, "❌ شما مالک نیستید!", True)
            return
        try: edit_message(MAIN_TOKEN, chat_id, mid, "⏳ در حال بارگذاری...")
        except: pass
        show_admin_panel(chat_id, uid)
        return

    if data.startswith("adm_bot:"):
        if uid != OWNER_ID: return
        token = data[8:]
        result = bot_info_card(token, chat_id, uid, is_admin=True)
        if not result:
            answer_callback(MAIN_TOKEN, cid, "❌ ربات پیدا نشد", True)
            return
        text, kb = result
        try: edit_message(MAIN_TOKEN, chat_id, mid, text, kb, parse_mode="HTML")
        except: send_message(MAIN_TOKEN, chat_id, text, kb, parse_mode="HTML")
        return

    if data.startswith("adm_del:"):
        if uid != OWNER_ID: return
        token = data[8:]
        bot = get_bot(token)
        if bot:
            display = f"@{bot[2]}" if bot[2] else (bot[3] or "ربات")
            stop_bot_process(token)
            delete_bot(token)
            send_message(MAIN_TOKEN, chat_id,
                f"<b>✅ ربات با موفقیت توسط مالک حذف شد!</b>\n\n"
                f"🤖 {esc(display)}\n🔴 خاموش شد\n🗑 از دیتابیس حذف شد",
                {"inline_keyboard": [[ibtn("👑 بازگشت به پنل ادمین", "admin_panel")]]}, parse_mode="HTML")
        return

    # ---------- لغو ----------
    if data == "cancel":
        clear_user_state(uid)
        send_message(MAIN_TOKEN, chat_id, main_text(), main_menu(uid), parse_mode="HTML")
        return

    # ---------- ساخت ----------
    if data == "create":
        used = get_user_used_uptime(uid) if not is_premium(uid) else 0
        if not is_premium(uid) and used >= MAX_WEEKLY_UPTIME:
            answer_callback(MAIN_TOKEN, cid, "⏰ سهمیه ۹۰ ساعته این چرخه تمام شده است.", True)
            send_message(MAIN_TOKEN, chat_id,
                "⏰ <b>سهمیه اجرای این چرخه تمام شده</b>\n\n"
                "بعد از شروع چرخه ۷روزه بعدی می‌تونی دوباره پروژه اجرا کنی.",
                {"inline_keyboard": [[ibtn("🔙 منوی اصلی", "back")]]},
                parse_mode="HTML")
            return
        if count_user_bots(uid) >= MAX_BOTS_PER_USER:
            answer_callback(MAIN_TOKEN, cid, f"🛑 محدودیت {MAX_BOTS_PER_USER} پروژه", True)
            return
        set_user_state(uid, "waiting_token")
        try:
            edit_message(MAIN_TOKEN, chat_id, mid,
                f"<b>🔑 مرحله ۱ از ۲</b>\n━━━━━━━━━━━━━━━━━━\n\nتوکن رباتت رو بفرست:\n\n⚠️ از @botfather بگیر\n\nفرمت:\n<code>123456789:ABCdef...</code>",
                {"inline_keyboard": [[ibtn("❌ لغو", "cancel")]]}, parse_mode="HTML")
        except: pass
        return

    # ---------- ادامه اجباری ----------
    if data == "force_run":
        state = get_user_state(uid)
        if not state or state[0] != "waiting_confirmation":
            send_message(MAIN_TOKEN, chat_id, "❌ مرحله منقضی شده.")
            return
        try:
            payload = json.loads(state[1])
            bot_token = payload["token"]
            code = payload["code"]
        except:
            send_message(MAIN_TOKEN, chat_id, "❌ داده معتبر نیست.")
            return
        clear_user_state(uid)
        if is_premium(uid): ask_runtime_mode(uid, chat_id, bot_token, code)
        else: finalize_run(uid, chat_id, bot_token, code, "polling")
        return

    # ---------- ربات‌های من ----------
    if data == "my_bots":
        bots = get_my_bots(uid)
        if not bots:
            try:
                edit_message(MAIN_TOKEN, chat_id, mid,
                    f"<b>📭 هنوز پروژه‌ای نساختی!</b>\n\n💡 اولین رباتت رو بساز:",
                    {"inline_keyboard": [
                        [ibtn("🚀 ساخت اولین پروژه", "create")],
                        [ibtn("🔙 بازگشت", "back")],
                    ]}, parse_mode="HTML")
            except: pass
            return
        
        premium = is_premium(uid)
        uptime = get_user_used_uptime(uid) if not premium else 0
        remaining = max(0, MAX_WEEKLY_UPTIME - uptime)
        if premium:
            quota_text = f"💎 <b>اشتراک ویژه:</b> فعال تا {fmt_date(get_subscription(uid)[0])}\n⏱ <b>محدودیت هفتگی:</b> ندارد\n"
        else:
            quota_text = f"⏱ <b>مصرف چرخه جاری:</b> {fmt_duration(uptime)}\n🟢 <b>زمان باقی‌مانده:</b> {fmt_duration(remaining)}\n"
        text = (
            f"<b>📊 داشبورد پروژه‌های من</b> ({len(bots)}/{MAX_BOTS_PER_USER})\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"{quota_text}"
            f"📦 <b>ظرفیت پروژه:</b> {MAX_BOTS_PER_USER - len(bots)} پروژه باقی‌مانده\n\n"
            f"برای مشاهده وضعیت یا مدیریت پروژه، روی آن بزن:"
        )
        rows = []
        for b in bots:
            token, uname, fname, is_running, created_at, started_at = b
            name = f"@{uname}" if uname else (fname or "ربات")
            icon = "🟢" if is_running else "🔴"
            rows.append([ibtn(f"{icon} {name}", f"bot:{token}")])
        rows.append([ibtn("🚀 ساخت پروژه جدید", "create")])
        rows.append([ibtn("🔙 بازگشت", "back")])
        
        try: edit_message(MAIN_TOKEN, chat_id, mid, text, {"inline_keyboard": rows}, parse_mode="HTML")
        except: send_message(MAIN_TOKEN, chat_id, text, {"inline_keyboard": rows}, parse_mode="HTML")
        return

    # ---------- کارت ربات ----------
    if data.startswith("bot:"):
        token = data[4:]
        result = bot_info_card(token, chat_id, uid, is_admin=False)
        if not result:
            answer_callback(MAIN_TOKEN, cid, "❌ ربات پیدا نشد", True)
            return
        text, kb = result
        try: edit_message(MAIN_TOKEN, chat_id, mid, text, kb, parse_mode="HTML")
        except: send_message(MAIN_TOKEN, chat_id, text, kb, parse_mode="HTML")
        return

    # ---------- دیدن کد ----------
    if data.startswith("code:"):
        token = data[5:]
        bot = get_bot(token)
        if not bot or bot[1] != uid:
            answer_callback(MAIN_TOKEN, cid, "❌ نامعتبر", True)
            return
        code = bot[4] or ""
        if len(code) > MAX_CODE_PREVIEW:
            code = code[:MAX_CODE_PREVIEW] + "\n\n... (کد کوتاه شد)"
        send_message(MAIN_TOKEN, chat_id,
            f"<b>📄 کد در حال اجرا</b>\n━━━━━━━━━━━━━━━━━━\n\n<pre>{esc(code)}</pre>",
            {"inline_keyboard": [[ibtn("🔙 بازگشت", f"bot:{token}")]]}, parse_mode="HTML")
        return

    # ---------- ریستارت ----------
    if data.startswith("restart:"):
        token = data[8:]
        bot = get_bot(token)
        if not bot or bot[1] != uid:
            answer_callback(MAIN_TOKEN, cid, "❌ نامعتبر", True)
            return
        
        answer_callback(MAIN_TOKEN, cid, "🔄 در حال ریستارت...", True)
        runtime_mode = get_bot_runtime_mode(token)
        stop_bot_process(token)
        if runtime_mode == "webhook":
            ok, reason = start_webhook_bot(token, bot[4], bot[5], uid)
            if not ok:
                send_message(MAIN_TOKEN, chat_id, "❌ ریستارت Webhook ناموفق بود: " + esc(reason), parse_mode="HTML")
                return
        else:
            db_execute("UPDATE bots SET started_at=?, is_running=1 WHERE token=?", (int(time.time()), token))
            run_user_bot(token, bot[4], bot[5], uid)
        
        send_message(MAIN_TOKEN, chat_id,
            f"<b>🔄 ربات ریستارت شد!</b>\n\n🤖 @{esc(bot[2] or 'ربات')}\n⏱ روشن از همین الان\n⚙️ روش اجرا: {'Webhook' if runtime_mode == 'webhook' else 'Polling'}",
            {"inline_keyboard": [[ibtn("📊 دیدن کارت", f"bot:{token}")]]}, parse_mode="HTML")
        return

    # ---------- تایید حذف ----------
    if data.startswith("del_confirm:"):
        token = data[12:]
        bot = get_bot(token)
        if not bot or bot[1] != uid:
            answer_callback(MAIN_TOKEN, cid, "❌ نامعتبر", True)
            return
        
        display = f"@{bot[2]}" if bot[2] else (bot[3] or "ربات")
        try:
            edit_message(MAIN_TOKEN, chat_id, mid,
                f"⚠️ <b>تایید حذف</b>\n━━━━━━━━━━━━━━━━━━\n\nآیا مطمئنی می‌خوای <b>{esc(display)}</b> رو حذف کنی؟\n\n❌ این کار برگشت‌پذیر نیست!\n🔴 ربات کاملاً خاموش می‌شه",
                {"inline_keyboard": [
                    [ibtn("✅ بله، حذف کن", f"del:{token}")],
                    [ibtn("❌ لغو", f"bot:{token}")],
                ]}, parse_mode="HTML")
        except: pass
        return

    # ---------- حذف ----------
    if data.startswith("del:"):
        token = data[4:]
        bot = get_bot(token)
        if not bot or bot[1] != uid:
            answer_callback(MAIN_TOKEN, cid, "❌ نامعتبر", True)
            return
        
        display = f"@{bot[2]}" if bot[2] else (bot[3] or "ربات")
        stop_bot_process(token)
        delete_bot(token)
        
        send_message(MAIN_TOKEN, chat_id,
            f"<b>✅ ربات با موفقیت حذف شد!</b>\n━━━━━━━━━━━━━━━━━━\n\n🤖 {esc(display)}\n🔴 کاملاً خاموش شد\n🗑 از لیست پاک شد",
            {"inline_keyboard": [
                [ibtn("📊 بازگشت به لیست", "my_bots")],
                [ibtn("🏠 منوی اصلی", "back")],
            ]}, parse_mode="HTML")
        return

    # ---------- راهنما ----------
    if data == "help":
        text = (
            f"<b>ℹ️ راهنمای سرور</b>\n━━━━━━━━━━━━━━━━━━\n\n"
            f"1️⃣ توکن رباتت رو می‌فرستی\n2️⃣ کد رباتت رو می‌فرستی\n3️⃣ سرور اون رو روشن می‌کنه\n\n"
            f"<b>🎁 پلن رایگان:</b>\n• {MAX_BOTS_PER_USER} پروژه\n• {MAX_WEEKLY_UPTIME // 3600} ساعت در هر چرخه ۷روزه\n\n"
            f"<b>💎 پلن ویژه:</b>\n• حذف محدودیت زمان هفتگی تا پایان اشتراک\n• پرداخت و فعال‌سازی با تأیید دستی مالک\n• Polling یا Webhook برای کدهای سازگار (با VPS و HTTPS)\n\n"
            f"<b>📚 مثال کد:</b>\n"
            f"<pre>from bale import Bot, Message\n\nBOT_TOKEN = \"PUT_HERE\"\nbot = Bot(token=BOT_TOKEN)\n\n@bot.event\nasync def on_message(m: Message):\n    if m.text:\n        await m.reply(m.text)\n\nbot.run()</pre>"
        )
        try: edit_message(MAIN_TOKEN, chat_id, mid, text, {"inline_keyboard": [[ibtn("🔙 بازگشت", "back")]]}, parse_mode="HTML")
        except: pass
        return

    if data == "back":
        clear_user_state(uid)
        try: edit_message(MAIN_TOKEN, chat_id, mid, main_text(), main_menu(uid), parse_mode="HTML")
        except: pass
        return


# =========================================================
# UPDATE ROUTING: مشترک بین Polling و Webhook
# =========================================================

def dispatch_update(u):
    """ارسال هر Update به همان handlerهای موجود؛ در هر دو حالت Polling/Webhook."""
    try:
        if "message" in u:
            handle_message(u["message"])
        elif "callback_query" in u:
            handle_callback(u["callback_query"])
    except Exception as e:
        print(f"HANDLE ERR: {e}")


def main_loop():
    """روش سازگار برای اجرا در Pydroid یا وقتی WEBHOOK_BASE_URL تنظیم نشده است."""
    print("🚀 SERVER BOT MAKER STARTED (POLLING)")
    offset = None
    while True:
        try:
            r = api(MAIN_TOKEN, "getUpdates", {"timeout": POLL_TIMEOUT, "offset": offset or 0})
            if not r.get("ok"):
                time.sleep(5)
                continue
            for u in r.get("result", []):
                upd_id = u.get("update_id")
                if upd_id is not None:
                    offset = upd_id + 1
                dispatch_update(u)
        except Exception as e:
            print(f"LOOP ERR: {e}")
            time.sleep(5)


def start_user_webhook_listener():
    """Listen for updates for premium user-created bots. Main bot stays on getUpdates polling."""
    global webhook_server
    # On Render, start the public HTTP listener even if the external URL is not set yet,
    # so /health can pass. Actual Webhook registration still requires an HTTPS base URL.
    if not WEBHOOK_BASE_URL and not os.environ.get("PORT"):
        print("🌐 User-bot Webhook listener off: no public hosting PORT configured.")
        return False
    if WEBHOOK_BASE_URL and not WEBHOOK_BASE_URL.lower().startswith("https://"):
        print("❌ WEBHOOK_BASE_URL must start with https://")
        return False
    if webhook_server is not None:
        return True

    webhook_path_prefix = "/user-bot/"
    class UserBotWebhookHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            print("USER WEBHOOK HTTP:", fmt % args)
        def do_GET(self):
            if self.path == "/health":
                body = b"ok"; self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            else: self.send_error(404)
        def do_POST(self):
            if not self.path.startswith(webhook_path_prefix):
                self.send_error(404); return
            pieces = self.path.strip("/").split("/")
            if len(pieces) != 3 or pieces[0] != "user-bot" or not secrets.compare_digest(pieces[2], WEBHOOK_SECRET):
                self.send_error(404); return
            token = webhook_id_to_token.get(pieces[1])
            q = active_webhook_queues.get(token) if token else None
            if not token or q is None:
                self.send_error(404); return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 1_000_000:
                    self.send_error(413 if length > 1_000_000 else 400); return
                update = json.loads(self.rfile.read(length).decode("utf-8"))
                if not isinstance(update, dict): self.send_error(400); return
                q.put(update)
                body = b"ok"; self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError): self.send_error(400)
            except Exception as e:
                print("USER WEBHOOK HANDLER ERROR:", e)
                try: self.send_error(500)
                except Exception: pass

    try:
        # Render's proxy can only reach services bound to 0.0.0.0, not loopback.
        webhook_server = ThreadingHTTPServer(("0.0.0.0", WEBHOOK_PORT), UserBotWebhookHandler)
        webhook_server.daemon_threads = True
        threading.Thread(target=webhook_server.serve_forever, daemon=True).start()
        print(f"🌐 User-bot Webhook listener active on 0.0.0.0:{WEBHOOK_PORT}")
        return True
    except OSError as e:
        webhook_server = None
        print("❌ Could not start user Webhook listener:", e)
        return False


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    multiprocessing.freeze_support()
    if not MAIN_TOKEN or MAIN_TOKEN == "توکن_جدید_ربات_اصلی_را_اینجا_قرار_بده":
        raise SystemExit("توکن اصلی را در متغیر محیطی MAIN_TOKEN تنظیم کن یا placeholder را جایگزین کن.")
    print("=" * 50)
    print("BALE BOT MAKER - USER BOT POLLING/WEBHOOK")
    print("=" * 50)
    print(f"👑 Owner ID: {OWNER_ID}")
    print(f"🛑 Max Bots: {MAX_BOTS_PER_USER}")
    print(f"⏳ Free Max Uptime: {MAX_WEEKLY_UPTIME // 3600} hours/week")
    print("💎 Premium subscriptions enabled")
    print("🤖 Main bot runtime: Polling (unchanged)")
    print("🌐 User-bot Webhook:", "configured" if WEBHOOK_BASE_URL else "off until VPS HTTPS is configured")

    # Only user-created bots use Webhook. The bot-maker itself remains on Polling.
    if WEBHOOK_BASE_URL or os.environ.get("PORT"):
        start_user_webhook_listener()
    try:
        api(MAIN_TOKEN, "deleteWebhook", {"drop_pending_updates": "false"})
    except Exception as e:
        print("MAIN BOT WEBHOOK CLEANUP WARNING:", e)

    threading.Thread(target=uptime_checker, daemon=True).start()

    # Restore persisted projects after process restart. Premium expiry is enforced first.
    now = int(time.time())
    old_bots = db_execute("SELECT token, code, file_path, owner_id, runtime_mode, is_running FROM bots", fetchall=True) or []
    for token, code, file_path, owner_id, runtime_mode, was_running in old_bots:
        if not was_running:
            continue
        if runtime_mode == "webhook":
            if not is_premium(owner_id):
                try: api(token, "deleteWebhook", {"drop_pending_updates": "true"})
                except Exception: pass
                db_execute("UPDATE bots SET is_running=0, started_at=0 WHERE token=?", (token,))
                send_message(MAIN_TOKEN, owner_id,
                    "⌛ اشتراک ویژه تمام شده؛ ربات متوقف ماند. تمدید کن یا با پلن رایگان به Polling برگرد.",
                    {"inline_keyboard": [[ibtn("💎 تمدید اشتراک", "premium")], [ibtn("🎁 ادامه رایگان", "continue_free")]]}, parse_mode="HTML")
                continue
            if not WEBHOOK_BASE_URL:
                db_execute("UPDATE bots SET is_running=0, started_at=0 WHERE token=?", (token,))
                send_message(MAIN_TOKEN, owner_id, "⚠️ ربات Webhook متوقف ماند چون WEBHOOK_BASE_URL تنظیم نیست.", parse_mode="HTML")
                continue
            ok, reason = start_webhook_bot(token, code, file_path, owner_id)
            if not ok:
                db_execute("UPDATE bots SET is_running=0, started_at=0 WHERE token=?", (token,))
                send_message(MAIN_TOKEN, owner_id, "❌ بازیابی Webhook ناموفق بود: " + esc(reason), parse_mode="HTML")
        else:
            db_execute("UPDATE bots SET started_at=?, is_running=1 WHERE token=?", (now, token))
            run_user_bot(token, code, file_path, owner_id)
        time.sleep(0.2)

    # The main bot always uses its existing getUpdates polling loop.
    main_loop()
