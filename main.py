import os
import sys
import time
import json
import urllib.parse
import logging
import concurrent.futures
import gc
from flask import Flask, request, render_template, Response

try:
    from instagrapi import Client
    from instagrapi.exceptions import LoginRequired, RateLimitError, ClientError
except ImportError:
    print("instagrapi not found. Run: pip install instagrapi")
    sys.exit(1)

# ─── CONFIG ──────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀ𝐎ᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙ𝐇ɪ𝐘ᴀ𝐍 𝐒ʜ𝐔ʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
DEFAULT_DELAY = 4
MIN_DELAY = 2
API_TIMEOUT = 15
THREAD_SCAN_LIMIT = 30

# ─── LOGGING ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── FLASK APP ────────────────────────────────────────────
app = Flask(__name__)

# ─── HELPERS ─────────────────────────────────────────────
def decode_session(session):
    if not session: return session
    try: return urllib.parse.unquote(session)
    except: return session

def with_timeout(func, timeout=API_TIMEOUT, default=None, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            return default
        except Exception:
            return default

def sse_event(event_type, message):
    timestamp = time.strftime("%H:%M:%S")
    data = json.dumps({"type": event_type, "msg": message, "time": timestamp})
    return f"data: {data}\n\n"

# ─── LOGIN (FIXED - No timeout argument) ──────────────
def login_session(session_id):
    session_id = decode_session(session_id)
    try:
        cl = Client()  # ✅ NO timeout argument
        cl.login_by_sessionid(session_id)
        return cl
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

# ─── THREAD ID FETCH ──────────────────────────────────
def fetch_thread_id_for_users(cl, user_ids):
    try:
        logger.info("🔍 Scanning last {} threads...".format(THREAD_SCAN_LIMIT))
        threads = with_timeout(cl.direct_threads, API_TIMEOUT, [], amount=THREAD_SCAN_LIMIT)
        if threads:
            for t in threads:
                if not t.is_group:
                    continue
                t_user_ids = []
                if hasattr(t, 'users'):
                    for u in t.users:
                        t_user_ids.append(u.pk if hasattr(u, 'pk') else int(u))
                if all(uid in t_user_ids for uid in user_ids):
                    tid = str(t.id)
                    logger.info(f"✅ Found matching thread: {tid}")
                    return tid, "thread_scan"
            # fallback: most recent group
            for t in threads:
                if t.is_group:
                    tid = str(t.id)
                    logger.info(f"✅ Using most recent group: {tid}")
                    return tid, "most_recent_group"
    except Exception as e:
        logger.warning(f"⚠️ Thread scan failed: {e}")
    return None, None

# ─── DUMMY REMOVAL (MULTI-METHOD) ──────────────────
def remove_dummy_multi_method(cl, thread_id, dummy_user_id):
    if not thread_id or not dummy_user_id:
        return False

    # Method A: direct_remove_user (if available)
    if hasattr(cl, 'direct_remove_user'):
        try:
            result = with_timeout(cl.direct_remove_user, API_TIMEOUT, None, thread_id, dummy_user_id)
            if result is not None:
                logger.info("✅ Removed via direct_remove_user")
                return True
        except Exception as e:
            logger.warning(f"⚠️ direct_remove_user failed: {e}")
    else:
        logger.info("ℹ️ direct_remove_user not available, trying fallback...")

    # Method B: private_request
    try:
        url = f"direct_v2/threads/{thread_id}/remove_users/"
        data = {"user_ids": f"[{dummy_user_id}]"}
        result = with_timeout(cl.private_request, API_TIMEOUT, None, url, data)
        if result and result.get("status") == "ok":
            logger.info("✅ Removed via private_request")
            return True
    except Exception as e:
        logger.warning(f"⚠️ private_request removal failed: {e}")

    # Method C: direct_messages.remove_user_from_group
    try:
        if hasattr(cl, 'direct_messages') and hasattr(cl.direct_messages, 'remove_user_from_group'):
            result = with_timeout(cl.direct_messages.remove_user_from_group, API_TIMEOUT, None, thread_id, dummy_user_id)
            if result is not None:
                logger.info("✅ Removed via direct_messages.remove_user_from_group")
                return True
    except Exception as e:
        logger.warning(f"⚠️ direct_messages removal failed: {e}")

    return False

# ─── MAIN ENGINE ──────────────────────────────────────
def create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay_seconds):
    yield sse_event("info", f"🚀 Starting GC Creator (delay: {delay_seconds}s)...")
    yield sse_event("info", f"📋 Users: {', '.join(usernames)}")
    yield sse_event("info", f"🧹 Remove: {remove_username}")
    yield sse_event("info", f"🔢 Total: {group_count} GCs")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed. Check Session ID.")
        return

    # Resolve user IDs
    user_ids = []
    for u in usernames:
        uid = with_timeout(cl.user_id_from_username, API_TIMEOUT, None, u)
        if uid is None:
            yield sse_event("error", f"❌ Could not resolve {u}")
            return
        user_ids.append(uid)

    dummy_user_id = None
    if remove_username:
        dummy_user_id = with_timeout(cl.user_id_from_username, API_TIMEOUT, None, remove_username)
        if dummy_user_id is None:
            yield sse_event("warn", f"⚠️ Could not resolve {remove_username}, skipping removal")
        else:
            yield sse_event("success", f"✅ {remove_username} resolved")

    yield sse_event("success", f"✅ {len(user_ids)} users resolved")

    for i in range(1, group_count + 1):
        yield sse_event("info", f"🌼 Creating GC {i}/{group_count}...")

        # ─── Step 1: Send message ──────────────────
        try:
            send_result = with_timeout(cl.direct_send, API_TIMEOUT, None, custom_message, user_ids=user_ids)
            if send_result is None:
                yield sse_event("error", f"❌ GC {i} send failed (timeout)")
                continue
            yield sse_event("success", f"📤 Message sent for GC {i}")
        except Exception as e:
            yield sse_event("error", f"❌ GC {i} send error: {e}")
            continue

        # ─── Step 2: Fetch thread ID ──────────────────
        thread_id, method = fetch_thread_id_for_users(cl, user_ids)

        if thread_id:
            yield sse_event("info", f"🔍 Thread found: {thread_id} (via {method})")
            if dummy_user_id:
                removal_success = remove_dummy_multi_method(cl, thread_id, dummy_user_id)
                if removal_success:
                    yield sse_event("success", f"🧹 Removed: {remove_username}")
                else:
                    yield sse_event("warn", f"⚠️ GC {i} created but dummy NOT removed")
            else:
                yield sse_event("warn", "⚠️ No dummy user to remove")
        else:
            yield sse_event("warn", f"⚠️ GC {i} created but thread_id NOT found")

        # Memory cleanup every 5 GCs
        if i % 5 == 0:
            gc.collect()

        if i < group_count:
            yield sse_event("info", f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)

    yield sse_event("success", "🎉 All GCs processed!")

# ─── ROUTES ──────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/", methods=["POST"])
def start_gc():
    session_id = request.form.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return "❌ Session ID required.", 400

    try:
        group_count = int(request.form.get("group_count", 10))
        if group_count < 1:
            raise ValueError
    except:
        return "❌ Invalid group count.", 400

    usernames_raw = request.form.get("usernames", "").strip()
    usernames = [u.strip() for u in usernames_raw.split(",") if u.strip()]
    if len(usernames) < 2:
        return "❌ At least 2 usernames required.", 400

    remove_username = request.form.get("remove_username", "").strip()
    if not remove_username:
        return "❌ Remove username required.", 400

    custom_message = request.form.get("message", DEFAULT_MESSAGE).strip()
    if not custom_message:
        custom_message = DEFAULT_MESSAGE

    try:
        delay = float(request.form.get("delay", DEFAULT_DELAY))
        if delay < MIN_DELAY:
            delay = MIN_DELAY
    except:
        delay = DEFAULT_DELAY

    def generate():
        yield from create_gcs_stream(
            session_id,
            group_count,
            usernames,
            remove_username,
            custom_message,
            delay
        )

    return Response(generate(), mimetype="text/event-stream")

# ─── MAIN ────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
