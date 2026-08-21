import os
import sys
import time
import json
import urllib.parse
import logging
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
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀ𝐎ᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙʜɪ𝐘ᴀ𝐍 𝐒ʜ𝐔ʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
DEFAULT_DELAY = 4
MIN_DELAY = 2
API_TIMEOUT = 10          # seconds for instagrapi client timeout
THREAD_SCAN_LIMIT = 5     # reduced from 30

# ─── LOGGING ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── FLASK APP ────────────────────────────────────────────
app = Flask(__name__)

# ─── HELPERS ─────────────────────────────────────────────
def decode_session(session):
    if not session:
        return session
    try:
        return urllib.parse.unquote(session)
    except:
        return session

def sse_event(event_type, message):
    timestamp = time.strftime("%H:%M:%S")
    data = json.dumps({"type": event_type, "msg": message, "time": timestamp})
    return f"data: {data}\n\n"

# ─── LOGIN ──────────────────────────────────────────────
def login_session(session_id):
    session_id = decode_session(session_id)
    try:
        cl = Client()
        # Set global timeout for all API calls (prevents hanging)
        cl.timeout = API_TIMEOUT
        cl.login_by_sessionid(session_id)
        return cl
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

# ─── THREAD ID FETCH (OPTIMIZED) ──────────────────────
def fetch_thread_id_for_users(cl, user_ids):
    """
    Try to find the thread ID for the given users.
    First try latest thread, then scan a few recent threads.
    """
    # ─── Method 1: Latest thread (amount=1) ──────────
    try:
        logger.info("🔍 Checking latest thread...")
        threads = cl.direct_threads(amount=1)
        if threads and len(threads) > 0:
            t = threads[0]
            if t.is_group:
                t_user_ids = []
                if hasattr(t, 'users'):
                    for u in t.users:
                        t_user_ids.append(u.pk if hasattr(u, 'pk') else int(u))
                if all(uid in t_user_ids for uid in user_ids):
                    tid = str(t.id)
                    logger.info(f"✅ Latest thread matches: {tid}")
                    return tid, "latest_thread"
                else:
                    logger.info("ℹ️ Latest thread is not the group we created, scanning more...")
    except Exception as e:
        logger.warning(f"⚠️ Latest thread fetch failed: {e}")

    # ─── Method 2: Scan recent threads (limit=5) ──────
    try:
        logger.info(f"🔍 Scanning last {THREAD_SCAN_LIMIT} threads...")
        threads = cl.direct_threads(amount=THREAD_SCAN_LIMIT)
        if threads:
            # Sort by last activity (most recent first)
            try:
                sorted_threads = sorted(threads, key=lambda t: getattr(t, 'last_activity_at', 0), reverse=True)
            except:
                sorted_threads = threads

            for t in sorted_threads:
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

            # If exact match not found, take the most recent group thread
            for t in sorted_threads:
                if t.is_group:
                    tid = str(t.id)
                    logger.info(f"✅ Using most recent group thread: {tid}")
                    return tid, "most_recent_group"
    except Exception as e:
        logger.warning(f"⚠️ Thread scan failed: {e}")

    return None, None

# ─── DUMMY REMOVAL (MULTI-METHOD) ──────────────────────
def remove_dummy_multi_method(cl, thread_id, dummy_user_id):
    if not thread_id or not dummy_user_id:
        return False

    # Method A: direct_remove_user (if available)
    try:
        if hasattr(cl, 'direct_remove_user'):
            result = cl.direct_remove_user(thread_id, dummy_user_id)
            if result is not None:
                logger.info("✅ Removed via direct_remove_user")
                return True
        else:
            logger.info("ℹ️ direct_remove_user not available, trying fallback...")
    except Exception as e:
        logger.warning(f"⚠️ direct_remove_user failed: {e}")

    # Method B: private_request
    try:
        url = f"direct_v2/threads/{thread_id}/remove_users/"
        data = {"user_ids": f"[{dummy_user_id}]"}
        result = cl.private_request(url, data)
        if result and result.get("status") == "ok":
            logger.info("✅ Removed via private_request")
            return True
    except Exception as e:
        logger.warning(f"⚠️ private_request removal failed: {e}")

    # Method C: direct_messages.remove_user_from_group
    try:
        if hasattr(cl, 'direct_messages') and hasattr(cl.direct_messages, 'remove_user_from_group'):
            result = cl.direct_messages.remove_user_from_group(thread_id, dummy_user_id)
            if result is not None:
                logger.info("✅ Removed via direct_messages.remove_user_from_group")
                return True
    except Exception as e:
        logger.warning(f"⚠️ direct_messages removal failed: {e}")

    return False

# ─── MAIN ENGINE ────────────────────────────────────────
def create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay_seconds):
    yield sse_event("info", f"🚀 Starting GC Creator (delay: {delay_seconds}s)...")
    yield sse_event("info", f"📋 Users: {', '.join(usernames)}")
    yield sse_event("info", f"🧹 Remove: {remove_username}")
    yield sse_event("info", f"🔢 Total: {group_count} GCs")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed.")
        return

    # Resolve user IDs
    user_ids = []
    for u in usernames:
        try:
            uid = cl.user_id_from_username(u)
            user_ids.append(uid)
        except Exception as e:
            yield sse_event("error", f"❌ Could not resolve {u}: {e}")
            return

    dummy_user_id = None
    if remove_username:
        try:
            dummy_user_id = cl.user_id_from_username(remove_username)
            yield sse_event("success", f"✅ {remove_username} resolved")
        except Exception as e:
            yield sse_event("warn", f"⚠️ Could not resolve {remove_username}, skipping removal")

    yield sse_event("success", f"✅ {len(user_ids)} users resolved")

    for i in range(1, group_count + 1):
        yield sse_event("info", f"🌼 Creating GC {i}/{group_count}...")

        # ─── Step 1: Send message (creates group) ──────
        try:
            send_result = cl.direct_send(custom_message, user_ids=user_ids)
            yield sse_event("success", f"📤 Message sent for GC {i}")
        except Exception as e:
            yield sse_event("error", f"❌ GC {i} send error: {e}")
            continue

        # Small wait for Instagram to register the group
        time.sleep(2)

        # ─── Step 2: Fetch thread ID ────────────────────
        thread_id, method = fetch_thread_id_for_users(cl, user_ids)

        if thread_id:
            yield sse_event("info", f"🔍 Thread found: {thread_id[:10]}... (via {method})")

            # ─── Step 3: Remove dummy ──────────────────
            if dummy_user_id:
                removal_success = remove_dummy_multi_method(cl, thread_id, dummy_user_id)
                if removal_success:
                    yield sse_event("success", f"🧹 Removed: {remove_username}")
                else:
                    yield sse_event("warn", f"⚠️ GC {i} created but dummy NOT removed (all removal methods failed)")
            else:
                yield sse_event("warn", f"⚠️ No dummy user to remove")
        else:
            # Thread ID not found – still count as success (group exists)
            yield sse_event("warn", f"⚠️ GC {i} created but thread_id NOT found (dummy not removed)")
            yield sse_event("info", "💡 Group exists, skipping removal (not mandatory)")

        # ─── Step 4: Memory cleanup ────────────────────
        gc.collect()

        # ─── Step 5: Delay ──────────────────────────────
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
