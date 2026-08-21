import os
import sys
import time
import json
import urllib.parse
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from flask import Flask, request, render_template, Response, stream_with_context
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, RateLimitError, ClientError

# ─── CONFIG ──────────────────────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀ𝐎ᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙ𝐇ɪ𝐘ᴀ𝐍 𝐒ʜ𝐔ʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
DEFAULT_DELAY = 5
MIN_DELAY = 2
ERROR_DELAY = 2
THREAD_FETCH_TIMEOUT = 8

# ─── LOGGING ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── FLASK APP ────────────────────────────────────────────────────────────
app = Flask(__name__)

# ─── HELPERS ──────────────────────────────────────────────────────────────
def decode_session(session):
    if not session:
        return session
    try:
        return urllib.parse.unquote(session)
    except:
        return session

def login_session(session_id):
    session_id = decode_session(session_id)
    try:
        cl = Client()
        cl.login_by_sessionid(session_id)
        return cl
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

def remove_user_from_thread(cl, thread_id, user_id):
    try:
        cl.private.post(
            f"https://i.instagram.com/api/v1/direct_v2/threads/{thread_id}/remove_users/",
            data={"user_ids": f"[{user_id}]"}
        )
        return True
    except Exception:
        return False

def get_latest_thread(cl, timeout=THREAD_FETCH_TIMEOUT):
    """Fetch latest thread with timeout - never hang."""
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(cl.direct_threads, amount=1)
            threads = future.result(timeout=timeout)
            return threads[0] if threads else None
    except (TimeoutError, Exception):
        return None

# ─── SSE EVENT ──────────────────────────────────────────────────────────
def sse_event(event_type, message):
    timestamp = time.strftime("%H:%M:%S")
    data = json.dumps({"type": event_type, "msg": message, "time": timestamp})
    return f"data: {data}\n\n"

# ─── MAIN ENGINE ──────────────────────────────────────────────────────────
def create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay_seconds):
    yield sse_event("info", f"🚀 Starting GC Creator (delay: {delay_seconds}s)...")
    yield sse_event("info", f"📋 Users: {', '.join(usernames)}")
    yield sse_event("info", f"🧹 Remove: {remove_username}")
    yield sse_event("info", f"🔢 Total: {group_count} GCs")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed!")
        return

    try:
        user_ids = [cl.user_id_from_username(u) for u in usernames]
        remove_id = cl.user_id_from_username(remove_username)
        yield sse_event("success", f"✅ {len(user_ids)} users resolved")
    except Exception as e:
        yield sse_event("error", f"❌ User resolve error: {e}")
        return

    success_count = 0
    fail_count = 0

    for i in range(1, group_count + 1):
        try:
            yield sse_event("info", f"🌼 Creating GC {i}/{group_count}...")

            # 1. Send message = create group
            cl.direct_send(custom_message, user_ids=user_ids)
            time.sleep(2)

            # 2. Get thread (with timeout)
            yield sse_event("info", f"🔍 Fetching thread...")
            thread = get_latest_thread(cl)

            if thread:
                yield sse_event("success", f"✅ GC {i} Created!")
                thread_id = thread.id

                # 3. Remove dummy
                if remove_user_from_thread(cl, thread_id, remove_id):
                    yield sse_event("success", f"🧹 Removed: {remove_username}")
                    success_count += 1
                else:
                    yield sse_event("warn", f"⚠️ GC created but dummy removal failed")
                    success_count += 1  # Group exists, so count as success
            else:
                # Thread not found - but message was sent, so group exists
                yield sse_event("warn", f"⚠️ Could not fetch thread, but GC likely created")
                success_count += 1

        except LoginRequired:
            yield sse_event("warn", "🔐 Login expired, re-logging...")
            cl = login_session(session_id)
            if not cl:
                yield sse_event("error", "❌ Relogin failed!")
                return
            i -= 1  # Retry this GC
            continue

        except RateLimitError:
            yield sse_event("warn", "⏳ Rate limit, waiting 15s...")
            time.sleep(15)
            i -= 1
            continue

        except Exception as e:
            yield sse_event("error", f"❌ Error: {str(e)[:100]}")
            fail_count += 1
            time.sleep(ERROR_DELAY)

        # Delay between GCs
        if i < group_count:
            yield sse_event("info", f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)

    yield sse_event("success", f"🎉 Done! Success: {success_count}, Failed: {fail_count}")
    yield sse_event("info", "✨ Complete")

# ─── ROUTES ──────────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/", methods=["POST"])
def start_gc():
    session_id = request.form.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return "❌ Session ID required", 400

    try:
        group_count = int(request.form.get("group_count", 10))
        if group_count < 1:
            raise ValueError
    except:
        return "❌ Invalid group count", 400

    usernames = [u.strip() for u in request.form.get("usernames", "").split(",") if u.strip()]
    if len(usernames) < 2:
        return "❌ At least 2 usernames required", 400

    remove_username = request.form.get("remove_username", "").strip()
    if not remove_username:
        return "❌ Remove username required", 400

    custom_message = request.form.get("message", DEFAULT_MESSAGE).strip() or DEFAULT_MESSAGE

    try:
        delay = float(request.form.get("delay", DEFAULT_DELAY))
        delay = max(MIN_DELAY, delay)
    except:
        delay = DEFAULT_DELAY

    def generate():
        yield from create_gcs_stream(
            session_id, group_count, usernames,
            remove_username, custom_message, delay
        )

    return Response(generate(), mimetype="text/event-stream")

# ─── MAIN ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
