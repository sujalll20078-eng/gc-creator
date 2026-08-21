import os
import sys
import time
import json
import urllib.parse
import logging
from concurrent.futures import TimeoutError as FuturesTimeoutError
import concurrent.futures
from flask import Flask, request, render_template, Response, stream_with_context
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, RateLimitError

# ─── CONFIG ──────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀ𝐎ᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙ𝐇ɪ𝐘ᴀ𝐍 𝐒ʜ𝐔ʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
DEFAULT_DELAY = 4
MIN_DELAY = 2
ERROR_DELAY = 2
THREAD_FETCH_TIMEOUT = 10  # seconds

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

def login_session(session_id, name_hint=""):
    session_id = decode_session(session_id)
    try:
        cl = Client()
        cl.login_by_sessionid(session_id)
        uname = getattr(cl, "username", None) or name_hint or "unknown"
        logger.info(f"✅ Logged in as {uname}")
        return cl
    except Exception as e:
        logger.error(f"❌ Login failed ({name_hint}): {e}")
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

# ─── FETCH LATEST THREAD WITH TIMEOUT ──────────────────
def fetch_latest_thread_with_timeout(cl, timeout=THREAD_FETCH_TIMEOUT):
    """Fetch the most recent thread with a timeout to prevent hanging."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(cl.direct_threads, amount=1)
        try:
            result = future.result(timeout=timeout)
            return result[0] if result else None
        except (FuturesTimeoutError, concurrent.futures.TimeoutError):
            return None
        except Exception:
            return None

# ─── SSE EVENT GENERATOR ─────────────────────────────────
def sse_event(event_type, message):
    timestamp = time.strftime("%H:%M:%S")
    data = json.dumps({"type": event_type, "msg": message, "time": timestamp})
    return f"data: {data}\n\n"

# ─── GC CREATION ENGINE ──────────────────────────────────
def create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay_seconds):
    yield sse_event("info", f"🚀 Initializing GC Creator (delay: {delay_seconds}s per GC)...")
    yield sse_event("info", f"📋 Target Users: {', '.join(usernames)}")
    yield sse_event("info", f"🧹 Dummy: {remove_username}")
    yield sse_event("info", f"🔢 Total GCs: {group_count}")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed. Check Session ID.")
        return

    try:
        user_ids = [cl.user_id_from_username(u) for u in usernames]
        remove_user_id = cl.user_id_from_username(remove_username)
        yield sse_event("success", f"✅ Resolved {len(user_ids)} users successfully.")
    except Exception as e:
        yield sse_event("error", f"❌ Username resolve failed: {e}")
        return

    for i in range(1, group_count + 1):
        try:
            yield sse_event("info", f"🌼 Creating GC {i}/{group_count}...")

            # Send message (this creates the group)
            cl.direct_send(custom_message, user_ids=user_ids)
            yield sse_event("info", f"📤 Message sent for GC {i}...")
            time.sleep(3)

            # Fetch latest thread with timeout
            yield sse_event("info", f"🔍 Fetching thread for GC {i}...")
            thread = fetch_latest_thread_with_timeout(cl)

            if not thread:
                yield sse_event("warn", f"⚠️ Could not fetch thread for GC {i}, skipping remove.")
                # Still count as success (group created, just dummy not removed)
                yield sse_event("success", f"✅ GC {i} Created (could not remove dummy)")
                time.sleep(delay_seconds)
                continue

            thread_id = thread.id
            yield sse_event("info", f"📋 Thread ID: {thread_id}")

            # Remove dummy user
            remove_success = remove_user_from_thread(cl, thread_id, remove_user_id)
            if remove_success:
                yield sse_event("success", f"✅ GC {i} Created | Added: {', '.join(usernames)} | Removed: {remove_username}")
            else:
                yield sse_event("warn", f"⚠️ GC {i} Created but dummy removal failed")

        except LoginRequired:
            yield sse_event("warn", "🔐 Login expired, re-logging...")
            cl = login_session(session_id)
            if not cl:
                yield sse_event("error", "❌ Re-login failed, aborting.")
                return
        except RateLimitError:
            yield sse_event("warn", "⏳ Rate limited, waiting 10s...")
            time.sleep(10)
        except Exception as e:
            yield sse_event("error", f"❌ Error on GC {i}: {e}")
            time.sleep(ERROR_DELAY)

        # Delay between groups (user controlled)
        if i < group_count:
            yield sse_event("info", f"⏳ Waiting {delay_seconds}s before next GC...")
            time.sleep(delay_seconds)

    yield sse_event("success", "🎉 All GCs created successfully!")
    yield sse_event("info", "✨ Done.")

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
