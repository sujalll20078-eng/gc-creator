import os
import sys
import time
import json
import urllib.parse
import logging
from flask import Flask, request, render_template, Response, stream_with_context
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, RateLimitError, ClientError

# ─── CONFIG ──────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀ𝐎ᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙ𝐇ɪ𝐘ᴀ𝐍 𝐒ʜ𝐔ʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
DEFAULT_DELAY = 5
MIN_DELAY = 2
ERROR_DELAY = 2

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

def login_session(session_id):
    session_id = decode_session(session_id)
    try:
        cl = Client()
        cl.login_by_sessionid(session_id)
        return cl
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

# ─── SSE EVENT GENERATOR ─────────────────────────────────
def sse_event(event_type, message):
    timestamp = time.strftime("%H:%M:%S")
    data = json.dumps({"type": event_type, "msg": message, "time": timestamp})
    return f"data: {data}\n\n"

# ─── HYBRID GC CREATION ──────────────────────────────────
def create_gc_hybrid(cl, user_ids, message, dummy_user_id=None, dummy_username=None):
    """
    Create GC using multiple fallback methods.
    Returns: (success, method_used, thread_id_or_none)
    """
    thread_id = None
    method_used = None

    # ─── METHOD 1: direct_send (creates group + sends message) ───
    try:
        cl.direct_send(message, user_ids=user_ids)
        # Get thread_id from the last message (thread id is returned)
        # Actually instagrapi's direct_send returns thread_id
        # But we can get it from the response if needed
        # Let's just fetch the thread id from user's last message
        # Instead, we'll try to get thread from the API
        method_used = "direct_send"
        # Try to get thread id (optional)
        try:
            threads = cl.direct_threads(amount=5)
            for t in threads:
                # Check if this thread has all the users
                thread_users = [u.pk for u in t.users if hasattr(u, 'pk')]
                if all(u in thread_users for u in user_ids):
                    thread_id = str(t.id)
                    break
        except:
            pass
        return (True, method_used, thread_id)
    except Exception as e:
        logger.warning(f"direct_send failed: {e}")

    # ─── METHOD 2: direct_create_group (pure group creation) ───
    try:
        thread = cl.direct_create_group(user_ids)
        if thread:
            thread_id = str(thread.id)
            # Send message after creating group
            try:
                cl.direct_send(message, thread_ids=[thread_id])
            except:
                pass
            method_used = "direct_create_group"
            return (True, method_used, thread_id)
    except Exception as e:
        logger.warning(f"direct_create_group failed: {e}")

    # ─── METHOD 3: Manual API call (private_request) ───
    try:
        # Create group via private API
        user_ids_str = ",".join(map(str, user_ids))
        url = "direct_v2/create_group_thread/"
        data = {"user_ids": user_ids_str, "text": message}
        result = cl.private_request(url, data=data)
        if result and result.get("status") == "ok":
            thread_id = result.get("thread_id") or result.get("thread_v2_id")
            if not thread_id:
                # Try to get from thread
                try:
                    threads = cl.direct_threads(amount=5)
                    for t in threads:
                        thread_users = [u.pk for u in t.users if hasattr(u, 'pk')]
                        if all(u in thread_users for u in user_ids):
                            thread_id = str(t.id)
                            break
                except:
                    pass
            method_used = "manual_api"
            return (True, method_used, thread_id)
    except Exception as e:
        logger.warning(f"manual_api failed: {e}")

    return (False, None, None)

# ─── DUMMY REMOVAL HYBRID ───────────────────────────────
def remove_dummy_hybrid(cl, thread_id, dummy_user_id, dummy_username="unknown"):
    """
    Remove dummy user using multiple fallback methods.
    Returns: (success, method_used)
    """
    if not thread_id:
        return (False, "no_thread_id")

    # ─── METHOD 1: direct_remove_user ───
    try:
        if hasattr(cl, 'direct_remove_user'):
            cl.direct_remove_user(thread_id, dummy_user_id)
            return (True, "direct_remove_user")
    except Exception as e:
        logger.warning(f"direct_remove_user failed: {e}")

    # ─── METHOD 2: private_request ───
    try:
        url = f"direct_v2/threads/{thread_id}/remove_users/"
        data = {"user_ids": f"[{dummy_user_id}]"}
        result = cl.private_request(url, data=data)
        if result and result.get("status") == "ok":
            return (True, "remove_users_api")
    except Exception as e:
        logger.warning(f"remove_users_api failed: {e}")

    # ─── METHOD 3: Try with thread object ───
    try:
        thread = cl.direct_thread(thread_id)
        if thread and hasattr(thread, 'remove_user'):
            thread.remove_user(dummy_user_id)
            return (True, "thread_remove_user")
    except Exception as e:
        logger.warning(f"thread_remove_user failed: {e}")

    # ─── METHOD 4: Try leaving the group (if dummy is the only one) ───
    # Not applicable here, just log and move on

    return (False, "all_failed")

# ─── GC CREATION STREAM ──────────────────────────────────
def create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay_seconds):
    yield sse_event("info", f"🚀 Starting GC Creator (delay: {delay_seconds}s)...")
    yield sse_event("info", f"📋 Users: {', '.join(usernames)}")
    yield sse_event("info", f"🧹 Remove: {remove_username}")
    yield sse_event("info", f"🔢 Total: {group_count} GCs")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed.")
        return

    try:
        user_ids = [cl.user_id_from_username(u) for u in usernames]
        remove_user_id = cl.user_id_from_username(remove_username)
        yield sse_event("success", f"✅ {len(user_ids)} users resolved")
    except Exception as e:
        yield sse_event("error", f"❌ Resolve failed: {e}")
        return

    for i in range(1, group_count + 1):
        try:
            yield sse_event("info", f"🌼 Creating GC {i}/{group_count}...")

            # ─── CREATE GC WITH FALLBACK ───
            success, method, thread_id = create_gc_hybrid(
                cl, user_ids, custom_message, remove_user_id, remove_username
            )

            if success:
                yield sse_event("success", f"✅ GC {i} Created! (method: {method})")
                # Log method used
                logger.info(f"GC {i} created using: {method}")

                # ─── REMOVE DUMMY WITH FALLBACK ───
                if thread_id and remove_user_id:
                    remove_success, remove_method = remove_dummy_hybrid(
                        cl, thread_id, remove_user_id, remove_username
                    )
                    if remove_success:
                        yield sse_event("success", f"🧹 Removed: {remove_username} (method: {remove_method})")
                        logger.info(f"GC {i} dummy removed using: {remove_method}")
                    else:
                        yield sse_event("warn", f"⚠️ Could not remove {remove_username} (all methods failed)")
                        logger.warning(f"GC {i} dummy removal failed")
                else:
                    yield sse_event("warn", f"⚠️ No thread_id, dummy not removed")
                    logger.warning(f"GC {i} no thread_id for dummy removal")
            else:
                yield sse_event("error", f"❌ GC {i} creation failed (all methods)")
                logger.error(f"GC {i} creation failed - all methods")

            # ─── DELAY ───
            if i < group_count:
                yield sse_event("info", f"⏳ Waiting {delay_seconds}s...")
                time.sleep(delay_seconds)

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
            yield sse_event("error", f"❌ Unexpected error: {e}")
            logger.error(f"Unexpected error on GC {i}: {e}")
            time.sleep(ERROR_DELAY)

    yield sse_event("success", "🎉 All GCs processed!")
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
