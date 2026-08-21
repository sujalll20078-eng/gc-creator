import os
import sys
import time
import threading
import urllib.parse
import logging
from flask import Flask, request, render_template, Response, stream_with_context
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, RateLimitError

# ─── CONFIG ──────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀ𝐎ᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙ𝐇ɪ𝐘ᴀ𝐍 𝐒ʜ𝐔ʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
CREATE_DELAY = 4
ERROR_DELAY = 2

# ─── LOGGING ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
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
    cl.private.post(
        f"https://i.instagram.com/api/v1/direct_v2/threads/{thread_id}/remove_users/",
        data={"user_ids": f"[{user_id}]"}
    )

# ─── GC CREATION ENGINE (Streams logs) ──────────────────
def create_gcs_engine(session_id, group_count, usernames, remove_username, custom_message, log_callback):
    """
    Main engine that creates GCs and streams logs via callback.
    """
    cl = login_session(session_id)
    if not cl:
        log_callback("❌ Login failed, aborting.")
        return

    try:
        user_ids = [cl.user_id_from_username(u) for u in usernames]
        remove_user_id = cl.user_id_from_username(remove_username)
    except Exception as e:
        log_callback(f"❌ Username resolve failed: {e}")
        return

    log_callback(f"⚡ Starting creation of {group_count} GCs...")

    for i in range(1, group_count + 1):
        try:
            log_callback(f"🌼 GC {i}/{group_count}")

            # Create group & send message
            cl.direct_send(custom_message, user_ids=user_ids)
            time.sleep(CREATE_DELAY)

            # Get the most recent thread (this is the one just created)
            thread = cl.direct_threads(amount=1)[0]
            thread_id = thread.id
            time.sleep(2)

            # Remove dummy user
            remove_user_from_thread(cl, thread_id, remove_user_id)
            time.sleep(1)

            log_callback(f"✅ Added: {', '.join(usernames)} | 🧃 Removed: {remove_username}")

        except LoginRequired:
            log_callback("🔐 Login expired, re-logging...")
            cl = login_session(session_id)
            if not cl:
                log_callback("❌ Re-login failed, aborting.")
                return
        except RateLimitError:
            log_callback("⏳ Rate limited, waiting 10s...")
            time.sleep(10)
        except Exception as e:
            log_callback(f"❌ Error: {e}")
            time.sleep(ERROR_DELAY)

        time.sleep(CREATE_DELAY)  # extra delay between groups

    log_callback("✅ All GCs created successfully!")

# ─── ROUTES ──────────────────────────────────────────────
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("index.html")

    # POST: Start the process
    # Get form data
    session_id = request.form.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return "❌ Session ID required. Set SESSION_ID env or provide in form."

    try:
        group_count = int(request.form.get("group_count", 10))
    except:
        return "❌ Invalid group count."

    usernames_raw = request.form.get("usernames", "").strip()
    usernames = [u.strip() for u in usernames_raw.split(",") if u.strip()]
    if len(usernames) < 2:
        return "❌ At least 2 usernames required."

    remove_username = request.form.get("remove_username", "").strip()
    if not remove_username:
        return "❌ Remove username required."

    custom_message = request.form.get("message", DEFAULT_MESSAGE).strip()
    if not custom_message:
        custom_message = DEFAULT_MESSAGE

    # Stream logs using Server-Sent Events? Use streaming response.
    def generate():
        # Capture log messages and yield as HTML lines
        def log_callback(msg):
            # Send as HTML <div>
            yield f"<div class='log-line'>{msg}</div>\n"

        # Run engine in same thread (blocking, but streaming)
        yield from log_callback("🔄 Starting GC creation...")
        create_gcs_engine(
            session_id,
            group_count,
            usernames,
            remove_username,
            custom_message,
            log_callback
        )
        yield from log_callback("✨ Done.")

    return Response(stream_with_context(generate()), mimetype="text/html")

# ─── MAIN ────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
