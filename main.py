import os
import sys
import time
import json
import urllib.parse
import logging
import gc
import requests
from flask import Flask, request, render_template, Response, jsonify

try:
    from instagrapi import Client
    from instagrapi.exceptions import LoginRequired, RateLimitError, ClientError
except ImportError:
    print("instagrapi not found. Run: pip install instagrapi")
    sys.exit(1)

# ─── CONFIG ──────────────────────────────────────────────
SESSION_ID = os.environ.get("SESSION_ID", "")
IG_USERNAME = os.environ.get("IG_USERNAME", "")
IG_PASSWORD = os.environ.get("IG_PASSWORD", "")
DEFAULT_MESSAGE = "𝐒ᴏ𝐍ᴀ 𝐔ʀ𝐅 𝐅ʀᴏᴏ𝐓ɪ 𝐂ʜ𝐔ᴅ𝐀𝐘𝐈 𝐀ʙʜɪ𝐘ᴀɴ 𝐒ʜᴜʀ𝐔 𝐁ʏ 𝐀ʏᴀɴ"
DEFAULT_DELAY = 4
MIN_DELAY = 2
API_TIMEOUT = 60
THREAD_SCAN_LIMIT = 5
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
BATCH_COOLDOWN = int(os.environ.get("BATCH_COOLDOWN", "300"))
ADD_USER_DELAY = int(os.environ.get("ADD_USER_DELAY", "10"))

# ─── LOGGING ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def retry_api_call(func, *args, max_retries=2, **kwargs):
    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except (RateLimitError, ClientError, requests.Timeout, requests.ConnectionError) as e:
            if attempt == max_retries:
                raise
            logger.warning(f"⚠️ API call failed ({e}), retrying {attempt+1}/{max_retries}...")
            time.sleep(5 * (attempt + 1))
    return None

# ─── LOGIN / RE-LOGIN ────────────────────────────────────
def login_session(session_id):
    session_id = decode_session(session_id)
    try:
        cl = Client()
        cl.timeout = API_TIMEOUT
        if IG_USERNAME and IG_PASSWORD:
            cl.login(IG_USERNAME, IG_PASSWORD)
        else:
            cl.login_by_sessionid(session_id)
        return cl
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None

def ensure_valid_session(cl, session_id):
    try:
        cl.user_id   # simple API call
        return cl
    except Exception:
        logger.warning("🔄 Session invalid, re-login...")
        return login_session(session_id)

# ─── FETCH ALL GROUPS (pagination with fast method first) ─
def fetch_all_groups_fast(cl, limit=200):
    """Fast method: direct_threads with high amount."""
    try:
        threads = cl.direct_threads(amount=limit)
        return [str(t.id) for t in threads if t.is_group]
    except Exception as e:
        logger.warning(f"direct_threads failed: {e}")
        return []

def fetch_all_groups_paginated(cl, limit=500):
    """Pagination fallback using private_request."""
    all_ids = []
    cursor = None
    while len(all_ids) < limit:
        try:
            data = {
                "visual_message_return_type": "unseen",
                "thread_message_limit": "1",
                "persistentBadging": "true",
                "limit": "20",
            }
            if cursor:
                data["cursor"] = cursor
            response = cl.private_request("direct_v2/inbox/", data=data)
            inbox = response.get("inbox", {})
            threads = inbox.get("threads", [])
            for t in threads:
                if t.get("is_group") or len(t.get("users", [])) > 1:
                    tid = str(t.get("thread_v2_id") or t.get("thread_id") or t.get("pk"))
                    if tid:
                        all_ids.append(tid)
            next_cursor = inbox.get("oldest_cursor") or inbox.get("next_cursor")
            has_older = inbox.get("has_older", False)
            if not has_older or not next_cursor:
                break
            cursor = next_cursor
            time.sleep(0.5)   # reduced delay
        except Exception as e:
            logger.warning(f"Pagination error: {e}")
            break
    return list(dict.fromkeys(all_ids))

def fetch_all_groups(cl):
    """Combine fast method and pagination fallback."""
    group_ids = fetch_all_groups_fast(cl, limit=200)
    if len(group_ids) < 50:
        paginated = fetch_all_groups_paginated(cl, limit=500)
        group_ids = list(dict.fromkeys(group_ids + paginated))
    return group_ids

# ─── FETCH THREAD ID FOR USERS (for GC Creator) ─────────
def fetch_thread_id_for_users(cl, user_ids):
    try:
        threads = retry_api_call(cl.direct_threads, amount=1)
        if threads and threads[0].is_group:
            t = threads[0]
            t_user_ids = [u.pk for u in t.users]
            if all(uid in t_user_ids for uid in user_ids):
                return str(t.id), "latest_thread"
    except:
        pass
    try:
        threads = retry_api_call(cl.direct_threads, amount=THREAD_SCAN_LIMIT)
        if threads:
            sorted_threads = sorted(threads, key=lambda t: getattr(t, 'last_activity_at', 0), reverse=True)
            for t in sorted_threads:
                if t.is_group:
                    t_user_ids = [u.pk for u in t.users]
                    if all(uid in t_user_ids for uid in user_ids):
                        return str(t.id), "thread_scan"
            for t in sorted_threads:
                if t.is_group:
                    return str(t.id), "most_recent_group"
    except:
        pass
    return None, None

# ─── ADD USER (2 methods + re-login) ────────────────────
def add_user_to_thread(cl, thread_id, user_id, session_id):
    # Method 1: direct_add_user
    try:
        if hasattr(cl, 'direct_add_user'):
            result = retry_api_call(cl.direct_add_user, thread_id, user_id)
            if result is not None:
                return True
    except Exception:
        pass

    # Method 2: private_request
    try:
        url = f"direct_v2/threads/{thread_id}/add_user/"
        data = {"user_id": str(user_id)}
        result = retry_api_call(cl.private_request, url, data)
        if result and result.get("status") == "ok":
            return True
    except Exception:
        pass

    # Re-login and retry once
    logger.warning("Add user failed with current session, attempting re-login...")
    new_cl = ensure_valid_session(cl, session_id)
    if new_cl:
        try:
            if hasattr(new_cl, 'direct_add_user'):
                result = retry_api_call(new_cl.direct_add_user, thread_id, user_id)
                if result is not None:
                    return True
        except:
            pass
        try:
            url = f"direct_v2/threads/{thread_id}/add_user/"
            data = {"user_id": str(user_id)}
            result = retry_api_call(new_cl.private_request, url, data)
            if result and result.get("status") == "ok":
                return True
        except:
            pass
    return False

# ─── REMOVE USER (2 methods + re-login) ─────────────────
def remove_user_from_thread(cl, thread_id, user_id, session_id):
    # Method 1: direct_remove_user
    try:
        if hasattr(cl, 'direct_remove_user'):
            result = retry_api_call(cl.direct_remove_user, thread_id, user_id)
            if result is not None:
                return True
    except:
        pass

    # Method 2: private_request
    try:
        url = f"direct_v2/threads/{thread_id}/remove_users/"
        data = {"user_ids": f"[{user_id}]"}
        result = retry_api_call(cl.private_request, url, data)
        if result and result.get("status") == "ok":
            return True
    except:
        pass

    # Re-login and retry once
    logger.warning("Remove user failed, re-login...")
    new_cl = ensure_valid_session(cl, session_id)
    if new_cl:
        try:
            if hasattr(new_cl, 'direct_remove_user'):
                result = retry_api_call(new_cl.direct_remove_user, thread_id, user_id)
                if result is not None:
                    return True
        except:
            pass
        try:
            url = f"direct_v2/threads/{thread_id}/remove_users/"
            data = {"user_ids": f"[{user_id}]"}
            result = retry_api_call(new_cl.private_request, url, data)
            if result and result.get("status") == "ok":
                return True
        except:
            pass
    return False

# ─── GC CREATOR ENGINE ─────────────────────────────────
def create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay_seconds, batch_size, batch_cooldown):
    yield sse_event("info", f"🚀 Starting GC Creator (delay: {delay_seconds}s)...")
    yield sse_event("info", f"📋 Users: {', '.join(usernames)}")
    yield sse_event("info", f"🧹 Remove: {remove_username}")
    yield sse_event("info", f"🔢 Total: {group_count} GCs")
    if batch_size > 0:
        yield sse_event("info", f"🛑 Batch cooldown: after every {batch_size} groups, wait {batch_cooldown}s")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed.")
        return

    user_ids = []
    for u in usernames:
        try:
            uid = retry_api_call(cl.user_id_from_username, u)
            user_ids.append(uid)
        except Exception as e:
            yield sse_event("error", f"❌ Could not resolve {u}: {e}")
            return

    dummy_user_id = None
    if remove_username:
        try:
            dummy_user_id = retry_api_call(cl.user_id_from_username, remove_username)
            yield sse_event("success", f"✅ {remove_username} resolved")
        except:
            yield sse_event("warn", f"⚠️ Could not resolve dummy {remove_username}")

    yield sse_event("success", f"✅ {len(user_ids)} users resolved")

    for i in range(1, group_count + 1):
        yield sse_event("info", f"🌼 Creating GC {i}/{group_count}...")
        try:
            retry_api_call(cl.direct_send, custom_message, user_ids=user_ids)
            yield sse_event("success", f"📤 Message sent for GC {i}")
        except Exception as e:
            yield sse_event("error", f"❌ GC {i} send error: {e}")
            continue

        time.sleep(2)

        thread_id, method = fetch_thread_id_for_users(cl, user_ids)
        if thread_id:
            yield sse_event("info", f"🔍 Thread found: {thread_id[:10]}... (via {method})")
            if dummy_user_id:
                if remove_user_from_thread(cl, thread_id, dummy_user_id, session_id):
                    yield sse_event("success", f"🧹 Removed: {remove_username}")
                else:
                    yield sse_event("warn", "⚠️ Dummy removal failed")
        else:
            yield sse_event("warn", "⚠️ Thread ID not found")

        gc.collect()

        if batch_size > 0 and i % batch_size == 0 and i < group_count:
            yield sse_event("info", f"🛑 Batch cooldown {batch_cooldown}s...")
            time.sleep(batch_cooldown)
        if i < group_count:
            yield sse_event("info", f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)

    yield sse_event("success", "🎉 All GCs processed!")

# ─── GC ADDER ENGINE ───────────────────────────────────
def add_users_stream(session_id, group_ids, main_users, dummy_users, delay_seconds, batch_size, batch_cooldown, add_user_delay):
    yield sse_event("info", f"🚀 Starting GC Adder (delay: {delay_seconds}s, user delay: {add_user_delay}s)...")
    yield sse_event("info", f"👥 Main users: {', '.join(main_users)}")
    yield sse_event("info", f"🧹 Dummy users: {', '.join(dummy_users)}")
    yield sse_event("info", f"🔢 Total groups selected: {len(group_ids)}")
    if batch_size > 0:
        yield sse_event("info", f"🛑 Batch cooldown: after every {batch_size} groups, wait {batch_cooldown}s")

    cl = login_session(session_id)
    if not cl:
        yield sse_event("error", "❌ Login failed.")
        return

    # Resolve main users
    main_user_ids = []
    for u in main_users:
        try:
            uid = retry_api_call(cl.user_id_from_username, u)
            main_user_ids.append(uid)
        except Exception as e:
            yield sse_event("error", f"❌ Could not resolve main user {u}: {e}")
            return

    # Resolve dummy users
    dummy_user_ids = []
    for u in dummy_users:
        try:
            uid = retry_api_call(cl.user_id_from_username, u)
            dummy_user_ids.append(uid)
        except:
            yield sse_event("warn", f"⚠️ Could not resolve dummy {u}, skipping removal")

    yield sse_event("success", "✅ Users resolved")

    for idx, thread_id in enumerate(group_ids):
        if not thread_id:
            continue

        # Session check before processing group
        cl = ensure_valid_session(cl, session_id)
        if not cl:
            yield sse_event("error", "❌ Session invalid and re-login failed. Stopping.")
            return

        yield sse_event("info", f"🌼 Processing GC {idx+1}/{len(group_ids)} (thread: {thread_id[:10]}...)")

        # Add main users
        for i, uid in enumerate(main_user_ids):
            username = main_users[i]
            yield sse_event("info", f"👤 Adding {username}...")
            if add_user_to_thread(cl, thread_id, uid, session_id):
                yield sse_event("success", f"✅ Added {username}")
            else:
                yield sse_event("error", f"❌ Failed to add {username}")
            # Delay between each user
            if add_user_delay > 0:
                time.sleep(add_user_delay)

        # Remove dummy users
        for i, uid in enumerate(dummy_user_ids):
            username = dummy_users[i]
            yield sse_event("info", f"🧹 Removing dummy {username}...")
            if remove_user_from_thread(cl, thread_id, uid, session_id):
                yield sse_event("success", f"✅ Removed dummy {username}")
            else:
                yield sse_event("warn", f"⚠️ Failed to remove dummy {username}")
            time.sleep(2)

        gc.collect()

        if batch_size > 0 and (idx + 1) % batch_size == 0 and idx + 1 < len(group_ids):
            yield sse_event("info", f"🛑 Batch cooldown {batch_cooldown}s...")
            time.sleep(batch_cooldown)
        if idx < len(group_ids) - 1:
            yield sse_event("info", f"⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)

    yield sse_event("success", "🎉 All groups processed!")

# ─── ROUTES ─────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/", methods=["POST"])
def start():
    mode = request.form.get("mode", "create")
    session_id = request.form.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return "❌ Session ID required.", 400

    if mode == "add":
        group_ids = request.form.get("group_ids", "").split(",") if request.form.get("group_ids") else []
        group_ids = [g.strip() for g in group_ids if g.strip()]
        if not group_ids:
            return "❌ No groups selected.", 400

        main_users_raw = request.form.get("main_users", "").strip()
        main_users = [u.strip() for u in main_users_raw.split(",") if u.strip()]
        if not main_users:
            return "❌ At least one main user required.", 400

        dummy_users_raw = request.form.get("dummy_users", "").strip()
        dummy_users = [u.strip() for u in dummy_users_raw.split(",") if u.strip()]

        delay = float(request.form.get("delay", DEFAULT_DELAY))
        if delay < MIN_DELAY:
            delay = MIN_DELAY

        batch_size = int(request.form.get("batch_size", BATCH_SIZE))
        batch_cooldown = int(request.form.get("batch_cooldown", BATCH_COOLDOWN))
        add_user_delay = int(request.form.get("add_user_delay", ADD_USER_DELAY))

        def generate():
            yield from add_users_stream(session_id, group_ids, main_users, dummy_users, delay, batch_size, batch_cooldown, add_user_delay)

        return Response(generate(), mimetype="text/event-stream")

    else:
        group_count = int(request.form.get("group_count", 10))
        usernames_raw = request.form.get("usernames", "").strip()
        usernames = [u.strip() for u in usernames_raw.split(",") if u.strip()]
        remove_username = request.form.get("remove_username", "").strip()
        custom_message = request.form.get("message", DEFAULT_MESSAGE).strip()

        delay = float(request.form.get("delay", DEFAULT_DELAY))
        if delay < MIN_DELAY:
            delay = MIN_DELAY

        batch_size = int(request.form.get("batch_size", BATCH_SIZE))
        batch_cooldown = int(request.form.get("batch_cooldown", BATCH_COOLDOWN))

        def generate():
            yield from create_gcs_stream(session_id, group_count, usernames, remove_username, custom_message, delay, batch_size, batch_cooldown)

        return Response(generate(), mimetype="text/event-stream")

@app.route("/api/fetch-groups", methods=["POST"])
def fetch_groups_api():
    data = request.json
    session_id = data.get("session_id", "").strip() or SESSION_ID
    if not session_id:
        return jsonify({"success": False, "error": "Session ID required"}), 400
    try:
        cl = login_session(session_id)
        if not cl:
            return jsonify({"success": False, "error": "Login failed"}), 400

        group_ids = fetch_all_groups(cl)
        if not group_ids:
            return jsonify({"success": False, "error": "No groups found"}), 400

        groups = []
        for tid in group_ids:
            try:
                thread = cl.direct_thread(int(tid))
                name = thread.thread_title or tid
            except:
                name = tid
            groups.append({"id": tid, "name": name})

        return jsonify({"success": True, "groups": groups})
    except Exception as e:
        logger.error(f"Fetch groups error: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
