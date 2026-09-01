import asyncio
import aiohttp
import json
import os
from flask import Flask, render_template, request, Response, stream_with_context
import threading
import queue

app = Flask(__name__)

API = "https://t10xmehedi-friend-api.vercel.app/add_friend?uid={guest_uid}&password={guest_pass}&friend_uid={target_uid}"
ACC_FILE = os.path.join(os.path.dirname(__file__), "acc.json")


def load_accounts():
    with open(ACC_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


async def add_friend(session, acc, target_uid, q):
    url = API.format(
        guest_uid=acc["uid"],
        guest_pass=acc["password"],
        target_uid=target_uid
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            data = await r.text()
            status_code = r.status
            if status_code == 200:
                q.put({"type": "success", "uid": acc["uid"], "status": status_code, "msg": data})
            else:
                q.put({"type": "fail", "uid": acc["uid"], "status": status_code, "msg": data})
    except Exception as e:
        q.put({"type": "fail", "uid": acc["uid"], "status": "ERR", "msg": str(e)})


async def run_spam(accounts, target_uid, q):
    connector = aiohttp.TCPConnector(limit=500)
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [add_friend(session, acc, target_uid, q) for acc in accounts]
        await asyncio.gather(*tasks)
    q.put(None)  # sentinel


def spam_thread(accounts, target_uid, q):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_spam(accounts, target_uid, q))
    except Exception as e:
        q.put({"type": "fail", "uid": "SYSTEM", "status": "ERR", "msg": f"Fatal error: {e}"})
    finally:
        q.put(None)  # always send sentinel so clients don't hang
        loop.close()


@app.route("/")
def index():
    accounts = load_accounts()
    return render_template("index.html", total=len(accounts))


@app.route("/stream")
def stream():
    target_uid = request.args.get("uid", "").strip()
    if not target_uid:
        return "No UID", 400

    accounts = load_accounts()
    q = queue.Queue()

    t = threading.Thread(target=spam_thread, args=(accounts, target_uid, q), daemon=True)
    t.start()

    def generate():
        success = 0
        fail = 0
        total = len(accounts)
        done = 0

        yield f"data: {json.dumps({'type': 'start', 'total': total, 'uid': target_uid})}\n\n"

        while True:
            item = q.get()
            if item is None:
                yield f"data: {json.dumps({'type': 'done', 'success': success, 'fail': fail, 'total': total})}\n\n"
                break

            done += 1
            if item["type"] == "success":
                success += 1
            else:
                fail += 1

            item["done"] = done
            item["success"] = success
            item["fail"] = fail
            yield f"data: {json.dumps(item)}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*"
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
