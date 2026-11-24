"""
CSEN 503 - Real-time chat server
Protocol 1: WebSocket (TCP) carrying JSON messages.

Run from the project root:
    py -m uvicorn server.main:app --host 0.0.0.0 --port 8000

Then open  http://<laptop-IP>:8000  on each phone (same Wi-Fi).

Message format (application-layer protocol, every message is one JSON object):
    client -> server
        {"type": "sync", "after": <last seq the client has>}
        {"type": "chat", "id": "<unique id>", "to": "<username>", "text": "..."}
    server -> client
        {"type": "users",   "online": [...]}                 who is connected now
        {"type": "history", "messages": [msg, ...]}          reply to "sync"
        {"type": "ack",     "message": msg}                  server stored your message
        {"type": "chat",    "message": msg}                  new message for you
        {"type": "error",   "reason": "..."}                 malformed message
    msg = {"id", "seq", "from", "to", "text", "ts"}
"""

import json
import time
from collections import deque
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

app = FastAPI()
CLIENT_DIR = Path(__file__).resolve().parent.parent / "client"

# ---------------------------------------------------------------- state
messages: list[dict] = []          # every chat message, ordered by seq (in memory for now)
seen_ids: set[str] = set()         # ids already stored -> a resent message is not stored twice
clients: dict[str, WebSocket] = {} # username -> its open WebSocket

stats = {
    "started": time.time(),
    "messages_in": 0,     # frames received from clients
    "messages_out": 0,    # frames sent to clients
    "bytes_in": 0,
    "bytes_out": 0,
}
recent_in: deque[float] = deque()  # timestamps of received frames, for the rate


# ---------------------------------------------------------------- helpers
async def send(ws: WebSocket, data: dict) -> bool:
    """Send one JSON frame. Returns False if the socket is already dead."""
    text = json.dumps(data)
    try:
        await ws.send_text(text)
    except Exception:
        return False
    stats["messages_out"] += 1
    stats["bytes_out"] += len(text.encode())
    return True


async def broadcast_users() -> None:
    """Tell every connected client who is online."""
    payload = {"type": "users", "online": sorted(clients)}
    for ws in list(clients.values()):
        await send(ws, payload)


def history_for(user: str, after: int) -> list[dict]:
    """All messages sent by or to `user` with seq greater than `after`."""
    return [m for m in messages if m["seq"] > after and user in (m["from"], m["to"])]


# ---------------------------------------------------------------- HTTP routes
@app.get("/")
async def index():
    return FileResponse(CLIENT_DIR / "index.html")


@app.get("/stats")
async def get_stats():
    """Real-time metrics for monitoring and debugging."""
    now = time.time()
    while recent_in and now - recent_in[0] > 10:
        recent_in.popleft()
    return {
        "active_connections": len(clients),
        "online_users": sorted(clients),
        "stored_messages": len(messages),
        "messages_in": stats["messages_in"],
        "messages_out": stats["messages_out"],
        "bytes_in": stats["bytes_in"],
        "bytes_out": stats["bytes_out"],
        "rate_in_msgs_per_sec_last_10s": round(len(recent_in) / 10, 2),
        "uptime_sec": round(now - stats["started"]),
    }


# ---------------------------------------------------------------- WebSocket
@app.websocket("/ws/{username}")
async def chat_socket(ws: WebSocket, username: str):
    await ws.accept()
    old = clients.get(username)
    clients[username] = ws
    if old is not None:            # same user opened a second tab: keep only the newest
        try:
            await old.close(code=4000)  # 4000 = replaced, tells the old tab not to reconnect
        except Exception:
            pass
    print(f"[+] {username} connected ({len(clients)} online)")
    await broadcast_users()

    try:
        while True:
            raw = await ws.receive_text()
            stats["messages_in"] += 1
            stats["bytes_in"] += len(raw.encode())
            recent_in.append(time.time())
            await handle(username, ws, raw)
    except WebSocketDisconnect:
        pass
    except Exception as e:         # never let one bad client crash the server
        print(f"[!] {username}: {e}")
    finally:
        if clients.get(username) is ws:
            del clients[username]
        print(f"[-] {username} disconnected ({len(clients)} online)")
        await broadcast_users()


async def handle(user: str, ws: WebSocket, raw: str) -> None:
    """Parse and act on one frame from `user`."""
    try:
        data = json.loads(raw)
        kind = data["type"]
    except (ValueError, KeyError, TypeError):
        await send(ws, {"type": "error", "reason": "frame must be JSON with a 'type'"})
        return

    if kind == "sync":
        after = int(data.get("after", 0))
        await send(ws, {"type": "history", "messages": history_for(user, after)})

    elif kind == "chat":
        msg_id, to, text = data.get("id"), data.get("to"), data.get("text")
        if not (isinstance(msg_id, str) and isinstance(to, str) and isinstance(text, str) and text.strip()):
            await send(ws, {"type": "error", "reason": "chat needs string id, to, text"})
            return

        if msg_id in seen_ids:     # client resent after a reconnect -> just ack again
            stored = next(m for m in messages if m["id"] == msg_id)
            await send(ws, {"type": "ack", "message": stored})
            return

        msg = {
            "id": msg_id,
            "seq": len(messages) + 1,
            "from": user,
            "to": to,
            "text": text[:2000],
            "ts": time.time(),
        }
        messages.append(msg)
        seen_ids.add(msg_id)

        await send(ws, {"type": "ack", "message": msg})
        target = clients.get(to)
        if target is not None and to != user:
            await send(target, {"type": "chat", "message": msg})
        # if the receiver is offline, it gets the message from "sync" when it reconnects

    else:
        await send(ws, {"type": "error", "reason": f"unknown type '{kind}'"})
