# csen503-realtime-chat

Real-time client-server messaging system for CSEN 503 (Winter 2026).
One laptop runs the server; each team member's phone opens the chat in its browser over the same Wi-Fi.

## Run

```bash
py -m pip install -r requirements.txt
py -m uvicorn server.main:app --host 0.0.0.0 --port 8000
```

1. Find the laptop's IP: run `ipconfig` and copy the **IPv4 Address** (e.g. `192.168.1.5`).
2. On each phone (same Wi-Fi) open `http://192.168.1.5:8000`, type a name, and join.
3. Live server metrics: `http://192.168.1.5:8000/stats`

If phones can't connect, allow Python through Windows Firewall on **Private** networks.

## Structure

```
server/main.py     FastAPI server: WebSocket chat + /stats metrics
client/index.html  Mobile web client (HTML + JS)
requirements.txt   Python dependencies
```

## Protocol 1: WebSocket + JSON

Every frame is one JSON object with a `type` field (full list at the top of `server/main.py`).

- **No loss:** the client keeps every message in an outbox until the server sends `ack`; on reconnect it resends them.
- **No duplication:** each message carries a unique `id`; the server ignores an `id` it has already stored and just re-acks it.
- **Sync after reconnect:** the client remembers the highest `seq` it has and sends `{"type":"sync","after":seq}` on every reconnect; the server replies with everything it missed.
- **Closed sockets:** a disconnect only removes that user; other connections keep running.

## Team

| Member | Part |
|---|---|
| | |
