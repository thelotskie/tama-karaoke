#!/usr/bin/env python3
"""
Tama Karaoke host server.

Serves the karaoke web app, proxies YouTube searches (so the API key never
leaves this machine), and relays the remote-control protocol over WebSocket.

Run:  python server.py
Then open:  http://localhost:8765   (the page shows a QR for phone remotes)

Needs: Python 3 only. No pip packages. A YouTube Data API v3 key goes in
yt_key.txt next to this file (see SETUP below).
"""
import base64
import hashlib
import json
import os
import socket
import struct
import threading
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8765"))
BASE = os.path.dirname(os.path.abspath(__file__))
WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

def public_ws_url():
    """The ws(s) URL remote apps pair with. On Render use the public host."""
    host = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
    if host:
        return "wss://%s/ws" % host
    return "ws://%s:%d/ws" % (lan_ip(), PORT)

# ---------------------------------------------------------------- key / net

def load_key():
    """Read the YouTube API key from env (cloud) or yt_key.txt (local PC).

    Never served to clients."""
    env_key = (os.environ.get("YT_API_KEY") or "").strip()
    if env_key:
        return env_key
    try:
        with open(os.path.join(BASE, "yt_key.txt"), encoding="utf-8") as f:
            key = f.read().strip()
        if key and not key.startswith("PASTE_"):
            return key
    except OSError:
        pass
    return None

def lan_ip():
    """Best guess at this machine's LAN address for the pairing QR."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "127.0.0.1"

# ------------------------------------------------------------- shared state

state_lock = threading.Lock()
clients = []          # list of dicts: {sock, role, name, send_lock}
queue = []            # list of dicts: {id, videoId, title, channel, thumb, by}
queue_seq = 0

def broadcast(obj, roles=None):
    """Send a JSON message to connected clients (optionally filtered by role)."""
    data = json.dumps(obj)
    dead = []
    with state_lock:
        targets = [c for c in clients if roles is None or c["role"] in roles]
    for c in targets:
        try:
            ws_send(c, data)
        except OSError:
            dead.append(c)
    for c in dead:
        drop_client(c)

def broadcast_queue():
    with state_lock:
        items = [dict(i) for i in queue]
    broadcast({"t": "queue", "items": items})

def drop_client(c):
    with state_lock:
        if c in clients:
            clients.remove(c)
    try:
        c["sock"].close()
    except OSError:
        pass

# ------------------------------------------------------------ websocket I/O

def ws_send(c, text):
    """Send one unmasked text frame."""
    raw = text.encode("utf-8")
    header = bytes([0x81])
    n = len(raw)
    if n < 126:
        header += bytes([n])
    elif n < 65536:
        header += bytes([126]) + struct.pack(">H", n)
    else:
        header += bytes([127]) + struct.pack(">Q", n)
    with c["send_lock"]:
        c["sock"].sendall(header + raw)

def ws_read_frame(f):
    """Read one client frame. Returns (opcode, payload bytes) or (None, None)."""
    head = f.read(2)
    if len(head) < 2:
        return None, None
    b1, b2 = head[0], head[1]
    opcode = b1 & 0x0F
    masked = b2 & 0x80
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack(">H", f.read(2))[0]
    elif length == 127:
        length = struct.unpack(">Q", f.read(8))[0]
    mask = f.read(4) if masked else b"\x00\x00\x00\x00"
    payload = b""
    while len(payload) < length:
        chunk = f.read(length - len(payload))
        if not chunk:
            return None, None
        payload += chunk
    if masked:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload

def ws_ping_all():
    """Keep-alive ticker; drops clients that stop answering."""
    while True:
        threading.Event().wait(25)
        dead = []
        with state_lock:
            targets = list(clients)
        for c in targets:
            try:
                with c["send_lock"]:
                    c["sock"].sendall(bytes([0x89, 0x00]))  # ping, empty
            except OSError:
                dead.append(c)
        for c in dead:
            drop_client(c)

# ------------------------------------------------------- message handling

def handle_message(c, msg):
    global queue_seq
    t = msg.get("t")

    if t == "search":
        q = (msg.get("q") or "").strip()
        req = msg.get("req")
        items, err = youtube_search(q)
        reply = {"t": "results", "req": req, "items": items}
        if err:
            reply["error"] = err
        try:
            ws_send(c, json.dumps(reply))
        except OSError:
            drop_client(c)

    elif t == "reserve":
        with state_lock:
            queue_seq += 1
            queue.append({
                "id": "q%d" % queue_seq,
                "videoId": msg.get("videoId", ""),
                "title": msg.get("title", "Unknown"),
                "channel": msg.get("channel", ""),
                "thumb": msg.get("thumb", ""),
                "by": c.get("name") or msg.get("by") or "screen",
            })
        broadcast_queue()

    elif t == "cancel":
        rid = msg.get("id")
        with state_lock:
            queue[:] = [i for i in queue if i["id"] != rid]
        broadcast_queue()

    elif t == "control":
        # forwarded to screen(s) only; server also advances the queue on next
        if msg.get("action") == "next":
            with state_lock:
                if queue:
                    queue.pop(0)
            broadcast_queue()
        else:
            broadcast({"t": "control", "action": msg.get("action")}, roles=("screen",))

    elif t == "ended":
        rid = msg.get("id")
        with state_lock:
            if queue and queue[0]["id"] == rid:
                queue.pop(0)
            else:
                queue[:] = [i for i in queue if i["id"] != rid]
        broadcast_queue()

    elif t == "now":
        broadcast({"t": "now", "videoId": msg.get("videoId", ""),
                   "title": msg.get("title", ""),
                   "state": msg.get("state", "")}, roles=("remote",))

# ------------------------------------------------------------- youtube api

def youtube_search(q):
    """Search YouTube for '<q> karaoke'. Returns (items, error)."""
    key = load_key()
    if not key:
        return [], "no_key"
    if not q:
        return [], None
    params = urllib.parse.urlencode({
        "part": "snippet",
        "type": "video",
        "videoEmbeddable": "true",
        "maxResults": 15,
        "order": "relevance",
        "q": q + " karaoke",
        "key": key,
    })
    url = "https://www.googleapis.com/youtube/v3/search?" + params
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            data = json.load(r)
    except Exception as e:  # bad key, quota out, offline...
        return [], "youtube_error: %s" % e
    items = []
    for it in data.get("items", []):
        vid = (it.get("id") or {}).get("videoId")
        sn = it.get("snippet") or {}
        if not vid:
            continue
        thumbs = sn.get("thumbnails") or {}
        thumb = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
        items.append({
            "videoId": vid,
            "title": sn.get("title", ""),
            "channel": sn.get("channelTitle", ""),
            "thumb": thumb,
        })
    return items, None

# ------------------------------------------------------------- http server

FORBIDDEN = {"server.py", "yt_key.txt", "PROTOCOL.md"}
MIME = {".html": "text/html; charset=utf-8", ".js": "text/javascript",
        ".css": "text/css", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".ico": "image/x-icon", ".json": "application/json"}

class Handler(BaseHTTPRequestHandler):
    server_version = "TamaKaraoke/1.0"

    def log_message(self, *a):
        pass  # keep the console quiet

    def _path(self):
        p = urllib.parse.urlparse(self.path).path
        p = urllib.parse.unquote(p)
        return os.path.normpath(p).lstrip("/")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        # --- websocket upgrade ---
        if parsed.path == "/ws":
            self.handle_ws()
            return

        # --- youtube search proxy ---
        if parsed.path == "/api/search":
            q = urllib.parse.parse_qs(parsed.query).get("q", [""])[0]
            items, err = youtube_search(q)
            body = json.dumps({"items": items, "error": err}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- static files ---
        rel = self._path()
        if rel in FORBIDDEN or rel.startswith(".") or ".." in rel:
            self.send_error(403)
            return
        if rel == "" or rel == "index.html":
            rel = "index.html"
        full = os.path.join(BASE, rel)
        if not os.path.isfile(full):
            self.send_error(404)
            return
        with open(full, "rb") as f:
            body = f.read()
        if rel == "index.html":
            # inject the pairing URL: public wss:// on Render, LAN ws:// at home
            body = body.replace(b"__WS_URL__", public_ws_url().encode("utf-8"))
        ext = os.path.splitext(rel)[1].lower()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_ws(self):
        """WebSocket handshake, then run the socket session on this thread."""
        if self.headers.get("Upgrade", "").lower() != "websocket":
            self.send_error(400)
            return
        key = self.headers.get("Sec-WebSocket-Key", "")
        accept = base64.b64encode(
            hashlib.sha1((key + WS_MAGIC).encode("utf-8")).digest()).decode()
        # raw write: the buffered wfile must not interleave with WS frames
        self.request.sendall(
            ("HTTP/1.1 101 Switching Protocols\r\n"
             "Upgrade: websocket\r\n"
             "Connection: Upgrade\r\n"
             "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode("latin-1"))
        c = {"sock": self.request, "role": "screen",
             "name": "", "send_lock": threading.Lock()}
        with state_lock:
            clients.append(c)
        ws_hello_then_loop(c)   # blocks until the socket closes
        self.close_connection = True

def ws_hello_then_loop(c):
    """Read the hello message to learn the role, then run the main loop."""
    f = c["sock"].makefile("rb")
    c["_file"] = f
    try:
        opcode, payload = ws_read_frame(f)
        if opcode == 0x1:
            try:
                hello = json.loads(payload.decode("utf-8"))
            except ValueError:
                hello = {}
            if hello.get("t") == "hello":
                c["role"] = hello.get("role", "screen")
                c["name"] = hello.get("name", "")
        # send current queue state to the newcomer
        with state_lock:
            items = [dict(i) for i in queue]
        try:
            ws_send(c, json.dumps({"t": "queue", "items": items}))
        except OSError:
            drop_client(c)
            return
    except OSError:
        drop_client(c)
        return
    # continue with the normal loop using the same file object
    try:
        while True:
            opcode, payload = ws_read_frame(f)
            if opcode is None:
                break
            if opcode == 0x8:
                break
            if opcode == 0x9:
                with c["send_lock"]:
                    c["sock"].sendall(bytes([0x8A, 0x00]))
                continue
            if opcode == 0x1:
                try:
                    handle_message(c, json.loads(payload.decode("utf-8")))
                except (ValueError, UnicodeDecodeError):
                    pass
    finally:
        drop_client(c)
        broadcast({"t": "now", "videoId": "", "title": "",
                   "state": "idle"}, roles=("remote",))

# ------------------------------------------------------------------ main

if __name__ == "__main__":
    print("Tama Karaoke server on port %d" % PORT)
    print("Pairing QR encodes: %s" % public_ws_url())
    if not load_key():
        print("NOTE: no YouTube API key yet — search will be disabled")
        print("      until YT_API_KEY is set (cloud) or yt_key.txt is filled (PC).")
    threading.Thread(target=ws_ping_all, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    # only auto-open a browser for a local run, not on a cloud host
    if not os.environ.get("RENDER_EXTERNAL_HOSTNAME"):
        try:
            webbrowser.open("http://localhost:%d" % PORT)
        except Exception:
            pass
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
