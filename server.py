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
import re
import random
import socket
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8765"))
BASE = os.path.dirname(os.path.abspath(__file__))
WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
# bump on every deploy: clients auto-reload when the server has newer code
CODE_VERSION = 52

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
autoplay_on = True    # shared autoplay switch (all screens stay in sync)
drain_seq = 0         # id of the latest queue-drain; autoplay reserves must
                      # echo it, so stale clients and racers can't refill
_drain_was_empty = True  # tracks empty->non-empty transitions for drain_seq

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
    global drain_seq, _drain_was_empty
    with state_lock:
        items = [dict(i) for i in queue]
        ap = autoplay_on
        is_empty = not items
        # new drain id only when the queue actually runs dry (not on
        # every empty broadcast, or legit reserves would go stale)
        if is_empty and not _drain_was_empty:
            drain_seq += 1
        _drain_was_empty = is_empty
        drain = drain_seq
    broadcast({"t": "queue", "items": items, "autoplay": ap, "drain": drain,
               "code_version": CODE_VERSION})

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
    global queue_seq, autoplay_on
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
            vid = msg.get("videoId", "")
            # no duplicates: skip if already in the queue (incl. playing song)
            if any(i.get("videoId") == vid for i in queue):
                ok = False
            elif msg.get("auto"):
                # autoplay reserve: honored only for the current drain while
                # the queue is still empty — one autoplay per drain; stale
                # clients (no drain) and racers are ignored
                ok = (msg.get("drain") == drain_seq and not queue)
            else:
                ok = True
            if ok:
                queue_seq += 1
                queue.append({
                    "id": "q%d" % queue_seq,
                    "videoId": vid,
                    "title": msg.get("title", "Unknown"),
                    "channel": msg.get("channel", ""),
                    "thumb": msg.get("thumb", ""),
                    "views": msg.get("views", ""),
                    "published": msg.get("published", ""),
                    "by": c.get("name") or msg.get("by") or "screen",
                })
        broadcast_queue()

    elif t == "playnow":
        # Play immediately: insert at the front of the queue. The song that
        # was playing shifts to second place and resumes afterwards.
        with state_lock:
            queue_seq += 1
            queue.insert(0, {
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

    elif t == "clear":
        # clear the queue but keep the currently playing song (queue[0]);
        # also turns autoplay off so it doesn't instantly refill
        with state_lock:
            if queue:
                del queue[1:]
            autoplay_on = False
        broadcast_queue()

    elif t == "autoplay":
        with state_lock:
            autoplay_on = bool(msg.get("on", True))
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

    elif t == "progress":
        # screen reports playback position so an interrupted song resumes
        # where it left off instead of restarting from 0
        vid = msg.get("videoId")
        try:
            pos = max(0, float(msg.get("time", 0)))
        except (TypeError, ValueError):
            pos = 0
        if vid and pos > 0:
            with state_lock:
                if queue and queue[0].get("videoId") == vid:
                    queue[0]["pos"] = pos
        # no broadcast: pos is only read when the song (re)starts

# ------------------------------------------------------------- youtube api

def _get_json(url, tries=3):
    """GET a JSON URL, retrying on HTTP 429 (YouTube rate limiting).
    Returns (data, error)."""
    last_err = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=12) as r:
                return json.load(r), None
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429 and attempt < tries - 1:
                time.sleep(2 * (attempt + 1))  # 2s, then 4s
                continue
            return None, "youtube_error: %s" % e
        except Exception as e:  # bad key, quota out, offline...
            return None, "youtube_error: %s" % e
    return None, "youtube_error: %s" % last_err


def youtube_api(path, params):
    """Call a YouTube Data API endpoint. The list endpoints used here cost
    ~1 quota unit each (a search costs 100). Returns (data, error)."""
    key = load_key()
    if not key:
        return None, "no_key"
    params = dict(params)
    params["key"] = key
    url = ("https://www.googleapis.com/youtube/v3/" + path + "?"
           + urllib.parse.urlencode(params))
    return _get_json(url)


def _artist_from_title(title):
    """Best-effort artist extraction from video titles.
    Handles 'Artist - Song', 'Song - Karaoke - Artist' (artist last),
    and 'Song by Artist'. Returns "" when no artist can be found."""
    t = (title or "").strip()
    for sep in (" - ", " – ", " — "):
        if sep in t:
            parts = [p.strip() for p in t.split(sep) if p.strip()]
            if not parts:
                return ""
            # "Song - Karaoke Version - Artist" / "Song (Karaoke) - Artist" /
            # "Karaoke - Artist - Song": the artist follows the karaoke label
            if "karaoke" in parts[0].lower():
                idx = 1 if parts[0].strip().lower() == "karaoke" and \
                    len(parts) >= 3 else -1
                cand = re.sub(r"\s*\(.*?\)\s*$", "",
                              parts[idx]).strip()
                if cand and "karaoke" not in cand.lower():
                    return cand
                return ""
            if len(parts) >= 3 and any("karaoke" in p.lower()
                                       for p in parts[1:-1]):
                cand = re.sub(r"\s*\(.*?\)\s*$", "", parts[-1]).strip()
                if cand and "karaoke" not in cand.lower():
                    return cand
                return ""
            cand = parts[0]
            if "karaoke" not in cand.lower():
                return cand
            return ""
    # "Artist: Song (…)" format
    if ":" in t:
        cand = t.split(":", 1)[0].strip()
        if cand and "karaoke" not in cand.lower() and len(cand) < 40:
            return cand
    # "Song by Artist (…)" format
    low = t.lower()
    if " by " in low:
        cand = t[low.rindex(" by ") + 4:].strip()
        cand = re.sub(r"\s*\(.*?\)\s*$", "", cand).strip()
        if cand and "karaoke" not in cand.lower():
            return cand
    return ""


def _song_core(title):
    """'Artist - Song (Karaoke…)' -> 'song': the song part, lowercased,
    with (...) / [...] segments and ' | ...' suffixes stripped."""
    t = (title or "").strip()
    for sep in (" - ", " – ", " — "):
        if sep in t:
            t = t.split(sep, 1)[1]
            break
    for sep in (" | ", "｜"):
        if sep in t:
            t = t.split(sep, 1)[0]
            break
    out, depth = [], 0
    for ch in t:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    return "".join(out).strip().lower()


def channel_uploads(video_id, max_results=12):
    """Related videos = latest uploads from the same channel as video_id.
    Costs ~3 quota units total instead of 100 for a search."""
    # 1) video -> channelId (1 unit)
    data, err = youtube_api("videos",
                            {"part": "snippet", "id": video_id,
                             "maxResults": 1})
    if err:
        return [], err
    items = data.get("items") or []
    if not items:
        return [], None
    snippet = items[0].get("snippet") or {}
    channel_id = snippet.get("channelId")
    channel_title = snippet.get("channelTitle", "")
    if not channel_id:
        return [], None
    # 2) channel -> uploads playlist id (1 unit)
    data, err = youtube_api("channels",
                            {"part": "contentDetails", "id": channel_id,
                             "maxResults": 1})
    if err:
        return [], err
    items = data.get("items") or []
    if not items:
        return [], None
    uploads = ((items[0].get("contentDetails") or {})
               .get("relatedPlaylists") or {}).get("uploads")
    if not uploads:
        return [], None
    # 3) uploads playlist -> videos (1 unit)
    data, err = youtube_api("playlistItems",
                            {"part": "snippet", "playlistId": uploads,
                             "maxResults": max_results})
    if err:
        return [], err
    out = []
    for it in data.get("items") or []:
        sn = it.get("snippet") or {}
        vid = (sn.get("resourceId") or {}).get("videoId")
        if not vid or vid == video_id:
            continue
        thumbs = sn.get("thumbnails") or {}
        thumb = ((thumbs.get("medium") or thumbs.get("default") or {})
                 .get("url", ""))
        out.append({
            "videoId": vid,
            "title": sn.get("title", ""),
            "channel": sn.get("channelTitle", channel_title),
            "thumb": thumb,
        })
    return out, None


def _scrape_search(q, max_results=15):
    """No-key YouTube search: fetch youtube.com/results HTML and parse the
    embedded ytInitialData JSON — the same data the YouTube website itself
    uses. No API key, no quota. Used as a fallback when the key is
    rate-limited; returns ([items], error)."""
    if not q:
        return [], None
    url = ("https://www.youtube.com/results?"
           + urllib.parse.urlencode({"search_query": q}))
    req = urllib.request.Request(url, headers={
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", "replace")
    except Exception as e:
        return [], "scrape_error: %s" % e
    marker = "var ytInitialData = "
    idx = html.find(marker)
    if idx < 0:
        return [], "scrape_error: no ytInitialData (blocked?)"
    try:
        data, _ = json.JSONDecoder().raw_decode(html[idx + len(marker):])
    except Exception as e:
        return [], "scrape_error: bad json %s" % e
    try:
        contents = (data["contents"]["twoColumnSearchResultsRenderer"]
                    ["primaryContents"]["sectionListRenderer"]["contents"])
    except (KeyError, TypeError):
        return [], "scrape_error: no results block"
    out = []
    for sec in contents:
        for item in sec.get("itemSectionRenderer", {}).get("contents", []):
            vr = item.get("videoRenderer")
            if not vr:
                continue
            vid = vr.get("videoId")
            if not vid:
                continue
            title = "".join(
                x.get("text", "") for x in vr.get("title", {}).get("runs", []))
            ch = ""
            for key in ("ownerText", "longBylineText"):
                runs = vr.get(key, {}).get("runs", [])
                if runs:
                    ch = runs[0].get("text", "")
                    break
            thumbs = vr.get("thumbnail", {}).get("thumbnails", [])
            thumb = thumbs[-1].get("url", "") if thumbs else ""
            # "1.2M views" and "2 days ago" like YouTube shows
            views = (vr.get("viewCountText", {}).get("simpleText") or "")
            published = (vr.get("publishedTimeText", {}).get("simpleText")
                         or "")
            # v51: video length ("3:45") for the thumbnail badge
            duration = (vr.get("lengthText", {}).get("simpleText") or "")
            out.append({"videoId": vid, "title": title,
                        "channel": ch, "thumb": thumb,
                        "views": views, "published": published,
                        "duration": duration})
            if len(out) >= max_results:
                return out, None
    return out, None


def _api_search(q, max_results=15, page_token=None):
    """Search YouTube for q. Returns (items, error, next_page_token)."""
    key = load_key()
    if not key:
        return [], "no_key", None
    if not q:
        return [], None, None
    params = urllib.parse.urlencode({
        "part": "snippet",
        "type": "video",
        "videoEmbeddable": "true",
        "maxResults": max_results,
        "order": "relevance",
        "q": q,
        "key": key,
    } | ({"pageToken": page_token} if page_token else {}))
    url = "https://www.googleapis.com/youtube/v3/search?" + params
    data, err = _get_json(url)
    if err:
        return [], err, None
    items = []
    for it in data.get("items", []):
        vid = (it.get("id") or {}).get("videoId")
        sn = it.get("snippet") or {}
        if not vid:
            continue
        thumbs = sn.get("thumbnails") or {}
        thumb = (thumbs.get("medium") or thumbs.get("default") or {}).get("url", "")
        # snippet has publishedAt (ISO); format like "2 days ago"
        pub = _rel_time(sn.get("publishedAt", ""))
        items.append({
            "videoId": vid,
            "title": sn.get("title", ""),
            "channel": sn.get("channelTitle", ""),
            "thumb": thumb,
            "views": "",
            "published": pub,
            "duration": "",
        })
    # v51: fetch durations in one videos.list call (1 quota unit for 50 ids)
    if items and key:
        vids = ",".join(it["videoId"] for it in items)
        durl = ("https://www.googleapis.com/youtube/v3/videos?" +
                urllib.parse.urlencode({"part": "contentDetails",
                                        "id": vids, "key": key}))
        ddata, derr = _get_json(durl)
        if not derr:
            for it in ddata.get("items", []):
                dur = _iso_duration((it.get("contentDetails") or {})
                                    .get("duration", ""))
                for vi in items:
                    if vi["videoId"] == it.get("id"):
                        vi["duration"] = dur
                        break
    return items, None, data.get("nextPageToken")


def _iso_duration(iso):
    """'PT3M45S' -> '3:45' for the thumbnail badge."""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return ""
    h, mn, s = (int(m.group(1) or 0), int(m.group(2) or 0),
                int(m.group(3) or 0))
    if h:
        return "%d:%02d:%02d" % (h, mn, s)
    return "%d:%02d" % (mn, s)


def _rel_time(iso):
    """'2026-10-10T12:00:00Z' -> '5 days ago' (YouTube style)."""
    if not iso:
        return ""
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
        if secs < 0:
            return ""
        mins, hours, days = secs / 60, secs / 3600, secs / 86400
        if mins < 1:
            return "just now"
        if mins < 60:
            return "%d minute%s ago" % (mins, "" if mins == 1 else "s")
        if hours < 24:
            return "%d hour%s ago" % (hours, "" if hours == 1 else "s")
        if days < 30:
            return "%d day%s ago" % (days, "" if days == 1 else "s")
        if days < 365:
            m = int(days / 30)
            return "%d month%s ago" % (m, "" if m == 1 else "s")
        y = int(days / 365)
        return "%d year%s ago" % (y, "" if y == 1 else "s")
    except Exception:
        return ""


def _key_limited(err):
    """True when the API key is missing, rate-limited, or out of quota —
    i.e. when the no-key website scrape should take over."""
    return bool(err) and (err == "no_key" or "429" in err
                          or "quota" in err.lower())


def _pollinations_chat(messages):
    """Free no-key LLM chat (Pollinations GET endpoint).
    Returns (reply_text, error)."""
    # flatten into a single prompt; GET endpoint is fast and reliable
    parts = []
    for m in messages:
        r = m.get("role")
        c = (m.get("content") or "").strip()
        if not c:
            continue
        if r == "system":
            parts.append("Instructions: " + c)
        elif r == "user":
            parts.append("User: " + c)
        else:
            parts.append("Assistant: " + c)
    parts.append("Assistant:")
    prompt = "\n\n".join(parts)
    url = ("https://text.pollinations.ai/"
           + urllib.parse.quote(prompt[:4000]))
    req = urllib.request.Request(url, headers={
        "User-Agent": "tama-karaoke/1.0",
        "Accept": "text/plain",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            text = r.read().decode("utf-8", "replace").strip()
    except Exception as e:
        return "", "pollinations_error: %s" % e
    if not text:
        return "", "pollinations_error: empty response"
    return text, None


def youtube_search(q, max_results=15, page_token=None):
    """Search YouTube for q. API key first (reliable, structured); falls
    back to the no-key website scrape when the key can't serve.
    Returns (items, error, next_page_token). Scrape has no pagination —
    next_page_token is None for scrape results."""
    items, err, token = _api_search(q, max_results, page_token)
    if _key_limited(err):
        sitems, serr = _scrape_search(q, max_results)
        return sitems, serr, None
    return items, err, token

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

        # --- code version for proactive client auto-update polling ---
        if parsed.path == "/api/version":
            body = json.dumps({"code_version": CODE_VERSION}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- youtube search proxy (q= text query) ---
        if parsed.path == "/api/search":
            qs = urllib.parse.parse_qs(parsed.query)
            q = qs.get("q", [""])[0]
            page_token = qs.get("pageToken", [""])[0] or None
            try:
                offset = int(qs.get("offset", ["0"])[0] or 0)
            except ValueError:
                offset = 0
            if page_token:
                # API pagination: proper next page
                items, err, next_token = youtube_search(
                    q, max_results=20, page_token=page_token)
            else:
                # scrape: no token; fetch a larger batch once and slice
                items, err, next_token = youtube_search(q, max_results=40)
                if offset:
                    items = items[offset:offset + 20]
                else:
                    # first page: 20 items; signal more if we got 40
                    next_token = "scrape-more" if len(items) >= 40 else None
                    items = items[:20]
            body = json.dumps({"items": items, "error": err,
                               "nextPageToken": next_token}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # --- related videos: latest uploads from the same channel ---
        # (YouTube removed relatedToVideoId from the Data API. We resolve the
        # video's channel via cheap list endpoints (~3 quota units total)
        # instead of a search (100 units), which also avoids rate-limiting.)
        if parsed.path == "/api/related":
            qs = urllib.parse.parse_qs(parsed.query)
            video_id = qs.get("videoId", [""])[0].strip()
            channel = qs.get("channel", [""])[0].strip()
            title = qs.get("title", [""])[0].strip()
            smode = qs.get("smode", ["karaoke"])[0].strip()
            more = qs.get("more", [""])[0].strip()
            exclude = set(x for x in qs.get("exclude", [""])[0].split(",") if x)
            # related follows the search mode: normal -> plain videos,
            # karaoke -> karaoke versions
            k_suffix = "" if smode == "normal" else " karaoke"
            items, err = [], None
            if title:
                # primary: artist-based "<artist>[ karaoke]" via the free
                # no-key scrape (0 quota) — different songs by the same
                # artist, YouTube-radio style, not 10 copies of one song
                artist = _artist_from_title(title)
                if k_suffix and "karaoke" not in title.lower():
                    song_q = title + k_suffix
                else:
                    song_q = title
                queries = ([(artist + k_suffix).strip()] if artist else [])
                queries.append(song_q)
                items, err, used_artist = [], None, False
                for i, qq in enumerate(queries):
                    items, err = _scrape_search(qq, max_results=12)
                    if items:
                        used_artist = (i == 0 and bool(artist))
                        break
                if used_artist:
                    # drop same-song uploads STRICTLY: related = DIFFERENT
                    # songs by the same artist, mixed up — never the same
                    # title repeated
                    core = _song_core(title)
                    core_l = (core.lower() if core and len(core) >= 3
                              else "")
                    if core_l:
                        items = [it for it in items
                                 if core_l not in (it.get("title") or "")
                                 .lower()]
                    # backfill: if too few different songs, pull more from
                    # the artist's greatest-hits / live searches (same
                    # artist, same genre) so related never runs dry
                    seen = set(it.get("videoId") for it in items)
                    if len(items) < 8 and artist:
                        for bq in ((artist + " greatest hits" + k_suffix)
                                   .strip(),
                                   (artist + " live" + k_suffix).strip()):
                            bitems, _ = _scrape_search(bq, max_results=12)
                            for it in bitems:
                                vid = it.get("videoId")
                                ititle = (it.get("title") or "").lower()
                                if (vid and vid not in seen and vid != video_id
                                        and (not core_l or
                                             core_l not in ititle)):
                                    items.append(it)
                                    seen.add(vid)
                            if len(items) >= 12:
                                break
                    # mix up the order so it's not just top search hits
                    random.shuffle(items)
            if not items and video_id:
                # fallback: latest uploads from the same channel (~3 units)
                items, err = channel_uploads(video_id)
                if _key_limited(err):
                    # key can't serve: no-key scrape of the channel
                    items, err = _scrape_search(
                        (channel + k_suffix).strip() if channel else "",
                        max_results=12)
            if not items and not video_id and not title:
                # legacy fallback for callers without a videoId
                items, err = youtube_search(
                    (channel + k_suffix).strip() if channel else "",
                    max_results=12)
            if more and title:
                # infinite scroll: cycle through query variations for more
                # videos — wraps around so it keeps going; exclude filters
                # already-shown so we never repeat
                artist = _artist_from_title(title)
                # v48: when no artist is in the title ("Song (Karaoke)"
                # style uploads), fall back to the channel — a karaoke
                # channel's catalog is different songs in the same genre,
                # so load-more doesn't die after the first batch
                if artist:
                    base = artist
                    base_variants = [
                        " live", " acoustic", " greatest hits",
                        " playlist", " unplugged", " cover",
                        " official video", " top songs", " best of",
                        " live performance",
                    ]
                elif channel:
                    base = channel
                    base_variants = [
                        "", " greatest hits", " playlist", " best of",
                        " top songs",
                    ]
                else:
                    base, base_variants = "", []
                variants = [(base + v + k_suffix).strip()
                            for v in base_variants] if base else []
                try:
                    n = int(more)
                except ValueError:
                    n = 1
                if n >= 1 and variants:
                    # cycle: variation index wraps, offset grows for deeper
                    # pages of the same variation
                    v_idx = (n - 1) % len(variants)
                    vitems, verr = _scrape_search(variants[v_idx],
                                                 max_results=30)
                    if vitems:
                        items, err = vitems, verr
            # drop already-shown videos (infinite scroll)
            if exclude:
                items = [it for it in items
                         if it.get("videoId") not in exclude]
            # also drop same-song titles on load-more (strict, like initial)
            core = _song_core(title)
            if core and len(core) >= 3:
                core_l = core.lower()
                items = [it for it in items
                         if core_l not in (it.get("title") or "").lower()]
                random.shuffle(items)
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
            # inject the code version so stale clients can auto-update
            body = body.replace(b"__CODE_VERSION__", str(CODE_VERSION).encode("utf-8"))
        ext = os.path.splitext(rel)[1].lower()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        # --- AI chat: Tama the karaoke host, via free Pollinations API ---
        if parsed.path == "/api/chat":
            try:
                n = int(self.headers.get("Content-Length", 0) or 0)
            except ValueError:
                n = 0
            try:
                payload = json.loads(self.rfile.read(n).decode("utf-8")
                                     or "{}")
            except ValueError:
                payload = {}
            msgs = payload.get("messages") or []
            # live app context so Tama knows what's playing / queued
            with state_lock:
                now = queue[0].get("title", "") if queue else ""
                upcoming = [i.get("title", "") for i in queue[1:4]]
            system = (
                "You are Tama, a warm, playful AI karaoke host (she/her) "
                "living inside the Tama Karaoke web app. The user calls you "
                "Tama; you call the user \"master\". Keep replies short — 1 "
                "to 3 sentences, chatty and fun, like texting a friend. You "
                "help find songs, manage the queue, and keep the party going. "
                "Now playing: %s. Up next: %s. "
                "When the user asks to play or queue a song, put a directive "
                "on its own line at the very end of your reply, exactly like "
                "[[play: song name]] or [[reserve: song name]]. Only include "
                "a directive when the user actually asked to play or queue "
                "something; otherwise reply normally with no directive."
                % (now or "nothing", ", ".join(upcoming) or "nothing"))
            chat_msgs = [{"role": "system", "content": system}]
            for m in msgs[-10:]:
                r, c = m.get("role"), m.get("content", "")
                if r in ("user", "assistant") and c:
                    chat_msgs.append({"role": r, "content": c[:2000]})
            reply, err = _pollinations_chat(chat_msgs)
            if err:
                reply = ("Hmm, my brain hiccuped for a sec, master — "
                         "try me again in a moment! 🎤")
            body = json.dumps({"reply": reply}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_error(404)

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
            ap = autoplay_on
            dr = drain_seq
        try:
            ws_send(c, json.dumps({"t": "queue", "items": items,
                                   "autoplay": ap, "drain": dr,
                                   "code_version": CODE_VERSION}))
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
