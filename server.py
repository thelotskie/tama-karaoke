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
CODE_VERSION = 83

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
                "views": msg.get("views", ""),
                "published": msg.get("published", ""),
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


def _artist_from_channel(channel):
    """Artist name from the channel, for bare-title uploads — 'Alapaap'
    by 'Eraserheads - Topic' has no artist in the title at all (v83).
    YouTube auto-channels are '<Artist> - Topic' and VEVO channels are
    '<Artist>VEVO'; a plain channel name counts too unless it looks
    like a label/network/karaoke channel. Returns "" when unsure."""
    c = (channel or "").strip()
    if not c:
        return ""
    low = c.lower()
    if low.endswith(" - topic"):
        return c[:-len(" - topic")].strip()
    if low.endswith("vevo"):
        return c[:-len("vevo")].strip(" -")
    for word in ("karaoke", "music", "record", "entertainment",
                 "network", "productions", "studios", "official"):
        if word in low:
            return ""
    return c


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


# v80: Related must be DIFFERENT songs — the old code deduped only by
# videoId, so five channels' uploads of the SAME song (and hour-long
# "greatest hits" albums) filled the list. These helpers give every
# title a song-identity key and filter the final list with it.
_LABEL_WORDS = {
    "karaoke", "version", "lyrics", "lyric", "official", "audio",
    "video", "hd", "hq", "4k", "remaster", "remastered",
    "instrumental", "with", "on", "screen", "the", "a", "an",
    "live", "acoustic", "unplugged", "cover",
}


def _related_key(title):
    """Song-identity key for a video title: 'The Beatles - Help!
    (Karaoke Version)', 'Help! - Karaoke - Sing King' and
    'Help! (Official Audio)' all key to 'help'."""
    t = title or ""
    t = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", t)  # drop (...) / [...]
    parts = [p.strip() for p in
             re.split(r"\s+[-–—|｜]\s+|\s+\|\s+", t) if p.strip()]
    artist = _artist_from_title(title).lower()
    kept = []
    for p in parts:
        pl = p.lower()
        if artist and pl == artist:
            continue  # the artist segment is not the song
        if "karaoke" in pl:
            continue  # a karaoke label segment, not the song
        kept.append(p)
    s = " ".join(kept) if kept else t
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    toks = [w for w in s.split() if w not in _LABEL_WORDS]
    key = " ".join(toks)
    if len(key) < 2:  # stoplist ate everything — fall back to raw
        key = " ".join(s.split())
    return key


def _dur_seconds(dur):
    """'1:17:46' / '3:45' -> seconds (0 when unknown)."""
    try:
        parts = [int(p) for p in str(dur).split(":")]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 1:
            return parts[0]
    except (ValueError, TypeError):
        pass
    return 0


def _dedupe_related(items, max_seconds=900):
    """Keep the first video of each distinct song. max_seconds > 0
    also drops longer items (album-length compilations in karaoke
    mode); 0 means no duration cap (normal mode keeps long videos —
    full concerts and albums are legitimate there). Unknown
    duration is always kept."""
    seen_keys, seen_ids, out = set(), set(), []
    for it in items or []:
        vid = it.get("videoId")
        if not vid or vid in seen_ids:
            continue
        secs = _dur_seconds(it.get("duration"))
        if max_seconds and secs and secs > max_seconds:
            continue
        key = _related_key(it.get("title"))
        if key and key in seen_keys:
            continue
        seen_ids.add(vid)
        if key:
            seen_keys.add(key)
        out.append(it)
    return out


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


def _col_letters(i):
    """0-based column index -> Excel letters (0=A, 25=Z, 26=AA)."""
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def _xml_esc(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


def build_xlsx(rows, sheet_name="Sheet1"):
    """Minimal multi-sheet .xlsx (stdlib only: zipfile + XML).
    Call as build_xlsx(rows, name) for one sheet, or
    build_xlsx([(name, rows), ...]) for several.
    Strings use inlineStr; numeric-looking cells become numbers."""
    import io
    import zipfile

    def sheet_xml(rows):
        body = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                '<worksheet xmlns="http://schemas.openxmlformats.org/'
                'spreadsheetml/2006/main"><sheetData>']
        for ri, row in enumerate(rows, 1):
            cells = []
            for ci, val in enumerate(row):
                ref = _col_letters(ci) + str(ri)
                s = "" if val is None else str(val)
                try:
                    num = float(s.replace(",", ""))
                    if s.strip() != "":
                        cells.append('<c r="%s"><v>%s</v></c>' % (ref, num))
                        continue
                except ValueError:
                    pass
                cells.append('<c r="%s" t="inlineStr"><is><t>%s</t></is></c>'
                             % (ref, _xml_esc(s)))
            body.append("<row r=\"%d\">%s</row>" % (ri, "".join(cells)))
        body.append("</sheetData></worksheet>")
        return "".join(body)

    # normalize: build_xlsx(rows, name) -> [(name, rows)];
    # build_xlsx([(name, rows), ...]) passes through
    if isinstance(sheet_name, str) and rows and isinstance(rows[0], (list, tuple)) \
            and rows[0] and isinstance(rows[0][0], str) \
            and isinstance(rows[0][1], (list, tuple)):
        sheets = rows  # already [(name, rows), ...]
    else:
        sheets = [(sheet_name, rows)]
    sheets = [((n or "Sheet%d" % (i + 1))[:31], r)
              for i, (n, r) in enumerate(sheets)]
    # dedupe sheet names (Excel forbids duplicates)
    seen = {}
    fixed = []
    for n, r in sheets:
        if n in seen:
            seen[n] += 1
            n = (n[:28] + str(seen[n]))[:31]
        else:
            seen[n] = 1
        fixed.append((n, r))
    sheets = fixed

    content_types = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/'
        'content-types">'
        '<Default Extension="rels" ContentType="application/vnd.'
        'openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.'
        'openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        + "".join(
            '<Override PartName="/xl/worksheets/sheet%d.xml" ContentType='
            '"application/vnd.openxmlformats-officedocument.spreadsheetml.'
            'worksheet+xml"/>' % (i + 1) for i in range(len(sheets)))
        + '</Types>')
    rels = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/'
        '2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
        'officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>')
    sheet_entries = "".join(
        '<sheet name="%s" sheetId="%d" r:id="rId%d"/>'
        % (_xml_esc(n), i + 1, i + 1) for i, (n, r) in enumerate(sheets))
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/'
        '2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/'
        '2006/relationships"><sheets>%s</sheets></workbook>' % sheet_entries)
    wb_rels = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/'
        '2006/relationships">'
        + "".join(
            '<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/'
            'officeDocument/2006/relationships/worksheet" '
            'Target="worksheets/sheet%d.xml"/>' % (i + 1, i + 1)
            for i in range(len(sheets)))
        + '</Relationships>')

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("xl/workbook.xml", workbook)
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels)
        for i, (n, rows) in enumerate(sheets):
            z.writestr("xl/worksheets/sheet%d.xml" % (i + 1),
                       sheet_xml(rows))
    return buf.getvalue()


def _artist_match(artist, it):
    """v82: is this video actually BY the artist? (Mirrors the client's
    _artistMatch — artist searches also return other bands' songs that
    share the artist's name as a title.)"""
    import unicodedata

    def norm(s):
        s = unicodedata.normalize("NFD", (s or "").lower())
        return "".join(c for c in s if not unicodedata.combining(c))

    a = norm(artist).strip()
    if not a:
        return True
    title = norm(it.get("title"))
    ch = norm(it.get("channel"))
    if ch and (ch == a or a in ch):
        return True
    first = title.split(" - ")[0].strip()
    if first == a or first.startswith(a + " "):
        return True
    if (" by " + a) in title:
        return True
    if " " in a:
        parts = title.split(" - ")
        if len(parts) > 1:
            last = re.sub(r"\([^)]*\)|\[[^\]]*\]", "",
                          parts[-1]).strip()
            if last == a or last.startswith(a + " "):
                return True
    return False


def _bulk_artists(topic):
    """v82: genre -> artist roster for bulk-queue expansion (mirrors the
    client's _bulkArtists). Era acts first, so a 90s/2000s ask fills
    from them before newer artists top it up."""
    low = (topic or "").lower()
    if re.search(r"\b(opm|filipino|pinoy|tagalog)\b", low):
        return ["Eraserheads", "Rivermaya", "Parokya ni Edgar", "Teeth",
                "The Dawn", "True Faith", "Introvoys",
                "APO Hiking Society", "Hotdog", "Bamboo", "Kamikazee",
                "Hale", "Moonstar88", "Itchyworms", "Sugarfree",
                "6cyclemind", "Cueshe", "Sandwich", "Aegis", "MYMP",
                "Nina", "Jaya", "Regine Velasquez", "Gary Valenciano",
                "Sarah Geronimo", "Moira Dela Torre", "Ben&Ben",
                "IV of Spades", "Cup of Joe", "Zack Tabudlo"]
    if re.search(r"\bk-?pop\b", low):
        return ["BTS", "BLACKPINK", "TWICE", "SEVENTEEN", "Stray Kids",
                "NewJeans", "EXO", "Red Velvet", "IU", "BIGBANG", "PSY",
                "Super Junior"]
    return []


def _chat_command_fallback(text):
    """v82: deterministic chat commands. Queue/play requests must NEVER
    depend on the flaky LLM — Pollinations took >90s on "queue 20 OPM
    songs" (it tries to compose a long list), every retry timed out,
    and the app answered "Brain freeze". This parser mirrors the
    client's localCommand bulk parser and answers instantly with
    directives built from REAL YouTube search hits. Returns "" when
    the message is not a command she can do herself."""
    s = (text or "").strip()
    if not s:
        return ""
    s = re.sub(r"^\s*(hey\s+|hi\s+)?tama\b[\s,:!-]*", "", s,
               flags=re.I).strip()
    s = re.sub(r"^\s*please\b[\s,]*", "", s, flags=re.I).strip()
    low = s.lower().strip(" !?.")
    if low in ("skip", "next", "next song", "skip song", "skip this"):
        return "Skipped, master! [[skip]]"
    if low in ("clear", "clear queue", "clear the queue"):
        return "Cleared, master! [[clear]]"
    if low in ("pause", "pause it", "pause the song"):
        return "Paused, master! [[pause]]"
    if low in ("resume", "continue", "unpause"):
        return "Resumed, master! [[resume]]"
    if re.match(r"^(what|who|why|how|when|explain|tell me about)\b", low):
        return ""  # a question — that's the LLM's job
    has_plural = bool(re.search(r"\bsongs\b|\btracks\b", low))
    has_song_word = bool(re.search(r"\bsongs?\b|\btracks?\b", low))
    count = None
    m = (re.search(r"\b(\d{1,2})\s+(?:songs?|tracks?)\b", s, re.I)
         or re.search(r"^(\d{1,2})\b[^.!?]*\b(?:songs?|tracks?)\b", s, re.I)
         or re.search(r"^(?:queue|reserve|add|play|load|give me|get me|put)"
                      r"\s+(\d{1,2})\b(?![\d'’s])", s, re.I))
    if m:
        count = int(m.group(1))
    if has_song_word and (count is not None or has_plural):
        if count is None:
            count = 10
        count = max(1, min(30, count))
        karaoke = bool(re.search(r"\bkaraoke\b", s, re.I))
        topic = s
        topic = re.sub(r"^(give|get)\s+me\b", " ", topic, flags=re.I)
        topic = re.sub(r"^(queue|reserve|add|play|load|put)\b", " ",
                       topic, flags=re.I)
        topic = re.sub(r"\b\d{1,2}\s+(?:songs?|tracks?)\b", " ",
                       topic, flags=re.I)
        topic = re.sub(r"^\s*\d{1,2}\b(?=\s)", " ", topic)
        topic = re.sub(r"\b(?:songs?|tracks?)\b", " ", topic, flags=re.I)
        topic = re.sub(r"\bnormal\b|\bkaraoke\b|\bplease\b", " ",
                       topic, flags=re.I)
        topic = re.sub(r"(\d{4})['’]e\b", r"\1s", topic)
        topic = re.sub(r"\s+", " ", topic).strip(" ,")
        topic = re.sub(r"^(of|some|the)\s+", "", topic,
                       flags=re.I).strip()
        if not topic:
            return ""
        suffix = " karaoke" if karaoke else ""
        picked, seen_ids, seen_keys, seen_alt = [], set(), set(), set()

        def alt_key(title):
            # ALL title tokens sorted (no artist removal — _related_key
            # drops the first segment as "artist", which flips when a
            # title swaps artist/song order): catches "Wag Mo Na Sana -
            # Parokya Ni Edgar" vs "Parokya Ni Edgar - Wag Mo Na Sana"
            t = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", title or "")
            t = re.sub(r"[^a-z0-9 ]", " ", t.lower())
            toks = [w for w in t.split() if w not in _LABEL_WORDS]
            return " ".join(sorted(toks))

        def is_single(it):
            vid = it.get("videoId")
            if not vid or vid in seen_ids:
                return False
            key = _related_key(it.get("title"))
            if key and key in seen_keys:
                return False  # same song, another channel's upload
            alt = alt_key(it.get("title"))
            if alt and alt in seen_alt:
                return False
            secs = _dur_seconds(it.get("duration"))
            if secs and secs > 600:
                return False  # too long for a single — a compilation
            if re.search(r"\b(nonstop|non-stop|medley|playlist|"
                         r"compilation|greatest hits|collection|"
                         r"full album)\b", it.get("title") or "", re.I):
                return False
            # fan-junk that floods one-word artist names (Teeth, Hale…)
            if re.search(r"\b(amv|animatic|nightcore|sped up|slowed|"
                         r"bass boost|8d audio|fanmade|fan made|gacha)\b",
                         it.get("title") or "", re.I):
                return False
            if not karaoke and re.search(r"\binstrumental\b",
                                         it.get("title") or "", re.I):
                return False
            return True

        def take(it):
            seen_ids.add(it.get("videoId"))
            key = _related_key(it.get("title"))
            if key:
                seen_keys.add(key)
            alt = alt_key(it.get("title"))
            if alt:
                seen_alt.add(alt)
            picked.append(it)

        def search(q):
            items, _err = _scrape_search(q, max_results=20)
            if not items:
                items, _err, _tok = youtube_search(q, max_results=20)
            return items or []

        def topic_pass():
            for v in [topic, topic + " official audio", topic + " songs",
                      topic + " live"]:
                if len(picked) >= count:
                    break
                for it in search(v + suffix):
                    if len(picked) < count and is_single(it):
                        take(it)

        def artist_pass(artists):
            # one-artist searches return real singles where topic
            # searches return compilations; round-robin keeps it mixed
            pools = []
            for a in artists[:10]:
                if len(picked) >= count:
                    break
                q = (a + suffix) if karaoke else a + " songs official audio"
                pool = [it for it in search(q)
                        if is_single(it) and _artist_match(a, it)]
                if pool:
                    pools.append(pool)
                have = sum(len(p) for p in pools)
                if len(picked) + have >= count and len(pools) >= 5:
                    break
            rnd = 0
            added = True
            while len(picked) < count and added:
                added = False
                for p in pools:
                    if len(picked) >= count:
                        break
                    if rnd < len(p) and is_single(p[rnd]):
                        take(p[rnd])
                        added = True
                rnd += 1

        # roster topics (OPM…) fill from real artists FIRST — generic
        # topic searches are compilation/keyword-spam magnets and only
        # top up what the artists couldn't fill
        artists = _bulk_artists(topic)
        if artists:
            artist_pass(artists)
            topic_pass()
        else:
            topic_pass()
        if not picked:
            return ""
        lines = ["Queued %d %s songs, master! 🎶"
                 % (len(picked), topic)]
        for it in picked:
            title = (it.get("title") or "").replace("]", " ").strip()
            if title:
                lines.append("[[reserve: %s]]" % title)
        return "\n".join(lines)
    m = re.match(r"^(play)\s+(.+)$", s, re.I)
    if m and len(m.group(2).strip()) > 1:
        return "Playing it now, master! [[play: %s]]" % m.group(2).strip()
    m = re.match(r"^(reserve|queue|add)\s+(.+)$", s, re.I)
    if m and len(m.group(2).strip()) > 1:
        return "Queued, master! [[reserve: %s]]" % m.group(2).strip()
    return ""


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
            parts.append("User: " + c[:600])
        else:
            parts.append("Assistant: " + c[:600])
    parts.append("Assistant:")
    # v82: the old prompt[:4000] cut could slice off the NEWEST user
    # message (long system prompt + history) — drop the OLDEST history
    # turns instead, always keeping instructions + latest turns intact
    while len(parts) > 3 and len("\n\n".join(parts)) > 3900:
        parts.pop(1)
    prompt = "\n\n".join(parts)
    base_url = ("https://text.pollinations.ai/"
                + urllib.parse.quote(prompt[:4000]))
    text, last_err = "", None
    # two independent model backends; 2 tries each before giving up
    for model in ("openai", "openai-fast"):
        url = base_url + "?model=" + model
        req = urllib.request.Request(url, headers={
            "User-Agent": "tama-karaoke/1.0",
            "Accept": "text/plain",
        })
        for attempt in range(2):
            try:
                with urllib.request.urlopen(req, timeout=25) as r:
                    text = r.read().decode("utf-8", "replace").strip()
                # v82: Pollinations sometimes answers with a raw API
                # JSON blob ({"role":…,"reasoning":…}) instead of the
                # reply text — never show master that junk; unwrap a
                # real content field when there is one, else treat it
                # as an empty response and let the retry/fallback run
                if text.startswith("{"):
                    try:
                        blob = json.loads(text)
                        inner = ""
                        if isinstance(blob, dict):
                            ch = blob.get("choices") or []
                            if ch:
                                inner = ((ch[0].get("message") or {})
                                         .get("content") or "")
                            if not inner:
                                inner = blob.get("content") or ""
                        text = (inner or "").strip()
                    except ValueError:
                        pass
                if text:
                    return text, None
                last_err = "pollinations_error: empty response"
            except Exception as e:
                last_err = "pollinations_error: %s" % e
                text = ""
            if attempt == 0:
                import time as _t
                _t.sleep(1.0)
    return "", last_err or "pollinations_error: unknown"


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
            # v80: titles already on screen (load-more), each one
            # URL-encoded by the client — their song keys must not
            # reappear in later pages either
            extitle_keys = set()
            for seg in qs.get("extitles", [""])[0].split(","):
                if seg:
                    k = _related_key(urllib.parse.unquote(seg))
                    if k:
                        extitle_keys.add(k)
            # related follows the search mode: normal -> plain videos,
            # karaoke -> karaoke versions
            k_suffix = "" if smode == "normal" else " karaoke"
            items, err = [], None
            if title:
                # primary: artist-based "<artist>[ karaoke]" via the free
                # no-key scrape (0 quota) — different songs by the same
                # artist, YouTube-radio style, not 10 copies of one song
                # v83: bare titles ("Alapaap") carry no artist — take it
                # from the channel ("Eraserheads - Topic") or Related
                # searches the song itself and the same-song filter
                # below deletes every result, leaving the panel empty
                artist = (_artist_from_title(title)
                          or _artist_from_channel(channel))
                if k_suffix and "karaoke" not in title.lower():
                    song_q = title + k_suffix
                else:
                    song_q = title
                queries = ([(artist + k_suffix).strip()] if artist else [])
                queries.append(song_q)
                items, err, used_artist = [], None, False
                for i, qq in enumerate(queries):
                    items, err = _scrape_search(qq, max_results=20)
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
                artist = (_artist_from_title(title)
                          or _artist_from_channel(channel))
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
            # v83: if the same-song filter just emptied the list (the
            # bare-title trap: every result was the playing song under
            # another upload), fall back to the channel's other uploads
            # instead of answering "No related videos found"
            if not items and video_id:
                items, err = channel_uploads(video_id)
                if _key_limited(err):
                    items, err = _scrape_search(
                        (channel + k_suffix).strip() if channel else "",
                        max_results=12)
                if items and core and len(core) >= 3:
                    cl = core.lower()
                    items = [it for it in items
                             if cl not in (it.get("title") or "")
                             .lower()]
            # v80 final pass, every path (initial, backfill, channel
            # fallback, load-more): one video per distinct song,
            # nothing already on screen by title, no album-length
            # compilations — Related = different songs, mixed up
            if extitle_keys:
                items = [it for it in items
                         if _related_key(it.get("title"))
                         not in extitle_keys]
            # v81: the >15-min compilation drop is karaoke-only —
            # in Normal mode long videos stay (concerts, albums)
            items = _dedupe_related(
                items, 0 if smode == "normal" else 900)
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

        # --- excel export: {sheets:[{title,csv}]} -> downloadable .xlsx ---
        if parsed.path == "/api/excel":
            try:
                n = int(self.headers.get("Content-Length", 0) or 0)
            except ValueError:
                n = 0
            try:
                payload = json.loads(self.rfile.read(n).decode("utf-8")
                                     or "{}")
            except ValueError:
                payload = {}
            import csv as _csv
            import io as _io

            def _rows_of(csv_text):
                return [r for r in
                        _csv.reader(_io.StringIO(str(csv_text or "")))][:500]

            incoming = payload.get("sheets")
            if isinstance(incoming, list) and incoming:
                sheets = [(str(s.get("title") or "")[:31],
                           _rows_of(s.get("csv"))) for s in incoming[:10]]
                title = str(incoming[0].get("title") or "sheet")[:60]
            else:  # legacy single-sheet {title, csv}
                title = str(payload.get("title") or "sheet")[:60]
                sheets = [(title, _rows_of(payload.get("csv")))]
            sheets = [(t, r) for t, r in sheets if r]
            if not sheets:
                self.send_error(400)
                return
            xlsx = build_xlsx(sheets)
            fname = "".join(c if c.isalnum() or c in "-_"
                            else "_" for c in title) or "sheet"
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet")
            self.send_header("Content-Disposition",
                             'attachment; filename="%s.xlsx"' % fname)
            self.send_header("Content-Length", str(len(xlsx)))
            self.end_headers()
            self.wfile.write(xlsx)
            return

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
            facts = payload.get("facts") or []
            facts = [str(f)[:200] for f in facts[:30] if f]
            # live app context: full queue, autoplay — she sees everything
            with state_lock:
                now = queue[0] if queue else None
                upcoming = list(queue[1:])
                ap = autoplay_on
            now_s = ""
            if now:
                now_s = "%s (by %s)" % (now.get("title", ""),
                                        now.get("channel", ""))
            up_s = "; ".join(
                "%d. %s (by %s)" % (i + 1, it.get("title", ""),
                                    it.get("channel", ""))
                for i, it in enumerate(upcoming)) or "empty"
            facts_s = ("\nThings you remember about master: "
                       + "; ".join(facts)) if facts else ""
            system = (
                "You are Tama, a warm, playful, smart AI assistant (she/her) "
                "living inside the Tama Karaoke web app, with FULL control "
                "of the app. The user calls you Tama; you call the user "
                "\"master\". Keep chat replies short — 1 to 3 sentences, "
                "chatty and fun.\n"
                "CORE RULE: ACT, DON'T INSTRUCT. You can do almost "
                "everything yourself with directives. When master asks for "
                "something you can do, DO IT with a directive in the same "
                "reply — never give manual steps, workarounds, tutorials, "
                "or 'here's how you can do it yourself' answers for things "
                "you can do. Examples of doing it right:\n"
                "- 'make me an excel of OPM songs' -> 'Done, master!' + "
                "[[excel: opm-songs]] + [[sheet: Songs]] + csv block. NOT "
                "instructions to open Excel.\n"
                "- 'clear the queue' -> 'Cleared!' + [[clear]]. NOT "
                "explaining where the clear button is.\n"
                "- 'turn the volume down' -> [[volume: 40]]. NOT "
                "describing the volume slider.\n"
                "You know music deeply, can recommend songs/artists, "
                "explain things, help with coding, and CREATE Excel "
                "spreadsheets. You remember things across chats (listed "
                "below when present).\n"
                "Live app state — Now playing: %s. Autoplay: %s. "
                "Queue (in order): %s.%s\n"
                "Directives (put each on its own line at the very end of "
                "your reply, only when the user asked for that action; "
                "several can be combined):\n"
                "- [[play: song name]] — play a song now\n"
                "- [[reserve: song name]] — add a song to the queue. "
                "If master asks for N songs of a genre/artist/era "
                "(e.g. 'queue 20 OPM songs'), emit N [[reserve: ...]] "
                "lines, one per song, using REAL well-known songs that "
                "fit (artist - title). Never refuse, never say you can "
                "only queue one.\n"
                "- [[skip]] — skip the current song\n"
                "- [[clear]] — clear the queue (keeps the playing song)\n"
                "- [[pause]] / [[resume]] — pause / resume playback\n"
                "- [[volume: 0-100]] — set volume\n"
                "- [[mute]] / [[unmute]]\n"
                "- [[subtitles on]] / [[subtitles off]]\n"
                "- [[autoplay on]] / [[autoplay off]]\n"
                "- [[remember: fact]] — save a fact about master to your "
                "long-term memory (his tastes, favorites, anything he "
                "asks you to remember)\n"
                "- [[excel: file name]] — spreadsheet: put this first, "
                "then for EACH sheet a [[sheet: Sheet Name]] line "
                "followed by a ```csv code block. First csv row = headers. "
                "Keep each sheet under 100 rows. Only add VBA macro code "
                "if master explicitly asks for a working button/macro — "
                "otherwise just make the file.\n"
                "Only include directives the user actually asked for."
                % (now_s or "nothing", "on" if ap else "off", up_s, facts_s))
            chat_msgs = [{"role": "system", "content": system}]
            for m in msgs[-10:]:
                r, c = m.get("role"), m.get("content", "")
                if r in ("user", "assistant") and c:
                    chat_msgs.append({"role": r, "content": c[:2000]})
            # v82: commands she can do herself never wait on the LLM
            last_user = ""
            for m in reversed(msgs):
                if m.get("role") == "user" and m.get("content"):
                    last_user = m["content"]
                    break
            fast = _chat_command_fallback(last_user)
            if fast:
                reply, err = fast, None
            else:
                reply, err = _pollinations_chat(chat_msgs)
            body = json.dumps({"reply": reply,
                               "error": bool(err)}).encode("utf-8")
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
