# Karaoke Remote Protocol v1

Transport: WebSocket, endpoint `ws://<host>:8765/ws`.
QR shown on the karaoke screen encodes exactly: `ws://<host>:8765/ws`

Every message is one JSON object per text frame. Field `t` = message type.

## Client → Server

| t | fields | meaning |
|---|--------|---------|
| `hello` | `role`: "screen" \| "remote", `name` (remote only, device label) | identify on connect |
| `search` | `q`: query text, `req`: request id string | remote asks server to YouTube-search "`q` karaoke" |
| `reserve` | `videoId`, `title`, `channel`, `thumb` | add to end of queue |
| `cancel` | `id`: queue item id | remove item from queue |
| `control` | `action`: "next" \| "pause" \| "resume" | playback control (server forwards to screen) |
| `ended` | `id`: queue item id | screen reports current video finished |
| `now` | `videoId`, `title`, `state`: "playing" \| "paused" \| "idle" | screen reports playback state (server broadcasts to remotes) |

## Server → Client

| t | fields | meaning |
|---|--------|---------|
| `results` | `req`: echo of request id, `items`: [{`videoId`,`title`,`channel`,`thumb`}] — or `error` | answer to `search`, sent only to requester |
| `queue` | `items`: [{`id`,`videoId`,`title`,`channel`,`thumb`,`by`}] | full queue state, broadcast on every change |
| `control` | `action`: "next" \| "pause" \| "resume" | forwarded to screen only |
| `now` | `videoId`, `title`, `state`: "playing" \| "paused" \| "idle" | screen's playback state, broadcast to remotes |

## Rules

- Server owns the queue. `id` is assigned by server (`q1`, `q2`, …).
- The screen plays `queue.items[0]`. When it changes, the screen loads the new head.
- On `ended` (or `control`/`next`), the server drops the head and broadcasts the new queue.
- `reserve` while idle → head becomes the new song → screen starts it.
- `search` with empty results → `items: []`. If the server has no YouTube API key → `error: "no_key"`.
- Server pings every 25 s; clients answer pong (standard WS ping/pong frames).
