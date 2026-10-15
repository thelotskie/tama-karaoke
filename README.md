# Tama Karaoke v2 — YouTube edition

The old song book (34,820 titles) and the MIDI starter pack are gone.
All music now comes from YouTube — every search automatically looks for the
**karaoke** version. A phone remote app pairs by scanning a QR on the screen.

## What's inside

- `index.html` — the karaoke screen app (search, queue, player, pairing QR)
- `server.py` — tiny host server. Serves the page, searches YouTube for you,
  and relays the phone remote. Python 3 only, nothing to install.
- `yt_key.txt` — paste your YouTube API key here
- `START-KARAOKE.bat` — double-click to run (Windows)
- `assets/` — the 3 background photos
- `PROTOCOL.md` — the phone-remote message protocol (for developers)

## Setup (one time, ~5 minutes)

1. **Python** — install from python.org (check "Add python.exe to PATH").
2. **YouTube API key** (free):
   - Go to console.cloud.google.com → create a project (any name)
   - "APIs & Services → Library" → enable **YouTube Data API v3**
   - "APIs & Services → Credentials" → Create Credentials → API key
   - Copy the key into `yt_key.txt` (replace the placeholder line)
3. **Remote APK** — install `KaraokeRemote-v1.0.apk` on your Android phone.

Free quota note: Google gives 10,000 units/day; one search costs 100 units,
so roughly **100 searches a day** on the free key. Plenty for a karaoke night.

## Every time you sing

1. Double-click `START-KARAOKE.bat` — the karaoke page opens in your browser.
2. Click **TAP TO START** (this unlocks sound so songs autoplay).
3. On your phone, open Karaoke Remote → **Scan** → point at the QR on the
   TV screen (Remote button, top-right).
4. Search songs on the phone, hit **Reserve**, sing. Cancel or Skip anytime.

Phone and PC must be on the **same Wi-Fi**.

## Honest limits

- The API key lives only on your PC — `server.py` never serves it out.
- A few YouTube owners block embedding; those songs auto-skip to the next.
- No key/tempo changer: YouTube's player doesn't offer it, so v2 plays
  songs as-is (speed control only).
