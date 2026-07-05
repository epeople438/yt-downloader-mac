# Download Engine Policy

This app intentionally pins the YouTube download engine instead of always using
the newest release.

## Current Verified Versions

- `yt-dlp==2025.12.08`
- `yt-dlp-ejs==0.3.2`

These versions were verified against YouTube audio extraction for videos that
only expose HLS formats in the embedded macOS runtime.

## Why Versions Are Pinned

YouTube frequently changes player JavaScript, stream manifests, login checks,
SABR behavior, and PO Token requirements. Newer `yt-dlp` versions often fix
those changes, but they can also regress specific embedded runtime cases.

Do not change `yt-dlp` with a broad `>=` range. Test a candidate version first,
then pin the exact version in `requirements.txt`.

## Maintenance Rule

When downloads break:

1. Reproduce with the same URL using the embedded runtime.
2. Test candidate `yt-dlp` and `yt-dlp-ejs` versions.
3. Pin the exact working versions in `requirements.txt`.
4. Rebuild the app so the embedded runtime is regenerated.

## Optional Missing-Subtitle Transcription

When a YouTube video has no downloadable captions, the app can optionally call a
local `whisper` command to transcribe English SRT subtitles from the downloaded
media, then pass that SRT into the existing Codex translation flow.

Whisper is intentionally not bundled into the DMG because model/runtime
dependencies can be very large. Install it on the Mac when this feature is
needed:

```bash
python3 -m pip install -U openai-whisper
```
