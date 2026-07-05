from __future__ import annotations

import asyncio
import glob
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import urllib.parse
import urllib.request
import socket
import ipaddress
from datetime import datetime, timezone
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import shutil

import yt_dlp
from fastapi import WebSocket
from yt_dlp.utils import DownloadError

from .models import DownloadTask, TaskCancel, TaskControl, TaskPause
from .utils import (
    ensure_dir,
    format_bytes,
    open_in_file_manager,
    percent_to_float,
    to_ytdlp_outtmpl,
    detect_system_proxy,
    strip_ansi,
    format_speed,
    format_eta,
    find_ffmpeg,
    ffmpeg_has_subtitles_filter,
)


DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# IMPORTANT (2025+):
# - Some clients (tv_simply/android/ios) may require PO Token -> formats skipped / 403.
# - Some clients (web_safari) can get SABR "missing url" formats depending on rollout.
# For most users, plain web clients are the least surprising default.
#
# NOTE: yt-dlp expects extractor-args values as *iterables of strings*.
# If you pass a plain string, yt-dlp will iterate it character-by-character,
# causing warnings like: "Skipping unsupported client 'w'".
#
# Keep defaults conservative to avoid PO Token clients (android/ios/tv_simply).
YOUTUBE_CLIENTS_DEFAULT = ["web", "mweb"]
YOUTUBE_CLIENTS_FALLBACK = ["web_safari", "web", "mweb"]
# For subtitle downloads: android/tv_simply clients don't require PO Token for subtitles.
YOUTUBE_CLIENTS_SUBTITLES = ["android", "tv_simply", "web"]
CODEX_TRANSLATE_BATCH_MAX_BLOCKS = 40
CODEX_TRANSLATE_BATCH_MAX_CHARS = 6000
CODEX_TRANSLATE_TIMEOUT_SEC = 420


class TaskManager:
    def __init__(self, config):
        self._lock = threading.RLock()
        self._ws_lock = threading.RLock()
        # Avoid concurrent WS sends from different download threads.
        self._broadcast_lock = threading.Lock()
        self._persist_lock = threading.Lock()
        self._loop = None  # asyncio event loop for cross-thread WS broadcasts

        self._config = config
        self._executor = ThreadPoolExecutor(max_workers=int(getattr(config, "max_concurrent", 3)))
        self._old_executors: List[ThreadPoolExecutor] = []

        self._tasks: Dict[str, DownloadTask] = {}
        self._controls: Dict[str, TaskControl] = {}
        self._futures: Dict[str, Future] = {}

        self._ws_clients: Set[WebSocket] = set()
        self._proxy_probe_cache: Dict[str, Tuple[float, bool]] = {}
        self._proxy_warned_unreachable: Set[str] = set()
        self._task_media_candidates: Dict[str, Set[str]] = {}
        self._last_persist_ts: float = 0.0
        self._state_dir = Path(__file__).resolve().parent.parent / "data"
        self._tasks_state_path = self._state_dir / "tasks_state.json"

        self._load_task_state()

    def yt_dlp_version(self) -> str:
        try:
            # NOTE: yt_dlp.version is a submodule, not a string.
            # Prefer the canonical version string for API responses.
            v = getattr(yt_dlp, "__version__", None)
            if isinstance(v, str) and v:
                return v
            vmod = getattr(yt_dlp, "version", None)
            v2 = getattr(vmod, "__version__", None) if vmod is not None else None
            return v2 if isinstance(v2, str) and v2 else "unknown"
        except Exception:
            return "unknown"

    def _append_task_log(self, task_id: str, line: str) -> None:
        text = strip_ansi(str(line or "").strip())
        if not text:
            return
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            task.engine_log.append(text)
            if len(task.engine_log) > 300:
                task.engine_log = task.engine_log[-300:]
            task.touch()

    def _make_ydl_logger(self, task_id: str):
        manager = self

        class _Logger:
            def debug(self, msg):
                s = str(msg or "").strip()
                if s and not s.startswith("[debug]"):
                    manager._append_task_log(task_id, s)

            def warning(self, msg):
                manager._append_task_log(task_id, f"WARNING: {msg}")

            def error(self, msg):
                manager._append_task_log(task_id, f"ERROR: {msg}")

        return _Logger()

    def update_config(self, config) -> None:
        with self._lock:
            old = self._config
            self._config = config
            # If max_concurrent changed, swap executors for new submissions.
            if int(getattr(old, "max_concurrent", 3)) != int(getattr(config, "max_concurrent", 3)):
                new_exec = ThreadPoolExecutor(max_workers=int(getattr(config, "max_concurrent", 3)))
                self._old_executors.append(self._executor)
                self._executor = new_exec
                # Best-effort shutdown of old executor (running tasks continue).
                try:
                    self._old_executors[-1].shutdown(wait=False, cancel_futures=False)
                except Exception:
                    pass

    def set_event_loop(self, loop) -> None:
        """Attach the FastAPI/uvicorn event loop for WS broadcasts.

        Download tasks run in worker threads and must schedule async WebSocket
        sends onto the main event loop.
        """
        self._loop = loop

    def _default_user_agent(self) -> str:
        return DEFAULT_UA

    def _remote_components(self) -> Optional[set]:
        """Enable yt-dlp remote components (EJS challenge solver) when supported.

        Newer yt-dlp versions gate some YouTube JS challenge solvers behind the
        `remote_components` allowlist (e.g. `ejs:github`, `ejs:npm`). When not
        enabled, extraction can get stuck at 0% with SABR / missing-url formats.

        Unknown params are ignored by older yt-dlp, so this is safe.
        """
        try:
            from yt_dlp.globals import supported_remote_components

            comps = list(getattr(supported_remote_components, "value", []) or [])
            return set(comps) if comps else None
        except Exception:
            return None

    def _effective_proxy(self) -> Optional[str]:
        """Return a proxy URL if configured or discoverable.

        Many users run this tool on networks where YouTube/GitHub is blocked or unstable.
        When launched via double-click (.command), shell env may not load, so we also
        try macOS system proxy settings.
        """
        p = (getattr(self._config, "proxy", "") or "").strip()
        if not p:
            p = (detect_system_proxy() or "").strip()
        if not p:
            return None

        # If proxy points to localhost but local service is not running, bypass it
        # to avoid long "preparing" stalls caused by repeated proxy timeouts.
        host, _ = self._proxy_host_port(p)
        if host and self._is_loopback_host(host):
            if not self._is_proxy_reachable_cached(p):
                if p not in self._proxy_warned_unreachable:
                    print(f"[proxy] Local proxy seems unreachable, bypassing: {p}", flush=True)
                    self._proxy_warned_unreachable.add(p)
                return None
            self._proxy_warned_unreachable.discard(p)
        return p

    def _manual_cookies_file(self) -> Optional[str]:
        raw = (getattr(self._config, "cookies_file", "") or "").strip()
        if not raw:
            return None
        try:
            p = Path(raw).expanduser()
            if p.exists() and p.is_file():
                return str(p)
        except Exception:
            return None
        return None

    def _stability_mode_enabled(self) -> bool:
        return bool(getattr(self._config, "stability_mode", False))

    def _proxy_host_port(self, proxy_url: str) -> Tuple[Optional[str], Optional[int]]:
        try:
            u = urllib.parse.urlparse(proxy_url)
        except Exception:
            return None, None

        host = u.hostname
        port = u.port
        if port is None:
            scheme = (u.scheme or "").lower()
            if scheme == "https":
                port = 443
            elif scheme in {"http"}:
                port = 80
            elif scheme.startswith("socks"):
                port = 1080
        return host, port

    def _is_loopback_host(self, host: str) -> bool:
        h = (host or "").strip().strip("[]").lower()
        if not h:
            return False
        if h == "localhost":
            return True
        try:
            return ipaddress.ip_address(h).is_loopback
        except ValueError:
            return False

    def _probe_proxy_once(self, proxy_url: str, timeout: float = 0.8) -> bool:
        host, port = self._proxy_host_port(proxy_url)
        if not host or not port:
            return False
        try:
            with socket.create_connection((host, int(port)), timeout=timeout):
                return True
        except OSError:
            return False

    def _is_proxy_reachable_cached(self, proxy_url: str) -> bool:
        now = time.time()
        cached = self._proxy_probe_cache.get(proxy_url)
        if cached and (now - cached[0]) < 8.0:
            return cached[1]

        ok = self._probe_proxy_once(proxy_url)
        self._proxy_probe_cache[proxy_url] = (now, ok)
        return ok

    def _url_looks_like_playlist(self, url: str) -> bool:
        u = (url or "").lower()
        if "list=" in u:
            return True
        if "/playlist" in u:
            return True
        return False

    def _extract_youtube_video_id(self, url: str) -> Optional[str]:
        try:
            u = urllib.parse.urlparse(url or "")
            host = (u.netloc or "").lower()
            path = (u.path or "").strip("/")
            if "youtu.be" in host and path:
                return path.split("/")[0]
            if "youtube.com" in host:
                q = urllib.parse.parse_qs(u.query or "")
                v = (q.get("v") or [None])[0]
                if v:
                    return str(v)
        except Exception:
            return None
        return None

    def _extract_youtube_like_id_from_filename(self, name: str) -> Optional[str]:
        """Best-effort parse a YouTube-like 11-char id from a filename."""
        stem = Path(name or "").stem
        if not stem:
            return None

        # Common yt-dlp naming pattern: "... [XXXXXXXXXXX]"
        m = re.search(r"\[([A-Za-z0-9_-]{11})\]", stem)
        if m:
            return m.group(1)

        # Alternate pattern: "...-XXXXXXXXXXX" / "..._XXXXXXXXXXX" / "... XXXXXXXXXXX"
        m = re.search(r"(?:^|[-_ ])([A-Za-z0-9_-]{11})$", stem)
        if m:
            return m.group(1)
        return None

    def _youtube_watch_url(self, video_id: str) -> str:
        return f"https://www.youtube.com/watch?v={video_id}"

    def _path_key(self, p: Path) -> str:
        try:
            return str(p.resolve())
        except Exception:
            return str(p)

    def _download_archive_path(self, task: DownloadTask) -> Path:
        archive_dir = self._state_dir / "download_archives"
        archive_dir.mkdir(parents=True, exist_ok=True)
        return archive_dir / f"{task.id}.txt"

    def _normalize_media_name(self, raw: str) -> str:
        s = str(raw or "").strip()
        if not s:
            return ""
        return Path(s).name.strip().lower()

    def _normalize_media_name_list(self, names: Optional[List[str]]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for raw in (names or []):
            n = self._normalize_media_name(raw)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(n)
        return out

    def _dedupe_media_names_preserve_case(self, names: Optional[List[str]]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for raw in (names or []):
            s = str(raw or "").strip()
            if not s:
                continue
            key = self._normalize_media_name(s)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(Path(s).name)
        return out

    def _is_http_url(self, raw: str) -> bool:
        s = str(raw or "").strip().lower()
        return s.startswith("http://") or s.startswith("https://")

    def _normalize_bilingual_layout(self, layout: object) -> str:
        value = str(layout or "").strip().lower()
        return "split_cn_top_en_bottom" if value == "split_cn_top_en_bottom" else ""

    def _should_use_split_bilingual_layout(self, task: Optional[DownloadTask]) -> bool:
        if not task or task.type != "video":
            return False
        if not bool(getattr(task, "subtitles_burnin", False)):
            return False
        return self._normalize_bilingual_layout(getattr(task, "subtitles_bilingual_layout", "")) == "split_cn_top_en_bottom"

    def _create_scan_report_task(
        self,
        *,
        source_url: str,
        save_dir: Path,
        fmt: str,
        quality: str,
        subtitles_langs: str,
        subtitles_codex_strict: bool = False,
        subtitles_bilingual_layout: str,
        subtitles_burnin: bool,
        subtitles_embed: bool,
        total: int,
        completed_guess: List[str],
        pending: List[str],
        missing_all: List[str],
        message: str,
    ) -> DownloadTask:
        task_id = str(uuid.uuid4())
        report_url = source_url if self._is_http_url(source_url) else "about:folder-scan-report"
        task = DownloadTask(
            id=task_id,
            url=report_url,
            type="video",
            format=str(fmt or "mp4"),
            quality=str(quality or "1080"),
            save_path=str(save_dir),
            filename_template=getattr(self._config, "filename_template", "{uploader} - {title}.{ext}"),
            subtitle_only=True,
            subtitle_existing_only=True,
            subtitles_download=False,
            subtitles_embed=bool(subtitles_embed),
            subtitles_burnin=bool(subtitles_burnin),
            subtitles_langs=(subtitles_langs or "zh,en"),
            subtitles_translate="",
            subtitles_codex_strict=bool(subtitles_codex_strict),
            subtitles_bilingual_layout=self._normalize_bilingual_layout(subtitles_bilingual_layout),
            subtitles_review_mode=False,
            subtitle_target_files=list(self._dedupe_media_names_preserve_case(pending)),
        )
        task.status = "completed"
        task.progress = 100.0
        task.speed = "-"
        task.eta = "--"
        task.title = f"目录扫描报告（{total}）"
        task.subtitle_missing_files = list(self._dedupe_media_names_preserve_case(missing_all))
        task.subtitle_failed_files = []
        task.subtitle_processed_files = int(len(completed_guess))
        task.subtitle_skipped_files = int(len(missing_all))
        task.subtitle_failed_count = 0
        task.subtitle_note = message
        task.touch()

        with self._lock:
            self._tasks[task_id] = task
            self._controls[task_id] = TaskControl()
            self._task_media_candidates.setdefault(task_id, set())

        self._push_task_update(task_id)
        return task

    def _list_media_files(self, save_dir: Path) -> List[Path]:
        media_exts = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".m4a", ".mp3", ".flac"}
        files: List[Path] = []
        try:
            for p in save_dir.iterdir():
                if not p.is_file():
                    continue
                if p.name.endswith(".part"):
                    continue
                if p.suffix.lower() not in media_exts:
                    continue
                files.append(p)
        except Exception:
            return []
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return files

    def _subtitle_duplicate_noise_score(self, p: Path) -> int:
        """Lower is better. Penalize likely duplicate/noisy subtitle files."""
        name_l = p.name.lower()
        stem_l = p.stem.lower()
        score = 0

        # Repeated generated suffixes like ".bilingual.bilingual.srt" / ".zh.zh.srt"
        if ".bilingual.bilingual" in stem_l or ".zh.zh" in stem_l:
            score += 4
        if ".bilingual.zh" in stem_l or ".zh.bilingual" in stem_l:
            score += 2

        # Common duplicate naming from Finder/browser downloads.
        if re.search(r"\s\(\d+\)(?=\.[^.]+$)", name_l):
            score += 3
        if re.search(r"(?:[-_])\d+(?=\.[^.]+$)", name_l):
            score += 1
        if " copy" in stem_l:
            score += 2

        # Prefer SRT for ffmpeg filter stability.
        ext = p.suffix.lower()
        if ext == ".srt":
            score += 0
        elif ext == ".ass":
            score += 1
        elif ext == ".vtt":
            score += 2
        else:
            score += 3

        # Tiny files are often broken/incomplete.
        try:
            if p.stat().st_size < 32:
                score += 2
        except Exception:
            score += 1
        return score

    def _plan_batch_subtitle_matches(
        self, task: DownloadTask, media_files: List[Path]
    ) -> Tuple[Dict[str, Path], int, List[str], List[str]]:
        """Pre-check and build one-to-one media->subtitle mapping for batch processing."""
        planned: Dict[str, Path] = {}
        used_subtitles: Set[str] = set()
        multi_candidate_media = 0
        missing_samples: List[str] = []
        missing_all: List[str] = []
        missing_seen: Set[str] = set()

        for media in media_files:
            cands = self._subtitle_candidates(media, task)
            if len(cands) > 1:
                multi_candidate_media += 1

            sub = self._pick_subtitle_file(task, media, used_subtitles=used_subtitles)
            if not sub or not sub.exists():
                if media.name not in missing_seen:
                    missing_seen.add(media.name)
                    missing_all.append(media.name)
                if len(missing_samples) < 6:
                    missing_samples.append(media.name)
                continue

            planned[self._path_key(media)] = sub
            used_subtitles.add(self._path_key(sub))

        return planned, multi_candidate_media, missing_samples, missing_all

    def _remember_media_candidate(self, task_id: str, filename: Optional[str]) -> None:
        if not filename:
            return
        s = str(filename).strip()
        if not s:
            return

        p = Path(s)
        ext = p.suffix.lower()
        media_exts = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".m4a", ".mp3", ".flac"}

        # yt-dlp often reports temporary *.part paths while downloading.
        if p.name.endswith(".part"):
            candidate = Path(str(p)[:-5])
            if candidate.suffix.lower() in media_exts:
                self._task_media_candidates.setdefault(task_id, set()).add(str(candidate))
            return

        if ext in media_exts:
            self._task_media_candidates.setdefault(task_id, set()).add(str(p))

    def _resolve_task_media_files(self, task_id: str, task: DownloadTask, save_dir: Path) -> List[Path]:
        """Resolve media files for the task (single video or playlist-like task)."""
        media_exts = {".mp4", ".mkv", ".webm", ".m4a", ".mp3", ".flac"}
        files: List[Path] = []
        seen: Set[str] = set()
        target_names = self._normalize_media_name_list(getattr(task, "subtitle_target_files", []))
        target_set = set(target_names)
        target_stems = {Path(n).stem for n in target_names}

        def is_target_match(p: Path) -> bool:
            if not target_set:
                return True
            n = p.name.lower()
            if n in target_set:
                return True
            return p.stem.lower() in target_stems

        def add_path(p: Path) -> None:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen:
                return
            if not p.exists() or not p.is_file():
                return
            if p.suffix.lower() not in media_exts:
                return
            if p.name.endswith(".part"):
                return
            if not is_target_match(p):
                return
            seen.add(rp)
            files.append(p)

        # Prefer exact candidates captured from progress hooks.
        for s in self._task_media_candidates.get(task_id, set()):
            add_path(Path(s))

        # Keep compatibility with old single-file flow.
        try:
            if task.filename:
                p = Path(task.filename)
                if p.name.endswith(".part"):
                    p = Path(str(p)[:-5])
                add_path(p)
        except Exception:
            pass

        # If we still have no candidate, fallback to directory scan.
        if not files:
            try:
                pool = [p for p in save_dir.iterdir() if p.is_file() and p.suffix.lower() in media_exts and not p.name.endswith(".part")]
            except Exception:
                pool = []

            if pool:
                is_playlist_task = self._url_looks_like_playlist(task.url)
                # Prefer files created/updated after this task was created.
                if not bool(getattr(task, "subtitle_only", False)):
                    try:
                        created_ts = float(task.created_at.timestamp()) - 300.0
                        recent = [p for p in pool if p.stat().st_mtime >= created_ts]
                        if recent:
                            pool = recent
                    except Exception:
                        pass

                vid = self._extract_youtube_video_id(task.url)
                if vid and not is_playlist_task:
                    by_id = [p for p in pool if vid.lower() in p.name.lower()]
                    if by_id:
                        pool = by_id

                title = (task.title or "").strip()
                if title and not title.startswith("解析中") and not is_playlist_task:
                    title_l = title.lower()
                    matched = [p for p in pool if title_l[:20] in p.name.lower()]
                    if matched:
                        pool = matched

                pool.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                # If it looks like playlist URL, keep all candidates; otherwise keep best one.
                if is_playlist_task or bool(getattr(task, "local_video_subtitles", False)) or len(target_set) > 1:
                    for p in pool:
                        add_path(p)
                else:
                    add_path(pool[0])
        else:
            files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        return files

    def _collect_task_output_files(self, task_id: str, task: DownloadTask, save_dir: Path) -> List[str]:
        output_exts = {
            ".mp4", ".mkv", ".webm", ".mov", ".m4v",
            ".mp3", ".m4a", ".flac", ".wav", ".opus",
            ".srt", ".vtt", ".ass",
        }
        files: List[Path] = []
        seen: Set[str] = set()

        def add(p: Path) -> None:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp in seen:
                return
            if not p.exists() or not p.is_file() or p.name.endswith(".part"):
                return
            if p.suffix.lower() not in output_exts:
                return
            seen.add(rp)
            files.append(p)

        for p in self._resolve_task_media_files(task_id, task, save_dir):
            add(p)
            stem = p.stem
            try:
                for sidecar in p.parent.glob(stem + "*"):
                    add(sidecar)
            except Exception:
                pass

        if task.filename:
            p = Path(task.filename)
            add(p)
            if p.exists():
                try:
                    for sidecar in p.parent.glob(p.stem + "*"):
                        add(sidecar)
                except Exception:
                    pass

        if not files:
            try:
                created_ts = float(task.created_at.timestamp()) - 300.0
                pool = [
                    p for p in save_dir.iterdir()
                    if p.is_file()
                    and p.suffix.lower() in output_exts
                    and not p.name.endswith(".part")
                    and p.stat().st_mtime >= created_ts
                ]
                pool.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                for p in pool[:50]:
                    add(p)
            except Exception:
                pass

        files.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
        return [str(p) for p in files[:80]]

    def _refresh_task_outputs(self, task_id: str) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            try:
                save_dir = Path(task.save_path)
                task.output_files = self._collect_task_output_files(task_id, task, save_dir)
                task.touch()
            except Exception:
                pass

    # ---------------------- WebSocket management ----------------------
    def ws_connect(self, ws: WebSocket) -> None:
        with self._ws_lock:
            self._ws_clients.add(ws)

    def ws_disconnect(self, ws: WebSocket) -> None:
        with self._ws_lock:
            self._ws_clients.discard(ws)

    async def _broadcast_async(self, payload: dict) -> None:
        dead: List[WebSocket] = []
        with self._ws_lock:
            clients = list(self._ws_clients)
        for ws in clients:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        if dead:
            with self._ws_lock:
                for ws in dead:
                    self._ws_clients.discard(ws)

    def broadcast(self, payload: dict) -> None:
        """Thread-safe broadcast to all WebSocket clients.

        Download tasks run in worker threads, so we must schedule async sends
        onto the server event loop.
        """
        with self._broadcast_lock:
            loop = self._loop
            if loop is not None and getattr(loop, 'is_running', lambda: False)():
                try:
                    asyncio.run_coroutine_threadsafe(self._broadcast_async(payload), loop)
                except Exception:
                    pass
                return

            # Fallback: if called from within the event loop thread.
            try:
                running = asyncio.get_running_loop()
                if running is not None and running.is_running():
                    asyncio.create_task(self._broadcast_async(payload))
            except Exception:
                pass

    def _push_task_update(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if not task:
            return
        if task.status in {"completed", "error", "paused", "canceled", "review_pending"} or float(task.progress or 0.0) >= 99.0:
            self._refresh_task_outputs(task_id)
            task = self._tasks.get(task_id)
            if not task:
                return
        self.broadcast({"type": "update", "task": task.to_public_dict()})
        self._persist_task_state(force=False)

    def _parse_datetime(self, raw: object) -> datetime:
        s = str(raw or "").strip()
        if not s:
            return datetime.utcnow()
        try:
            # Compatible with ISO strings emitted by datetime.isoformat().
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return datetime.utcnow()

    def _snapshot_task(self, t: DownloadTask) -> dict:
        d = t.to_public_dict()
        d["created_at"] = (t.created_at or datetime.utcnow()).isoformat()
        d["updated_at"] = (t.updated_at or datetime.utcnow()).isoformat()
        d["filename_template"] = t.filename_template
        return d

    def _task_from_snapshot(self, d: dict) -> Optional[DownloadTask]:
        try:
            task_id = str(d.get("task_id") or d.get("id") or uuid.uuid4())
            t = DownloadTask(
                id=task_id,
                url=str(d.get("url") or ""),
                type=str(d.get("type") or "video"),
                format=str(d.get("format") or "mp4"),
                quality=str(d.get("quality") or "best"),
                save_path=str(d.get("save_path") or getattr(self._config, "default_path", str(Path.home()))),
                filename_template=str(d.get("filename_template") or getattr(self._config, "filename_template", "{uploader} - {title}.{ext}")),
                subtitles_download=bool(d.get("subtitles_download", False)),
                subtitles_embed=bool(d.get("subtitles_embed", False)),
                subtitles_burnin=bool(d.get("subtitles_burnin", False)),
                subtitle_only=bool(d.get("subtitle_only", False)),
                local_video_subtitles=bool(d.get("local_video_subtitles", False)),
                subtitle_existing_only=bool(d.get("subtitle_existing_only", False)),
                subtitles_langs=str(d.get("subtitles_langs") or "zh,en"),
                subtitles_translate=str(d.get("subtitles_translate") or ""),
                subtitles_codex_strict=bool(d.get("subtitles_codex_strict", False)),
                subtitles_transcribe_missing=bool(d.get("subtitles_transcribe_missing", False)),
                subtitles_bilingual_layout=self._normalize_bilingual_layout(d.get("subtitles_bilingual_layout")),
                subtitles_review_mode=bool(d.get("subtitles_review_mode", False)),
                subtitle_note=str(d.get("subtitle_note") or ""),
                subtitle_file_path=str(d.get("subtitle_file_path") or ""),
                subtitle_target_files=self._dedupe_media_names_preserve_case(d.get("subtitle_target_files") or []),
                subtitle_missing_files=self._dedupe_media_names_preserve_case(d.get("subtitle_missing_files") or []),
                subtitle_failed_files=self._dedupe_media_names_preserve_case(d.get("subtitle_failed_files") or []),
                subtitle_processed_files=int(d.get("subtitle_processed_files") or 0),
                subtitle_skipped_files=int(d.get("subtitle_skipped_files") or 0),
                subtitle_failed_count=int(d.get("subtitle_failed_count") or 0),
                status=str(d.get("status") or "queued"),
                progress=float(d.get("progress") or 0.0),
                speed=str(d.get("speed") or "0 KB/s"),
                eta=str(d.get("eta") or "--"),
                title=str(d.get("title") or "解析中..."),
                uploader=str(d.get("uploader") or ""),
                filename=str(d.get("filename") or ""),
                total_size=str(d.get("total_size") or "--"),
                thumbnail=str(d.get("thumbnail") or ""),
                error_message=str(d.get("error_message") or ""),
                engine_log=[str(x) for x in (d.get("engine_log") or [])][-300:],
                output_files=[str(x) for x in (d.get("output_files") or [])],
                created_at=self._parse_datetime(d.get("created_at")),
                updated_at=self._parse_datetime(d.get("updated_at")),
            )

            # App restart recovery: running states cannot continue automatically.
            if t.status in {"queued", "preparing", "downloading", "processing"}:
                t.status = "paused"
                if not t.subtitle_note:
                    t.subtitle_note = "应用重启后任务已暂停，可点击继续。"
            return t
        except Exception:
            return None

    def _load_task_state(self) -> None:
        try:
            if not self._tasks_state_path.exists():
                return
            raw = json.loads(self._tasks_state_path.read_text(encoding="utf-8"))
            items = raw.get("tasks", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
            if not isinstance(items, list):
                return

            restored = 0
            for obj in items:
                if not isinstance(obj, dict):
                    continue
                t = self._task_from_snapshot(obj)
                if not t:
                    continue
                self._tasks[t.id] = t
                self._controls[t.id] = TaskControl()
                self._task_media_candidates.setdefault(t.id, set())
                restored += 1

            if restored:
                print(f"[state] Restored {restored} tasks from disk.", flush=True)
        except Exception as e:
            print(f"[state] Failed to load tasks_state: {e}", flush=True)

    def _persist_task_state(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_persist_ts) < 1.0:
            return

        with self._persist_lock:
            now = time.time()
            if not force and (now - self._last_persist_ts) < 1.0:
                return
            try:
                self._state_dir.mkdir(parents=True, exist_ok=True)
                with self._lock:
                    tasks = sorted(self._tasks.values(), key=lambda t: t.created_at, reverse=True)
                    payload = {
                        "version": 1,
                        "updated_at": datetime.utcnow().isoformat(),
                        "tasks": [self._snapshot_task(t) for t in tasks],
                    }
                tmp = self._tasks_state_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                os.replace(str(tmp), str(self._tasks_state_path))
                self._last_persist_ts = now
            except Exception as e:
                print(f"[state] Failed to persist tasks_state: {e}", flush=True)

    # ---------------------- Task list operations ----------------------
    def list_tasks(self) -> List[DownloadTask]:
        with self._lock:
            # Return newest first
            return sorted(self._tasks.values(), key=lambda t: t.created_at, reverse=True)

    def create_tasks(
        self,
        urls: List[str],
        dl_type: str,
        fmt: str,
        quality: str,
        save_path: str,
        subtitle_only: bool = False,
        local_video_subtitles: bool = False,
        subtitle_existing_only: bool = False,
        subtitles_download: bool = False,
        subtitles_embed: bool = False,
        subtitles_burnin: bool = False,
        subtitles_langs: str = "zh,en",
        subtitles_translate: str = "",
        subtitles_codex_strict: bool = False,
        subtitles_transcribe_missing: bool = False,
        subtitles_bilingual_layout: str = "",
        subtitles_review_mode: bool = False,
        subtitle_target_files: Optional[List[str]] = None,
    ) -> List[DownloadTask]:
        created: List[DownloadTask] = []
        save_dir = ensure_dir(save_path or getattr(self._config, "default_path", str(Path.home())))
        filename_template = getattr(self._config, "filename_template", "{uploader} - {title}.{ext}")
        normalized_targets = self._normalize_media_name_list(subtitle_target_files)

        for raw in urls:
            url = (raw or "").strip()
            if not url:
                continue

            task_local_video = bool(local_video_subtitles) if dl_type == "video" else False
            task_subtitle_only = (bool(subtitle_only) or task_local_video) if dl_type == "video" else False
            task_existing_only = bool(subtitle_existing_only) if dl_type == "video" else False
            task_burnin = bool(subtitles_burnin) if dl_type == "video" else False
            task_embed = bool(subtitles_embed) if dl_type == "video" else False
            task_download = bool(subtitles_download) if dl_type == "video" else False
            task_translate = (subtitles_translate or "") if dl_type == "video" else ""
            task_codex_strict = bool(subtitles_codex_strict) if dl_type == "video" else False
            task_transcribe_missing = bool(subtitles_transcribe_missing) if dl_type == "video" else False
            task_bilingual_layout = self._normalize_bilingual_layout(subtitles_bilingual_layout) if dl_type == "video" else ""
            task_review_mode = bool(subtitles_review_mode) if dl_type == "video" else False

            # Server-side normalization so API callers also get consistent behavior.
            if task_burnin:
                task_embed = False
                if not task_existing_only:
                    task_download = True
            else:
                task_bilingual_layout = ""
                task_review_mode = False

            if task_existing_only:
                task_subtitle_only = True
                task_local_video = False
                task_download = False
                if not task_burnin and not task_embed:
                    fmt_l = (fmt or "").lower()
                    if fmt_l == "mp4":
                        task_burnin = True
                    elif fmt_l in {"mkv"}:
                        task_embed = True

            task_id = str(uuid.uuid4())
            task = DownloadTask(
                id=task_id,
                url=url,
                type=dl_type,
                format=fmt,
                quality=quality,
                save_path=str(save_dir),
                filename_template=filename_template,
                subtitle_only=task_subtitle_only,
                local_video_subtitles=task_local_video,
                subtitle_existing_only=task_existing_only,
                subtitles_burnin=task_burnin,
                subtitles_download=task_download,
                subtitles_embed=task_embed,
                subtitles_langs=(subtitles_langs or "zh,en"),
                subtitles_translate=task_translate,
                subtitles_codex_strict=task_codex_strict,
                subtitles_transcribe_missing=task_transcribe_missing,
                subtitles_bilingual_layout=task_bilingual_layout,
                subtitles_review_mode=task_review_mode,
                subtitle_target_files=list(normalized_targets),
            )

            with self._lock:
                self._tasks[task_id] = task
                self._controls[task_id] = TaskControl()
                self._task_media_candidates.setdefault(task_id, set())

            created.append(task)
            self._push_task_update(task_id)
            self._submit(task_id)

        return created

    def _retry_target_files_from_task(self, task: DownloadTask) -> List[str]:
        raw = list(getattr(task, "subtitle_missing_files", []) or []) + list(getattr(task, "subtitle_failed_files", []) or [])
        return self._dedupe_media_names_preserve_case(raw)

    def analyze_subtitle_missing(self, task_id: str) -> Tuple[bool, str, dict]:
        task = self._get_task(task_id)
        if not task:
            return False, "任务不存在", {}
        if task.type != "video":
            return False, "仅视频任务支持字幕缺失识别", {}

        save_dir = ensure_dir(task.save_path)
        media_files = self._resolve_task_media_files(task_id, task, save_dir)
        if not media_files:
            return False, "未找到可分析的视频文件", {}

        planned_map, multi_candidate_media, missing_samples, missing_all = self._plan_batch_subtitle_matches(task, media_files)
        total = len(media_files)
        matched = len(planned_map)
        missing = len(missing_all)

        preview = "、".join(missing_all[:3])
        if preview and len(missing_all) > 3:
            preview += f" 等 {len(missing_all)} 项"

        with self._lock:
            t = self._tasks.get(task_id)
            if t:
                t.subtitle_missing_files = list(missing_all)
                t.subtitle_skipped_files = max(int(getattr(t, "subtitle_skipped_files", 0) or 0), missing)
                if missing:
                    t.subtitle_note = (
                        f"字幕缺失识别：总视频 {total}，已匹配 {matched}，缺失 {missing}。"
                        + (f" 缺失文件：{preview}" if preview else "")
                    )
                else:
                    t.subtitle_note = f"字幕缺失识别：总视频 {total}，全部已匹配。"
                t.touch()
        self._push_task_update(task_id)

        return True, (
            f"缺失 {missing} 项" if missing else "未发现缺失项"
        ), {
            "total_media": total,
            "matched_media": matched,
            "missing_count": missing,
            "multi_candidate_media": multi_candidate_media,
            "missing_samples": missing_samples,
            "missing_files": missing_all,
        }

    def retry_missing_subtitles(self, task_id: str, filenames: Optional[List[str]] = None) -> Tuple[bool, str, List[DownloadTask]]:
        with self._lock:
            src = self._tasks.get(task_id)
            if not src:
                return False, "任务不存在", []
            if src.type != "video":
                return False, "仅视频任务支持字幕补处理", []

            src_cfg = {
                "url": src.url,
                "format": src.format,
                "quality": src.quality,
                "save_path": src.save_path,
                "subtitles_embed": bool(getattr(src, "subtitles_embed", False)),
                "subtitles_burnin": bool(getattr(src, "subtitles_burnin", False)),
                "local_video_subtitles": bool(getattr(src, "local_video_subtitles", False)),
                "subtitles_langs": (getattr(src, "subtitles_langs", "") or "zh,en"),
                "subtitles_translate": (getattr(src, "subtitles_translate", "") or ""),
                "subtitles_codex_strict": bool(getattr(src, "subtitles_codex_strict", False)),
                "subtitles_transcribe_missing": bool(getattr(src, "subtitles_transcribe_missing", False)),
                "subtitles_bilingual_layout": self._normalize_bilingual_layout(getattr(src, "subtitles_bilingual_layout", "")),
                "subtitles_review_mode": bool(getattr(src, "subtitles_review_mode", False)),
                "targets": self._retry_target_files_from_task(src),
            }

        targets = list(src_cfg["targets"])
        if not targets:
            ok, msg, report = self.analyze_subtitle_missing(task_id)
            if not ok:
                return False, msg, []
            targets = self._dedupe_media_names_preserve_case((report or {}).get("missing_files") or [])

        requested = self._normalize_media_name_list(filenames)
        if requested:
            allowed_map = {self._normalize_media_name(x): x for x in targets}
            selected: List[str] = []
            for key in requested:
                raw_name = allowed_map.get(key)
                if raw_name and raw_name not in selected:
                    selected.append(raw_name)
            if not selected:
                return False, "指定文件不在当前缺失项列表中", []
            targets = selected

        if not targets:
            return False, "没有可补处理的缺失项", []

        def create_retry_tasks(urls: List[str], target_files: List[str]) -> List[DownloadTask]:
            return self.create_tasks(
                urls=urls,
                dl_type="video",
                fmt=str(src_cfg["format"] or "mp4"),
                quality=str(src_cfg["quality"] or "best"),
                save_path=str(src_cfg["save_path"] or ""),
                subtitle_only=True,
                local_video_subtitles=bool(src_cfg["local_video_subtitles"]),
                subtitle_existing_only=False,
                subtitles_download=True,
                subtitles_embed=bool(src_cfg["subtitles_embed"]),
                subtitles_burnin=bool(src_cfg["subtitles_burnin"]),
                subtitles_langs=str(src_cfg["subtitles_langs"] or "zh,en"),
                subtitles_translate=str(src_cfg["subtitles_translate"] or ""),
                subtitles_codex_strict=bool(src_cfg["subtitles_codex_strict"]),
                subtitles_transcribe_missing=bool(src_cfg["subtitles_transcribe_missing"]),
                subtitles_bilingual_layout=str(src_cfg["subtitles_bilingual_layout"] or ""),
                subtitles_review_mode=bool(src_cfg["subtitles_review_mode"]),
                subtitle_target_files=list(target_files),
            )

        retry_url = str(src_cfg["url"] or "").strip()
        source_url_ok = self._is_http_url(retry_url)
        source_vid = self._extract_youtube_video_id(retry_url) if source_url_ok else None
        playlist_like = source_url_ok and self._url_looks_like_playlist(retry_url)
        target_vid_map: Dict[str, str] = {}
        for media_name in targets:
            vid = self._extract_youtube_like_id_from_filename(media_name)
            if vid:
                target_vid_map[media_name] = vid

        # For single-target retry, always force a single-video URL when possible.
        if len(targets) == 1 and source_vid and targets[0] not in target_vid_map:
            target_vid_map[targets[0]] = source_vid
            retry_url = self._youtube_watch_url(source_vid)
            playlist_like = False

        created: List[DownloadTask] = []
        unresolved_count = 0

        if playlist_like and target_vid_map:
            # Playlist source + specific missing files: split by video id to avoid long playlist subtitle scan.
            for media_name in targets:
                vid = target_vid_map.get(media_name)
                if not vid:
                    unresolved_count += 1
                    continue
                created.extend(create_retry_tasks([self._youtube_watch_url(vid)], [media_name]))
        elif source_url_ok:
            created = create_retry_tasks([retry_url], list(targets))
        else:
            # No source URL available: fallback to per-file URL by parsed video ID.
            for media_name in targets:
                vid = target_vid_map.get(media_name)
                if not vid:
                    unresolved_count += 1
                    continue
                created.extend(create_retry_tasks([self._youtube_watch_url(vid)], [media_name]))
            if not created:
                preview = "、".join(targets[:2])
                return False, f"缺少可用源链接，且无法从文件名提取视频ID：{preview}", []

        if created:
            updated_ids: List[str] = []
            with self._lock:
                for idx, item in enumerate(created, start=1):
                    t = self._tasks.get(item.id)
                    if not t:
                        continue
                    t.title = f"缺失项补处理（{len(targets)}）" if len(created) == 1 else f"缺失项补处理（{idx}/{len(created)}）"
                    t.subtitle_note = (
                        "仅处理指定缺失视频："
                        + "、".join(targets[:2])
                        + (f" 等 {len(targets)} 项" if len(targets) > 2 else "")
                    )
                    t.touch()
                    updated_ids.append(item.id)
            for tid in updated_ids:
                self._push_task_update(tid)

        extra = f"，另有 {unresolved_count} 项无法提取视频ID已跳过" if unresolved_count > 0 else ""
        return True, f"已创建缺失项补处理任务（{len(created)} 个任务，目标 {len(targets)} 项）{extra}", created

    def rebuild_from_folder(
        self,
        save_path: str,
        url: str = "",
        fmt: str = "mp4",
        quality: str = "1080",
        subtitles_burnin: bool = True,
        subtitles_embed: bool = False,
        subtitles_langs: str = "zh,en",
        subtitles_bilingual_layout: str = "",
        dry_run: bool = False,
    ) -> Tuple[bool, str, List[DownloadTask], dict]:
        save_dir = ensure_dir(save_path or getattr(self._config, "default_path", str(Path.home())))
        fmt_l = (fmt or "mp4").lower()
        burnin = bool(subtitles_burnin)
        embed = bool(subtitles_embed) and not burnin

        # Keep server-side constraints aligned with UI behavior.
        if fmt_l != "mp4":
            burnin = False
        if fmt_l not in {"mp4", "mkv"}:
            embed = False
        if not burnin and not embed:
            if fmt_l == "mp4":
                burnin = True
            elif fmt_l == "mkv":
                embed = True

        source_url = (url or "").strip() or "about:folder-scan"
        scan_task = DownloadTask(
            id="__scan__",
            url=source_url,
            type="video",
            format=fmt_l,
            quality=str(quality or "1080"),
            save_path=str(save_dir),
            filename_template=getattr(self._config, "filename_template", "{uploader} - {title}.{ext}"),
            subtitle_only=True,
            subtitle_existing_only=True,
            subtitles_download=False,
            subtitles_embed=embed,
            subtitles_burnin=burnin,
            local_video_subtitles=False,
            subtitles_langs=(subtitles_langs or "zh,en"),
            subtitles_translate="",
            subtitles_bilingual_layout=self._normalize_bilingual_layout(subtitles_bilingual_layout),
            subtitles_review_mode=False,
        )

        # Rebuild scan is for subtitle/video post-processing only.
        # Exclude audio-only files to avoid false "missing subtitle" counts.
        media_files = [p for p in self._list_media_files(save_dir) if p.suffix.lower() in {".mp4", ".mkv", ".webm"}]
        if not media_files:
            return False, "扫描目录下未找到可处理的视频文件。", [], {}

        planned_map, multi_candidate_media, missing_samples, missing_all = self._plan_batch_subtitle_matches(scan_task, media_files)
        pending: List[str] = []
        completed_guess: List[str] = []

        for media in media_files:
            sub = planned_map.get(self._path_key(media))
            if not sub or not sub.exists():
                continue

            try:
                media_mtime = float(media.stat().st_mtime)
                sub_mtime = float(sub.stat().st_mtime)
            except Exception:
                pending.append(media.name)
                continue

            # Heuristic for "already postprocessed":
            # if media file mtime is later than subtitle mtime, it was likely re-encoded/muxed after subtitle prepared.
            if media_mtime > (sub_mtime + 3.0):
                completed_guess.append(media.name)
            else:
                pending.append(media.name)

        total = len(media_files)
        matched = len(planned_map)
        missing = len(missing_all)
        pending = self._dedupe_media_names_preserve_case(pending)
        completed_guess = self._dedupe_media_names_preserve_case(completed_guess)

        report = {
            "total_media": total,
            "matched_media": matched,
            "missing_count": missing,
            "missing_files": list(missing_all),
            "pending_count": len(pending),
            "pending_files": list(pending),
            "completed_guess_count": len(completed_guess),
            "completed_guess_files": list(completed_guess),
            "multi_candidate_media": multi_candidate_media,
            "missing_samples": list(missing_samples),
        }

        # Safety guard: avoid accidental full-batch re-burn caused by low-confidence heuristics.
        unsafe_bulk = total >= 8 and len(pending) >= max(8, int(total * 0.8))
        if dry_run:
            return True, f"扫描完成：待补处理 {len(pending)}，缺字幕 {missing}。", [], report
        if unsafe_bulk:
            return True, (
                f"扫描完成：待补处理 {len(pending)} / {total}，数量过大，已阻止自动重建。"
                "建议先手动检查并使用‘识别缺失项/仅处理此项’。"
            ), [], report
        if not pending:
            summary = f"扫描完成：未发现待补处理项（缺字幕 {missing}）。"
            # Always create a visible report task so user can inspect and trigger retry flows.
            report_task = self._create_scan_report_task(
                source_url=source_url,
                save_dir=save_dir,
                fmt=fmt_l,
                quality=str(quality or "1080"),
                subtitles_langs=(subtitles_langs or "zh,en"),
                subtitles_bilingual_layout=self._normalize_bilingual_layout(subtitles_bilingual_layout),
                subtitles_burnin=burnin,
                subtitles_embed=embed,
                total=total,
                completed_guess=completed_guess,
                pending=pending,
                missing_all=missing_all,
                message=(
                    f"目录扫描完成：总视频 {total}，推断已完成 {len(completed_guess)}，待补处理 {len(pending)}，缺字幕 {missing}。"
                    "可在右侧详情中对缺失项执行补处理。"
                ),
            )
            return True, summary, [report_task], report

        created = self.create_tasks(
            urls=[source_url],
            dl_type="video",
            fmt=fmt_l,
            quality=str(quality or "1080"),
            save_path=str(save_dir),
            subtitle_only=True,
            subtitle_existing_only=True,  # critical: never redownload media/subtitles
            subtitles_download=False,
            subtitles_embed=embed,
            subtitles_burnin=burnin,
            local_video_subtitles=False,
            subtitles_langs=(subtitles_langs or "zh,en"),
            subtitles_translate="",
            subtitles_bilingual_layout=self._normalize_bilingual_layout(subtitles_bilingual_layout),
            subtitles_review_mode=False,
            subtitle_target_files=list(pending),
        )

        if created:
            first = created[0]
            with self._lock:
                t = self._tasks.get(first.id)
                if t:
                    t.title = f"文件夹重建补处理（{len(pending)}）"
                    miss_preview = "、".join(missing_all[:2])
                    miss_msg = f"；缺字幕 {missing}" + (f"（{miss_preview}）" if miss_preview else "")
                    t.subtitle_note = (
                        f"目录扫描：总视频 {total}，推断已完成 {len(completed_guess)}，待补处理 {len(pending)}{miss_msg}。"
                        "本任务仅处理待补视频，不会重下视频/字幕。"
                    )
                    # Keep missing list on the task so UI can directly offer "补处理缺失项".
                    t.subtitle_missing_files = list(self._dedupe_media_names_preserve_case(missing_all))
                    t.touch()
            self._push_task_update(first.id)

        return True, f"已根据目录重建补处理任务：待补处理 {len(pending)}，缺字幕 {missing}。", created, report

    def _submit(self, task_id: str) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            # Clear control flags for fresh run
            ctrl = self._controls.setdefault(task_id, TaskControl())
            ctrl.pause_requested = False
            ctrl.cancel_requested = False
            fut = self._executor.submit(self._run_download, task_id)
            self._futures[task_id] = fut

    def pause(self, task_id: str) -> Tuple[bool, str]:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False, "任务不存在"
            if task.status in {"completed", "error", "canceled"}:
                return False, f"当前状态不支持暂停: {task.status}"
            self._controls.setdefault(task_id, TaskControl()).pause_requested = True
            task.status = "paused"  # optimistic; worker will exit soon
            task.touch()
        self._push_task_update(task_id)
        return True, "已请求暂停"

    def resume(self, task_id: str) -> Tuple[bool, str]:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False, "任务不存在"
            fut = self._futures.get(task_id)
            if fut and not fut.done():
                return False, "任务正在运行中，无需继续"

            if task.status == "review_pending":
                # Keep existing API behavior for manual subtitle review flow.
                pass
            elif task.status not in {"paused", "error", "processing"}:
                return False, f"当前状态不支持继续: {task.status}"

            # If media already exists and current failure is in subtitle/post-processing,
            # continue from post-processing only to avoid re-downloading the video.
            save_dir = Path(task.save_path)
            local_media = self._resolve_task_media_files(task_id, task, save_dir)
            need_subtitle_flow = bool(
                task.type == "video"
                and (
                    getattr(task, "subtitles_download", False)
                    or getattr(task, "subtitles_embed", False)
                    or getattr(task, "subtitles_burnin", False)
                    or getattr(task, "subtitles_translate", "")
                    or getattr(task, "subtitles_transcribe_missing", False)
                )
            )

            if task.status == "review_pending":
                # For review mode, "resume" means continue burn-in.
                pass
            elif task.status in {"error", "processing"} and need_subtitle_flow and local_media:
                task.status = "queued"
                task.error_message = ""
                task.subtitle_note = "继续字幕处理中（不会重新下载视频）…"
                task.touch()
                self._push_task_update(task_id)
                self._submit_postprocess_only(task_id)
                return True, "已继续字幕处理（不会重下视频）"
            else:
                task.status = "queued"
                task.touch()
        self._push_task_update(task_id)
        if task.status == "review_pending":
            return self.continue_burnin(task_id)
        self._submit(task_id)
        return True, "已继续"

    def _submit_postprocess_only(self, task_id: str) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return
            ctrl = self._controls.setdefault(task_id, TaskControl())
            ctrl.pause_requested = False
            ctrl.cancel_requested = False
            fut = self._executor.submit(self._run_postprocess_only, task_id)
            self._futures[task_id] = fut

    def _run_postprocess_only(self, task_id: str) -> None:
        task = self._get_task(task_id)
        if not task:
            return

        save_dir = ensure_dir(task.save_path)
        outtmpl = to_ytdlp_outtmpl(save_dir, task.filename_template)

        try:
            with self._lock:
                task.status = "processing"
                task.progress = max(task.progress or 0.0, 99.0)
                task.error_message = ""
                task.touch()
            self._push_task_update(task_id)

            if bool(getattr(task, "subtitle_only", False)) and not bool(getattr(task, "subtitle_existing_only", False)):
                if bool(getattr(task, "local_video_subtitles", False)) or not self._is_http_url(task.url):
                    task.subtitle_note = "本地视频字幕模式：继续处理已有视频…"
                else:
                    task.subtitle_note = "字幕模式：继续获取字幕…"
                    self._download_subtitles_only_best_effort(task, outtmpl)

            self._postprocess_subtitles(task_id, save_dir, outtmpl)

            with self._lock:
                t = self._tasks.get(task_id)
                if t and t.status not in {"error", "canceled", "review_pending"}:
                    t.status = "completed"
                    t.progress = 100.0
                    t.speed = "-"
                    t.eta = "--"
                    t.touch()
            self._push_task_update(task_id)
        except Exception as e:
            with self._lock:
                t = self._tasks.get(task_id)
                if t:
                    t.status = "error"
                    t.error_message = self._friendly_error(str(e))
                    t.touch()
            self._push_task_update(task_id)

    def continue_burnin(self, task_id: str) -> Tuple[bool, str]:
        """Continue burn-in after user has reviewed/edited the subtitle file."""
        task = self._get_task(task_id)
        if not task:
            return False, "任务不存在"
        if task.status != "review_pending":
            return False, f"当前状态不是等待审核: {task.status}"

        sub_path = task.subtitle_file_path
        if not sub_path:
            return False, "字幕文件路径不存在"

        sub = Path(sub_path)
        if not sub.exists():
            return False, f"字幕文件不存在: {sub_path}"

        save_dir = Path(task.save_path)

        # Run burn-in in background thread
        def do_burnin():
            op_result = self._burnin_subtitles_if_possible(task_id, save_dir, sub)
            # After burn-in, mark as completed
            with self._lock:
                t = self._tasks.get(task_id)
                if t and t.status not in {"error", "canceled"}:
                    t.status = "completed"
                    t.progress = 100.0
                    t.subtitle_missing_files = []
                    t.subtitle_failed_files = []
                    t.subtitle_processed_files = 1 if op_result == "ok" else 0
                    t.subtitle_skipped_files = 0 if op_result == "ok" else 1
                    t.subtitle_failed_count = 0 if op_result == "ok" else 1
                    t.touch()
            self._push_task_update(task_id)

        self._executor.submit(do_burnin)
        return True, "正在烧录字幕..."

    def cancel(self, task_id: str) -> Tuple[bool, str]:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False, "任务不存在"
            if task.status in {"completed", "canceled"}:
                return False, f"当前状态不支持取消: {task.status}"
            ctrl = self._controls.setdefault(task_id, TaskControl())
            ctrl.cancel_requested = True
            task.status = "canceled"  # optimistic
            task.touch()

            fut = self._futures.get(task_id)
            if fut and fut.cancel():
                # If it was not started yet, cancelled immediately.
                pass
        self._push_task_update(task_id)
        return True, "已请求取消"

    def remove(self, task_id: str, delete_files: bool = False) -> Tuple[bool, str]:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False, "任务不存在"

            if delete_files:
                self._delete_task_files(task)

            # Best-effort: cancel if still running
            ctrl = self._controls.get(task_id)
            if ctrl:
                ctrl.cancel_requested = True

            self._tasks.pop(task_id, None)
            self._controls.pop(task_id, None)
            self._futures.pop(task_id, None)
            self._task_media_candidates.pop(task_id, None)
            self._persist_task_state(force=True)

        self.broadcast({"type": "removed", "task_id": task_id})
        return True, "已移除"

    def clear_completed(self) -> int:
        removed = 0
        with self._lock:
            ids = [tid for tid, t in self._tasks.items() if t.status == "completed"]
        for tid in ids:
            ok, _ = self.remove(tid, delete_files=False)
            if ok:
                removed += 1
        if removed:
            self._persist_task_state(force=True)
        return removed

    # ---------------------- Download worker ----------------------
    def _control_flags(self, task_id: str) -> TaskControl:
        with self._lock:
            return self._controls.setdefault(task_id, TaskControl())

    def _get_task(self, task_id: str) -> Optional[DownloadTask]:
        with self._lock:
            return self._tasks.get(task_id)

    def _run_download(self, task_id: str) -> None:
        task = self._get_task(task_id)
        if not task:
            return

        save_dir = ensure_dir(task.save_path)

        with self._lock:
            task.status = "preparing"
            task.progress = 0.0
            task.speed = "0 KB/s"
            task.eta = "--"
            task.error_message = ""
            self._task_media_candidates[task_id] = set()
            task.touch()
        self._push_task_update(task_id)

        try:
            if bool(getattr(task, "subtitle_only", False)):
                outtmpl = to_ytdlp_outtmpl(save_dir, task.filename_template)
                existing_only = bool(getattr(task, "subtitle_existing_only", False))
                local_video_mode = bool(getattr(task, "local_video_subtitles", False)) or not self._is_http_url(task.url)
                with self._lock:
                    task.status = "processing"
                    task.progress = max(task.progress or 0.0, 98.0)
                    task.speed = "-"
                    task.eta = "--"
                    task.subtitle_note = (
                        "本地视频字幕模式：跳过下载，正在处理已有视频…"
                        if local_video_mode
                        else
                        "字幕模式：跳过视频下载，使用现有字幕文件进行处理…"
                        if existing_only
                        else "字幕模式：跳过视频下载，正在获取字幕…"
                    )
                    task.error_message = ""
                    task.touch()
                self._push_task_update(task_id)

                # Download subtitle sidecars from URL only, then process existing local media.
                if not existing_only and not local_video_mode:
                    self._download_subtitles_only_best_effort(task, outtmpl)
                self._postprocess_subtitles(task_id, save_dir, outtmpl)

                with self._lock:
                    if task.status not in {"review_pending", "error", "canceled", "paused"}:
                        task.status = "completed"
                        task.progress = 100.0
                        task.speed = "-"
                        task.eta = "--"
                        task.touch()
                self._push_task_update(task_id)
                return

            # Metadata extraction (title/thumbnail/uploader)
            # Keep this best-effort with a hard timeout to avoid long "preparing" stalls.
            info = self._extract_info_best_effort(task, task_id, timeout_sec=6.0)
            if info:
                with self._lock:
                    task.title = info.get("title") or task.title
                    task.uploader = info.get("uploader") or ""
                    task.thumbnail = info.get("thumbnail") or ""
                    size = info.get("filesize") or info.get("filesize_approx")
                    if size:
                        task.total_size = format_bytes(size)
                    task.touch()
                self._push_task_update(task_id)

            # Build yt-dlp options
            outtmpl = to_ytdlp_outtmpl(save_dir, task.filename_template)

            def hook(d: dict):
                self._progress_hook(task_id, d)

            ydl_opts = self._build_ydl_opts(task, outtmpl, hook)

            def run_cookie_attempts(build_opts, note_template: str, no_proxy: bool = False) -> None:
                errors: List[str] = []
                for source in self._browser_cookie_sources():
                    label = source[0]
                    opts = build_opts(source)
                    if no_proxy:
                        opts.pop("proxy", None)
                    with self._lock:
                        task.status = "downloading"
                        task.subtitle_note = note_template.format(label=label)
                        task.error_message = ""
                        task.touch()
                    self._push_task_update(task_id)

                    try:
                        with yt_dlp.YoutubeDL(opts) as cookie_ydl:
                            cookie_ydl.download([task.url])
                        return
                    except DownloadError as cookie_error:
                        raw_error = str(cookie_error)
                        friendly = self._probe_attempt_error_label(raw_error)
                        raw_hint = ""
                        if self._is_format_unavailable(raw_error):
                            raw_hint = " [format_unavailable]"
                        elif self._should_try_browser_cookies(raw_error):
                            raw_hint = " [auth_or_cookie_required]"
                        errors.append(f"{label}: {friendly}{raw_hint}")

                detail = "；".join(errors[-4:]) if errors else "没有可用浏览器登录态"
                raise DownloadError(f"浏览器登录态重试失败：{detail}")

            with self._lock:
                task.status = "downloading"
                task.touch()
            self._push_task_update(task_id)

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    ydl.download([task.url])
                except DownloadError as e:
                    err1 = str(e)
                    # yt-dlp/YouTube extraction changes may require a JS runtime,
                    # different client, or browser cookies. Some networks also reset
                    # TLS connections (SSL EOF). We run a small fallback pipeline.

                    if not (self._should_retry_with_fallback(err1) or self._is_ssl_error(err1)):
                        raise

                    # ---- Attempt #2: stable direct/progressive fallback with browser login ----
                    with self._lock:
                        task.status = "downloading"
                        task.subtitle_note = (
                            "正在切换稳定下载链路（2/3）…"
                            if self._is_format_unavailable(err1)
                            else "正在使用浏览器登录态重试（2/3）…"
                            if self._should_try_browser_cookies(err1)
                            else "网络重试中（2/3）…"
                        )
                        task.error_message = ""
                        task.touch()
                    self._push_task_update(task_id)

                    try:
                        run_cookie_attempts(
                            lambda source: self._stable_cookies_fallback_opts(task, outtmpl, hook, ydl_opts, source),
                            "正在使用 {label} 重试（2/3）…",
                            no_proxy=self._should_try_no_proxy(err1),
                        )
                    except DownloadError as e2:
                        err2 = str(e2)

                        # ---- Attempt #3: HLS/web fallback with browser cookies ----
                        if self._should_try_browser_cookies(err2) or self._is_ssl_error(err2) or self._should_retry_with_fallback(err2):
                            with self._lock:
                                task.status = "downloading"
                                task.subtitle_note = (
                                    "正在使用浏览器登录态重试（3/3）…"
                                    if self._should_try_browser_cookies(err2)
                                    else "正在切换兼容下载链路（3/3）…"
                                )
                                task.error_message = ""
                                task.touch()
                            self._push_task_update(task_id)

                            fb3_seed = self._fallback_ydl_opts(task, outtmpl, hook, ydl_opts)
                            run_cookie_attempts(
                                lambda source: self._cookies_fallback_opts(
                                    task,
                                    outtmpl,
                                    hook,
                                    fb3_seed,
                                    ssl_fallback=self._is_ssl_error(err2),
                                    cookie_source=source,
                                ),
                                "正在使用 {label} 兼容重试（3/3）…",
                                no_proxy=self._should_try_no_proxy(err2),
                            )
                        else:
                            raise

            # Optional post-processing: subtitles (download / embed / burn-in)
            self._postprocess_subtitles(task_id, save_dir, outtmpl)

            # If we got here without exception, mark completed
            # But don't override "review_pending" status (user needs to continue burn-in)
            with self._lock:
                if task.status not in {"review_pending", "error", "canceled", "paused"}:
                    task.status = "completed"
                    task.progress = 100.0
                    task.speed = "-"
                    task.eta = "--"
                    if (task.subtitle_note or "").startswith("网络重试中"):
                        task.subtitle_note = ""
                    task.touch()
            self._push_task_update(task_id)

            if bool(getattr(self._config, "auto_open", False)):
                open_in_file_manager(save_dir)

        except TaskPause:
            with self._lock:
                task.status = "paused"
                task.speed = "0 KB/s"
                task.subtitle_note = ""
                task.touch()
            self._push_task_update(task_id)
        except TaskCancel:
            with self._lock:
                task.status = "canceled"
                task.speed = "-"
                task.subtitle_note = ""
                task.touch()
            self._push_task_update(task_id)
        except DownloadError as e:
            msg = self._friendly_error(str(e))
            with self._lock:
                task.status = "error"
                task.error_message = msg
                task.speed = "-"
                task.subtitle_note = ""
                task.touch()
            self._push_task_update(task_id)
        except Exception as e:
            with self._lock:
                task.status = "error"
                task.error_message = self._friendly_error(str(e))
                task.speed = "-"
                task.subtitle_note = ""
                task.touch()
            self._push_task_update(task_id)

    def _extract_info(self, task: DownloadTask, task_id: str) -> Optional[dict]:
        ctrl = self._control_flags(task_id)
        if ctrl.cancel_requested:
            raise TaskCancel()
        if ctrl.pause_requested:
            raise TaskPause()

        proxy = self._effective_proxy()

        # Same idea as _build_ydl_opts: enable available JS runtimes.
        # If yt-dlp determines a remote component is required (EJS challenge solver),
        # enabling remote_components allows it to fetch the needed assets. When the
        # network/proxy can't reach GitHub/NPM, yt-dlp will warn and continue with
        # whatever it can (tasks will surface a clear error instead of hanging).
        detected = {}
        for name in ("deno", "node", "quickjs", "bun"):
            if shutil.which(name):
                detected[name] = {}
        js_runtimes = {k: {} for k in ("deno", "node", "quickjs", "bun") if k in detected} if detected else None

        ydl_opts = {
            "quiet": True,
            "skip_download": True,
            "proxy": proxy,
            "noplaylist": False,
            "extractor_args": {"youtube": {"player_client": YOUTUBE_CLIENTS_DEFAULT}},
            # Keep extraction retries short so invalid networks/proxies fail fast.
            "extractor_retries": 0,
            "retries": 0,
            "socket_timeout": 8,
            "http_headers": {"User-Agent": self._default_user_agent()},
        }
        rc = self._remote_components()
        if rc:
            ydl_opts["remote_components"] = rc
        if js_runtimes:
            ydl_opts["js_runtimes"] = js_runtimes
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(task.url, download=False)
        except Exception:
            return None

    def _extract_info_best_effort(self, task: DownloadTask, task_id: str, timeout_sec: float = 6.0) -> Optional[dict]:
        """Run metadata extraction with a hard timeout.

        Some network/proxy combinations can hang during metadata resolution.
        We skip metadata if timeout happens so download can continue.
        """
        ex = ThreadPoolExecutor(max_workers=1)
        try:
            fut = ex.submit(self._extract_info, task, task_id)
            return fut.result(timeout=timeout_sec)
        except FuturesTimeoutError:
            print(f"[meta] extract timeout (> {timeout_sec:.1f}s), continue without metadata", flush=True)
            return None
        except Exception:
            return None
        finally:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    def _extract_info_for_url(
        self,
        url: str,
        clients: List[str],
        timeout_sec: float = 8.0,
        *,
        no_proxy: bool = False,
        cookies_browser: Optional[str] = None,
        cookies_profile: Optional[str] = None,
        cookies_file: Optional[str] = None,
    ) -> Tuple[Optional[dict], str]:
        proxy = None if no_proxy else self._effective_proxy()

        detected = {}
        for name in ("deno", "node", "quickjs", "bun"):
            if shutil.which(name):
                detected[name] = {}
        js_runtimes = {k: {} for k in ("deno", "node", "quickjs", "bun") if k in detected} if detected else None

        opts = {
            "quiet": True,
            "skip_download": True,
            "ignore_no_formats_error": True,
            "proxy": proxy,
            "noplaylist": False,
            "extractor_args": {"youtube": {"player_client": clients}},
            "extractor_retries": 0,
            "retries": 0,
            "socket_timeout": 8,
            "http_headers": {"User-Agent": self._default_user_agent()},
        }
        rc = self._remote_components()
        if rc:
            opts["remote_components"] = rc
        if js_runtimes:
            opts["js_runtimes"] = js_runtimes
        if cookies_file:
            opts["cookiefile"] = cookies_file
        elif cookies_browser:
            opts["cookiesfrombrowser"] = (cookies_browser, cookies_profile) if cookies_profile else (cookies_browser,)

        ex = ThreadPoolExecutor(max_workers=1)
        try:
            def do_extract():
                with yt_dlp.YoutubeDL(opts) as ydl:
                    return ydl.extract_info(url, download=False)

            fut = ex.submit(do_extract)
            info = fut.result(timeout=timeout_sec)
            return info, ""
        except FuturesTimeoutError:
            return None, "格式探测超时"
        except Exception as e:
            return None, str(e or "")
        finally:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    def _flatten_probe_info(self, info: dict) -> dict:
        if not isinstance(info, dict):
            return {}
        if info.get("formats"):
            return info
        entries = info.get("entries")
        if isinstance(entries, list):
            for item in entries:
                if isinstance(item, dict) and item.get("formats"):
                    return item
        return info

    def _atlas_cookie_profile(self) -> Optional[str]:
        root = Path.home() / "Library" / "Application Support" / "com.openai.atlas" / "browser-data" / "host"
        if not root.exists():
            return None

        patterns = [
            str(root / "user-*" / "Cookies"),
            str(root / "Default" / "Cookies"),
        ]
        candidates: List[Tuple[int, str]] = []
        for pattern in patterns:
            for cookie_db in glob.glob(pattern):
                try:
                    con = sqlite3.connect(f"file:{cookie_db}?mode=ro", uri=True)
                    cur = con.cursor()
                    count = int(cur.execute(
                        "select count(*) from cookies where host_key like '%youtube%' or host_key like '%google.com%'"
                    ).fetchone()[0] or 0)
                    con.close()
                except Exception:
                    count = 0
                candidates.append((count, str(Path(cookie_db).parent)))

        if not candidates:
            return None

        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        best_count, best_profile = candidates[0]
        if best_count <= 0:
            return None
        return best_profile

    def _chrome_cookie_profiles(self) -> List[Tuple[str, str]]:
        root = Path.home() / "Library" / "Application Support" / "Google" / "Chrome"
        if not root.exists():
            return []

        candidates: List[Tuple[int, str, str]] = []
        for profile_dir in sorted(root.glob("*")):
            if not profile_dir.is_dir():
                continue
            cookie_db = profile_dir / "Cookies"
            network_cookie_db = profile_dir / "Network" / "Cookies"
            db = network_cookie_db if network_cookie_db.exists() else cookie_db
            if not db.exists():
                continue

            count = 0
            try:
                con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
                cur = con.cursor()
                count = int(cur.execute(
                    "select count(*) from cookies where host_key like '%youtube%' or host_key like '%google.com%'"
                ).fetchone()[0] or 0)
                con.close()
            except Exception:
                count = 0

            if profile_dir.name == "Default":
                count += 1
            if count > 0:
                label = "Chrome Default 登录态" if profile_dir.name == "Default" else f"Chrome {profile_dir.name} 登录态"
                candidates.append((count, label, str(profile_dir)))

        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return [(label, path) for _, label, path in candidates[:4]]

    def _browser_cookie_sources(self) -> List[Tuple[str, str, Optional[str]]]:
        sources: List[Tuple[str, str, Optional[str]]] = []
        manual_cookie = self._manual_cookies_file()
        if manual_cookie:
            sources.append(("cookies.txt 登录态", "cookiefile", manual_cookie))

        atlas_profile = self._atlas_cookie_profile()
        if atlas_profile:
            sources.append(("Atlas 登录态", "chrome", atlas_profile))

        seen_profiles: Set[str] = {atlas_profile} if atlas_profile else set()
        for label, profile in self._chrome_cookie_profiles():
            if profile in seen_profiles:
                continue
            sources.append((label, "chrome", profile))
            seen_profiles.add(profile)

        sources.append(("Chrome 自动登录态", "chrome", None))
        sources.append(("Safari 登录态", "safari", None))
        return sources

    def _apply_browser_cookie_source(self, opts: dict, source: Tuple[str, str, Optional[str]]) -> None:
        _, browser, profile = source
        opts.pop("cookiefile", None)
        opts.pop("cookiesfrombrowser", None)
        if browser == "cookiefile":
            if profile:
                opts["cookiefile"] = profile
            return
        opts["cookiesfrombrowser"] = (browser, profile) if profile else (browser,)

    def _probe_attempt_label(self, no_proxy: bool, cookies_browser: Optional[str], cookies_label: Optional[str] = None) -> str:
        if cookies_label:
            return cookies_label
        if cookies_browser == "chrome":
            return "Chrome 登录态"
        if cookies_browser == "safari":
            return "Safari 登录态"
        if no_proxy:
            return "匿名模式（无代理）"
        return "匿名模式"

    def _probe_attempt_error_label(self, raw: str) -> str:
        err = str(raw or "").strip()
        lower = err.lower()
        if "operation not permitted" in lower and "cookies.binarycookies" in lower:
            return "macOS 拒绝访问 Safari Cookies"
        if "operation not permitted" in lower and "cookies" in lower:
            return "macOS 拒绝访问浏览器 Cookies"
        if "could not find chrome cookies" in lower or "could not find chrome" in lower:
            return "未找到 Chrome 登录态"
        if "could not find safari cookies" in lower or "could not find safari" in lower:
            return "未找到 Safari 登录态"
        if "cookiesfrombrowser" in lower or "cookies from browser" in lower:
            return "浏览器登录态读取失败"
        if "格式探测超时" in err:
            return err
        return self._friendly_error(err)

    def _probe_video_summary(self, info: dict, quality: str) -> dict:
        flat = self._flatten_probe_info(info)
        formats = flat.get("formats") or []
        heights: List[int] = []
        progressive_mp4: List[int] = []
        sample_rows: List[dict] = []
        seen_rows: Set[Tuple[int, str, str]] = set()

        for fmt in formats:
            if not isinstance(fmt, dict):
                continue
            if fmt.get("vcodec") in {None, "none"}:
                continue
            height = int(fmt.get("height") or 0)
            ext = str(fmt.get("ext") or "").lower()
            acodec = str(fmt.get("acodec") or "none").lower()
            if height > 0:
                heights.append(height)
            if ext == "mp4" and acodec not in {"", "none"} and height > 0:
                progressive_mp4.append(height)

            if height <= 0:
                continue
            audio_tag = "含音频" if acodec not in {"", "none"} else "仅视频"
            row_key = (height, ext, audio_tag)
            if row_key in seen_rows:
                continue
            seen_rows.add(row_key)
            sample_rows.append({
                "height": height,
                "ext": ext or "-",
                "audio": audio_tag,
                "label": f"{height}p {ext.upper() if ext else '-'} {audio_tag}",
            })

        unique_heights = sorted({h for h in heights if h > 0}, reverse=True)
        sample_rows.sort(key=lambda x: (-int(x["height"] or 0), x["ext"], x["audio"]))
        sample_rows = sample_rows[:12]

        requested_height = int(quality) if str(quality or "").isdigit() else 0
        highest_visible = unique_heights[0] if unique_heights else 0
        highest_mp4 = max(progressive_mp4) if progressive_mp4 else 0
        selected_available = True
        selected_reason = ""
        recommended_quality = "best"

        if requested_height > 0:
            selected_available = requested_height in unique_heights or highest_visible >= requested_height
            if not selected_available:
                if highest_visible > 0:
                    selected_reason = f"当前环境仅探测到最高 {highest_visible}p"
                    recommended_quality = str(highest_visible)
                else:
                    selected_reason = "当前未探测到可用视频清晰度"
            else:
                recommended_quality = str(requested_height)
        elif highest_visible > 0:
            recommended_quality = str(highest_visible)

        return {
            "available_qualities": unique_heights,
            "highest_visible_quality": highest_visible,
            "highest_progressive_mp4_quality": highest_mp4,
            "selected_quality": str(quality or "best"),
            "selected_quality_available": bool(selected_available),
            "selected_quality_reason": selected_reason,
            "recommended_quality": recommended_quality,
            "sample_formats": sample_rows,
        }

    def _probe_stable_video_summary(self, info: dict, quality: str) -> dict:
        flat = self._flatten_probe_info(info)
        formats = flat.get("formats") or []
        direct_formats = []
        for fmt in formats:
            if not isinstance(fmt, dict):
                continue
            proto = str(fmt.get("protocol") or "").lower()
            if not (proto.startswith("http") and "m3u8" not in proto):
                continue
            if fmt.get("vcodec") in {None, "none"}:
                continue
            direct_formats.append(fmt)
        return self._probe_video_summary({"formats": direct_formats}, quality)

    def probe_formats(self, url: str, dl_type: str = "video", fmt: str = "mp4", quality: str = "1080") -> Tuple[bool, str, dict]:
        raw_url = str(url or "").strip()
        if not self._is_http_url(raw_url):
            return False, "请先输入有效链接", {}

        last_err = ""
        info: Optional[dict] = None
        best_info: Optional[dict] = None
        best_auth_source_label = "未使用浏览器登录态"
        auth_attempts: List[dict] = []
        auth_source_label = "未使用浏览器登录态"
        manual_cookie = self._manual_cookies_file()
        atlas_profile = self._atlas_cookie_profile()
        attempts: List[Tuple[List[str], bool, Optional[str], Optional[str], Optional[str], Optional[str]]] = []
        if manual_cookie:
            attempts.extend([
                (YOUTUBE_CLIENTS_DEFAULT, False, None, None, "cookies.txt 登录态", manual_cookie),
                (YOUTUBE_CLIENTS_FALLBACK, False, None, None, "cookies.txt 登录态", manual_cookie),
            ])
        if atlas_profile:
            attempts.extend([
                (YOUTUBE_CLIENTS_DEFAULT, False, "chrome", atlas_profile, "Atlas 登录态", None),
                (YOUTUBE_CLIENTS_FALLBACK, False, "chrome", atlas_profile, "Atlas 登录态", None),
            ])
        attempts.extend([
            (YOUTUBE_CLIENTS_DEFAULT, False, "chrome", None, "Chrome 登录态", None),
            (YOUTUBE_CLIENTS_FALLBACK, False, "chrome", None, "Chrome 登录态", None),
            (YOUTUBE_CLIENTS_DEFAULT, False, None, None, None, None),
            (YOUTUBE_CLIENTS_FALLBACK, False, None, None, None, None),
        ])
        if self._effective_proxy():
            attempts.extend([
                (YOUTUBE_CLIENTS_DEFAULT, True, None, None, None, None),
                (YOUTUBE_CLIENTS_FALLBACK, True, None, None, None, None),
            ])
        attempts.extend([
            (YOUTUBE_CLIENTS_DEFAULT, False, "safari", None, "Safari 登录态", None),
            (YOUTUBE_CLIENTS_FALLBACK, False, "safari", None, "Safari 登录态", None),
        ])

        for clients, no_proxy, cookies_browser, cookies_profile, cookies_label, cookies_file in attempts:
            label = self._probe_attempt_label(no_proxy, cookies_browser, cookies_label)
            info, err = self._extract_info_for_url(
                raw_url,
                list(clients),
                timeout_sec=10.0 if (cookies_browser or cookies_file) else 7.0,
                no_proxy=no_proxy,
                cookies_browser=cookies_browser,
                cookies_profile=cookies_profile,
                cookies_file=cookies_file,
            )
            if info:
                auth_source_label = label
                flat_info = self._flatten_probe_info(info)
                highest_visible = 1
                if dl_type == "video":
                    highest_visible = int(self._probe_video_summary(flat_info, quality).get("highest_visible_quality") or 0)
                auth_attempts.append({
                    "label": label,
                    "status": "success",
                    "clients": list(clients),
                    "error": "",
                })
                if best_info is None:
                    best_info = info
                    best_auth_source_label = label
                if dl_type != "video" or highest_visible > 0:
                    break
                info = None
                continue
            last_err = err
            auth_attempts.append({
                "label": label,
                "status": "failed",
                "clients": list(clients),
                "error": self._probe_attempt_error_label(err),
            })

        if not info and best_info is not None:
            info = best_info
            auth_source_label = best_auth_source_label

        if not info:
            return False, self._friendly_error(last_err or "格式探测失败"), {
                "url": raw_url,
                "type": dl_type,
                "format": fmt,
                "quality": str(quality or "1080"),
                "auth_source_label": auth_source_label,
                "auth_attempts": auth_attempts,
            }

        flat = self._flatten_probe_info(info)
        is_playlist = bool(info.get("entries")) and flat is not info
        report = {
            "url": raw_url,
            "type": dl_type,
            "format": fmt,
            "quality": str(quality or "1080"),
            "title": str(flat.get("title") or info.get("title") or ""),
            "uploader": str(flat.get("uploader") or info.get("uploader") or ""),
            "thumbnail": str(flat.get("thumbnail") or info.get("thumbnail") or ""),
            "is_playlist": bool(info.get("entries")),
            "probe_target": "playlist_first_entry" if is_playlist else "single_video",
            "auth_source_label": auth_source_label,
            "auth_attempts": auth_attempts,
        }

        if dl_type == "video":
            report.update(self._probe_video_summary(flat, quality))
            stable_attempts: List[Tuple[Optional[str], Optional[str], str, Optional[str]]] = []
            if manual_cookie:
                stable_attempts.append((None, None, "cookies.txt 登录态", manual_cookie))
            if atlas_profile:
                stable_attempts.append(("chrome", atlas_profile, "Atlas 登录态", None))
            stable_attempts.append(("chrome", None, "Chrome 登录态", None))

            stable_info = None
            stable_label = ""
            for cookies_browser, cookies_profile, label, cookies_file in stable_attempts:
                s_info, _ = self._extract_info_for_url(
                    raw_url,
                    ["android", "web"],
                    timeout_sec=8.0,
                    no_proxy=False,
                    cookies_browser=cookies_browser,
                    cookies_profile=cookies_profile,
                    cookies_file=cookies_file,
                )
                if s_info:
                    stable_info = s_info
                    stable_label = label
                    break

            if stable_info:
                stable_summary = self._probe_stable_video_summary(stable_info, quality)
                stable_highest = int(stable_summary.get("highest_visible_quality") or 0)
                report["stable_available_qualities"] = stable_summary.get("available_qualities") or []
                report["stable_highest_quality"] = stable_highest
                report["stable_auth_source_label"] = stable_label
                if stable_highest > 0:
                    report["selected_quality_available"] = bool(stable_summary.get("selected_quality_available"))
                    report["selected_quality_reason"] = str(stable_summary.get("selected_quality_reason") or "")
                    report["recommended_quality"] = stable_summary.get("recommended_quality") or report.get("recommended_quality") or "best"
                    if not report["selected_quality_available"]:
                        visible_highest = int(report.get("highest_visible_quality") or 0)
                        if visible_highest > stable_highest:
                            report["selected_quality_reason"] = (
                                f"当前稳定可下载最高 {stable_highest}p；更高画质仅在可见流中出现，当前实测可能失败。"
                            )
                else:
                    report["stable_available_qualities"] = []
                    report["stable_highest_quality"] = 0
                    report["stable_auth_source_label"] = stable_label
            else:
                report["stable_available_qualities"] = []
                report["stable_highest_quality"] = 0
                report["stable_auth_source_label"] = ""
            highest = int(report.get("highest_visible_quality") or 0)
            stable_highest = int(report.get("stable_highest_quality") or 0)
            if highest > 0:
                msg = f"探测完成：当前环境最高可见 {highest}p"
            elif stable_highest > 0:
                msg = f"探测完成：当前稳定可下载最高 {stable_highest}p"
            else:
                msg = "探测完成：未识别到可用视频清晰度"
            if is_playlist:
                msg += "（当前仅展示合集首个视频）"
            return True, msg, report

        report.update({
            "available_qualities": [],
            "highest_visible_quality": 0,
            "highest_progressive_mp4_quality": 0,
            "selected_quality": str(quality or "best"),
            "selected_quality_available": True,
            "selected_quality_reason": "",
            "recommended_quality": "best",
            "sample_formats": [],
        })
        return True, "探测完成：音频模式将按可用最佳音频提取", report

    def _build_ydl_opts(self, task: DownloadTask, outtmpl: str, hook) -> dict:
        proxy = self._effective_proxy()

        # yt-dlp 2025+ uses JS runtimes to solve YouTube's JS challenges.
        # IMPORTANT: only "deno" is enabled by default upstream. Many users
        # install Node.js (node) instead, so we enable whichever runtimes are
        # found in PATH to avoid "Only deno is enabled" / stuck-at-0% issues.
        js_runtimes: dict | None = None
        detected = {}
        for name in ("deno", "node", "quickjs", "bun"):
            if shutil.which(name):
                detected[name] = {}
        if detected:
            # Keep upstream priority order: deno > node > quickjs > bun
            js_runtimes = {k: {} for k in ("deno", "node", "quickjs", "bun") if k in detected}

        base = {
            "outtmpl": outtmpl,
            "progress_hooks": [hook],
            "logger": self._make_ydl_logger(task.id),
            "noplaylist": False,
            "ignoreerrors": self._url_looks_like_playlist(task.url),
            "quiet": True,
            # Networks that reset/EOF TLS connections are common; keep retries higher.
            "retries": 5 if self._stability_mode_enabled() else 3,
            "fragment_retries": 5 if self._stability_mode_enabled() else 3,
            "extractor_retries": 2 if self._stability_mode_enabled() else 1,
            "continuedl": True,
            "nopart": False,
            # Reduce parallel fragment connections (can trigger TLS resets on some networks/proxies)
            "concurrent_fragment_downloads": 1,
            "proxy": proxy,
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
                    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15"
                    if self._stability_mode_enabled()
                    else self._default_user_agent()
                )
            },
            # YouTube extractor tuning: avoid clients that commonly require PO Tokens.
            "extractor_args": {"youtube": {"player_client": YOUTUBE_CLIENTS_FALLBACK if self._stability_mode_enabled() else YOUTUBE_CLIENTS_DEFAULT}},
            "socket_timeout": 15,
        }
        if self._stability_mode_enabled():
            base["sleep_interval_requests"] = 1

        manual_cookie = self._manual_cookies_file()
        if manual_cookie:
            base["cookiefile"] = manual_cookie

        if self._url_looks_like_playlist(task.url):
            base["download_archive"] = str(self._download_archive_path(task))

        rc = self._remote_components()
        if rc:
            base["remote_components"] = rc

        if js_runtimes:
            # yt-dlp CLI flag: --js-runtimes node[:PATH],deno[:PATH],...
            base["js_runtimes"] = js_runtimes

        if task.type == "video":
            base["format"] = self._video_format_string(task.quality)
            base["merge_output_format"] = task.format
            self._apply_subtitles(task, base)
            return base

        base["format"] = self._audio_format_string(stable=False)
        pp = {
            "key": "FFmpegExtractAudio",
            "preferredcodec": task.format,
        }
        # yt-dlp expects preferredquality as string number; omit for best
        if task.quality.isdigit():
            pp["preferredquality"] = task.quality
        base["postprocessors"] = [pp]
        return base

    def _apply_subtitles(self, task: DownloadTask, opts: dict) -> None:
        """Subtitle fetching is handled in post-process.

        Fetching subtitles during the main media download can make a finished video
        task fail due to transient subtitle-side 429/rate-limit errors.
        """
        return

    def _audio_format_string(self, stable: bool = False) -> str:
        if stable:
            return (
                "bestaudio[ext=m4a][protocol=https]/"
                "bestaudio[protocol=https]/"
                "bestaudio[protocol=http]/"
                "bestaudio/"
                "best[acodec!=none]/"
                "18/"
                "best"
            )
        return (
            "bestaudio[ext=m4a]/"
            "bestaudio/"
            "best[acodec!=none]/"
            "18/"
            "best"
        )

    def _subtitle_language_batches(self, task: DownloadTask, user_langs: List[str]) -> List[List[str]]:
        translate_mode = (getattr(task, "subtitles_translate", "") or "").strip().lower()
        langs = []
        seen: Set[str] = set()
        for raw in user_langs:
            lang = str(raw or "").strip()
            if not lang:
                continue
            key = lang.lower()
            if key in seen:
                continue
            seen.add(key)
            langs.append(lang)

        non_zh = [lang for lang in langs if not lang.lower().startswith("zh")]
        wants_zh = any(lang.lower().startswith("zh") for lang in langs)

        batches: List[List[str]] = []
        if translate_mode in {"zh", "bilingual"}:
            # Translation mode only needs a source subtitle; avoid hammering zh variants.
            preferred = non_zh or ["en"]
            for lang in preferred:
                batches.append([lang])
            return batches

        for lang in langs:
            batches.append([lang])

        if wants_zh:
            for variant in ["zh-Hans", "zh-Hant", "zh-CN", "zh-TW"]:
                if variant.lower() not in seen:
                    seen.add(variant.lower())
                    batches.append([variant])
        return batches or [["en"]]

    def _subtitle_pick_lang_preferences(self, task: DownloadTask) -> List[str]:
        raw = [s.strip().lower() for s in (task.subtitles_langs or "").split(",") if s.strip()]
        translate_mode = (getattr(task, "subtitles_translate", "") or "").strip().lower()
        existing_only = bool(getattr(task, "subtitle_existing_only", False))
        if translate_mode in {"zh", "bilingual"} and not existing_only:
            non_zh = [lang for lang in raw if not lang.startswith("zh")]
            fallback = ["en", "en-us", "en-gb", "en-orig"]
            ordered = non_zh + [lang for lang in fallback if lang not in non_zh]
            if raw:
                ordered.extend([lang for lang in raw if lang not in ordered])
            return ordered
        return raw

    def _video_format_string(self, quality: str) -> str:
        if not quality or quality == "best":
            # Prefer directly downloadable progressive formats first. This avoids
            # many YouTube cases where DASH/HLS combinations exist in metadata
            # but are not actually retrievable in the current client/session.
            return (
                "best[protocol=https][acodec!=none]/"
                "best[protocol=http][acodec!=none]/"
                "18/"
                "bestvideo+bestaudio/"
                "best"
            )
        if quality.isdigit():
            h = int(quality)
            return (
                f"best[height<={h}][protocol=https][acodec!=none]/"
                f"best[height<={h}][protocol=http][acodec!=none]/"
                f"bestvideo[height<={h}]+bestaudio/"
                f"best[height<={h}]/best"
            )
        return (
            "best[protocol=https][acodec!=none]/"
            "best[protocol=http][acodec!=none]/"
            "18/"
            "bestvideo+bestaudio/"
            "best"
        )

    def _stable_video_format_string(self, quality: str) -> str:
        if not quality or quality == "best":
            return (
                "best[protocol=https][acodec!=none]/"
                "best[protocol=http][acodec!=none]/"
                "18/best"
            )
        if quality.isdigit():
            h = int(quality)
            return (
                f"best[height<={h}][protocol=https][acodec!=none]/"
                f"best[height<={h}][protocol=http][acodec!=none]/"
                f"bestvideo[height<={h}][protocol=https]+bestaudio[protocol=https]/"
                f"bestvideo[height<={h}][protocol=http]+bestaudio[protocol=http]/"
                "18/best"
            )
        return (
            "best[protocol=https][acodec!=none]/"
            "best[protocol=http][acodec!=none]/"
            "18/best"
        )

    def _should_retry_with_fallback(self, err: str) -> bool:
        s = (err or "").lower()
        signals = [
            "javascript runtime",
            "js runtime",
            "sabr",
            "missing a url",
            "unable to extract",
            "signature",
            "player",
            "po token",
            "potoken",
            "http error 403",
            "403",
            "requested format is not available",
            "format is not available",
            "requested format not available",
            "format_unavailable",
            "当前视频没有你指定",
            "格式组合",
            "可用格式",
            "login required",
            "authentication required",
            "please log in",
            "sign in to confirm you",
            "sign in if you've been granted access",
            "not a bot",
            "downloaded file is empty",
            "fragment not found",
        ]
        return any(x in s for x in signals)

    def _is_format_unavailable(self, err: str) -> bool:
        s = (err or "").lower()
        return (
            "requested format is not available" in s
            or "format is not available" in s
            or "requested format not available" in s
            or "format_unavailable" in s
            or "当前视频没有你指定" in s
            or "格式组合" in s
        )

    def _is_ssl_error(self, err: str) -> bool:
        s = (err or "").lower()
        # Common patterns from Python ssl / OpenSSL / urllib3
        return any(
            x in s
            for x in (
                "ssl:",
                "tls",
                "handshake",
                "certificate verify",
                "unexpected_eof",
                "eof occurred in violation of protocol",
                "connection reset",
            )
        )

    def _should_try_browser_cookies(self, err: str) -> bool:
        s = (err or "").lower()
        signals = [
            "challenge",
            "unable to extract",
            "initial data",
            "sabr",
            "missing a url",
            "po token",
            "potoken",
            "403",
            "login required",
            "authentication required",
            "please log in",
            "sign in",
            "consent",
            "forbidden",
            "downloaded file is empty",
            "fragment not found",
            "http error 403",
        ]
        return any(x in s for x in signals)

    def _should_try_no_proxy(self, err: str) -> bool:
        s = (err or "").lower()
        signals = [
            "timed out",
            "timeout",
            "connection",
            "proxy",
            "refused",
            "reset",
            "network",
            "temporary failure in name resolution",
            "fragment not found",
            "downloaded file is empty",
        ]
        return any(x in s for x in signals)

    def _cookies_fallback_opts(
        self,
        task: DownloadTask,
        outtmpl: str,
        hook,
        original: dict,
        ssl_fallback: bool,
        cookie_source: Optional[Tuple[str, str, Optional[str]]] = None,
    ) -> dict:
        """Final fallback:
        - use cookies from local browser to avoid JS challenges/consent
        - for flaky TLS networks, use curl external downloader with http1.1

        This is still local-only and reads cookies from the user's machine.
        """

        o = dict(original)
        o["outtmpl"] = outtmpl
        o["progress_hooks"] = [hook]
        o["retries"] = 1
        o["fragment_retries"] = 1
        o["extractor_retries"] = 1
        o["socket_timeout"] = 12

        self._apply_browser_cookie_source(o, cookie_source or self._browser_cookie_sources()[0])

        if ssl_fallback:
            # Use curl for downloads; it often behaves better with proxies/TLS in China.
            o["external_downloader"] = "curl"
            o["external_downloader_args"] = {
                "curl": [
                    "--location",
                    "--fail",
                    "--retry",
                    "5",
                    "--retry-all-errors",
                    "--retry-delay",
                    "1",
                    "--http1.1",
                ]
            }
        return o

    def _stable_cookies_fallback_opts(
        self,
        task: DownloadTask,
        outtmpl: str,
        hook,
        original: dict,
        cookie_source: Optional[Tuple[str, str, Optional[str]]] = None,
    ) -> dict:
        """Prefer direct/progressive formats with browser login state before trying HLS."""
        o = dict(original)
        o["outtmpl"] = outtmpl
        o["progress_hooks"] = [hook]
        o["retries"] = 1
        o["fragment_retries"] = 1
        o["extractor_retries"] = 1
        o["socket_timeout"] = 12
        o["extractor_args"] = {"youtube": {"player_client": ["android", "web"]}}
        if task.type == "video":
            o["format"] = self._stable_video_format_string(task.quality)
            o["merge_output_format"] = task.format
        else:
            o["format"] = self._audio_format_string(stable=True)
        self._apply_browser_cookie_source(o, cookie_source or self._browser_cookie_sources()[0])
        return o

    def _fallback_ydl_opts(self, task: DownloadTask, outtmpl: str, hook, original: dict) -> dict:
        # One-shot fallback: relax format constraints and try an alternate client order.
        fb = dict(original)
        fb["outtmpl"] = outtmpl
        fb["progress_hooks"] = [hook]
        fb["http_headers"] = {"User-Agent": self._default_user_agent()}
        fb["extractor_args"] = {"youtube": {"player_client": YOUTUBE_CLIENTS_FALLBACK}}
        fb["socket_timeout"] = 12
        fb["retries"] = 1
        fb["fragment_retries"] = 1
        fb["extractor_retries"] = 1

        rc = self._remote_components()
        if rc:
            fb["remote_components"] = rc

        if task.type == "video":
            fb["format"] = "best/18/bestvideo+bestaudio"
            fb["merge_output_format"] = task.format
        else:
            fb["format"] = self._audio_format_string(stable=False)
        return fb

    def _progress_hook(self, task_id: str, d: dict) -> None:
        task = self._get_task(task_id)
        if not task:
            return

        ctrl = self._control_flags(task_id)
        if ctrl.cancel_requested:
            raise TaskCancel()
        if ctrl.pause_requested:
            raise TaskPause()

        status = d.get("status")

        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded = d.get("downloaded_bytes")
            if total and downloaded:
                try:
                    pct = float(downloaded) * 100.0 / float(total)
                except Exception:
                    pct = percent_to_float(strip_ansi(d.get("_percent_str") or ""))
            else:
                pct = percent_to_float(strip_ansi(d.get("_percent_str") or ""))

            speed_num = d.get("speed")
            if speed_num:
                speed = format_speed(speed_num)
            else:
                speed = strip_ansi((d.get("_speed_str") or "").strip()) or "0 KB/s"

            eta_num = d.get("eta")
            if eta_num is not None:
                eta = format_eta(eta_num)
            else:
                eta = strip_ansi((d.get("_eta_str") or "").strip()) or "--"
            info_dict = d.get("info_dict") or {}
            filename = (
                d.get("filename")
                or info_dict.get("filepath")
                or info_dict.get("_filename")
                or ""
            )
            self._remember_media_candidate(task_id, filename)

            if total:
                size = format_bytes(total)
            else:
                size = task.total_size

            with self._lock:
                task.progress = pct
                task.speed = speed
                task.eta = eta
                task.filename = filename
                if filename and (task.title.startswith('解析中') or not task.title.strip()):
                    try:
                        name = Path(filename).name
                        if name.endswith('.part'):
                            name = name[:-5]
                        stem = Path(name).stem
                        if stem:
                            task.title = stem
                    except Exception:
                        pass
                if size and size != "--":
                    task.total_size = size
                task.status = "downloading"
                task.touch()
            self._push_task_update(task_id)

        elif status == "finished":
            # Download finished, yt-dlp may now merge/convert
            info_dict = d.get("info_dict") or {}
            filename = (
                d.get("filename")
                or info_dict.get("filepath")
                or info_dict.get("_filename")
                or ""
            )
            if filename:
                self._remember_media_candidate(task_id, filename)
            with self._lock:
                task.status = "processing"
                task.progress = max(task.progress, 99.0)
                task.speed = "-"
                task.eta = "--"
                if filename and not str(filename).endswith(".part"):
                    task.filename = str(filename)
                task.touch()
            self._push_task_update(task_id)

    def _download_subtitles_only_best_effort(self, task: DownloadTask, outtmpl: str) -> None:
        """Best-effort download subtitle sidecar files without re-downloading media."""
        proxy = self._effective_proxy()
        target_count = len(self._normalize_media_name_list(getattr(task, "subtitle_target_files", [])))
        playlist_like = self._url_looks_like_playlist(task.url)
        force_single_video = target_count > 0 and target_count <= 1
        is_playlist_download = playlist_like and not force_single_video

        download_url = task.url
        if force_single_video:
            vid = self._extract_youtube_video_id(task.url)
            if vid:
                download_url = self._youtube_watch_url(vid)

        detected = {}
        for name in ("deno", "node", "quickjs", "bun"):
            if shutil.which(name):
                detected[name] = {}
        js_runtimes = {k: {} for k in ("deno", "node", "quickjs", "bun") if k in detected} if detected else None

        # Build subtitle language batches from user preferences.
        langs_raw = (getattr(task, 'subtitles_langs', '') or '').strip()
        user_langs = [s.strip() for s in langs_raw.split(',') if s.strip()] or ['zh', 'en']
        lang_batches = self._subtitle_language_batches(task, user_langs)

        # Keep subtitle probing compact to avoid long stalls on flaky networks.
        subtitle_formats = ["srt", "vtt/srt"]
        last_err = ""
        had_success = False
        manual_cookie = self._manual_cookies_file()
        atlas_profile = self._atlas_cookie_profile()

        def run_subtitle_attempt(sub_langs: List[str], sub_fmt: str) -> Tuple[bool, str]:
            ydl_opts = {
                "outtmpl": outtmpl,
                "quiet": True,
                "logger": self._make_ydl_logger(task.id),
                "skip_download": True,
                "proxy": proxy,
                "noplaylist": not is_playlist_download,
                "ignoreerrors": is_playlist_download,
                "retries": 5 if self._stability_mode_enabled() else 1,
                "extractor_retries": 2 if self._stability_mode_enabled() else 1,
                "fragment_retries": 5 if self._stability_mode_enabled() else 1,
                "writesubtitles": True,
                "writeautomaticsub": True,
                "subtitleslangs": sub_langs,
                "subtitlesformat": sub_fmt,
                "extractor_args": {"youtube": {"player_client": YOUTUBE_CLIENTS_SUBTITLES}},
                "socket_timeout": 15,
                "http_headers": {"User-Agent": self._default_user_agent()},
            }
            if self._stability_mode_enabled():
                ydl_opts["sleep_interval_requests"] = 1

            if manual_cookie:
                ydl_opts["cookiefile"] = manual_cookie
            elif atlas_profile:
                ydl_opts["cookiesfrombrowser"] = ("chrome", atlas_profile)
            else:
                ydl_opts["cookiesfrombrowser"] = ("chrome",)

            rc = self._remote_components()
            if rc:
                ydl_opts["remote_components"] = rc
            if js_runtimes:
                ydl_opts["js_runtimes"] = js_runtimes

            ffmpeg = find_ffmpeg()
            if ffmpeg:
                ydl_opts["ffmpeg_location"] = ffmpeg

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([download_url])
                return True, ""
            except Exception as e:
                return False, str(e or "")

        for sub_langs in lang_batches:
            for sub_fmt in subtitle_formats:
                ok, msg = run_subtitle_attempt(sub_langs, sub_fmt)
                if ok:
                    had_success = True
                    break
                last_err = msg
                low = msg.lower()
                # Fatal authorization/privacy errors do not benefit from format retries.
                if (
                    "private video" in low
                    or "sign in if you've been granted access" in low
                    or "login required" in low
                    or "this video is unavailable" in low
                ):
                    with self._lock:
                        task.subtitle_note = self._friendly_error(msg)
                        task.touch()
                    return
                # 429 on one subtitle variant should not abort the entire subtitle flow.
                if "too many requests" in low or "unable to download video subtitles" in low:
                    continue
                continue
            if had_success:
                # In translation mode, one source subtitle is enough.
                translate_mode = (getattr(task, "subtitles_translate", "") or "").strip().lower()
                if translate_mode in {"zh", "bilingual"}:
                    return

        translate_mode = (getattr(task, "subtitles_translate", "") or "").strip().lower()
        if not had_success and translate_mode in {"zh", "bilingual"}:
            for sub_fmt in subtitle_formats:
                ok, msg = run_subtitle_attempt(["all"], sub_fmt)
                if ok:
                    had_success = True
                    break
                last_err = msg or last_err

        if had_success:
            return

        if last_err:
            with self._lock:
                task.subtitle_note = self._friendly_error(last_err)
                task.touch()

    def _ensure_subtitle_file(self, task_id: str, save_dir: Path, outtmpl: str, allow_download: bool = True) -> Optional[Path]:
        task = self._get_task(task_id)
        if not task:
            return None
        src = self._resolve_final_media_file(task, save_dir, task_id=task_id)
        if not src or not src.exists():
            return None

        # Update status to show we're looking for subtitles
        with self._lock:
            task.subtitle_note = "正在查找字幕文件…"
            task.touch()
        self._push_task_update(task_id)

        subs = self._subtitle_candidates(src, task)
        if not subs and allow_download:
            # Some formats/clients may not fetch subs during the main download; try again.
            with self._lock:
                task.subtitle_note = "正在下载字幕…"
                task.touch()
            self._push_task_update(task_id)

            self._download_subtitles_only_best_effort(task, outtmpl)
            subs = self._subtitle_candidates(src, task)

        if not subs:
            # Still no subtitles found
            return None

        sub = self._pick_subtitle_file(task, src)
        return sub if (sub and sub.exists()) else None

    def _whisper_command(self) -> Optional[List[str]]:
        env_whisper = os.environ.get("YTDL_WHISPER") or ""
        whisper_candidates = [
            env_whisper,
            shutil.which("whisper") or "",
            str(Path.home() / ".local" / "bin" / "whisper"),
            str(Path.home() / "Library" / "Python" / "3.13" / "bin" / "whisper"),
            str(Path.home() / "Library" / "Python" / "3.12" / "bin" / "whisper"),
            str(Path.home() / "Library" / "Python" / "3.11" / "bin" / "whisper"),
            "/opt/homebrew/bin/whisper",
            "/usr/local/bin/whisper",
            "/Library/Frameworks/Python.framework/Versions/Current/bin/whisper",
            "/Library/Frameworks/Python.framework/Versions/3.13/bin/whisper",
            "/Library/Frameworks/Python.framework/Versions/3.12/bin/whisper",
            "/Library/Frameworks/Python.framework/Versions/3.11/bin/whisper",
        ]
        for exe in whisper_candidates:
            exe = str(exe or "").strip()
            if exe and Path(exe).exists() and os.access(exe, os.X_OK):
                return [exe]

        python_candidates = [
            sys.executable,
            shutil.which("python3") or "",
            shutil.which("python") or "",
            "/opt/homebrew/bin/python3",
            "/usr/local/bin/python3",
            "/Library/Frameworks/Python.framework/Versions/Current/bin/python3",
            "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3.13",
            "/Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12",
            "/Library/Frameworks/Python.framework/Versions/3.11/bin/python3.11",
        ]
        seen: Set[str] = set()
        for py in python_candidates:
            py = str(py or "").strip()
            if not py or py in seen:
                continue
            seen.add(py)
            if not Path(py).exists() or not os.access(py, os.X_OK):
                continue
            try:
                proc = subprocess.run(
                    [py, "-c", "import whisper"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if proc.returncode == 0:
                    return [py, "-m", "whisper"]
            except Exception:
                pass
        return None

    def _transcribe_english_subtitle_if_possible(self, task_id: str, media_file: Path) -> Optional[Path]:
        task = self._get_task(task_id)
        if not task or not media_file or not media_file.exists():
            return None

        output_path = media_file.with_name(media_file.stem + ".whisper.en.srt")
        if output_path.exists() and output_path.stat().st_size > 0:
            with self._lock:
                task.subtitle_note = "使用已有 Whisper 英文转写字幕。"
                task.touch()
            self._push_task_update(task_id)
            return output_path

        cmd_base = self._whisper_command()
        if not cmd_base:
            with self._lock:
                task.subtitle_note = (
                    "未安装本机 Whisper，无法自动生成英文字幕。"
                    "请先运行：python3 -m pip install -U openai-whisper，然后重启 App。"
                )
                task.touch()
            self._push_task_update(task_id)
            return None

        with self._lock:
            task.status = "processing"
            task.progress = max(task.progress or 0.0, 99.0)
            task.speed = "-"
            task.eta = "--"
            task.subtitle_note = "未找到源字幕，正在用 Whisper 生成英文字幕…"
            task.touch()
        self._push_task_update(task_id)

        with tempfile.TemporaryDirectory(prefix="ytdl_whisper_") as tmp:
            tmp_dir = Path(tmp)
            cmd = [
                *cmd_base,
                str(media_file),
                "--language",
                "English",
                "--task",
                "transcribe",
                "--model",
                "base",
                "--output_format",
                "srt",
                "--output_dir",
                str(tmp_dir),
                "--fp16",
                "False",
            ]
            try:
                self._append_task_log(task_id, "Whisper: generating English subtitles")
                proc = subprocess.run(cmd, capture_output=True, text=True)
            except Exception as e:
                self._append_task_log(task_id, f"Whisper failed: {e}")
                with self._lock:
                    task.subtitle_note = "Whisper 英文字幕生成失败：" + self._friendly_error(str(e))
                    task.touch()
                self._push_task_update(task_id)
                return None

            if proc.returncode != 0:
                msg = (proc.stderr or proc.stdout or "").strip() or "Whisper 转写失败"
                self._append_task_log(task_id, "Whisper failed: " + msg[-2000:])
                with self._lock:
                    task.subtitle_note = "Whisper 英文字幕生成失败：" + self._friendly_error(msg)
                    task.touch()
                self._push_task_update(task_id)
                return None

            candidates = sorted(tmp_dir.glob("*.srt"), key=lambda p: p.stat().st_mtime, reverse=True)
            if not candidates:
                with self._lock:
                    task.subtitle_note = "Whisper 已运行，但未生成 SRT 字幕文件。"
                    task.touch()
                self._push_task_update(task_id)
                return None

            try:
                os.replace(str(candidates[0]), str(output_path))
            except Exception as e:
                with self._lock:
                    task.subtitle_note = "保存 Whisper 字幕失败：" + self._friendly_error(str(e))
                    task.touch()
                self._push_task_update(task_id)
                return None

        with self._lock:
            task.subtitle_note = f"已生成英文转写字幕：{output_path.name}"
            task.touch()
        self._push_task_update(task_id)
        return output_path

    def _embed_soft_subtitles_if_possible(
        self,
        task_id: str,
        save_dir: Path,
        outtmpl: str,
        sub_path: Optional[Path] = None,
        media_file: Optional[Path] = None,
    ) -> str:
        """Mux subtitles into the output container without re-encoding video."""
        task = self._get_task(task_id)
        if not task:
            return "skip"
        if (task.format or "").lower() not in {"mp4", "mkv"}:
            return "skip"

        src = media_file if (media_file and media_file.exists()) else self._resolve_final_media_file(task, save_dir, task_id=task_id)
        if not src or not src.exists():
            return "skip"
        if src.suffix.lower() not in {".mp4", ".mkv"}:
            with self._lock:
                task.subtitle_note = f"软字幕封装仅支持 MP4/MKV，已跳过：{src.name}"
                task.touch()
            self._push_task_update(task_id)
            return "skip"

        # Use provided subtitle path (potentially translated) or ensure from candidates
        sub = sub_path if sub_path and sub_path.exists() else self._ensure_subtitle_file(task_id, save_dir, outtmpl)
        if not sub or not sub.exists():
            with self._lock:
                task.subtitle_note = "未找到字幕文件，无法合并字幕。"
                task.touch()
            self._push_task_update(task_id)
            return "skip"

        ffmpeg = find_ffmpeg()
        if not ffmpeg:
            with self._lock:
                task.subtitle_note = "未找到 ffmpeg，无法合并字幕（请安装 ffmpeg 后重试）。"
                task.touch()
            self._push_task_update(task_id)
            return "fail"

        with self._lock:
            task.status = "processing"
            task.progress = max(task.progress or 0.0, 99.0)
            task.speed = "-"
            task.eta = "--"
            task.subtitle_note = "正在合并字幕…"
            task.touch()
        self._push_task_update(task_id)

        final_out = src.with_name(src.stem + ".subtitled" + src.suffix)
        if final_out.exists():
            try:
                out_mtime = final_out.stat().st_mtime
                if out_mtime >= src.stat().st_mtime and out_mtime >= sub.stat().st_mtime:
                    with self._lock:
                        task.subtitle_note = f"已存在软字幕封装视频：{final_out.name}"
                        if media_file is None:
                            task.filename = str(final_out)
                        task.touch()
                    self._push_task_update(task_id)
                    return "ok"
            except Exception:
                pass

        tmp_out = src.with_name(src.stem + ".__submux" + src.suffix)
        try:
            if tmp_out.exists():
                tmp_out.unlink(missing_ok=True)  # type: ignore[arg-type]

            # Set a reasonable language tag
            lang_pref = (task.subtitles_langs or "").split(",")[0].strip().lower()
            lang_tag = (lang_pref.split("-")[0] if lang_pref else "und") or "und"

            if src.suffix.lower() == ".mp4":
                # MP4 needs mov_text subtitle codec.
                cmd = [
                    ffmpeg, "-y",
                    "-i", str(src),
                    "-i", str(sub),
                    "-map", "0",
                    "-map", "1:0",
                    "-c", "copy",
                    "-c:s", "mov_text",
                    f"-metadata:s:s:0", f"language={lang_tag}",
                    str(tmp_out),
                ]
            else:
                # MKV supports SRT tracks well.
                cmd = [
                    ffmpeg, "-y",
                    "-i", str(src),
                    "-i", str(sub),
                    "-map", "0",
                    "-map", "1:0",
                    "-c", "copy",
                    "-c:s", "srt",
                    f"-metadata:s:s:0", f"language={lang_tag}",
                    str(tmp_out),
                ]

            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                msg = (proc.stderr or proc.stdout or "").strip() or "ffmpeg 合并字幕失败"
                self._append_task_log(task_id, "ffmpeg subtitle mux failed: " + msg[-2000:])
                with self._lock:
                    task.subtitle_note = "合并字幕失败：" + self._friendly_error(msg)
                    task.touch()
                self._push_task_update(task_id)
                return "fail"

            os.replace(str(tmp_out), str(final_out))
            with self._lock:
                task.subtitle_note = f"已合并软字幕：{final_out.name}"
                if media_file is None:
                    task.filename = str(final_out)
                task.touch()
            self._push_task_update(task_id)
            return "ok"

        finally:
            try:
                if tmp_out.exists():
                    tmp_out.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass

    def _postprocess_subtitles(self, task_id: str, save_dir: Path, outtmpl: str) -> None:
        task = self._get_task(task_id)
        if not task or task.type != "video":
            return

        download = bool(getattr(task, "subtitles_download", False))
        embed = bool(getattr(task, "subtitles_embed", False))
        burnin = bool(getattr(task, "subtitles_burnin", False))
        translate_mode = getattr(task, "subtitles_translate", "") or ""
        review_mode = bool(getattr(task, "subtitles_review_mode", False))
        existing_only = bool(getattr(task, "subtitle_existing_only", False))

        if existing_only:
            # Existing-only mode never fetches subtitles from network, but may still
            # translate existing local subtitle files.
            download = False

        if not download and not embed and not burnin and not translate_mode:
            return

        with self._lock:
            task.subtitle_missing_files = []
            task.subtitle_failed_files = []
            task.subtitle_processed_files = 0
            task.subtitle_skipped_files = 0
            task.subtitle_failed_count = 0

        # Preflight once for batch/single to avoid per-file repeated failures.
        if burnin:
            ffmpeg = find_ffmpeg()
            if not ffmpeg:
                with self._lock:
                    task.status = "error"
                    task.error_message = "未找到 ffmpeg，无法执行硬字幕烧录。"
                    task.subtitle_note = "请安装 ffmpeg 后重试（建议：brew install ffmpeg）。"
                    task.touch()
                self._push_task_update(task_id)
                return
            if not ffmpeg_has_subtitles_filter():
                msg = (
                    "当前 ffmpeg 不支持硬字幕烧录（缺少 subtitles/libass 过滤器）。"
                    "请安装支持 libass 的 ffmpeg 后重试。"
                )
                with self._lock:
                    task.status = "error"
                    task.error_message = msg
                    task.subtitle_note = msg
                    task.touch()
                self._push_task_update(task_id)
                return
        elif embed:
            ffmpeg = find_ffmpeg()
            if not ffmpeg:
                with self._lock:
                    task.status = "error"
                    task.error_message = "未找到 ffmpeg，无法执行软字幕合并。"
                    task.subtitle_note = "请安装 ffmpeg 后重试（建议：brew install ffmpeg）。"
                    task.touch()
                self._push_task_update(task_id)
                return

        media_files = self._resolve_task_media_files(task_id, task, save_dir)
        if not media_files:
            target_files = self._normalize_media_name_list(getattr(task, "subtitle_target_files", []))
            target_hint = ""
            if target_files:
                preview = "、".join(target_files[:2])
                if len(target_files) > 2:
                    preview += f" 等 {len(target_files)} 项"
                target_hint = f" 目标文件：{preview}。"
            with self._lock:
                task.status = "error"
                task.error_message = "未找到可处理的视频文件。请确认保存目录下已有对应视频后重试。" + target_hint
                task.subtitle_note = "字幕处理中止：未找到可处理的视频文件。" + target_hint
                task.touch()
            self._push_task_update(task_id)
            return

        # Playlist-like task: one task contains multiple media files.
        # Process each media file locally so we don't redownload videos.
        if len(media_files) > 1:
            if burnin and review_mode:
                # One task can't represent 50 independent "待审核" checkpoints.
                # For batch mode, continue automatically and keep a clear note.
                review_mode = False
                with self._lock:
                    task.subtitle_note = "合集任务检测到多视频，已跳过人工逐条审核并自动继续字幕处理。"
                    task.touch()
                self._push_task_update(task_id)

            total = len(media_files)
            done = 0
            skipped = 0
            failed = 0
            failed_files: List[str] = []
            skipped_files: List[str] = []
            planned_map, multi_candidate_media, missing_samples, missing_all = self._plan_batch_subtitle_matches(task, media_files)

            matched = len(planned_map)
            missing_count = max(total - matched, 0)
            preview = ""
            if missing_samples:
                preview = "；缺失示例：" + "、".join(missing_samples[:3])
            with self._lock:
                task.subtitle_note = (
                    f"字幕预检查：视频 {total}，已匹配 {matched}，缺失 {missing_count}，多候选 {multi_candidate_media}（已自动去重匹配）"
                    + preview
                )
                task.touch()
            self._push_task_update(task_id)

            for idx, media in enumerate(media_files, start=1):
                with self._lock:
                    task.status = "processing"
                    task.subtitle_note = f"字幕处理中（{idx}/{total}）：{media.name}"
                    task.touch()
                self._push_task_update(task_id)

                sub = planned_map.get(self._path_key(media))
                if (
                    (not sub or not sub.exists())
                    and not existing_only
                    and bool(getattr(task, "subtitles_transcribe_missing", False))
                ):
                    sub = self._transcribe_english_subtitle_if_possible(task_id, media)

                if not sub or not sub.exists():
                    skipped += 1
                    if media.name not in skipped_files:
                        skipped_files.append(media.name)
                    continue

                if translate_mode in ("zh", "bilingual"):
                    translated = self._translate_subtitle_if_needed(task_id, sub)
                    if not translated or not translated.exists():
                        failed += 1
                        if media.name not in failed_files:
                            failed_files.append(media.name)
                        continue
                    sub = translated

                op_result = "ok"
                if burnin:
                    op_result = self._burnin_subtitles_if_possible(task_id, save_dir, sub, media_file=media)
                elif embed:
                    op_result = self._embed_soft_subtitles_if_possible(task_id, save_dir, outtmpl, sub, media_file=media)

                if op_result == "ok":
                    done += 1
                elif op_result == "fail":
                    failed += 1
                    if media.name not in failed_files:
                        failed_files.append(media.name)
                else:
                    skipped += 1
                    if media.name not in skipped_files:
                        skipped_files.append(media.name)

            with self._lock:
                task.subtitle_processed_files = int(done)
                task.subtitle_skipped_files = int(skipped)
                task.subtitle_failed_count = int(failed)
                task.subtitle_missing_files = list(skipped_files)
                task.subtitle_failed_files = list(failed_files)
                if skipped_files:
                    skip_preview = "、".join(skipped_files[:2])
                    if len(skipped_files) > 2:
                        skip_preview += f" 等 {len(skipped_files)} 项"
                    task.subtitle_note = f"合集字幕处理完成：成功 {done}，跳过 {skipped}，失败 {failed}。缺失文件：{skip_preview}"
                else:
                    task.subtitle_note = f"合集字幕处理完成：成功 {done}，跳过 {skipped}，失败 {failed}。"
                if done == 0 and (burnin or embed):
                    task.status = "error"
                    task.error_message = f"字幕处理未完成：成功 {done}，跳过 {skipped}，失败 {failed}。"
                task.touch()
            self._push_task_update(task_id)
            return

        single_media_name = media_files[0].name if media_files else ""

        # Ensure subtitle sidecar exists (and convert VTT->SRT if needed)
        sub = self._ensure_subtitle_file(task_id, save_dir, outtmpl, allow_download=not existing_only)
        if (
            (not sub or not sub.exists())
            and not existing_only
            and bool(getattr(task, "subtitles_transcribe_missing", False))
            and media_files
        ):
            sub = self._transcribe_english_subtitle_if_possible(task_id, media_files[0])

        transcribe_note = str(getattr(task, "subtitle_note", "") or "").strip()
        transcribe_attempted = bool(getattr(task, "subtitles_transcribe_missing", False)) and not existing_only
        if not sub or not sub.exists():
            missing_msg = (
                "未找到可用源字幕文件（当前为“使用现有字幕”模式，Codex/翻译未执行）。"
                if existing_only and translate_mode in ("zh", "bilingual")
                else "未找到可用字幕文件（当前为“使用现有字幕”模式，不会自动下载）。"
                if existing_only
                else f"{transcribe_note}；Codex/翻译未执行。"
                if transcribe_attempted and transcribe_note and translate_mode in ("zh", "bilingual")
                else transcribe_note
                if transcribe_attempted and transcribe_note
                else "该视频没有可用字幕，且英文字幕自动生成失败；Codex/翻译未执行。"
                if bool(getattr(task, "subtitles_transcribe_missing", False)) and translate_mode in ("zh", "bilingual")
                else "该视频没有可用字幕，且英文字幕自动生成失败。"
                if bool(getattr(task, "subtitles_transcribe_missing", False))
                else "未找到可用源字幕（或字幕下载失败），Codex/翻译未执行。"
                if translate_mode in ("zh", "bilingual")
                else "该视频没有可用字幕（或字幕下载失败）。"
            )
            with self._lock:
                task.subtitle_note = missing_msg
                task.subtitle_missing_files = [single_media_name] if single_media_name else []
                task.subtitle_skipped_files = 1
                task.subtitle_processed_files = 0
                task.subtitle_failed_count = 0
                task.subtitle_failed_files = []
                if burnin or embed:
                    task.status = "error"
                    task.error_message = missing_msg
                task.touch()
            self._push_task_update(task_id)
            return

        # Translate subtitle if needed (before burn-in or embed)
        if translate_mode in ("zh", "bilingual"):
            sub = self._translate_subtitle_if_needed(task_id, sub)
            if not sub or not sub.exists():
                with self._lock:
                    task.subtitle_note = "字幕翻译失败。"
                    task.subtitle_missing_files = []
                    task.subtitle_failed_files = [single_media_name] if single_media_name else []
                    task.subtitle_processed_files = 0
                    task.subtitle_skipped_files = 0
                    task.subtitle_failed_count = 1
                    task.touch()
                self._push_task_update(task_id)
                return

        # If review mode is enabled for burn-in, pause here for user to review/edit subtitle
        if burnin and review_mode:
            with self._lock:
                task.status = "review_pending"
                task.subtitle_file_path = str(sub)
                task.subtitle_note = f"字幕已准备好，请审核后点击继续烧录。字幕文件：{sub.name}"
                task.subtitle_missing_files = []
                task.subtitle_failed_files = []
                task.subtitle_processed_files = 1
                task.subtitle_skipped_files = 0
                task.subtitle_failed_count = 0
                task.touch()
            self._push_task_update(task_id)
            return  # Stop here; user will call continue_burnin after review

        # Burn-in has priority over embed (UI already enforces this, but keep server-side too).
        op_result = "ok"
        if burnin:
            op_result = self._burnin_subtitles_if_possible(task_id, save_dir, sub)
        elif embed:
            op_result = self._embed_soft_subtitles_if_possible(task_id, save_dir, outtmpl, sub)
        else:
            mode_desc = ""
            if translate_mode == "zh":
                mode_desc = "（中文翻译）"
            elif translate_mode == "bilingual":
                mode_desc = "（中英双语）"
            with self._lock:
                task.subtitle_note = f"已下载字幕文件{mode_desc}。"
                task.subtitle_missing_files = []
                task.subtitle_failed_files = []
                task.subtitle_processed_files = 1
                task.subtitle_skipped_files = 0
                task.subtitle_failed_count = 0
                task.touch()
            self._push_task_update(task_id)

        if op_result != "ok":
            with self._lock:
                task.status = "error"
                task.error_message = task.subtitle_note or "字幕处理失败。"
                task.subtitle_missing_files = []
                task.subtitle_failed_files = [single_media_name] if single_media_name else []
                task.subtitle_processed_files = 0
                task.subtitle_skipped_files = 0 if op_result == "fail" else 1
                task.subtitle_failed_count = 1 if op_result == "fail" else 0
                task.touch()
            self._push_task_update(task_id)
        else:
            with self._lock:
                task.subtitle_missing_files = []
                task.subtitle_failed_files = []
                task.subtitle_processed_files = 1
                task.subtitle_skipped_files = 0
                task.subtitle_failed_count = 0
                task.touch()
            self._push_task_update(task_id)

    def _burnin_subtitles_if_possible(
        self,
        task_id: str,
        save_dir: Path,
        sub_path: Optional[Path] = None,
        media_file: Optional[Path] = None,
    ) -> str:
        """Burn-in subtitles to the final MP4 if the user enabled it.

        This is a best-effort post-process step:
        - Requires ffmpeg
        - Requires a subtitle file (we download subtitles as .srt)
        - Re-encodes video (hard subtitles) and writes a separate burned MP4
        """

        task = self._get_task(task_id)
        if not task:
            return "skip"

        # Only supported for MP4 output (keeps UI / user expectations simple).
        if (task.format or "").lower() != "mp4":
            return "skip"

        src = media_file if (media_file and media_file.exists()) else self._resolve_final_media_file(task, save_dir, task_id=task_id)
        if not src or not src.exists():
            return "skip"

        # Use provided subtitle path (potentially translated) or pick from candidates
        sub = sub_path if sub_path and sub_path.exists() else self._pick_subtitle_file(task, src)
        if not sub or not sub.exists():
            with self._lock:
                task.subtitle_note = "未找到字幕文件，无法烧录字幕。"
                task.touch()
            self._push_task_update(task_id)
            return "skip"

        ffmpeg = find_ffmpeg()
        if not ffmpeg:
            # Don't fail the whole task; just surface a note.
            with self._lock:
                task.subtitle_note = "未找到 ffmpeg，无法烧录字幕（请安装 ffmpeg 后重试）。"
                task.touch()
            self._push_task_update(task_id)
            return "fail"

        # Check if ffmpeg has subtitles filter (requires libass)
        if not ffmpeg_has_subtitles_filter():
            with self._lock:
                task.subtitle_note = (
                    "当前 ffmpeg 不支持字幕烧录（缺少 libass 库）。"
                    "请运行 `brew reinstall ffmpeg` 或安装完整版 ffmpeg。"
                )
                task.touch()
            self._push_task_update(task_id)
            return "fail"

        # Update status so frontend shows progress instead of staying on "解析中".
        with self._lock:
            task.status = "processing"
            task.progress = max(task.progress or 0.0, 99.0)
            task.speed = "-"
            task.eta = "--"
            task.touch()
        self._push_task_update(task_id)

        layout_mode = self._normalize_bilingual_layout(getattr(task, "subtitles_bilingual_layout", ""))
        render_sub = sub
        generated_layout_sub: Optional[Path] = None
        split_layout_applied = False

        if self._should_use_split_bilingual_layout(task):
            generated_layout_sub = self._build_split_bilingual_ass(sub, media_file)
            if generated_layout_sub and generated_layout_sub.exists():
                render_sub = generated_layout_sub
                split_layout_applied = True

        # ffmpeg subtitles filter has complex escaping rules for paths with spaces,
        # parentheses, etc. Use a temporary symlink with a simple name to avoid issues.
        import tempfile
        tmp_sub_link = None
        try:
            link_ext = render_sub.suffix or ".srt"
            tmp_sub_link = Path(tempfile.gettempdir()) / f"_ytdl_sub_{task_id}{link_ext}"
            if tmp_sub_link.exists():
                tmp_sub_link.unlink()
            tmp_sub_link.symlink_to(render_sub.resolve())
            sub_path_for_filter = str(tmp_sub_link)
        except Exception:
            # Fallback to escaped path if symlink fails
            sub_path_for_filter = self._ffmpeg_filter_escape_path(str(render_sub))

        # Escape colons in the path for ffmpeg filter syntax
        sub_f = sub_path_for_filter.replace(":", "\\:")
        # Style: compact subtitles at bottom with semi-transparent dark background.
        if split_layout_applied:
            vf = f"subtitles={sub_f}"
        else:
            force_style = "FontSize=14,MarginV=12,BorderStyle=4,BackColour=&H96000000,Outline=1,Alignment=2,WrapStyle=2,Spacing=1"
            vf = f"subtitles={sub_f}:force_style='{force_style}'"

        final_out = src.with_name(src.stem + ".burned.mp4")
        if final_out.exists():
            try:
                out_mtime = final_out.stat().st_mtime
                if out_mtime >= src.stat().st_mtime and out_mtime >= sub.stat().st_mtime:
                    with self._lock:
                        task.subtitle_note = f"已存在硬字幕视频：{final_out.name}"
                        if media_file is None:
                            task.filename = str(final_out)
                        task.touch()
                    self._push_task_update(task_id)
                    return "ok"
            except Exception:
                pass

        tmp_out = src.with_name(src.stem + ".__burnin.mp4")
        try:
            if tmp_out.exists():
                tmp_out.unlink(missing_ok=True)  # type: ignore[arg-type]

            cmd = [
                ffmpeg,
                "-y",
                "-i",
                str(src),
                "-vf",
                vf,
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                # Hard subtitles require re-encode. Keep the original resolution/FPS
                # and use a high quality encode to avoid visible quality loss.
                "-c:v",
                "libx264",
                "-preset",
                "slow",
                "-crf",
                "17",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "copy",
                "-movflags",
                "+faststart",
                str(tmp_out),
            ]

            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                msg = (proc.stderr or proc.stdout or "").strip() or "ffmpeg 处理失败"
                self._append_task_log(task_id, "ffmpeg burn-in failed: " + msg[-2000:])
                # Filter out version info to show actual error
                lines = msg.split('\n')
                error_lines = [l for l in lines if not l.startswith('  ') and 'ffmpeg version' not in l.lower() and 'configuration:' not in l.lower() and 'lib' not in l[:10].lower()]
                error_msg = '\n'.join(error_lines[-3:]) if error_lines else msg[:200]
                with self._lock:
                    task.subtitle_note = "烧录字幕失败：" + error_msg
                    task.touch()
                self._push_task_update(task_id)
                return "fail"

            os.replace(str(tmp_out), str(final_out))
            with self._lock:
                if split_layout_applied:
                    task.subtitle_note = f"已烧录字幕（中文顶部 / 英文底部）：{final_out.name}"
                elif layout_mode == "split_cn_top_en_bottom":
                    task.subtitle_note = f"已烧录字幕（未识别出双语分离结构，已回退为标准底部布局）：{final_out.name}"
                else:
                    task.subtitle_note = f"已烧录字幕（硬字幕）：{final_out.name}"
                if media_file is None:
                    task.filename = str(final_out)
                task.touch()
            self._push_task_update(task_id)
            return "ok"
        finally:
            try:
                if tmp_out.exists():
                    tmp_out.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            # Clean up temporary symlink
            try:
                if tmp_sub_link and tmp_sub_link.exists():
                    tmp_sub_link.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            try:
                if generated_layout_sub and generated_layout_sub.exists():
                    generated_layout_sub.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass

    def _resolve_final_media_file(self, task: DownloadTask, save_dir: Path, task_id: Optional[str] = None) -> Optional[Path]:
        """Best-effort find the final media file path for this task."""
        if task_id:
            files = self._resolve_task_media_files(task_id, task, save_dir)
            if files:
                want_ext = f".{(task.format or '').lower()}" if task.format else ""
                if want_ext:
                    exact = [p for p in files if p.suffix.lower() == want_ext]
                    if exact:
                        exact.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                        return exact[0]
                return files[0]

        # Backward-compatible fallback when task_id is not available.
        files = self._resolve_task_media_files(task.id, task, save_dir)
        return files[0] if files else None

    def _subtitle_candidates(self, media_file: Path, task: Optional[DownloadTask] = None) -> List[Path]:
        """Find all subtitle files that might belong to the given media file.

        yt-dlp generates subtitles with names like:
        - "Video Title.en.vtt"
        - "Video Title.en-US.vtt"
        - "Video Title.zh-Hans.srt"

        We try multiple matching strategies:
        1. Exact prefix match (media filename without extension)
        2. All subtitle files in the directory (fallback)
        """
        base = media_file.with_suffix("")
        parent = media_file.parent
        subs: List[Path] = []

        # Collect all sidecar subtitle files in this directory once.
        all_subs: List[Path] = []
        for ext in ("srt", "vtt", "ass"):
            try:
                for p in parent.glob(f"*.{ext}"):
                    if p.is_file():
                        all_subs.append(p)
            except Exception:
                continue

        base_name = base.name.lower()
        strict_prefixes = (
            base_name + ".",
            base_name + "_",
            base_name + "-",
            base_name + " ",
            base_name + "(",
            base_name + "（",
            base_name + "[",
        )
        # Strategy 1: strict same-base match.
        for p in all_subs:
            stem_l = p.stem.lower()
            if stem_l == base_name or any(stem_l.startswith(prefix) for prefix in strict_prefixes):
                subs.append(p)

        # de-dupe helper
        seen = set()
        prioritized: List[Path] = []
        for p in subs:
            if p in seen:
                continue
            seen.add(p)
            prioritized.append(p)

        if prioritized:
            return prioritized

        # Strategy 2: optional fallback by URL video id (safer than global wildcard).
        vid_candidates: Set[str] = set()
        media_vid = self._extract_youtube_like_id_from_filename(media_file.name)
        if media_vid:
            vid_candidates.add(media_vid.lower())

        if task:
            vid = self._extract_youtube_video_id(task.url)
            if vid:
                vid_candidates.add(vid.lower())

        if vid_candidates:
            by_vid: List[Path] = []
            seen = set()
            for p in all_subs:
                name_l = p.name.lower()
                if any(v in name_l for v in vid_candidates) and p not in seen:
                    by_vid.append(p)
                    seen.add(p)
            if by_vid:
                return by_vid

        # Strategy 3: only allow broad fallback in tiny dirs to avoid cross-match in batch.
        seen = set()
        uniq: List[Path] = []
        for p in all_subs:
            if p in seen:
                continue
            seen.add(p)
            uniq.append(p)
        return uniq if len(uniq) <= 3 else []

    def _parse_sub_lang(self, media_file: Path, sub_file: Path) -> str:
        base = media_file.with_suffix("")
        name = sub_file.name
        prefix = base.name + "."
        if name.startswith(prefix):
            rest = name[len(prefix):]
            parts = rest.split(".")
            if len(parts) >= 2:
                return parts[0].lower()
        return ""

    def _lang_match_score(self, preferred: List[str], cand: str) -> Tuple[int, int]:
        """Lower is better. (index in preferred list, match quality)"""
        if not cand:
            return (len(preferred) + 10, 9)
        cand = cand.lower()
        for i, p in enumerate(preferred):
            p = p.lower()
            if cand == p:
                return (i, 0)
            if cand.startswith(p + "-") or cand.startswith(p + "_") or cand.startswith(p):
                return (i, 1)
        return (len(preferred) + 5, 2)

    def _vtt_to_srt(self, vtt_path: Path, srt_path: Path) -> bool:
        """Best-effort VTT -> SRT conversion without ffmpeg."""
        try:
            txt = vtt_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            try:
                txt = vtt_path.read_text(encoding="utf-16", errors="ignore")
            except Exception:
                return False

        lines = [l.rstrip("\r\n") for l in txt.splitlines()]

        # Split into cue blocks separated by blank lines
        blocks: List[List[str]] = []
        cur: List[str] = []
        for l in lines:
            if not l.strip():
                if cur:
                    blocks.append(cur)
                    cur = []
                continue
            cur.append(l)
        if cur:
            blocks.append(cur)

        def conv_ts(t: str) -> str:
            t = t.strip()
            # normalize
            t = t.replace(",", ".")
            if "." in t:
                hhmmss, ms = t.split(".", 1)
                ms = (ms + "000")[:3]
            else:
                hhmmss, ms = t, "000"
            if hhmmss.count(":") == 1:
                hhmmss = "00:" + hhmmss
            return f"{hhmmss},{ms}"

        out: List[str] = []
        idx = 1
        for b in blocks:
            if not b:
                continue
            head = b[0].strip()
            if head.startswith("WEBVTT"):
                continue
            if head.startswith("NOTE"):
                continue

            timing_i = None
            for j, line in enumerate(b):
                if "-->" in line:
                    timing_i = j
                    break
            if timing_i is None:
                continue

            timing_line = b[timing_i]
            # strip cue settings after end timestamp
            parts = timing_line.split("-->")
            if len(parts) < 2:
                continue
            start = parts[0].strip()
            end = parts[1].strip().split(" ")[0].strip()

            text_lines = b[timing_i + 1 :]
            out.append(str(idx))
            out.append(f"{conv_ts(start)} --> {conv_ts(end)}")
            out.extend(text_lines if text_lines else [""])
            out.append("")
            idx += 1

        if idx == 1:
            return False

        try:
            srt_path.write_text("\n".join(out).strip() + "\n", encoding="utf-8")
            return True
        except Exception:
            return False

    def _find_codex_executable(self) -> Optional[str]:
        env_path = str(os.environ.get("CODEX_EXECUTABLE") or "").strip()
        candidates: List[str] = []
        if env_path:
            candidates.append(env_path)
        candidates.extend([
            "/Applications/Codex.app/Contents/Resources/codex",
            "/Applications/Codex.app/Contents/MacOS/codex",
        ])
        wh = shutil.which("codex")
        if wh:
            candidates.append(wh)
        candidates.extend([
            str(Path.home() / ".npm-global/bin/codex"),
            "/opt/homebrew/bin/codex",
            "/usr/local/bin/codex",
        ])

        seen: Set[str] = set()
        for raw in candidates:
            path = str(raw or "").strip()
            if not path or path in seen:
                continue
            seen.add(path)
            try:
                p = Path(path).expanduser()
            except Exception:
                continue
            if p.exists() and os.access(str(p), os.X_OK):
                return str(p)
        return None

    def _json_loads_loose(self, raw: str) -> Optional[object]:
        text = str(raw or "").strip()
        if not text:
            return None
        attempts = [
            text,
            re.sub(r",(\s*[\]}])", r"\1", text),
        ]
        for candidate in attempts:
            try:
                payload = json.loads(candidate)
            except Exception:
                continue
            if isinstance(payload, (dict, list)):
                return payload
        return None

    def _balanced_json_candidates(self, raw: str) -> List[str]:
        candidates: List[str] = []
        pairs = [("{", "}"), ("[", "]")]
        for open_ch, close_ch in pairs:
            starts = [idx for idx, ch in enumerate(raw) if ch == open_ch]
            for start in starts[:8]:
                depth = 0
                in_string = False
                escape = False
                for idx in range(start, len(raw)):
                    ch = raw[idx]
                    if in_string:
                        if escape:
                            escape = False
                        elif ch == "\\":
                            escape = True
                        elif ch == '"':
                            in_string = False
                        continue
                    if ch == '"':
                        in_string = True
                    elif ch == open_ch:
                        depth += 1
                    elif ch == close_ch:
                        depth -= 1
                        if depth == 0:
                            candidates.append(raw[start : idx + 1].strip())
                            break
        return candidates

    def _extract_json_payload(self, text: str) -> Optional[object]:
        raw = str(text or "").strip()
        if not raw:
            return None

        candidates: List[str] = [raw]
        for fenced in re.findall(r"```(?:json)?\s*(.*?)\s*```", raw, flags=re.IGNORECASE | re.DOTALL):
            candidates.append(str(fenced or "").strip())
        candidates.extend(self._balanced_json_candidates(raw))

        seen: Set[str] = set()
        for candidate in candidates:
            candidate = str(candidate or "").strip()
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            payload = self._json_loads_loose(candidate)
            if isinstance(payload, (dict, list)):
                return payload
        return None

    def _collect_codex_translation_rows(self, payload: object) -> List[object]:
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("translations", "items", "results", "data"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return rows
        return []

    def _subtitle_text_for_translation(self, lines: List[str]) -> str:
        parts: List[str] = []
        for line in list(lines or []):
            cleaned = self._strip_ass_override_tags(line)
            if cleaned:
                parts.append(cleaned)
        return re.sub(r"\s+", " ", " ".join(parts)).strip()

    def _chunk_translation_items(self, items: List[dict]) -> List[List[dict]]:
        batches: List[List[dict]] = []
        current: List[dict] = []
        current_chars = 0

        for item in items:
            text = str(item.get("text") or "")
            text_len = max(len(text), 1) + 64
            if current and (
                len(current) >= CODEX_TRANSLATE_BATCH_MAX_BLOCKS
                or current_chars + text_len > CODEX_TRANSLATE_BATCH_MAX_CHARS
            ):
                batches.append(current)
                current = []
                current_chars = 0
            current.append(item)
            current_chars += text_len

        if current:
            batches.append(current)
        return batches

    def _retry_codex_missing_items(
        self,
        items: List[dict],
        translated: Dict[int, str],
    ) -> Tuple[Dict[int, str], str]:
        """Retry missing Codex translations with smaller batches.

        Codex occasionally returns a valid JSON payload but drops some rows.
        In that case, retry the missing subset in smaller chunks before
        declaring the batch incomplete.
        """
        if not items:
            return translated, ""

        remaining = [item for item in items if int(item["position"]) not in translated]
        if not remaining:
            return translated, ""

        last_err = ""
        queue: List[List[dict]] = [remaining]
        while queue:
            batch = queue.pop(0)
            if not batch:
                continue
            ok, sub_result, err = self._translate_subtitle_batch_with_codex(batch)
            if sub_result:
                translated.update(sub_result)
            remaining_positions = {
                int(item["position"]) for item in batch if int(item["position"]) not in translated
            }
            if not remaining_positions:
                continue
            last_err = err or last_err or "Codex 返回的翻译条目不完整"
            if len(batch) == 1:
                continue
            mid = max(1, len(batch) // 2)
            queue.insert(0, batch[mid:])
            queue.insert(0, batch[:mid])

        return translated, last_err

    def _build_codex_translation_prompt(self, items: List[dict]) -> str:
        payload = {
            "items": [
                {
                    "position": int(item["position"]),
                    "text": str(item.get("text") or ""),
                }
                for item in items
            ]
        }
        return (
            "你是一个专业字幕翻译器。请把输入 JSON 中每一条英文字幕翻译成自然、准确、简洁的简体中文。\n"
            "只返回 JSON，不要返回 Markdown，不要加解释，不要加代码块。\n"
            "输出格式必须严格为：\n"
            "{\"translations\":[{\"position\":0,\"chinese\":\"...\"}]}\n"
            "规则：\n"
            "1. 保持条目数量与顺序一致，不得合并、拆分、遗漏。\n"
            "2. `position` 必须原样返回。\n"
            "3. `chinese` 只写中文字幕文本，不要编号、不要时间轴、不要额外说明。\n"
            "4. 除非是品牌名、人名、课程名等必须保留的专有名词，否则不要把原英文句子抄进 `chinese`。\n"
            "5. 如果原文里有被重复念三遍的句子，`chinese` 也只翻译当前这一条字幕本身，不要补写别的重复内容。\n"
            "4. 保留专有名词、语气、标点和字幕风格，避免直译生硬。\n"
            "6. 如果原文已经是中文或无需翻译，则自然保留。\n"
            "输入 JSON：\n"
            + json.dumps(payload, ensure_ascii=False)
        )

    def _translate_subtitle_batch_with_codex(self, items: List[dict]) -> Tuple[bool, Dict[int, str], str]:
        if not items:
            return True, {}, ""

        codex_exec = self._find_codex_executable()
        if not codex_exec:
            return False, {}, "未找到 codex 可执行文件"

        tmp_dir = Path(tempfile.mkdtemp(prefix="ytdl_codex_"))
        out_file = tmp_dir / "codex_last_message.txt"
        prompt = self._build_codex_translation_prompt(items)
        env = os.environ.copy()
        env["HOME"] = env.get("HOME") or str(Path.home())
        env["PATH"] = str(Path(codex_exec).parent) + os.pathsep + env.get("PATH", "")

        cmd = [
            codex_exec,
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "-c",
            'model_reasoning_effort="low"',
            "-C",
            str(tmp_dir),
            "--output-last-message",
            str(out_file),
            "-",
        ]

        try:
            proc = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=CODEX_TRANSLATE_TIMEOUT_SEC,
                env=env,
            )
        except subprocess.TimeoutExpired:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return False, {}, "调用 Codex 翻译超时"
        except Exception as exc:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return False, {}, f"调用 Codex 失败：{exc}"

        try:
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "").strip() or "Codex 执行失败"
                return False, {}, err

            raw = out_file.read_text(encoding="utf-8", errors="ignore")
            if not raw.strip():
                err = (proc.stdout or proc.stderr or "").strip()
                if "requires a newer version of Codex" in err:
                    return False, {}, "当前 Codex CLI 版本过旧，请升级 Codex App 或 CLI 后重试"
                if "not supported when using Codex with a ChatGPT account" in err:
                    return False, {}, "当前 Codex 账号不支持所选模型，请在 Codex 配置中换成可用模型后重试"
                if "no last agent message" in err:
                    return False, {}, "Codex 未返回翻译内容，请检查 Codex CLI 登录状态或模型配置"
                return False, {}, "Codex 未返回翻译内容"
            payload = self._extract_json_payload(raw)
            if not payload:
                return False, {}, "Codex 返回内容不是有效 JSON"

            rows = self._collect_codex_translation_rows(payload)
            if not rows:
                return False, {}, "Codex 返回缺少可识别的翻译条目"

            translated: Dict[int, str] = {}
            ordered_positions = [int(item["position"]) for item in items]
            required_positions = set(ordered_positions)
            sequential_values: List[str] = []

            for row in rows:
                if isinstance(row, str):
                    value = re.sub(r"\s+", " ", row).strip()
                    if value:
                        sequential_values.append(value)
                    continue
                if not isinstance(row, dict):
                    continue
                chinese = re.sub(
                    r"\s+",
                    " ",
                    str(
                        row.get("chinese")
                        or row.get("translation")
                        or row.get("translated")
                        or row.get("text")
                        or ""
                    ),
                ).strip()
                if not chinese:
                    continue
                pos_raw = row.get("position", row.get("index"))
                try:
                    pos = int(pos_raw)
                except Exception:
                    sequential_values.append(chinese)
                    continue
                if pos in required_positions:
                    translated[pos] = chinese

            if sequential_values:
                missing_positions = [pos for pos in ordered_positions if pos not in translated]
                for pos, chinese in zip(missing_positions, sequential_values):
                    translated[pos] = chinese

            missing_positions = sorted(required_positions - set(translated.keys()))
            if missing_positions:
                return False, translated, "Codex 返回的翻译条目不完整"

            return True, translated, ""
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _translate_subtitle_batch_with_google(self, items: List[dict], proxy: Optional[str] = None) -> Dict[int, str]:
        translated: Dict[int, str] = {}
        for idx, item in enumerate(items, start=1):
            pos = int(item["position"])
            text = str(item.get("text") or "")
            zh = self._translate_text_google(text, "zh-CN", proxy)
            translated[pos] = re.sub(r"\s+", " ", str(zh or text)).strip()
            if idx % 10 == 0:
                time.sleep(0.5)
        return translated

    def _translate_text_google(self, text: str, target_lang: str = "zh-CN", proxy: Optional[str] = None) -> Optional[str]:
        """Translate text using Google Translate free API."""
        if not text or not text.strip():
            return text

        try:
            # Use the free Google Translate API endpoint
            base_url = "https://translate.googleapis.com/translate_a/single"
            params = {
                "client": "gtx",
                "sl": "auto",  # auto-detect source language
                "tl": target_lang,
                "dt": "t",
                "q": text,
            }
            url = f"{base_url}?{urllib.parse.urlencode(params)}"

            req = urllib.request.Request(url)
            req.add_header("User-Agent", DEFAULT_UA)

            # Configure proxy if provided
            if proxy:
                if proxy.startswith("socks"):
                    # urllib doesn't support socks directly, skip proxy for socks
                    opener = urllib.request.build_opener()
                else:
                    proxy_handler = urllib.request.ProxyHandler({
                        "http": proxy,
                        "https": proxy,
                    })
                    opener = urllib.request.build_opener(proxy_handler)
            else:
                opener = urllib.request.build_opener()

            with opener.open(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                # Response format: [[["translated text", "original text", ...], ...], ...]
                if data and isinstance(data, list) and data[0]:
                    translated_parts = []
                    for item in data[0]:
                        if isinstance(item, list) and len(item) > 0:
                            translated_parts.append(item[0])
                    return "".join(translated_parts) if translated_parts else None
        except Exception:
            return None
        return None

    def _parse_srt_blocks(self, srt_content: str) -> List[dict]:
        """Parse SRT content into blocks with index, timing, and text."""
        blocks = []
        current_block = {"index": "", "timing": "", "text": []}
        lines = srt_content.strip().split("\n")

        state = "index"  # states: index, timing, text
        for line in lines:
            line = line.rstrip("\r")

            if state == "index":
                if line.strip().isdigit():
                    current_block["index"] = line.strip()
                    state = "timing"
                elif line.strip():
                    # Handle malformed SRT where index might be missing
                    if "-->" in line:
                        current_block["timing"] = line
                        state = "text"
            elif state == "timing":
                if "-->" in line:
                    current_block["timing"] = line
                    state = "text"
            elif state == "text":
                if not line.strip():
                    # End of current block
                    if current_block["timing"]:
                        blocks.append(current_block)
                    current_block = {"index": "", "timing": "", "text": []}
                    state = "index"
                else:
                    current_block["text"].append(line)

        # Don't forget the last block
        if current_block["timing"]:
            blocks.append(current_block)

        return blocks

    def _parse_srt_timestamp(self, ts: str) -> float:
        """Parse SRT timestamp to seconds."""
        ts = ts.strip().replace(',', '.')
        parts = ts.split(':')
        if len(parts) == 3:
            h, m, s = parts
            return float(h) * 3600 + float(m) * 60 + float(s)
        elif len(parts) == 2:
            m, s = parts
            return float(m) * 60 + float(s)
        return 0.0

    def _format_srt_timestamp(self, seconds: float) -> str:
        """Format seconds to SRT timestamp."""
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = seconds % 60
        ms = int((s - int(s)) * 1000)
        return f"{h:02d}:{m:02d}:{int(s):02d},{ms:03d}"

    def _translate_srt(
        self,
        task_id: Optional[str],
        srt_path: Path,
        output_path: Path,
        mode: str,
        proxy: Optional[str] = None,
        strict_codex: bool = False,
    ) -> Tuple[bool, str, str]:
        """Translate SRT file to Chinese or bilingual.

        Args:
            task_id: Optional task id for progress updates
            srt_path: Path to source SRT file
            output_path: Path to write translated SRT
            mode: "zh" for Chinese only, "bilingual" for Chinese + original
            proxy: Optional proxy URL

        Returns:
            (success, engine_label)
        """
        try:
            content = srt_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            try:
                content = srt_path.read_text(encoding="utf-16", errors="ignore")
            except Exception:
                return False, "", "读取字幕文件失败"

        blocks = self._parse_srt_blocks(content)
        if not blocks:
            return False, "", "字幕文件为空或格式无法识别"

        prepared_items: List[dict] = []
        original_text_map: Dict[int, str] = {}
        for i, block in enumerate(blocks):
            original_text = self._subtitle_text_for_translation(list(block.get("text") or []))
            original_text_map[i] = original_text
            if original_text:
                prepared_items.append({"position": i, "text": original_text})

        translations: Dict[int, str] = {}
        engines_used: Set[str] = set()
        batches = self._chunk_translation_items(prepared_items)
        total_batches = len(batches)

        for batch_index, batch in enumerate(batches, start=1):
            task = self._get_task(task_id) if task_id else None
            if task:
                with self._lock:
                    if total_batches > 1:
                        task.subtitle_note = f"正在调用 Codex 翻译字幕（{batch_index}/{total_batches} 批）…"
                    else:
                        task.subtitle_note = "正在调用 Codex 翻译字幕…"
                    task.touch()
                self._push_task_update(task_id)

            ok, batch_result, codex_err = self._translate_subtitle_batch_with_codex(batch)
            if batch_result:
                translations.update(batch_result)
                engines_used.add("codex")
            if ok:
                continue

            retried_result, retry_err = self._retry_codex_missing_items(batch, dict(batch_result))
            if retried_result:
                translations.update(retried_result)
                batch_result = retried_result
                engines_used.add("codex")
            missing_batch = [item for item in batch if int(item["position"]) not in batch_result]
            codex_err = retry_err or codex_err

            if not missing_batch:
                continue

            if strict_codex:
                return False, "codex", codex_err or "Codex 翻译失败"

            engines_used.add("google")
            if task:
                with self._lock:
                    prefix = self._friendly_error(codex_err or "Codex 翻译失败")
                    if batch_result:
                        if total_batches > 1:
                            task.subtitle_note = f"Codex 已翻译部分条目，缺失项回退普通翻译（{batch_index}/{total_batches} 批）… {prefix}"
                        else:
                            task.subtitle_note = f"Codex 已翻译部分条目，缺失项回退普通翻译… {prefix}"
                    else:
                        if total_batches > 1:
                            task.subtitle_note = f"Codex 翻译不可用，正在回退普通翻译（{batch_index}/{total_batches} 批）… {prefix}"
                        else:
                            task.subtitle_note = f"Codex 翻译不可用，正在回退普通翻译… {prefix}"
                    task.touch()
                self._push_task_update(task_id)
            translations.update(self._translate_subtitle_batch_with_google(missing_batch, proxy))

        translated_blocks = []
        for i, block in enumerate(blocks):
            original_text = original_text_map.get(i, "")
            translated = re.sub(r"\s+", " ", str(translations.get(i) or "")).strip()

            if translated:
                if mode == "bilingual":
                    styled_english = r"{\b1\c&HFFFFFF&\fsp1}" + original_text
                    styled_chinese = r"{\b0\c&HDDDDDD&\fsp0}" + translated
                    new_text = [styled_english, styled_chinese]
                else:
                    new_text = [translated]
            else:
                combined = original_text or " ".join(line.strip() for line in list(block.get("text") or []))
                new_text = [combined]

            translated_blocks.append({
                "index": str(i + 1),
                "timing": block["timing"],
                "text": new_text,
            })

        # Second pass: fix overlapping timestamps
        # Ensure each subtitle ends before or when the next one starts
        for i in range(len(translated_blocks) - 1):
            current = translated_blocks[i]
            next_block = translated_blocks[i + 1]

            # Parse current timing
            timing_parts = current["timing"].split("-->")
            if len(timing_parts) != 2:
                continue
            current_start = self._parse_srt_timestamp(timing_parts[0])
            current_end = self._parse_srt_timestamp(timing_parts[1])

            # Parse next timing
            next_timing_parts = next_block["timing"].split("-->")
            if len(next_timing_parts) != 2:
                continue
            next_start = self._parse_srt_timestamp(next_timing_parts[0])

            # If current end overlaps with next start, truncate current end
            if current_end > next_start:
                # Set current end to next start (with tiny gap to ensure no overlap)
                new_end = max(current_start + 0.1, next_start - 0.001)
                current["timing"] = f"{self._format_srt_timestamp(current_start)} --> {self._format_srt_timestamp(new_end)}"

        # Write output SRT
        output_lines = []
        for block in translated_blocks:
            output_lines.append(block["index"])
            output_lines.append(block["timing"])
            for text_line in block["text"]:
                output_lines.append(text_line)
            output_lines.append("")

        try:
            output_path.write_text("\n".join(output_lines), encoding="utf-8")
            if not engines_used or engines_used == {"codex"}:
                return True, "codex", ""
            if engines_used == {"google"}:
                return True, "google", ""
            return True, "codex+google", ""
        except Exception:
            return False, "", "写入翻译字幕文件失败"

    def _translate_subtitle_if_needed(self, task_id: str, sub_path: Path) -> Optional[Path]:
        """Translate subtitle file if translation is enabled.

        Returns the path to the translated subtitle, or the original if no translation needed.
        """
        task = self._get_task(task_id)
        if not task:
            return sub_path

        translate_mode = getattr(task, "subtitles_translate", "") or ""
        if not translate_mode or translate_mode not in ("zh", "bilingual"):
            return sub_path
        strict_codex = bool(getattr(task, "subtitles_codex_strict", False))

        # Update status
        with self._lock:
            task.subtitle_note = "正在准备用 Codex 翻译字幕（严格模式）…" if strict_codex else "正在准备用 Codex 翻译字幕…"
            task.touch()
        self._push_task_update(task_id)

        # Generate output filename
        suffix = ".zh" if translate_mode == "zh" else ".bilingual"
        stem_l = sub_path.stem.lower()
        if translate_mode == "zh" and ".zh" in stem_l:
            with self._lock:
                task.subtitle_note = "检测到已有中文字幕，直接复用。"
                task.touch()
            self._push_task_update(task_id)
            return sub_path
        if translate_mode == "bilingual" and ".bilingual" in stem_l:
            with self._lock:
                task.subtitle_note = "检测到已有中英双语字幕，直接复用。"
                task.touch()
            self._push_task_update(task_id)
            return sub_path

        # Avoid generating chains like ".bilingual.bilingual.srt" on retries.
        clean_stem = re.sub(r"(\.(?:zh|bilingual))+$", "", sub_path.stem, flags=re.IGNORECASE)
        output_path = sub_path.with_name(clean_stem + suffix + sub_path.suffix)

        # Skip if already exists
        if output_path.exists():
            with self._lock:
                task.subtitle_note = "使用已有翻译字幕。"
                task.touch()
            self._push_task_update(task_id)
            return output_path

        # Get proxy
        proxy = self._effective_proxy()

        # Perform translation
        success, engine_label, err_msg = self._translate_srt(task_id, sub_path, output_path, translate_mode, proxy, strict_codex=strict_codex)

        if success:
            mode_desc = "中文" if translate_mode == "zh" else "中英双语"
            with self._lock:
                if engine_label == "codex":
                    task.subtitle_note = f"已通过 Codex 翻译为{mode_desc}字幕。"
                elif engine_label == "codex+google":
                    task.subtitle_note = f"已翻译为{mode_desc}字幕（Codex 优先，部分批次使用兜底翻译）。"
                elif engine_label == "google":
                    task.subtitle_note = f"Codex 当前不可用，已回退普通翻译并生成{mode_desc}字幕。"
                else:
                    task.subtitle_note = f"已翻译为{mode_desc}字幕。"
                task.touch()
            self._push_task_update(task_id)
            return output_path
        else:
            with self._lock:
                if strict_codex:
                    task.subtitle_note = "Codex 翻译失败（严格模式），已停止当前字幕任务。"
                    task.error_message = self._friendly_error(err_msg or "Codex 翻译失败")
                else:
                    task.subtitle_note = "字幕翻译失败，使用原始字幕。"
                task.touch()
            self._push_task_update(task_id)
            return None if strict_codex else sub_path

    def _strip_ass_override_tags(self, text: str) -> str:
        return re.sub(r"\{[^{}]*\}", "", str(text or "")).strip()

    def _count_cjk_chars(self, text: str) -> int:
        return len(re.findall(r"[\u3400-\u9fff]", str(text or "")))

    def _count_latin_chars(self, text: str) -> int:
        return len(re.findall(r"[A-Za-z]", str(text or "")))

    def _subtitle_role_from_ass_tags(self, raw: str) -> str:
        s = str(raw or "")
        head = "".join(re.findall(r"\{[^{}]*\}", s[:96]))
        head_l = head.lower()
        if not head_l:
            return ""
        if ("\\b1" in head_l and "\\fsp1" in head_l) or "\\c&hffffff&" in head_l:
            return "en"
        if ("\\b0" in head_l and "\\fsp0" in head_l) or "\\c&hdddddd&" in head_l:
            return "cn"
        return ""

    def _single_line_subtitle_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", self._strip_ass_override_tags(text or "")).strip()

    def _sanitize_cn_split_text(self, text: str) -> str:
        s = self._single_line_subtitle_text(text)
        if not s:
            return ""
        parts = re.split(r"([，。！？；：,.!?;:])", s)
        kept: List[str] = []
        current = ""
        for token in parts:
            if token is None:
                continue
            current += token
            if token in "，。！？；：,.!?;:":
                seg = current.strip()
                current = ""
                if not seg:
                    continue
                cjk = self._count_cjk_chars(seg)
                latin = self._count_latin_chars(seg)
                if cjk == 0 and latin >= 6:
                    continue
                if cjk > 0 and latin > cjk * 1.25 and latin >= 8:
                    continue
                kept.append(seg)
        tail = current.strip()
        if tail:
            cjk = self._count_cjk_chars(tail)
            latin = self._count_latin_chars(tail)
            if not (cjk == 0 and latin >= 6) and not (cjk > 0 and latin > cjk * 1.25 and latin >= 8):
                kept.append(tail)
        cleaned = " ".join(x.strip() for x in kept if x.strip()).strip()
        if self._count_cjk_chars(cleaned) > 0:
            # In split layout, the top line should be Chinese-only. Strip residual
            # English words/phrases and keep only unavoidable punctuation/digits.
            cleaned = re.sub(r"[\"“”'`‘’]?[A-Za-z][A-Za-z0-9'’.-]*(?:\s+[A-Za-z][A-Za-z0-9'’.-]*)*[\"“”'`‘’]?", " ", cleaned)
            cleaned = re.sub(r"\s+([，。！？；：、,.!?;:])", r"\1", cleaned)
            cleaned = re.sub(r"([（(])\s+", r"\1", cleaned)
            cleaned = re.sub(r"\s+([）)])", r"\1", cleaned)
            cleaned = re.sub(r"[，。！？；：、,.!?;:]{2,}", lambda m: m.group(0)[0], cleaned)
            cleaned = re.sub(r"(^|[\s])([，。！？；：、,.!?;:])", r"\1", cleaned)
            cleaned = re.sub(r"[\"“”'`‘’]+", "", cleaned)
        return re.sub(r"\s+", " ", cleaned)

    def _sanitize_en_split_text(self, text: str) -> str:
        s = self._single_line_subtitle_text(text)
        if not s:
            return ""
        # Remove obvious Chinese fragments accidentally mixed into the English line.
        s = re.sub(r"[\u3400-\u9fff]+", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    def _classify_bilingual_lines(self, lines: List[str]) -> Tuple[str, str]:
        cn_lines: List[str] = []
        en_lines: List[str] = []
        other_lines: List[str] = []

        for raw in (lines or []):
            role = self._subtitle_role_from_ass_tags(raw)
            cleaned = self._single_line_subtitle_text(raw)
            if not cleaned:
                continue
            if role == "cn":
                cn_lines.append(cleaned)
                continue
            if role == "en":
                en_lines.append(cleaned)
                continue
            cjk = self._count_cjk_chars(cleaned)
            latin = self._count_latin_chars(cleaned)
            if cjk > 0 and cjk >= latin:
                cn_lines.append(cleaned)
            elif latin > 0 and latin > cjk:
                en_lines.append(cleaned)
            else:
                other_lines.append(cleaned)

        for item in other_lines:
            if cn_lines and not en_lines:
                en_lines.append(item)
            elif en_lines and not cn_lines:
                cn_lines.append(item)
            elif self._count_cjk_chars(item) > 0:
                cn_lines.append(item)
            else:
                en_lines.append(item)

        cn_text = " ".join(cn_lines).strip()
        en_text = " ".join(en_lines).strip()
        return self._sanitize_cn_split_text(cn_text), self._sanitize_en_split_text(en_text)

    def _format_ass_timestamp(self, seconds: float) -> str:
        total = max(0.0, float(seconds or 0.0))
        hours = int(total // 3600)
        minutes = int((total % 3600) // 60)
        secs = total % 60.0
        whole = int(secs)
        centis = int(round((secs - whole) * 100))
        if centis >= 100:
            whole += 1
            centis = 0
        if whole >= 60:
            minutes += whole // 60
            whole = whole % 60
        if minutes >= 60:
            hours += minutes // 60
            minutes = minutes % 60
        return f"{hours}:{minutes:02d}:{whole:02d}.{centis:02d}"

    def _ass_escape_text(self, text: str) -> str:
        s = str(text or "").replace("\\", r"\\")
        s = s.replace("{", r"\{").replace("}", r"\}")
        return s.replace("\r", "").replace("\n", r"\N").strip()

    def _probe_media_resolution(self, media_file: Optional[Path]) -> Tuple[int, int]:
        default_size = (1280, 720)
        if not media_file:
            return default_size
        try:
            media = Path(media_file)
        except Exception:
            return default_size
        if not media.exists():
            return default_size

        ffmpeg = find_ffmpeg()
        if ffmpeg:
            ffmpeg_path = Path(ffmpeg)
            ffprobe = ffmpeg_path.with_name("ffprobe")
            if ffprobe.exists():
                try:
                    proc = subprocess.run(
                        [
                            str(ffprobe),
                            "-v",
                            "error",
                            "-select_streams",
                            "v:0",
                            "-show_entries",
                            "stream=width,height",
                            "-of",
                            "json",
                            str(media),
                        ],
                        capture_output=True,
                        text=True,
                        timeout=6,
                    )
                    data = json.loads(proc.stdout or "{}")
                    streams = data.get("streams") or []
                    if streams:
                        width = int(streams[0].get("width") or 0)
                        height = int(streams[0].get("height") or 0)
                        if width > 0 and height > 0:
                            return width, height
                except Exception:
                    pass
            try:
                proc = subprocess.run(
                    [ffmpeg, "-hide_banner", "-i", str(media)],
                    capture_output=True,
                    text=True,
                    timeout=6,
                )
                combined = "\n".join([proc.stdout or "", proc.stderr or ""])
                match = re.search(r"(\d{3,5})x(\d{3,5})", combined)
                if match:
                    width = int(match.group(1))
                    height = int(match.group(2))
                    if width > 0 and height > 0:
                        return width, height
            except Exception:
                pass
        return default_size

    def _split_subtitle_layout_metrics(self, media_file: Optional[Path]) -> dict:
        width, height = self._probe_media_resolution(media_file)
        scale = max(1.0, float(height) / 720.0)
        cn_font = max(18, int(round(28.0 * scale)))
        en_font = max(cn_font + 1, int(round(30.0 * scale)))
        margin_x = max(24, int(round(32.0 * scale)))
        top_margin = max(28, int(round(40.0 * scale)))
        bottom_margin = max(30, int(round(44.0 * scale)))
        pad_x = max(12, int(round(20.0 * scale)))
        pad_y = max(8, int(round(14.0 * scale)))
        return {
            "width": width,
            "height": height,
            "cn_font": cn_font,
            "en_font": en_font,
            "margin_x": margin_x,
            "top_margin": top_margin,
            "bottom_margin": bottom_margin,
            "pad_x": pad_x,
            "pad_y": pad_y,
            # BorderStyle=3 already renders an opaque box, so keep outline at 0
            # to avoid fuzzy text edges.
            "outline": 0,
            "en_outline": 0,
            "cn_back_color": "&HAA000000",
            "en_back_color": "&HE6000000",
            "outline_color": "&H00000000",
            "cn_primary_color": "&H00FFFFFF",
            "en_primary_color": "&H00FFFFFF",
        }

    def _single_line_ass_overrides(self, text: str, role: str, metrics: dict) -> str:
        clean = self._single_line_subtitle_text(text)
        cjk = self._count_cjk_chars(clean)
        latin = self._count_latin_chars(clean)
        other = max(len(re.sub(r"\s+", "", clean)) - cjk - latin, 0)

        base_fs = int(metrics.get("cn_font") if role == "cn" else metrics.get("en_font") or 24)
        available_width = max(320.0, float(metrics.get("width") or 1280) - float(metrics.get("margin_x") or 24) * 2.0)
        pad_x = int(metrics.get("pad_x") or 12)
        pad_y = int(metrics.get("pad_y") or 6)

        if role == "cn":
            predicted_width = cjk * base_fs * 0.98 + latin * base_fs * 0.56 + other * base_fs * 0.62
            min_scale_x = 92
        else:
            predicted_width = cjk * base_fs * 0.92 + latin * base_fs * 0.58 + other * base_fs * 0.55
            min_scale_x = 92

        if predicted_width <= 1.0:
            return r"{\q2}"

        ratio = min(1.0, available_width / predicted_width)
        fs = base_fs
        if role == "en":
            if ratio < 0.94:
                fs = max(18, int(round(base_fs * 0.97)))
            if ratio < 0.86:
                fs = max(18, int(round(base_fs * 0.94)))
            if ratio < 0.78:
                fs = max(17, int(round(base_fs * 0.90)))
            if ratio < 0.70:
                fs = max(17, int(round(base_fs * 0.86)))
        else:
            if ratio < 0.92:
                fs = max(17, int(round(base_fs * 0.97)))
            if ratio < 0.84:
                fs = max(17, int(round(base_fs * 0.93)))
            if ratio < 0.76:
                fs = max(16, int(round(base_fs * 0.89)))

        adjusted_width = predicted_width * (float(fs) / float(base_fs))
        adjusted_ratio = min(1.0, available_width / max(adjusted_width, 1.0))
        scale_x = max(min_scale_x, min(100, int(round(adjusted_ratio * 100.0))))
        return (
            r"{\q2\blur0\fs"
            + str(fs)
            + r"\fscx"
            + str(scale_x)
            + r"\fsp0"
            + r"\xbord"
            + str(pad_x)
            + r"\ybord"
            + str(pad_y)
            + "}"
        )

    def _build_split_bilingual_ass(self, sub_path: Path, media_file: Optional[Path] = None) -> Optional[Path]:
        if sub_path.suffix.lower() != ".srt":
            return None

        try:
            content = sub_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            try:
                content = sub_path.read_text(encoding="utf-16", errors="ignore")
            except Exception:
                return None

        blocks = self._parse_srt_blocks(content)
        if not blocks:
            return None

        metrics = self._split_subtitle_layout_metrics(media_file)
        events: List[str] = []
        for block in blocks:
            timing = str(block.get("timing") or "")
            parts = timing.split("-->")
            if len(parts) != 2:
                continue

            start = self._parse_srt_timestamp(parts[0])
            end = self._parse_srt_timestamp(parts[1])
            if end <= start:
                continue

            texts = list(block.get("text") or [])
            cn_text, en_text = self._classify_bilingual_lines(texts)
            raw_text = "\n".join(
                cleaned
                for cleaned in (self._strip_ass_override_tags(x) for x in texts)
                if cleaned
            ).strip()

            start_ass = self._format_ass_timestamp(start)
            end_ass = self._format_ass_timestamp(end)

            if cn_text:
                cn_line = self._single_line_subtitle_text(cn_text)
                cn_text_ass = self._single_line_ass_overrides(cn_line, "cn", metrics) + self._ass_escape_text(cn_line)
                events.append(f"Dialogue: 0,{start_ass},{end_ass},CNTop,,0,0,0,,{cn_text_ass}")
            if en_text:
                en_line = self._single_line_subtitle_text(en_text)
                en_text_ass = self._single_line_ass_overrides(en_line, "en", metrics) + self._ass_escape_text(en_line)
                events.append(f"Dialogue: 0,{start_ass},{end_ass},ENBottom,,0,0,0,,{en_text_ass}")
            if not cn_text and not en_text and raw_text:
                raw_line = self._single_line_subtitle_text(raw_text)
                raw_text_ass = self._single_line_ass_overrides(raw_line, "en", metrics) + self._ass_escape_text(raw_line)
                events.append(f"Dialogue: 0,{start_ass},{end_ass},ENBottom,,0,0,0,,{raw_text_ass}")

        if not events:
            return None

        ass_text = "\n".join([
            "[Script Info]",
            "Title: Split Bilingual Burn-In",
            "ScriptType: v4.00+",
            f"PlayResX: {metrics['width']}",
            f"PlayResY: {metrics['height']}",
            "WrapStyle: 2",
            "ScaledBorderAndShadow: yes",
            "YCbCr Matrix: TV.709",
            "",
            "[V4+ Styles]",
            "Format: Name,Fontname,Fontsize,PrimaryColour,SecondaryColour,OutlineColour,BackColour,Bold,Italic,Underline,StrikeOut,ScaleX,ScaleY,Spacing,Angle,BorderStyle,Outline,Shadow,Alignment,MarginL,MarginR,MarginV,Encoding",
            f"Style: CNTop,PingFang SC,{metrics['cn_font']},{metrics['cn_primary_color']},{metrics['cn_primary_color']},{metrics['outline_color']},{metrics['cn_back_color']},0,0,0,0,100,100,0,0,3,{metrics['outline']},0,8,{metrics['margin_x']},{metrics['margin_x']},{metrics['top_margin']},1",
            f"Style: ENBottom,Helvetica Neue,{metrics['en_font']},{metrics['en_primary_color']},{metrics['en_primary_color']},{metrics['outline_color']},{metrics['en_back_color']},1,0,0,0,100,100,0,0,3,{metrics['en_outline']},0,2,{metrics['margin_x']},{metrics['margin_x']},{metrics['bottom_margin']},1",
            "",
            "[Events]",
            "Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text",
            *events,
            "",
        ])

        try:
            import tempfile

            fd, tmp_name = tempfile.mkstemp(prefix="_ytdl_split_", suffix=".ass")
            os.close(fd)
            out_path = Path(tmp_name)
            out_path.write_text(ass_text, encoding="utf-8")
            return out_path
        except Exception:
            return None

    def _pick_subtitle_file(self, task: DownloadTask, media_file: Path, used_subtitles: Optional[Set[str]] = None) -> Optional[Path]:
        """Pick a subtitle file for embed/burn-in (prefer user's language order)."""
        subs = self._subtitle_candidates(media_file, task)
        if not subs:
            return None

        preferred = self._subtitle_pick_lang_preferences(task)
        base_name = media_file.with_suffix("").name.lower()
        video_ids: Set[str] = set()
        url_vid = self._extract_youtube_video_id(task.url)
        if url_vid:
            video_ids.add(url_vid.lower())
        media_vid = self._extract_youtube_like_id_from_filename(media_file.name)
        if media_vid:
            video_ids.add(media_vid.lower())

        def match_confidence(p: Path) -> int:
            n = p.stem.lower()
            if n == base_name or n.startswith(base_name + ".") or n.startswith(base_name + "_") or n.startswith(base_name + "-"):
                return 0
            if video_ids and any(v in n for v in video_ids):
                return 1
            if n.startswith(base_name[:20]):
                return 2
            if base_name and (base_name in n or n in base_name):
                return 3
            return 9

        def variant_score(p: Path) -> int:
            n = p.name.lower()
            has_bilingual = any(x in n for x in (".bilingual.", "_bilingual.", "-bilingual.", ".bi."))
            has_zh = any(x in n for x in (".zh.", "_zh.", "-zh.", ".zh-", "_zh-", "-zh-", "zh-hans", "zh-hant", "zh-cn", "zh-tw"))
            target = (getattr(task, "subtitles_translate", "") or "").strip().lower()
            existing_only = bool(getattr(task, "subtitle_existing_only", False))
            split_layout = self._should_use_split_bilingual_layout(task)

            # Explicit target from user settings.
            if split_layout:
                return 0 if has_bilingual else (1 if has_zh else 3)
            if target == "bilingual":
                return 0 if has_bilingual else (1 if has_zh else 3)
            if target == "zh":
                return 0 if has_zh else (2 if has_bilingual else 3)

            # Existing-only burn-in: prefer already prepared bilingual/zh subtitles.
            if existing_only and bool(getattr(task, "subtitles_burnin", False)):
                if has_bilingual:
                    return 0
                if has_zh:
                    return 1
            return 2

        ranked: List[Tuple[int, int, Tuple[int, int], int, float, int, str, Path]] = []
        for p in subs:
            lang = self._parse_sub_lang(media_file, p)
            score = self._lang_match_score(preferred, lang) if preferred else (999, 9)
            noise = self._subtitle_duplicate_noise_score(p)
            try:
                mtime = float(p.stat().st_mtime)
            except Exception:
                mtime = 0.0
            ranked.append((match_confidence(p), variant_score(p), score, noise, -mtime, len(p.name), p.name.lower(), p))

        ranked.sort(key=lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6]))
        best_conf = ranked[0][0]
        # In large batches, low-confidence matching is riskier than skipping.
        if best_conf >= 9:
            return None
        if len(subs) > 8 and best_conf > 3:
            return None

        chosen: Optional[Path] = None
        for conf, _, _, _, _, _, _, p in ranked:
            if conf >= 9:
                continue
            if len(subs) > 8 and conf > 3:
                continue
            if used_subtitles and self._path_key(p) in used_subtitles:
                continue
            chosen = p
            break

        if not chosen:
            return None
        # Convert VTT -> SRT for ffmpeg subtitles filter and wide player support.
        if chosen.suffix.lower() == ".vtt":
            srt = chosen.with_suffix(".srt")
            if not srt.exists():
                ok = self._vtt_to_srt(chosen, srt)
                if not ok:
                    return None
            return srt

        return chosen

    def _ffmpeg_filter_escape_path(self, path: str) -> str:
        # ffmpeg subtitles filter uses ':' as delimiter, so escape it.
        # Also escape backslashes on Windows style paths.
        s = path.replace("\\", "\\\\")
        s = s.replace(":", "\\:")
        # Quote isn't used here (we pass as arg), but ffmpeg still parses the filter string.
        # Single quotes can appear in file names; escape conservatively.
        s = s.replace("'", "\\'")
        return s

    def _friendly_error(self, raw: str) -> str:
        s = (raw or "").strip()
        lower = s.lower()
        codex_context = (
            "codex" in lower
            or "openai" in lower
            or "ai 字幕" in s
            or "字幕翻译" in s
            or "subtitle translation" in lower
        )
        auth_context = (
            "api key" in lower
            or "login" in lower
            or "authentication" in lower
            or "not logged in" in lower
        )

        if "未找到 codex 可执行文件" in s or "codex executable" in lower:
            return (
                "未找到 Codex CLI，无法执行 AI 字幕翻译。\n"
                "请先确认本机已安装 codex，并且当前账号已登录。"
            )

        if codex_context and auth_context:
            return (
                "Codex CLI 当前未登录，无法执行 AI 字幕翻译。\n"
                "请先在终端运行 `codex` 完成登录，然后再回到 App 重试。"
            )

        if "model_reasoning_effort" in lower and "unknown variant" in lower:
            return "Codex CLI 本地配置无效，程序已尝试覆盖默认值；请重新启动 App 后再试。"

        if "浏览器登录态重试失败" in s:
            return (
                s + "\n"
                "请确认 Chrome 登录的是包含 YouTube 权限的同一个用户 Profile；如果仍失败，可以在设置里导入 cookies.txt，"
                "或给本 App / Xcode 授予“完全磁盘访问权限”后重启再试。"
            )

        if "cookiesfrombrowser" in lower or "cookies from browser" in lower:
            return (
                "需要读取浏览器 Cookies 来通过 YouTube 验证，但当前环境无法读取（例如未安装 Chrome 或权限受限）。\n"
                "建议：1) 安装/打开 Chrome 并登录 YouTube；2) 在设置里导入 cookies.txt；3) 或填写可用代理后重试。"
            )

        if (
            "login required" in lower
            or "sign in" in lower
            or "not logged in" in lower
            or ("authentication" in lower and ("youtube" in lower or "video" in lower))
        ):
            return (
                "YouTube 要求登录或额外验证，当前匿名下载链路被拦截。\n"
                "这是下载登录态问题，不是字幕问题。建议先在 Chrome 登录 YouTube，然后重新继续任务；必要时在设置里填写可用代理。"
            )

        if "operation not permitted" in lower and "cookies.binarycookies" in lower:
            return (
                "格式探测尝试读取 Safari 登录态时，被 macOS 权限拦截。\n"
                "建议：1) 改用已登录 YouTube 的 Chrome；2) 或在系统设置里允许相关 Cookie 访问后再试。"
            )

        if "ssl" in lower or "unexpected_eof" in lower or "eof occurred" in lower:
            return (
                "网络连接被中断（SSL/TLS EOF）。这通常是网络/代理导致（例如直连 YouTube 被重置）。\n"
                "请在【设置】里填写代理（如 http://127.0.0.1:7890 或 socks5h://127.0.0.1:7890），"
                "或确认系统代理已开启，然后重试。"
            )

        if "javascript runtime" in lower or "js runtime" in lower:
            return "YouTube 解析需要 JS 运行时：请安装 Node.js 或 Deno（mac 可用 brew install node / deno），然后重试"

        if "po token" in lower or "potoken" in lower or "gvs po token" in lower:
            return (
                "YouTube 当前对部分客户端/格式要求 PO Token（否则可能 403）。\n"
                "我已默认优先使用 web/mweb 等客户端，并启用可用的 JS 运行时来尽量绕开该限制；若仍失败，建议：\n"
                "1) 更新 yt-dlp 到最新（本工具会随依赖自动更新）；2) 试试更换网络/代理；\n"
                "3) 终极方案：使用浏览器 cookies（登录状态）或按 yt-dlp 的 PO Token Guide 提供 PO Token"
            )

        if "downloaded file is empty" in lower or "forbidden" in lower or "http error 403" in lower:
            return (
                "YouTube 拒绝了当前视频流请求（403/空文件）。\n"
                "这通常说明该清晰度或当前客户端需要浏览器登录态、Cookies 或额外验证信息。\n"
                "建议：1) 确认本机 Chrome 已登录 YouTube；2) 重新继续任务，让程序自动尝试浏览器 Cookies；3) 必要时改用较低清晰度。"
            )

        if "unable to download video subtitles" in lower and "429" in lower:
            return (
                "YouTube 当前对字幕请求做了限流（429）。\n"
                "程序会优先改用浏览器登录态并降级字幕语言请求；若仍失败，稍后重试通常会恢复。"
            )

        if "fragment not found" in lower:
            return (
                "当前拿到的是 HLS 视频流，但实际分片地址不可用，通常是 YouTube 当前流策略或该客户端链路不稳定导致。\n"
                "程序会优先切换到更稳定的直链格式；若仍失败，建议降低清晰度或改天再试。"
            )

        if "requested format is not available" in lower or "format is not available" in lower:
            return "当前视频没有你指定的清晰度/格式组合。程序会优先回退到可用格式；若仍失败，通常是 YouTube 对该视频流做了额外限制。"

        if "sign in to confirm you" in lower or "not a bot" in lower:
            return (
                "YouTube 要求额外的人机验证，匿名或当前客户端链路被拦截了。\n"
                "程序会自动尝试浏览器登录态与兼容客户端；若仍失败，通常是该视频当前限制较严。"
            )

        if "private video" in lower or "sign in if you've been granted access" in lower:
            return "该视频为私有视频（无权限访问），已跳过此条。若你有权限，请在浏览器登录后重试。"

        if "unsupported url" in lower or "invalid url" in lower:
            return "链接格式不正确，请检查"
        if "video unavailable" in lower or "unavailable" in lower:
            return "视频不存在或已被删除"
        if "geo" in lower and "restricted" in lower:
            return "该视频在当前地区不可用"
        if "connection" in lower or "timed out" in lower or "network" in lower:
            return "网络连接失败，请检查网络/代理"

        return s if s else "下载失败（未知错误）"

    def _delete_task_files(self, task: DownloadTask) -> None:
        # Best-effort file deletion. We delete the resolved final file if known,
        # and also try to delete any .part/.ytdl sidecar files with the same prefix.
        try:
            if task.filename:
                p = Path(task.filename)
                if p.exists():
                    p.unlink(missing_ok=True)  # type: ignore[arg-type]

                # Remove partials
                for extra in p.parent.glob(p.name + ".*"):
                    if extra.suffix in {".part", ".ytdl", ".temp"}:
                        extra.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
