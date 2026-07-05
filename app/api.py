import asyncio
import json
import os
import shutil
import socket
import ipaddress
import subprocess
import sys
from urllib.parse import urlparse
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .task_manager import TaskManager
from .utils import default_download_dir, check_ffmpeg, detect_system_proxy

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_PATH = DATA_DIR / "config.json"
STATIC_DIR = BASE_DIR / "static"
INSTANCE_TOKEN = os.getenv("YTDL_INSTANCE_TOKEN", "")


class DownloadRequest(BaseModel):
    urls: List[str] = Field(..., min_length=1)
    type: str = Field("video", pattern="^(video|audio)$")
    format: str
    quality: str
    save_path: str
    subtitle_only: Optional[bool] = False
    local_video_subtitles: Optional[bool] = False
    subtitle_existing_only: Optional[bool] = False
    # Optional subtitle behavior (if omitted, use config defaults)
    subtitles_download: Optional[bool] = None
    subtitles_embed: Optional[bool] = None
    subtitles_burnin: Optional[bool] = None
    subtitles_langs: Optional[str] = None
    subtitles_translate: Optional[str] = None  # "" (none), "zh" (Chinese only), "bilingual" (Chinese + English)
    subtitles_codex_strict: Optional[bool] = None
    subtitles_transcribe_missing: Optional[bool] = None
    subtitles_bilingual_layout: Optional[str] = None  # "" (default), "split_cn_top_en_bottom"
    subtitles_review_mode: Optional[bool] = None  # pause after translation for manual review
    subtitle_target_files: Optional[List[str]] = None  # optional: only process these local media files in subtitle-only mode


class LocalSubtitleRequest(BaseModel):
    save_path: str
    format: str = "mp4"
    quality: str = "local"
    subtitles_translate: str = Field("bilingual", pattern="^(|zh|bilingual)$")
    subtitles_embed: bool = False
    subtitles_burnin: bool = False
    subtitles_codex_strict: bool = False
    subtitles_transcribe_missing: bool = True
    subtitles_bilingual_layout: str = ""
    subtitle_target_files: Optional[List[str]] = None


class TaskControlRequest(BaseModel):
    task_id: str


class RemoveRequest(BaseModel):
    task_id: str
    delete_files: bool = False


class RetryMissingRequest(BaseModel):
    task_id: str
    filenames: Optional[List[str]] = None


class RebuildFromFolderRequest(BaseModel):
    save_path: str
    url: Optional[str] = ""
    format: Optional[str] = "mp4"
    quality: Optional[str] = "1080"
    subtitles_burnin: Optional[bool] = True
    subtitles_embed: Optional[bool] = False
    subtitles_langs: Optional[str] = "zh,en"
    subtitles_bilingual_layout: Optional[str] = ""
    subtitles_codex_strict: Optional[bool] = False
    dry_run: Optional[bool] = False


class FormatProbeRequest(BaseModel):
    url: str
    type: str = Field("video", pattern="^(video|audio)$")
    format: str = "mp4"
    quality: str = "1080"


class ConfigModel(BaseModel):
    default_path: str
    proxy: str = ""
    max_concurrent: int = Field(3, ge=1, le=5)
    filename_template: str = "{uploader} - {title}.{ext}"
    auto_open: bool = False
    # Subtitles
    subtitles_download: bool = False
    subtitles_embed: bool = False
    subtitles_burnin: bool = False
    subtitles_langs: str = "en"
    subtitles_translate: str = ""  # "" (none), "zh" (Chinese only), "bilingual" (Chinese + English)
    subtitles_codex_strict: bool = False
    subtitles_transcribe_missing: bool = False
    subtitles_bilingual_layout: str = ""  # "" (default), "split_cn_top_en_bottom"
    cookies_file: str = ""
    stability_mode: bool = False


def _load_config() -> ConfigModel:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

            # Auto-heal stale local proxy config (common when proxy app isn't running).
            # Without this, users may see long "解析中" stalls due to repeated timeouts.
            p = (data.get("proxy") or "").strip()
            if p:
                try:
                    legacy_defaults = {
                        "socks5h://127.0.0.1:7897",
                        "socks5://127.0.0.1:7897",
                        "http://127.0.0.1:7897",
                        "http://localhost:7897",
                        "socks5h://localhost:7897",
                        "socks5://localhost:7897",
                    }
                    if p.lower() in legacy_defaults:
                        data["proxy"] = ""
                        p = ""

                    u = urlparse(p)
                    host = (u.hostname or "").strip().strip("[]").lower()
                    port = u.port
                    if port is None:
                        scheme = (u.scheme or "").lower()
                        if scheme == "https":
                            port = 443
                        elif scheme == "http":
                            port = 80
                        elif scheme.startswith("socks"):
                            port = 1080

                    is_loopback = False
                    if host == "localhost":
                        is_loopback = True
                    elif host:
                        try:
                            is_loopback = ipaddress.ip_address(host).is_loopback
                        except ValueError:
                            is_loopback = False

                    if is_loopback and port:
                        try:
                            with socket.create_connection((host, int(port)), timeout=0.8):
                                pass
                        except OSError:
                            data["proxy"] = ""
                except Exception:
                    pass

            cfg = ConfigModel(**data)
            if cfg.model_dump().get("proxy", "") != p:
                CONFIG_PATH.write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
            return cfg
        except Exception:
            pass
    cfg = ConfigModel(default_path=str(default_download_dir()))
    CONFIG_PATH.write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    return cfg


def _save_config(cfg: ConfigModel) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(cfg.model_dump_json(indent=2), encoding="utf-8")


config = _load_config()
manager = TaskManager(config)

app = FastAPI(title="YouTube Downloader Pro", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)




@app.on_event("startup")
async def _startup_attach_loop():
    # Attach the running event loop so background threads can push WS updates.
    manager.set_event_loop(asyncio.get_running_loop())


@app.get("/api/health")
def health():
    ffmpeg_ok, ffmpeg_msg = check_ffmpeg()
    eff_proxy = (config.proxy or "").strip() or (detect_system_proxy() or "")
    cookies_file = (config.cookies_file or "").strip()
    cookies_path = Path(cookies_file).expanduser() if cookies_file else None
    cookies_ok = bool(cookies_path and cookies_path.exists() and cookies_path.is_file())
    return {
        "status": "ok",
        "instance_token": INSTANCE_TOKEN,
        "ffmpeg": ffmpeg_ok,
        "ffmpeg_message": ffmpeg_msg,
        "yt_dlp_version": manager.yt_dlp_version(),
        "proxy": eff_proxy,
        "proxy_configured": bool((config.proxy or "").strip()),
        "cookies_file": str(cookies_path) if cookies_path else "",
        "cookies_file_exists": cookies_ok,
        "stability_mode": bool(config.stability_mode),
    }


@app.get("/api/config")
def get_config():
    data = config.model_dump()
    if not (data.get("proxy") or "").strip():
        p = detect_system_proxy()
        if p:
            data["proxy"] = p
            data["proxy_auto"] = True
    else:
        data["proxy_auto"] = False
    return data


class ConfigUpdate(BaseModel):
    default_path: Optional[str] = None
    proxy: Optional[str] = None
    max_concurrent: Optional[int] = Field(None, ge=1, le=5)
    filename_template: Optional[str] = None
    auto_open: Optional[bool] = None
    subtitles_download: Optional[bool] = None
    subtitles_embed: Optional[bool] = None
    subtitles_burnin: Optional[bool] = None
    subtitles_langs: Optional[str] = None
    subtitles_translate: Optional[str] = None
    subtitles_codex_strict: Optional[bool] = None
    subtitles_transcribe_missing: Optional[bool] = None
    subtitles_bilingual_layout: Optional[str] = None
    cookies_file: Optional[str] = None
    stability_mode: Optional[bool] = None


@app.post("/api/config")
def update_config(body: ConfigUpdate):
    global config
    data = config.model_dump()
    for k, v in body.model_dump(exclude_none=True).items():
        data[k] = v

    # Validate and normalize path
    if data.get("default_path"):
        data["default_path"] = str(Path(data["default_path"]).expanduser())
    if data.get("cookies_file"):
        data["cookies_file"] = str(Path(data["cookies_file"]).expanduser())

    config = ConfigModel(**data)
    _save_config(config)
    manager.update_config(config)
    return {"status": "success", "config": config.model_dump()}


@app.post("/api/update_ytdlp")
def update_ytdlp():
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "-U", "yt-dlp"],
            capture_output=True,
            text=True,
            timeout=240,
        )
    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "yt-dlp 更新超时，请检查网络/代理后重试。"}
    except Exception as e:
        return {"status": "error", "message": f"yt-dlp 更新失败：{e}"}

    output = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
    if proc.returncode != 0:
        return {
            "status": "error",
            "message": output or f"yt-dlp 更新失败，退出码 {proc.returncode}",
        }

    return {
        "status": "success",
        "message": "yt-dlp 已更新。请重启 App 或重新运行工程后使用新版本。",
        "yt_dlp_version": manager.yt_dlp_version(),
        "log": output[-4000:],
    }


@app.get("/api/tasks")
def list_tasks():
    return {"tasks": [t.to_public_dict() for t in manager.list_tasks()]}


@app.post("/api/download")
def create_download(body: DownloadRequest):
    is_video = body.type == "video"
    tasks = manager.create_tasks(
        urls=body.urls,
        dl_type=body.type,
        fmt=body.format,
        quality=body.quality,
        save_path=body.save_path,
        subtitle_only=bool(body.subtitle_only) if is_video else False,
        local_video_subtitles=bool(body.local_video_subtitles) if is_video else False,
        subtitle_existing_only=bool(body.subtitle_existing_only) if is_video else False,
        subtitles_download=(body.subtitles_download if body.subtitles_download is not None else config.subtitles_download) if is_video else False,
        subtitles_embed=(body.subtitles_embed if body.subtitles_embed is not None else config.subtitles_embed) if is_video else False,
        subtitles_burnin=(body.subtitles_burnin if body.subtitles_burnin is not None else config.subtitles_burnin) if is_video else False,
        subtitles_langs=(body.subtitles_langs if body.subtitles_langs is not None else config.subtitles_langs),
        subtitles_translate=(body.subtitles_translate if body.subtitles_translate is not None else config.subtitles_translate) if is_video else "",
        subtitles_codex_strict=(body.subtitles_codex_strict if body.subtitles_codex_strict is not None else config.subtitles_codex_strict) if is_video else False,
        subtitles_transcribe_missing=(body.subtitles_transcribe_missing if body.subtitles_transcribe_missing is not None else config.subtitles_transcribe_missing) if is_video else False,
        subtitles_bilingual_layout=(
            body.subtitles_bilingual_layout
            if body.subtitles_bilingual_layout is not None
            else config.subtitles_bilingual_layout
        ) if is_video else "",
        subtitles_review_mode=(body.subtitles_review_mode if body.subtitles_review_mode is not None else False) if is_video else False,
        subtitle_target_files=(body.subtitle_target_files or []) if is_video else [],
    )
    return {
        "status": "success",
        "task_ids": [t.id for t in tasks],
        "tasks": [t.to_public_dict() for t in tasks],
    }


@app.post("/api/local_subtitles")
def create_local_subtitles(body: LocalSubtitleRequest):
    tasks = manager.create_tasks(
        urls=["about:local-video-subtitles"],
        dl_type="video",
        fmt=body.format or "mp4",
        quality=body.quality or "local",
        save_path=body.save_path,
        subtitle_only=True,
        local_video_subtitles=True,
        subtitle_existing_only=False,
        subtitles_download=False,
        subtitles_embed=bool(body.subtitles_embed) and not bool(body.subtitles_burnin),
        subtitles_burnin=bool(body.subtitles_burnin),
        subtitles_langs="en",
        subtitles_translate=body.subtitles_translate or "",
        subtitles_codex_strict=bool(body.subtitles_codex_strict),
        subtitles_transcribe_missing=bool(body.subtitles_transcribe_missing),
        subtitles_bilingual_layout=body.subtitles_bilingual_layout or "",
        subtitles_review_mode=False,
        subtitle_target_files=body.subtitle_target_files or [],
    )
    return {
        "status": "success",
        "task_ids": [t.id for t in tasks],
        "tasks": [t.to_public_dict() for t in tasks],
    }


@app.post("/api/probe_formats")
def probe_formats(body: FormatProbeRequest):
    ok, msg, report = manager.probe_formats(
        url=body.url,
        dl_type=body.type,
        fmt=body.format,
        quality=body.quality,
    )
    return {
        "status": "success" if ok else "error",
        "message": msg,
        "report": report or {},
    }


@app.post("/api/pause")
def pause_task(body: TaskControlRequest):
    ok, msg = manager.pause(body.task_id)
    return {"status": "success" if ok else "error", "message": msg}


@app.post("/api/resume")
def resume_task(body: TaskControlRequest):
    ok, msg = manager.resume(body.task_id)
    return {"status": "success" if ok else "error", "message": msg}


@app.post("/api/cancel")
def cancel_task(body: TaskControlRequest):
    ok, msg = manager.cancel(body.task_id)
    return {"status": "success" if ok else "error", "message": msg}


@app.post("/api/remove")
def remove_task(body: RemoveRequest):
    ok, msg = manager.remove(body.task_id, delete_files=body.delete_files)
    return {"status": "success" if ok else "error", "message": msg}


@app.post("/api/clear_completed")
def clear_completed():
    n = manager.clear_completed()
    return {"status": "success", "removed": n}


@app.post("/api/continue_burnin")
def continue_burnin(body: TaskControlRequest):
    """Continue burn-in after user has reviewed/edited the subtitle file."""
    ok, msg = manager.continue_burnin(body.task_id)
    return {"status": "success" if ok else "error", "message": msg}


@app.post("/api/analyze_subtitle_missing")
def analyze_subtitle_missing(body: TaskControlRequest):
    ok, msg, report = manager.analyze_subtitle_missing(body.task_id)
    return {
        "status": "success" if ok else "error",
        "message": msg,
        "report": report or {},
    }


@app.post("/api/retry_missing_subtitles")
def retry_missing_subtitles(body: RetryMissingRequest):
    ok, msg, tasks = manager.retry_missing_subtitles(body.task_id, body.filenames or [])
    return {
        "status": "success" if ok else "error",
        "message": msg,
        "task_ids": [t.id for t in (tasks or [])],
        "tasks": [t.to_public_dict() for t in (tasks or [])],
    }


@app.post("/api/rebuild_from_folder")
def rebuild_from_folder(body: RebuildFromFolderRequest):
    ok, msg, tasks, report = manager.rebuild_from_folder(
        save_path=body.save_path,
        url=(body.url or ""),
        fmt=(body.format or "mp4"),
        quality=(body.quality or "1080"),
        subtitles_burnin=bool(body.subtitles_burnin),
        subtitles_embed=bool(body.subtitles_embed),
        subtitles_langs=(body.subtitles_langs or "zh,en"),
        subtitles_bilingual_layout=(body.subtitles_bilingual_layout or ""),
        dry_run=bool(body.dry_run),
    )
    return {
        "status": "success" if ok else "error",
        "message": msg,
        "report": report or {},
        "task_ids": [t.id for t in (tasks or [])],
        "tasks": [t.to_public_dict() for t in (tasks or [])],
    }


@app.websocket("/ws/progress")
async def ws_progress(ws: WebSocket):
    await ws.accept()
    manager.ws_connect(ws)
    try:
        # Push current tasks snapshot on connect
        await ws.send_json({"type": "snapshot", "tasks": [t.to_public_dict() for t in manager.list_tasks()]})
        while True:
            # Keep alive; we don't require client messages
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.ws_disconnect(ws)
    except Exception:
        manager.ws_disconnect(ws)


# Serve the UI as static files. We register API routes before mounting static.
app.mount(
    "/",
    StaticFiles(directory=str(STATIC_DIR), html=True),
    name="static",
)


def run():
    import uvicorn

    host = os.getenv("YTDL_HOST", "127.0.0.1")
    port = int(os.getenv("YTDL_PORT", "8000"))

    uvicorn.run(
        "app.api:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )
