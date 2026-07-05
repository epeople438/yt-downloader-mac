import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Tuple, Optional


def detect_system_proxy() -> Optional[str]:
    """Best-effort proxy detection.

    Priority:
      1) Environment variables (HTTPS_PROXY/ALL_PROXY/HTTP_PROXY)
      2) macOS system proxy settings (scutil --proxy)

    Returns a proxy URL string usable by yt-dlp.
    """

    # 1) Env vars (common in terminal/proxy tools like Clash)
    for k in (
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ):
        v = (os.getenv(k) or "").strip()
        if v:
            return v

    # 2) macOS system proxy (GUI settings) — note: double-clicked .command
    # often does NOT inherit shell profile, so env vars may be empty.
    try:
        if os.uname().sysname.lower() != "darwin":
            return None
    except Exception:
        return None

    try:
        out = subprocess.check_output(["scutil", "--proxy"], text=True, timeout=2)
        kv = {}
        for line in out.splitlines():
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            kv[k.strip()] = v.strip()

        def _enabled(prefix: str) -> bool:
            return kv.get(f"{prefix}Enable", "0") == "1"

        # Prefer SOCKS if configured
        if _enabled("SOCKS"):
            host = kv.get("SOCKSProxy")
            port = kv.get("SOCKSPort")
            if host and port:
                return f"socks5h://{host}:{port}"

        # HTTPS proxy
        if _enabled("HTTPS"):
            host = kv.get("HTTPSProxy")
            port = kv.get("HTTPSPort")
            if host and port:
                return f"http://{host}:{port}"

        # HTTP proxy
        if _enabled("HTTP"):
            host = kv.get("HTTPProxy")
            port = kv.get("HTTPPort")
            if host and port:
                return f"http://{host}:{port}"

    except Exception:
        return None

    return None


def default_download_dir() -> Path:
    # Cross-platform default: ~/Downloads/YouTubeDownloader
    return Path.home() / "Downloads" / "YouTubeDownloader"


def ensure_dir(path: str) -> Path:
    p = Path(path).expanduser().resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


_ANSI_RE = re.compile(r"""(?:\x1B\[[0-?]*[ -/]*[@-~])""")
_ANSI_WEIRD_RE = re.compile(r"""[图区]\[[0-9;]*m""")  # seen when ESC is mangled in some environments

def strip_ansi(s: str) -> str:
    """Remove ANSI color/control sequences from strings."""
    if not s:
        return ""
    s = _ANSI_RE.sub("", s)
    s = _ANSI_WEIRD_RE.sub("", s)
    return s

def format_eta(seconds: Optional[float]) -> str:
    if seconds is None:
        return "--"
    try:
        sec = int(seconds)
    except Exception:
        return "--"
    if sec < 0:
        return "--"
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

def format_speed(bytes_per_sec: Optional[float]) -> str:
    if not bytes_per_sec or bytes_per_sec <= 0:
        return "0 KB/s"
    return f"{format_bytes(bytes_per_sec)}/s"

def find_ffmpeg() -> Optional[str]:
    """Locate ffmpeg binary with explicit support for bundled runtime ffmpeg.

    Priority:
      1) YTDL_FFMPEG (explicit path set by app launcher)
      2) Bundled imageio-ffmpeg when YTDL_FORCE_BUNDLED_FFMPEG is enabled
      3) System PATH ffmpeg
      4) Bundled imageio-ffmpeg fallback
    """
    env_ffmpeg = (os.getenv("YTDL_FFMPEG") or "").strip()
    if env_ffmpeg:
        p = Path(env_ffmpeg)
        if p.exists() and os.access(str(p), os.X_OK):
            return str(p)

    force_bundled = (os.getenv("YTDL_FORCE_BUNDLED_FFMPEG") or "").strip().lower() in {"1", "true", "yes", "on"}

    def _bundled_imageio_ffmpeg() -> Optional[str]:
        try:
            import imageio_ffmpeg  # type: ignore
            p = imageio_ffmpeg.get_ffmpeg_exe()
            if p and Path(p).exists():
                return str(p)
        except Exception:
            return None
        return None

    if force_bundled:
        bundled = _bundled_imageio_ffmpeg()
        if bundled:
            return bundled

    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        return ffmpeg

    return _bundled_imageio_ffmpeg()


def check_ffmpeg() -> Tuple[bool, str]:
    ffmpeg = find_ffmpeg()
    if not ffmpeg:
        return False, "ffmpeg 未在 PATH 中找到。请安装 FFmpeg 并确保命令行可用。"
    try:
        proc = subprocess.run([ffmpeg, "-version"], capture_output=True, text=True, timeout=3)
        if proc.returncode == 0:
            first_line = (proc.stdout or "").splitlines()[0] if proc.stdout else "ffmpeg"
            return True, first_line
        return False, proc.stderr.strip() or "ffmpeg 检测失败"
    except Exception as e:
        return False, f"ffmpeg 检测异常: {e}"


def ffmpeg_has_subtitles_filter() -> bool:
    """Check if ffmpeg supports the 'subtitles' filter (requires libass)."""
    ffmpeg = find_ffmpeg()
    if not ffmpeg:
        return False
    try:
        proc = subprocess.run(
            [ffmpeg, "-filters"],
            capture_output=True,
            text=True,
            timeout=5
        )
        # Look for " subtitles " in the filter list
        return " subtitles " in (proc.stdout or "") or "subtitles" in (proc.stdout or "").split()
    except Exception:
        return False


def format_bytes(num: Optional[float]) -> str:
    if not num or num <= 0:
        return "--"
    step = 1024.0
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    n = float(num)
    while n >= step and i < len(units) - 1:
        n /= step
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    return f"{n:.1f} {units[i]}"


def percent_to_float(pct: str) -> float:
    if not pct:
        return 0.0
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)%", pct)
    if not m:
        return 0.0
    try:
        return float(m.group(1))
    except Exception:
        return 0.0


_TEMPLATE_MAP = {
    "{title}": "%(title)s",
    "{id}": "%(id)s",
    "{uploader}": "%(uploader)s",
    "{date}": "%(upload_date)s",
    "{ext}": "%(ext)s",
}


def to_ytdlp_outtmpl(save_dir: Path, filename_template: str) -> str:
    tpl = filename_template or "{uploader} - {title}.{ext}"
    for k, v in _TEMPLATE_MAP.items():
        tpl = tpl.replace(k, v)

    # Backwards compatible: allow raw yt-dlp templates too
    if "%(" not in tpl:
        # If user removed all variables accidentally, ensure at least title/ext
        tpl = "%(title)s.%(ext)s"

    return str(save_dir / tpl)


def open_in_file_manager(path: Path) -> None:
    try:
        if os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif os.uname().sysname.lower() == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        # Best-effort; ignore.
        pass
