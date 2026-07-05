#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BACKEND_DIR="$ROOT_DIR/mac_app/BackendBundle"
RUNTIME_DIR="$BACKEND_DIR/runtime"
PY_RUNTIME_ROOT="$RUNTIME_DIR/python"
SITE_PACKAGES_DIR="$RUNTIME_DIR/site-packages"
FFMPEG_RUNTIME_DIR="$RUNTIME_DIR/ffmpeg/bin"
VERSION_FILE="$RUNTIME_DIR/PYTHON_VERSION"
ARCH_FILE="$RUNTIME_DIR/ARCH"
EXISTING_FFMPEG="$BACKEND_DIR/runtime/ffmpeg/bin/ffmpeg"

has_subtitles_filter() {
  local ff="$1"
  local filters_out
  [ -x "$ff" ] || return 1
  filters_out="$("$ff" -filters 2>&1 || true)"
  grep -Fq "Render text subtitles onto input video using the libass library." <<<"$filters_out"
}

DEFAULT_VENV="$HOME/Library/Application Support/YouTubeDownloaderPro/backend/.venv"
SOURCE_PY="${SOURCE_PYTHON:-$DEFAULT_VENV/bin/python}"

if [ ! -x "$SOURCE_PY" ]; then
  if [ -x "/opt/homebrew/bin/python3" ]; then
    SOURCE_PY="/opt/homebrew/bin/python3"
  elif command -v python3 >/dev/null 2>&1; then
    SOURCE_PY="$(command -v python3)"
  else
    echo "[ERROR] 未找到可用 Python（需要 python3）。"
    exit 1
  fi
fi

PY_VER="$("$SOURCE_PY" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
PY_ARCH="$("$SOURCE_PY" -c 'import platform; print(platform.machine())')"
PY_BASE_PREFIX="$("$SOURCE_PY" -c 'import sys; print(sys.base_prefix)')"
FRAMEWORK_ROOT="$(cd "$PY_BASE_PREFIX/../.." && pwd)"

if [ ! -d "$FRAMEWORK_ROOT/Versions/$PY_VER" ]; then
  echo "[ERROR] Python Framework 不完整: $FRAMEWORK_ROOT/Versions/$PY_VER"
  exit 1
fi

echo "[INFO] Embedded Python: $SOURCE_PY"
echo "[INFO] Python version: $PY_VER"
echo "[INFO] Python arch: $PY_ARCH"
echo "[INFO] Framework root: $FRAMEWORK_ROOT"

KEEP_EXISTING_FFMPEG=0
if has_subtitles_filter "$EXISTING_FFMPEG"; then
  KEEP_EXISTING_FFMPEG=1
fi

rm -rf "$PY_RUNTIME_ROOT" "$SITE_PACKAGES_DIR"
if [ "$KEEP_EXISTING_FFMPEG" -ne 1 ]; then
  rm -rf "$RUNTIME_DIR/ffmpeg"
fi
mkdir -p "$PY_RUNTIME_ROOT" "$SITE_PACKAGES_DIR" "$FFMPEG_RUNTIME_DIR"

echo "[INFO] Copying Python.framework ..."
ditto "$FRAMEWORK_ROOT" "$PY_RUNTIME_ROOT/Python.framework"

# Trim optional large folders; keep runtime-required stdlib and dynload modules.
rm -rf "$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/lib/python$PY_VER/test" || true
rm -rf "$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/lib/python$PY_VER/idlelib" || true
rm -rf "$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/lib/python$PY_VER/turtledemo" || true

VENV_SITE="$DEFAULT_VENV/lib/python$PY_VER/site-packages"
EMBED_PY="$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/bin/python$PY_VER"
if [ ! -x "$EMBED_PY" ]; then
  EMBED_PY="$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/bin/python3"
fi

if [ ! -x "$EMBED_PY" ]; then
  echo "[ERROR] Embedded python executable not found."
  exit 1
fi

if [ -d "$VENV_SITE" ]; then
  echo "[INFO] Copying site-packages from existing venv ..."
  ditto "$VENV_SITE" "$SITE_PACKAGES_DIR"
else
  echo "[WARN] Existing venv not found; installing from requirements.txt ..."
  DYLD_FRAMEWORK_PATH="$PY_RUNTIME_ROOT" "$EMBED_PY" -m ensurepip --upgrade
  DYLD_FRAMEWORK_PATH="$PY_RUNTIME_ROOT" "$EMBED_PY" -m pip install --upgrade pip
fi

DYLD_FRAMEWORK_PATH="$PY_RUNTIME_ROOT" "$EMBED_PY" -m pip install -r "$ROOT_DIR/requirements.txt" --target "$SITE_PACKAGES_DIR" --upgrade --force-reinstall

find "$SITE_PACKAGES_DIR" -type d -name "__pycache__" -prune -exec rm -rf {} + || true
find "$SITE_PACKAGES_DIR" -type f -name "*.pyc" -delete || true

if [ -d "$SITE_PACKAGES_DIR/imageio_ffmpeg/binaries" ]; then
  chmod +x "$SITE_PACKAGES_DIR"/imageio_ffmpeg/binaries/* || true
fi

# Build a stable bundled ffmpeg path inside runtime/ffmpeg/bin/ffmpeg
EMBED_PY="$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/bin/python$PY_VER"
if [ ! -x "$EMBED_PY" ]; then
  EMBED_PY="$PY_RUNTIME_ROOT/Python.framework/Versions/$PY_VER/bin/python3"
fi

if [ "$KEEP_EXISTING_FFMPEG" -eq 1 ]; then
  echo "[INFO] Reusing existing bundled ffmpeg with subtitles/libass support ..."
else
  BUNDLED_SRC="$(DYLD_FRAMEWORK_PATH="$PY_RUNTIME_ROOT" PYTHONPATH="$SITE_PACKAGES_DIR" "$EMBED_PY" - <<'PY'
import imageio_ffmpeg
print(imageio_ffmpeg.get_ffmpeg_exe())
PY
)"

  if [ -n "$BUNDLED_SRC" ] && [ -x "$BUNDLED_SRC" ]; then
    cp "$BUNDLED_SRC" "$FFMPEG_RUNTIME_DIR/ffmpeg"
    chmod +x "$FFMPEG_RUNTIME_DIR/ffmpeg"
  fi
fi

# Ensure this bundled ffmpeg can burn subtitles (requires libass filter).
if ! has_subtitles_filter "$FFMPEG_RUNTIME_DIR/ffmpeg"; then
  echo "[ERROR] 内置 ffmpeg 缺少 subtitles/libass 过滤器，无法硬烧录字幕。"
  "$FFMPEG_RUNTIME_DIR/ffmpeg" -version | sed -n '1,3p' || true
  exit 1
fi

echo "$PY_VER" > "$VERSION_FILE"
echo "$PY_ARCH" > "$ARCH_FILE"

echo "[INFO] Verifying embedded runtime ..."
DYLD_FRAMEWORK_PATH="$PY_RUNTIME_ROOT" PYTHONPATH="$SITE_PACKAGES_DIR" "$EMBED_PY" - <<'PY'
import fastapi
import uvicorn
import yt_dlp
import pydantic
import imageio_ffmpeg
from pathlib import Path

ff = imageio_ffmpeg.get_ffmpeg_exe()
print("[OK] imports: fastapi uvicorn yt_dlp pydantic imageio_ffmpeg")
print(f"[OK] imageio ffmpeg: {ff}")
print(f"[OK] ffmpeg exists: {Path(ff).exists()}")
PY

echo "[OK] bundled ffmpeg: $FFMPEG_RUNTIME_DIR/ffmpeg"
echo "[OK] bundled ffmpeg subtitles filter present."

du -sh "$RUNTIME_DIR"
echo "[OK] Embedded runtime prepared at: $RUNTIME_DIR"
