#!/bin/bash

# Make sure common Homebrew locations are available when double-clicking
# this script (PATH can be different vs an interactive shell).
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
set -e
cd "$(dirname "$0")"

# macOS double-click starter

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] 没找到 python3。请先安装 Python 3（建议用官方安装包或 Homebrew）。"
  echo "安装后重新双击 run_mac.command 即可。"
  read -n 1 -s -r -p "按任意键退出..."
  echo
  exit 1
fi

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate

# Install/upgrade deps (YouTube rules change frequently; keep yt-dlp fresh)
pip install -r requirements.txt --upgrade

# Start server
export YTDL_HOST=${YTDL_HOST:-127.0.0.1}
export YTDL_PORT=${YTDL_PORT:-8000}

# AUTO_PORT: if the default port is in use, pick the next available one.
PORT=$YTDL_PORT
for p in $(seq $PORT $((PORT+20))); do
  if lsof -iTCP:$p -sTCP:LISTEN >/dev/null 2>&1; then
    continue
  fi
  PORT=$p
  break
done
export YTDL_PORT=$PORT

URL="http://$YTDL_HOST:$YTDL_PORT/"

echo "\n[INFO] 启动服务: $URL"
echo "[INFO] 关闭服务请在此窗口按 Ctrl+C\n"

python main.py &
PID=$!

# Give server a moment, then open browser
sleep 1
open "$URL" >/dev/null 2>&1 || true

# Wait for server process
wait $PID
