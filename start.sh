#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

PYBIN="python3"
if ! command -v "$PYBIN" >/dev/null 2>&1; then
  PYBIN="python"
fi

if [ ! -d .venv ]; then
  "$PYBIN" -m venv .venv
fi

source .venv/bin/activate
pip -q install -r requirements.txt
python main.py
