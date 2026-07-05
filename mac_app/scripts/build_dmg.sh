#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT_PATH="$ROOT_DIR/YouTubeDownloaderMac.xcodeproj"
SCHEME="YouTubeDownloaderMac"
CONFIG="Release"
BUILD_ROOT="$ROOT_DIR/build"
ARCHIVE_PATH="$BUILD_ROOT/$SCHEME.xcarchive"
EXPORT_DIR="$BUILD_ROOT/dmg"
APP_NAME="$SCHEME.app"
DIST_DIR="$ROOT_DIR/dist"
DMG_PATH="$DIST_DIR/$SCHEME.dmg"

if [ ! -d "$PROJECT_PATH" ]; then
  echo "[ERROR] 未找到 $PROJECT_PATH，请先运行 xcodegen generate"
  exit 1
fi

echo "[INFO] Preparing backend bundle (source + pinned runtime)..."
SRCROOT="$ROOT_DIR" "$ROOT_DIR/mac_app/scripts/xcode_prepare_backend.sh"

mkdir -p "$BUILD_ROOT" "$DIST_DIR"
rm -rf "$ARCHIVE_PATH" "$EXPORT_DIR" "$DMG_PATH"

xcodebuild \
  -project "$PROJECT_PATH" \
  -scheme "$SCHEME" \
  -configuration "$CONFIG" \
  -derivedDataPath "$BUILD_ROOT/DerivedData" \
  -archivePath "$ARCHIVE_PATH" \
  archive \
  CODE_SIGNING_ALLOWED=NO

mkdir -p "$EXPORT_DIR"
ditto "$ARCHIVE_PATH/Products/Applications/$APP_NAME" "$EXPORT_DIR/$APP_NAME"

if ! hdiutil create \
  -volname "$SCHEME" \
  -srcfolder "$EXPORT_DIR" \
  -ov \
  -format UDZO \
  "$DMG_PATH"; then
  echo "[WARN] 第一次创建 DMG 失败，1 秒后重试..."
  sleep 1
  if ! hdiutil create \
    -volname "$SCHEME" \
    -srcfolder "$EXPORT_DIR" \
    -ov \
    -format UDZO \
    "$DMG_PATH"; then
    echo "[ERROR] 自动创建 DMG 失败，请手动执行："
    echo "hdiutil create -volname \"$SCHEME\" -srcfolder \"$EXPORT_DIR\" -ov -format UDZO \"$DMG_PATH\""
    exit 1
  fi
fi

echo "[OK] DMG 已生成: $DMG_PATH"
