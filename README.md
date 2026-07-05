# YouTube Downloader Mac

本项目是一个本地运行的 macOS YouTube 视频/音频下载工具，重点支持下载、字幕生成、字幕翻译和本地视频字幕处理。

## 功能

- 下载视频或纯音频，支持批量 URL 和队列任务。
- 支持英文字幕、英文 + 中文字幕、无字幕视频 Whisper 转写。
- 支持 Codex CLI 翻译中文字幕。
- 支持软字幕封装和硬字幕烧录。
- 支持本地已有视频生成字幕。
- 支持合集任务续处理、缺失字幕识别和补处理。
- 支持 cookies.txt 兜底登录态、稳定下载模式、yt-dlp 手动更新。

## 运行源码

要求：

- macOS 13+
- Python 3.10+
- FFmpeg
- 可选：Codex CLI，用于字幕翻译
- 可选：openai-whisper，用于无字幕视频转写

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

然后打开：

```text
http://127.0.0.1:8000
```

macOS 也可以双击 `run_mac.command` 启动本地 Web 版本。

## 运行 macOS App

直接打开 `YouTubeDownloaderMac.xcodeproj`，选择 `YouTubeDownloaderMac` scheme 后运行。

Xcode 构建会自动把 `app/`、`static/`、`main.py` 和依赖运行时同步到 `mac_app/BackendBundle/`。运行时目录不提交到 Git，首次构建会自动准备。

## 打包 DMG

```bash
mac_app/scripts/build_dmg.sh
```

生成文件位于：

```text
dist/YouTubeDownloaderMac.dmg
```

当前脚本生成的是本地未签名 DMG。如需公开分发，建议配置 Developer ID 签名和 notarization。

## 配置和隐私

本地配置会写入 `data/config.json`，其中可能包含保存目录、代理或 cookies 文件路径。该文件已被 `.gitignore` 排除，不应提交。

不要把以下内容提交到公开仓库：

- `data/*.json`
- `dist/`
- `build/`
- `mac_app/BackendBundle/runtime/`
- `cookies.txt`
- 下载的视频、音频、字幕文件

## 免责声明

仅供个人学习和研究使用。请遵守所在地法律法规以及相关平台服务条款。
