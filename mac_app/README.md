# macOS App

这是 YouTube Downloader Mac 的 SwiftUI 外壳。App 内部启动本地 Python 后端，并用 WebView 加载前端界面。

## 开发运行

1. 打开项目根目录下的 `YouTubeDownloaderMac.xcodeproj`。
2. 选择 `YouTubeDownloaderMac` scheme。
3. 直接 Run。

构建前脚本会同步后端源码到 `mac_app/BackendBundle/`，并在需要时准备内置 Python runtime。

## 打包

从项目根目录运行：

```bash
mac_app/scripts/build_dmg.sh
```

输出：

```text
dist/YouTubeDownloaderMac.dmg
```

默认 DMG 未签名，仅适合本地测试。公开分发请配置 Developer ID 签名和 notarization。
