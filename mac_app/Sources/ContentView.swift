import SwiftUI
import AppKit

struct ContentView: View {
    @EnvironmentObject var backend: BackendController
    @State private var showLogs = false

    private var canStart: Bool {
        !backend.isRunning && backend.state != .preparing && backend.state != .starting
    }

    private var canRestart: Bool {
        if backend.isRunning {
            return true
        }
        switch backend.state {
        case .failed, .idle:
            return true
        default:
            return false
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            Group {
                if backend.isRunning {
                    DownloaderWebView(url: backend.serverURL, reloadToken: backend.reloadToken)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    placeholder
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                }
            }

            if showLogs {
                Divider()
                    .opacity(0.45)
                logPanel
            }
        }
        .background(Color(NSColor.windowBackgroundColor))
        .onReceive(NotificationCenter.default.publisher(for: NSApplication.willTerminateNotification)) { _ in
            backend.stop(force: true)
        }
    }

    private var placeholder: some View {
        VStack(spacing: 14) {
            Image(systemName: "tray.and.arrow.down")
                .font(.system(size: 38, weight: .regular))
                .foregroundStyle(.secondary)

            Text("服务准备中")
                .font(.title3.weight(.semibold))

            Text("后端启动后会自动加载下载界面。\n如果启动失败，请点击下方“启动服务”或“重启服务”。")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 460)

            HStack(spacing: 10) {
                Button {
                    Task { await backend.start() }
                } label: {
                    Label("启动服务", systemImage: "play.fill")
                }
                .buttonStyle(.borderedProminent)
                .tint(.blue)
                .disabled(!canStart)

                Button {
                    Task { await backend.restart() }
                } label: {
                    Label("重启服务", systemImage: "arrow.clockwise")
                }
                .buttonStyle(.bordered)
                .disabled(!canRestart)
            }

            HStack(spacing: 10) {
                Button {
                    withAnimation(.easeInOut(duration: 0.15)) {
                        showLogs.toggle()
                    }
                } label: {
                    Label(showLogs ? "隐藏开发者日志" : "查看开发者日志", systemImage: "doc.text.magnifyingglass")
                }
                .buttonStyle(.bordered)

                Button {
                    NSWorkspace.shared.open(backend.serverURL)
                } label: {
                    Label("浏览器打开", systemImage: "safari")
                }
                .buttonStyle(.bordered)
                .disabled(!backend.isRunning)
            }

            if case .failed(let reason) = backend.state {
                Text(reason)
                    .font(.system(size: 12, weight: .medium))
                    .foregroundStyle(.red)
                    .padding(.top, 2)
            }
        }
        .padding(24)
    }

    private var logPanel: some View {
        ScrollView {
            Text(backend.logText.isEmpty ? "暂无日志" : backend.logText)
                .font(.system(size: 11, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(12)
        }
        .frame(height: 180)
        .background(Color(NSColor.textBackgroundColor).opacity(0.9))
    }
}
