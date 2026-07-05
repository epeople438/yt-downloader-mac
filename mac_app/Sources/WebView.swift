import SwiftUI
import WebKit
import AppKit

struct DownloaderWebView: NSViewRepresentable {
    let url: URL
    let reloadToken: UUID

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        config.defaultWebpagePreferences.allowsContentJavaScript = true
        config.userContentController.add(context.coordinator, name: "appBridge")

        let webView = WKWebView(frame: .zero, configuration: config)
        webView.setValue(false, forKey: "drawsBackground")
        webView.allowsBackForwardNavigationGestures = true
        webView.uiDelegate = context.coordinator
        context.coordinator.webView = webView
        return webView
    }

    func updateNSView(_ webView: WKWebView, context: Context) {
        if context.coordinator.lastToken != reloadToken || webView.url?.absoluteString != url.absoluteString {
            context.coordinator.lastToken = reloadToken
            webView.load(URLRequest(url: url))
        }
    }

    final class Coordinator: NSObject, WKUIDelegate, WKScriptMessageHandler {
        var lastToken: UUID?
        weak var webView: WKWebView?

        private func makeAlert(for webView: WKWebView, title: String, message: String) -> NSAlert {
            let alert = NSAlert()
            alert.alertStyle = .informational
            alert.messageText = title
            alert.informativeText = message
            if let host = webView.url?.host, !host.isEmpty {
                alert.window.title = host
            }
            return alert
        }

        func webView(
            _ webView: WKWebView,
            runJavaScriptAlertPanelWithMessage message: String,
            initiatedByFrame frame: WKFrameInfo,
            completionHandler: @escaping () -> Void
        ) {
            let alert = makeAlert(for: webView, title: "提示", message: message)
            alert.addButton(withTitle: "确定")
            _ = alert.runModal()
            completionHandler()
        }

        func webView(
            _ webView: WKWebView,
            runJavaScriptConfirmPanelWithMessage message: String,
            initiatedByFrame frame: WKFrameInfo,
            completionHandler: @escaping (Bool) -> Void
        ) {
            let alert = makeAlert(for: webView, title: "请确认", message: message)
            alert.addButton(withTitle: "确定")
            alert.addButton(withTitle: "取消")
            completionHandler(alert.runModal() == .alertFirstButtonReturn)
        }

        func webView(
            _ webView: WKWebView,
            runJavaScriptTextInputPanelWithPrompt prompt: String,
            defaultText: String?,
            initiatedByFrame frame: WKFrameInfo,
            completionHandler: @escaping (String?) -> Void
        ) {
            let alert = makeAlert(for: webView, title: "请输入", message: prompt)
            let input = NSTextField(frame: NSRect(x: 0, y: 0, width: 360, height: 24))
            input.stringValue = defaultText ?? ""
            alert.accessoryView = input
            alert.addButton(withTitle: "确定")
            alert.addButton(withTitle: "取消")
            let result = alert.runModal()
            completionHandler(result == .alertFirstButtonReturn ? input.stringValue : nil)
        }

        func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
            guard message.name == "appBridge" else { return }
            guard let body = message.body as? [String: Any],
                  let action = body["action"] as? String else {
                return
            }

            if action == "chooseDirectory" {
                chooseDirectory(currentPath: body["currentPath"] as? String)
            } else if action == "chooseFile" {
                let exts = body["allowedExtensions"] as? [String] ?? []
                chooseFile(
                    currentPath: body["currentPath"] as? String,
                    allowedExtensions: exts,
                    message: body["message"] as? String
                )
            }
        }

        private func sendJavaScriptCallback(_ callback: String, value: String?) {
            let payload: Any = value ?? NSNull()
            let encoded: String
            if let data = try? JSONSerialization.data(withJSONObject: payload, options: [.fragmentsAllowed]),
               let text = String(data: data, encoding: .utf8) {
                encoded = text
            } else {
                encoded = "null"
            }
            webView?.evaluateJavaScript("window.\(callback) && window.\(callback)(\(encoded));")
        }

        private func chooseDirectory(currentPath: String?) {
            let panel = NSOpenPanel()
            panel.canChooseFiles = false
            panel.canChooseDirectories = true
            panel.allowsMultipleSelection = false
            panel.canCreateDirectories = true
            panel.prompt = "选择"
            panel.message = "选择保存目录"
            if let currentPath, !currentPath.isEmpty {
                panel.directoryURL = URL(fileURLWithPath: currentPath, isDirectory: true)
            }

            let result = panel.runModal()
            let selectedPath = result == .OK ? panel.url?.path : nil
            sendJavaScriptCallback("__nativeChooseDirectoryResult", value: selectedPath)
        }

        private func chooseFile(currentPath: String?, allowedExtensions: [String], message: String?) {
            let panel = NSOpenPanel()
            panel.canChooseFiles = true
            panel.canChooseDirectories = false
            panel.allowsMultipleSelection = false
            panel.canCreateDirectories = false
            panel.prompt = "选择"
            panel.message = message ?? "选择文件"
            if !allowedExtensions.isEmpty {
                panel.allowedFileTypes = allowedExtensions
            }
            if let currentPath, !currentPath.isEmpty {
                let url = URL(fileURLWithPath: currentPath)
                panel.directoryURL = url.hasDirectoryPath ? url : url.deletingLastPathComponent()
            }

            let result = panel.runModal()
            let selectedPath = result == .OK ? panel.url?.path : nil
            sendJavaScriptCallback("__nativeChooseFileResult", value: selectedPath)
        }
    }
}
