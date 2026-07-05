import Foundation
import SwiftUI
import Darwin

@MainActor
final class BackendController: ObservableObject {
    enum State: Equatable {
        case idle
        case preparing
        case starting
        case running
        case stopping
        case failed(String)
    }

    @Published var state: State = .idle
    @Published var logText: String = ""
    @Published var reloadToken: UUID = UUID()

    let host = "127.0.0.1"
    @Published private(set) var port = 8000

    private var process: Process?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?
    private var runtimeBackendURL: URL?
    private var expectedHealthToken: String = ""

    var serverURL: URL {
        URL(string: "http://\(host):\(port)/")!
    }

    var isRunning: Bool {
        if case .running = state { return true }
        return false
    }

    var statusText: String {
        switch state {
        case .idle:
            return "未启动"
        case .preparing:
            return "准备运行环境"
        case .starting:
            return "正在启动服务"
        case .running:
            return "服务运行中"
        case .stopping:
            return "正在停止"
        case .failed(let reason):
            return "启动失败：\(reason)"
        }
    }

    func start() async {
        if process?.isRunning == true {
            return
        }

        appendLog("[APP] Starting backend...")
        state = .preparing

        do {
            let runtime = try await Task.detached(priority: .userInitiated) {
                try Self.prepareRuntimeBackend()
            }.value

            runtimeBackendURL = runtime
            let candidatePorts = await Task.detached(priority: .userInitiated, operation: {
                Self.candidatePorts()
            }).value

            guard !candidatePorts.isEmpty else {
                throw NSError(
                    domain: "YouTubeDownloaderMac",
                    code: 1002,
                    userInfo: [NSLocalizedDescriptionKey: "没有可用端口，请关闭占用进程后重试。"]
                )
            }

            var lastErrorMessage = "服务启动失败，请检查日志"
            for (idx, candidatePort) in candidatePorts.enumerated() {
                if process?.isRunning == true {
                    stop(force: true)
                    try? await Task.sleep(nanoseconds: 200_000_000)
                }

                port = candidatePort
                expectedHealthToken = UUID().uuidString
                appendLog("[APP] Launch attempt \(idx + 1)/\(candidatePorts.count), port: \(port)")

                do {
                    try runBackendProcess(in: runtime, port: port, token: expectedHealthToken)
                } catch {
                    lastErrorMessage = "启动进程失败：\(error.localizedDescription)"
                    appendLog("[APP] \(lastErrorMessage)")
                    continue
                }

                state = .starting
                appendLog("[APP] Backend process launched, waiting for health check...")

                let healthy = await waitForHealth(maxAttempts: 60, delayNanos: 500_000_000, expectedToken: expectedHealthToken)
                if healthy {
                    state = .running
                    reloadToken = UUID()
                    appendLog("[APP] Backend ready at \(serverURL.absoluteString)")
                    return
                }

                lastErrorMessage = "端口 \(port) 启动失败，尝试下一个端口"
                appendLog("[APP] \(lastErrorMessage)")
                stop(force: true)
                try? await Task.sleep(nanoseconds: 200_000_000)
            }

            throw NSError(
                domain: "YouTubeDownloaderMac",
                code: 1003,
                userInfo: [NSLocalizedDescriptionKey: lastErrorMessage]
            )
        } catch {
            stop(force: true)
            state = .failed(error.localizedDescription)
            appendLog("[APP] Start failed: \(error.localizedDescription)")
        }
    }

    func restart() async {
        stop(force: true)
        try? await Task.sleep(nanoseconds: 400_000_000)
        await start()
    }

    func stop(force: Bool = false) {
        guard let process else {
            state = .idle
            return
        }

        state = .stopping
        appendLog("[APP] Stopping backend...")

        stdoutPipe?.fileHandleForReading.readabilityHandler = nil
        stderrPipe?.fileHandleForReading.readabilityHandler = nil

        if process.isRunning {
            process.terminate()
            if force {
                let pid = process.processIdentifier
                DispatchQueue.global().asyncAfter(deadline: .now() + 1.2) {
                    if process.isRunning {
                        kill(pid_t(pid), SIGKILL)
                    }
                }
            }
        }

        self.process = nil
        self.stdoutPipe = nil
        self.stderrPipe = nil
        state = .idle
    }

    private func runBackendProcess(in runtime: URL, port: Int, token: String) throws {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: "/bin/bash")
        p.arguments = ["-lc", Self.launchScript(runtimePath: runtime.path, host: host, port: port, token: token)]

        let out = Pipe()
        let err = Pipe()
        p.standardOutput = out
        p.standardError = err

        attach(pipe: out)
        attach(pipe: err)

        p.terminationHandler = { [weak self] proc in
            Task { @MainActor in
                self?.handleTermination(proc)
            }
        }

        try p.run()

        process = p
        stdoutPipe = out
        stderrPipe = err

        appendLog("[APP] PID: \(p.processIdentifier)")
    }

    private func handleTermination(_ proc: Process) {
        if let current = process, current !== proc {
            return
        }

        let code = proc.terminationStatus
        appendLog("[APP] Backend exited with code \(code)")

        stdoutPipe?.fileHandleForReading.readabilityHandler = nil
        stderrPipe?.fileHandleForReading.readabilityHandler = nil

        process = nil
        stdoutPipe = nil
        stderrPipe = nil

        if case .stopping = state {
            state = .idle
            return
        }

        if case .idle = state {
            return
        }

        if code == 0 {
            state = .idle
        } else {
            state = .failed("进程退出码 \(code)")
        }
    }

    private func waitForHealth(maxAttempts: Int, delayNanos: UInt64, expectedToken: String) async -> Bool {
        for _ in 0..<maxAttempts {
            if await isHealthy(expectedToken: expectedToken) {
                return true
            }

            if process?.isRunning != true {
                return false
            }

            try? await Task.sleep(nanoseconds: delayNanos)
        }
        return false
    }

    private func isHealthy(expectedToken: String) async -> Bool {
        let url = serverURL.appendingPathComponent("api/health")
        var req = URLRequest(url: url)
        req.timeoutInterval = 1.5

        do {
            let (data, resp) = try await URLSession.shared.data(for: req)
            guard let http = resp as? HTTPURLResponse else { return false }
            guard (200...299).contains(http.statusCode) else { return false }

            guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                return false
            }

            guard let token = json["instance_token"] as? String else {
                return false
            }

            return token == expectedToken
        } catch {
            return false
        }
    }

    private func attach(pipe: Pipe) {
        pipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }

            let text = String(decoding: data, as: UTF8.self)
            Task { @MainActor in
                self?.appendLog(text)
            }
        }
    }

    private func appendLog(_ text: String) {
        let normalized = text.replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")
            .trimmingCharacters(in: .newlines)

        guard !normalized.isEmpty else { return }

        if logText.isEmpty {
            logText = normalized
        } else {
            logText += "\n" + normalized
        }

        let lines = logText.split(separator: "\n", omittingEmptySubsequences: false)
        if lines.count > 800 {
            logText = lines.suffix(800).joined(separator: "\n")
        }
    }

    nonisolated private static func prepareRuntimeBackend() throws -> URL {
        let fm = FileManager.default

        guard let bundleRoot = locateBundledBackendRoot() else {
            throw NSError(
                domain: "YouTubeDownloaderMac",
                code: 1001,
                userInfo: [NSLocalizedDescriptionKey: "App 内未找到后端资源（BackendBundle 或扁平 Resources）"]
            )
        }

        let appSupport = try fm.url(for: .applicationSupportDirectory,
                                    in: .userDomainMask,
                                    appropriateFor: nil,
                                    create: true)

        let runtimeRoot = appSupport.appendingPathComponent("YouTubeDownloaderPro", isDirectory: true)
        let runtimeBackend = runtimeRoot.appendingPathComponent("backend", isDirectory: true)

        try fm.createDirectory(at: runtimeRoot, withIntermediateDirectories: true)

        if !fm.fileExists(atPath: runtimeBackend.path) {
            try fm.copyItem(at: bundleRoot, to: runtimeBackend)
            try migrateRuntimeConfigIfNeeded(in: runtimeBackend)
            return runtimeBackend
        }

        try syncEntry(named: "app", from: bundleRoot, to: runtimeBackend)
        try syncEntry(named: "static", from: bundleRoot, to: runtimeBackend)
        try syncEntry(named: "main.py", from: bundleRoot, to: runtimeBackend)
        try syncEntry(named: "requirements.txt", from: bundleRoot, to: runtimeBackend)
        let runtimeSrc = bundleRoot.appendingPathComponent("runtime", isDirectory: true)
        if fm.fileExists(atPath: runtimeSrc.path) {
            try syncEntry(named: "runtime", from: bundleRoot, to: runtimeBackend)
        }

        let dataDst = runtimeBackend.appendingPathComponent("data", isDirectory: true)
        if !fm.fileExists(atPath: dataDst.path) {
            let dataSrc = bundleRoot.appendingPathComponent("data", isDirectory: true)
            if fm.fileExists(atPath: dataSrc.path) {
                try fm.copyItem(at: dataSrc, to: dataDst)
            }
        }

        try migrateRuntimeConfigIfNeeded(in: runtimeBackend)
        return runtimeBackend
    }

    nonisolated private static func locateBundledBackendRoot() -> URL? {
        var candidates: [URL] = []
        if let res = Bundle.main.resourceURL {
            candidates.append(res.appendingPathComponent("BackendBundle", isDirectory: true))
            candidates.append(res)
        }

        let bundle = Bundle.main.bundleURL
        candidates.append(bundle.appendingPathComponent("Contents/Resources/BackendBundle", isDirectory: true))
        candidates.append(bundle.appendingPathComponent("Contents/Resources", isDirectory: true))
        candidates.append(bundle.appendingPathComponent("Contents", isDirectory: true))

        for c in candidates {
            if isValidBackendRoot(c) {
                return c
            }
        }
        return nil
    }

    nonisolated private static func isValidBackendRoot(_ root: URL) -> Bool {
        let fm = FileManager.default
        let appDir = root.appendingPathComponent("app", isDirectory: true).path
        let staticDir = root.appendingPathComponent("static", isDirectory: true).path
        let mainPy = root.appendingPathComponent("main.py").path
        let req = root.appendingPathComponent("requirements.txt").path

        return fm.fileExists(atPath: appDir)
            && fm.fileExists(atPath: staticDir)
            && fm.fileExists(atPath: mainPy)
            && fm.fileExists(atPath: req)
    }

    nonisolated private static func syncEntry(named name: String, from srcRoot: URL, to dstRoot: URL) throws {
        let fm = FileManager.default
        let src = srcRoot.appendingPathComponent(name)
        let dst = dstRoot.appendingPathComponent(name)

        if fm.fileExists(atPath: dst.path) {
            try fm.removeItem(at: dst)
        }
        try fm.copyItem(at: src, to: dst)
    }

    nonisolated private static func migrateRuntimeConfigIfNeeded(in runtimeBackend: URL) throws {
        let fm = FileManager.default
        let configURL = runtimeBackend
            .appendingPathComponent("data", isDirectory: true)
            .appendingPathComponent("config.json", isDirectory: false)

        guard fm.fileExists(atPath: configURL.path) else {
            return
        }

        let raw = try Data(contentsOf: configURL)
        guard var obj = (try JSONSerialization.jsonObject(with: raw)) as? [String: Any] else {
            return
        }

        guard let rawProxy = obj["proxy"] as? String else {
            return
        }
        let proxy = rawProxy.trimmingCharacters(in: .whitespacesAndNewlines)
        if proxy.isEmpty {
            return
        }

        let legacyProxies: Set<String> = [
            "socks5h://127.0.0.1:7897",
            "socks5://127.0.0.1:7897",
            "http://127.0.0.1:7897",
            "http://localhost:7897",
            "socks5h://localhost:7897",
            "socks5://localhost:7897",
        ]

        var shouldClear = legacyProxies.contains(proxy.lowercased())

        if !shouldClear, let (host, port) = parseProxyHostAndPort(proxy) {
            if isLoopbackHost(host) && !isPortReachable(port) {
                shouldClear = true
            }
        }

        guard shouldClear else {
            return
        }

        obj["proxy"] = ""
        let updated = try JSONSerialization.data(
            withJSONObject: obj,
            options: [.prettyPrinted, .sortedKeys]
        )
        try updated.write(to: configURL, options: .atomic)
    }

    nonisolated private static func parseProxyHostAndPort(_ proxy: String) -> (String, Int)? {
        guard let comp = URLComponents(string: proxy), let host = comp.host else {
            return nil
        }

        let port: Int
        if let p = comp.port {
            port = p
        } else {
            switch (comp.scheme ?? "").lowercased() {
            case "https":
                port = 443
            case "http":
                port = 80
            case let scheme where scheme.hasPrefix("socks"):
                port = 1080
            default:
                return nil
            }
        }
        return (host, port)
    }

    nonisolated private static func isLoopbackHost(_ host: String) -> Bool {
        let h = host.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if h == "localhost" || h == "::1" {
            return true
        }
        if h.hasPrefix("127.") {
            return true
        }
        return false
    }

    nonisolated private static func launchScript(runtimePath: String, host: String, port: Int, token: String) -> String {
        let escaped = runtimePath.replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")

        return """
        set -e
        cd "\(escaped)"
        export PATH="$HOME/.local/bin:$HOME/Library/Python/3.13/bin:$HOME/Library/Python/3.12/bin:$HOME/Library/Python/3.11/bin:/opt/homebrew/bin:/usr/local/bin:/Library/Frameworks/Python.framework/Versions/Current/bin:/Library/Frameworks/Python.framework/Versions/3.13/bin:/Library/Frameworks/Python.framework/Versions/3.12/bin:/Library/Frameworks/Python.framework/Versions/3.11/bin:$PATH"

        EMBEDDED_ROOT="./runtime"
        EMBEDDED_PY_ROOT="$EMBEDDED_ROOT/python"
        EMBEDDED_SITE="$EMBEDDED_ROOT/site-packages"
        EMBEDDED_VERSION_FILE="$EMBEDDED_ROOT/PYTHON_VERSION"
        EMBEDDED_PY=""

        if [ -f "$EMBEDDED_VERSION_FILE" ]; then
          PY_VER="$(tr -d '[:space:]' < "$EMBEDDED_VERSION_FILE")"
          if [ -n "$PY_VER" ] && [ -x "$EMBEDDED_PY_ROOT/Python.framework/Versions/$PY_VER/bin/python$PY_VER" ]; then
            EMBEDDED_PY="$EMBEDDED_PY_ROOT/Python.framework/Versions/$PY_VER/bin/python$PY_VER"
          fi
        fi

        if [ -z "$EMBEDDED_PY" ] && [ -x "$EMBEDDED_PY_ROOT/Python.framework/Versions/Current/bin/python3" ]; then
          EMBEDDED_PY="$EMBEDDED_PY_ROOT/Python.framework/Versions/Current/bin/python3"
        fi

        if [ -z "$EMBEDDED_PY" ] && [ -x "$EMBEDDED_PY_ROOT/Python.framework/Versions/3.14/bin/python3.14" ]; then
          EMBEDDED_PY="$EMBEDDED_PY_ROOT/Python.framework/Versions/3.14/bin/python3.14"
        fi

        if [ -n "$EMBEDDED_PY" ] && [ -d "$EMBEDDED_SITE" ]; then
          export DYLD_FRAMEWORK_PATH="$EMBEDDED_PY_ROOT"
          export PYTHONPATH="$EMBEDDED_SITE"
          BUNDLED_FFMPEG="$EMBEDDED_ROOT/ffmpeg/bin/ffmpeg"
          if [ -x "$BUNDLED_FFMPEG" ]; then
            export YTDL_FFMPEG="$BUNDLED_FFMPEG"
            export YTDL_FORCE_BUNDLED_FFMPEG=1
            export PATH="$EMBEDDED_ROOT/ffmpeg/bin:$EMBEDDED_SITE/imageio_ffmpeg/binaries:$PATH"
          else
            export YTDL_FORCE_BUNDLED_FFMPEG=1
            export PATH="$EMBEDDED_SITE/imageio_ffmpeg/binaries:$PATH"
          fi
          export YTDL_HOST=\(host)
          export YTDL_PORT=\(port)
          export YTDL_INSTANCE_TOKEN=\(token)
          "$EMBEDDED_PY" main.py
          exit $?
        fi

        echo "[WARN] 未找到内置运行时，回退到系统 Python 模式。"

        if ! command -v python3 >/dev/null 2>&1; then
          echo "[ERROR] 未检测到 python3，请先安装 Python 3。"
          exit 127
        fi

        if [ ! -d ".venv" ]; then
          python3 -m venv .venv
        fi

        source .venv/bin/activate

        REQ_HASH=$(shasum requirements.txt | awk '{print $1}')
        INSTALLED_HASH=""
        if [ -f ".venv/.requirements_hash" ]; then
          INSTALLED_HASH=$(cat .venv/.requirements_hash)
        fi

        if [ "$REQ_HASH" != "$INSTALLED_HASH" ]; then
          python3 -m pip install -r requirements.txt --upgrade
          echo "$REQ_HASH" > .venv/.requirements_hash
        fi

        export YTDL_HOST=\(host)
        export YTDL_PORT=\(port)
        export YTDL_INSTANCE_TOKEN=\(token)
        python3 main.py
        """
    }

    nonisolated private static func candidatePorts() -> [Int] {
        let preferred = Array(8000...8020)
        var freePorts: [Int] = []
        var occupiedPorts: [Int] = []

        for p in preferred {
            if isPortReachable(p) {
                occupiedPorts.append(p)
            } else {
                freePorts.append(p)
            }
        }

        var result = freePorts + occupiedPorts
        var randomFallback: [Int] = []

        while randomFallback.count < 10 {
            let p = Int.random(in: 10000...30000)
            if !result.contains(p) && !randomFallback.contains(p) {
                randomFallback.append(p)
            }
        }
        result.append(contentsOf: randomFallback)

        return result
    }

    nonisolated private static func isPortReachable(_ port: Int) -> Bool {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { return true }
        defer { close(fd) }

        var addr = sockaddr_in()
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = in_port_t(UInt16(port).bigEndian)
        addr.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))

        let connectResult = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }

        return connectResult == 0
    }
}
