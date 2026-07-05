import SwiftUI

@main
struct YouTubeDownloaderMacApp: App {
    @StateObject private var backend = BackendController()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(backend)
                .frame(minWidth: 1120, minHeight: 760)
                .task {
                    await backend.start()
                }
        }
        .windowStyle(.hiddenTitleBar)
    }
}
