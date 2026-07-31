import SwiftUI
import DemoCore

/// A HappySync demo you can run with no backend at all: the engine is wired to the in-memory fake
/// from `HappySyncTestSupport`, so "another device" is a button (issue #58).
///
/// Everything the app does — writing, deleting with a cascade, reading through `ValueObservation`,
/// showing status, repairing parked writes — is the real engine. Only the transport is fake.
@main
struct HappySyncDemoApp: App {
    @StateObject private var store = DemoStore.live()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(store)
                .task { await store.start() }
        }
    }
}
