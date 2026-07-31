import SwiftUI
import DemoCore

struct ContentView: View {
    @EnvironmentObject private var store: DemoStore

    var body: some View {
        VStack(spacing: 0) {
            SyncStatusBar()
            Divider()
            HStack(alignment: .top, spacing: 0) {
                ListsPane()
                Divider()
                ItemsPane()
            }
            Divider()
            DemoControls()
        }
        .frame(minWidth: 640, minHeight: 460)
    }
}

// MARK: - Lists

private struct ListsPane: View {
    @EnvironmentObject private var store: DemoStore
    @State private var title = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Lists").font(.headline)
            List(store.lists) { list in
                Button {
                    store.selectedListID = list.id
                } label: {
                    Text(list.title)
                        .foregroundStyle(list.id == store.selectedListID ? Color.accentColor : .primary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .buttonStyle(.plain)
            }
            HStack {
                TextField("New list", text: $title)
                Button("Add") { add() }.disabled(title.isEmpty)
            }
            // One plain DELETE. SQLite cascades to the list's items and each removed item's own
            // capture trigger queues its tombstone — watch the delete count in the controls below.
            Button("Delete list (cascades)") {
                Task { await store.deleteSelectedList() }
            }
            .disabled(store.selectedListID == nil)
        }
        .padding()
        .frame(minWidth: 240)
    }

    @MainActor private func add() {
        let value = title
        title = ""
        Task { await store.addList(title: value) }
    }
}

// MARK: - Items

private struct ItemsPane: View {
    @EnvironmentObject private var store: DemoStore
    @State private var title = ""

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Items").font(.headline)
            List(store.items(in: store.selectedListID)) { item in
                Button {
                    Task { await store.toggle(item) }
                } label: {
                    Label(item.title, systemImage: item.done ? "checkmark.circle.fill" : "circle")
                }
                .buttonStyle(.plain)
            }
            HStack {
                TextField("New item", text: $title)
                Button("Add") { add() }.disabled(title.isEmpty || store.selectedListID == nil)
            }
        }
        .padding()
        .frame(maxWidth: .infinity)
    }

    @MainActor private func add() {
        let value = title
        title = ""
        Task { await store.addItem(title: value) }
    }
}

// MARK: - The parts of sync a UI has to get right

private struct SyncStatusBar: View {
    @EnvironmentObject private var store: DemoStore

    var body: some View {
        HStack {
            // Ask `isHealthy`, never `phase == .idle` alone: a settled pass with writes still failing
            // or parked is `.degraded`, and a green tick there is the bug this API exists to prevent.
            Circle()
                .fill(store.status.isHealthy ? Color.green : Color.orange)
                .frame(width: 10, height: 10)
            Text(store.healthLine)
            Spacer()
            if let lastSynced = store.status.lastSyncedAt {
                Text("last sync \(lastSynced.formatted(date: .omitted, time: .standard))")
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal)
        .padding(.vertical, 8)
    }
}

private struct DemoControls: View {
    @EnvironmentObject private var store: DemoStore

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Button("Change from device B") { Task { await store.publishFromAnotherDevice() } }
                Button("Make the server reject writes") { Task { await store.sync.rejectWrites() } }
                Button("Let the server accept again") { Task { await store.sync.acceptWrites() } }
                Button("Sign out → wipe → sign in") { Task { await signOutAndBackIn() } }
            }
            DeadLetterInspector()
        }
        .padding()
    }

    /// The order is the point: `stop()` awaits the in-flight pass, and only then is the store safe to
    /// replace. The demo re-observes afterwards because the database object itself was swapped.
    @MainActor private func signOutAndBackIn() async {
        try? await store.sync.signOutAndBackIn()
        store.observe()
    }
}

/// Parked writes, and the two ways out of them. Repair is an API, not a support ticket: a write that
/// the server refused permanently stops retrying and waits here until the cause is fixed.
private struct DeadLetterInspector: View {
    @EnvironmentObject private var store: DemoStore

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("Parked writes (\(store.deadLetters.count))").font(.headline)
                Spacer()
                Button("Retry") { Task { await store.retryDeadLetters() } }
                Button("Discard") { Task { await store.discardDeadLetters() } }
            }
            .disabled(store.deadLetters.isEmpty)

            ForEach(store.deadLetters, id: \.seq) { letter in
                Text("\(letter.op.rawValue) \(letter.table)/\(letter.pk) — \(letter.failure.description)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }
}
