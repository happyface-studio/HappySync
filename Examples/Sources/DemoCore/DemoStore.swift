import Foundation
import Combine
import GRDB
import HappySync

/// View state for the demo: observed reads, the live sync status, and the dead-letter inspector.
///
/// The reads are the part worth noticing — they're **plain GRDB `ValueObservation`**. HappySync
/// never sits between the app and its data: a pulled row lands in SQLite and the observation fires,
/// exactly as it does for a local write.
@MainActor
public final class DemoStore: ObservableObject {
    @Published public private(set) var lists: [TodoList] = []
    @Published public private(set) var items: [TodoItem] = []
    /// The engine's live status. Drive UI off `isHealthy`, not `phase == .idle` — see `healthLine`.
    @Published public private(set) var status = SyncStatus()
    /// Writes the server refused, parked so they stop retrying. Refreshed whenever the status says
    /// the count changed.
    @Published public private(set) var deadLetters: [DeadLetter] = []
    @Published public var selectedListID: String?

    public let sync: DemoSync

    private var readsTask: Task<Void, Never>?
    private var statusTask: Task<Void, Never>?

    public init(sync: DemoSync) {
        self.sync = sync
    }

    /// Builds the whole stack. Trapping on failure is a demo's privilege: every error here is a
    /// programming mistake (a manifest that disagrees with the schema, a migration that didn't run),
    /// which is precisely what `SyncEngine.init` is designed to surface loudly at launch.
    public static func live() -> DemoStore {
        do {
            return DemoStore(sync: try DemoSync())
        } catch {
            fatalError("HappySync demo failed to start: \(error)")
        }
    }

    /// Starts sync and begins observing. Safe to call again after `signOutAndBackIn` replaced the
    /// store — it re-points the observations at the new database.
    public func start() async {
        await sync.start()
        observe()
    }

    public func observe() {
        readsTask?.cancel()
        statusTask?.cancel()

        let database = sync.db
        readsTask = Task { [weak self] in
            let observation = ValueObservation.tracking { db -> ([TodoList], [TodoItem]) in
                (try TodoList.order(Column("title")).fetchAll(db),
                 try TodoItem.order(Column("title")).fetchAll(db))
            }
            do {
                for try await (lists, items) in observation.values(in: database) {
                    guard let self else { return }
                    self.lists = lists
                    self.items = items
                    if self.selectedListID == nil || !lists.contains(where: { $0.id == self.selectedListID }) {
                        self.selectedListID = lists.first?.id
                    }
                }
            } catch {
                // The store was replaced out from under the observation (sign-out). `observe()` runs
                // again against the new one.
            }
        }

        let engine = sync.engine
        statusTask = Task { [weak self] in
            for await status in engine.status {
                guard let self else { return }
                let parkedCountChanged = status.deadLetters != self.status.deadLetters
                self.status = status
                if parkedCountChanged { await self.refreshDeadLetters() }
            }
        }
    }

    public func items(in listID: String?) -> [TodoItem] {
        guard let listID else { return [] }
        return items.filter { $0.listId == listID }
    }

    // MARK: - Actions

    public func addList(title: String) async {
        try? await sync.addList(title: title)
    }

    public func addItem(title: String) async {
        guard let listID = selectedListID else { return }
        try? await sync.addItem(title: title, to: listID)
    }

    public func toggle(_ item: TodoItem) async {
        try? await sync.toggle(itemID: item.id)
    }

    public func deleteSelectedList() async {
        guard let listID = selectedListID else { return }
        try? await sync.deleteList(id: listID)
    }

    public func publishFromAnotherDevice() async {
        await sync.publishFromAnotherDevice(title: "From device B")
    }

    // MARK: - Dead-letter repair

    public func refreshDeadLetters() async {
        deadLetters = (try? await sync.engine.deadLetters()) ?? []
    }

    /// Re-queue parked writes. Do this *after* fixing the cause — an RLS policy, a migration, an app
    /// update — or they park again on the next pass.
    public func retryDeadLetters() async {
        try? await sync.engine.retryDeadLetters()
        await refreshDeadLetters()
    }

    /// Abandon the local writes and take the server's version instead. Discard also re-pulls the
    /// affected rows, so a row that diverged while parked converges rather than staying stuck.
    public func discardDeadLetters() async {
        try? await sync.engine.discardDeadLetters()
        await refreshDeadLetters()
    }

    // MARK: - Status, read the way a UI should read it

    /// `.idle` alone is the tempting check and the wrong one: it means "no pass is running", which is
    /// also true while the user's writes sit in the outbox failing. Ask `isHealthy` first, then
    /// branch — `.degraded` is a settled pass with writes outstanding, and `.failed` carries a
    /// classified cause rather than prose to substring-match.
    public var healthLine: String {
        if status.isHealthy { return "Synced" }
        switch status.phase {
        case .syncing: return "Syncing…"
        case .idle: return "Synced"
        case .degraded:
            return "\(status.failedUploads) retrying, \(status.deadLetters) parked"
        case .failed(.authExpired): return "Session expired — sign in again"
        case .failed(.network): return "Offline — will retry"
        case .failed(let failure): return "Sync failed: \(failure)"
        }
    }
}
