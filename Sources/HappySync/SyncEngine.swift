import Foundation
import os
import GRDB
import Supabase

/// Multicasts the latest `SyncStatus` to any number of subscribers, replaying the latest snapshot
/// to each new one. CookThis has two consumers (the status UI and a refresh loop); a bare
/// `AsyncStream` is single-consumer, so the engine fans out through this instead.
final class StatusBroadcaster: Sendable {
    private struct State {
        var latest: SyncStatus
        var subscribers: [UUID: AsyncStream<SyncStatus>.Continuation] = [:]
    }

    private let state: OSAllocatedUnfairLock<State>

    init(initial: SyncStatus) {
        state = OSAllocatedUnfairLock(initialState: State(latest: initial))
    }

    /// A fresh stream that immediately replays the latest status, then receives every update.
    func subscribe() -> AsyncStream<SyncStatus> {
        AsyncStream { continuation in
            let id = UUID()
            let latest = state.withLock { s -> SyncStatus in
                s.subscribers[id] = continuation
                return s.latest
            }
            continuation.yield(latest)
            continuation.onTermination = { [state] _ in
                state.withLock { _ = $0.subscribers.removeValue(forKey: id) }
            }
        }
    }

    /// Records the new status and fans it out. Yields outside the lock to avoid reentrancy.
    func send(_ status: SyncStatus) {
        let continuations = state.withLock { s -> [AsyncStream<SyncStatus>.Continuation] in
            s.latest = status
            return Array(s.subscribers.values)
        }
        for continuation in continuations { continuation.yield(status) }
    }

    func finish() {
        let continuations = state.withLock { s -> [AsyncStream<SyncStatus>.Continuation] in
            let all = Array(s.subscribers.values)
            s.subscribers.removeAll()
            return all
        }
        for continuation in continuations { continuation.finish() }
    }
}

/// The HappySync engine: owns the outbox drain, cursor pull, tombstones, FK ordering,
/// Realtime doorbell, status, and retry/backoff.
///
/// It does **not** own reads — the app keeps observing GRDB with `ValueObservation` — or schema.
///
/// > Scaffold (APPS-412): the public surface and the internal-table migrations are real; the
/// > sync behaviour (`start`/`enqueue`/`pullNow`) is stubbed pending APPS-413/414/415.
public actor SyncEngine {
    private let db: any DatabaseWriter
    private let supabase: SupabaseClient
    private let tables: [SyncTable]
    private let auth: @Sendable () async -> String

    private nonisolated let statusBroadcaster: StatusBroadcaster
    private var isRunning = false

    /// Live engine status. Each access returns an independent stream that replays the latest
    /// snapshot, so multiple consumers (status UI, refresh loop) can observe concurrently.
    public nonisolated var status: AsyncStream<SyncStatus> {
        statusBroadcaster.subscribe()
    }

    /// - Parameters:
    ///   - db: GRDB writer (`DatabaseQueue` or `DatabasePool`) — the local source of truth.
    ///   - supabase: Supabase client for PostgREST upsert/pull and the Realtime doorbell.
    ///   - tables: Synced tables in any order; the engine sorts them by `dependsOn`.
    ///   - auth: Returns a fresh Supabase access token, called before each authenticated batch.
    public init(
        db: any DatabaseWriter,
        supabase: SupabaseClient,
        tables: [SyncTable],
        auth: @escaping @Sendable () async -> String
    ) throws {
        self.db = db
        self.supabase = supabase
        self.tables = tables
        self.auth = auth
        self.statusBroadcaster = StatusBroadcaster(initial: SyncStatus())

        try SyncSchema.migrator().migrate(db)
    }

    /// Begins draining the outbox and pulling changes. Idempotent.
    public func start() {
        // ponytail: flag only for now — uploader/downloader/doorbell land in APPS-413/414/415.
        isRunning = true
    }

    /// Stops background sync and finishes all status streams.
    public func stop() {
        isRunning = false
        statusBroadcaster.finish()
    }

    /// Records a write in the outbox in the same transaction as the domain write.
    /// - Note: Implemented in APPS-413 (transactional write + ordered idempotent drain).
    public func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws {
        throw SyncError.notImplemented("enqueue lands in APPS-413")
    }

    /// Pulls rows changed since each table's cursor and applies them last-write-wins.
    /// - Note: Implemented in APPS-414 (tuple cursor + LWW apply + tombstones).
    public func pullNow() async throws {
        throw SyncError.notImplemented("pullNow lands in APPS-414")
    }
}
