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
/// > Status: the upload path is live — `enqueue` (transactional write+outbox) and `drainOutbox`
/// > (FK-ordered, idempotent, retrying) ship in APPS-413. `pullNow` (APPS-414) and the background
/// > scheduler / Realtime doorbell that drives `start` (APPS-415) are still stubbed.
public actor SyncEngine {
    private let db: any DatabaseWriter
    private let tables: [SyncTable]
    private let remote: any SyncRemote

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
        try self.init(db: db, remote: SupabaseRemote(client: supabase, auth: auth), tables: tables)
    }

    /// Injects a `SyncRemote` directly — used by tests to drive the drain with a fake.
    init(db: any DatabaseWriter, remote: any SyncRemote, tables: [SyncTable]) throws {
        self.db = db
        self.remote = remote
        self.tables = tables
        self.statusBroadcaster = StatusBroadcaster(initial: SyncStatus())

        try SyncSchema.migrator().migrate(db)
    }

    /// Begins background sync. Idempotent.
    ///
    /// The drain itself (`drainOutbox`) and its backoff policy (`backoffDelay`) are implemented;
    /// what's still missing is the loop that *calls* them on a schedule and on the Realtime
    /// doorbell — that lands in APPS-415, so for now `start` only flips the running flag.
    public func start() {
        isRunning = true
    }

    /// Stops background sync and finishes all status streams.
    public func stop() {
        isRunning = false
        statusBroadcaster.finish()
    }

    /// Records a write in the outbox in the same transaction as the domain write, so the local
    /// store and the pending-upload queue can never disagree. An `.upsert` writes the row to its
    /// table; a `.delete` removes it locally (the tombstone is propagated to the server on drain).
    public func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws {
        guard let spec = tables.first(where: { $0.name == table }) else {
            throw SyncError.unknownTable(table)
        }
        let columns = try RowCoding.encode(row, jsonColumns: Set(spec.jsonColumns))
        guard let pkValue = columns[spec.primaryKey], !pkValue.isNull else {
            throw SyncError.missingPrimaryKey(table: table, column: spec.primaryKey)
        }
        let pk = RowCoding.pkString(pkValue)

        try db.write { db in
            switch op {
            case .upsert:
                try RowCoding.upsertLocalRow(db, table: table, primaryKey: spec.primaryKey, columns: columns)
            case .delete:
                try db.execute(
                    sql: "DELETE FROM \"\(table)\" WHERE \"\(spec.primaryKey)\" = ?",
                    arguments: [pkValue]
                )
            }
            try db.execute(
                sql: """
                    INSERT INTO \(SyncSchema.outboxTable) (table_name, pk, op, queued_at, attempts)
                    VALUES (?, ?, ?, ?, 0)
                    """,
                arguments: [table, pk, op.rawValue, Date()]
            )
        }
    }

    /// Pulls rows changed since each table's cursor and applies them last-write-wins.
    /// - Note: Implemented in APPS-414 (tuple cursor + LWW apply + tombstones).
    public func pullNow() async throws {
        throw SyncError.notImplemented("pullNow lands in APPS-414")
    }

    /// Processes every pending outbox entry once, in FK-safe order. Each op is idempotent by
    /// primary key, so a failed entry is simply left in place (with its `attempts` bumped) to be
    /// retried on the next drain; one failure never blocks the others.
    func drainOutbox() async throws {
        let pending = try await db.read { db in
            try OutboxEntry.fetchAll(db, sql: "SELECT * FROM \(SyncSchema.outboxTable)")
        }
        for entry in orderForUpload(pending, tables: tables) {
            do {
                try await process(entry)
            } catch {
                // Idempotent retry: keep the entry, count the attempt; backoff is the scheduler's job.
                try await db.write { db in
                    try db.execute(
                        sql: "UPDATE \(SyncSchema.outboxTable) SET attempts = attempts + 1 WHERE seq = ?",
                        arguments: [entry.seq]
                    )
                }
            }
        }
    }

    private func process(_ entry: OutboxEntry) async throws {
        guard let spec = tables.first(where: { $0.name == entry.tableName }) else {
            try await clear(entry) // table no longer synced — drop the stale entry
            return
        }
        switch entry.op {
        case .upsert:
            guard let payload = try await readPayload(spec: spec, pk: entry.pk) else {
                try await clear(entry) // row gone locally before upload — nothing to send
                return
            }
            let server = try await remote.upsert(table: spec.name, row: payload)
            try await stampAndClear(entry, spec: spec, server: server)
        case .delete:
            try await remote.delete(table: spec.name, primaryKey: spec.primaryKey, pk: entry.pk)
            try await clear(entry)
        }
    }

    private func readPayload(spec: SyncTable, pk: String) async throws -> [String: AnyJSON]? {
        try await db.read { db in
            guard let row = try Row.fetchOne(
                db,
                sql: "SELECT * FROM \"\(spec.name)\" WHERE \"\(spec.primaryKey)\" = ?",
                arguments: [pk]
            ) else { return nil }
            return RowCoding.payload(
                from: row,
                jsonColumns: Set(spec.jsonColumns),
                excluding: Set(spec.serverOwnedColumns)
            )
        }
    }

    /// Writes the server-stamped `updated_at` back and clears the entry in one transaction, so the
    /// row is marked clean (its cursor won't re-pull it) only after the server confirms.
    private func stampAndClear(_ entry: OutboxEntry, spec: SyncTable, server: [String: AnyJSON]) async throws {
        let updatedAt: String? = if case .string(let value) = server["updated_at"] { value } else { nil }
        try await db.write { db in
            if let updatedAt {
                try db.execute(
                    sql: "UPDATE \"\(spec.name)\" SET \"updated_at\" = ? WHERE \"\(spec.primaryKey)\" = ?",
                    arguments: [updatedAt, entry.pk]
                )
            }
            try db.execute(
                sql: "DELETE FROM \(SyncSchema.outboxTable) WHERE seq = ?",
                arguments: [entry.seq]
            )
        }
    }

    private func clear(_ entry: OutboxEntry) async throws {
        try await db.write { db in
            try db.execute(
                sql: "DELETE FROM \(SyncSchema.outboxTable) WHERE seq = ?",
                arguments: [entry.seq]
            )
        }
    }
}
