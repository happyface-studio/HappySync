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

/// A tombstone seen during a pull, deferred so all deletes can be applied children-first.
private struct PendingDelete: Sendable {
    let table: String
    let primaryKey: String
    let pk: String
    let updatedAt: String
}

/// The HappySync engine: owns the outbox drain, cursor pull, tombstones, FK ordering,
/// Realtime doorbell, status, and retry/backoff.
///
/// It does **not** own reads — the app keeps observing GRDB with `ValueObservation` — or schema.
///
/// > Status: upload (`enqueue` + `drainOutbox`, APPS-413), download (`pullNow` — tuple cursor,
/// > LWW, tombstones, pagination, APPS-414), and the scheduler that drives them (`start` — debounced
/// > Realtime doorbell, periodic fallback, status, backoff, APPS-415) are all live. M1 is complete.
public actor SyncEngine {
    private let db: any DatabaseWriter
    private let tables: [SyncTable]
    private let remote: any SyncRemote
    private let pageSize: Int
    private let doorbell: any SyncDoorbell
    private let pollInterval: TimeInterval
    private let debounceInterval: TimeInterval

    private nonisolated let statusBroadcaster: StatusBroadcaster
    private var isRunning = false
    private var lastSyncedAt: Date?
    private var consecutiveFailures = 0
    private var loopTask: Task<Void, Never>?
    private var debounceTask: Task<Void, Never>?
    private var wake: AsyncStream<Void>.Continuation?

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
        try self.init(
            db: db,
            remote: SupabaseRemote(client: supabase, auth: auth),
            tables: tables,
            doorbell: SupabaseDoorbell(client: supabase, tables: tables.map(\.name))
        )
    }

    /// Injects the `SyncRemote`/`SyncDoorbell` seams directly — used by tests to drive sync with
    /// fakes. `pageSize` forces pagination on a small dataset; `pollInterval`/`debounceInterval` let
    /// tests shrink the scheduler's timing without waiting on production intervals.
    init(
        db: any DatabaseWriter,
        remote: any SyncRemote,
        tables: [SyncTable],
        pageSize: Int = 500,
        doorbell: any SyncDoorbell = SilentDoorbell(),
        pollInterval: TimeInterval = 30,
        debounceInterval: TimeInterval = 0.3
    ) throws {
        self.db = db
        self.remote = remote
        self.tables = tables
        self.pageSize = pageSize
        self.doorbell = doorbell
        self.pollInterval = pollInterval
        self.debounceInterval = debounceInterval
        self.statusBroadcaster = StatusBroadcaster(initial: SyncStatus())

        try SyncSchema.migrator().migrate(db)
    }

    /// Begins background sync. Idempotent. Runs an immediate convergence sync, then keeps the local
    /// store in sync from three trigger sources, all funnelled through one serialised runner:
    ///  - the Realtime **doorbell**, debounced so a burst of change events collapses to one pull;
    ///  - a **periodic** poll that converges even if the Realtime channel drops;
    ///  - (the doorbell/periodic both retry with exponential `backoffDelay` while syncs are failing).
    public func start() {
        guard !isRunning else { return }
        isRunning = true

        let (stream, continuation) = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
        wake = continuation
        // ponytail: the running subtasks hold `self` for the engine's lifetime; `stop()` is the
        // teardown that cancels them. Fine for an app-lifetime engine, revisit if it must be GC'd live.
        loopTask = Task { [weak self] in
            guard let self else { return }
            await withTaskGroup(of: Void.self) { group in
                group.addTask { await self.runLoop(stream) }
                group.addTask { await self.doorbellLoop() }
                group.addTask { await self.periodicLoop() }
            }
        }
        poke() // converge immediately on start
    }

    /// Stops background sync and finishes all status streams.
    public func stop() {
        isRunning = false
        loopTask?.cancel(); loopTask = nil
        debounceTask?.cancel(); debounceTask = nil
        wake?.finish(); wake = nil
        statusBroadcaster.finish()
    }

    /// The single serialised runner: one sync at a time, so pushes and pulls never overlap. Pokes
    /// that arrive mid-run coalesce (the wake channel buffers only the newest), so a backlog drains
    /// in one extra pass rather than one-per-poke.
    private func runLoop(_ wake: AsyncStream<Void>) async {
        for await _ in wake {
            do {
                try await runSyncOnce()
                consecutiveFailures = 0
            } catch {
                consecutiveFailures += 1 // status already shows .failed; the periodic loop retries with backoff
            }
        }
    }

    /// Each doorbell ring (re)arms a trailing debounce; only the last ring in a burst survives to
    /// poke the runner, so a multi-row remote change triggers exactly one pull.
    private func doorbellLoop() async {
        for await _ in doorbell.ring() {
            debounceTask?.cancel()
            debounceTask = Task { [debounceInterval, weak self] in
                try? await Task.sleep(for: .seconds(debounceInterval))
                guard !Task.isCancelled else { return }
                await self?.poke()
            }
        }
    }

    /// Periodic convergence: pokes every `pollInterval` while healthy, or after `backoffDelay` once
    /// syncs start failing. This is what guarantees convergence if the Realtime doorbell goes silent.
    private func periodicLoop() async {
        while !Task.isCancelled {
            let delay = nextDelay(consecutiveFailures: consecutiveFailures, pollInterval: pollInterval)
            try? await Task.sleep(for: .seconds(delay))
            guard !Task.isCancelled else { break }
            poke()
        }
    }

    /// Requests an immediate sync through the serialised runner — for app-driven triggers like
    /// returning to the foreground or pull-to-refresh. Coalesces with any in-flight run; a no-op
    /// until `start()` has been called.
    public func syncNow() {
        poke()
    }

    private func poke() {
        wake?.yield(())
    }

    /// One full sync pass: push local changes then pull remote ones, driving `status` across the
    /// run (`.syncing` → `.idle` on success with `lastSyncedAt` stamped, or `.failed` then rethrow).
    /// The scheduler serialises these, so a push and pull never overlap.
    func runSyncOnce() async throws {
        statusBroadcaster.send(SyncStatus(phase: .syncing, lastSyncedAt: lastSyncedAt))
        do {
            try await drainOutbox()
            try await pullNow()
        } catch {
            statusBroadcaster.send(SyncStatus(phase: .failed("\(error)"), lastSyncedAt: lastSyncedAt))
            throw error
        }
        lastSyncedAt = Date()
        statusBroadcaster.send(SyncStatus(phase: .idle, lastSyncedAt: lastSyncedAt))
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

    /// Pulls rows changed since each table's `(updated_at, id)` cursor and applies them
    /// last-write-wins. Upserts run parents-first (so a child's FK target exists before the child);
    /// tombstones are deferred and applied children-first (so a parent is never deleted out from
    /// under a child). Each page's applies + cursor-advance happen in one transaction; pages are
    /// pulled until one comes back short.
    public func pullNow() async throws {
        let order = topologicalOrder(tables)
        var pendingDeletes: [PendingDelete] = []

        for tableName in order {
            guard let spec = tables.first(where: { $0.name == tableName }) else { continue }
            var cursor = try await readCursor(table: spec.name)
            while true {
                let page = try await remote.fetch(
                    table: spec.name, since: cursor, primaryKey: spec.primaryKey, limit: pageSize
                )
                if page.isEmpty { break }

                let pageCursor = cursor // immutable copy for the Sendable write closure
                let (advanced, deletes) = try await db.write { db -> (SyncCursor?, [PendingDelete]) in
                    var advanced = pageCursor
                    var deferred: [PendingDelete] = []
                    for row in page {
                        let pk = row[spec.primaryKey]?.stringValue ?? ""
                        let updatedAt = row["updated_at"]?.stringValue ?? ""
                        let tombstoned = !(row["deleted_at"]?.isNil ?? true)
                        if try Self.lwwAllows(
                            db, table: spec.name, primaryKey: spec.primaryKey, pk: pk, remoteUpdatedAt: updatedAt
                        ) {
                            if tombstoned {
                                deferred.append(PendingDelete(
                                    table: spec.name, primaryKey: spec.primaryKey, pk: pk, updatedAt: updatedAt
                                ))
                            } else {
                                try RowCoding.upsertLocalRow(
                                    db, table: spec.name, primaryKey: spec.primaryKey,
                                    columns: RowCoding.localColumns(from: row)
                                )
                            }
                        }
                        advanced = SyncCursor(updatedAt: updatedAt, id: pk) // cursor advances whether or not we applied
                    }
                    if let advanced { try Self.writeCursor(db, table: spec.name, cursor: advanced) }
                    return (advanced, deferred)
                }
                if let advanced { cursor = advanced }
                pendingDeletes.append(contentsOf: deletes)
                if page.count < pageSize { break }
            }
        }

        // Phase 2: tombstones, children-first (reverse FK order).
        let rank = Dictionary(uniqueKeysWithValues: order.enumerated().map { ($1, $0) })
        for del in pendingDeletes.sorted(by: { (rank[$0.table] ?? 0) > (rank[$1.table] ?? 0) }) {
            try await db.write { db in
                // Re-check the gate: a local edit may have arrived between phases.
                guard try Self.lwwAllows(
                    db, table: del.table, primaryKey: del.primaryKey, pk: del.pk, remoteUpdatedAt: del.updatedAt
                ) else { return }
                try db.execute(
                    sql: "DELETE FROM \"\(del.table)\" WHERE \"\(del.primaryKey)\" = ?",
                    arguments: [del.pk]
                )
            }
        }
    }

    /// Last-write-wins gate, evaluated inside the apply transaction: a remote row is applied only
    /// if the local row isn't dirty (no pending outbox entry — its queued upload wins) and the
    /// remote is strictly newer than the local copy (an absent local row always loses).
    private static func lwwAllows(
        _ db: Database, table: String, primaryKey: String, pk: String, remoteUpdatedAt: String
    ) throws -> Bool {
        let dirty = try Int.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE table_name = ? AND pk = ?",
            arguments: [table, pk]
        ) ?? 0
        if dirty > 0 { return false }

        guard let local: String = try String.fetchOne(
            db,
            sql: "SELECT updated_at FROM \"\(table)\" WHERE \"\(primaryKey)\" = ?",
            arguments: [pk]
        ) else {
            return true // no local row (or never-stamped) → apply
        }
        return remoteUpdatedAt > local // ISO-8601 UTC sorts lexicographically
    }

    private func readCursor(table: String) async throws -> SyncCursor? {
        try await db.read { db in
            guard let row = try Row.fetchOne(
                db,
                sql: "SELECT updated_at, last_id FROM \(SyncSchema.stateTable) WHERE table_name = ?",
                arguments: [table]
            ), let updatedAt: String = row["updated_at"], let id: String = row["last_id"] else {
                return nil
            }
            return SyncCursor(updatedAt: updatedAt, id: id)
        }
    }

    private static func writeCursor(_ db: Database, table: String, cursor: SyncCursor) throws {
        try db.execute(
            sql: """
                INSERT INTO \(SyncSchema.stateTable) (table_name, updated_at, last_id) VALUES (?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET updated_at = excluded.updated_at, last_id = excluded.last_id
                """,
            arguments: [table, cursor.updatedAt, cursor.id]
        )
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
