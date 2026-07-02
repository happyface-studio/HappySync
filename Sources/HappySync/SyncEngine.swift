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
    let cursorColumn: String
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
    /// Resolves the current user's download-partition value (auth uid) for tables that declare a
    /// `scopeColumn`, or nil when signed out. Called per pull so a user switch re-scopes (APPS-469).
    private let scope: @Sendable () async -> String?
    /// After this many failed upload attempts a transient entry is dead-lettered (parked). A
    /// permanent (4xx) failure parks immediately regardless. See APPS-470.
    private let deadLetterAfter: Int

    private nonisolated let statusBroadcaster: StatusBroadcaster
    private var isRunning = false
    private var lastSyncedAt: Date?
    private var consecutiveFailures = 0
    /// The serialized runner consuming the wake stream. Kept separate from the trigger loops so
    /// `stop()` can let the in-flight pass finish gracefully (finish the wake stream, don't cancel)
    /// while cancelling the triggers (APPS-473).
    private var runnerTask: Task<Void, Never>?
    /// The doorbell + periodic loops that poke the runner. Cancelled on `stop()`.
    private var triggersTask: Task<Void, Never>?
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
    ///   - scope: Resolves the current user's download-partition value (auth uid) for tables that
    ///     declare a `scopeColumn`. Defaults to `nil` (no partition beyond RLS). Called per pull, so
    ///     a signed-in user change re-scopes without re-declaring tables. See APPS-469.
    public init(
        db: any DatabaseWriter,
        supabase: SupabaseClient,
        tables: [SyncTable],
        auth: @escaping @Sendable () async -> String,
        scope: @escaping @Sendable () async -> String? = { nil }
    ) throws {
        try self.init(
            db: db,
            remote: SupabaseRemote(client: supabase, auth: auth),
            tables: tables,
            doorbell: SupabaseDoorbell(
                client: supabase,
                tables: tables.map { ($0.name, $0.scopeColumn) },
                scope: scope
            ),
            scope: scope
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
        debounceInterval: TimeInterval = 0.3,
        scope: @escaping @Sendable () async -> String? = { nil },
        deadLetterAfter: Int = 8
    ) throws {
        self.db = db
        self.remote = remote
        self.tables = tables
        self.pageSize = pageSize
        self.doorbell = doorbell
        self.pollInterval = pollInterval
        self.debounceInterval = debounceInterval
        self.scope = scope
        self.deadLetterAfter = deadLetterAfter
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
        // The runner is its own task (not in the trigger group) so `stop()` can drain it gracefully.
        runnerTask = Task { [weak self] in
            guard let self else { return }
            await self.runLoop(stream)
        }
        triggersTask = Task { [weak self] in
            guard let self else { return }
            await withTaskGroup(of: Void.self) { group in
                group.addTask { await self.doorbellLoop() }
                group.addTask { await self.periodicLoop() }
            }
        }
        poke() // converge immediately on start
    }

    /// Stops background sync and finishes all status streams. **Awaits the in-flight sync pass**
    /// before returning: after `await stop()` the engine has quiesced — no further DB writes and no
    /// network calls — so a consumer can safely wipe or repurpose the database (e.g. on sign-out /
    /// account switch). Idempotent. See APPS-473 and the README teardown section.
    public func stop() async {
        guard isRunning else { return }
        isRunning = false
        wake?.finish(); wake = nil            // no more passes enqueued; runner exits after the current one
        triggersTask?.cancel()                // stop the doorbell + periodic loops from poking
        debounceTask?.cancel(); debounceTask = nil
        await runnerTask?.value               // await the in-flight pass to finish (uncancelled → completes)
        await triggersTask?.value             // and the trigger loops (incl. doorbell channel teardown) to unwind
        runnerTask = nil; triggersTask = nil
        statusBroadcaster.finish()
    }

    /// The single serialised runner: one sync at a time, so pushes and pulls never overlap. Pokes
    /// that arrive mid-run coalesce (the wake channel buffers only the newest), so a backlog drains
    /// in one extra pass rather than one-per-poke.
    private func runLoop(_ wake: AsyncStream<Void>) async {
        for await _ in wake {
            do {
                let outcome = try await runSyncOnce()
                // A pull can succeed while uploads keep failing. Transient upload failures still
                // drive scheduler backoff (so a flapping upload isn't hammered); dead-lettered
                // entries don't — they've stopped retrying, so the engine is healthy again (APPS-470).
                consecutiveFailures = outcome.failed > 0 ? consecutiveFailures + 1 : 0
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
    ///
    /// The settled `.idle` status still carries `failedUploads`/`deadLetters`: a drain can complete
    /// (so the *pass* is idle) while individual entries are still failing or parked. Health is
    /// `phase == .idle && failedUploads == 0 && deadLetters == 0`, not the phase alone (APPS-470).
    @discardableResult
    func runSyncOnce() async throws -> DrainOutcome {
        statusBroadcaster.send(SyncStatus(phase: .syncing, lastSyncedAt: lastSyncedAt))
        let outcome: DrainOutcome
        do {
            outcome = try await drainOutbox()
            try await pullNow()
        } catch {
            statusBroadcaster.send(SyncStatus(phase: .failed("\(error)"), lastSyncedAt: lastSyncedAt))
            throw error
        }
        lastSyncedAt = Date()
        let deadLetters = try await db.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 1") ?? 0
        }
        statusBroadcaster.send(SyncStatus(
            phase: .idle, lastSyncedAt: lastSyncedAt,
            failedUploads: outcome.failed, deadLetters: deadLetters
        ))
        return outcome
    }

    /// Records a write in the outbox in the same transaction as the domain write, so the local
    /// store and the pending-upload queue can never disagree. An `.upsert` writes the row to its
    /// table; a `.delete` removes it locally (the tombstone is propagated to the server on drain).
    ///
    /// Supported row value types: `String`, numbers, `Bool`, `Date` (encoded as canonical ISO-8601,
    /// APPS-475), `null`, and — for columns declared in `jsonColumns` — nested objects/arrays.
    /// `Data`/blob fields are **not** supported and throw `SyncError.encoding`.
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
            var scopeFilter: ScopeFilter?
            if let scopeColumn = spec.scopeColumn {
                // Scoped table: resolve the partition value for the signed-in user. Signed out (nil)
                // → skip the table this pass rather than pull it unfiltered (which would download the
                // whole public catalog); the periodic loop retries once a value is available.
                guard let value = await scope() else { continue }
                scopeFilter = ScopeFilter(column: scopeColumn, value: value)
            }
            var cursor = try await readCursor(table: spec.name)
            while true {
                let page = try await remote.fetch(
                    table: spec.name, cursorColumn: spec.cursorColumn, since: cursor,
                    primaryKey: spec.primaryKey, scope: scopeFilter, limit: pageSize
                )
                if page.isEmpty { break }

                let pageCursor = cursor // immutable copy for the Sendable write closure
                let (advanced, deletes) = try await db.write { db -> (SyncCursor?, [PendingDelete]) in
                    var advanced = pageCursor
                    var deferred: [PendingDelete] = []
                    for row in page {
                        let pk = row[spec.primaryKey]?.stringValue ?? ""
                        let updatedAt = row[spec.cursorColumn]?.stringValue ?? ""
                        let tombstoned = !(row["deletedAt"]?.isNil ?? true)
                        if try Self.lwwAllows(
                            db, table: spec.name, cursorColumn: spec.cursorColumn,
                            primaryKey: spec.primaryKey, pk: pk, remoteUpdatedAt: updatedAt
                        ) {
                            if tombstoned {
                                deferred.append(PendingDelete(
                                    table: spec.name, primaryKey: spec.primaryKey,
                                    cursorColumn: spec.cursorColumn, pk: pk, updatedAt: updatedAt
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
                    db, table: del.table, cursorColumn: del.cursorColumn,
                    primaryKey: del.primaryKey, pk: del.pk, remoteUpdatedAt: del.updatedAt
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
        _ db: Database, table: String, cursorColumn: String, primaryKey: String, pk: String, remoteUpdatedAt: String
    ) throws -> Bool {
        // A dead-lettered entry is excluded: it has stopped retrying, so it must not keep the row
        // dirty and block every remote update forever (APPS-470).
        let dirty = try Int.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE table_name = ? AND pk = ? AND dead_lettered = 0",
            arguments: [table, pk]
        ) ?? 0
        if dirty > 0 { return false }

        guard let local: String = try String.fetchOne(
            db,
            sql: "SELECT \"\(cursorColumn)\" FROM \"\(table)\" WHERE \"\(primaryKey)\" = ?",
            arguments: [pk]
        ) else {
            return true // no local row (or never-stamped) → apply
        }
        // Canonicalize both sides to one format before the lexicographic compare: PostgREST
        // (`…+00:00`, microseconds), client (`…123Z`), and non-fractional legacy strings otherwise
        // sort inconsistently (e.g. a fractional remote vs a non-fractional local of the same
        // second). Same-format canonical strings sort chronologically (APPS-474).
        return SyncTimestamp.canonicalize(remoteUpdatedAt) > SyncTimestamp.canonicalize(local)
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

    /// Result of one outbox drain pass (APPS-470): `failed` entries hit a transient error and are
    /// still being retried with backoff; `deadLettered` entries were parked this pass (permanent
    /// failure, or the transient retry cap reached) and will not be retried again.
    struct DrainOutcome: Sendable, Equatable {
        var failed: Int
        var deadLettered: Int
    }

    /// Processes every pending (non-parked) outbox entry once, in FK-safe order. Each op is
    /// idempotent by primary key. A failed entry stays in place with `attempts`/`last_attempt_at`
    /// bumped and is skipped until its per-entry backoff window elapses; one that fails permanently
    /// (4xx) or exhausts `deadLetterAfter` retries is dead-lettered (parked). One failure never
    /// blocks the others.
    @discardableResult
    func drainOutbox(now: Date = Date()) async throws -> DrainOutcome {
        let pending = try await db.read { db in
            try OutboxEntry.fetchAll(db, sql: "SELECT * FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 0")
        }
        // Net each (table, pk) to one op first (APPS-472), then FK-order the net ops.
        let collapsed = collapseOutbox(pending)
        let groupSeqs = Dictionary(uniqueKeysWithValues: collapsed.map { ($0.net.seq, $0.seqs) })
        var outcome = DrainOutcome(failed: 0, deadLettered: 0)
        for entry in orderForUpload(collapsed.map(\.net), tables: tables) {
            // Per-entry exponential backoff: skip an entry still inside its retry window so a failing
            // entry isn't re-attempted on every drain pass (and doorbell ring and poll).
            if let last = entry.lastAttemptAt, now.timeIntervalSince(last) < backoffDelay(attempts: entry.attempts) {
                continue
            }
            let seqs = groupSeqs[entry.seq] ?? [entry.seq]
            do {
                try await process(entry, clearing: seqs)
            } catch {
                let attempts = entry.attempts + 1
                // Permanent (4xx) → park now; transient → park once it exhausts the retry cap.
                let park = remoteErrorIsPermanent(error) || attempts >= deadLetterAfter
                try await recordFailure(entry, groupSeqs: seqs, attempts: attempts, now: now, park: park, error: error)
                if park { outcome.deadLettered += 1 } else { outcome.failed += 1 }
            }
        }
        return outcome
    }

    /// Records a failed upload attempt: bumps `attempts`, stamps `last_attempt_at` (for the backoff
    /// window) and `last_error` (telemetry) on the net entry. When parking, the **whole collapsed
    /// group** is dead-lettered together — else a superseded older op (e.g. an orphaned delete)
    /// could become the net op on a later drain and mis-apply.
    private func recordFailure(_ entry: OutboxEntry, groupSeqs: [Int64], attempts: Int, now: Date, park: Bool, error: Error) async throws {
        try await db.write { db in
            try db.execute(
                sql: """
                    UPDATE \(SyncSchema.outboxTable)
                    SET attempts = ?, last_attempt_at = ?, last_error = ?, dead_lettered = ?
                    WHERE seq = ?
                    """,
                arguments: [attempts, now, "\(error)", park ? 1 : 0, entry.seq]
            )
            if park {
                let others = groupSeqs.filter { $0 != entry.seq }
                if !others.isEmpty {
                    let placeholders = others.map { _ in "?" }.joined(separator: ", ")
                    try db.execute(
                        sql: "UPDATE \(SyncSchema.outboxTable) SET dead_lettered = 1, last_error = ? WHERE seq IN (\(placeholders))",
                        arguments: StatementArguments(["\(error)"] as [any DatabaseValueConvertible] + others.map { $0 as any DatabaseValueConvertible })
                    )
                }
            }
        }
    }

    /// Applies one collapsed op (the net op for a `(table, pk)`) and, on success, clears **every**
    /// `seqs` entry the group collapsed (APPS-472) — not just the net one.
    private func process(_ entry: OutboxEntry, clearing seqs: [Int64]) async throws {
        guard let spec = tables.first(where: { $0.name == entry.tableName }) else {
            try await clear(seqs) // table no longer synced — drop the stale entries
            return
        }
        switch entry.op {
        case .upsert:
            guard let payload = try await readPayload(spec: spec, pk: entry.pk) else {
                try await clear(seqs) // row gone locally before upload — nothing to send
                return
            }
            let server = try await remote.upsert(table: spec.name, row: payload)
            try await stampAndClear(entry, spec: spec, server: server, seqs: seqs)
        case .delete:
            try await remote.delete(table: spec.name, primaryKey: spec.primaryKey, pk: entry.pk)
            try await clear(seqs)
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

    /// Writes the server-stamped `updated_at` back and clears the collapsed group's entries in one
    /// transaction, so the row is marked clean (its cursor won't re-pull it) only after the server
    /// confirms.
    private func stampAndClear(_ entry: OutboxEntry, spec: SyncTable, server: [String: AnyJSON], seqs: [Int64]) async throws {
        let updatedAt: String? = if case .string(let value) = server[spec.cursorColumn] { value } else { nil }
        try await db.write { db in
            if let updatedAt {
                try db.execute(
                    sql: "UPDATE \"\(spec.name)\" SET \"\(spec.cursorColumn)\" = ? WHERE \"\(spec.primaryKey)\" = ?",
                    arguments: [updatedAt, entry.pk]
                )
            }
            try Self.deleteEntries(db, seqs: seqs)
        }
    }

    private func clear(_ seqs: [Int64]) async throws {
        try await db.write { db in try Self.deleteEntries(db, seqs: seqs) }
    }

    private static func deleteEntries(_ db: Database, seqs: [Int64]) throws {
        guard !seqs.isEmpty else { return }
        let placeholders = seqs.map { _ in "?" }.joined(separator: ", ")
        try db.execute(
            sql: "DELETE FROM \(SyncSchema.outboxTable) WHERE seq IN (\(placeholders))",
            arguments: StatementArguments(seqs)
        )
    }
}
