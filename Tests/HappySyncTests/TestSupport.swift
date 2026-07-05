import Foundation
import os
import GRDB
import Supabase
@testable import HappySync

/// A `SyncRemote` test double: records upload calls, can simulate a run of upsert failures, and
/// serves downloads from an in-memory `dataset` honouring the `(updated_at, id)` tuple cursor and
/// page limit — so ordering, retry, LWW, tombstones, and pagination are all observable offline.
actor FakeRemote: SyncRemote {
    let serverUpdatedAt = "2026-06-30T12:00:00.000Z"
    private(set) var upsertCalls: [(table: String, row: [String: AnyJSON], onConflict: String?)] = []
    private(set) var deleteCalls: [(table: String, pk: String)] = []
    private(set) var fetchCalls = 0
    /// The scope passed to the most recent `fetch` — lets a test assert the engine forwarded the
    /// resolved partition filter (APPS-469).
    private(set) var lastScope: ScopeFilter?
    private var remainingFailures: Int
    private var remainingFetchFailures: Int
    private let permanentUpsertFailures: Bool
    private let upsertError: Error?
    private let dataset: [String: [[String: AnyJSON]]]

    /// - failUpserts: number of upserts that throw before succeeding.
    /// - permanentUpserts: when true, those upsert failures are *permanent* (a classified 4xx-shaped
    ///   error) so the drain dead-letters them immediately; when false they're transient (retryable).
    /// - upsertError: a specific transport error to raise on each failed upsert (e.g. `HTTPError(401)`
    ///   or a `PostgrestError`). It's wrapped in `RemoteFailure` with the production classification,
    ///   exactly as `SupabaseRemote` does — so the drain sees a faithfully-classified failure. Takes
    ///   precedence over `permanentUpserts`.
    init(
        failUpserts: Int = 0,
        failFetches: Int = 0,
        permanentUpserts: Bool = false,
        upsertError: Error? = nil,
        dataset: [String: [[String: AnyJSON]]] = [:]
    ) {
        remainingFailures = failUpserts
        remainingFetchFailures = failFetches
        permanentUpsertFailures = permanentUpserts
        self.upsertError = upsertError
        self.dataset = dataset
    }

    enum Failure: Error { case simulated }
    /// A permanent (non-retryable) upsert failure — conforms to the engine's retry-classification
    /// seam so the drain dead-letters it on the first attempt.
    struct PermanentFailure: ClassifiedSyncError { let isPermanent = true }

    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        upsertCalls.append((table, row, onConflict))
        if remainingFailures > 0 {
            remainingFailures -= 1
            if let upsertError {
                // Mirror SupabaseRemote.classify so the drain sees the same RemoteFailure it would in prod.
                throw RemoteFailure(isPermanent: remoteErrorIsPermanent(upsertError), underlying: upsertError)
            }
            throw permanentUpsertFailures ? PermanentFailure() : Failure.simulated
        }
        var server = row
        server["updatedAt"] = .string(serverUpdatedAt)
        return server
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {
        deleteCalls.append((table, pk))
    }

    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]] {
        fetchCalls += 1
        lastScope = scope
        if remainingFetchFailures > 0 { remainingFetchFailures -= 1; throw Failure.simulated }
        let scoped = (dataset[table] ?? []).filter { row in
            guard let scope else { return true }
            return row[scope.column]?.stringValue == scope.value
        }
        let sorted = scoped.sorted { tuple($0, cursorColumn, primaryKey) < tuple($1, cursorColumn, primaryKey) }
        let filtered: [[String: AnyJSON]]
        if let cursor {
            filtered = sorted.filter { tuple($0, cursorColumn, primaryKey) > (cursor.updatedAt, cursor.id) }
        } else {
            filtered = sorted
        }
        return Array(filtered.prefix(limit))
    }

    private func tuple(_ row: [String: AnyJSON], _ cursorColumn: String, _ primaryKey: String) -> (String, String) {
        (row[cursorColumn]?.stringValue ?? "", row[primaryKey]?.stringValue ?? "")
    }
}

/// A one-shot async signal: `wait()` suspends until someone calls `fire()` (idempotent). Lets a
/// test rendezvous with background work without polling — e.g. block a fetch mid-pass (APPS-473).
actor Signal {
    private var fired = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    /// Non-blocking peek — has `fire()` been called yet?
    var isFired: Bool { fired }

    func fire() {
        guard !fired else { return }
        fired = true
        for waiter in waiters { waiter.resume() }
        waiters.removeAll()
    }

    func wait() async {
        if fired { return }
        await withCheckedContinuation { waiters.append($0) }
    }
}

/// A `SyncRemote` whose `fetch` announces it has started, then blocks on `gate` until the test
/// releases it — so a test can hold a sync pass in-flight while it exercises `stop()` (APPS-473).
struct GatedRemote: SyncRemote {
    let started: Signal
    let gate: Signal
    let dataset: [String: [[String: AnyJSON]]]

    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] { row }
    func delete(table: String, primaryKey: String, pk: String) async throws {}

    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]] {
        await started.fire()
        await gate.wait()
        return dataset[table] ?? []
    }
}

/// A `SyncRemote` whose `upsert` returns a caller-supplied *representation* of the server row —
/// letting a test prove the drain writes the full server row back locally, not just the cursor:
/// server-normalized column values and the `conflictColumns` merged row (re-keyed to the client's
/// pk) included (APPS-506). `fetch`/`delete` are inert.
actor RepresentationRemote: SyncRemote {
    private let make: @Sendable ([String: AnyJSON]) -> [String: AnyJSON]
    private(set) var upsertCalls: [[String: AnyJSON]] = []

    /// - representation: maps the uploaded row to the server's representation returned to the drain.
    init(representation: @escaping @Sendable ([String: AnyJSON]) -> [String: AnyJSON]) {
        self.make = representation
    }

    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        upsertCalls.append(row)
        return make(row)
    }
    func delete(table: String, primaryKey: String, pk: String) async throws {}
    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]] { [] }
}

/// A `SyncRemote` whose `upsert` announces it has started, then blocks on `gate` until the test
/// releases it — so a test can enqueue a mid-flight local edit while an upload is in flight, then
/// assert the server write-back doesn't clobber that pending edit (APPS-506). Returns the uploaded
/// row (with the server cursor stamped) as the representation once released.
struct UpsertGatedRemote: SyncRemote {
    let serverUpdatedAt = "2026-06-30T12:00:00.000Z"
    let started: Signal
    let gate: Signal

    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        await started.fire()
        await gate.wait()
        var server = row
        server["updatedAt"] = .string(serverUpdatedAt)
        return server
    }
    func delete(table: String, primaryKey: String, pk: String) async throws {}
    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]] { [] }
}

/// A `SyncDoorbell` test double. Each `ring(scope:)` starts a fresh subscription (a new stream),
/// recording the scope it was asked to filter on — so a test can prove the engine re-subscribes on
/// an auth change (APPS-509). `fire()` rings the current subscription; `fire(subscription:)` rings a
/// specific past one, to show a torn-down (old-uid) subscription no longer pokes the runner.
final class FakeDoorbell: SyncDoorbell, @unchecked Sendable {
    private struct State {
        var scopes: [String?] = []                              // scope of each ring(), in order
        var continuations: [AsyncStream<Void>.Continuation] = [] // parallel to `scopes`
        var liveCount = 0                                        // subscriptions not yet torn down
    }
    private let state = OSAllocatedUnfairLock(initialState: State())

    /// The scope of every subscription the engine has opened, in order — `[nil, "u1"]` proves a
    /// signed-out→signed-in re-scope.
    var ringScopes: [String?] { state.withLock { $0.scopes } }
    /// Subscriptions currently live (ring() opened, not yet cancelled) — settles to 1 after a
    /// re-scope, proving the previous channel was torn down.
    var liveSubscriptions: Int { state.withLock { $0.liveCount } }

    func ring(scope: String?) -> AsyncStream<Void> {
        AsyncStream { continuation in
            state.withLock {
                $0.scopes.append(scope)
                $0.continuations.append(continuation)
                $0.liveCount += 1
            }
            continuation.onTermination = { [state] _ in state.withLock { $0.liveCount -= 1 } }
        }
    }

    /// Rings the current (latest) subscription — a Realtime change for the signed-in user.
    func fire() { state.withLock { $0.continuations.last }?.yield(()) }

    /// Rings a specific past subscription by index — to show an old-uid event no longer pokes.
    func fire(subscription index: Int) {
        state.withLock { index < $0.continuations.count ? $0.continuations[index] : nil }?.yield(())
    }
}

/// Engine over a caller-supplied DB (and, by default, a no-op `FakeRemote`) so tests can
/// pre-create domain tables and only wire a dataset/failure remote when they exercise sync.
func makeEngine(
    db: any DatabaseWriter,
    tables: [SyncTable],
    remote: any SyncRemote = FakeRemote(),
    scope: @escaping @Sendable () async -> String? = { nil }
) throws -> SyncEngine {
    try SyncEngine(db: db, remote: remote, tables: tables, scope: scope)
}

/// Pushes every outbox entry's `last_attempt_at` into the past so the per-entry backoff window no
/// longer skips it — lets a test drive a retry deterministically without sleeping (APPS-470).
func agePastBackoff(_ db: any DatabaseWriter) async throws {
    try await db.write { db in
        try db.execute(sql: "UPDATE _sync_outbox SET last_attempt_at = ?", arguments: [Date.distantPast])
    }
}

/// A DB with one `recipes` domain table (id, title, updated_at) plus HappySync's internal tables.
func recipesDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("updatedAt", .text)
        }
    }
    return db
}
