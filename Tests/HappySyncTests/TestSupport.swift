import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
@testable import HappySync

// The remote/doorbell fakes and the async helpers live in the shipped `HappySyncTestSupport`
// product, not here — our tests drive sync through exactly what a consumer gets, so the public
// seams can't quietly stop working (issue #53). What remains below is genuinely local: schema
// fixtures, and helpers that reach into HappySync's internal tables.

/// Accumulates every `SyncStatus` a subscriber receives, so a test can drive a `for await` loop the
/// way a SwiftUI `.task` does and then assert on what arrived — including *after* a stop/start cycle,
/// which a finished stream would never deliver (issue #45).
actor StatusCollector {
    private(set) var statuses: [SyncStatus] = []
    var count: Int { statuses.count }
    /// How many `.syncing` statuses arrived. One per sync pass, and `stop()` never broadcasts one — so
    /// a test can prove a *new* pass reached this subscriber rather than re-counting teardown's settle.
    var syncingCount: Int { statuses.filter { $0.phase == .syncing }.count }

    func append(_ status: SyncStatus) { statuses.append(status) }
}

/// Engine over a caller-supplied DB (and, by default, a no-op `InMemorySyncRemote`) so tests can
/// pre-create domain tables and only wire a dataset/failure remote when they exercise sync.
func makeEngine(
    db: any DatabaseWriter,
    tables: [SyncTable],
    remote: any SyncRemote = InMemorySyncRemote(),
    scope: @escaping @Sendable () async -> String? = { nil }
) throws -> SyncEngine {
    try SyncEngine.forTesting(db: db, remote: remote, tables: tables, scope: scope)
}

/// Returns `remote.fetchCalls` once it has stopped changing for `stableFor` — i.e. no sync pass is in
/// flight or pending — so a test can capture a baseline that a still-settling background pull won't
/// later perturb. Robust replacement for "sleep a bit, then read the count" before a *negative*
/// assertion (issue #19): the subscription-state conditions flip at the start of a pass, before its
/// pull lands, so reading the count right after them races the pull.
func settledFetchCalls(_ remote: InMemorySyncRemote, stableFor: Duration = .milliseconds(150), timeout: Duration = .seconds(5)) async -> Int {
    let deadline = ContinuousClock.now.advanced(by: timeout)
    var last = await remote.fetchCalls
    var lastChange = ContinuousClock.now
    while ContinuousClock.now < deadline {
        try? await Task.sleep(for: .milliseconds(10))
        let current = await remote.fetchCalls
        if current != last {
            last = current
            lastChange = ContinuousClock.now
        } else if lastChange.duration(to: ContinuousClock.now) >= stableFor {
            return current
        }
    }
    return await remote.fetchCalls
}

/// Pushes every outbox entry's `last_attempt_at` into the past so the per-entry backoff window no
/// longer skips it — lets a test drive a retry deterministically without sleeping (APPS-470).
func agePastBackoff(_ db: any DatabaseWriter) async throws {
    try await db.write { db in
        try db.execute(sql: "UPDATE _sync_outbox SET last_attempt_at = ?", arguments: [Date.distantPast])
    }
}

/// A plain GRDB write — **no engine call**. Since issue #48 the table's capture trigger records the
/// op in this same transaction, so this is how an app writes and how these tests drive the outbox.
func write(_ db: any DatabaseWriter, _ sql: String, _ arguments: StatementArguments = StatementArguments()) async throws {
    try await db.write { try $0.execute(sql: sql, arguments: arguments) }
}

/// Every pending outbox entry as `"table/pk:op"`, in `seq` order — the queue the capture triggers built.
func queuedOps(_ db: any DatabaseReader) async throws -> [String] {
    try await db.read { db in
        try Row.fetchAll(db, sql: "SELECT table_name, pk, op FROM \(SyncSchema.outboxTable) ORDER BY seq")
            .map { "\($0["table_name"] as String)/\($0["pk"] as String):\($0["op"] as String)" }
    }
}

/// A DB with one `recipes` domain table (id, title, nutrition, updatedAt) plus HappySync's internal
/// tables. `nutrition` is the JSON column, so a manifest declaring `jsonColumns: ["nutrition"]`
/// against this fixture names a column that exists — which init now checks (issue #49).
func recipesDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("nutrition", .text)
            t.column("updatedAt", .text)
        }
    }
    return db
}

/// CookThis's recipe graph with real SQLite FK constraints — `ON DELETE CASCADE` throughout, which is
/// what makes a parent delete reach the child tables' own capture triggers (issue #48). GRDB enforces
/// foreign keys by default, so this is the shape an app declares to get cascading tombstones.
func recipeGraphDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes", onDelete: .cascade)
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeSteps") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes", onDelete: .cascade)
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeStepIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("stepId", .text).references("recipeSteps", onDelete: .cascade)
            t.column("ingredientId", .text).references("recipeIngredients", onDelete: .cascade)
            t.column("updatedAt", .text)
        }
    }
    return db
}

/// The graph's manifest, declared child-first to prove ordering doesn't rely on input order.
let recipeGraphTables = [
    SyncTable(name: "recipeStepIngredients", dependsOn: ["recipeSteps", "recipeIngredients"]),
    SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]),
    SyncTable(name: "recipeSteps", dependsOn: ["recipes"]),
    SyncTable(name: "recipes"),
]
