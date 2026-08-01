import Testing
import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
@testable import HappySync

// The outbox drain sends consecutive same-table, same-op entries as one request, and falls back to
// one request per entry when the server rejects the batch (issue #54). These tests hold both halves
// down: the request count on the healthy path, and per-entry failure isolation on the unhealthy one.

// MARK: - Chunking

@Test func chunksSplitOnTableOpAndCap() {
    let entries = [
        entry(seq: 1, table: "recipes", pk: "r1", op: .upsert),
        entry(seq: 2, table: "recipes", pk: "r2", op: .upsert),
        entry(seq: 3, table: "recipes", pk: "r3", op: .upsert),
        entry(seq: 4, table: "recipeSteps", pk: "s1", op: .upsert), // table boundary
        entry(seq: 5, table: "recipeSteps", pk: "s2", op: .delete), // op boundary
    ]

    let shape = uploadChunks(entries, limit: 2).map { $0.map(\.pk) }
    #expect(shape == [["r1", "r2"], ["r3"], ["s1"], ["s2"]]) // cap splits the first run at two
}

@Test func chunkingNeverReordersToMakeABiggerBatch() {
    // Interleaved tables: batching only ever merges *adjacent* entries, so the FK-safe order
    // `orderForUpload` produced survives even when regrouping would give fewer requests.
    let entries = [
        entry(seq: 1, table: "recipes", pk: "r1", op: .upsert),
        entry(seq: 2, table: "recipeSteps", pk: "s1", op: .upsert),
        entry(seq: 3, table: "recipes", pk: "r2", op: .upsert),
    ]

    let shape = uploadChunks(entries, limit: 256).map { $0.map(\.pk) }
    #expect(shape == [["r1"], ["s1"], ["r2"]])
}

// MARK: - Healthy path

@Test func healthyBacklogUploadsInOneRequest() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    for i in 1...5 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    try await engine.drainOutbox()

    #expect(await remote.upsertRequests == 1)      // five rows, one round trip
    #expect(await remote.upsertCalls.count == 5)   // …carrying every row
    let (pending, stamped) = try await db.read { db -> (Int, Int) in
        (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1,
         try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipes WHERE updatedAt = ?", arguments: [remote.serverUpdatedAt]) ?? -1)
    }
    #expect(pending == 0)  // every entry cleared
    #expect(stamped == 5)  // and every row carries the server's representation (#29 preserved)
}

@Test func backlogIsSplitAtTheChunkCap() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")], uploadChunkSize: 2
    )
    for i in 1...5 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    try await engine.drainOutbox()

    #expect(await remote.upsertRequests == 3) // ⌈5/2⌉ — the cap bounds the request, not the pass
    #expect(await remote.upsertCalls.count == 5)
    #expect(try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") } == 0)
}

@Test func batchedUploadKeepsParentsBeforeChildren() async throws {
    let db = try recipeGraphDB()
    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: recipeGraphTables)
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await write(db, "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
    try await write(db, "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i2', 'r1')")

    try await engine.drainOutbox()

    // One request per table — the children batch together, but never ahead of their parent.
    #expect(await remote.upsertRequests == 2)
    #expect(await remote.upsertCalls.map(\.table) == ["recipes", "recipeIngredients", "recipeIngredients"])
}

@Test func deletesTombstoneInOneRequest() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    for i in 1...3 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }
    try await engine.drainOutbox() // upload them first, so the deletes below are the only pending ops
    try await write(db, "DELETE FROM recipes")

    try await engine.drainOutbox()

    #expect(await remote.deleteRequests == 1) // one `in.(…)` tombstone update for all three
    #expect(await remote.deleteCalls.map(\.pk).sorted() == ["r1", "r2", "r3"])
    #expect(try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") } == 0)
}

// MARK: - Failure isolation

@Test func poisonRowInABatchParksAloneAndItsNeighboursUpload() async throws {
    let db = try recipesDB()
    // The server rejects r3 — and, like PostgREST, rejects any array body containing it rather than
    // singling the row out. The drain has to find the culprit itself.
    let remote = PoisonRowRemote(poison: "r3")
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    for i in 1...4 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    let outcome = try await engine.drainOutbox()

    #expect(outcome.deadLettered == 1)
    #expect(await remote.uploaded.sorted() == ["r1", "r2", "r4"]) // the healthy neighbours still landed
    #expect(await remote.requests == 5)                            // the rejected batch, then one per row
    let parked = try await engine.deadLetters()
    #expect(parked.map(\.pk) == ["r3"])
    // Classified per row, exactly as an unbatched drain would — the batch failure told us nothing.
    #expect(parked.first?.failure == .constraintViolation(code: "23505"))
    let pending = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE dead_lettered = 0") }
    #expect(pending == 0) // nothing but the poison row is left in the outbox
}

@Test func batchFailureChargesNoAttemptsToHealthyEntries() async throws {
    let db = try recipesDB()
    // Every request fails transiently. The batch attempt must not charge the retry budget on its own
    // — otherwise one chunk-sized failure would spend two attempts per entry and park healthy writes
    // at half the configured cap.
    let remote = InMemorySyncRemote(failUpserts: 99)
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 3
    )
    for i in 1...3 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    let outcome = try await engine.drainOutbox()

    #expect(outcome.failed == 3)
    #expect(outcome.deadLettered == 0)
    let attempts = try await db.read { try Int.fetchAll($0, sql: "SELECT attempts FROM _sync_outbox ORDER BY seq") }
    #expect(attempts == [1, 1, 1]) // one attempt each — the failed batch itself charged nothing
}

@Test func connectivityBatchFailureFailsThePassInsteadOfHuntingRowByRow() async throws {
    let db = try recipesDB()
    // The batch dies because the device is offline — no row is at fault, so re-running it row by row
    // would only send three more doomed requests (against a timing-out network, hours of them). The
    // drain must fail the pass — the scheduler's backoff owns the retry — and charge no attempts, so
    // the batch re-runs whole once connectivity returns.
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(URLError(.notConnectedToInternet)))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    for i in 1...3 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    await #expect(throws: (any Error).self) { try await engine.drainOutbox() }

    #expect(await remote.upsertRequests == 1) // the dead batch, and nothing after it
    let attempts = try await db.read { try Int.fetchAll($0, sql: "SELECT attempts FROM _sync_outbox ORDER BY seq") }
    #expect(attempts == [0, 0, 0]) // no attempts charged — these writes are healthy

    try await engine.drainOutbox() // connectivity back → the whole backlog lands, still batched
    #expect(try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") } == 0)
}

@Test func authShapedBatchFailureFailsThePassInsteadOfHuntingRowByRow() async throws {
    let db = try recipesDB()
    // Same shape for an expired token: the credential failed, not a row. Falling back per row would
    // spend one doomed request per entry on every drain of a signed-out stretch (APPS-502).
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(httpError(401)))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    for i in 1...2 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    await #expect(throws: (any Error).self) { try await engine.drainOutbox() }

    #expect(await remote.upsertRequests == 1)
    let attempts = try await db.read { try Int.fetchAll($0, sql: "SELECT attempts FROM _sync_outbox ORDER BY seq") }
    #expect(attempts == [0, 0])
}

@Test func representationsAreMatchedByPrimaryKeyNotArrivalOrder() async throws {
    let db = try recipesDB()
    // A server that answers a batch with the representations reversed. Matching them positionally
    // would stamp each row with its neighbour's server state; matching by primary key can't.
    let remote = ReversingRemote()
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    for i in 1...3 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "T\(i)"])
    }

    try await engine.drainOutbox()

    let titles = try await db.read {
        try String.fetchAll($0, sql: "SELECT title FROM recipes ORDER BY id")
    }
    #expect(titles == ["T1 (server)", "T2 (server)", "T3 (server)"]) // each row got *its own* representation
}

// MARK: - Fixtures

/// Builds an `HTTPError` carrying `code`, so a batch-failure test can exercise the auth-shaped path.
private func httpError(_ code: Int) -> HTTPError {
    HTTPError(
        data: Data(),
        response: HTTPURLResponse(url: URL(string: "https://x")!, statusCode: code, httpVersion: nil, headerFields: nil)!
    )
}

private func entry(seq: Int64, table: String, pk: String, op: SyncOp) -> OutboxEntry {
    OutboxEntry(row: ["seq": seq, "table_name": table, "pk": pk, "op": op.rawValue, "attempts": 0])
}

/// A remote that rejects one primary key the way Postgres rejects a constraint violation — and
/// rejects any batch containing it, since PostgREST refuses the whole array body rather than the
/// offending row. Records what actually landed, and how many requests it took.
private actor PoisonRowRemote: SyncRemote {
    let poison: String
    private(set) var uploaded: [String] = []
    private(set) var requests = 0

    init(poison: String) { self.poison = poison }

    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        try send([row])[0]
    }

    func upsert(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]] {
        try send(rows)
    }

    private func send(_ rows: [[String: AnyJSON]]) throws -> [[String: AnyJSON]] {
        requests += 1
        guard !rows.contains(where: { $0["id"]?.stringValue == poison }) else {
            throw RemoteFailure(classifying: PostgrestError(code: "23505", message: "unique_violation"))
        }
        uploaded.append(contentsOf: rows.compactMap { $0["id"]?.stringValue })
        return rows.map { row in
            var server = row
            server["updatedAt"] = .string("2026-06-30T12:00:00.000Z")
            return server
        }
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {}

    func fetch(
        table: String, cursorColumn: String, since cursor: SyncCursor?,
        primaryKey: String, scope: ScopeFilter?, limit: Int
    ) async throws -> [[String: AnyJSON]] { [] }
}

/// A remote that answers a batch with its representations in reverse order, each tagged so a
/// mis-matched write-back is visible in the row it lands on.
private struct ReversingRemote: SyncRemote {
    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        Self.representation(of: row)
    }

    func upsert(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]] {
        Array(rows.map(Self.representation).reversed())
    }

    private static func representation(of row: [String: AnyJSON]) -> [String: AnyJSON] {
        var server = row
        server["title"] = .string("\(row["title"]?.stringValue ?? "") (server)")
        server["updatedAt"] = .string("2026-06-30T12:00:00.000Z")
        return server
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {}

    func fetch(
        table: String, cursorColumn: String, since cursor: SyncCursor?,
        primaryKey: String, scope: ScopeFilter?, limit: Int
    ) async throws -> [[String: AnyJSON]] { [] }
}
