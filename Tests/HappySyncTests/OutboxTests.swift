import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// FakeRemote, makeEngine, and recipesDB live in TestSupport.swift.

// MARK: - FK topological ordering

@Test func topologicalOrderPlacesParentsBeforeChildren() {
    // CookThis's recipe graph, declared child-first to prove ordering doesn't rely on input order.
    let tables = [
        SyncTable(name: "recipeStepIngredients", dependsOn: ["recipeSteps", "recipeIngredients"]),
        SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]),
        SyncTable(name: "recipeSteps", dependsOn: ["recipes"]),
        SyncTable(name: "recipes"),
    ]
    let order = topologicalOrder(tables)
    func rank(_ name: String) -> Int { order.firstIndex(of: name)! }

    #expect(rank("recipes") < rank("recipeIngredients"))
    #expect(rank("recipes") < rank("recipeSteps"))
    #expect(rank("recipeSteps") < rank("recipeStepIngredients"))
    #expect(rank("recipeIngredients") < rank("recipeStepIngredients"))
}

// MARK: - Transactional capture

@Test func writeRecordsDomainRowAndOutboxEntryAtomically() async throws {
    let db = try recipesDB()
    _ = try makeEngine(db: db, tables: [SyncTable(name: "recipes")])

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    let (title, outbox) = try db.read { db -> (String?, Row?) in
        let title = try String.fetchOne(db, sql: "SELECT title FROM recipes WHERE id = 'r1'")
        let outbox = try Row.fetchOne(db, sql: "SELECT table_name, pk, op, attempts FROM _sync_outbox")
        return (title, outbox)
    }
    #expect(title == "Soup")
    #expect(outbox?["table_name"] == "recipes")
    #expect(outbox?["pk"] == "r1")
    #expect(outbox?["op"] == "upsert")
    #expect(outbox?["attempts"] == 0)
}

@Test func encodesScalarsBoolsAndJSONColumns() throws {
    struct Recipe: Encodable {
        let id: String
        let servings: Int
        let isFavorite: Bool
        let nutrition: [String: Int]
    }
    let columns = try RowCoding.encode(
        Recipe(id: "r1", servings: 4, isFavorite: true, nutrition: ["kcal": 500]),
        jsonColumns: ["nutrition"]
    )

    #expect(columns["id"] == "r1".databaseValue)
    #expect(columns["servings"] == 4.databaseValue)
    #expect(columns["isFavorite"] == 1.databaseValue) // Bool → 0/1

    // JSON column kept as JSON text, not a scalar.
    let json = try #require(String.fromDatabaseValue(columns["nutrition"]!))
    let parsed = try JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Int]
    #expect(parsed == ["kcal": 500])
}

// MARK: - Outbox drain

@Test func drainUploadsPendingUpsertStampsUpdatedAtAndClearsEntry() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    let upserts = await remote.upsertCalls
    #expect(upserts.count == 1)
    #expect(upserts.first?.table == "recipes")
    #expect(upserts.first?.row["id"] == .string("r1"))
    #expect(upserts.first?.row["title"] == .string("Soup"))

    let (outboxCount, stamped) = try await db.read { db -> (Int, String?) in
        let count = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        let stamped = try String.fetchOne(db, sql: "SELECT updatedAt FROM recipes WHERE id = 'r1'")
        return (count, stamped)
    }
    #expect(outboxCount == 0)              // entry cleared after server confirmed
    #expect(stamped == remote.serverUpdatedAt) // server updatedAt written back locally
}

@Test func drainUploadsParentsBeforeChildrenRegardlessOfEnqueueOrder() async throws {
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text)
            t.column("updatedAt", .text)
        }
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db,
        remote: remote,
        tables: [SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]), SyncTable(name: "recipes")]
    )
    // Write the CHILD first (lower seq); the drain must still upload the parent first.
    try await write(db, "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
    try await write(db, "INSERT INTO recipes (id) VALUES ('r1')")

    try await engine.drainOutbox()

    let order = await remote.upsertCalls.map(\.table)
    #expect(order == ["recipes", "recipeIngredients"])
}

@Test func drainUploadsSameTableInSeqOrder() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('a', 'first')")
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('b', 'second')")

    try await engine.drainOutbox()

    let ids = await remote.upsertCalls.map { $0.row["id"] }
    #expect(ids == [.string("a"), .string("b")])
}

// MARK: - Server representation write-back (APPS-506)

@Test func drainWritesBackServerNormalizedColumnValues() async throws {
    let db = try recipesDB()
    // The server normalizes `title` on write (a trigger/default the client can't compute) and stamps
    // the cursor. The whole representation must land locally — not just the cursor column.
    let remote = RepresentationRemote { row in
        var server = row
        server["title"] = .string("Normalized Soup")
        server["updatedAt"] = .string("2026-06-30T12:00:00.000Z")
        return server
    }
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    let (title, updatedAt, pending) = try await db.read { db -> (String?, String?, Int) in
        let title = try String.fetchOne(db, sql: "SELECT title FROM recipes WHERE id = 'r1'")
        let updatedAt = try String.fetchOne(db, sql: "SELECT updatedAt FROM recipes WHERE id = 'r1'")
        let pending = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        return (title, updatedAt, pending)
    }
    #expect(title == "Normalized Soup")               // server-normalized value reached the writing device
    #expect(updatedAt == "2026-06-30T12:00:00.000Z")  // cursor still stamped clean
    #expect(pending == 0)                             // entry cleared after server confirmed
}

@Test func drainDoesNotOverwriteAnEditEnqueuedWhileUploadInFlight() async throws {
    let db = try recipesDB()
    let started = Signal(), gate = Signal()
    let remote = UpsertGatedRemote(started: started, gate: gate)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    // Drive the drain concurrently; it uploads "Soup" then blocks in `upsert` awaiting the server.
    let drain = Task { try await engine.drainOutbox() }
    await started.wait()
    // The user edits the same row while that upload is in flight → a newer outbox entry for r1.
    try await write(db, "UPDATE recipes SET title = 'User edit' WHERE id = 'r1'")
    await gate.fire() // let the in-flight upload complete and write back
    try await drain.value

    let (title, pending) = try await db.read { db -> (String?, Int) in
        let title = try String.fetchOne(db, sql: "SELECT title FROM recipes WHERE id = 'r1'")
        let pending = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE pk = 'r1'") ?? -1
        return (title, pending)
    }
    #expect(title == "User edit") // mid-flight edit preserved — the stale write-back was skipped
    #expect(pending == 1)         // the newer edit is still queued; it uploads on the next drain
}

@Test func drainAdoptsMergedServerStateForConflictUpsert() async throws {
    // A conflictColumns upsert can merge onto an existing server row and return it re-keyed to the
    // client's pk with server-owned fields recomputed (APPS-478). The drain must adopt that merged
    // state locally rather than keep what this client sent (APPS-506).
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "userRecipeInteractions") { t in
            t.column("id", .text).primaryKey()
            t.column("userId", .text)
            t.column("recipeId", .text)
            t.column("cookedCount", .integer)
            t.column("updatedAt", .text)
        }
    }
    let remote = RepresentationRemote { row in
        var server = row
        server["cookedCount"] = .integer(7) // the merged server total, not the 1 this client sent
        server["updatedAt"] = .string("2026-06-30T12:00:00.000Z")
        return server
    }
    let engine = try SyncEngine(
        db: db,
        remote: remote,
        tables: [SyncTable(name: "userRecipeInteractions", conflictColumns: ["userId", "recipeId"])]
    )
    try await write(
        db,
        "INSERT INTO userRecipeInteractions (id, userId, recipeId, cookedCount) VALUES ('x1', 'u1', 'r1', 1)"
    )

    try await engine.drainOutbox()

    let (count, pending) = try await db.read { db -> (Int?, Int) in
        let count = try Int.fetchOne(db, sql: "SELECT cookedCount FROM userRecipeInteractions WHERE id = 'x1'")
        let pending = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        return (count, pending)
    }
    #expect(count == 7) // local row adopted the merged server value
    #expect(pending == 0)
}

// MARK: - Conflict target (secondary unique constraint)

@Test func drainForwardsConflictColumnsAsOnConflictTarget() async throws {
    // A table with a server-side secondary unique constraint (userId, recipeId) must upsert with
    // that constraint as the PostgREST conflict target — else a fresh-id insert 409s forever on a
    // device that hasn't yet pulled the existing server row (APPS-478).
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "userRecipeInteractions") { t in
            t.column("id", .text).primaryKey()
            t.column("userId", .text)
            t.column("recipeId", .text)
            t.column("updatedAt", .text)
        }
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db,
        remote: remote,
        tables: [SyncTable(name: "userRecipeInteractions", conflictColumns: ["userId", "recipeId"])]
    )
    try await write(db, "INSERT INTO userRecipeInteractions (id, userId, recipeId) VALUES ('x1', 'u1', 'r1')")

    try await engine.drainOutbox()

    #expect(await remote.upsertCalls.first?.onConflict == "userId,recipeId")
}

@Test func drainUsesPrimaryKeyConflictTargetByDefault() async throws {
    // No conflictColumns declared → the upsert conflicts on the primary key (onConflict nil), the
    // engine's default. Guards against every table paying the id-churn cost of a merge it doesn't need.
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    #expect(await remote.upsertCalls.first?.onConflict == nil)
}

// MARK: - Retry & backoff

@Test func failedUpsertIsKeptCountedAndRetriedIdempotently() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1) // first upsert throws, then succeeds
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    // First drain: upsert fails — entry stays, attempts bumped, row not yet marked clean.
    try await engine.drainOutbox()
    let afterFail = try await db.read { db -> (Int, Int, String?) in
        let pending = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        let attempts = try Int.fetchOne(db, sql: "SELECT attempts FROM _sync_outbox WHERE pk = 'r1'") ?? -1
        let stamped = try String.fetchOne(db, sql: "SELECT updatedAt FROM recipes WHERE id = 'r1'")
        return (pending, attempts, stamped)
    }
    #expect(afterFail.0 == 1)   // entry still pending
    #expect(afterFail.1 == 1)   // attempts counter incremented
    #expect(afterFail.2 == nil) // not stamped clean on failure

    // Second drain: succeeds idempotently — entry cleared, row stamped, retried exactly once.
    // (Age past the per-entry backoff window first, else the retry is intentionally skipped — APPS-470.)
    try await agePastBackoff(db)
    try await engine.drainOutbox()
    let afterSuccess = try await db.read { db -> (Int, String?) in
        let pending = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        let stamped = try String.fetchOne(db, sql: "SELECT updatedAt FROM recipes WHERE id = 'r1'")
        return (pending, stamped)
    }
    #expect(afterSuccess.0 == 0)
    #expect(afterSuccess.1 == remote.serverUpdatedAt)
    #expect(await remote.upsertCalls.count == 2)
}

@Test func backoffGrowsExponentiallyAndCaps() {
    // Base doubles each attempt and caps at 2^6; ±20% jitter (APPS-514) keeps each within
    // [0.8, 1.2] of its base.
    func withinJitter(_ delay: TimeInterval, base: Double) -> Bool { delay >= base * 0.8 && delay <= base * 1.2 }
    #expect(withinJitter(backoffDelay(attempts: 1), base: 2))
    #expect(withinJitter(backoffDelay(attempts: 2), base: 4))
    #expect(withinJitter(backoffDelay(attempts: 3), base: 8))
    #expect(withinJitter(backoffDelay(attempts: 10), base: 64)) // capped at 2^6
}

// MARK: - Server-owned columns & deletes

@Test func drainStripsServerOwnedColumnsFromUpsertPayload() async throws {
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "userRecipeInteractions") { t in
            t.column("id", .text).primaryKey()
            t.column("cookedCount", .integer)
            t.column("updatedAt", .text)
        }
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db,
        remote: remote,
        tables: [SyncTable(name: "userRecipeInteractions", serverOwnedColumns: ["cookedCount"])]
    )
    try await write(db, "INSERT INTO userRecipeInteractions (id, cookedCount) VALUES ('u1', 5)")

    try await engine.drainOutbox()

    let payload = try #require(await remote.upsertCalls.first?.row)
    #expect(payload["id"] == .string("u1"))
    #expect(payload.keys.contains("cookedCount") == false) // server owns it — never uploaded
}

// issue #47: the cursor column is server-stamped by contract §1/§4, so the engine strips it from
// every upload with nothing declared. Without this a table missing its `updatedAt` trigger silently
// promotes the client's clock to the LWW ordering authority.

@Test func drainNeverUploadsTheCursorColumn() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    // A row carrying an explicit client `updatedAt` — the shape the download path leaves behind too.
    try await write(db, "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'Soup', '2020-01-01T00:00:00.000Z')")

    try await engine.drainOutbox()

    let payload = try #require(await remote.upsertCalls.first?.row)
    #expect(payload["title"] == .string("Soup"))
    #expect(payload.keys.contains("updatedAt") == false) // the server stamps it — never uploaded
    // The local column keeps its value (it's the LWW/cursor comparand); only the wire drops it.
    let stored = try await db.read { try String.fetchOne($0, sql: "SELECT updatedAt FROM recipes WHERE id='r1'") }
    #expect(stored == remote.serverUpdatedAt) // and the write-back stamped the server's value over it
}

@Test func drainStripsACustomCursorColumn() async throws {
    // `recipe_translations` cursors on `translatedAt` (it has no `updatedAt`) — the stripping follows
    // the declared cursor column, not the name `updatedAt`.
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipe_translations") { t in
            t.column("id", .text).primaryKey()
            t.column("locale", .text)
            t.column("translatedAt", .text)
        }
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db,
        remote: remote,
        // serverColumns names it explicitly: even an allow-listed cursor column stays off the wire.
        tables: [SyncTable(
            name: "recipe_translations",
            cursorColumn: "translatedAt",
            serverColumns: ["id", "locale", "translatedAt"]
        )]
    )
    try await write(
        db,
        "INSERT INTO recipe_translations (id, locale, translatedAt) VALUES ('t1', 'de', '2020-01-01T00:00:00.000Z')"
    )

    try await engine.drainOutbox()

    let payload = try #require(await remote.upsertCalls.first?.row)
    #expect(payload["locale"] == .string("de"))
    #expect(payload.keys.contains("translatedAt") == false)
}

@Test func drainExcludesColumnsTheServerDoesNotKnow() async throws {
    // The server has since dropped `legacyNotes`; a shipped client still has it locally. Declaring
    // `serverColumns` intersects the payload so the upsert doesn't PGRST204 and dead-letter every
    // write on the table (APPS-504).
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("legacyNotes", .text) // a column the server no longer has
            t.column("updatedAt", .text)
        }
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db,
        remote: remote,
        tables: [SyncTable(name: "recipes", serverColumns: ["id", "title", "updatedAt"])]
    )
    try await write(db, "INSERT INTO recipes (id, title, legacyNotes) VALUES ('r1', 'Soup', 'old')")

    try await engine.drainOutbox()

    let payload = try #require(await remote.upsertCalls.first?.row)
    #expect(payload["id"] == .string("r1"))
    #expect(payload["title"] == .string("Soup"))
    #expect(payload.keys.contains("legacyNotes") == false) // server dropped it — excluded from upload
}

@Test func drainPropagatesDeleteAndClearsEntry() async throws {
    let db = try recipesDB()
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await write(db, "DELETE FROM recipes WHERE id = 'r1'")

    // The row is gone locally the moment the write commits; the tombstone propagates on drain.
    let localCount = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes WHERE id = 'r1'") }
    #expect(localCount == 0)

    try await engine.drainOutbox()

    let deletes = await remote.deleteCalls
    #expect(deletes.count == 1)
    #expect(deletes.first?.table == "recipes")
    #expect(deletes.first?.pk == "r1")
    let pending = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") }
    #expect(pending == 0)
}
