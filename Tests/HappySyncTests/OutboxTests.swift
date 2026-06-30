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

// MARK: - Transactional enqueue

@Test func enqueueWritesDomainRowAndOutboxEntryAtomically() async throws {
    let db = try recipesDB()
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")])

    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

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

@Test func enqueueRollsBackOutboxWhenDomainWriteFails() async throws {
    // "ghost" is declared as a synced table but has no local table → the domain write fails,
    // so the outbox insert in the same transaction must roll back too.
    let db = try recipesDB()
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "ghost")])

    await #expect(throws: (any Error).self) {
        try await engine.enqueue(.upsert, table: "ghost", row: ["id": "g1"])
    }
    let outboxCount = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") }
    #expect(outboxCount == 0)
}

@Test func enqueueUnknownTableThrows() async throws {
    let engine = try makeEngine(db: try recipesDB(), tables: [SyncTable(name: "recipes")])
    await #expect(throws: SyncError.self) {
        try await engine.enqueue(.upsert, table: "notdeclared", row: ["id": "x"])
    }
}

@Test func enqueueMissingPrimaryKeyThrows() async throws {
    let engine = try makeEngine(db: try recipesDB(), tables: [SyncTable(name: "recipes")])
    await #expect(throws: SyncError.self) {
        try await engine.enqueue(.upsert, table: "recipes", row: ["title": "no id here"])
    }
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
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

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
    // Enqueue the CHILD first (lower seq); the drain must still upload the parent first.
    try await engine.enqueue(.upsert, table: "recipeIngredients", row: ["id": "i1", "recipeId": "r1"])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1"])

    try await engine.drainOutbox()

    let order = await remote.upsertCalls.map(\.table)
    #expect(order == ["recipes", "recipeIngredients"])
}

@Test func drainUploadsSameTableInSeqOrder() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "a", "title": "first"])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "b", "title": "second"])

    try await engine.drainOutbox()

    let ids = await remote.upsertCalls.map { $0.row["id"] }
    #expect(ids == [.string("a"), .string("b")])
}

// MARK: - Retry & backoff

@Test func failedUpsertIsKeptCountedAndRetriedIdempotently() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1) // first upsert throws, then succeeds
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

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
    #expect(backoffDelay(attempts: 1) == 2)
    #expect(backoffDelay(attempts: 2) == 4)
    #expect(backoffDelay(attempts: 3) == 8)
    #expect(backoffDelay(attempts: 10) == 64) // capped at 2^6
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
    try await engine.enqueue(
        .upsert,
        table: "userRecipeInteractions",
        row: ["id": "u1", "cookedCount": 5] as [String: AnyJSON]
    )

    try await engine.drainOutbox()

    let payload = try #require(await remote.upsertCalls.first?.row)
    #expect(payload["id"] == .string("u1"))
    #expect(payload.keys.contains("cookedCount") == false) // server owns it — never uploaded
}

@Test func drainPropagatesDeleteAndClearsEntry() async throws {
    let db = try recipesDB()
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])

    // enqueue removes the local row immediately; the tombstone propagates on drain.
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
