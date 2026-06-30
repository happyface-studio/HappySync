import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// MARK: - Apply + tuple cursor

@Test func pullAppliesRemoteRowAndAdvancesTupleCursor() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Soup", "updated_at": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let (title, cursorUpdatedAt, cursorId) = try await db.read { db -> (String?, String?, String?) in
        let title = try String.fetchOne(db, sql: "SELECT title FROM recipes WHERE id = 'r1'")
        let cu = try String.fetchOne(db, sql: "SELECT updated_at FROM _sync_state WHERE table_name = 'recipes'")
        let cid = try String.fetchOne(db, sql: "SELECT last_id FROM _sync_state WHERE table_name = 'recipes'")
        return (title, cu, cid)
    }
    #expect(title == "Soup")
    #expect(cursorUpdatedAt == "2026-06-30T10:00:00.000Z") // cursor advanced to the row's (updated_at, id)
    #expect(cursorId == "r1")
}

// MARK: - LWW & dirty-row protection

private func seedRecipe(_ db: DatabaseQueue, id: String, title: String, updatedAt: String) async throws {
    try await db.write { db in
        try db.execute(
            sql: "INSERT INTO recipes (id, title, updated_at) VALUES (?, ?, ?)",
            arguments: [id, title, updatedAt]
        )
    }
}

@Test func pullSkipsRemoteRowOlderThanLocal() async throws {
    let db = try recipesDB()
    try await seedRecipe(db, id: "r1", title: "Local", updatedAt: "2026-06-30T11:00:00.000Z")
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Stale Remote", "updated_at": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id = 'r1'") }
    #expect(title == "Local") // older remote must not clobber newer local
}

@Test func pullAppliesRemoteRowNewerThanLocal() async throws {
    let db = try recipesDB()
    try await seedRecipe(db, id: "r1", title: "Old", updatedAt: "2026-06-30T09:00:00.000Z")
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Fresh", "updated_at": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id = 'r1'") }
    #expect(title == "Fresh")
}

@Test func pullDoesNotClobberDirtyLocalRow() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Remote Wins?", "updated_at": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)
    // A pending local edit makes the row dirty; its queued upload must win.
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "My Local Edit"])

    try await engine.pullNow()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id = 'r1'") }
    #expect(title == "My Local Edit") // dirty row protected from the incoming remote
}

// MARK: - Tombstones

@Test func pullDeletesTombstonedRow() async throws {
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("updated_at", .text)
            t.column("deleted_at", .text)
        }
        try db.execute(sql: "INSERT INTO recipes (id, title, updated_at) VALUES ('r1', 'Doomed', '2026-06-30T09:00:00.000Z')")
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Doomed", "updated_at": "2026-06-30T10:00:00.000Z", "deleted_at": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let count = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes WHERE id = 'r1'") }
    #expect(count == 0) // tombstone (deleted_at set) removes the row locally
}

@Test func pullDeletesTombstonesChildrenBeforeParents() async throws {
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("updated_at", .text)
            t.column("deleted_at", .text)
        }
        try db.create(table: "recipeIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes", onDelete: .restrict)
            t.column("updated_at", .text)
            t.column("deleted_at", .text)
        }
        try db.execute(sql: "INSERT INTO recipes (id, updated_at) VALUES ('r1', '2026-06-30T09:00:00.000Z')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId, updated_at) VALUES ('i1', 'r1', '2026-06-30T09:00:00.000Z')")
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "updated_at": "2026-06-30T10:00:00.000Z", "deleted_at": "2026-06-30T10:00:00.000Z"]],
        "recipeIngredients": [["id": "i1", "recipeId": "r1", "updated_at": "2026-06-30T10:00:00.000Z", "deleted_at": "2026-06-30T10:00:00.000Z"]],
    ])
    let engine = try makeEngine(db: db, tables: [
        SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]),
        SyncTable(name: "recipes"),
    ], remote: remote)

    // Deleting the parent before the child would violate the FK; pull must order deletes child-first.
    try await engine.pullNow()

    let (recipes, ingredients) = try await db.read { db -> (Int, Int) in
        (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipes") ?? -1,
         try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipeIngredients") ?? -1)
    }
    #expect(recipes == 0)
    #expect(ingredients == 0)
}

// MARK: - Pagination & tuple boundary

@Test func pullPaginatesAcrossSameMillisecondBoundary() async throws {
    let db = try recipesDB()
    // r2 and r3 share a millisecond; with page size 1 the page boundary falls between them, so a
    // bare-timestamp cursor would drop r3. The (updated_at, id) tuple cursor must keep it.
    let remote = FakeRemote(dataset: [
        "recipes": [
            ["id": "r1", "title": "A", "updated_at": "2026-06-30T10:00:00.000Z"],
            ["id": "r2", "title": "B", "updated_at": "2026-06-30T10:00:01.000Z"],
            ["id": "r3", "title": "C", "updated_at": "2026-06-30T10:00:01.000Z"],
        ]
    ])
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], pageSize: 1)

    try await engine.pullNow()

    let (count, lastId) = try await db.read { db -> (Int, String?) in
        (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipes") ?? -1,
         try String.fetchOne(db, sql: "SELECT last_id FROM _sync_state WHERE table_name = 'recipes'"))
    }
    #expect(count == 3)     // every page applied, nothing dropped at the boundary
    #expect(lastId == "r3") // cursor ends on the final (updated_at, id)
}
