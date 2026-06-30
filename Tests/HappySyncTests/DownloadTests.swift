import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// MARK: - Apply + tuple cursor

@Test func pullAppliesRemoteRowAndAdvancesTupleCursor() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Soup", "updatedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let (title, cursorUpdatedAt, cursorId) = try await db.read { db -> (String?, String?, String?) in
        let title = try String.fetchOne(db, sql: "SELECT title FROM recipes WHERE id = 'r1'")
        // _sync_state is HappySync's own control table — snake_case columns.
        let cu = try String.fetchOne(db, sql: "SELECT updated_at FROM _sync_state WHERE table_name = 'recipes'")
        let cid = try String.fetchOne(db, sql: "SELECT last_id FROM _sync_state WHERE table_name = 'recipes'")
        return (title, cu, cid)
    }
    #expect(title == "Soup")
    #expect(cursorUpdatedAt == "2026-06-30T10:00:00.000Z") // cursor advanced to the row's (updatedAt, id)
    #expect(cursorId == "r1")
}

@Test func pullCursorsOnPerTableColumn() async throws {
    // recipe_translations has no updatedAt — it's immutable and stamped translatedAt. The engine must
    // order, filter, apply, and advance the cursor using the table's declared cursorColumn.
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipe_translations") { t in
            t.column("id", .text).primaryKey()
            t.column("lang", .text)
            t.column("translatedAt", .text)
        }
    }
    let remote = FakeRemote(dataset: [
        "recipe_translations": [["id": "t1", "lang": "de", "translatedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(
        db: db,
        tables: [SyncTable(name: "recipe_translations", cursorColumn: "translatedAt")],
        remote: remote
    )

    try await engine.pullNow()

    let (lang, cursor) = try await db.read { db -> (String?, String?) in
        (try String.fetchOne(db, sql: "SELECT lang FROM recipe_translations WHERE id = 't1'"),
         try String.fetchOne(db, sql: "SELECT updated_at FROM _sync_state WHERE table_name = 'recipe_translations'"))
    }
    #expect(lang == "de")                            // row applied, keyed off translatedAt
    #expect(cursor == "2026-06-30T10:00:00.000Z")    // _sync_state stores the translatedAt cursor value
}

// MARK: - LWW & dirty-row protection

private func seedRecipe(_ db: DatabaseQueue, id: String, title: String, updatedAt: String) async throws {
    try await db.write { db in
        try db.execute(
            sql: "INSERT INTO recipes (id, title, updatedAt) VALUES (?, ?, ?)",
            arguments: [id, title, updatedAt]
        )
    }
}

@Test func pullSkipsRemoteRowOlderThanLocal() async throws {
    let db = try recipesDB()
    try await seedRecipe(db, id: "r1", title: "Local", updatedAt: "2026-06-30T11:00:00.000Z")
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Stale Remote", "updatedAt": "2026-06-30T10:00:00.000Z"]]
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
        "recipes": [["id": "r1", "title": "Fresh", "updatedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id = 'r1'") }
    #expect(title == "Fresh")
}

@Test func pullDoesNotClobberDirtyLocalRow() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Remote Wins?", "updatedAt": "2026-06-30T10:00:00.000Z"]]
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
            t.column("updatedAt", .text)
            t.column("deletedAt", .text)
        }
        try db.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'Doomed', '2026-06-30T09:00:00.000Z')")
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Doomed", "updatedAt": "2026-06-30T10:00:00.000Z", "deletedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let count = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes WHERE id = 'r1'") }
    #expect(count == 0) // tombstone (deletedAt set) removes the row locally
}

@Test func pullDeletesTombstonesChildrenBeforeParents() async throws {
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("updatedAt", .text)
            t.column("deletedAt", .text)
        }
        try db.create(table: "recipeIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes", onDelete: .restrict)
            t.column("updatedAt", .text)
            t.column("deletedAt", .text)
        }
        try db.execute(sql: "INSERT INTO recipes (id, updatedAt) VALUES ('r1', '2026-06-30T09:00:00.000Z')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId, updatedAt) VALUES ('i1', 'r1', '2026-06-30T09:00:00.000Z')")
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "updatedAt": "2026-06-30T10:00:00.000Z", "deletedAt": "2026-06-30T10:00:00.000Z"]],
        "recipeIngredients": [["id": "i1", "recipeId": "r1", "updatedAt": "2026-06-30T10:00:00.000Z", "deletedAt": "2026-06-30T10:00:00.000Z"]],
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
    // bare-timestamp cursor would drop r3. The (updatedAt, id) tuple cursor must keep it.
    let remote = FakeRemote(dataset: [
        "recipes": [
            ["id": "r1", "title": "A", "updatedAt": "2026-06-30T10:00:00.000Z"],
            ["id": "r2", "title": "B", "updatedAt": "2026-06-30T10:00:01.000Z"],
            ["id": "r3", "title": "C", "updatedAt": "2026-06-30T10:00:01.000Z"],
        ]
    ])
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], pageSize: 1)

    try await engine.pullNow()

    let (count, lastId) = try await db.read { db -> (Int, String?) in
        (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipes") ?? -1,
         try String.fetchOne(db, sql: "SELECT last_id FROM _sync_state WHERE table_name = 'recipes'"))
    }
    #expect(count == 3)     // every page applied, nothing dropped at the boundary
    #expect(lastId == "r3") // cursor ends on the final (updatedAt, id)
}
