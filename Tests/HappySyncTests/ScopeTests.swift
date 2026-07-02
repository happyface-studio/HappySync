import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-469: a scoped table filters downloads (and the Realtime doorbell) to the current user's
// partition instead of relying on RLS alone — which, for CookThis's `isPublic OR userId=uid`
// policy, would pull the entire public catalog to every device.

/// A `recipes` DB carrying the `userId` partition column a scoped pull filters on.
private func scopedRecipesDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("userId", .text)
            t.column("updatedAt", .text)
        }
    }
    return db
}

@Test func pullScopedTableAppliesPartitionFilter() async throws {
    let db = try scopedRecipesDB()
    // The remote holds two users' rows (as CookThis's shared `recipes` table does); the scoped pull
    // must apply only the signed-in user's row, never the other user's.
    let remote = FakeRemote(dataset: [
        "recipes": [
            ["id": "r1", "title": "Mine", "userId": "u1", "updatedAt": "2026-06-30T10:00:00.000Z"],
            ["id": "r2", "title": "Theirs", "userId": "u2", "updatedAt": "2026-06-30T10:00:01.000Z"],
        ]
    ])
    let engine = try makeEngine(
        db: db,
        tables: [SyncTable(name: "recipes", scopeColumn: "userId")],
        remote: remote,
        scope: { "u1" }
    )

    try await engine.pullNow()

    let ids = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(ids == ["r1"]) // only the current user's row; the other user's stays out
    let lastScope = await remote.lastScope
    #expect(lastScope == ScopeFilter(column: "userId", value: "u1")) // filter passed alongside the cursor
}

@Test func pullUnscopedTableAppliesEveryRow() async throws {
    // Sanity: a table with no scopeColumn behaves exactly as before — no filter, all rows applied.
    let db = try scopedRecipesDB()
    let remote = FakeRemote(dataset: [
        "recipes": [
            ["id": "r1", "title": "A", "userId": "u1", "updatedAt": "2026-06-30T10:00:00.000Z"],
            ["id": "r2", "title": "B", "userId": "u2", "updatedAt": "2026-06-30T10:00:01.000Z"],
        ]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote, scope: { "u1" })

    try await engine.pullNow()

    let ids = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(ids == ["r1", "r2"])
    let lastScope = await remote.lastScope
    #expect(lastScope == nil) // unscoped → no partition filter
}

@Test func doorbellChangeFilterEncodesScopedColumn() {
    // The Realtime doorbell subscribes with a PostgREST-style filter for scoped tables.
    #expect(SupabaseDoorbell.changeFilter(column: "userId", value: "u1") == "userId=eq.u1")
}
