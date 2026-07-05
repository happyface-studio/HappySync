import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-471: a device offline past the server's tombstone-purge horizon never sees the tombstones
// for rows deleted while it was away, so it keeps them forever (and re-uploads dirty ones). On
// reconnect it must full-resync: clear cursors, re-pull everything, and drop local rows the server
// no longer has — except pending local writes.

private func markLastSynced(_ db: any DatabaseWriter, _ date: Date) async throws {
    try await db.write { db in
        try db.execute(
            sql: "INSERT INTO _sync_meta (key, value) VALUES ('last_synced_at', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            arguments: [SyncTimestamp.string(from: date)]
        )
    }
}

@Test func staleDeviceResyncDropsRowsPurgedFromServer() async throws {
    let db = try recipesDB()
    // Local: r1 (still on the server), r2 (deleted server-side long ago, tombstone purged → absent).
    try await db.write { db in
        for id in ["r1", "r2"] {
            try db.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES (?, ?, '2026-01-01T00:00:00.000Z')", arguments: [id, id])
        }
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "r1", "updatedAt": "2026-01-01T00:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)
    // r3 is a pending local-only create (dirty) — must NOT be reconciled away.
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r3", "title": "local only"])
    // This device last synced 100 days ago — well past the default 30-day offline gap.
    try await markLastSynced(db, Date(timeIntervalSinceNow: -100 * 24 * 3600))

    try await engine.runSyncOnce()

    let ids = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(ids == ["r1", "r3"]) // r2 (purged delete) dropped; r1 kept; r3 (dirty) kept
}

@Test func recentDeviceDoesNotReconcile() async throws {
    let db = try recipesDB()
    // A local row absent from the server, but the device synced moments ago → NOT stale, so no
    // reconcile. (If resync ran unconditionally, r2 would be wrongly deleted.)
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r2', 'keep', '2026-01-01T00:00:00.000Z')") }
    let remote = FakeRemote(dataset: ["recipes": []])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)
    try await markLastSynced(db, Date()) // synced just now

    try await engine.runSyncOnce()

    let count = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes WHERE id='r2'") }
    #expect(count == 1)
}

@Test func firstSyncWithoutPriorTimestampDoesNotReconcile() async throws {
    // No _sync_meta timestamp → the engine can't know the device is stale, so it must not treat a
    // fresh install's local-only rows as purged deletes.
    let db = try recipesDB()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r2', 'keep', '2026-01-01T00:00:00.000Z')") }
    let remote = FakeRemote(dataset: ["recipes": []])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.runSyncOnce()

    let count = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes WHERE id='r2'") }
    #expect(count == 1)
}

// APPS-501: a stale device whose scoped tables can't be pulled yet (`scope()` nil — cold launch
// before session restoration, or signed out) must NOT reconcile: the pull skips those tables, so
// reconciling would read "server holds zero rows" and wipe every non-dirty local row. The resync
// defers instead, and the stale check stays armed until a partition value resolves.

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

/// A mutable scope holder, so a test can start signed-out (nil) and "sign in" mid-test.
private actor ScopeBox {
    private var value: String?
    init(_ value: String?) { self.value = value }
    func get() -> String? { value }
    func set(_ newValue: String?) { value = newValue }
}

@Test func staleDeviceWithNilScopeDefersResyncThenReconcilesOnceScopeResolves() async throws {
    let db = try scopedRecipesDB()
    // Local: r1 (still on the server), r2 (deleted server-side, tombstone purged → absent).
    try await db.write { db in
        for id in ["r1", "r2"] {
            try db.execute(
                sql: "INSERT INTO recipes (id, title, userId, updatedAt) VALUES (?, ?, 'u1', '2026-01-01T00:00:00.000Z')",
                arguments: [id, id]
            )
        }
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "r1", "userId": "u1", "updatedAt": "2026-01-01T00:00:00.000Z"]]
    ])
    let scopeBox = ScopeBox(nil) // session not restored yet
    let engine = try makeEngine(
        db: db,
        tables: [SyncTable(name: "recipes", scopeColumn: "userId")],
        remote: remote,
        scope: { await scopeBox.get() }
    )
    let staleTimestamp = Date(timeIntervalSinceNow: -100 * 24 * 3600)
    try await markLastSynced(db, staleTimestamp)

    try await engine.runSyncOnce()

    // Zero deletions: the resync deferred rather than reconciling the skipped table against [].
    let idsAfterDeferred = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(idsAfterDeferred == ["r1", "r2"])
    // The stale check stays armed: last_synced_at must not advance while the resync is deferred,
    // or a relaunch (or the next in-process pass) would see a freshly-synced device and never resync.
    let storedWhileDeferred = try await db.read { try String.fetchOne($0, sql: "SELECT value FROM _sync_meta WHERE key='last_synced_at'") }
    #expect(storedWhileDeferred == SyncTimestamp.string(from: staleTimestamp))

    await scopeBox.set("u1") // session restored → partition value available
    try await engine.runSyncOnce()

    // The re-armed check now full-resyncs: r2 (purged delete) dropped, r1 kept.
    let idsAfterResync = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(idsAfterResync == ["r1"])
    // And the check has completed, so last_synced_at persists again.
    let storedAfterResync = try await db.read { try String.fetchOne($0, sql: "SELECT value FROM _sync_meta WHERE key='last_synced_at'") }
    #expect(storedAfterResync != SyncTimestamp.string(from: staleTimestamp))
}

@Test func staleResyncWithLegitimatelyEmptyServerStillReconciles() async throws {
    // A completed full fetch that returns zero rows is NOT the skipped case: the server genuinely
    // holds nothing, so non-dirty local rows are purged deletes and must still be dropped.
    let db = try recipesDB()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'stale', '2026-01-01T00:00:00.000Z')") }
    let remote = FakeRemote(dataset: ["recipes": []])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)
    try await markLastSynced(db, Date(timeIntervalSinceNow: -100 * 24 * 3600))

    try await engine.runSyncOnce()

    let count = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes") }
    #expect(count == 0)
}

@Test func successfulSyncPersistsLastSyncedAt() async throws {
    let db = try recipesDB()
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")])

    try await engine.runSyncOnce()

    let stored = try await db.read { try String.fetchOne($0, sql: "SELECT value FROM _sync_meta WHERE key='last_synced_at'") }
    #expect(stored != nil)
    #expect(SyncTimestamp.date(from: stored ?? "") != nil) // stored in canonical form
}
