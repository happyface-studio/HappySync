import Testing
import Foundation
import GRDB
import Supabase
import HappySync
import HappySyncTestSupport

// Deliberately **no `@testable` import**: this file may only touch what a consumer of the package
// gets. It is the regression guard for issue #53 — if the public seams stop being enough to test a
// sync integration offline, this file stops compiling, rather than the gap surfacing months later in
// an app that gave up and shipped sync untested.
//
// The three scenarios below are the ones the issue named as impossible to write before:
// does a cascade delete queue tombstones for the children, does the status UI show degraded when an
// upload parks, and has the engine actually quiesced by the time sign-out wipes the database.

/// CookThis's recipe graph in miniature, with the `ON DELETE CASCADE` that makes a parent delete
/// reach each child's own capture trigger.
private func recipeGraph() throws -> DatabaseQueue {
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
    }
    return db
}

private let graphTables = [
    SyncTable(name: "recipes"),
    SyncTable(name: "recipeIngredients"),
    SyncTable(name: "recipeSteps"),
]

@Test func deletingAParentQueuesTombstonesForEveryChild() async throws {
    let db = try recipeGraph()
    // Seeded before the engine exists, so its triggers capture only the delete below.
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')")
    }

    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: graphTables, pollInterval: 999, debounceInterval: 0.02
    )
    await engine.start()

    // One plain GRDB delete. SQLite cascades to the children, and each child's capture trigger
    // queues its own tombstone — the app never enumerates them.
    try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = 'r1'") }

    #expect(await eventually { await remote.deleteCalls.count == 3 })
    let calls = await remote.deleteCalls
    let tombstoned = Set(calls.map { "\($0.table)/\($0.pk)" })
    #expect(tombstoned == ["recipes/r1", "recipeIngredients/i1", "recipeSteps/s1"])
    await engine.stop()
}

@Test func statusGoesDegradedWhenAnUploadParks() async throws {
    let db = try recipeGraph()
    // Every upload is rejected the way an RLS policy rejects one: permanently, so it parks on the
    // first attempt instead of retrying.
    let remote = InMemorySyncRemote(failUpserts: 99, upsertFailure: .permanent)
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: graphTables, pollInterval: 999, debounceInterval: 0.02
    )
    await engine.start()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')") }

    // `.degraded` is the case a status UI gets wrong: the pass completed, so nothing looks broken,
    // yet the user's write never reached the server. Ask `isHealthy`, not the phase (issue #51).
    #expect(await eventually { await latestStatus(engine)?.phase == .degraded })
    #expect(await latestStatus(engine)?.isHealthy == false)

    let parked = try await engine.deadLetters()
    #expect(parked.count == 1)
    #expect(parked.first?.table == "recipes")
    #expect(parked.first?.pk == "r1")
    await engine.stop()
}

@Test func stopQuiescesTheEngineBeforeSignOutWipes() async throws {
    let db = try recipeGraph()
    let remote = InMemorySyncRemote()
    let doorbell = ManualDoorbell()
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: graphTables,
        doorbell: doorbell, pollInterval: 999, debounceInterval: 0.02
    )
    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 1 }) // start()'s immediate pass ran

    // A ring drives a pull with no stop/start — the doorbell is the only wake source here.
    let beforeRing = await remote.fetchCalls
    doorbell.fire()
    #expect(await eventually { await remote.fetchCalls > beforeRing })

    await engine.stop() // the sign-out step an app must await before wiping the database

    // After stop() returns nothing may still be in flight: a late pull would write into the store
    // the app is about to delete, and mid-account-switch would upload the previous user's rows.
    let atStop = await remote.fetchCalls
    doorbell.fire() // a Realtime event racing the sign-out must not restart anything
    try await Task.sleep(for: .milliseconds(200))
    #expect(await remote.fetchCalls == atStop)
}

/// The engine's latest status snapshot. Every `status` access is an independent stream that replays
/// the most recent value, so this reads it without holding a subscription open.
private func latestStatus(_ engine: SyncEngine) async -> SyncStatus? {
    var iterator = engine.status.makeAsyncIterator()
    return await iterator.next()
}
