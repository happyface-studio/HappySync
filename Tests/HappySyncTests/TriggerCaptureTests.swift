import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// Issue #48: SQLite triggers capture every local write into the outbox, so a plain GRDB write syncs.
// Before this, a write that didn't go through `enqueue` silently never uploaded — no assertion, no
// log, no status change, no dead letter. These tests pin the new contract: any write from any source
// is queued, downloaded state is *not*, and the queue commits atomically with the domain rows.

// MARK: - Capture

@Test func plainGRDBWriteQueuesAnUpload() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    // The write the old design silently dropped on the floor.
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')") }

    #expect(try await queuedOps(db) == ["recipes/r1:upsert"])
    try await engine.drainOutbox()
    #expect(await remote.upsertCalls.map(\.table) == ["recipes"])
    #expect(await remote.upsertCalls.first?.row["title"] == .string("Soup"))
}

@Test func partialUpdateQueuesAnUpload() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await engine.drainOutbox() // clear the insert so the update stands alone

    // A partial update — no whole-row Encodable in sight, which the old `enqueue` signature required.
    try await write(db, "UPDATE recipes SET title = 'Stew' WHERE id = 'r1'")

    #expect(try await queuedOps(db) == ["recipes/r1:upsert"])
    try await engine.drainOutbox()
    #expect(await remote.upsertCalls.last?.row["title"] == .string("Stew"))
}

@Test func expressionUpdateQueuesAnUpload() async throws {
    // `UPDATE … SET count = count + 1` never materialises a row client-side, so it was unreachable
    // through `enqueue`. The trigger reads the post-write row, so it needs nothing from the caller.
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "counters") { t in
            t.column("id", .text).primaryKey()
            t.column("cooked", .integer)
            t.column("updatedAt", .text)
        }
        try db.execute(sql: "INSERT INTO counters (id, cooked) VALUES ('c1', 4)")
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "counters")])

    try await write(db, "UPDATE counters SET cooked = cooked + 1 WHERE id = 'c1'")
    try await engine.drainOutbox()

    #expect(await remote.upsertCalls.first?.row["cooked"] == .integer(5))
}

@Test func plainDeleteQueuesATombstone() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await engine.drainOutbox()

    try await write(db, "DELETE FROM recipes WHERE id = 'r1'")

    #expect(try await queuedOps(db) == ["recipes/r1:delete"])
    try await engine.drainOutbox()
    #expect(await remote.deleteCalls.map(\.pk) == ["r1"])
}

@Test func batchStatementQueuesOneEntryPerRow() async throws {
    let db = try recipesDB()
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('a', 'A'), ('b', 'B'), ('c', 'C')")

    #expect(try await queuedOps(db) == ["recipes/a:upsert", "recipes/b:upsert", "recipes/c:upsert"])
}

@Test func primaryKeyChangeQueuesTombstoneForTheOldKey() async throws {
    // Re-keying a row leaves the server holding the old key. Nothing else would ever remove it, so
    // the UPDATE trigger queues a tombstone for OLD.pk alongside the upsert of NEW.pk.
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('old', 'Soup')")
    try await engine.drainOutbox()

    try await write(db, "UPDATE recipes SET id = 'new' WHERE id = 'old'")

    #expect(try await queuedOps(db) == ["recipes/new:upsert", "recipes/old:delete"])
    try await engine.drainOutbox()
    // FK ordering runs all upserts before all deletes, so the new row exists before the old one goes.
    #expect(await remote.upsertCalls.last?.row["id"] == .string("new"))
    #expect(await remote.deleteCalls.map(\.pk) == ["old"])
}

@Test func insertOrReplaceQueuesATombstoneForTheDisplacedRow() async throws {
    // REPLACE evicting a *different* row via a unique index is only visible to delete triggers when
    // `PRAGMA recursive_triggers` is on — which the engine enables and asserts at init. Without it the
    // server's copy of the displaced row would live forever with nothing to report it.
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("slug", .text).unique()
            t.column("updatedAt", .text)
        }
        try db.execute(sql: "INSERT INTO recipes (id, slug) VALUES ('r1', 'soup')")
    }
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])

    try await write(db, "INSERT OR REPLACE INTO recipes (id, slug) VALUES ('r2', 'soup')")

    #expect(Set(try await queuedOps(db)) == ["recipes/r1:delete", "recipes/r2:upsert"])
}

@Test func recursiveTriggersIsEnabledOnTheWriter() async throws {
    let db = try recipesDB()
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])

    let enabled = try await db.write { try Int.fetchOne($0, sql: "PRAGMA recursive_triggers") }
    #expect(enabled == 1)
}

// MARK: - Atomicity with the domain write

@Test func multiTableTransactionCommitsRowsAndOutboxTogether() async throws {
    // "Create a recipe + its ingredients" is one user intent. `enqueue` opened its own transaction per
    // call, so this was 4 transactions and a crash in the middle left a half-recipe *and* a half-batch.
    let db = try recipeGraphDB()
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: recipeGraphTables)

    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
        for id in ["i1", "i2", "i3"] {
            try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES (?, 'r1')", arguments: [id])
        }
    }

    #expect(try await queuedOps(db) == [
        "recipes/r1:upsert",
        "recipeIngredients/i1:upsert", "recipeIngredients/i2:upsert", "recipeIngredients/i3:upsert",
    ])
}

@Test func rolledBackTransactionLeavesNeitherRowsNorOutboxEntries() async throws {
    struct Boom: Error {}
    let db = try recipeGraphDB()
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: recipeGraphTables)

    await #expect(throws: Boom.self) {
        try await db.write { db in
            try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
            try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
            throw Boom() // crash at #2 of the batch
        }
    }

    let recipes = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes")! }
    #expect(recipes == 0)
    #expect(try await queuedOps(db).isEmpty) // the queue rolled back with the rows — never half-applied
}

// MARK: - Re-entrancy: downloaded state must not re-enqueue

@Test func downloadedRowsDoNotReEnqueueThemselves() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "From server", "updatedAt": "2026-06-30T12:00:00.000Z"]]
    ])
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await engine.pullNow()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "From server")     // the row landed…
    #expect(try await queuedOps(db).isEmpty) // …and did not queue itself straight back for upload
}

@Test func downloadedTombstonesDoNotReEnqueueThemselves() async throws {
    let db = try recipesDB()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1','Local','2026-01-01T00:00:00.000Z')") }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Gone", "updatedAt": "2026-06-30T12:00:00.000Z", "deletedAt": "2026-06-30T12:00:00.000Z"]]
    ])
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await engine.pullNow()

    let rows = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes")! }
    #expect(rows == 0)
    #expect(try await queuedOps(db).isEmpty)
}

@Test func serverWriteBackDoesNotReEnqueueTheRowItConfirms() async throws {
    // The drain writes the server's representation back locally. Capturing that write would queue the
    // very upload it just confirmed, and the outbox would never drain to empty.
    let db = try recipesDB()
    let remote = RepresentationRemote { row in
        var server = row
        server["title"] = .string("Normalized")
        server["updatedAt"] = .string("2026-06-30T12:00:00.000Z")
        return server
    }
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Normalized")
    #expect(try await queuedOps(db).isEmpty) // settled — one upload, not an endless loop
}

@Test func fullResyncReconcileDoesNotQueueDeletes() async throws {
    // Rows dropped because the server no longer has them are purged deletes, not user deletes —
    // queueing tombstones for them would push the reconcile back at the server.
    let db = try recipesDB()
    try await db.write { db in
        for id in ["r1", "r2"] {
            try db.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES (?, ?, '2026-01-01T00:00:00.000Z')", arguments: [id, id])
        }
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "r1", "updatedAt": "2026-01-01T00:00:00.000Z"]]
    ])
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await engine.fullResync()

    let ids = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(ids == ["r1"])
    #expect(try await queuedOps(db).isEmpty)
}

@Test func discardingADeadLetterDoesNotQueueADelete() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Poison')")
    try await engine.drainOutbox() // parks the entry

    try await engine.discardDeadLetters() // drops the entry and the local row

    #expect(try await queuedOps(db).isEmpty) // dropping the row is convergence, not a delete to upload
}

// MARK: - Manifest ↔ schema agreement

@Test func initThrowsWhenADeclaredTableIsMissingLocally() async throws {
    // A manifest that names a table the schema doesn't have used to sync nothing, silently, forever.
    let db = try recipesDB()
    #expect(throws: SyncError.self) {
        _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "ghost")])
    }
}

@Test func initThrowsWhenThePrimaryKeyColumnIsMissing() async throws {
    let db = try recipesDB()
    #expect(throws: SyncError.self) {
        _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes", primaryKey: "uuid")])
    }
}

@Test func triggersAreDroppedForTablesRemovedFromTheManifest() async throws {
    let db = try recipeGraphDB()
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: recipeGraphTables)
    // A later app build stops syncing recipeIngredients: its triggers must go, or it keeps queueing
    // uploads for a table the engine no longer knows about.
    _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await write(db, "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")

    #expect(try await queuedOps(db) == ["recipes/r1:upsert"])
}

@Test func reinstallingTriggersIsIdempotent() async throws {
    // Two engines over one database (a sign-out/sign-in rebuild) must not double-queue every write.
    let db = try recipesDB()
    let first = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])
    let second = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    #expect(try await queuedOps(db) == ["recipes/r1:upsert"])
    let triggerCount = try await db.read { db in
        try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'")!
    }
    #expect(triggerCount == 3) // insert/update/delete — one set, not two

    await first.stop()  // never started, so these are no-ops — they just keep both engines alive
    await second.stop() // (and both outbox observers attached) across the write above
}

// MARK: - Collapse & ordering still hold for trigger-queued entries

@Test func repeatedEditsCollapseToOneUpload() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'One')")
    try await write(db, "UPDATE recipes SET title = 'Two' WHERE id = 'r1'")
    try await write(db, "UPDATE recipes SET title = 'Three' WHERE id = 'r1'")
    #expect(try await queuedOps(db).count == 3) // three ops queued…

    try await engine.drainOutbox()

    #expect(await remote.upsertCalls.count == 1) // …netted to one upload of the final state
    #expect(await remote.upsertCalls.first?.row["title"] == .string("Three"))
    #expect(try await queuedOps(db).isEmpty)
}

@Test func triggerQueuedEntriesUploadParentsBeforeChildren() async throws {
    let db = try recipeGraphDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: recipeGraphTables)

    // One transaction, parent and child together — the drain still FK-orders the uploads.
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
        try db.execute(sql: "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1')")
    }
    try await engine.drainOutbox()

    let order = await remote.upsertCalls.map(\.table)
    func rank(_ t: String) -> Int { order.firstIndex(of: t)! }
    #expect(rank("recipes") < rank("recipeSteps"))
    #expect(rank("recipes") < rank("recipeIngredients"))
    #expect(rank("recipeSteps") < rank("recipeStepIngredients"))
    #expect(rank("recipeIngredients") < rank("recipeStepIngredients"))
}

@Test func queuedAtIsReadableAsADate() async throws {
    // SQLite stamps `queued_at` in the triggers; `DeadLetter.queuedAt` must still decode it.
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    let letter = try #require(try await engine.deadLetters().first)
    #expect(abs(letter.queuedAt.timeIntervalSinceNow) < 60) // parsed as a real instant, not 1970
}

// MARK: - A local write wakes the runner

@Test func aPlainWriteWakesTheRunnerWithoutSyncNow() async throws {
    // `enqueue` used to poke the runner itself. With no engine call on the write path, the engine
    // listens for outbox inserts instead — otherwise a direct write would sit until the next poll.
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        pollInterval: 3600,       // the periodic loop can't be what uploads this
        debounceInterval: 0.05
    )
    await engine.start()

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    #expect(await eventually { await remote.upsertCalls.count == 1 })
    await engine.stop()
}
