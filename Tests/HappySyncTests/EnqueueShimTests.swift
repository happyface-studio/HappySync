import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// Issue #48 deprecated `enqueue` in favour of plain GRDB writes, but keeps it working for one release
// so consumers can migrate without a flag day. These tests pin the shim's remaining behaviour: it
// performs the write, the table's capture trigger records the op exactly once, and the argument
// validation callers rely on still throws.
//
// Everything here is marked deprecated too — Swift doesn't warn when deprecated API is called from
// deprecated code, so the shim stays covered without spraying warnings across the suite.

@available(*, deprecated)
private func shimEnqueue(
    _ engine: SyncEngine, _ op: SyncOp, table: String, row: some Encodable & Sendable
) async throws {
    try await engine.enqueue(op, table: table, row: row)
}

@available(*, deprecated)
@Test func enqueueUpsertWritesTheRowAndIsCapturedOnce() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    try await shimEnqueue(engine, .upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Soup")
    // Exactly one entry: the shim no longer inserts its own, so it can't double up with the trigger's.
    #expect(try await queuedOps(db) == ["recipes/r1:upsert"])

    try await engine.drainOutbox()
    #expect(await remote.upsertCalls.first?.row["title"] == .string("Soup"))
}

@available(*, deprecated)
@Test func enqueueDeleteRemovesTheRowAndQueuesATombstone() async throws {
    let db = try recipesDB()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')") }
    let engine = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])

    try await shimEnqueue(engine, .delete, table: "recipes", row: ["id": "r1"])

    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes")! }
    #expect(remaining == 0)
    #expect(try await queuedOps(db) == ["recipes/r1:delete"])
}

@available(*, deprecated)
@Test func enqueueEncodesCodableDatesAsCanonicalISO8601() async throws {
    // APPS-475: a `Date` property must encode as ISO-8601 text, not JSONEncoder's default Double.
    struct NewRecipe: Encodable, Sendable { let id: String; let title: String; let updatedAt: Date }
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    let date = try #require(SyncTimestamp.date(from: "2026-07-02T10:00:00.123Z"))

    try await shimEnqueue(engine, .upsert, table: "recipes", row: NewRecipe(id: "r1", title: "Soup", updatedAt: date))

    // The local column is what witnesses the encoding: `updatedAt` is the cursor column, and issue #47
    // strips that from every payload — including one the shim queued — so the wire can't show it.
    let stored = try await db.read { try String.fetchOne($0, sql: "SELECT updatedAt FROM recipes WHERE id='r1'") }
    #expect(stored == "2026-07-02T10:00:00.123Z")

    try await engine.drainOutbox()
    let payload = try #require(await remote.upsertCalls.first?.row)
    #expect(payload["title"] == .string("Soup"))
    #expect(payload.keys.contains("updatedAt") == false) // server-stamped; never uploaded (issue #47)
}

@available(*, deprecated)
@Test func enqueueUnknownTableThrows() async throws {
    let engine = try SyncEngine(db: try recipesDB(), remote: FakeRemote(), tables: [SyncTable(name: "recipes")])
    await #expect(throws: SyncError.self) {
        try await shimEnqueue(engine, .upsert, table: "notdeclared", row: ["id": "x"])
    }
}

@available(*, deprecated)
@Test func enqueueMissingPrimaryKeyThrows() async throws {
    let db = try recipesDB()
    let engine = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes")])
    await #expect(throws: SyncError.self) {
        try await shimEnqueue(engine, .upsert, table: "recipes", row: ["title": "no id here"])
    }
    #expect(try await queuedOps(db).isEmpty) // nothing written, so nothing captured
}
