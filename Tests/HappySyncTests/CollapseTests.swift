import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-472: same-(table, pk) outbox entries collapse to their net effect at drain, so a
// delete-then-recreate within one drain window uploads the row once (alive) instead of
// upsert-then-soft-delete, which would tombstone the re-created row on every device.

private func obEntry(seq: Int64, table: String, pk: String, op: SyncOp) -> OutboxEntry {
    OutboxEntry(row: ["seq": seq, "table_name": table, "pk": pk, "op": op.rawValue, "attempts": 0])
}

@Test func collapseSelectsNetOpPerKeyInSeqOrder() {
    let entries = [
        obEntry(seq: 1, table: "recipes", pk: "r1", op: .delete),
        obEntry(seq: 2, table: "recipes", pk: "r1", op: .upsert), // net for r1 = upsert (last op)
        obEntry(seq: 3, table: "recipes", pk: "r2", op: .upsert),
        obEntry(seq: 4, table: "recipes", pk: "r2", op: .delete), // net for r2 = delete (last op)
    ]
    let byPk = Dictionary(uniqueKeysWithValues: collapseOutbox(entries).map { ($0.net.pk, $0) })

    #expect(byPk.count == 2)
    #expect(byPk["r1"]?.net.op == .upsert)
    #expect(byPk["r1"]?.seqs.sorted() == [1, 2]) // both entries clear together
    #expect(byPk["r2"]?.net.op == .delete)
    #expect(byPk["r2"]?.seqs.sorted() == [3, 4])
}

/// A `recipes` DB with a `deletedAt` column, so the collapsed upsert can prove it un-tombstones.
private func tombstoneableRecipesDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("updatedAt", .text)
            t.column("deletedAt", .text)
        }
    }
    return db
}

@Test func deleteThenRecreateSamePkUploadsOnceAlive() async throws {
    let db = try tombstoneableRecipesDB()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Original')") }
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    // Offline: delete r1, then re-create it under the same pk.
    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Recreated"])

    try await engine.drainOutbox()

    // The delete is collapsed away — only the upsert reaches the server, un-tombstoned.
    #expect(await remote.deleteCalls.isEmpty)
    let upserts = await remote.upsertCalls
    #expect(upserts.count == 1)
    #expect(upserts.first?.row["title"] == .string("Recreated"))
    #expect(upserts.first?.row["deletedAt"] == .null) // explicitly clears the tombstone server-side

    let (pending, title) = try await db.read { db -> (Int, String?) in
        (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1,
         try String.fetchOne(db, sql: "SELECT title FROM recipes WHERE id='r1'"))
    }
    #expect(pending == 0)       // both collapsed entries cleared
    #expect(title == "Recreated") // the re-creation survives locally
}

@Test func recreateThenDeleteSamePkCollapsesToDelete() async throws {
    let db = try recipesDB()
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'X')") }
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Edited"])
    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])

    try await engine.drainOutbox()

    #expect(await remote.upsertCalls.isEmpty)    // the upsert is collapsed away
    #expect(await remote.deleteCalls.count == 1) // net effect is the delete
    let pending = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") }
    #expect(pending == 0)
}

@Test func twoUpsertsSamePkCollapseToOneFreshPayload() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "First"])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Second"])

    try await engine.drainOutbox()

    let upserts = await remote.upsertCalls
    #expect(upserts.count == 1)                          // N edits to one row → one upload, not N
    #expect(upserts.first?.row["title"] == .string("Second")) // fresh payload = latest local state
}
