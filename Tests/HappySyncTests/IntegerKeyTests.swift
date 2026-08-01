import Testing
import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
@testable import HappySync

// PostgREST serializes an int8 primary key as a JSON *number*, not a string. The upload path always
// handled that (`_sync_outbox.pk` has TEXT affinity, and batch matching reads integer keys through
// `wireKey`); these tests hold the download path to the same contract — LWW, tombstones, the cursor
// id, and the full resync's reconcile must all key off the integer's text form rather than silently
// treating every row as pk "".

/// A table keyed by an integer primary key, the way a Postgres `bigint identity` table is.
private func countersDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "counters") { t in
            t.column("id", .integer).primaryKey()
            t.column("value", .integer)
            t.column("updatedAt", .text)
            t.column("deletedAt", .text)
        }
    }
    return db
}

@Test func pullAppliesIntegerKeyedRowsAndAdvancesTheCursor() async throws {
    let db = try countersDB()
    let remote = InMemorySyncRemote(dataset: [
        "counters": [["id": .integer(7), "value": .integer(1), "updatedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "counters")], remote: remote)

    try await engine.pullNow()

    let (value, cursorId) = try await db.read { db -> (Int?, String?) in
        (try Int.fetchOne(db, sql: "SELECT value FROM counters WHERE id = 7"),
         try String.fetchOne(db, sql: "SELECT last_id FROM _sync_state WHERE table_name = 'counters'"))
    }
    #expect(value == 1)
    #expect(cursorId == "7") // the cursor id is the key's text form — the outbox's alphabet
}

@Test func integerKeyedTombstoneDeletesTheLocalRow() async throws {
    let db = try countersDB()
    try await db.write { try $0.execute(sql: "INSERT INTO counters (id, value, updatedAt) VALUES (7, 1, '2026-06-30T09:00:00.000Z')") }
    let remote = InMemorySyncRemote(dataset: [
        "counters": [["id": .integer(7), "value": .integer(1), "updatedAt": "2026-06-30T10:00:00.000Z", "deletedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "counters")], remote: remote)

    try await engine.pullNow()

    let count = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM counters") }
    #expect(count == 0) // the tombstone's DELETE bound the key's text form, which SQLite affinity-matches
}

@Test func integerKeyedDirtyRowIsNotClobbered() async throws {
    let db = try countersDB()
    let remote = InMemorySyncRemote(dataset: [
        "counters": [["id": .integer(7), "value": .integer(99), "updatedAt": "2026-06-30T10:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "counters")], remote: remote)
    // A trigger-captured local write: the outbox stores pk '7' (TEXT affinity), and the pull's dirty
    // check must find that entry from the wire row's *integer* key.
    try await write(db, "INSERT INTO counters (id, value) VALUES (7, 1)")

    try await engine.pullNow()

    let value = try await db.read { try Int.fetchOne($0, sql: "SELECT value FROM counters WHERE id = 7") }
    #expect(value == 1) // the pending local write wins, exactly as with a string key
}

@Test func fullResyncReconcilesIntegerKeyedTables() async throws {
    let db = try countersDB()
    // Local: 7 (still on the server) and 8 (deleted server-side, tombstone purged → absent). Neither
    // is dirty. Before the wire-key fix the resync saw *nothing* as seen — which would have wiped 7
    // too — and the reconcile's local-pk read must CAST, or GRDB refuses to decode the integer
    // column as String at all.
    try await db.write { db in
        try db.execute(sql: "INSERT INTO counters (id, value, updatedAt) VALUES (7, 1, '2026-01-01T00:00:00.000Z')")
        try db.execute(sql: "INSERT INTO counters (id, value, updatedAt) VALUES (8, 1, '2026-01-01T00:00:00.000Z')")
    }
    let remote = InMemorySyncRemote(dataset: [
        "counters": [["id": .integer(7), "value": .integer(1), "updatedAt": "2026-01-01T00:00:00.000Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "counters")], remote: remote)

    try await engine.fullResync()

    let ids = try await db.read { try Int.fetchAll($0, sql: "SELECT id FROM counters ORDER BY id") }
    #expect(ids == [7]) // 8 (purged delete) dropped; 7 kept — not wiped wholesale
}
