import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-508: the engine must let the consumer inspect, retry, and discard dead-lettered outbox
// entries — repair was previously only possible by hand-editing `_sync_outbox`, which the app never
// touches. Retry re-uploads once the cause is fixed; discard converges the row to the server's copy.

// MARK: - List

@Test func deadLettersListParkedEntriesWithDetail() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true) // parks on the first attempt
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox() // r1 parks permanently

    let letters = try await engine.deadLetters()
    #expect(letters.count == 1)
    let letter = try #require(letters.first)
    #expect(letter.table == "recipes")
    #expect(letter.pk == "r1")
    #expect(letter.op == .upsert)
    #expect(letter.attempts == 1)
    #expect(letter.lastError != nil) // the repair breadcrumb is surfaced, not just a count
}

@Test func deadLettersOmitEntriesStillRetrying() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1) // transient — retried, not parked
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 8)
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox() // fails transiently, stays pending (not parked)

    #expect(try await engine.deadLetters().isEmpty) // a failing-but-retrying entry is not a dead letter
}

// MARK: - Retry

@Test func retryDeadLettersReuploadsAfterCauseFixed() async throws {
    let db = try recipesDB()
    // Parks on the first attempt (permanent), then the remote accepts writes — the cause is "fixed".
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox() // parks r1
    #expect(try await engine.deadLetters().count == 1)

    try await engine.retryDeadLetters() // un-park all; resets attempts + backoff

    let (parked, attempts, lastAttempt): (Int?, Int?, Date?) = try await db.read {
        (try Int.fetchOne($0, sql: "SELECT dead_lettered FROM _sync_outbox WHERE pk='r1'"),
         try Int.fetchOne($0, sql: "SELECT attempts FROM _sync_outbox WHERE pk='r1'"),
         try Date.fetchOne($0, sql: "SELECT last_attempt_at FROM _sync_outbox WHERE pk='r1'"))
    }
    #expect(parked == 0)          // no longer dead-lettered
    #expect(attempts == 0)        // retry budget reset
    #expect(lastAttempt == nil)   // backoff window cleared → next drain attempts immediately

    try await engine.drainOutbox() // re-uploads; the remote now accepts it
    #expect(await remote.upsertCalls.count == 2) // the parked attempt + the successful retry
    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE pk='r1'") }
    #expect(remaining == 0) // cleared after the server confirmed the write
}

@Test func retryDeadLettersSelectsBySeq() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 99, permanentUpserts: true) // every write parks
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'A')")
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r2', 'B')")
    try await engine.drainOutbox() // both park

    let r1 = try #require(try await engine.deadLetters().first { $0.pk == "r1" })
    try await engine.retryDeadLetters([r1.seq]) // only r1

    let remaining = try await engine.deadLetters()
    #expect(remaining.count == 1)
    #expect(remaining.first?.pk == "r2") // r2 stays parked; only the named seq was un-parked
}

@Test func retryRefreshesStatusSoDeadLettersDropImmediately() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await engine.runSyncOnce() // parks r1; status shows one dead letter

    var beforeStatus = engine.status.makeAsyncIterator()
    #expect(await beforeStatus.next()?.deadLetters == 1)

    try await engine.retryDeadLetters() // must refresh the broadcast immediately
    var afterStatus = engine.status.makeAsyncIterator()
    #expect(await afterStatus.next()?.deadLetters == 0) // dropped without waiting for a sync pass
}

// MARK: - Discard

@Test func discardDeadLettersConvergesToServerVersion() async throws {
    let db = try recipesDB()
    // The parked local edit is *newer* than the server row, so plain LWW keeps the local version
    // even after the entry stops blocking downloads — the row is stuck diverged (APPS-505). Discard
    // must still converge it back to the server's copy.
    let remote = FakeRemote(
        failUpserts: 1, permanentUpserts: true,
        dataset: ["recipes": [["id": "r1", "title": "Server Wins", "updatedAt": "2026-06-30T12:00:00.000Z"]]]
    )
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'Poison', '2026-07-01T09:00:00.000Z')")

    try await engine.drainOutbox() // parks the poison entry
    try await engine.pullNow()     // advances the cursor past the server row, but LWW keeps "Poison"
    let beforeDiscard = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(beforeDiscard == "Poison") // confirm the divergence discard has to repair

    try await engine.discardDeadLetters() // drop the entry + local row, clear the cursor
    #expect(try await engine.deadLetters().isEmpty)

    try await engine.pullNow() // cleared cursor → full re-fetch → server row applies to the absent row
    let afterDiscard = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(afterDiscard == "Server Wins") // converged to the server's version
    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE pk='r1'") }
    #expect(remaining == 0)
}

@Test func discardDeadLettersReinstatesServerRowForAParkedDelete() async throws {
    let db = try recipesDB()
    // A local delete that could never propagate parks; the server still holds the row. Discarding the
    // delete means "accept the server's version" — the row must come back locally.
    let remote = FakeRemote(
        failUpserts: 0,
        dataset: ["recipes": [["id": "r1", "title": "Still Here", "updatedAt": "2026-06-30T12:00:00.000Z"]]]
    )
    // Seeded before the engine exists, so only the delete below is captured.
    try await db.write { try $0.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'Local', '2026-06-01T00:00:00.000Z')") }
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "DELETE FROM recipes WHERE id = 'r1'") // removes the local row + queues a delete
    // Park the queued delete by hand — the delete path can't be failed via FakeRemote, but the repair
    // API operates purely on `_sync_outbox` state regardless of *why* an entry parked.
    try await db.write { try $0.execute(sql: "UPDATE _sync_outbox SET dead_lettered = 1 WHERE pk='r1'") }

    #expect(try await engine.deadLetters().count == 1)
    let gone = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes WHERE id='r1'") }
    #expect(gone == 0) // deleted locally

    try await engine.discardDeadLetters()
    try await engine.pullNow() // re-pull reinstates the server's row

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Still Here") // server's version restored
}

@Test func discardKeepsRowWithAStillPendingWrite() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Parked')")
    try await engine.drainOutbox() // parks the first write

    // The user edits the same row again after it parked → a fresh, live (non-parked) outbox entry.
    try await write(db, "UPDATE recipes SET title = 'Fresh Edit' WHERE id = 'r1'")

    try await engine.discardDeadLetters() // discards only the parked entry

    // The live write and its local row must survive the discard.
    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Fresh Edit")
    let live = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE pk='r1' AND dead_lettered = 0") }
    #expect(live == 1) // the pending write is still queued
}

@Test func selectiveDiscardResetsRowDespiteAParkedSibling() async throws {
    let db = try recipesDB()
    // Two *parked* entries on the same row. Discarding one seq must still reset the local row toward
    // the server's version: the sibling that stays parked has stopped retrying, so — unlike a live
    // pending write — it isn't a reason to leave the row diverged (APPS-508 follow-up).
    let remote = FakeRemote(
        dataset: ["recipes": [["id": "r1", "title": "Server", "updatedAt": "2026-06-30T12:00:00.000Z"]]]
    )
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'First', '2026-07-01T09:00:00.000Z')")
    try await write(db, "UPDATE recipes SET title = 'Second', updatedAt = '2026-07-01T10:00:00.000Z' WHERE id = 'r1'")
    // Park both by hand — two dead-lettered siblings on one (table, pk).
    try await db.write { try $0.execute(sql: "UPDATE _sync_outbox SET dead_lettered = 1 WHERE pk='r1'") }

    let parked = try await engine.deadLetters().sorted { $0.seq < $1.seq }
    #expect(parked.count == 2)

    try await engine.discardDeadLetters([parked[0].seq]) // discard only the first parked entry

    // The other entry is still parked (a lingering sibling dead-letter)…
    let remainingParked = try await engine.deadLetters()
    #expect(remainingParked.count == 1)
    #expect(remainingParked.first?.seq == parked[1].seq)
    // …yet the local row was reset despite it, so a re-pull converges the row to the server's copy.
    try await engine.pullNow()
    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Server")
}
