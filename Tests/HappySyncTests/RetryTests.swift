import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-470: upload failures must be visible, backed off per entry, and dead-lettered — not silently
// retried on every drain while the status reports a healthy idle.

// MARK: - Permanent vs transient classification (pure)

@Test func remoteErrorClassifiesByHTTPStatus() {
    func http(_ code: Int) -> HTTPError {
        HTTPError(
            data: Data(),
            response: HTTPURLResponse(url: URL(string: "https://x")!, statusCode: code, httpVersion: nil, headerFields: nil)!
        )
    }
    #expect(remoteErrorIsPermanent(http(409)) == true)   // conflict (unique constraint) → give up
    #expect(remoteErrorIsPermanent(http(401)) == true)   // unauthorized / RLS reject → give up
    #expect(remoteErrorIsPermanent(http(422)) == true)   // validation → give up
    #expect(remoteErrorIsPermanent(http(429)) == false)  // throttled → retry
    #expect(remoteErrorIsPermanent(http(408)) == false)  // request timeout → retry
    #expect(remoteErrorIsPermanent(http(503)) == false)  // server down → retry
    #expect(remoteErrorIsPermanent(URLError(.notConnectedToInternet)) == false) // offline → retry
}

// MARK: - Per-entry backoff

@Test func drainSkipsEntryStillInsideBackoffWindow() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1) // first upsert fails (transient), then would succeed
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    try await engine.drainOutbox() // attempt 1 fails, last_attempt_at = now
    try await engine.drainOutbox() // immediately again — inside the 2s window → skipped, not retried

    #expect(await remote.upsertCalls.count == 1)
    let attempts = try await db.read { try Int.fetchOne($0, sql: "SELECT attempts FROM _sync_outbox WHERE pk='r1'") }
    #expect(attempts == 1) // no second attempt while backing off
}

// MARK: - Dead-letter

@Test func permanentFailureDeadLettersImmediately() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 1, permanentUpserts: true)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    let outcome = try await engine.drainOutbox()

    #expect(outcome.deadLettered == 1)
    let parked = try await db.read { try Int.fetchOne($0, sql: "SELECT dead_lettered FROM _sync_outbox WHERE pk='r1'") }
    #expect(parked == 1)

    // A parked entry is never retried, even once the backoff window elapses.
    try await agePastBackoff(db)
    try await engine.drainOutbox()
    #expect(await remote.upsertCalls.count == 1)
}

@Test func transientFailureDeadLettersAfterCap() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 99) // always fails (transient)
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 2)
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    try await engine.drainOutbox()          // attempt 1 — still retrying
    try await agePastBackoff(db)
    let outcome = try await engine.drainOutbox() // attempt 2 — hits the cap → parked

    #expect(outcome.deadLettered == 1)
    let parked = try await db.read { try Int.fetchOne($0, sql: "SELECT dead_lettered FROM _sync_outbox WHERE pk='r1'") }
    #expect(parked == 1)
}

@Test func deadLetteredEntryDoesNotBlockRemoteUpdate() async throws {
    let db = try recipesDB()
    // A poison local edit parks after failing permanently; a newer remote version of the same row
    // must still apply. If the LWW dirty gate counted the parked entry, the row would wedge forever.
    let remote = FakeRemote(
        failUpserts: 1, permanentUpserts: true,
        dataset: ["recipes": [["id": "r1", "title": "Server Wins", "updatedAt": "2026-06-30T12:00:00.000Z"]]]
    )
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Poison", "updatedAt": "2026-06-30T09:00:00.000Z"])

    try await engine.drainOutbox() // parks the poison entry
    try await engine.pullNow()     // newer remote row should now be free to apply

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Server Wins")
}

// MARK: - Status visibility

@Test func statusSurfacesFailedThenDeadLetteredUploads() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 99) // always fails transiently
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 2)
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    try await engine.runSyncOnce() // attempt 1 fails; pull succeeds
    var firstStatus = engine.status.makeAsyncIterator()
    let afterFail = await firstStatus.next() // replayed latest snapshot
    #expect(afterFail?.phase == .idle)     // pull succeeded, so not .failed…
    #expect(afterFail?.failedUploads == 1) // …but the failing upload is surfaced, not hidden as healthy
    #expect(afterFail?.deadLetters == 0)

    try await agePastBackoff(db)
    try await engine.runSyncOnce() // attempt 2 hits the cap → parked
    var secondStatus = engine.status.makeAsyncIterator()
    let afterPark = await secondStatus.next()
    #expect(afterPark?.failedUploads == 0) // no longer actively retrying…
    #expect(afterPark?.deadLetters == 1)   // …now surfaced as a dead-letter for the consumer to repair
}
