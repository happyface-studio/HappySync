import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-470: upload failures must be visible, backed off per entry, and dead-lettered — not silently
// retried on every drain while the status reports a healthy idle.

// MARK: - Permanent vs transient classification (pure)

/// Builds an `HTTPError` carrying `code` so a classification test can exercise a status path.
private func httpError(_ code: Int) -> HTTPError {
    HTTPError(
        data: Data(),
        response: HTTPURLResponse(url: URL(string: "https://x")!, statusCode: code, httpVersion: nil, headerFields: nil)!
    )
}

@Test func remoteErrorClassifiesByHTTPStatus() {
    #expect(remoteErrorIsPermanent(httpError(409)) == true)   // conflict (unique constraint) → give up
    #expect(remoteErrorIsPermanent(httpError(422)) == true)   // validation → give up
    #expect(remoteErrorIsPermanent(httpError(404)) == true)   // not found → give up
    #expect(remoteErrorIsPermanent(httpError(401)) == false)  // auth may be mid-refresh → retry (APPS-502)
    #expect(remoteErrorIsPermanent(httpError(403)) == false)  // token refresh could be pending → retry (APPS-502)
    #expect(remoteErrorIsPermanent(httpError(429)) == false)  // throttled → retry
    #expect(remoteErrorIsPermanent(httpError(408)) == false)  // request timeout → retry
    #expect(remoteErrorIsPermanent(httpError(503)) == false)  // server down → retry
    #expect(remoteErrorIsPermanent(URLError(.notConnectedToInternet)) == false) // offline → retry
}

@Test func remoteErrorClassifiesPostgrestByCode() {
    func pg(_ code: String?) -> PostgrestError { PostgrestError(code: code, message: code ?? "no code") }
    // Permanent: the write is malformed or forbidden; no retry will ever land it.
    #expect(remoteErrorIsPermanent(pg("23505")) == true)  // unique_violation
    #expect(remoteErrorIsPermanent(pg("23503")) == true)  // foreign_key_violation
    #expect(remoteErrorIsPermanent(pg("42501")) == true)  // insufficient_privilege (RLS reject)
    #expect(remoteErrorIsPermanent(pg("42703")) == true)  // undefined_column
    // Transient Postgres states — retry rather than drop the write.
    #expect(remoteErrorIsPermanent(pg("40001")) == false) // serialization_failure
    #expect(remoteErrorIsPermanent(pg("40P01")) == false) // deadlock_detected
    #expect(remoteErrorIsPermanent(pg("53300")) == false) // too_many_connections
    #expect(remoteErrorIsPermanent(pg("08006")) == false) // connection_failure
    // Unknown / JWT / absent codes default to transient.
    #expect(remoteErrorIsPermanent(pg("PGRST301")) == false) // JWT expired — recovers on refresh
    #expect(remoteErrorIsPermanent(pg(nil)) == false)
}

@Test func remoteErrorRecognisesAuthShapedFailures() {
    func pg(_ code: String?) -> PostgrestError { PostgrestError(code: code, message: code ?? "") }
    // Auth-shaped (exempt from the retry budget)…
    #expect(remoteErrorIsAuthTransient(httpError(401)) == true)
    #expect(remoteErrorIsAuthTransient(httpError(403)) == true)
    #expect(remoteErrorIsAuthTransient(pg("PGRST301")) == true) // JWT expired
    #expect(remoteErrorIsAuthTransient(pg("PGRST302")) == true) // no JWT
    // …recognised even through the RemoteFailure wrapper the production remote raises.
    #expect(remoteErrorIsAuthTransient(RemoteFailure(isPermanent: false, underlying: httpError(401))) == true)
    // Not auth-shaped: a real transient (5xx) and a permanent constraint violation.
    #expect(remoteErrorIsAuthTransient(httpError(503)) == false)
    #expect(remoteErrorIsAuthTransient(pg("23505")) == false)
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

@Test func authFailureIsRetriedNotParked() async throws {
    let db = try recipesDB()
    // A single 401 (stale token) on the first attempt, then auth has recovered and the write lands.
    let remote = FakeRemote(failUpserts: 1, upsertError: httpError(401))
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 2)
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    let outcome = try await engine.drainOutbox() // 401 → transient auth failure, not a dead letter
    #expect(outcome.deadLettered == 0)
    #expect(outcome.failed == 1) // surfaced as a failing upload, not hidden…
    let (parked, attempts) = try await db.read {
        (try Int.fetchOne($0, sql: "SELECT dead_lettered FROM _sync_outbox WHERE pk='r1'"),
         try Int.fetchOne($0, sql: "SELECT attempts FROM _sync_outbox WHERE pk='r1'"))
    }
    #expect(parked == 0)
    #expect(attempts == 0) // …and the auth failure didn't charge the retry budget (APPS-502)

    try await agePastBackoff(db)
    try await engine.drainOutbox() // auth recovered → the write clears the outbox
    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE pk='r1'") }
    #expect(remaining == 0)
}

@Test func transientPostgrestStateIsRetried() async throws {
    let db = try recipesDB()
    // 40001 serialization_failure surfaces as a PostgrestError but is a transient Postgres state.
    let remote = FakeRemote(failUpserts: 1, upsertError: PostgrestError(code: "40001", message: "serialization_failure"))
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    let outcome = try await engine.drainOutbox()
    #expect(outcome.deadLettered == 0) // retried with backoff, not parked
    let parked = try await db.read { try Int.fetchOne($0, sql: "SELECT dead_lettered FROM _sync_outbox WHERE pk='r1'") }
    #expect(parked == 0)

    try await agePastBackoff(db)
    try await engine.drainOutbox() // Postgres recovered → the write lands
    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE pk='r1'") }
    #expect(remaining == 0)
}

@Test func constraintViolationPostgrestParksImmediately() async throws {
    let db = try recipesDB()
    // 23505 unique_violation is a PostgrestError the server will never accept as-is → dead-letter now.
    let remote = FakeRemote(failUpserts: 1, upsertError: PostgrestError(code: "23505", message: "unique_violation"))
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])

    let outcome = try await engine.drainOutbox()
    #expect(outcome.deadLettered == 1)
    let parked = try await db.read { try Int.fetchOne($0, sql: "SELECT dead_lettered FROM _sync_outbox WHERE pk='r1'") }
    #expect(parked == 1)
    #expect(await remote.upsertCalls.count == 1) // parked on the first attempt, no retry
}

@Test func fullOutbox401StormRecoversWithZeroDeadLetters() async throws {
    let db = try recipesDB()
    let entryCount = 5
    // Every upload in the first drain pass 401s (token stale at cold launch); afterwards auth refreshed.
    let remote = FakeRemote(failUpserts: entryCount, upsertError: httpError(401))
    // A deliberately tiny cap: proves auth failures don't park even when the budget is nearly spent.
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 2)
    for i in 0..<entryCount {
        try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r\(i)", "title": "T\(i)"])
    }

    try await engine.drainOutbox() // the entire outbox 401s in a single pass
    let deadAfterStorm = try await db.read {
        try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox WHERE dead_lettered = 1")
    }
    #expect(deadAfterStorm == 0) // no dead letters despite the whole outbox failing at once

    try await agePastBackoff(db)
    try await engine.drainOutbox() // auth returns a valid token → every write lands
    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") }
    #expect(remaining == 0)
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

@Test func deadLetterRefetchesRemoteVersionSkippedWhileDirty() async throws {
    let db = try recipesDB()
    // Device B's newer version sits on the server; Device A's poison local edit is older and parks
    // permanently. A pull while A's row is still dirty skips B's version but advances the cursor past
    // it — so after the entry dead-letters, the skipped version must still be re-fetched (APPS-505).
    let remote = FakeRemote(
        failUpserts: 1, permanentUpserts: true,
        dataset: ["recipes": [["id": "r1", "title": "Server Wins", "updatedAt": "2026-06-30T12:00:00.000Z"]]]
    )
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Poison", "updatedAt": "2026-06-30T09:00:00.000Z"])

    // 1. Pull while the row is dirty: the queued upload should win, so the server version is skipped —
    //    but the cursor advances past it all the same.
    try await engine.pullNow()
    #expect(try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") } == "Poison")

    // 2. The upload dead-letters; the row stops being dirty, and the skipped version is now behind the cursor.
    let outcome = try await engine.drainOutbox()
    #expect(outcome.deadLettered == 1)

    // 3. A subsequent pull must still converge on the server version. Without resetting the cursor on
    //    park, the fetch would resume past T2 and the row would stay diverged forever.
    try await engine.pullNow()
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
