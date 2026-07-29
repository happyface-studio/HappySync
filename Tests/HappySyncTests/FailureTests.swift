import Testing
import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
@testable import HappySync

// Issue #52: the engine classifies remote failures precisely, then used to stringify all of it at the
// public boundary — leaving the consumer substring-matching PostgREST prose to tell an RLS reject from
// a dropped connection. The classification must reach `Phase.failed` and `DeadLetter.failure` intact,
// and survive the round-trip through `_sync_outbox` (a dead letter is rebuilt from the DB, long after
// the live `Error` is gone).
//
// Issue #51: `.idle` used to mean only "no pass is running", which reads as health and isn't. A settled
// pass with outstanding writes is `.degraded`, and `isHealthy` is the one way to ask.

private func httpError(_ code: Int) -> HTTPError {
    HTTPError(
        data: Data(),
        response: HTTPURLResponse(url: URL(string: "https://x")!, statusCode: code, httpVersion: nil, headerFields: nil)!
    )
}

private func pg(_ code: String?, _ message: String = "") -> PostgrestError {
    PostgrestError(code: code, message: message)
}

// MARK: - Classification (pure)

@Test func classifiesHTTPStatuses() {
    // Auth-shaped: the app's response is re-auth, not repair — matching the retry exemption (APPS-502).
    #expect(SyncFailure.classify(httpError(401)) == .authExpired)
    #expect(SyncFailure.classify(httpError(403)) == .authExpired)
    // Everything else keeps its status rather than collapsing into a generic "offline".
    #expect(SyncFailure.classify(httpError(503)) == .server(status: 503))
    #expect(SyncFailure.classify(httpError(409)) == .server(status: 409))
    #expect(SyncFailure.classify(httpError(429)) == .server(status: 429))
}

@Test func classifiesURLErrorAsNetwork() {
    #expect(SyncFailure.classify(URLError(.notConnectedToInternet)) == .network)
    #expect(SyncFailure.classify(URLError(.timedOut)) == .network)
}

@Test func classifiesPostgrestByCode() {
    // Integrity violations keep their SQLSTATE, so an app can tell a duplicate from an FK violation.
    #expect(SyncFailure.classify(pg("23505")) == .constraintViolation(code: "23505"))
    #expect(SyncFailure.classify(pg("23503")) == .constraintViolation(code: "23503"))
    #expect(SyncFailure.classify(pg("42501")) == .permissionDenied) // RLS rejected the write
    #expect(SyncFailure.classify(pg("PGRST301")) == .authExpired)   // JWT expired
    #expect(SyncFailure.classify(pg("PGRST302")) == .authExpired)   // JWT missing
    #expect(SyncFailure.classify(pg("08006")) == .network)          // connection_failure
    // Transient Postgres states get no case of their own — they clear on retry and the engine already
    // retries them — so they keep their text under `.other` rather than being mislabelled.
    #expect(SyncFailure.classify(pg("40001", "serialization_failure")) == .other("serialization_failure"))
    #expect(SyncFailure.classify(pg(nil, "no code")) == .other("no code"))
}

@Test func schemaMismatchNamesTheColumnWhenTheServerDoes() {
    // Postgres phrasing (double quotes) and PostgREST's schema-cache phrasing (single quotes).
    #expect(SyncFailure.classify(pg("42703", #"column "nutrition" does not exist"#))
            == .schemaMismatch(column: "nutrition"))
    #expect(SyncFailure.classify(pg("PGRST204", "Could not find the 'nutrition' column of 'recipes' in the schema cache"))
            == .schemaMismatch(column: "nutrition"))
    // A message that names nothing still classifies — the column is typed optional for exactly this.
    #expect(SyncFailure.classify(pg("42703", "undefined column")) == .schemaMismatch(column: nil))
}

@Test func classifiesThroughTheRemoteFailureWrapper() {
    // Production wraps every transport error in RemoteFailure; a wrapped error must classify identically.
    #expect(SyncFailure.classify(RemoteFailure(isPermanent: false, underlying: httpError(401))) == .authExpired)
    #expect(SyncFailure.classify(RemoteFailure(isPermanent: true, underlying: pg("42501"))) == .permissionDenied)
}

@Test func unrecognisedErrorsKeepTheirText() {
    struct Weird: Error, CustomStringConvertible { var description: String { "something odd" } }
    #expect(SyncFailure.classify(Weird()) == .other("something odd")) // never lose the breadcrumb
}

@Test func pgrst204IsRetriedEvenThoughItClassifiesAsSchemaMismatch() async throws {
    let db = try recipesDB()
    // A deliberate divergence between the kind and the retry decision: PostgREST raises PGRST204 from
    // a *stale schema cache* as well as from a genuinely missing column, and the stale case clears on
    // its own — so the engine retries rather than parking on the first attempt, while the kind still
    // tells the app what to show if it does eventually park.
    let error = pg("PGRST204", "Could not find the 'nutrition' column of 'recipes' in the schema cache")
    #expect(SyncFailure.classify(error) == .schemaMismatch(column: "nutrition"))
    #expect(remoteErrorIsPermanent(error) == false) // …but not permanent — retried, not parked

    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(error))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 8)
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    #expect(try await engine.deadLetters().isEmpty) // still retrying, not parked
}

@Test func everyKindHasADisplayableDescription() {
    // A consumer that had `case .failed(let reason): showBanner(reason)` against the old String payload
    // still has something to render — the classified cases carry no server prose of their own.
    let rendered: [String] = [
        SyncFailure.network, .authExpired, .permissionDenied,
        .constraintViolation(code: "23505"), .schemaMismatch(column: "nutrition"),
        .schemaMismatch(column: nil), .server(status: 500),
    ].map(\.description)
    #expect(rendered.allSatisfy { !$0.isEmpty })
    #expect(Set(rendered).count == rendered.count)      // each kind reads distinctly
    #expect(SyncFailure.other("raw text").description == "raw text") // `.other` shows the real thing
}

// MARK: - Persistence round-trip (pure)

@Test func everyFailureKindRoundTripsThroughItsWireValue() {
    let cases: [SyncFailure] = [
        .network, .authExpired, .permissionDenied,
        .constraintViolation(code: "23505"),
        .schemaMismatch(column: "nutrition"),
        .schemaMismatch(column: nil),
        .server(status: 503),
    ]
    for failure in cases {
        #expect(SyncFailure.decode(wire: failure.wireValue, message: nil) == failure)
    }
    // `.other` stores only its kind and rehydrates its text from `last_error`, so the message isn't
    // written to the row twice.
    #expect(SyncFailure.decode(wire: SyncFailure.other("boom").wireValue, message: "boom") == .other("boom"))
}

@Test func unknownOrAbsentWireValuesDegradeToOther() {
    // An entry parked before happysync_v5 (NULL), and a row written by a newer HappySync than this
    // binary — neither may fail the read.
    #expect(SyncFailure.decode(wire: nil, message: "legacy text") == .other("legacy text"))
    #expect(SyncFailure.decode(wire: "quantumFlux:7", message: "from the future") == .other("from the future"))
    #expect(SyncFailure.decode(wire: "server:notAnInt", message: "malformed") == .other("malformed"))
}

// MARK: - Dead letters carry the classification

@Test func deadLetterCarriesTheClassifiedCause() async throws {
    let db = try recipesDB()
    // RLS rejected the write: the one case an app most needs to tell apart, and the one that used to
    // arrive as an unparseable String.
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(pg("42501", "new row violates row-level security policy")))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox() // permanent → parks immediately

    let letter = try #require(try await engine.deadLetters().first)
    #expect(letter.failure == .permissionDenied)
    #expect(letter.lastError != nil) // the raw text survives alongside the kind, not instead of it
}

@Test func deadLetterKeepsTheConstraintCodeAcrossTheRoundTrip() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(pg("23505", "duplicate key value violates unique constraint")))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox()

    // The payload survives the DB round-trip — the classification is stored, not re-derived from prose.
    #expect(try await engine.deadLetters().first?.failure == .constraintViolation(code: "23505"))
    let stored = try await db.read { try String.fetchOne($0, sql: "SELECT failure_kind FROM _sync_outbox WHERE pk='r1'") }
    #expect(stored == "constraint:23505")
}

@Test func everyEntryInAParkedGroupRecordsTheFailureKind() async throws {
    let db = try recipesDB()
    // Two edits to one row collapse to a single net op; when it parks, the whole group parks — and the
    // classification must land on every entry, not just the net one.
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(pg("42501", "rls")))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await write(db, "UPDATE recipes SET title = 'Stew' WHERE id = 'r1'")

    try await engine.drainOutbox()

    let letters = try await engine.deadLetters()
    #expect(letters.count == 2)
    #expect(letters.allSatisfy { $0.failure == .permissionDenied })
}

@Test func aParkedEntryWithNoStoredKindStillReadsAsAFailure() async throws {
    let db = try recipesDB()
    let engine = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    // Parked by hand with no `failure_kind` — an entry from before happysync_v5, or one an operator
    // parked directly. It must degrade to `.other`, never fail the read.
    try await write(db, "UPDATE _sync_outbox SET dead_lettered = 1, last_error = 'legacy failure' WHERE pk='r1'")

    let letter = try #require(try await engine.deadLetters().first)
    #expect(letter.failure == .other("legacy failure"))
}

@Test func payloadKindsSurviveTheEngineRoundTripNotJustTheWireFormat() async throws {
    let db = try recipesDB()
    // `.server(status:)` end to end — classified from a live HTTPError, written to `failure_kind`,
    // read back through `deadLetters()`. The pure wire test can't catch a break in that path.
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(httpError(404)))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.drainOutbox() // 404 is permanent → parks at once

    #expect(try await engine.deadLetters().first?.failure == .server(status: 404))
    let stored = try await db.read { try String.fetchOne($0, sql: "SELECT failure_kind FROM _sync_outbox WHERE pk='r1'") }
    #expect(stored == "server:404")
}

@Test func retryClearsTheStoredFailureKind() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(pg("42501", "rls")))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await engine.drainOutbox() // parks with failure_kind = permissionDenied

    try await engine.retryDeadLetters() // the cause is fixed — the stale classification must go too

    let kind = try await db.read { try String.fetchOne($0, sql: "SELECT failure_kind FROM _sync_outbox WHERE pk='r1'") }
    #expect(kind == nil) // else a later, different failure would be read through the old kind
}

// MARK: - Status carries the classification

@Test func aFailedPassCarriesTheClassifiedCause() async throws {
    let db = try recipesDB()
    // The pull 401s — the pass itself fails, as opposed to an individual write failing.
    let remote = InMemorySyncRemote(failFetches: 1, fetchFailure: .error(httpError(401)))
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])

    await #expect(throws: (any Error).self) { try await engine.runSyncOnce() }

    var iter = engine.status.makeAsyncIterator()
    let failed = await iter.next()
    #expect(failed?.phase == .failed(.authExpired)) // "prompt re-auth", not an opaque String
    #expect(failed?.isHealthy == false)
}

// MARK: - Health (issue #51)

@Test func isHealthyRequiresCleanCountsNotJustIdle() {
    #expect(SyncStatus(phase: .idle).isHealthy)
    // The counts are still checked on a consumer-constructed status (a preview, a test fixture) — the
    // engine derives the phase from them, but nothing stops a caller pairing `.idle` with a backlog.
    #expect(SyncStatus(phase: .idle, failedUploads: 1).isHealthy == false)
    #expect(SyncStatus(phase: .idle, deadLetters: 1).isHealthy == false)
    #expect(SyncStatus(phase: .syncing).isHealthy == false)
    #expect(SyncStatus(phase: .degraded).isHealthy == false)
    #expect(SyncStatus(phase: .failed(.network)).isHealthy == false)
}

@Test func aCleanPassSettlesIdleAndHealthy() async throws {
    let db = try recipesDB()
    let engine = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")

    try await engine.runSyncOnce() // the write uploads cleanly

    var iter = engine.status.makeAsyncIterator()
    let settled = await iter.next()
    #expect(settled?.phase == .idle) // nothing outstanding → `.idle` genuinely means healthy
    #expect(settled?.isHealthy == true)
}

@Test func repairingTheLastDeadLetterReturnsTheStatusToIdle() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote(failUpserts: 1, upsertFailure: .permanent)
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await engine.runSyncOnce() // parks r1 → degraded

    var degraded = engine.status.makeAsyncIterator()
    #expect(await degraded.next()?.phase == .degraded)

    try await engine.discardDeadLetters() // the repair broadcast must settle the phase too, not just the count

    var repaired = engine.status.makeAsyncIterator()
    let settled = await repaired.next()
    #expect(settled?.phase == .idle)
    #expect(settled?.isHealthy == true)
}

@Test func repairingTheLastDeadLetterStaysDegradedWhileAnotherWriteIsFailing() async throws {
    let db = try recipesDB()
    // The repair broadcast used to assert `failedUploads: 0` instead of reading it, so clearing the
    // last parked entry settled `.idle` — a green checkmark over a write that was still failing —
    // until the next drain corrected it. That is precisely the misread this PR exists to remove.
    let remote = InMemorySyncRemote(failUpserts: 99) // every upload fails transiently
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: [SyncTable(name: "recipes")], deadLetterAfter: 8)
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r2', 'Stew')")

    try await engine.drainOutbox() // both fail transiently: attempts > 0, neither parked
    // Park only r1, leaving r2 failing-but-retrying — the two states the repair path has to tell apart.
    try await write(db, "UPDATE _sync_outbox SET dead_lettered = 1 WHERE pk = 'r1'")

    try await engine.discardDeadLetters() // clears the last dead letter; r2 is untouched

    var iter = engine.status.makeAsyncIterator()
    let settled = await iter.next()
    #expect(settled?.deadLetters == 0)     // the repair landed…
    #expect(settled?.failedUploads == 1)   // …but r2 is still failing, and must be re-read, not assumed
    #expect(settled?.phase == .degraded)
    #expect(settled?.isHealthy == false)
}
