import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-475: RowCoding.encode must map a Codable Date to a canonical ISO-8601 string, not the
// JSONEncoder default (`.deferredToDate` → a Double), which would break the §4 field mapping.

@Test func encodeMapsCodableDateToISO8601String() throws {
    struct Row: Encodable { let id: String; let createdAt: Date }
    let date = try #require(SyncTimestamp.date(from: "2026-07-02T10:00:00.123Z"))

    let columns = try RowCoding.encode(Row(id: "r1", createdAt: date), jsonColumns: [])

    #expect(columns["createdAt"] == "2026-07-02T10:00:00.123Z".databaseValue) // ISO text, not a Double
}

@Test func codableDateRoundTripsAsISOStringThroughUpload() async throws {
    struct NewRecipe: Encodable, Sendable { let id: String; let title: String; let updatedAt: Date }
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: [SyncTable(name: "recipes")])
    let date = try #require(SyncTimestamp.date(from: "2026-07-02T10:00:00.123Z"))

    try await engine.enqueue(.upsert, table: "recipes", row: NewRecipe(id: "r1", title: "Soup", updatedAt: date))

    let stored = try await db.read { try String.fetchOne($0, sql: "SELECT updatedAt FROM recipes WHERE id='r1'") }
    #expect(stored == "2026-07-02T10:00:00.123Z") // local column holds ISO text

    try await engine.drainOutbox()
    let payload = await remote.upsertCalls.first?.row
    #expect(payload?["updatedAt"] == .string("2026-07-02T10:00:00.123Z")) // uploaded as ISO text
}

// APPS-474: canonicalize the three timestamp shapes that coexist so the LWW lexicographic compare
// is format-stable.

@Test func canonicalizeCollapsesCrossFormatTimestamps() {
    // Same instant in the three shapes that coexist (contract §4) → one canonical string.
    let postgrest = "2026-07-02T10:00:00.123456+00:00" // PostgREST: microseconds, +00:00
    let client = "2026-07-02T10:00:00.123Z"            // client ISO8601: millis, Z
    #expect(SyncTimestamp.canonicalize(postgrest) == "2026-07-02T10:00:00.123Z")
    #expect(SyncTimestamp.canonicalize(client) == "2026-07-02T10:00:00.123Z")
    #expect(SyncTimestamp.canonicalize(postgrest) == SyncTimestamp.canonicalize(client))
}

@Test func canonicalizePreservesChronologicalOrderAcrossFormats() {
    // Non-fractional legacy vs fractional, different zones — canonical form must sort chronologically.
    let earlier = "2026-07-02T10:00:00Z"               // legacy, no fraction
    let later = "2026-07-02T10:00:00.001+00:00"         // 1ms later, PostgREST shape
    #expect(SyncTimestamp.canonicalize(earlier) < SyncTimestamp.canonicalize(later))

    let utc = "2026-07-02T10:00:00.500Z"
    let sameInstantOffset = "2026-07-02T12:00:00.500+02:00" // identical instant, +02:00
    #expect(SyncTimestamp.canonicalize(utc) == SyncTimestamp.canonicalize(sameInstantOffset))
}

@Test func canonicalizeLeavesUnparseableInput() {
    #expect(SyncTimestamp.canonicalize("not-a-timestamp") == "not-a-timestamp") // never drop a value
}

@Test func lwwAppliesFractionalRemoteOverNonFractionalLocalSameSecond() async throws {
    let db = try recipesDB()
    // Legacy local row stored WITHOUT fractional seconds; the remote is the same second + 1ms
    // (chronologically newer). Raw lexicographic compare puts "…00Z" after "…00.001Z" (`.` < `Z`),
    // so the newer remote would be wrongly rejected — canonicalization fixes the ordering.
    try await db.write {
        try $0.execute(sql: "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1','Local','2026-07-02T10:00:00Z')")
    }
    let remote = FakeRemote(dataset: [
        "recipes": [["id": "r1", "title": "Remote", "updatedAt": "2026-07-02T10:00:00.001Z"]]
    ])
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)

    try await engine.pullNow()

    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Remote") // compared chronologically, not lexically
}

// MARK: - Cursor or-filter escaping

@Test func cursorFilterQuotesReservedCharacters() {
    // A `+`-bearing timestamp and a pk with or-filter reserved chars (`,` `(` `)`) must be
    // double-quoted so they can't corrupt the PostgREST or= grammar.
    let cursor = SyncCursor(updatedAt: "2026-07-02T10:00:00.123456+00:00", id: "a,b(c)")
    let filter = SupabaseRemote.cursorFilter(cursorColumn: "updatedAt", primaryKey: "id", cursor: cursor)
    #expect(filter == #"updatedAt.gt."2026-07-02T10:00:00.123456+00:00",and(updatedAt.eq."2026-07-02T10:00:00.123456+00:00",id.gt."a,b(c)")"#)
}

@Test func cursorFilterEscapesEmbeddedQuotes() {
    let cursor = SyncCursor(updatedAt: "2026-07-02T10:00:00.000Z", id: #"a"b"#)
    let filter = SupabaseRemote.cursorFilter(cursorColumn: "updatedAt", primaryKey: "id", cursor: cursor)
    #expect(filter.contains(#"id.gt."a\"b""#)) // embedded " backslash-escaped inside the quoted value
}
