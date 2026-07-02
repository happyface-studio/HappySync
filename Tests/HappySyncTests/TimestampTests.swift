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
