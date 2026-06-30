import Foundation
import GRDB
import Supabase
@testable import HappySync

/// A `SyncRemote` test double: records upload calls, can simulate a run of upsert failures, and
/// serves downloads from an in-memory `dataset` honouring the `(updated_at, id)` tuple cursor and
/// page limit — so ordering, retry, LWW, tombstones, and pagination are all observable offline.
actor FakeRemote: SyncRemote {
    let serverUpdatedAt = "2026-06-30T12:00:00.000Z"
    private(set) var upsertCalls: [(table: String, row: [String: AnyJSON])] = []
    private(set) var deleteCalls: [(table: String, pk: String)] = []
    private var remainingFailures: Int
    private let dataset: [String: [[String: AnyJSON]]]

    init(failUpserts: Int = 0, dataset: [String: [[String: AnyJSON]]] = [:]) {
        remainingFailures = failUpserts
        self.dataset = dataset
    }

    enum Failure: Error { case simulated }

    func upsert(table: String, row: [String: AnyJSON]) async throws -> [String: AnyJSON] {
        upsertCalls.append((table, row))
        if remainingFailures > 0 { remainingFailures -= 1; throw Failure.simulated }
        var server = row
        server["updated_at"] = .string(serverUpdatedAt)
        return server
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {
        deleteCalls.append((table, pk))
    }

    func fetch(table: String, since cursor: SyncCursor?, primaryKey: String, limit: Int) async throws -> [[String: AnyJSON]] {
        let sorted = (dataset[table] ?? []).sorted { tuple($0, primaryKey) < tuple($1, primaryKey) }
        let filtered: [[String: AnyJSON]]
        if let cursor {
            filtered = sorted.filter { tuple($0, primaryKey) > (cursor.updatedAt, cursor.id) }
        } else {
            filtered = sorted
        }
        return Array(filtered.prefix(limit))
    }

    private func tuple(_ row: [String: AnyJSON], _ primaryKey: String) -> (String, String) {
        (row["updated_at"]?.stringValue ?? "", row[primaryKey]?.stringValue ?? "")
    }
}

/// Engine over a caller-supplied DB (and, by default, a no-op `FakeRemote`) so tests can
/// pre-create domain tables and only wire a dataset/failure remote when they exercise sync.
func makeEngine(db: any DatabaseWriter, tables: [SyncTable], remote: any SyncRemote = FakeRemote()) throws -> SyncEngine {
    try SyncEngine(db: db, remote: remote, tables: tables)
}

/// A DB with one `recipes` domain table (id, title, updated_at) plus HappySync's internal tables.
func recipesDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("updated_at", .text)
        }
    }
    return db
}
