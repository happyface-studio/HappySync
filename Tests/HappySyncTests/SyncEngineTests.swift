import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

@Test func emptyEngineMigratesAndStartsIdle() async throws {
    let db = try DatabaseQueue() // in-memory
    let client = SupabaseClient(
        supabaseURL: URL(string: "https://example.supabase.co")!,
        supabaseKey: "test-anon-key"
    )

    let engine = try SyncEngine(
        db: db,
        supabase: client,
        tables: [SyncTable(name: "recipes", jsonColumns: ["nutrition"])],
        auth: { "test-token" }
    )

    // Internal tables exist and the outbox is empty.
    let (outboxCount, hasState) = try await db.read { db in
        let count = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        return (count, try db.tableExists("_sync_state"))
    }
    #expect(outboxCount == 0)
    #expect(hasState)

    // The status stream emits an initial idle status.
    var statuses = engine.status.makeAsyncIterator()
    let first = await statuses.next()
    #expect(first?.phase == .idle)
    #expect(first?.lastSyncedAt == nil)
}

@Test func stubbedSyncOpsThrowNotImplemented() async throws {
    let engine = try SyncEngine(
        db: try DatabaseQueue(),
        supabase: SupabaseClient(
            supabaseURL: URL(string: "https://example.supabase.co")!,
            supabaseKey: "test-anon-key"
        ),
        tables: [],
        auth: { "test-token" }
    )

    await #expect(throws: SyncError.self) {
        try await engine.enqueue(.upsert, table: "recipes", row: ["id": "1"])
    }
    await #expect(throws: SyncError.self) {
        try await engine.pullNow()
    }
}
