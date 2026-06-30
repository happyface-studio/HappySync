import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

private func makeEngine(tables: [SyncTable] = []) throws -> SyncEngine {
    try SyncEngine(
        db: try DatabaseQueue(), // in-memory
        supabase: SupabaseClient(
            supabaseURL: URL(string: "https://example.supabase.co")!,
            supabaseKey: "test-anon-key"
        ),
        tables: tables,
        auth: { "test-token" }
    )
}

@Test func emptyEngineMigratesAndStartsIdle() async throws {
    let db = try DatabaseQueue()
    let engine = try SyncEngine(
        db: db,
        supabase: SupabaseClient(
            supabaseURL: URL(string: "https://example.supabase.co")!,
            supabaseKey: "test-anon-key"
        ),
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

@Test func statusFansOutToMultipleSubscribers() async throws {
    let engine = try makeEngine()

    // Two independent subscribers each replay the initial idle snapshot — a bare AsyncStream
    // would starve the second consumer.
    var a = engine.status.makeAsyncIterator()
    var b = engine.status.makeAsyncIterator()
    #expect(await a.next()?.phase == .idle)
    #expect(await b.next()?.phase == .idle)
}

@Test func syncTableCarriesServerOwnedColumns() {
    let interactions = SyncTable(name: "userRecipeInteractions", serverOwnedColumns: ["cookedCount"])
    #expect(interactions.serverOwnedColumns == ["cookedCount"])
    #expect(SyncTable(name: "recipes").serverOwnedColumns.isEmpty)
}

@Test func pullNowIsStillStubbed() async throws {
    let engine = try makeEngine()

    // pullNow lands in APPS-414; until then it reports notImplemented.
    await #expect(throws: SyncError.self) {
        try await engine.pullNow()
    }
}
