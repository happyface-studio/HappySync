import Testing
import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
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
    // The manifest and the schema must agree — a declared table gets capture triggers at init, so it
    // has to exist first (issue #48).
    let db = try recipesDB()
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
    let (outboxCount, hasState, hasControl) = try await db.read { db in
        let count = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _sync_outbox") ?? -1
        return (count, try db.tableExists("_sync_state"), try db.tableExists("_sync_control"))
    }
    #expect(outboxCount == 0)
    #expect(hasState)
    #expect(hasControl)

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

@Test func uploadExclusionsAlwaysCoverTheCursorColumn() {
    // The declared list stays "extra columns the server owns"; the cursor column joins it only at the
    // payload boundary, so a consumer can't opt out of the §4 rule by passing a list (issue #47).
    let interactions = SyncTable(name: "userRecipeInteractions", serverOwnedColumns: ["cookedCount"])
    #expect(interactions.uploadExcludedColumns == Set(["cookedCount", "updatedAt"]))
    #expect(SyncTable(name: "recipes").uploadExcludedColumns == Set(["updatedAt"]))
    let translations = SyncTable(name: "recipe_translations", cursorColumn: "translatedAt")
    #expect(translations.uploadExcludedColumns == Set(["translatedAt"]))
    // deletedAt is never excluded — collapseOutbox uploads `deletedAt = null` to un-tombstone.
    #expect(SyncTable(name: "recipes").uploadExcludedColumns.contains("deletedAt") == false)
}

@Test func pullNowWithNoTablesIsNoOp() async throws {
    let engine = try makeEngine()
    try await engine.pullNow() // no tables declared → nothing to pull, must not throw
}

@Test func pullNowReturnsNothing() async throws {
    // Issue #55: the pull's `[String: Set<String>]` is `fullResync`'s input, and its subtlest property
    // — a skipped scoped table has no entry, which is not the same as an empty set — is an invariant
    // of that reconcile. Nothing outside the engine can act on it correctly, so the public entry point
    // hands back Void. This annotation is the assertion: it stops compiling if the value comes back.
    let engine = try makeEngine()
    let _: Void = try await engine.pullNow()
}
