import Testing
import Foundation
import os
import GRDB
import Supabase
@testable import HappySync

// APPS-509: the Realtime doorbell must track auth changes. It used to resolve the partition scope
// once, when the engine started — so a cold launch that started the engine before sign-in skipped
// every scoped table for the process lifetime, and a later user switch kept filtering on the old
// uid. The engine now re-subscribes the doorbell whenever a pull resolves a different scope, with no
// consumer-side stop/start.

/// A thread-safe, mutable partition value standing in for the app's auth state — flip it to
/// simulate a sign-in or user switch between sync passes.
private final class ScopeBox: @unchecked Sendable {
    private let lock = OSAllocatedUnfairLock(initialState: String?.none)
    var value: String? {
        get { lock.withLock { $0 } }
        set { lock.withLock { $0 = newValue } }
    }
}

/// A `recipes` DB carrying the `userId` partition column a scoped subscription filters on.
private func scopedRecipesDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("userId", .text)
            t.column("updatedAt", .text)
        }
    }
    return db
}

@Test func signInReSubscribesDoorbellAndRingsWithoutStopStart() async throws {
    let db = try scopedRecipesDB()
    let remote = FakeRemote()
    let doorbell = FakeDoorbell()
    let scope = ScopeBox() // nil = signed out at start (cold launch before session restore)
    let engine = try SyncEngine(
        db: db, remote: remote,
        tables: [SyncTable(name: "recipes", scopeColumn: "userId")],
        doorbell: doorbell, pollInterval: 999, debounceInterval: 0.02,
        scope: { scope.value }
    )

    await engine.start()
    try await Task.sleep(for: .milliseconds(60)) // the first pass subscribes the doorbell
    #expect(doorbell.ringScopes == [nil]) // started signed-out: subscribed, scoped table skipped

    scope.value = "u1"     // the user signs in — no manual stop()/start()
    await engine.syncNow() // the next convergence pass (foreground or poll would do the same)
    try await Task.sleep(for: .milliseconds(60))
    #expect(doorbell.ringScopes == [nil, "u1"]) // engine re-subscribed to the user's partition
    #expect(doorbell.liveSubscriptions == 1)    // …and tore down the signed-out subscription

    // A Realtime change on the user's own recipe now rings through to a pull.
    let baseline = await remote.fetchCalls
    doorbell.fire()
    try await Task.sleep(for: .milliseconds(60))
    let after = await remote.fetchCalls
    await engine.stop()

    #expect(after - baseline == 1) // the scoped subscription rings without a stop/start
}

@Test func userSwitchReFiltersDoorbellAndOldEventsStopPoking() async throws {
    let db = try scopedRecipesDB()
    let remote = FakeRemote()
    let doorbell = FakeDoorbell()
    let scope = ScopeBox()
    scope.value = "u1" // signed in as u1 from the start
    let engine = try SyncEngine(
        db: db, remote: remote,
        tables: [SyncTable(name: "recipes", scopeColumn: "userId")],
        doorbell: doorbell, pollInterval: 999, debounceInterval: 0.02,
        scope: { scope.value }
    )

    await engine.start()
    try await Task.sleep(for: .milliseconds(60))
    #expect(doorbell.ringScopes == ["u1"]) // subscribed filtering u1's rows

    scope.value = "u2"     // a different user signs in
    await engine.syncNow()
    try await Task.sleep(for: .milliseconds(60))
    #expect(doorbell.ringScopes == ["u1", "u2"]) // re-filtered to u2
    #expect(doorbell.liveSubscriptions == 1)     // only the u2 subscription is live; u1 torn down

    // An event on the old (u1) subscription must no longer poke the runner.
    let baseline = await remote.fetchCalls
    doorbell.fire(subscription: 0) // ring the torn-down u1 stream
    try await Task.sleep(for: .milliseconds(60))
    #expect(await remote.fetchCalls == baseline) // old-uid event is ignored

    // …while an event on the current (u2) subscription still pokes.
    doorbell.fire()
    try await Task.sleep(for: .milliseconds(60))
    let after = await remote.fetchCalls
    await engine.stop()

    #expect(after == baseline + 1) // exactly the u2 event drove a pull
}

@Test func channelNameIsUniquePerEngineInstance() {
    // Two engines on one Supabase client must not join the same Realtime channel and cross-ring.
    let a = SupabaseDoorbell.makeChannelName()
    let b = SupabaseDoorbell.makeChannelName()
    #expect(a != b)
    #expect(a.hasPrefix("happysync-")) // stable, recognizable base
    #expect(b.hasPrefix("happysync-"))
}

@Test func unscopedEngineSubscribesDoorbellOnceAcrossAuthChanges() async throws {
    // With no scoped tables the subscription filter doesn't depend on the uid, so an auth change
    // must not churn the channel — the doorbell subscribes exactly once.
    let db = try recipesDB()
    let doorbell = FakeDoorbell()
    let scope = ScopeBox()
    let engine = try SyncEngine(
        db: db, remote: FakeRemote(),
        tables: [SyncTable(name: "recipes")], // unscoped
        doorbell: doorbell, pollInterval: 999, debounceInterval: 0.02,
        scope: { scope.value }
    )

    await engine.start()
    try await Task.sleep(for: .milliseconds(60))
    scope.value = "u1"     // sign in
    await engine.syncNow()
    try await Task.sleep(for: .milliseconds(60))
    await engine.stop()

    #expect(doorbell.ringScopes == [nil]) // one subscription, unaffected by the auth change
}
