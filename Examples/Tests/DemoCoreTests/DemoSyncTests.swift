import Testing
import Foundation
import GRDB
import HappySync
import HappySyncTestSupport
import DemoCore

// The example, asserted. Every one of these is a behaviour the SwiftUI app puts on screen — writes
// uploading with no engine call, a parent delete tombstoning its children, a change from "device B"
// arriving through the doorbell, a refused write parking and being repaired, and sign-out quiescing
// the engine before the store is replaced. Running in CI is what stops the demo from rotting (#58).

@MainActor
@Test func aPlainWriteUploadsWithNoEngineCall() async throws {
    let sync = try DemoSync()
    let remote = sync.remote
    await sync.start()
    defer { Task { await sync.engine.stop() } }

    try await sync.addList(title: "Groceries")

    #expect(await eventually { await remote.upsertCalls.contains { $0.table == "lists" } })
}

@MainActor
@Test func deletingAListTombstonesItsItemsChildrenFirst() async throws {
    let sync = try DemoSync()
    let remote = sync.remote
    await sync.start()
    defer { Task { await sync.engine.stop() } }

    let list = try await sync.addList(title: "Trip")
    try await sync.addItem(title: "Passport", to: list.id)
    try await sync.addItem(title: "Charger", to: list.id)
    #expect(await eventually { await remote.upsertCalls.count == 3 })

    // One DELETE of the parent. SQLite cascades; each removed child's own capture trigger queues its
    // tombstone, so the app never enumerates children.
    try await sync.deleteList(id: list.id)

    #expect(await eventually { await remote.deleteCalls.count == 3 })
    let deletes = await remote.deleteCalls
    #expect(deletes.filter { $0.table == "items" }.count == 2)
    #expect(deletes.last?.table == "lists") // children before parent — the FK-safe delete order
}

@MainActor
@Test func aChangeFromAnotherDeviceArrivesThroughTheDoorbell() async throws {
    let sync = try DemoSync()
    let database = sync.db
    await sync.start()
    defer { Task { await sync.engine.stop() } }

    // Realtime is a doorbell only: the ring triggers a pull, and the row arrives through the cursor
    // fetch — never from the event payload.
    await sync.publishFromAnotherDevice(title: "From device B")

    #expect(await eventually {
        await count(in: database, "SELECT COUNT(*) FROM lists WHERE title = 'From device B'") == 1
    })
}

@MainActor
@Test func aRefusedWriteParksAndRetryRepairsIt() async throws {
    let sync = try DemoSync()
    let engine = sync.engine
    await sync.start()
    defer { Task { await engine.stop() } }

    // The server refuses the write the way an RLS policy does — permanently, so the drain parks the
    // entry on its first attempt instead of retrying it forever.
    await sync.rejectWrites()
    try await sync.addList(title: "Doomed")

    #expect(await eventually { await parked(engine).count == 1 })
    let letter = try #require(await parked(engine).first)
    #expect(letter.table == "lists")
    #expect(letter.op == .upsert)

    // Repair: fix the cause first, then re-queue. Retrying a still-broken write just parks it again.
    await sync.acceptWrites()
    try await engine.retryDeadLetters()

    #expect(await eventually { await parked(engine).isEmpty })
    #expect(await eventually { await outboxCount(sync) == 0 }) // the write finally landed
}

@MainActor
@Test func signOutQuiescesTheEngineBeforeTheStoreIsReplaced() async throws {
    let sync = try DemoSync()
    let remote = sync.remote
    await sync.start()
    try await sync.addList(title: "Before sign-out")
    #expect(await eventually { await remote.upsertCalls.contains { $0.table == "lists" } })

    // stop() → replace the store → start(). Wiping before the engine has quiesced is how an app
    // writes into a database it is about to delete; deleting rows instead of replacing the store is
    // how it queues a tombstone for everything the user owns.
    try await sync.signOutAndBackIn()
    defer { Task { await sync.engine.stop() } }

    #expect(await count(in: sync.db, "SELECT COUNT(*) FROM lists") == 0)
    #expect(await count(in: sync.db, "SELECT COUNT(*) FROM _sync_outbox") == 0)
}

// MARK: - Helpers

/// A scalar count, or `-1` if the read failed — shaped for use inside `eventually`, which polls a
/// non-throwing condition.
private func count(in db: DatabaseQueue, _ sql: String) async -> Int {
    (try? await db.read { db in try Int.fetchOne(db, sql: sql) ?? 0 }) ?? -1
}

private func parked(_ engine: SyncEngine) async -> [DeadLetter] {
    (try? await engine.deadLetters()) ?? []
}

@MainActor
private func outboxCount(_ sync: DemoSync) async -> Int {
    await count(in: sync.db, "SELECT COUNT(*) FROM _sync_outbox")
}
