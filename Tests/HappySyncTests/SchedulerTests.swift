import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// MARK: - Retry-delay policy

@Test func nextDelayUsesPollIntervalWhenHealthy() {
    #expect(nextDelay(consecutiveFailures: 0, pollInterval: 30) == 30)
}

@Test func nextDelayBacksOffAfterFailures() {
    #expect(nextDelay(consecutiveFailures: 2, pollInterval: 30) == 4)  // backoffDelay(2) = 2^2
    #expect(nextDelay(consecutiveFailures: 10, pollInterval: 30) == 64) // clamped at 2^6
}

// MARK: - Status transitions

@Test func syncRunDrivesStatusSyncingThenIdleWithTimestamp() async throws {
    let db = try recipesDB()
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")])
    var status = engine.status.makeAsyncIterator()
    _ = await status.next() // initial idle replay

    try await engine.runSyncOnce()

    let syncing = await status.next()
    let settled = await status.next()
    #expect(syncing?.phase == .syncing)
    #expect(settled?.phase == .idle)
    #expect(settled?.lastSyncedAt != nil) // a successful run stamps lastSyncedAt
}

@Test func failedSyncDrivesStatusToFailed() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failFetches: 1)
    let engine = try makeEngine(db: db, tables: [SyncTable(name: "recipes")], remote: remote)
    var status = engine.status.makeAsyncIterator()
    _ = await status.next() // initial idle replay

    await #expect(throws: Error.self) { try await engine.runSyncOnce() }

    let syncing = await status.next()
    let failed = await status.next()
    #expect(syncing?.phase == .syncing)
    if case .failed = failed?.phase {} else { Issue.record("expected .failed, got \(String(describing: failed?.phase))") }
}

// MARK: - Doorbell (debounced) & periodic fallback

@Test func doorbellBurstTriggersExactlyOnePull() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let doorbell = FakeDoorbell()
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: doorbell, pollInterval: 999, debounceInterval: 0.02
    )
    await engine.start()
    try await Task.sleep(for: .milliseconds(60)) // let the initial start-sync settle
    let baseline = await remote.fetchCalls

    for _ in 0..<5 { doorbell.fire() } // a burst within one debounce window
    try await Task.sleep(for: .milliseconds(90))
    let after = await remote.fetchCalls
    await engine.stop()

    #expect(after - baseline == 1) // the whole burst coalesced into a single pull
}

@Test func periodicPollConvergesWhenDoorbellSilent() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    // SilentDoorbell never rings — convergence must come entirely from the periodic poll, proving
    // the engine still syncs if the Realtime channel drops.
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 0.03, debounceInterval: 0.3
    )
    await engine.start()
    try await Task.sleep(for: .milliseconds(150)) // several poll intervals
    let pulls = await remote.fetchCalls
    await engine.stop()

    #expect(pulls >= 3) // initial sync + repeated periodic polls, with no doorbell at all
}
