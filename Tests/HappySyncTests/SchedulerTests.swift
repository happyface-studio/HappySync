import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// MARK: - Retry-delay policy

@Test func nextDelayUsesPollIntervalWhenHealthy() {
    #expect(nextDelay(consecutiveFailures: 0, pollInterval: 30) == 30) // healthy → exact poll, no jitter
}

@Test func nextDelayBacksOffAfterFailures() {
    // Exponential backoff carries ±20% jitter (APPS-514), so assert within the base's jitter bounds.
    let d2 = nextDelay(consecutiveFailures: 2, pollInterval: 30)
    #expect(d2 >= 4 * 0.8 && d2 <= 4 * 1.2)     // base backoffDelay(2) = 2^2 = 4
    let d10 = nextDelay(consecutiveFailures: 10, pollInterval: 30)
    #expect(d10 >= 64 * 0.8 && d10 <= 64 * 1.2) // clamped at 2^6 = 64
}

// APPS-514: ±20% jitter, deterministic under a seeded RNG.

/// A deterministic `RandomNumberGenerator` (SplitMix64) so the jitter is reproducible in tests.
private struct SeededGenerator: RandomNumberGenerator {
    private var state: UInt64
    init(seed: UInt64) { state = seed }
    mutating func next() -> UInt64 {
        state = state &+ 0x9E3779B97F4A7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58476D1CE4E5B9
        z = (z ^ (z >> 27)) &* 0x94D049BB133111EB
        return z ^ (z >> 31)
    }
}

@Test func backoffJitterStaysWithinTwentyPercentOfBase() {
    var rng = SeededGenerator(seed: 42)
    for attempts in 1...8 {
        let base = pow(2.0, Double(min(max(attempts, 1), 6)))
        for _ in 0..<200 {
            let delay = backoffDelay(attempts: attempts, using: &rng)
            #expect(delay >= base * 0.8)
            #expect(delay <= base * 1.2)
        }
    }
}

@Test func backoffJitterIsReproducibleForASeed() {
    var a = SeededGenerator(seed: 7)
    var b = SeededGenerator(seed: 7)
    #expect(backoffDelay(attempts: 3, using: &a) == backoffDelay(attempts: 3, using: &b))
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

// MARK: - Local writes wake the runner (APPS-503)

@Test func enqueueTriggersDrainWithoutSyncNow() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    // Long poll, silent doorbell → the only thing that can drive an upload is the enqueue itself.
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999, debounceInterval: 0.02
    )
    await engine.start()
    try await Task.sleep(for: .milliseconds(50)) // initial start-sync settles
    let baseline = await remote.upsertCalls.count

    // A local write with no syncNow() follow-up must still upload promptly.
    try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r1", "title": "Soup"])
    try await Task.sleep(for: .milliseconds(80))
    let after = await remote.upsertCalls.count
    await engine.stop()

    #expect(after - baseline == 1) // the enqueue alone drove a drain, no explicit nudge
}

@Test func burstOfEnqueuesCoalescesIntoOneDrainPass() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999, debounceInterval: 0.05
    )
    await engine.start()
    try await Task.sleep(for: .milliseconds(80)) // initial start-sync settles
    let baseline = await remote.fetchCalls

    // e.g. importing a recipe with many ingredients: a burst of writes inside one debounce window.
    for i in 0..<20 {
        try await engine.enqueue(.upsert, table: "recipes", row: ["id": "r\(i)", "title": "row \(i)"])
    }
    try await Task.sleep(for: .milliseconds(120))
    let passes = await remote.fetchCalls - baseline // one fetch per sync pass (empty dataset)
    let uploads = await remote.upsertCalls.count
    await engine.stop()

    #expect(passes <= 2)   // the whole burst coalesced — not one pass per write
    #expect(passes >= 1)   // …but it did drain
    #expect(uploads == 20) // and every queued write was uploaded in that pass
}

@Test func syncNowForcesAnImmediatePull() async throws {
    let db = try recipesDB()
    let remote = FakeRemote()
    // Long poll interval, silent doorbell → only an explicit nudge can cause another pull.
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999, debounceInterval: 0.3
    )
    await engine.start()
    try await Task.sleep(for: .milliseconds(50)) // initial sync settles
    let baseline = await remote.fetchCalls

    await engine.syncNow() // e.g. the app returning to the foreground
    try await Task.sleep(for: .milliseconds(50))
    let after = await remote.fetchCalls
    await engine.stop()

    #expect(after > baseline) // foreground nudge pulls without waiting for the periodic poll
}
