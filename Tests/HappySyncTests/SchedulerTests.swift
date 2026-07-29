import Testing
import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
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
    let remote = InMemorySyncRemote(failFetches: 1)
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
    let remote = InMemorySyncRemote()
    let doorbell = ManualDoorbell()
    // A comfortably large debounce so a synchronous burst reliably lands inside one window even under
    // load (issue #19); long poll so only the doorbell drives pulls.
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: doorbell, pollInterval: 999, debounceInterval: 0.5
    )
    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 1 }) // the initial start-sync settled
    let baseline = await remote.fetchCalls

    for _ in 0..<5 { doorbell.fire() } // a burst within one debounce window
    #expect(await eventually { await remote.fetchCalls > baseline }) // the burst produced a pull
    try await Task.sleep(for: .seconds(1)) // give any erroneous second debounce ample time to fire
    let after = await remote.fetchCalls
    await engine.stop()

    #expect(after - baseline == 1) // the whole burst coalesced into a single pull
}

@Test func periodicPollConvergesWhenDoorbellSilent() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    // SilentDoorbell never rings — convergence must come entirely from the periodic poll, proving
    // the engine still syncs if the Realtime channel drops.
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 0.03, debounceInterval: 0.3
    )
    await engine.start()
    // Wait (generously) for the periodic poll to fire several times, rather than asserting a fixed
    // count landed inside a tight fixed sleep — the latter starves under a loaded runner (issue #19).
    let converged = await eventually { await remote.fetchCalls >= 3 }
    await engine.stop()

    #expect(converged) // initial sync + repeated periodic polls, with no doorbell at all
}

// MARK: - Local writes wake the runner (APPS-503, issue #48)

@Test func localWriteTriggersDrainWithoutSyncNow() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    // Long poll, silent doorbell → the only thing that can drive an upload is the local write itself.
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999, debounceInterval: 0.02
    )
    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 1 }) // initial start-sync settled
    let baseline = await remote.upsertCalls.count

    // A plain GRDB write with no syncNow() follow-up must still upload promptly: the capture trigger
    // queues it and the engine's outbox observer wakes the runner (issue #48).
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    #expect(await eventually { await remote.upsertCalls.count == baseline + 1 }) // the write alone drove a drain
    await engine.stop()
}

@Test func burstOfWritesCoalescesIntoOneDrainPass() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    // A large debounce so the 20 sequential writes finish before it fires — the burst then coalesces
    // into one drain instead of splitting across windows under load (issue #19).
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999, debounceInterval: 0.5
    )
    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 1 }) // initial start-sync settled
    let baseline = await remote.fetchCalls

    // e.g. importing a recipe with many ingredients: a burst of writes inside one debounce window.
    for i in 0..<20 {
        try await write(db, "INSERT INTO recipes (id, title) VALUES (?, ?)", ["r\(i)", "row \(i)"])
    }
    // Wait until every queued write has uploaded, then count how many sync passes it took.
    #expect(await eventually { await remote.upsertCalls.count == 20 })
    let passes = await remote.fetchCalls - baseline // one fetch per sync pass (empty dataset)
    await engine.stop()

    #expect(passes >= 1) // it drained…
    #expect(passes <= 2) // …and the whole burst coalesced — not one pass per write
}

@Test func syncNowForcesAnImmediatePull() async throws {
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    // Long poll interval, silent doorbell → only an explicit nudge can cause another pull.
    let engine = try SyncEngine.forTesting(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999, debounceInterval: 0.3
    )
    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 1 }) // initial sync settled
    let baseline = await remote.fetchCalls

    await engine.syncNow() // e.g. the app returning to the foreground
    #expect(await eventually { await remote.fetchCalls > baseline }) // foreground nudge pulls promptly
    await engine.stop()
}
