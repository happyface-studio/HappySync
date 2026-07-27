import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// APPS-473: stop() must await the in-flight sync pass, so a consumer can wipe/replace the database
// on sign-out knowing the engine has quiesced (no more DB writes, no more network calls).

@Test func stopAwaitsInFlightSyncPass() async throws {
    let db = try recipesDB()
    let started = Signal()
    let gate = Signal()
    let remote = GatedRemote(
        started: started, gate: gate,
        dataset: ["recipes": [["id": "r1", "title": "Applied", "updatedAt": "2026-07-02T10:00:00.000Z"]]]
    )
    // Long poll, silent doorbell → the only pass is the immediate one start() kicks.
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999
    )

    await engine.start()
    await started.wait() // the pull is now in-flight, blocked in fetch

    // Launch stop() and record when it returns. While the pass is blocked, stop must NOT return.
    let stopped = Signal()
    let stopTask = Task { await engine.stop(); await stopped.fire() }

    try await Task.sleep(for: .milliseconds(60))
    #expect(await stopped.isFired == false) // still waiting on the in-flight pass

    await gate.fire()      // let the in-flight pull complete
    await stopTask.value
    #expect(await stopped.isFired == true) // stop returned only after the pass finished

    // The in-flight pass ran to completion before stop returned — its write is present.
    let title = try await db.read { try String.fetchOne($0, sql: "SELECT title FROM recipes WHERE id='r1'") }
    #expect(title == "Applied")
}

// APPS-513: stop() must bail cooperatively at the next transaction boundary of a long pull, not run
// the whole (possibly first-ever / full-resync) pull to completion — while leaving the pages it did
// apply consistent for the next start() to resume from.

@Test func stopBailsAtPageBoundaryMidPull() async throws {
    let db = try recipesDB()
    let started = Signal()
    let gate = Signal()
    // Three single-row pages; the 2nd fetch blocks so the test can stop() mid-pull.
    let rows: [[String: AnyJSON]] = [
        ["id": "r0", "title": "r0", "updatedAt": "2026-01-01T00:00:00.000Z"],
        ["id": "r1", "title": "r1", "updatedAt": "2026-01-01T00:00:00.001Z"],
        ["id": "r2", "title": "r2", "updatedAt": "2026-01-01T00:00:00.002Z"],
    ]
    let remote = PagedGatedRemote(rows: rows, gateOnFetch: 2, started: started, gate: gate)
    // pageSize 1 → one row per fetch; long poll + silent doorbell → the only pass is start()'s.
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        pageSize: 1, doorbell: SilentDoorbell(), pollInterval: 999
    )

    await engine.start()
    await started.wait() // page 1 (r0) applied; page 2's fetch is blocked mid-pull

    let stopped = Signal()
    let stopTask = Task { await engine.stop(); await stopped.fire() }
    try await Task.sleep(for: .milliseconds(60)) // let stop() flip `stopping` before the gate releases
    #expect(await stopped.isFired == false)      // still waiting on the in-flight (blocked) fetch
    await gate.fire()    // release page 2; the engine must bail at the next boundary, not fetch page 3
    await stopTask.value
    #expect(await stopped.isFired)

    let ids = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(ids == ["r0", "r1"])          // page 3 (r2) never applied — teardown bailed at the boundary
    #expect(await remote.fetchCount == 2) // …and never fetched page 3

    // Restart resumes from the advanced cursor: the un-fetched row converges with no duplicates.
    let engine2 = try SyncEngine(
        db: db, remote: PagedGatedRemote(rows: rows, gateOnFetch: 99, started: Signal(), gate: Signal()),
        tables: [SyncTable(name: "recipes")], pageSize: 1, doorbell: SilentDoorbell(), pollInterval: 999
    )
    try await engine2.runSyncOnce()
    let resumed = try await db.read { try String.fetchAll($0, sql: "SELECT id FROM recipes ORDER BY id") }
    #expect(resumed == ["r0", "r1", "r2"]) // resumed cleanly from where teardown stopped
}

// MARK: - Status streams survive teardown (issue #45)

// stop() used to finish() every status stream, so the typical long-lived consumer —
// `.task { for await status in engine.status { … } }` — exited its loop for good. A later start()
// then broadcast to zero subscribers and the sync-status UI stayed dead for the rest of the process,
// on exactly the flow teardown exists for: sign-out → wipe → sign-in.

@Test func statusStreamStillDeliversAfterStopThenStart() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(dataset: ["recipes": [["id": "r1", "title": "Soup", "updatedAt": "2026-07-02T10:00:00.000Z"]]])
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999
    )

    // One subscription, taken before the stop/start cycle and consumed the way a SwiftUI `.task`
    // does — a `for await` loop that must outlive teardown.
    let collected = StatusCollector()
    let consumer = Task { for await status in engine.status { await collected.append(status) } }
    #expect(await eventually { await collected.count >= 1 }) // initial idle replay landed

    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 1 }) // start()'s immediate pass ran
    #expect(await eventually { await collected.syncingCount >= 1 })
    await engine.stop()
    let passesSeen = await collected.syncingCount

    // The restarted engine's pass must reach that *original* subscriber. With the old finish()-on-stop
    // behaviour the loop above has already exited, so no further status ever arrives. Counting
    // `.syncing` statuses pins the assertion to a *new pass* — stop()'s settle only broadcasts `.idle`.
    await engine.start()
    #expect(await eventually { await remote.fetchCalls >= 2 })              // the post-restart pass ran…
    #expect(await eventually { await collected.syncingCount > passesSeen }) // …and reached the subscriber

    await engine.stop()
    consumer.cancel()
}

@Test func stopSettlesStatusToDegradedKeepingCounts() async throws {
    let db = try recipesDB()
    let remote = FakeRemote(failUpserts: 99, permanentUpserts: true) // parks the write on its first attempt
    let engine = try SyncEngine(
        db: db, remote: remote, tables: [SyncTable(name: "recipes")],
        doorbell: SilentDoorbell(), pollInterval: 999
    )
    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')") // trigger-captured
    try await engine.runSyncOnce() // parks r1 → deadLetters == 1

    var status = engine.status.makeAsyncIterator()
    #expect(await status.next()?.deadLetters == 1) // replayed pre-stop snapshot

    await engine.start()
    await engine.stop()

    // stop() leaves a settled, non-syncing status whose outbox counts still surface the parked write —
    // teardown doesn't touch the outbox, so hiding them would read as healthy. And it settles *through*
    // those counts: a stopped engine with a parked write is `.degraded`, not `.idle` (issue #51).
    var afterStop = engine.status.makeAsyncIterator()
    let settled = await afterStop.next()
    #expect(settled?.phase == .degraded)
    #expect(settled?.deadLetters == 1)
    #expect(settled?.isHealthy == false)
}
