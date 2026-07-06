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
