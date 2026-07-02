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
