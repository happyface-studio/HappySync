import Foundation
import os
import GRDB
import Supabase

/// Multicasts the latest `SyncStatus` to any number of subscribers, replaying the latest snapshot
/// to each new one. CookThis has two consumers (the status UI and a refresh loop); a bare
/// `AsyncStream` is single-consumer, so the engine fans out through this instead.
final class StatusBroadcaster: Sendable {
    private struct State {
        var latest: SyncStatus
        var subscribers: [UUID: AsyncStream<SyncStatus>.Continuation] = [:]
    }

    private let state: OSAllocatedUnfairLock<State>

    init(initial: SyncStatus) {
        state = OSAllocatedUnfairLock(initialState: State(latest: initial))
    }

    /// A fresh stream that immediately replays the latest status, then receives every update.
    func subscribe() -> AsyncStream<SyncStatus> {
        AsyncStream { continuation in
            let id = UUID()
            let latest = state.withLock { s -> SyncStatus in
                s.subscribers[id] = continuation
                return s.latest
            }
            continuation.yield(latest)
            continuation.onTermination = { [state] _ in
                state.withLock { _ = $0.subscribers.removeValue(forKey: id) }
            }
        }
    }

    /// The most recent snapshot — what a new subscriber would replay.
    var latest: SyncStatus { state.withLock { $0.latest } }

    /// Records the new status and fans it out. Yields outside the lock to avoid reentrancy.
    func send(_ status: SyncStatus) {
        let continuations = state.withLock { s -> [AsyncStream<SyncStatus>.Continuation] in
            s.latest = status
            return Array(s.subscribers.values)
        }
        for continuation in continuations { continuation.yield(status) }
    }

    /// Terminates every subscriber's stream and drops them. **Disposal only** — reserved for the
    /// engine's `deinit`. `stop()` must not call this: a subscriber loop (`for await status in
    /// engine.status` inside a SwiftUI `.task`) would exit for good, so a later `start()` would
    /// broadcast to nobody and the status UI would stay dead for the rest of the process (issue #45).
    func finish() {
        let continuations = state.withLock { s -> [AsyncStream<SyncStatus>.Continuation] in
            let all = Array(s.subscribers.values)
            s.subscribers.removeAll()
            return all
        }
        for continuation in continuations { continuation.finish() }
    }
}

/// A tombstone seen during a pull, deferred so all deletes can be applied children-first.
private struct PendingDelete: Sendable {
    let table: String
    let primaryKey: String
    let cursorColumn: String
    let pk: String
    let updatedAt: String
}

/// The HappySync engine: owns the outbox drain, cursor pull, tombstones, FK ordering,
/// Realtime doorbell, status, and retry/backoff.
///
/// It does **not** own reads — the app keeps observing GRDB with `ValueObservation` — or schema.
///
/// **Writes need no engine call.** Every synced table carries SQLite triggers that record each
/// insert/update/delete into the outbox, in the same transaction as the write itself, so
/// `try await db.write { try recipe.save($0) }` uploads exactly like a call through the deprecated
/// `enqueue` shim did — and so does a partial update, a `count = count + 1`, raw SQL, or a
/// twelve-row multi-table transaction (issue #48).
///
/// > Status: upload (trigger capture + `drainOutbox`, APPS-413), download (`pullNow` — tuple cursor,
/// > LWW, tombstones, pagination, APPS-414), and the scheduler that drives them (`start` — debounced
/// > Realtime doorbell, periodic fallback, status, backoff, APPS-415) are all live. M1 is complete.
public actor SyncEngine {
    private let db: any DatabaseWriter
    private let tables: [SyncTable]
    private let remote: any SyncRemote
    private let pageSize: Int
    private let doorbell: any SyncDoorbell
    private let pollInterval: TimeInterval
    private let debounceInterval: TimeInterval
    /// Resolves the current user's download-partition value (auth uid) for tables that declare a
    /// `scopeColumn`, or nil when signed out. Called per pull so a user switch re-scopes (APPS-469).
    private let scope: @Sendable () async -> String?
    /// Whether any synced table declares a `scopeColumn`. When false the doorbell subscription is
    /// scope-independent, so an auth change needn't tear down and rebuild its channel (APPS-509).
    private let hasScopedTables: Bool
    /// After this many failed upload attempts a transient entry is dead-lettered (parked). A
    /// permanent (4xx) failure parks immediately regardless. See APPS-470.
    private let deadLetterAfter: Int
    /// If this device hasn't successfully synced within this window, its cursor may point past
    /// tombstones the server has since purged — so the first sync full-resyncs and reconciles
    /// instead of trusting the stale cursor. **Must be ≤ the server's tombstone-purge retention**
    /// (CookThis purges at 90 days). See APPS-471 / contract §1.
    private let maxOfflineGap: TimeInterval
    /// The wall-clock time the stale-cursor check last **confirmed the device fresh** (APPS-471), or
    /// nil when it hasn't completed — the first pass, or a pass that deferred it (stale device, scoped
    /// tables, no partition value yet — APPS-501). Re-evaluated every pass rather than gated
    /// once-per-process: nil forces a re-check, and a non-nil baseline older than `maxOfflineGap`
    /// re-arms it, so a long-lived process (menu-bar Mac app, a laptop that mostly sleeps) that passes
    /// the check then sits offline past the gap while alive resyncs on reconnect instead of trusting a
    /// cursor that may point past purged tombstones (APPS-512). In steady state the baseline is recent,
    /// so this is a date comparison with no extra I/O.
    private var resyncBaseline: Date?

    private nonisolated let statusBroadcaster: StatusBroadcaster
    /// Structured logging for field diagnosis (APPS-514): pass lifecycle, dead-letters, stale-cursor
    /// resync, doorbell (re)subscription, and scoped-table skips. **Never logs row payloads or tokens**
    /// (user content) — only table names, counts, primary keys (redacted in release builds by the
    /// default `.private` interpolation), and error text.
    private let log = Logger(subsystem: "studio.happyface.happysync", category: "engine")
    private var isRunning = false
    private var lastSyncedAt: Date?
    private var consecutiveFailures = 0
    /// The serialized runner consuming the wake stream. Kept separate from the trigger loops so
    /// `stop()` can let the in-flight pass finish gracefully (finish the wake stream, don't cancel)
    /// while cancelling the triggers (APPS-473).
    private var runnerTask: Task<Void, Never>?
    /// The periodic loop that pokes the runner. Cancelled on `stop()`.
    private var triggersTask: Task<Void, Never>?
    /// The Realtime doorbell's consumer. Managed apart from the trigger loops so an auth change can
    /// tear it down and re-subscribe under the new partition scope — without a stop/start (APPS-509).
    /// Cancelled on `stop()`.
    private var doorbellTask: Task<Void, Never>?
    /// Which partition value the doorbell is currently subscribed under, so a pull that resolves a
    /// different scope is detected as a transition and re-subscribes (APPS-509).
    private var doorbellState: DoorbellState = .idle
    private var debounceTask: Task<Void, Never>?
    private var wake: AsyncStream<Void>.Continuation?
    /// Set by `stop()` before it awaits the in-flight pass, so `runSyncOnce` can unwind cooperatively
    /// at the next transaction boundary instead of running a first-ever pull / `fullResync` / large
    /// drain to completion on a slow network — bounding teardown latency for sign-out/account-switch,
    /// which must `await stop()` before wiping the DB (APPS-513). Cleared by `start()`.
    private var stopping = false
    /// Watches `_sync_outbox` for trigger-queued writes so a plain GRDB write still wakes the runner
    /// promptly, the way `enqueue`'s own poke used to (issue #48). Held strongly here and registered
    /// with `.observerLifetime`, so it goes away with the engine rather than outliving it on the
    /// database.
    private nonisolated let outboxObserver: OutboxObserver

    /// Thrown at a cooperative checkpoint when `stop()` has begun teardown, so an in-flight pass
    /// unwinds at the next transaction boundary rather than completing. Caught in `runSyncOnce`, which
    /// settles without flipping to `.failed` — every unit already committed is consistent (APPS-513).
    private struct StopRequested: Error {}

    /// Tracks the doorbell's current subscription so an auth change surfaces as a state transition.
    private enum DoorbellState: Equatable {
        case idle                    // not subscribed — before start() / after stop()
        case ringing(scope: String?) // subscribed, filtering this partition (nil = signed out)
    }

    /// Live engine status. Each access returns an independent stream that replays the latest
    /// snapshot, so multiple consumers (status UI, refresh loop) can observe concurrently.
    ///
    /// A stream stays alive for as long as the engine does — `stop()` settles it but doesn't end it —
    /// so a view can subscribe once and keep receiving updates across a stop/start cycle (issue #45).
    public nonisolated var status: AsyncStream<SyncStatus> {
        statusBroadcaster.subscribe()
    }

    /// - Parameters:
    ///   - db: GRDB writer (`DatabaseQueue` or `DatabasePool`) — the local source of truth.
    ///   - supabase: Supabase client for PostgREST upsert/pull and the Realtime doorbell.
    ///   - tables: Synced tables in any order; the engine sorts them by `dependsOn`.
    ///   - auth: Returns a fresh Supabase access token, called before each authenticated batch.
    ///   - scope: Resolves the current user's download-partition value (auth uid) for tables that
    ///     declare a `scopeColumn`. Defaults to `nil` (no partition beyond RLS). Called per pull, so
    ///     a signed-in user change re-scopes without re-declaring tables. See APPS-469.
    ///   - maxOfflineGap: If the device hasn't synced within this window, the first sync full-resyncs
    ///     to reconcile against purged tombstones. **Must be ≤ the server's tombstone-purge
    ///     retention.** Defaults to 30 days. See APPS-471.
    public init(
        db: any DatabaseWriter,
        supabase: SupabaseClient,
        tables: [SyncTable],
        auth: @escaping @Sendable () async -> String,
        scope: @escaping @Sendable () async -> String? = { nil },
        maxOfflineGap: TimeInterval = 30 * 24 * 3600
    ) throws {
        try self.init(
            db: db,
            remote: SupabaseRemote(client: supabase, auth: auth),
            tables: tables,
            doorbell: SupabaseDoorbell(
                client: supabase,
                tables: tables.map { ($0.name, $0.scopeColumn) },
                channelBase: SupabaseDoorbell.makeChannelName()
            ),
            scope: scope,
            maxOfflineGap: maxOfflineGap
        )
    }

    /// Injects the `SyncRemote`/`SyncDoorbell` seams directly — used by tests to drive sync with
    /// fakes. `pageSize` forces pagination on a small dataset; `pollInterval`/`debounceInterval` let
    /// tests shrink the scheduler's timing without waiting on production intervals.
    init(
        db: any DatabaseWriter,
        remote: any SyncRemote,
        tables: [SyncTable],
        pageSize: Int = 500,
        doorbell: any SyncDoorbell = SilentDoorbell(),
        pollInterval: TimeInterval = 30,
        debounceInterval: TimeInterval = 0.3,
        scope: @escaping @Sendable () async -> String? = { nil },
        deadLetterAfter: Int = 8,
        maxOfflineGap: TimeInterval = 30 * 24 * 3600
    ) throws {
        self.db = db
        self.remote = remote
        self.tables = tables
        self.pageSize = pageSize
        self.doorbell = doorbell
        self.pollInterval = pollInterval
        self.debounceInterval = debounceInterval
        self.scope = scope
        self.hasScopedTables = tables.contains { $0.scopeColumn != nil }
        self.deadLetterAfter = deadLetterAfter
        self.maxOfflineGap = maxOfflineGap
        self.statusBroadcaster = StatusBroadcaster(initial: SyncStatus())
        self.outboxObserver = OutboxObserver()

        try SyncSchema.migrator().migrate(db)

        // Wake the runner when a trigger queues a write, so a direct GRDB write uploads as promptly as
        // `enqueue`'s own poke used to (issue #48). `weak self`: GRDB holds the observer only as long
        // as the engine does, and a poke into a deallocated engine has nothing left to sync.
        outboxObserver.onNewEntries = { [weak self] in
            Task { await self?.armDebouncedPoke() }
        }

        // `PRAGMA recursive_triggers` is connection state and the observer attaches to a connection,
        // so both go on the writer — the only connection triggers ever fire on — outside a transaction.
        try db.writeWithoutTransaction { [outboxObserver] db in
            try SyncTriggers.enableRecursiveTriggers(db)
            db.add(transactionObserver: outboxObserver, extent: .observerLifetime)
        }

        // Install the write-capture triggers for the declared manifest. This runs at every init rather
        // than in a numbered migration: the manifest is source code, so adding or removing a
        // `SyncTable` ships without a schema version bump. `install` is idempotent — a steady-state
        // launch reads `sqlite_master` and issues no DDL.
        try db.write { db in
            // Insurance only: `applying` is transactional, so a crash mid-apply already rolled it back.
            try SyncTriggers.setApplying(db, 0)
            try SyncTriggers.install(db, tables: tables)
        }
    }

    /// Begins background sync. Idempotent. Runs an immediate convergence sync, then keeps the local
    /// store in sync from three trigger sources, all funnelled through one serialised runner:
    ///  - the Realtime **doorbell**, debounced so a burst of change events collapses to one pull;
    ///  - a **periodic** poll that converges even if the Realtime channel drops;
    ///  - (the doorbell/periodic both retry with exponential `backoffDelay` while syncs are failing).
    public func start() {
        guard !isRunning else { return }
        isRunning = true
        stopping = false // a fresh run: clear any teardown flag a prior stop() left set (APPS-513)

        let (stream, continuation) = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
        wake = continuation
        // ponytail: the running subtasks hold `self` for the engine's lifetime; `stop()` is the
        // teardown that cancels them. Fine for an app-lifetime engine, revisit if it must be GC'd live.
        // The runner is its own task (not in the trigger group) so `stop()` can drain it gracefully.
        runnerTask = Task { [weak self] in
            guard let self else { return }
            await self.runLoop(stream)
        }
        // The periodic poll pokes the runner on a timer; the Realtime doorbell is subscribed by the
        // first sync pass and re-subscribed on any auth change, both via `updateDoorbellScope` — so
        // scoped tables ring for the current user even if start() preceded sign-in (APPS-509).
        triggersTask = Task { [weak self] in
            guard let self else { return }
            await self.periodicLoop()
        }
        poke() // converge immediately on start (the first pass also subscribes the doorbell)
    }

    /// Stops background sync. **Awaits the in-flight sync pass** before returning: after
    /// `await stop()` the engine has quiesced — no further DB writes and no network calls — so a
    /// consumer can safely wipe or repurpose the database (e.g. on sign-out / account switch).
    /// Idempotent. See APPS-473 and the README teardown section.
    ///
    /// Status streams **stay open** across teardown: `stop()` broadcasts a settled status and leaves
    /// every subscriber registered, so the usual long-lived consumer (`for await status in
    /// engine.status` inside a SwiftUI `.task`) keeps receiving updates after a later `start()`.
    /// Terminating the streams here would silently kill the status UI for the rest of the process on
    /// the very flow this API exists to serve — sign-out → wipe → sign-in (issue #45). Only `deinit`
    /// finishes them.
    public func stop() async {
        guard isRunning else { return }
        isRunning = false
        stopping = true                       // let the in-flight pass bail at its next checkpoint (APPS-513)
        wake?.finish(); wake = nil            // no more passes enqueued; runner exits after the current one
        triggersTask?.cancel()                // stop the periodic loop from poking
        doorbellTask?.cancel()                // tear down the Realtime doorbell channel
        debounceTask?.cancel(); debounceTask = nil
        await runnerTask?.value               // await the in-flight pass to finish (uncancelled → completes)
        await triggersTask?.value             // and the periodic loop to unwind
        await doorbellTask?.value             // and the doorbell consumer (channel teardown) to unwind
        runnerTask = nil; triggersTask = nil; doorbellTask = nil
        doorbellState = .idle                 // a later start() re-subscribes from scratch
        // Settle the status rather than finishing the streams (issue #45). A pass that was mid-flight
        // left `.syncing` broadcast, and nothing is syncing any more — so settle, and drop a `.failed`
        // reason from the last pass too: with the engine stopped nothing is retrying it, so it's no
        // longer live state. The outbox counts carry over — teardown doesn't touch the outbox, so
        // `failedUploads`/`deadLetters` stay visible across a stop, and settling *through* them lands on
        // `.degraded` rather than reporting a stopped engine with parked writes as `.idle` (issue #51).
        let carried = statusBroadcaster.latest
        statusBroadcaster.send(transitionalStatus(
            SyncStatus.settledPhase(failedUploads: carried.failedUploads, deadLetters: carried.deadLetters)
        ))
    }

    /// A status for a phase change that doesn't itself re-derive the outbox counts — `.syncing` at the
    /// head of a pass, `.failed` when one throws, and the settled `.idle` `stop()` broadcasts — carrying
    /// `failedUploads`/`deadLetters` over from the last broadcast. Those counts describe the outbox, not
    /// the pass: zeroing them would blink a degraded-sync indicator off for the length of every pass,
    /// and since `stop()` settles from the last broadcast it would also hide a parked write across
    /// teardown (issue #45). The settled `.idle` at the end of a pass re-reads both from the DB.
    private func transitionalStatus(_ phase: SyncStatus.Phase) -> SyncStatus {
        let previous = statusBroadcaster.latest
        return SyncStatus(
            phase: phase, lastSyncedAt: lastSyncedAt,
            failedUploads: previous.failedUploads, deadLetters: previous.deadLetters
        )
    }

    /// The one genuine disposal path: the engine is going away, so no `start()` can follow and the
    /// status streams should terminate instead of hanging their consumers forever. `stop()` deliberately
    /// does *not* do this — see its documentation and issue #45.
    deinit {
        statusBroadcaster.finish()
    }

    /// The single serialised runner: one sync at a time, so pushes and pulls never overlap. Pokes
    /// that arrive mid-run coalesce (the wake channel buffers only the newest), so a backlog drains
    /// in one extra pass rather than one-per-poke.
    private func runLoop(_ wake: AsyncStream<Void>) async {
        for await _ in wake {
            do {
                let status = try await runSyncOnce()
                // A pull can succeed while uploads keep failing. Any still-failing upload (attempts > 0,
                // not yet parked) keeps the scheduler backed off so a flapping upload isn't hammered —
                // including on passes where the entry sat inside its backoff window and the drain skipped
                // it, so backoff no longer collapses to the steady poll mid-failure (APPS-507).
                // Dead-lettered entries don't count — they've stopped retrying, so the engine is healthy
                // again (APPS-470).
                consecutiveFailures = status.failedUploads > 0 ? consecutiveFailures + 1 : 0
            } catch {
                consecutiveFailures += 1 // status already shows .failed; the periodic loop retries with backoff
            }
        }
    }

    /// (Re)subscribes the Realtime doorbell to `scope` whenever it differs from the current
    /// subscription. Driven from every sync pass, so an auth change the runner resolves — nil→uid at
    /// sign-in, uid→uid' on a user switch — tears down the stale channel and re-subscribes to the
    /// right partition, without the app stopping/starting the engine (APPS-509). Unscoped-only
    /// engines subscribe once: their filter doesn't depend on the uid, so an auth change needn't
    /// churn the channel.
    private func updateDoorbellScope(_ scope: String?) {
        guard isRunning else { return } // the doorbell only runs between start() and stop()
        let target = hasScopedTables ? scope : nil
        guard doorbellState != .ringing(scope: target) else { return }
        doorbellState = .ringing(scope: target)
        // Log the transition without the raw partition value (an auth uid — PII): only whether the
        // subscription is scoped (APPS-514).
        log.info("doorbell subscribing (scoped: \(target != nil, privacy: .public)); previous subscription torn down")
        doorbellTask?.cancel() // end the previous subscription — its channel tears down on cancel
        doorbellTask = Task { [weak self] in
            guard let self else { return }
            await self.consumeDoorbell(scope: target)
        }
    }

    /// Consumes one doorbell subscription: each ring (re)arms the shared debounce, so a burst of
    /// remote changes collapses to a single pull. Ends when the subscription is torn down (stop, or
    /// a re-scope), which cancels this task.
    private func consumeDoorbell(scope: String?) async {
        for await _ in doorbell.ring(scope: scope) {
            armDebouncedPoke()
        }
    }

    /// (Re)arms a single trailing debounce that pokes the runner after `debounceInterval`. Shared by
    /// the doorbell (remote changes) and `outboxObserver` (local writes the capture triggers queued),
    /// so a burst from either source — or a mix of both — coalesces into one drain pass instead of one
    /// per event. Cancelled on `stop()`.
    private func armDebouncedPoke() {
        debounceTask?.cancel()
        debounceTask = Task { [debounceInterval, weak self] in
            try? await Task.sleep(for: .seconds(debounceInterval))
            guard !Task.isCancelled else { return }
            await self?.poke()
        }
    }

    /// Periodic convergence: pokes every `pollInterval` while healthy, or after `backoffDelay` once
    /// syncs start failing. This is what guarantees convergence if the Realtime doorbell goes silent.
    private func periodicLoop() async {
        while !Task.isCancelled {
            let delay = nextDelay(consecutiveFailures: consecutiveFailures, pollInterval: pollInterval)
            try? await Task.sleep(for: .seconds(delay))
            guard !Task.isCancelled else { break }
            poke()
        }
    }

    /// Requests an immediate sync through the serialised runner — for app-driven triggers like
    /// returning to the foreground or pull-to-refresh. Coalesces with any in-flight run; a no-op
    /// until `start()` has been called.
    public func syncNow() {
        poke()
    }

    private func poke() {
        wake?.yield(())
    }

    /// One full sync pass: push local changes then pull remote ones, driving `status` across the
    /// run (`.syncing` → `.idle`/`.degraded` on success with `lastSyncedAt` stamped, or `.failed` then
    /// rethrow).
    /// The scheduler serialises these, so a push and pull never overlap.
    ///
    /// The settled status still carries `failedUploads`/`deadLetters`: a drain can complete (so the
    /// *pass* is over) while individual entries are still failing or parked — which settles as
    /// `.degraded`, not `.idle` (issue #51). Both counts are read from the DB — not the drain pass — so
    /// an entry skipped inside its per-entry backoff window still surfaces as failing rather than
    /// flapping to healthy (APPS-507). Ask `status.isHealthy`, never the phase alone (APPS-470).
    @discardableResult
    func runSyncOnce(now: Date = Date()) async throws -> SyncStatus {
        // Keep the Realtime doorbell filtered on the current signed-in user: scope() is resolved
        // every pass, so an auth change (sign-in, user switch) re-subscribes the doorbell to the
        // right partition here — before the pull — without a stop/start (APPS-509).
        updateDoorbellScope(await scope())
        log.debug("sync pass starting")
        statusBroadcaster.send(transitionalStatus(.syncing))
        var resyncDeferred = false
        do {
            // Stale-cursor guard (APPS-471): if this device is past the offline horizon, full-resync
            // before the normal push/pull so a stale cursor can't skip purged tombstones. Re-evaluated
            // every pass — run it when the check hasn't completed yet (baseline nil: first pass, or a
            // prior deferral) or when the device has since gone stale (a long-lived process offline past
            // the gap while alive — APPS-512). In steady state the baseline is recent, so this is just a
            // date comparison with no extra I/O.
            let needsResyncCheck = resyncBaseline.map { now.timeIntervalSince($0) > maxOfflineGap } ?? true
            if needsResyncCheck {
                // A completed check returns true; `false` = deferred (scoped tables, scope() still nil):
                // leave the check armed and don't advance the baseline / last_synced_at, so a relaunch or
                // later pass re-runs it once auth settles (APPS-501). A transient failure here throws and
                // re-checks next pass anyway.
                let checkCompleted = try await resyncIfStale(now: now)
                resyncDeferred = !checkCompleted
            }
            try await drainOutbox()
            try await pullNow()
        } catch is StopRequested {
            // stop() flipped `stopping` mid-pass; the pass unwound at a transaction boundary. Every
            // unit is transactional, so what already committed is consistent and the next start()
            // resumes from the advanced cursor. Leave the last settled status (don't flip to .failed)
            // and don't advance the sync baseline — this pass didn't complete (APPS-513).
            log.info("sync pass aborted for teardown")
            return SyncStatus(phase: .idle, lastSyncedAt: lastSyncedAt)
        } catch {
            log.error("sync pass failed: \(String(describing: error), privacy: .public)")
            statusBroadcaster.send(transitionalStatus(.failed(SyncFailure.classify(error))))
            throw error
        }
        lastSyncedAt = now
        // Persist and re-baseline only when the resync check actually completed. While it's deferred,
        // advancing either would make the device look freshly synced and disarm the stale check — in
        // this process and across relaunches — even though the scoped tables were never reconciled
        // (APPS-501). Re-baselining every completed pass keeps the APPS-512 re-arm cheap: `now - baseline`
        // stays small while the device syncs, and only crosses `maxOfflineGap` once it's been offline.
        if resyncDeferred {
            resyncBaseline = nil
        } else {
            try await writeLastSyncedAt(now)
            resyncBaseline = now
        }
        // Settle both counts from the live outbox, not the drain pass just run. The drain skips
        // entries still inside their per-entry backoff window without counting them, so `outcome.failed`
        // alternates 1 → 0 across passes for a single persistently-failing entry — reading healthy
        // while the write is still failing. An entry with `attempts > 0` that isn't dead-lettered is,
        // by definition, still failing and retrying, whether or not this pass happened to re-attempt
        // it (APPS-507).
        let (failedUploads, deadLetters) = try await db.read { db -> (Int, Int) in
            let failing = try Int.fetchOne(
                db, sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE attempts > 0 AND dead_lettered = 0"
            ) ?? 0
            let dead = try Int.fetchOne(
                db, sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 1"
            ) ?? 0
            return (failing, dead)
        }
        // `.idle` only when both counts are clean; otherwise the pass settled *degraded* — it completed,
        // but writes are still outstanding. Deriving the phase here is what makes `.idle` mean healthy
        // for a consumer switching on it (issue #51).
        let status = SyncStatus(
            phase: SyncStatus.settledPhase(failedUploads: failedUploads, deadLetters: deadLetters),
            lastSyncedAt: lastSyncedAt,
            failedUploads: failedUploads, deadLetters: deadLetters
        )
        log.info("sync pass settled: failedUploads=\(failedUploads, privacy: .public) deadLetters=\(deadLetters, privacy: .public)")
        statusBroadcaster.send(status)
        return status
    }

    private func deadLetterCount() async throws -> Int {
        try await db.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 1") ?? 0
        }
    }

    // MARK: - Dead-letter repair (APPS-508)

    /// The parked outbox entries, oldest first — each a write that exhausted its retries (or failed
    /// permanently) and stopped uploading. Surfaces the `(table, pk, op)` and the `last_error`
    /// breadcrumb the `_sync_outbox` row carries, so the consumer can decide whether to fix the cause
    /// and `retryDeadLetters`, or `discardDeadLetters` and accept the server's version.
    public func deadLetters() async throws -> [DeadLetter] {
        try await db.read { db in
            try Row.fetchAll(
                db,
                sql: """
                    SELECT seq, table_name, pk, op, attempts, failure_kind, last_error, queued_at
                    FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 1 ORDER BY seq
                    """
            ).map { row in
                DeadLetter(
                    seq: row["seq"],
                    table: row["table_name"],
                    pk: row["pk"],
                    op: SyncOp(rawValue: row["op"]) ?? .upsert,
                    attempts: row["attempts"],
                    // Rebuilt from the stored kind, not re-derived from the message — the live Error is
                    // long gone by the time a consumer asks (issue #52).
                    failure: SyncFailure.decode(wire: row["failure_kind"], message: row["last_error"]),
                    lastError: row["last_error"],
                    queuedAt: row["queued_at"]
                )
            }
        }
    }

    /// Un-parks dead-lettered entries so the drain re-uploads them — for after the cause is fixed
    /// (an RLS/policy change, a schema migration, an app update). Clears `dead_lettered` and resets
    /// the retry bookkeeping (`attempts`, `last_attempt_at`, `last_error`, `failure_kind`) so the next
    /// drain attempts immediately rather than inside a stale backoff window, and so a later failure
    /// can't be read through the stale classification of the one just repaired. Pass specific `seqs`,
    /// or nil for all.
    /// Refreshes the broadcast status (so `deadLetters` drops at once) and pokes a drain.
    public func retryDeadLetters(_ seqs: [Int64]? = nil) async throws {
        try await db.write { db in
            var sql = """
                UPDATE \(SyncSchema.outboxTable)
                SET dead_lettered = 0, attempts = 0, last_attempt_at = NULL,
                    last_error = NULL, failure_kind = NULL
                WHERE dead_lettered = 1
                """
            var arguments = StatementArguments()
            if let seqs {
                guard !seqs.isEmpty else { return }
                let placeholders = seqs.map { _ in "?" }.joined(separator: ", ")
                sql += " AND seq IN (\(placeholders))"
                arguments = StatementArguments(seqs)
            }
            try db.execute(sql: sql, arguments: arguments)
        }
        try await broadcastStatusRefresh()
        poke() // drain the re-queued entries promptly
    }

    /// Discards dead-lettered entries and converges their rows back to the server's version — for
    /// when the local write is the one to abandon. Deletes the parked entries, drops each affected
    /// local row (unless a newer, still-pending write for it remains), and clears the affected
    /// tables' cursors so the next pull re-fetches from scratch — otherwise the advanced cursor would
    /// skip the server rows that were passed over while the entry was dirty (APPS-505). Pass specific
    /// `seqs`, or nil for all. Refreshes the broadcast status and pokes a pull.
    public func discardDeadLetters(_ seqs: [Int64]? = nil) async throws {
        // Which parked entries are we discarding, and which `(table, pk)` do they touch? Dead-letter
        // sets are small (personal scale), so fetch all parked and filter in memory.
        let parked = try await db.read { db -> [(seq: Int64, table: String, pk: String)] in
            try Row.fetchAll(
                db, sql: "SELECT seq, table_name, pk FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 1"
            ).map { (seq: $0["seq"] as Int64, table: $0["table_name"] as String, pk: $0["pk"] as String) }
        }
        let targets: [(seq: Int64, table: String, pk: String)]
        if let seqs {
            let wanted = Set(seqs)
            targets = parked.filter { wanted.contains($0.seq) }
        } else {
            targets = parked
        }
        guard !targets.isEmpty else { return }

        let targetSeqs = targets.map { $0.seq }
        let affectedTables = Set(targets.map { $0.table })
        // Resolve each target's local-row coordinates now, so the write closure captures only value
        // types (not the actor) — an unknown table simply has no local row to reset.
        let rowResets: [(table: String, primaryKey: String, pk: String)] = targets.compactMap { target in
            tables.first { $0.name == target.table }.map { (table: $0.name, primaryKey: $0.primaryKey, pk: target.pk) }
        }
        try await db.write { db in
            try Self.deleteEntries(db, seqs: targetSeqs)
            // Drop each affected local row so a re-pull can overwrite it with the server's copy (an
            // absent local row always loses LWW, so the server version applies cleanly). A row that
            // still has a live, non-parked outbox entry is left alone — that pending write, and its
            // dirty-gate protection, must survive the discard. The guard keys specifically on a
            // *non-parked* sibling (`dead_lettered = 0`): a lingering parked sibling has stopped
            // retrying, so resetting its row to the server's version is the correct convergence, not
            // a clobbered pending write (APPS-508 follow-up).
            // Suppressed: dropping the row is "accept the server's version", so it must not queue a
            // delete that would propagate the abandoned write after all (issue #48).
            try SyncTriggers.applyingRemoteState(db) {
                for reset in rowResets {
                    try db.execute(
                        sql: """
                            DELETE FROM "\(reset.table)" WHERE "\(reset.primaryKey)" = ?
                            AND NOT EXISTS (
                                SELECT 1 FROM \(SyncSchema.outboxTable)
                                WHERE table_name = ? AND pk = ? AND dead_lettered = 0
                            )
                            """,
                        arguments: [reset.pk, reset.table, reset.pk]
                    )
                }
            }
            // Clear the affected tables' cursors so the next pull re-fetches them from scratch,
            // reinstating server rows the advanced cursor would otherwise skip (APPS-505).
            let placeholders = affectedTables.map { _ in "?" }.joined(separator: ", ")
            try db.execute(
                sql: "DELETE FROM \(SyncSchema.stateTable) WHERE table_name IN (\(placeholders))",
                arguments: StatementArguments(Array(affectedTables))
            )
        }
        try await broadcastStatusRefresh()
        poke() // re-pull so the discarded rows converge to the server's version
    }

    /// Re-broadcasts status after a repair mutation so `deadLetters` reflects the new count at once,
    /// rather than waiting for the next sync pass. `failedUploads` resets to 0 here; the drain the
    /// caller pokes re-derives it on its next pass.
    private func broadcastStatusRefresh() async throws {
        let deadLetters = try await deadLetterCount()
        statusBroadcaster.send(SyncStatus(
            phase: SyncStatus.settledPhase(failedUploads: 0, deadLetters: deadLetters),
            lastSyncedAt: lastSyncedAt,
            failedUploads: 0, deadLetters: deadLetters
        ))
    }

    /// Writes one row and lets the table's capture trigger queue the upload — a thin compatibility
    /// shim over what is now a plain database write (issue #48).
    ///
    /// Prefer writing GRDB directly; the triggers pick it up either way, and a direct write can do
    /// what this signature can't — partial updates, `UPDATE … SET count = count + 1`, batch
    /// statements, raw SQL, and (most usefully) several rows across several tables in **one**
    /// transaction, so "create a recipe + 12 ingredients" is one atomic user intent rather than
    /// thirteen separate transactions:
    ///
    /// ```swift
    /// try await db.write { db in
    ///     try recipe.save(db)
    ///     for ingredient in ingredients { try ingredient.save(db) }
    /// }
    /// ```
    ///
    /// Supported row value types: `String`, numbers, `Bool`, `Date` (encoded as canonical ISO-8601,
    /// APPS-475), `null`, and — for columns declared in `jsonColumns` — nested objects/arrays.
    /// `Data`/blob fields are **not** supported and throw `SyncError.encoding`.
    @available(*, deprecated, message: """
        Write to the database directly — SQLite triggers now capture every write into the outbox. \
        `enqueue(.upsert, table: t, row: r)` becomes `try await db.write { try r.save($0) }`; \
        `enqueue(.delete, …)` becomes a plain DELETE (declare ON DELETE CASCADE for children).
        """)
    public func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws {
        guard let spec = tables.first(where: { $0.name == table }) else {
            throw SyncError.unknownTable(table)
        }
        let columns = try RowCoding.encode(row, jsonColumns: Set(spec.jsonColumns))
        guard let pkValue = columns[spec.primaryKey], !pkValue.isNull else {
            throw SyncError.missingPrimaryKey(table: table, column: spec.primaryKey)
        }
        // No outbox insert and no poke here any more: the table's trigger records the op in this same
        // transaction, and `outboxObserver` wakes the runner when it commits.
        try db.write { db in
            switch op {
            case .upsert:
                try RowCoding.upsertLocalRow(db, table: table, primaryKey: spec.primaryKey, columns: columns)
            case .delete:
                try db.execute(
                    sql: "DELETE FROM \"\(table)\" WHERE \"\(spec.primaryKey)\" = ?",
                    arguments: [pkValue]
                )
            }
        }
    }

    /// Pulls rows changed since each table's `(updated_at, id)` cursor and applies them
    /// last-write-wins. Upserts run parents-first (so a child's FK target exists before the child);
    /// tombstones are deferred and applied children-first (so a parent is never deleted out from
    /// under a child). Each page's applies + cursor-advance happen in one transaction; pages are
    /// pulled until one comes back short.
    /// Returns the set of primary keys each table's fetch returned this pull. On a full pull (cursors
    /// cleared, as the stale-cursor resync does) that's every pk the server currently holds — the
    /// input the resync reconcile diffs against local rows (APPS-471). On an incremental pull it's
    /// just the changed pks, which the normal path ignores.
    ///
    /// A table that was **skipped** this pass (scoped, no partition value) has no entry at all —
    /// distinct from an empty set, which means its fetch completed and the server returned zero
    /// rows. The resync reconcile relies on that distinction to avoid treating "not pulled" as
    /// "server holds nothing" (APPS-501).
    @discardableResult
    public func pullNow() async throws -> [String: Set<String>] {
        let order = topologicalOrder(tables)
        var pendingDeletes: [PendingDelete] = []
        var seenPks: [String: Set<String>] = [:]

        for tableName in order {
            try await abortIfStopping(pendingDeletes, order: order) // teardown: bail between tables (APPS-513)
            guard let spec = tables.first(where: { $0.name == tableName }) else { continue }
            var scopeFilter: ScopeFilter?
            if let scopeColumn = spec.scopeColumn {
                // Scoped table: resolve the partition value for the signed-in user. Signed out (nil)
                // → skip the table this pass rather than pull it unfiltered (which would download the
                // whole public catalog); the periodic loop retries once a value is available.
                guard let value = await scope() else {
                    // Surface the skip — otherwise a persistently-nil scope silently hides a real bug
                    // (e.g. the app never wiring up scope()) as an apparently-healthy, empty sync (APPS-514).
                    log.notice("pull: skipping scoped table \(spec.name, privacy: .public) — no partition value")
                    continue
                }
                scopeFilter = ScopeFilter(column: scopeColumn, value: value)
            }
            seenPks[spec.name] = [] // mark the table as fetched — a skipped table has no entry
            var cursor = try await readCursor(table: spec.name)
            // One introspection per table per pass: the columns the local schema actually has, so we
            // drop wire columns a server that migrated ahead added but this app build lacks — an old
            // client silently doesn't see the new column instead of bricking every pull (APPS-504).
            let knownColumns = try await db.read { db in try RowCoding.tableColumns(db, table: spec.name) }
            while true {
                try await abortIfStopping(pendingDeletes, order: order) // teardown: bail between pages (APPS-513)
                let page = try await remote.fetch(
                    table: spec.name, cursorColumn: spec.cursorColumn, since: cursor,
                    primaryKey: spec.primaryKey, scope: scopeFilter, limit: pageSize
                )
                if page.isEmpty { break }

                for row in page where !(row[spec.primaryKey]?.stringValue ?? "").isEmpty {
                    seenPks[spec.name, default: []].insert(row[spec.primaryKey]!.stringValue!)
                }

                let pageCursor = cursor // immutable copy for the Sendable write closure
                let (advanced, deletes) = try await db.write { db -> (SyncCursor?, [PendingDelete]) in
                    // Downloaded rows must not re-enqueue themselves: the capture triggers stay inert
                    // for the whole page apply (issue #48).
                    try SyncTriggers.applyingRemoteState(db) { () throws -> (SyncCursor?, [PendingDelete]) in
                        var advanced = pageCursor
                        var deferred: [PendingDelete] = []
                        for row in page {
                            let pk = row[spec.primaryKey]?.stringValue ?? ""
                            let updatedAt = row[spec.cursorColumn]?.stringValue ?? ""
                            let tombstoned = !(row["deletedAt"]?.isNil ?? true)
                            if try Self.lwwAllows(
                                db, table: spec.name, cursorColumn: spec.cursorColumn,
                                primaryKey: spec.primaryKey, pk: pk, remoteUpdatedAt: updatedAt
                            ) {
                                if tombstoned {
                                    deferred.append(PendingDelete(
                                        table: spec.name, primaryKey: spec.primaryKey,
                                        cursorColumn: spec.cursorColumn, pk: pk, updatedAt: updatedAt
                                    ))
                                } else {
                                    try RowCoding.upsertLocalRow(
                                        db, table: spec.name, primaryKey: spec.primaryKey,
                                        columns: RowCoding.localColumns(from: row),
                                        restrictingTo: knownColumns
                                    )
                                }
                            }
                            advanced = SyncCursor(updatedAt: updatedAt, id: pk) // cursor advances whether or not we applied
                        }
                        if let advanced { try Self.writeCursor(db, table: spec.name, cursor: advanced) }
                        return (advanced, deferred)
                    }
                }
                if let advanced { cursor = advanced }
                pendingDeletes.append(contentsOf: deletes)
                if page.count < pageSize { break }
            }
        }

        // Phase 2: tombstones, children-first (reverse FK order).
        try await applyDeferredDeletes(pendingDeletes, order: order)
        return seenPks
    }

    /// Applies deferred tombstones children-first (reverse FK order), re-checking the LWW gate in case
    /// a local edit landed between phases. Factored out of `pullNow` so the cooperative-teardown
    /// checkpoint can flush the tombstones deferred so far before bailing: the pull's cursor has
    /// already advanced past those tombstoned rows, so leaving them un-applied would keep the rows
    /// locally and never re-fetch them (APPS-513).
    private func applyDeferredDeletes(_ pendingDeletes: [PendingDelete], order: [String]) async throws {
        guard !pendingDeletes.isEmpty else { return }
        let rank = Dictionary(uniqueKeysWithValues: order.enumerated().map { ($1, $0) })
        for del in pendingDeletes.sorted(by: { (rank[$0.table] ?? 0) > (rank[$1.table] ?? 0) }) {
            try await db.write { db in
                // A tombstone the server already holds — applying it must not queue a delete straight
                // back at the server (issue #48). Any local `ON DELETE CASCADE` children it removes are
                // suppressed with it, and the server's own child tombstones arrive on their own tables.
                try SyncTriggers.applyingRemoteState(db) {
                    // Re-check the gate: a local edit may have arrived between phases.
                    guard try Self.lwwAllows(
                        db, table: del.table, cursorColumn: del.cursorColumn,
                        primaryKey: del.primaryKey, pk: del.pk, remoteUpdatedAt: del.updatedAt
                    ) else { return }
                    try db.execute(
                        sql: "DELETE FROM \"\(del.table)\" WHERE \"\(del.primaryKey)\" = ?",
                        arguments: [del.pk]
                    )
                }
            }
        }
    }

    /// Cooperative-teardown checkpoint for the pull loop (APPS-513). When `stop()` has begun, flush the
    /// tombstones deferred so far — so the advanced cursor doesn't outrun them — then unwind via
    /// `StopRequested`. Called between pages and between tables, bounding teardown to a single page
    /// rather than the whole (possibly first-ever / full-resync) pull.
    private func abortIfStopping(_ pendingDeletes: [PendingDelete], order: [String]) async throws {
        guard stopping else { return }
        try await applyDeferredDeletes(pendingDeletes, order: order)
        throw StopRequested()
    }

    /// Full-resyncs when this device has been offline past `maxOfflineGap` — its cursor may point
    /// past tombstones the server has since purged, so it would keep deleted rows (and re-upload
    /// dirty ones) forever. No-op on a fresh install (no recorded sync) or a recently-synced device.
    ///
    /// Returns whether the check **completed**. `false` means the device is stale but the resync
    /// was deferred: a scoped table needs a partition value and `scope()` is still nil (cold launch
    /// before session restoration, or signed out). Resyncing then would skip those tables' pulls
    /// and reconcile against "nothing seen", wiping their local rows — so the caller must keep the
    /// check armed and retry on a later pass (APPS-501).
    func resyncIfStale(now: Date = Date()) async throws -> Bool {
        guard let last = try await readLastSyncedAt(), now.timeIntervalSince(last) > maxOfflineGap else { return true }
        if tables.contains(where: { $0.scopeColumn != nil }), await scope() == nil {
            log.notice("stale cursor: resync deferred — no partition value yet (signed out / session not restored)")
            return false
        }
        log.notice("stale cursor: full resync — offline \(Int(now.timeIntervalSince(last)), privacy: .public)s exceeds maxOfflineGap \(Int(self.maxOfflineGap), privacy: .public)s")
        try await fullResync()
        return true
    }

    /// Clears every cursor, re-pulls all tables from scratch, and reconciles: drops local rows the
    /// server no longer has (purged deletes) — **except** rows with a pending outbox entry, which are
    /// local writes the drain will upload. Keeping dirty rows can resurrect a row deleted elsewhere
    /// beyond the purge horizon; for single-user LWW that honours the user's own pending edit.
    ///
    /// Only tables whose full fetch completed reconcile. A table the pull skipped (scoped, no
    /// partition value) has no `seen` entry — reconciling it against an empty set would delete
    /// every non-dirty local row (APPS-501).
    func fullResync() async throws {
        try await db.write { db in try db.execute(sql: "DELETE FROM \(SyncSchema.stateTable)") }
        let seen = try await pullNow() // full pull (cursors cleared) → every pk the server holds
        for spec in tables {
            if stopping { throw StopRequested() } // teardown: bail between reconcile steps (APPS-513)
            guard let seenPks = seen[spec.name] else { continue }
            let (localPks, dirtyPks) = try await db.read { db -> (Set<String>, Set<String>) in
                let local = Set(try String.fetchAll(db, sql: "SELECT \"\(spec.primaryKey)\" FROM \"\(spec.name)\""))
                let dirty = Set(try String.fetchAll(
                    db, sql: "SELECT pk FROM \(SyncSchema.outboxTable) WHERE table_name = ?", arguments: [spec.name]
                ))
                return (local, dirty)
            }
            let toDelete = localPks.subtracting(seenPks).subtracting(dirtyPks)
            guard !toDelete.isEmpty else { continue }
            try await db.write { db in
                // These rows are already gone server-side (purged tombstones) — dropping them locally
                // is convergence, not a user delete, so the capture triggers stay inert (issue #48).
                try SyncTriggers.applyingRemoteState(db) {
                    // ponytail: single IN-list. Personal-scale divergence is small; chunk only if a table
                    // ever diverges by more than SQLite's ~32k variable limit.
                    let placeholders = toDelete.map { _ in "?" }.joined(separator: ", ")
                    try db.execute(
                        sql: "DELETE FROM \"\(spec.name)\" WHERE \"\(spec.primaryKey)\" IN (\(placeholders))",
                        arguments: StatementArguments(Array(toDelete))
                    )
                }
            }
        }
    }

    private func readLastSyncedAt() async throws -> Date? {
        try await db.read { db in
            guard let value = try String.fetchOne(
                db, sql: "SELECT value FROM \(SyncSchema.metaTable) WHERE key = 'last_synced_at'"
            ) else { return nil }
            return SyncTimestamp.date(from: value)
        }
    }

    private func writeLastSyncedAt(_ date: Date) async throws {
        try await db.write { db in
            try db.execute(
                sql: """
                    INSERT INTO \(SyncSchema.metaTable) (key, value) VALUES ('last_synced_at', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                arguments: [SyncTimestamp.string(from: date)]
            )
        }
    }

    /// Last-write-wins gate, evaluated inside the apply transaction: a remote row is applied only
    /// if the local row isn't dirty (no pending outbox entry — its queued upload wins) and the
    /// remote is strictly newer than the local copy (an absent local row always loses).
    private static func lwwAllows(
        _ db: Database, table: String, cursorColumn: String, primaryKey: String, pk: String, remoteUpdatedAt: String
    ) throws -> Bool {
        // A dead-lettered entry is excluded: it has stopped retrying, so it must not keep the row
        // dirty and block every remote update forever (APPS-470).
        let dirty = try Int.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE table_name = ? AND pk = ? AND dead_lettered = 0",
            arguments: [table, pk]
        ) ?? 0
        if dirty > 0 { return false }

        guard let local: String = try String.fetchOne(
            db,
            sql: "SELECT \"\(cursorColumn)\" FROM \"\(table)\" WHERE \"\(primaryKey)\" = ?",
            arguments: [pk]
        ) else {
            return true // no local row (or never-stamped) → apply
        }
        // Compare the two instants at microsecond precision: PostgREST (`…+00:00`, microseconds),
        // client (`…123Z`), and non-fractional legacy strings otherwise sort inconsistently, and a
        // string compare truncates to milliseconds — so two writes in the same millisecond would tie
        // and the newer be skipped forever (APPS-474, APPS-511). Strict `>`; an equal instant (an own
        // upload echo) doesn't re-apply. See contract §4.
        return SyncTimestamp.isStrictlyNewer(remoteUpdatedAt, than: local)
    }

    private func readCursor(table: String) async throws -> SyncCursor? {
        try await db.read { db in
            guard let row = try Row.fetchOne(
                db,
                sql: "SELECT updated_at, last_id FROM \(SyncSchema.stateTable) WHERE table_name = ?",
                arguments: [table]
            ), let updatedAt: String = row["updated_at"], let id: String = row["last_id"] else {
                return nil
            }
            return SyncCursor(updatedAt: updatedAt, id: id)
        }
    }

    private static func writeCursor(_ db: Database, table: String, cursor: SyncCursor) throws {
        try db.execute(
            sql: """
                INSERT INTO \(SyncSchema.stateTable) (table_name, updated_at, last_id) VALUES (?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET updated_at = excluded.updated_at, last_id = excluded.last_id
                """,
            arguments: [table, cursor.updatedAt, cursor.id]
        )
    }

    /// Result of one outbox drain pass (APPS-470): `failed` entries hit a transient error and are
    /// still being retried with backoff; `deadLettered` entries were parked this pass (permanent
    /// failure, or the transient retry cap reached) and will not be retried again.
    struct DrainOutcome: Sendable, Equatable {
        var failed: Int
        var deadLettered: Int
    }

    /// Processes every pending (non-parked) outbox entry once, in FK-safe order. Each op is
    /// idempotent by primary key. A failed entry stays in place with `attempts`/`last_attempt_at`
    /// bumped and is skipped until its per-entry backoff window elapses; one that fails permanently
    /// (4xx) or exhausts `deadLetterAfter` retries is dead-lettered (parked). One failure never
    /// blocks the others.
    @discardableResult
    func drainOutbox(now: Date = Date()) async throws -> DrainOutcome {
        let pending = try await db.read { db in
            try OutboxEntry.fetchAll(db, sql: "SELECT * FROM \(SyncSchema.outboxTable) WHERE dead_lettered = 0")
        }
        // Net each (table, pk) to one op first (APPS-472), then FK-order the net ops.
        let collapsed = collapseOutbox(pending)
        let groupSeqs = Dictionary(uniqueKeysWithValues: collapsed.map { ($0.net.seq, $0.seqs) })
        var outcome = DrainOutcome(failed: 0, deadLettered: 0)
        for entry in orderForUpload(collapsed.map(\.net), tables: tables) {
            if stopping { throw StopRequested() } // teardown: bail between entries — each clear is transactional (APPS-513)
            // Per-entry exponential backoff: skip an entry still inside its retry window so a failing
            // entry isn't re-attempted on every drain pass (and doorbell ring and poll).
            if let last = entry.lastAttemptAt, now.timeIntervalSince(last) < backoffDelay(attempts: entry.attempts) {
                continue
            }
            let seqs = groupSeqs[entry.seq] ?? [entry.seq]
            do {
                try await process(entry, clearing: seqs)
            } catch {
                // Auth-shaped failures (expired/refreshing token) recover out-of-band, so retry them
                // without charging the retry budget — a token-refresh stretch mustn't dead-letter
                // healthy writes and strip their dirty-row protection (APPS-502). Genuine transient
                // failures still count toward `deadLetterAfter`; permanent ones park at once.
                let attempts = remoteErrorIsAuthTransient(error) ? entry.attempts : entry.attempts + 1
                // Permanent (4xx / constraint / RLS) → park now; transient → park once it exhausts the cap.
                let park = remoteErrorIsPermanent(error) || attempts >= deadLetterAfter
                try await recordFailure(entry, groupSeqs: seqs, attempts: attempts, now: now, park: park, error: error)
                if park { outcome.deadLettered += 1 } else { outcome.failed += 1 }
            }
        }
        return outcome
    }

    /// Records a failed upload attempt: bumps `attempts`, stamps `last_attempt_at` (for the backoff
    /// window) and `last_error` (telemetry) on the net entry. When parking, the **whole collapsed
    /// group** is dead-lettered together — else a superseded older op (e.g. an orphaned delete)
    /// could become the net op on a later drain and mis-apply — and the table's download cursor is
    /// reset so the next pull re-fetches whatever remote version the LWW gate skipped while the row
    /// was dirty (APPS-505).
    private func recordFailure(_ entry: OutboxEntry, groupSeqs: [Int64], attempts: Int, now: Date, park: Bool, error: Error) async throws {
        // Classify here, while the live Error still exists — `DeadLetter` is rebuilt from this row long
        // after it's gone, and prose can't be reclassified after the fact (issue #52).
        let kind = SyncFailure.classify(error).wireValue
        try await db.write { db in
            try db.execute(
                sql: """
                    UPDATE \(SyncSchema.outboxTable)
                    SET attempts = ?, last_attempt_at = ?, last_error = ?, failure_kind = ?, dead_lettered = ?
                    WHERE seq = ?
                    """,
                arguments: [attempts, now, "\(error)", kind, park ? 1 : 0, entry.seq]
            )
            if park {
                let others = groupSeqs.filter { $0 != entry.seq }
                if !others.isEmpty {
                    let placeholders = others.map { _ in "?" }.joined(separator: ", ")
                    try db.execute(
                        sql: "UPDATE \(SyncSchema.outboxTable) SET dead_lettered = 1, last_error = ?, failure_kind = ? WHERE seq IN (\(placeholders))",
                        arguments: StatementArguments(["\(error)", kind] as [any DatabaseValueConvertible] + others.map { $0 as any DatabaseValueConvertible })
                    )
                }
                try Self.resetCursor(db, table: entry.tableName)
            }
        }
        if park {
            // Diagnostic breadcrumb for a parked write — table/pk/op/attempts + classified error, never
            // the row payload (user content). pk is redacted in release by the default privacy (APPS-514).
            log.error("dead-lettered \(entry.tableName, privacy: .public) pk=\(entry.pk) op=\(entry.op.rawValue, privacy: .public) attempts=\(attempts, privacy: .public): \(String(describing: error), privacy: .public)")
        }
    }

    /// Clears a table's download cursor so the next pull re-scans it from scratch. Invoked when an
    /// entry dead-letters: while its row was dirty, `lwwAllows` skipped every remote version of that
    /// row (the queued upload was meant to win), yet the cursor still advanced past them. Once the
    /// entry parks the row stops being dirty, so those skipped versions now sit *behind* the cursor
    /// and would never be re-fetched — leaving the local row permanently diverged from the server
    /// (APPS-505). Re-scanning the whole table is cheap at personal scale, and LWW re-applies only
    /// the versions that are genuinely newer than the local row. The future dead-letter repair API
    /// should call this same reset when it discards an entry (APPS-508).
    private static func resetCursor(_ db: Database, table: String) throws {
        try db.execute(
            sql: "DELETE FROM \(SyncSchema.stateTable) WHERE table_name = ?",
            arguments: [table]
        )
    }

    /// Applies one collapsed op (the net op for a `(table, pk)`) and, on success, clears **every**
    /// `seqs` entry the group collapsed (APPS-472) — not just the net one.
    private func process(_ entry: OutboxEntry, clearing seqs: [Int64]) async throws {
        guard let spec = tables.first(where: { $0.name == entry.tableName }) else {
            try await clear(seqs) // table no longer synced — drop the stale entries
            return
        }
        switch entry.op {
        case .upsert:
            guard let payload = try await readPayload(spec: spec, pk: entry.pk) else {
                try await clear(seqs) // row gone locally before upload — nothing to send
                return
            }
            let onConflict = spec.conflictColumns.isEmpty ? nil : spec.conflictColumns.joined(separator: ",")
            let server = try await remote.upsert(table: spec.name, row: payload, onConflict: onConflict)
            try await stampAndClear(entry, spec: spec, server: server, seqs: seqs)
        case .delete:
            try await remote.delete(table: spec.name, primaryKey: spec.primaryKey, pk: entry.pk)
            try await clear(seqs)
        }
    }

    private func readPayload(spec: SyncTable, pk: String) async throws -> [String: AnyJSON]? {
        try await db.read { db in
            guard let row = try Row.fetchOne(
                db,
                sql: "SELECT * FROM \"\(spec.name)\" WHERE \"\(spec.primaryKey)\" = ?",
                arguments: [pk]
            ) else { return nil }
            return RowCoding.payload(
                from: row,
                jsonColumns: Set(spec.jsonColumns),
                excluding: spec.uploadExcludedColumns,
                restrictingTo: spec.serverColumns.isEmpty ? nil : Set(spec.serverColumns)
            )
        }
    }

    /// Writes the server's representation back to the local row and clears the collapsed group's
    /// entries in one transaction, so the row is marked clean (its cursor won't re-pull it) only
    /// after the server confirms.
    ///
    /// The representation carries what the client can't compute locally: column **defaults** the
    /// server filled in, **trigger-normalized** fields, recomputed `serverOwnedColumns`, and — for a
    /// `conflictColumns` upsert — the **merged** server row re-keyed to the client's pk (APPS-478).
    /// Stamping only the cursor column discarded all of it, and the next pull couldn't repair it:
    /// local `updatedAt` now equals the server's, so `lwwAllows` sees remote == local (not strictly
    /// newer) and skips the row forever — the writing device was the one device never to see the
    /// server's version of its own write (APPS-506). Unknown wire columns are dropped, mirroring the
    /// download path (APPS-504).
    ///
    /// The write-back is skipped when a *newer* outbox entry exists for this pk (one enqueued after
    /// the `seqs` we're clearing): the user edited the row mid-flight and the local copy already
    /// holds that pending edit — the same dirty-check the pull's `lwwAllows` gate uses. Clobbering it
    /// would lose the edit; it uploads on the next drain instead. The confirmed group's entries clear
    /// either way.
    private func stampAndClear(_ entry: OutboxEntry, spec: SyncTable, server: [String: AnyJSON], seqs: [Int64]) async throws {
        try await db.write { db in
            // The server's own representation of the row we just uploaded — suppressed, or writing it
            // back would queue the very upload it confirms and the drain would never settle (issue #48).
            try SyncTriggers.applyingRemoteState(db) {
                if try Self.writeBackAllowed(db, table: spec.name, pk: entry.pk, excluding: seqs) {
                    try RowCoding.upsertLocalRow(
                        db, table: spec.name, primaryKey: spec.primaryKey,
                        columns: RowCoding.localColumns(from: server),
                        restrictingTo: try RowCoding.tableColumns(db, table: spec.name)
                    )
                }
            }
            try Self.deleteEntries(db, seqs: seqs)
        }
    }

    /// Whether the server representation may overwrite the local row: false when a live (non
    /// dead-lettered) outbox entry exists for this pk *other than* the `seqs` being cleared — a
    /// mid-flight edit whose pending upload must win. Mirrors `lwwAllows`' dirty-check on the pull
    /// path; the excluded `seqs` are the confirmed group, still present until `deleteEntries` runs.
    private static func writeBackAllowed(_ db: Database, table: String, pk: String, excluding seqs: [Int64]) throws -> Bool {
        let placeholders = seqs.map { _ in "?" }.joined(separator: ", ")
        let notCleared = seqs.isEmpty ? "" : " AND seq NOT IN (\(placeholders))"
        let dirty = try Int.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable) WHERE table_name = ? AND pk = ? AND dead_lettered = 0\(notCleared)",
            arguments: StatementArguments([table, pk] as [any DatabaseValueConvertible] + seqs.map { $0 as any DatabaseValueConvertible })
        ) ?? 0
        return dirty == 0
    }

    private func clear(_ seqs: [Int64]) async throws {
        try await db.write { db in try Self.deleteEntries(db, seqs: seqs) }
    }

    private static func deleteEntries(_ db: Database, seqs: [Int64]) throws {
        guard !seqs.isEmpty else { return }
        let placeholders = seqs.map { _ in "?" }.joined(separator: ", ")
        try db.execute(
            sql: "DELETE FROM \(SyncSchema.outboxTable) WHERE seq IN (\(placeholders))",
            arguments: StatementArguments(seqs)
        )
    }

}
