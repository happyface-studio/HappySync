import Foundation

/// A one-shot async signal: `wait()` suspends until someone calls `fire()` (idempotent).
///
/// Sync is concurrent, so most of the interesting assertions are about *when* something happened —
/// that `stop()` didn't return while a pull was in flight, that a mid-upload edit survived the
/// server's write-back. A signal lets a test rendezvous with background work at an exact point
/// instead of sleeping and hoping:
///
/// ```swift
/// let started = AsyncSignal(), gate = AsyncSignal()
/// await remote.onUpsert { _ in await started.fire(); await gate.wait() }
/// let drain = Task { try await engine.syncNow() }
/// await started.wait()          // the upload is now in flight, blocked in the remote
/// try await db.write { … }      // race a local edit against it
/// await gate.fire()             // let the upload finish
/// ```
public actor AsyncSignal {
    private var fired = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    public init() {}

    /// Non-blocking peek — has `fire()` been called yet? Use it to assert something *hasn't*
    /// happened, e.g. that a `stop()` task is still suspended.
    public var isFired: Bool { fired }

    /// Releases every waiter, now and in future. Calling it twice is harmless.
    public func fire() {
        guard !fired else { return }
        fired = true
        for waiter in waiters { waiter.resume() }
        waiters.removeAll()
    }

    /// Suspends until `fire()` — returning immediately if it already happened, so a wait can't miss
    /// a signal that arrived first.
    public func wait() async {
        if fired { return }
        await withCheckedContinuation { waiters.append($0) }
    }
}

/// Polls `condition` every few milliseconds until it holds or `timeout` elapses; returns whether it
/// held.
///
/// The right tool for anything the engine converges to in the background — a pull landing, a status
/// reaching `.degraded`, a doorbell ring producing a fetch. A fixed sleep followed by a single
/// assertion is the tempting alternative and it flakes under a loaded CI runner: the window is
/// either too short (spurious failure) or padded so far that the suite crawls.
///
/// ```swift
/// #expect(await eventually { await remote.fetchCalls >= 1 })
/// ```
///
/// For a *negative* assertion ("no further pull happened") this is the wrong shape — poll until the
/// count stops moving, then assert it didn't move again.
public func eventually(
    timeout: Duration = .seconds(5),
    _ condition: @Sendable () async -> Bool
) async -> Bool {
    let deadline = ContinuousClock.now.advanced(by: timeout)
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return await condition()
}
