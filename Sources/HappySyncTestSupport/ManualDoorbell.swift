import Foundation
import os
import HappySync

/// A `SyncDoorbell` that rings only when a test tells it to — the Realtime channel, made
/// deterministic.
///
/// Each `ring(scope:)` the engine performs opens a fresh subscription and records the partition it
/// was asked to filter on, so a test can prove the engine re-subscribes on an auth change (APPS-509)
/// and tore the old subscription down. `fire()` then delivers an event on the current subscription,
/// the way a remote write would:
///
/// ```swift
/// let doorbell = ManualDoorbell()
/// let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: manifest, doorbell: doorbell)
/// await engine.start()
/// doorbell.fire()                       // a remote change lands
/// #expect(await eventually { await remote.fetchCalls >= 2 })
/// ```
///
/// Rings are debounced by the engine, so a burst of `fire()` calls inside one debounce window
/// collapses into a single pull — which is the behaviour worth asserting, not a bug to work around.
public final class ManualDoorbell: SyncDoorbell, @unchecked Sendable {
    private struct State {
        var scopes: [String?] = []                               // scope of each ring(), in order
        var continuations: [AsyncStream<Void>.Continuation] = []  // parallel to `scopes`
        var liveCount = 0                                         // subscriptions not yet torn down
    }
    private let state = OSAllocatedUnfairLock(initialState: State())

    public init() {}

    /// The scope of every subscription the engine has opened, in order — `[nil, "u1"]` proves a
    /// signed-out → signed-in re-scope happened without a `stop()`/`start()`.
    public var ringScopes: [String?] { state.withLock { $0.scopes } }

    /// Subscriptions currently live: opened by `ring(scope:)` and not yet cancelled. Settles to `1`
    /// after a re-scope, proving the previous channel was actually torn down rather than leaked.
    public var liveSubscriptions: Int { state.withLock { $0.liveCount } }

    public func ring(scope: String?) -> AsyncStream<Void> {
        AsyncStream { continuation in
            state.withLock {
                $0.scopes.append(scope)
                $0.continuations.append(continuation)
                $0.liveCount += 1
            }
            continuation.onTermination = { [state] _ in state.withLock { $0.liveCount -= 1 } }
        }
    }

    /// Rings the current (latest) subscription — a Realtime change for the signed-in user.
    public func fire() { state.withLock { $0.continuations.last }?.yield(()) }

    /// Rings a specific past subscription by index, to show that a torn-down one (the previous
    /// user's) no longer pokes the engine.
    public func fire(subscription index: Int) {
        state.withLock { index < $0.continuations.count ? $0.continuations[index] : nil }?.yield(())
    }
}
