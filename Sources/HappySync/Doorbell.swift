import Foundation
import Supabase

/// The Realtime "doorbell": rings once per relevant remote change so the engine knows to pull.
/// Payloads are deliberately discarded — a ring only triggers a debounced `pullNow()`, and all
/// correctness stays in the idempotent cursor-pull. Abstracted behind a protocol so the scheduler
/// can be driven by a fake in tests; the production conformance is the only place touching Realtime.
protocol SyncDoorbell: Sendable {
    /// A stream that yields `()` whenever a watched row changes, filtered to the partition `scope`
    /// (the signed-in user's uid, or nil when signed out). The engine re-invokes this with a new
    /// scope on an auth change, terminating the previous stream first — so scoped tables ring for
    /// the current user even when `start()` preceded sign-in (APPS-509).
    func ring(scope: String?) -> AsyncStream<Void>
}

/// A doorbell that never rings — the default when no Realtime client is wired. The engine still
/// converges through its periodic pull, so this is a safe fallback rather than a broken one.
struct SilentDoorbell: SyncDoorbell {
    func ring(scope: String?) -> AsyncStream<Void> { AsyncStream { _ in } }
}

/// `SyncDoorbell` over a Supabase Realtime channel. Subscribes to `postgres_changes` for the
/// synced tables (RLS scopes events to the user's own rows) and rings on any insert/update/delete.
/// The partition value (uid) is passed in per subscription rather than resolved here, so the engine
/// can re-`ring()` with a new value when auth changes (APPS-509).
///
/// ponytail: not unit-tested — the doorbell only *triggers* a pull, and the pull (which carries all
/// correctness) is covered with a fake. RealtimeClientV2 handles reconnect + auth-token refresh, so
/// a dropped socket re-subscribes itself; the engine's periodic pull covers any gap meanwhile.
struct SupabaseDoorbell: SyncDoorbell {
    let client: SupabaseClient
    /// One entry per synced table, carrying its optional partition-scope column (APPS-469).
    let tables: [(name: String, scopeColumn: String?)]
    /// Stable per-engine channel-name base. Made unique per engine instance so two engines sharing
    /// one Supabase client don't collide (APPS-509); each `ring()` then appends a fresh per-ring
    /// token so successive subscriptions never share a topic (see `ring`). Build with
    /// `makeChannelName()`.
    let channelBase: String

    /// A unique-per-engine channel-name base: a stable prefix plus a random token. Two engines on
    /// one Supabase client subscribe to distinct Realtime channels instead of colliding (APPS-509).
    static func makeChannelName() -> String { "happysync-\(UUID().uuidString)" }

    func ring(scope uid: String?) -> AsyncStream<Void> {
        AsyncStream { continuation in
            // Derive a fresh topic per subscription (base + per-ring token). Reusing one per-engine
            // topic across re-scopes let the previous channel's async teardown (onTermination →
            // detached unsubscribe/removeChannel) race the next synchronous subscribe on the *same*
            // topic, leaving the doorbell mis-subscribed under rapid auth churn until the next
            // periodic pull recovered. A distinct topic per subscription makes teardown and the new
            // subscribe touch different channels, so they can't collide (APPS-509 follow-up).
            let topic = "\(channelBase)-\(UUID().uuidString)"
            // Create the channel here (synchronous) so `onTermination` can tear it down — otherwise
            // the socket keeps the channel joined until the process exits, and repeated start/stop
            // (or auth re-scope) cycles leak a channel each time (APPS-473).
            let channel = client.realtimeV2.channel(topic)
            let task = Task {
                // Listeners must be registered before subscribe(); any change rings the doorbell.
                var streams: [AsyncStream<AnyAction>] = []
                for table in tables {
                    if let column = table.scopeColumn {
                        // Scoped table: only ring for the user's own rows. Signed out → skip it (the
                        // periodic pull still converges). Subscribing unfiltered would ring on every
                        // public-catalog change and hammer the pull — the churn APPS-469 fixes.
                        guard let uid else { continue }
                        streams.append(channel.postgresChange(
                            AnyAction.self, schema: "public", table: table.name,
                            filter: Self.changeFilter(column: column, value: uid)
                        ))
                    } else {
                        streams.append(channel.postgresChange(AnyAction.self, schema: "public", table: table.name))
                    }
                }
                await channel.subscribe()
                await withTaskGroup(of: Void.self) { group in
                    for stream in streams {
                        group.addTask { for await _ in stream { continuation.yield(()) } }
                    }
                }
            }
            // On teardown, cancel the listener AND unsubscribe/remove the channel so the socket
            // doesn't keep it joined. Cleanup runs in a fresh (uncancelled) task so the async
            // unsubscribe actually completes. A later `ring()` builds a clean channel from scratch.
            continuation.onTermination = { [client] _ in
                task.cancel()
                Task { await channel.unsubscribe(); await client.realtimeV2.removeChannel(channel) }
            }
        }
    }

    /// PostgREST-style Realtime filter for a scoped table: `column=eq.value`.
    static func changeFilter(column: String, value: String) -> String { "\(column)=eq.\(value)" }
}
