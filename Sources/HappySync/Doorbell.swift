import Foundation
import Supabase

/// The Realtime "doorbell": rings once per relevant remote change so the engine knows to pull.
/// Payloads are deliberately discarded — a ring only triggers a debounced `pullNow()`, and all
/// correctness stays in the idempotent cursor-pull. Abstracted behind a protocol so the scheduler
/// can be driven by a fake in tests; the production conformance is the only place touching Realtime.
protocol SyncDoorbell: Sendable {
    /// A stream that yields `()` whenever a watched row changes. Consumed once by the engine.
    func ring() -> AsyncStream<Void>
}

/// A doorbell that never rings — the default when no Realtime client is wired. The engine still
/// converges through its periodic pull, so this is a safe fallback rather than a broken one.
struct SilentDoorbell: SyncDoorbell {
    func ring() -> AsyncStream<Void> { AsyncStream { _ in } }
}

/// `SyncDoorbell` over a Supabase Realtime channel. Subscribes to `postgres_changes` for the
/// synced tables (RLS scopes events to the user's own rows) and rings on any insert/update/delete.
///
/// ponytail: not unit-tested — the doorbell only *triggers* a pull, and the pull (which carries all
/// correctness) is covered with a fake. RealtimeClientV2 handles reconnect + auth-token refresh, so
/// a dropped socket re-subscribes itself; the engine's periodic pull covers any gap meanwhile.
struct SupabaseDoorbell: SyncDoorbell {
    let client: SupabaseClient
    let tables: [String]

    func ring() -> AsyncStream<Void> {
        AsyncStream { continuation in
            let task = Task {
                let channel = client.realtimeV2.channel("happysync")
                // Listeners must be registered before subscribe(); any change rings the doorbell.
                let streams = tables.map {
                    channel.postgresChange(AnyAction.self, schema: "public", table: $0)
                }
                await channel.subscribe()
                await withTaskGroup(of: Void.self) { group in
                    for stream in streams {
                        group.addTask { for await _ in stream { continuation.yield(()) } }
                    }
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}
