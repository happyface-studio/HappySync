import Foundation
import Supabase

/// The server side of the upload path. Abstracted behind a protocol so the drain can be unit-tested
/// with a fake that records call order and simulates failures — the production conformance is the
/// only place that touches the network.
protocol SyncRemote: Sendable {
    /// Upserts one row and returns the server's representation, which carries the stamped
    /// `updated_at` the drain writes back locally to mark the row clean.
    func upsert(table: String, row: [String: AnyJSON]) async throws -> [String: AnyJSON]
    /// Propagates a delete for one row, keyed by primary key.
    func delete(table: String, primaryKey: String, pk: String) async throws
    /// Fetches up to `limit` rows changed since `cursor`, ordered by the `(updated_at, id)` tuple
    /// so the caller can resume exactly where it left off. Tombstoned rows (`deleted_at != null`)
    /// are included.
    func fetch(table: String, since cursor: SyncCursor?, primaryKey: String, limit: Int) async throws -> [[String: AnyJSON]]
}

/// `SyncRemote` over a Supabase PostgREST client. Idempotent by primary key, so the drain can
/// safely retry. Auth token is fetched fresh per request.
struct SupabaseRemote: SyncRemote {
    let client: SupabaseClient
    let auth: @Sendable () async -> String

    func upsert(table: String, row: [String: AnyJSON]) async throws -> [String: AnyJSON] {
        let token = await auth()
        let rows: [[String: AnyJSON]] = try await client
            .from(table)
            .upsert(row, returning: .representation)
            .setHeader(name: "Authorization", value: "Bearer \(token)")
            .execute()
            .value
        return rows.first ?? row
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {
        let token = await auth()
        // ponytail: hard delete for now. Becomes a soft-delete PATCH (set deleted_at) once M2 adds
        // tombstone columns + the child-cascade trigger; the drain seam doesn't change either way.
        try await client
            .from(table)
            .delete()
            .eq(primaryKey, value: pk)
            .setHeader(name: "Authorization", value: "Bearer \(token)")
            .execute()
    }

    func fetch(table: String, since cursor: SyncCursor?, primaryKey: String, limit: Int) async throws -> [[String: AnyJSON]] {
        let token = await auth()
        var query = client.from(table).select()
        if let cursor {
            // (updated_at, id) > (cursor.updatedAt, cursor.id), as a PostgREST or-filter.
            query = query.or(
                "updated_at.gt.\(cursor.updatedAt),and(updated_at.eq.\(cursor.updatedAt),\(primaryKey).gt.\(cursor.id))"
            )
        }
        return try await query
            .order("updated_at", ascending: true)
            .order(primaryKey, ascending: true)
            .limit(limit)
            .setHeader(name: "Authorization", value: "Bearer \(token)")
            .execute()
            .value
    }
}
