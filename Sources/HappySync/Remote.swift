import Foundation
import Supabase

/// The server side of the upload path. Abstracted behind a protocol so the drain can be unit-tested
/// with a fake that records call order and simulates failures — the production conformance is the
/// only place that touches the network.
protocol SyncRemote: Sendable {
    /// Upserts one row and returns the server's representation, which carries the stamped
    /// cursor column (`updatedAt`) the drain writes back locally to mark the row clean.
    func upsert(table: String, row: [String: AnyJSON]) async throws -> [String: AnyJSON]
    /// Propagates a delete for one row, keyed by primary key (soft delete — sets the tombstone).
    func delete(table: String, primaryKey: String, pk: String) async throws
    /// Fetches up to `limit` rows changed since `cursor`, ordered by the `(cursorColumn, id)` tuple
    /// so the caller can resume exactly where it left off. Tombstoned rows (`deletedAt != null`)
    /// are included.
    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, limit: Int) async throws -> [[String: AnyJSON]]
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
        // Soft delete: set the tombstone so the deletion propagates on the next cursor pull (a hard
        // DELETE would simply vanish, never reaching other devices). The server's BEFORE UPDATE
        // trigger stamps updatedAt — advancing the cursor — and an AFTER trigger tombstones the row's
        // children. deletedAt is the marker; the server-stamped updatedAt is what ordering trusts.
        try await client
            .from(table)
            .update(["deletedAt": AnyJSON.string(Self.nowISO8601())])
            .eq(primaryKey, value: pk)
            .setHeader(name: "Authorization", value: "Bearer \(token)")
            .execute()
    }

    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, limit: Int) async throws -> [[String: AnyJSON]] {
        let token = await auth()
        var query = client.from(table).select()
        if let cursor {
            // (cursorColumn, id) > (cursor.updatedAt, cursor.id), as a PostgREST or-filter.
            query = query.or(
                "\(cursorColumn).gt.\(cursor.updatedAt),and(\(cursorColumn).eq.\(cursor.updatedAt),\(primaryKey).gt.\(cursor.id))"
            )
        }
        return try await query
            .order(cursorColumn, ascending: true)
            .order(primaryKey, ascending: true)
            .limit(limit)
            .setHeader(name: "Authorization", value: "Bearer \(token)")
            .execute()
            .value
    }

    /// ISO-8601 with fractional seconds — the contract's canonical timestamp format. Used only for
    /// the client-set `deletedAt` marker; row ordering trusts the server-stamped `updatedAt`, not this.
    private static func nowISO8601() -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: Date())
    }
}
