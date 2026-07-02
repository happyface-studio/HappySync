import Foundation
import Supabase

/// A resolved partition filter applied to a download alongside the cursor: `column = value`. The
/// value is the current user's partition key (e.g. the Supabase auth uid), resolved per pull — so
/// signing in as a different user re-scopes without re-declaring tables. See APPS-469.
struct ScopeFilter: Sendable, Equatable {
    let column: String
    let value: String
}

/// A remote failure the drain can classify for retry (APPS-470). Conform a thrown error to signal
/// whether a retry could ever succeed: **permanent** failures (the server will never accept the
/// write as-is — RLS, constraint, validation) are dead-lettered immediately; everything else is
/// treated as transient and retried with backoff. Unrecognized errors default to transient — safer
/// to retry than to silently drop a user's write.
protocol ClassifiedSyncError: Error {
    var isPermanent: Bool { get }
}

/// Wraps a transport error with a retry classification, preserving the underlying error's text so
/// the `last_error` telemetry breadcrumb stays useful.
struct RemoteFailure: ClassifiedSyncError, CustomStringConvertible {
    let isPermanent: Bool
    let underlying: Error
    var description: String { "\(underlying)" }
}

/// True when a transport error is **permanent** — the server will never accept the write as-is, so
/// the drain should dead-letter it immediately rather than burn retries on it. A 4xx (except 408
/// request-timeout and 429 throttle, which are worth retrying) or any structured PostgREST rejection
/// (unique/FK constraint, RLS, validation) is permanent; network errors and 5xx are transient.
func remoteErrorIsPermanent(_ error: Error) -> Bool {
    if let classified = error as? any ClassifiedSyncError { return classified.isPermanent }
    if let http = error as? HTTPError {
        let code = http.response.statusCode
        return (400..<500).contains(code) && code != 408 && code != 429
    }
    if error is PostgrestError { return true }
    return false
}

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
    /// are included. When `scope` is set, only rows matching `scope.column = scope.value` are
    /// returned (the download partition — orthogonal to the cursor).
    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]]
}

/// `SyncRemote` over a Supabase PostgREST client. Idempotent by primary key, so the drain can
/// safely retry. Auth token is fetched fresh per request.
struct SupabaseRemote: SyncRemote {
    let client: SupabaseClient
    let auth: @Sendable () async -> String

    func upsert(table: String, row: [String: AnyJSON]) async throws -> [String: AnyJSON] {
        let token = await auth()
        do {
            let rows: [[String: AnyJSON]] = try await client
                .from(table)
                .upsert(row, returning: .representation)
                .setHeader(name: "Authorization", value: "Bearer \(token)")
                .execute()
                .value
            return rows.first ?? row
        } catch {
            throw Self.classify(error) // tag permanence so the drain can dead-letter poison writes
        }
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {
        let token = await auth()
        // Soft delete: set the tombstone so the deletion propagates on the next cursor pull (a hard
        // DELETE would simply vanish, never reaching other devices). The server's BEFORE UPDATE
        // trigger stamps updatedAt — advancing the cursor — and an AFTER trigger tombstones the row's
        // children. deletedAt is the marker; the server-stamped updatedAt is what ordering trusts.
        do {
            try await client
                .from(table)
                .update(["deletedAt": AnyJSON.string(Self.nowISO8601())])
                .eq(primaryKey, value: pk)
                .setHeader(name: "Authorization", value: "Bearer \(token)")
                .execute()
        } catch {
            throw Self.classify(error)
        }
    }

    /// Tags a transport error with its retry classification for the drain (APPS-470).
    private static func classify(_ error: Error) -> RemoteFailure {
        RemoteFailure(isPermanent: remoteErrorIsPermanent(error), underlying: error)
    }

    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]] {
        let token = await auth()
        var query = client.from(table).select()
        if let scope {
            // Partition filter, AND-ed with the cursor below (PostgREST ANDs top-level filters).
            query = query.eq(scope.column, value: scope.value)
        }
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
