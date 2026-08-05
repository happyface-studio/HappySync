import Foundation
import Supabase

/// A resolved partition filter applied to a download alongside the cursor: `column = value`. The
/// value is the current user's partition key (e.g. the Supabase auth uid), resolved per pull — so
/// signing in as a different user re-scopes without re-declaring tables. See APPS-469.
public struct ScopeFilter: Sendable, Equatable {
    public let column: String
    public let value: String

    public init(column: String, value: String) {
        self.column = column
        self.value = value
    }
}

/// A remote failure the drain can classify for retry (APPS-470). Conform a thrown error to signal
/// whether a retry could ever succeed: **permanent** failures (the server will never accept the
/// write as-is — RLS, constraint, validation) are dead-lettered immediately; everything else is
/// treated as transient and retried with backoff. Unrecognized errors default to transient — safer
/// to retry than to silently drop a user's write.
///
/// Public so a custom `SyncRemote` — or a test double simulating a poison write — can say which of
/// the two it is, instead of leaving the drain to guess transient and burn eight retries on a write
/// the server was never going to accept.
public protocol ClassifiedSyncError: Error {
    var isPermanent: Bool { get }
}

/// Wraps a transport error with a retry classification, preserving the underlying error's text so
/// the `last_error` telemetry breadcrumb stays useful.
public struct RemoteFailure: ClassifiedSyncError, CustomStringConvertible {
    public let isPermanent: Bool
    public let underlying: Error
    public var description: String { "\(underlying)" }

    public init(isPermanent: Bool, underlying: Error) {
        self.isPermanent = isPermanent
        self.underlying = underlying
    }

    /// Wraps `underlying` with **the classification the production remote would give it** — the same
    /// `PostgrestError` code / HTTP status table `SupabaseRemote` applies. A test double raising a
    /// real transport error (`HTTPError(401)`, `PostgrestError(code: "42501")`) should build its
    /// failure this way, so the drain sees exactly what it would see in the field rather than a
    /// hand-picked permanence that may not match.
    public init(classifying underlying: Error) {
        self.init(isPermanent: remoteErrorIsPermanent(underlying), underlying: underlying)
    }
}

/// True when a transport error is **permanent** — the server will never accept the write as-is, so
/// the drain should dead-letter it immediately rather than burn retries on it. Classification:
///
/// * `PostgrestError` — by its Postgres/PostgREST `code`: integrity-constraint violations (class
///   `23`, e.g. `23505` unique, `23503` FK), `42501` insufficient_privilege (RLS reject) and `42703`
///   undefined_column are permanent; transient Postgres states (`40001` serialization_failure,
///   `40P01` deadlock, `53xxx` resource exhaustion, `08xxx` connection errors) and every
///   unrecognized code — including PostgREST's `PGRSTxxx` auth codes — default to transient.
/// * `HTTPError` — a 4xx is permanent *except* 401/403 (auth may be mid-refresh; recovers
///   out-of-band — APPS-502), 408 (request timeout) and 429 (throttle), which are worth retrying;
///   5xx is transient.
/// * Anything else (network `URLError`, etc.) is transient.
///
/// An unrecognized or absent code defaults to transient — safer to retry than to silently drop a
/// user's write.
func remoteErrorIsPermanent(_ error: Error) -> Bool {
    if let classified = error as? any ClassifiedSyncError { return classified.isPermanent }
    if let postgrest = error as? PostgrestError { return postgrestCodeIsPermanent(postgrest.code) }
    if let http = error as? HTTPError { return httpStatusIsPermanent(http.response.statusCode) }
    return false
}

/// Permanence of a `PostgrestError`'s Postgres/PostgREST `code`. See `remoteErrorIsPermanent`.
private func postgrestCodeIsPermanent(_ code: String?) -> Bool {
    guard let code else { return false } // no code → default transient
    if code.hasPrefix("23") { return true } // integrity_constraint_violation (unique, FK, not-null, check)
    switch code {
    case "42501": return true // insufficient_privilege — RLS rejected the write
    case "42703": return true // undefined_column — schema mismatch the server will never accept
    default: return false      // 40001 / 40P01 / 53xxx / 08xxx transient states + unknown codes → retry
    }
}

/// Permanence of an HTTP status. See `remoteErrorIsPermanent`.
private func httpStatusIsPermanent(_ code: Int) -> Bool {
    guard (400..<500).contains(code) else { return false } // 5xx and non-4xx → transient
    switch code {
    case 401, 403, 408, 429: return false // auth mid-refresh / timeout / throttle → retry
    default: return true
    }
}

/// True when a failure is **auth-shaped** — a 401/403 or a PostgREST JWT rejection — i.e. it stems
/// from a stale or missing token, not the write itself. These recover out-of-band on the next token
/// refresh, so the drain retries them **without** charging the `deadLetterAfter` budget: an expired-
/// token stretch (session-refresh race at cold launch, briefly-revoked session, clock skew) must not
/// burn a healthy write's retries and dead-letter the whole outbox in a single drain pass (APPS-502).
func remoteErrorIsAuthTransient(_ error: Error) -> Bool {
    let underlying = (error as? RemoteFailure)?.underlying ?? error
    if let postgrest = underlying as? PostgrestError {
        // PostgREST surfaces an expired/invalid JWT as PGRST301 (and a missing one as PGRST302).
        return postgrest.code == "PGRST301" || postgrest.code == "PGRST302"
    }
    if let http = underlying as? HTTPError {
        let code = http.response.statusCode
        return code == 401 || code == 403
    }
    return false
}

/// True when a failure names the **connection**, not the payload: the request never completed
/// (`URLError` — offline, DNS, timeout, dropped socket) or Postgres reported a connection error
/// (class `08`). These say nothing about any individual write, so the drain treats them like the
/// auth-shaped failures above: no retry-budget charge (a device is *expected* to sit offline far
/// longer than the backoff cap allows — APPS-502's reasoning exactly), and no per-row re-run of a
/// failed batch, which would only multiply doomed requests. An HTTP status is deliberately *not*
/// connectivity — the server answered, so a per-row re-run can still isolate a poison row.
func remoteErrorIsConnectivity(_ error: Error) -> Bool {
    let underlying = (error as? RemoteFailure)?.underlying ?? error
    if underlying is URLError { return true }
    if let postgrest = underlying as? PostgrestError {
        return postgrest.code?.hasPrefix("08") ?? false // connection_exception class
    }
    return false
}

/// The server side of the sync path. Abstracted behind a protocol so the drain can be driven by a
/// fake that records call order and simulates failures — the production conformance
/// (`SupabaseRemote`) is the only place that touches the network.
///
/// Public so a consumer can test its own integration offline: build the engine with
/// `SyncEngine.forTesting(db:remote:tables:)` and an `InMemorySyncRemote` from the
/// `HappySyncTestSupport` product, and questions like "does deleting a recipe queue tombstones for
/// its children?" become assertions instead of a live Supabase project (issue #53).
public protocol SyncRemote: Sendable {
    /// Upserts one row and returns the server's representation, which carries the stamped
    /// cursor column (`updatedAt`) the drain writes back locally to mark the row clean. `onConflict`
    /// is the comma-joined columns of a secondary unique constraint to use as the PostgREST conflict
    /// target, or nil to conflict on the primary key (the default). See APPS-478.
    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON]
    /// Upserts a **batch** of rows for one table in a single request, returning one representation
    /// per row. Every row carries the same columns and the same `onConflict` target, so this is one
    /// PostgREST call rather than `rows.count` of them (issue #54).
    ///
    /// The default implementation forwards to the single-row `upsert` in a loop, so an existing
    /// conformance keeps working unchanged — override it to get the batch. Either way the drain
    /// treats a thrown error as rejecting the **whole** batch and re-runs the rows one at a time, so
    /// a single poison row parks alone instead of taking its neighbours with it.
    func upsert(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]]
    /// Propagates a delete for one row, keyed by primary key (soft delete — sets the tombstone).
    func delete(table: String, primaryKey: String, pk: String) async throws
    /// Propagates a **batch** of deletes for one table in a single request. Defaults to looping over
    /// the single-row `delete`; see the batch `upsert` for the failure contract.
    func delete(table: String, primaryKey: String, pks: [String]) async throws
    /// Fetches up to `limit` rows changed since `cursor`, ordered by the `(cursorColumn, id)` tuple
    /// so the caller can resume exactly where it left off. Tombstoned rows (`deletedAt != null`)
    /// are included. When `scope` is set, only rows matching `scope.column = scope.value` are
    /// returned (the download partition — orthogonal to the cursor).
    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String, scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]]
}

/// Batch defaults, so `SyncRemote` stays the same three methods to implement. A conformance that
/// can genuinely send one request per batch (`SupabaseRemote` does) overrides them; one that can't
/// loses nothing but the round trips.
public extension SyncRemote {
    func upsert(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]] {
        var representations: [[String: AnyJSON]] = []
        representations.reserveCapacity(rows.count)
        for row in rows {
            representations.append(try await upsert(table: table, row: row, onConflict: onConflict))
        }
        return representations
    }

    func delete(table: String, primaryKey: String, pks: [String]) async throws {
        for pk in pks {
            try await delete(table: table, primaryKey: primaryKey, pk: pk)
        }
    }
}

/// `SyncRemote` over a Supabase PostgREST client. Idempotent by primary key, so the drain can
/// safely retry. Auth token is fetched fresh per request.
struct SupabaseRemote: SyncRemote {
    let client: SupabaseClient
    let auth: @Sendable () async -> String

    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        try await send(table: table, rows: [row], onConflict: onConflict).first ?? row
    }

    /// One PostgREST request for the whole batch: an array body upserts against the same conflict
    /// target, and the representation comes back one row per uploaded row, in request order (issue
    /// #54). PostgREST requires every object in the body to carry the same keys — which holds here,
    /// since a table's payloads are built from the same column list — and rejects the request
    /// outright otherwise, which the drain reads as "re-run this chunk one row at a time".
    func upsert(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]] {
        try await send(table: table, rows: rows, onConflict: onConflict)
    }

    private func send(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]] {
        guard !rows.isEmpty else { return [] } // never spend a request (or send `[]`) on nothing
        let token = await auth()
        do {
            // onConflict nil → PostgREST conflicts on the primary key (its default); non-nil → the
            // named secondary unique constraint is the conflict target, merging onto the existing row.
            return try await client
                .from(table)
                .upsert(rows, onConflict: onConflict, returning: .representation)
                .setHeader(name: "Authorization", value: "Bearer \(token)")
                .execute()
                .value
        } catch {
            throw Self.classify(error) // tag permanence so the drain can dead-letter poison writes
        }
    }

    func delete(table: String, primaryKey: String, pk: String) async throws {
        try await delete(table: table, primaryKey: primaryKey, pks: [pk])
    }

    func delete(table: String, primaryKey: String, pks: [String]) async throws {
        guard !pks.isEmpty else { return } // an empty `in.()` is a filter no row matches — skip the trip
        let token = await auth()
        // Soft delete: set the tombstone so the deletion propagates on the next cursor pull (a hard
        // DELETE would simply vanish, never reaching other devices). The server's BEFORE UPDATE
        // trigger stamps updatedAt — advancing the cursor — and an AFTER trigger tombstones the row's
        // children. deletedAt is the marker; the server-stamped updatedAt is what ordering trusts.
        //
        // One request for the whole batch: `in.(pk, …)` tombstones every named row in a single
        // UPDATE, the delete-side counterpart of the batched upsert (issue #54).
        do {
            try await client
                .from(table)
                .update(["deletedAt": AnyJSON.string(Self.nowISO8601())])
                .in(primaryKey, values: pks)
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
            // (cursorColumn, id) > (cursor.updatedAt, cursor.id), as a PostgREST or-filter with the
            // values double-quoted so reserved characters can't corrupt the grammar (APPS-474).
            query = query.or(Self.cursorFilter(cursorColumn: cursorColumn, primaryKey: primaryKey, cursor: cursor))
        }
        return try await query
            .order(cursorColumn, ascending: true)
            .order(primaryKey, ascending: true)
            .limit(limit)
            .setHeader(name: "Authorization", value: "Bearer \(token)")
            .execute()
            .value
    }

    /// Builds the tuple-cursor PostgREST `or=` filter `(cursorColumn, id) > (updatedAt, id)`, with
    /// the cursor values **double-quoted** so reserved characters (`,` `(` `)` `.`) or a `+` in a
    /// timestamp/pk can't corrupt the filter grammar or be mis-decoded (APPS-474). Pure + internal
    /// so the encoding is regression-tested without a live PostgREST.
    static func cursorFilter(cursorColumn: String, primaryKey: String, cursor: SyncCursor) -> String {
        let ts = quote(cursor.updatedAt)
        let id = quote(cursor.id)
        return "\(cursorColumn).gt.\(ts),and(\(cursorColumn).eq.\(ts),\(primaryKey).gt.\(id))"
    }

    /// PostgREST protects a filter value with double quotes; inside, `\` and `"` are backslash-escaped.
    static func quote(_ value: String) -> String {
        let escaped = value
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
        return "\"\(escaped)\""
    }

    /// ISO-8601 with fractional seconds — the contract's canonical timestamp format. Used only for
    /// the client-set `deletedAt` marker; row ordering trusts the server-stamped `updatedAt`, not this.
    private static func nowISO8601() -> String {
        SyncTimestamp.string(from: Date())
    }
}
