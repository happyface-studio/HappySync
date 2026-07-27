import Foundation
import Supabase

/// Why a sync operation failed, classified where the transport error is raised rather than
/// stringified at the public boundary (issue #52).
///
/// The engine already distinguishes these cases internally to decide whether to retry
/// (`remoteErrorIsPermanent`, `remoteErrorIsAuthTransient`); this is that same knowledge, kept
/// intact for the consumer. Without it the only way to tell "RLS rejected this write" from "the
/// network is down" is substring-matching PostgREST prose, which is not an API.
///
/// The kind answers *what to show the user*; it is deliberately **not** a retry signal. Whether a
/// write is still being retried or has been parked is `SyncStatus.failedUploads` vs `deadLetters`,
/// which the engine derives from its own permanence rules.
public enum SyncFailure: Error, Sendable, Equatable, CustomStringConvertible {
    /// The request never got an answer — offline, DNS, connection dropped (`URLError`). Recovers on
    /// its own; a UI generally shows nothing for this.
    case network
    /// The token was rejected or missing (HTTP 401/403, PostgREST `PGRST301`/`PGRST302`). Recovers
    /// out of band on the next refresh; a sustained run of these means the session is gone and the
    /// app should prompt re-auth.
    case authExpired
    /// Row-level security refused the write (`42501`). Not transient and not the user's doing —
    /// either a policy bug or a genuine permissions problem worth surfacing.
    case permissionDenied
    /// An integrity constraint rejected the row (Postgres class `23` — unique, foreign key,
    /// not-null, check), carrying the SQLSTATE so the app can tell `23505` from `23503`.
    case constraintViolation(code: String)
    /// The client sent a column the server's schema doesn't have (`42703`, or PostgREST's
    /// `PGRST204` schema-cache miss) — the app is ahead of the database, so prompt an update. The
    /// column name is parsed from the server's message when it names one.
    case schemaMismatch(column: String?)
    /// The server answered with an HTTP status that carries no more specific meaning here —
    /// including 5xx. Distinct from `.network`: something responded, and the status is worth
    /// keeping rather than folding into a generic "offline".
    case server(status: Int)
    /// Anything unrecognised, with the original error text preserved so nothing is ever lost.
    case other(String)
}

extension SyncFailure {
    /// Classifies a transport error into its public kind, from the same inputs and by the same
    /// reasoning as the retry classification in `remoteErrorIsPermanent` /
    /// `remoteErrorIsAuthTransient`.
    ///
    /// **One deliberate divergence:** `PGRST204` classifies as `.schemaMismatch`, but
    /// `postgrestCodeIsPermanent` leaves it transient, so the engine retries it to the
    /// `deadLetterAfter` cap rather than parking at once. That's intended — PostgREST raises it from a
    /// *stale schema cache* as well as from a genuinely absent column, and the stale case clears on
    /// its own, so retrying first costs nothing and self-heals. The kind still tells the app what to
    /// show if the write does eventually park.
    ///
    /// Unwraps the `RemoteFailure` envelope the production remote raises, so a classified error
    /// classifies identically whether it arrives wrapped or bare.
    static func classify(_ error: Error) -> SyncFailure {
        if let failure = error as? SyncFailure { return failure }
        let underlying = (error as? RemoteFailure)?.underlying ?? error
        if let postgrest = underlying as? PostgrestError {
            return classify(postgrest: postgrest.code, message: postgrest.message)
        }
        if let http = underlying as? HTTPError {
            let status = http.response.statusCode
            // 401/403 are auth-shaped rather than "the server said no" — the engine already exempts
            // them from the retry budget (APPS-502), and the app's response is re-auth, not repair.
            return (status == 401 || status == 403) ? .authExpired : .server(status: status)
        }
        if underlying is URLError { return .network }
        return .other("\(underlying)")
    }

    /// Classifies a PostgREST/Postgres error by its `code`. Transient Postgres states (`40001`
    /// serialization failure, `40P01` deadlock, `53xxx` resource exhaustion) get no case of their
    /// own — they're the server's problem, they clear on retry, and the engine already retries them
    /// — so they land in `.other` with the message intact. `08xxx` connection errors *are* a
    /// connectivity failure and map to `.network`.
    private static func classify(postgrest code: String?, message: String) -> SyncFailure {
        guard let code else { return .other(message) }
        if code.hasPrefix("23") { return .constraintViolation(code: code) }
        if code.hasPrefix("08") { return .network } // connection_exception class
        switch code {
        case "42501": return .permissionDenied              // insufficient_privilege — RLS reject
        case "42703": return .schemaMismatch(column: quotedIdentifier(in: message))
        case "PGRST204": return .schemaMismatch(column: quotedIdentifier(in: message))
        case "PGRST301", "PGRST302": return .authExpired    // JWT expired / missing
        default: return .other(message)
        }
    }

    /// Pulls the first quoted identifier out of a server message — Postgres says
    /// `column "nutrition" does not exist`, PostgREST says `Could not find the 'nutrition' column of
    /// 'recipes' in the schema cache`. Scans for whichever quote character appears **first**, so a
    /// later double quote can't beat an earlier single-quoted identifier. Best-effort: nil when the
    /// message names nothing, which the `column:` payload is typed to allow.
    private static func quotedIdentifier(in message: String) -> String? {
        guard let open = message.firstIndex(where: { $0 == "\"" || $0 == "'" }) else { return nil }
        let quote = message[open]
        let rest = message.index(after: open)
        guard let close = message[rest...].firstIndex(of: quote), close > rest else { return nil }
        return String(message[rest..<close])
    }

    /// A short, displayable rendering — so a consumer that had `case .failed(let reason):
    /// showBanner(reason)` against the old `String` payload still has something to show. The classified
    /// cases don't carry the server's prose (only `.other` does), and this is what stands in for it;
    /// the full text is in the engine's log line, and on `DeadLetter.lastError` for a parked write.
    public var description: String {
        switch self {
        case .network: return "Network unavailable"
        case .authExpired: return "Session expired"
        case .permissionDenied: return "Permission denied by the server"
        case .constraintViolation(let code): return "Server rejected the row (constraint \(code))"
        case .schemaMismatch(let column):
            return column.map { "Server has no column '\($0)'" } ?? "Server schema mismatch"
        case .server(let status): return "Server error (HTTP \(status))"
        case .other(let message): return message
        }
    }
}

// MARK: - Persistence

/// `DeadLetter` is reconstructed from `_sync_outbox` long after the live `Error` is gone, so the
/// classification is stored alongside `last_error` in the `failure_kind` column (`happysync_v5`).
/// The payload-carrying cases encode as `kind:payload` and round-trip exactly.
///
/// `.other` is the exception: it stores only its kind and rehydrates its text from `last_error`, so
/// the message isn't written to the row twice — which means the decoded value carries whatever
/// `recordFailure` stored (`"\(error)"`, the error's full description) rather than the narrower text
/// `classify` happened to capture. For a `PostgrestError` that's the whole rendering, not just
/// `.message`. Nothing is lost — the stored text is a superset — but `decode(wire:message:)` is not
/// an equality-preserving inverse of `classify` for this case, only for the others.
extension SyncFailure {
    var wireValue: String {
        switch self {
        case .network: return "network"
        case .authExpired: return "authExpired"
        case .permissionDenied: return "permissionDenied"
        case .constraintViolation(let code): return "constraint:\(code)"
        case .schemaMismatch(let column): return column.map { "schema:\($0)" } ?? "schema"
        case .server(let status): return "server:\(status)"
        case .other: return "other"
        }
    }

    /// Rebuilds a failure from its stored `failure_kind` and the row's `last_error` breadcrumb.
    ///
    /// A nil `wire` is an entry that parked before the `happysync_v5` migration (or one parked by
    /// hand), and an unrecognised `wire` is a row written by a newer HappySync than this binary —
    /// both degrade to `.other` carrying whatever text there is, rather than failing the read.
    static func decode(wire: String?, message: String?) -> SyncFailure {
        let fallback = SyncFailure.other(message ?? "")
        guard let wire else { return fallback }
        let parts = wire.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
        let kind = String(parts[0])
        let payload = parts.count > 1 ? String(parts[1]) : nil
        switch kind {
        case "network": return .network
        case "authExpired": return .authExpired
        case "permissionDenied": return .permissionDenied
        case "constraint": return .constraintViolation(code: payload ?? "")
        case "schema": return .schemaMismatch(column: payload)
        case "server":
            guard let payload, let status = Int(payload) else { return fallback }
            return .server(status: status)
        default: return fallback
        }
    }
}
