import Foundation

/// Describes a table HappySync keeps in sync between local GRDB and Supabase.
///
/// Tables are declared up front so the engine can topologically order work by foreign-key
/// dependencies: upload/download parents before children, delete children before parents.
public struct SyncTable: Sendable, Hashable {
    /// Table name — identical on the local SQLite DB and the Supabase Postgres DB.
    public let name: String
    /// Primary-key column. Used as the outbox `pk` and the cursor tie-breaker.
    public let primaryKey: String
    /// Column the download cursor orders, filters, and advances by — the server-stamped change
    /// time. Defaults to `updatedAt`; an immutable insert-only table overrides it (e.g.
    /// `recipe_translations` cursors on `translatedAt`, since it has no `updatedAt`).
    public let cursorColumn: String
    /// Names of tables this one references via foreign keys. Drives sync ordering.
    public let dependsOn: [String]
    /// Columns stored as JSON/JSONB that need encode/decode rather than scalar mapping.
    public let jsonColumns: [String]
    /// Columns the server owns (e.g. RPC-managed counters) — stripped from every upsert so a
    /// stale client value never clobbers the authoritative one. They still arrive on download.
    public let serverOwnedColumns: [String]
    /// Optional partition column scoping downloads to the current user (e.g. `userId`). When set,
    /// the engine filters both the cursor pull and the Realtime doorbell to `column = <partition
    /// value>`, where the value is resolved per signed-in user at pull time (see the engine's
    /// `scope` closure). Leave `nil` when RLS already scopes the table to exactly the synced
    /// partition; set it when RLS is deliberately broader than the partition — CookThis's `recipes`
    /// policy is `isPublic = true OR userId = auth.uid()`, so an unfiltered pull would download the
    /// whole public catalog to every device. See APPS-469 / contract §1.
    public let scopeColumn: String?

    public init(
        name: String,
        primaryKey: String = "id",
        cursorColumn: String = "updatedAt",
        dependsOn: [String] = [],
        jsonColumns: [String] = [],
        serverOwnedColumns: [String] = [],
        scopeColumn: String? = nil
    ) {
        self.name = name
        self.primaryKey = primaryKey
        self.cursorColumn = cursorColumn
        self.dependsOn = dependsOn
        self.jsonColumns = jsonColumns
        self.serverOwnedColumns = serverOwnedColumns
        self.scopeColumn = scopeColumn
    }
}

/// A pending sync operation recorded in the outbox.
public enum SyncOp: String, Sendable, Codable {
    case upsert
    case delete
}

/// Engine status, surfaced as an `AsyncStream` for the app's sync-status UI.
public struct SyncStatus: Sendable, Equatable {
    public enum Phase: Sendable, Equatable {
        case idle
        case syncing
        /// Last sync attempt failed; carries a human-readable reason.
        case failed(String)
    }

    public var phase: Phase
    /// Time of the last successful pull/push, or `nil` if never synced.
    public var lastSyncedAt: Date?
    /// Entries that failed to upload on the last drain and are still being retried with backoff. A
    /// nonzero value on an otherwise-`idle` status means "some local changes haven't reached the
    /// server yet" — the sync is degraded, not broken (APPS-470).
    public var failedUploads: Int
    /// Entries parked after exhausting retries (or a permanent 4xx). They are no longer retried and
    /// no longer block downloads for their row; surface them so the consumer can log/repair.
    public var deadLetters: Int

    public init(phase: Phase = .idle, lastSyncedAt: Date? = nil, failedUploads: Int = 0, deadLetters: Int = 0) {
        self.phase = phase
        self.lastSyncedAt = lastSyncedAt
        self.failedUploads = failedUploads
        self.deadLetters = deadLetters
    }
}

/// Errors thrown by the engine's public API.
public enum SyncError: Error, Sendable {
    /// Functionality scheduled for a later milestone is not wired up yet.
    case notImplemented(String)
    /// `enqueue` was called for a table not declared in the engine's `tables`.
    case unknownTable(String)
    /// The encoded row had no value for the table's primary-key column.
    case missingPrimaryKey(table: String, column: String)
    /// The row could not be encoded to SQLite column values.
    case encoding(String)
}
