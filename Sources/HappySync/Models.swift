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

    public init(
        name: String,
        primaryKey: String = "id",
        cursorColumn: String = "updatedAt",
        dependsOn: [String] = [],
        jsonColumns: [String] = [],
        serverOwnedColumns: [String] = []
    ) {
        self.name = name
        self.primaryKey = primaryKey
        self.cursorColumn = cursorColumn
        self.dependsOn = dependsOn
        self.jsonColumns = jsonColumns
        self.serverOwnedColumns = serverOwnedColumns
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

    public init(phase: Phase = .idle, lastSyncedAt: Date? = nil) {
        self.phase = phase
        self.lastSyncedAt = lastSyncedAt
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
