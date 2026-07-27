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
    /// Extra columns the server owns (e.g. RPC-managed counters) — stripped from every upsert so a
    /// stale client value never clobbers the authoritative one. They still arrive on download.
    /// The `cursorColumn` is server-owned by contract and stripped unconditionally; it does not need
    /// to be listed here (see `uploadExcludedColumns`).
    public let serverOwnedColumns: [String]
    /// Full set of columns the **server**'s schema has for this table. When non-empty, the upload
    /// payload is intersected against it, so a column the server has since dropped or renamed can't
    /// reject the whole upsert (`PGRST204`) and dead-letter every write on the table (APPS-504).
    /// Empty (the default) uploads every non-server-owned local column and relies instead on the
    /// §8 contract rule that a removed server column stays nullable-and-ignored until no shipped
    /// client still sends it. Opt in only when you accept the maintenance cost — a stale list
    /// silently stops uploading a real column. (Downloads need no such list: they intersect against
    /// the *local* schema, introspected per pass.)
    public let serverColumns: [String]
    /// Optional partition column scoping downloads to the current user (e.g. `userId`). When set,
    /// the engine filters both the cursor pull and the Realtime doorbell to `column = <partition
    /// value>`, where the value is resolved per signed-in user at pull time (see the engine's
    /// `scope` closure). Leave `nil` when RLS already scopes the table to exactly the synced
    /// partition; set it when RLS is deliberately broader than the partition — CookThis's `recipes`
    /// policy is `isPublic = true OR userId = auth.uid()`, so an unfiltered pull would download the
    /// whole public catalog to every device. See APPS-469 / contract §1.
    public let scopeColumn: String?
    /// Columns forming a **secondary unique constraint** on the server that a fresh-primary-key
    /// insert would violate. When set, the engine upserts with PostgREST `on_conflict=<these>`, so a
    /// row merges onto the existing server row by this constraint instead of 409-ing on the
    /// duplicate — e.g. CookThis's `userRecipeInteractions` has `UNIQUE(userId, recipeId)`, and a
    /// device that hasn't pulled a server-created interaction yet would otherwise mint a new id and
    /// poison its outbox forever. Empty (the default) means the primary key is the only uniqueness
    /// the upsert can hit. **Only safe on a leaf table** (nothing foreign-keys its primary key): the
    /// merge re-keys the server row to the client's id, which would orphan children of a non-leaf
    /// table. See APPS-478.
    public let conflictColumns: [String]

    public init(
        name: String,
        primaryKey: String = "id",
        cursorColumn: String = "updatedAt",
        dependsOn: [String] = [],
        jsonColumns: [String] = [],
        serverOwnedColumns: [String] = [],
        scopeColumn: String? = nil,
        conflictColumns: [String] = [],
        serverColumns: [String] = []
    ) {
        self.name = name
        self.primaryKey = primaryKey
        self.cursorColumn = cursorColumn
        self.dependsOn = dependsOn
        self.jsonColumns = jsonColumns
        self.serverOwnedColumns = serverOwnedColumns
        self.scopeColumn = scopeColumn
        self.conflictColumns = conflictColumns
        self.serverColumns = serverColumns
    }

    /// Columns never included in an upload payload: the declared `serverOwnedColumns` **plus the
    /// `cursorColumn`**, which the server stamps by contract §1/§4 and a client must therefore never
    /// send. Stripping it is defence in depth for the one server-side requirement the engine can't
    /// verify: with the `BEFORE INSERT/UPDATE` trigger in place a client-sent value is merely
    /// overwritten, but on a table whose trigger was never added (or was dropped by a migration) the
    /// uploaded value sticks and the *client's* clock silently becomes the LWW ordering authority —
    /// rows that won't converge, a device that never sees its own write, a skewed device that always
    /// wins or always loses. Enforced here rather than defaulted into `serverOwnedColumns` so the
    /// public field keeps meaning "extra columns the server owns" and a consumer can't opt out of the
    /// contract by passing an explicit list.
    ///
    /// `deletedAt` is deliberately **not** stripped: `collapseOutbox` relies on an upsert carrying
    /// `deletedAt = null` to un-tombstone a re-created row.
    var uploadExcludedColumns: Set<String> {
        Set(serverOwnedColumns).union([cursorColumn])
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
    /// Outbox entries that have failed at least once and are still being retried with backoff
    /// (`attempts > 0`, not dead-lettered) — counted from the live outbox, so an entry sitting inside
    /// its per-entry backoff window still shows up rather than flapping to healthy on a pass that
    /// skipped it (APPS-507). A nonzero value on an otherwise-`idle` status means "some local changes
    /// haven't reached the server yet" — the sync is degraded, not broken (APPS-470).
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

/// A dead-lettered outbox entry, surfaced so the consumer can inspect, retry, or discard it
/// (APPS-508). Parking is otherwise opaque — `SyncStatus.deadLetters` is only a count — and the
/// underlying `_sync_outbox` row (which the app never touches directly) is where the `(table, pk,
/// op)` and the `last_error` breadcrumb live. See `SyncEngine.deadLetters()`.
public struct DeadLetter: Sendable {
    /// The outbox row's stable identifier — pass it to `retryDeadLetters`/`discardDeadLetters`.
    public let seq: Int64
    public let table: String
    public let pk: String
    public let op: SyncOp
    /// How many upload attempts were charged before the entry parked.
    public let attempts: Int
    /// The last upload error's text, if one was recorded — the repair breadcrumb.
    public let lastError: String?
    /// When the write was first enqueued.
    public let queuedAt: Date

    public init(
        seq: Int64, table: String, pk: String, op: SyncOp,
        attempts: Int, lastError: String?, queuedAt: Date
    ) {
        self.seq = seq
        self.table = table
        self.pk = pk
        self.op = op
        self.attempts = attempts
        self.lastError = lastError
        self.queuedAt = queuedAt
    }
}

/// Errors thrown by the engine's public API.
public enum SyncError: Error, Sendable {
    /// Functionality scheduled for a later milestone is not wired up yet.
    case notImplemented(String)
    /// The deprecated `enqueue` shim was called for a table not declared in the engine's `tables`.
    case unknownTable(String)
    /// The encoded row had no value for the table's primary-key column.
    case missingPrimaryKey(table: String, column: String)
    /// The row could not be encoded to SQLite column values.
    case encoding(String)
    /// A declared `SyncTable` has no table of that name in the local database, so its write-capture
    /// triggers can't be installed. Create the table (run the app's migrations) before constructing
    /// the engine — the manifest and the schema must agree, or the table would silently sync nothing
    /// (issue #48).
    case missingLocalTable(String)
    /// A declared `SyncTable`'s `primaryKey` column doesn't exist on the local table — usually a typo
    /// in the manifest, or a table whose key column isn't `id`.
    case missingPrimaryKeyColumn(table: String, column: String)
    /// `PRAGMA recursive_triggers` could not be enabled on the writer connection. Without it a
    /// REPLACE-displaced row (and, on older SQLite, a `ON DELETE CASCADE` child) is deleted locally
    /// without a tombstone ever reaching the server (issue #48).
    case recursiveTriggersUnavailable
}
