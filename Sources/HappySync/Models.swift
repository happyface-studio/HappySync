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
    /// Names of tables this one references via foreign keys. Drives sync ordering — parents upload
    /// before children, and children delete before parents.
    ///
    /// `nil` (the default) means **derive it from the schema**: the engine reads `PRAGMA
    /// foreign_key_list` at init and uses this table's real foreign-key parents, narrowed to tables
    /// the manifest declares (issue #49). The FK graph is already in the database, so restating it
    /// here is work the consumer shouldn't have to do — and a restatement that drifts from the schema
    /// surfaces days later as a constraint dead-letter on another device.
    ///
    /// Declare it explicitly to **override** that, for the case the schema can't express: a *logical*
    /// dependency with no real FK constraint (e.g. a `recipeId` column carrying no `REFERENCES`
    /// clause). An explicit list replaces the derived one rather than adding to it, and every name in
    /// it must be a declared `SyncTable`. A self-reference is dropped from either form — a tree table
    /// can't be uploaded before itself, and SQLite's cascade walks it at delete time.
    public let dependsOn: [String]?
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
        dependsOn: [String]? = nil,
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

    /// This spec with `dependsOn` pinned to the list `SyncManifest.resolve` settled on — declared, or
    /// introspected from the schema's foreign keys. The engine's manifest holds only resolved specs,
    /// so nothing downstream of init has to re-decide what `nil` meant.
    func dependingOn(_ tables: [String]) -> SyncTable {
        SyncTable(
            name: name,
            primaryKey: primaryKey,
            cursorColumn: cursorColumn,
            dependsOn: tables,
            jsonColumns: jsonColumns,
            serverOwnedColumns: serverOwnedColumns,
            scopeColumn: scopeColumn,
            conflictColumns: conflictColumns,
            serverColumns: serverColumns
        )
    }
}

/// A pending sync operation recorded in the outbox.
public enum SyncOp: String, Sendable, Codable {
    /// The row exists locally and its current state should be uploaded — queued by an `INSERT` or an
    /// `UPDATE`, and idempotent by primary key.
    case upsert
    /// The row was removed locally and should be **soft**-deleted on the server (its tombstone set),
    /// so the deletion propagates to other devices on their next pull.
    case delete
}

/// Engine status, surfaced as an `AsyncStream` for the app's sync-status UI.
public struct SyncStatus: Sendable, Equatable {
    public enum Phase: Sendable, Equatable {
        /// Settled and clean: the last pass completed and nothing is failing or parked. This is
        /// genuinely "everything is fine" — see `.degraded` for the case it used to be confused with.
        case idle
        case syncing
        /// Settled, but writes are outstanding: the last pass completed (so nothing is *broken*) while
        /// `failedUploads`/`deadLetters` are nonzero — some local changes haven't reached the server.
        ///
        /// This case exists because `.idle` previously meant only "no pass is running", which reads as
        /// health and isn't: a UI switching on the phase showed a green checkmark while the user's
        /// writes sat parked. The counts stay on `SyncStatus` rather than riding as an associated
        /// value, so there's one source of truth for them (issue #51).
        case degraded
        /// The last sync attempt threw — the pass itself failed, not an individual write. Carries the
        /// classified cause so the app can branch on it (issue #52); `SyncFailure.other` preserves the
        /// original text for anything unrecognised.
        case failed(SyncFailure)
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

    /// **The** way to ask whether sync is healthy: the last pass settled *and* nothing is failing or
    /// parked. `phase == .idle` alone was the tempting read and the wrong one — it says no pass is
    /// running, which is also true while a user's writes sit in the outbox failing (issue #51).
    ///
    /// The engine now derives `.idle` vs `.degraded` from these same counts, so on a status the engine
    /// broadcast this is equivalent to `phase == .idle`. It still checks all three, because a
    /// `SyncStatus` a consumer constructed (a preview, a test fixture) can pair `.idle` with nonzero
    /// counts and shouldn't be reported healthy.
    public var isHealthy: Bool {
        phase == .idle && failedUploads == 0 && deadLetters == 0
    }

    /// The phase a **settled** status takes for a given pair of outbox counts — `.idle` when clean,
    /// `.degraded` when writes are outstanding. One place, so every settle path (the end of a pass,
    /// teardown, a dead-letter repair) agrees on what `.idle` is allowed to mean.
    static func settledPhase(failedUploads: Int, deadLetters: Int) -> Phase {
        failedUploads == 0 && deadLetters == 0 ? .idle : .degraded
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
    /// Why the upload failed, classified — so repair UI can branch on the cause (offer re-auth, prompt
    /// an app update, report an RLS bug) instead of substring-matching `lastError` (issue #52).
    /// Persisted with the entry, so it survives the process that classified it.
    public let failure: SyncFailure
    /// The last upload error's text, if one was recorded — the raw breadcrumb behind `failure`, kept
    /// for logs and bug reports.
    public let lastError: String?
    /// When the write was first enqueued.
    public let queuedAt: Date

    public init(
        seq: Int64, table: String, pk: String, op: SyncOp,
        attempts: Int, failure: SyncFailure, lastError: String?, queuedAt: Date
    ) {
        self.seq = seq
        self.table = table
        self.pk = pk
        self.op = op
        self.attempts = attempts
        self.failure = failure
        self.lastError = lastError
        self.queuedAt = queuedAt
    }
}

/// Errors thrown by the engine's public API.
public enum SyncError: Error, Sendable, CustomStringConvertible {
    /// The deprecated `enqueue` shim was called for a table not declared in the engine's `tables`.
    case unknownTable(String)
    /// The encoded row had no value for the table's primary-key column.
    case missingPrimaryKey(table: String, column: String)
    /// The row could not be encoded to SQLite column values.
    case encoding(String)
    /// A declared `SyncTable` doesn't match the local schema — thrown by `SyncEngine.init`, which
    /// validates the whole manifest against the database before installing a single trigger (issue
    /// #49). `reason` names the field and the value at fault; see `ManifestProblem`.
    ///
    /// Every one of these used to be silent: the table simply never synced, or the wrong partition
    /// downloaded, or a column corrupted in both directions. Failing loudly at launch is the point —
    /// run the app's migrations before constructing the engine, then fix what the message names.
    case invalidManifest(table: String, reason: ManifestProblem)
    /// `PRAGMA recursive_triggers` could not be enabled on the writer connection. Without it a
    /// REPLACE-displaced row (and, on older SQLite, a `ON DELETE CASCADE` child) is deleted locally
    /// without a tombstone ever reaching the server (issue #48).
    case recursiveTriggersUnavailable

    /// A message that stands on its own in a crash log — every case names what to go and fix, since
    /// most of these are configuration errors surfaced at app launch.
    public var description: String {
        switch self {
        case .unknownTable(let table):
            "HappySync: enqueue(table: \"\(table)\") — no SyncTable of that name is declared"
        case .missingPrimaryKey(let table, let column):
            "HappySync: the row enqueued for \"\(table)\" has no value for its primary key \"\(column)\""
        case .encoding(let detail):
            "HappySync: could not encode row — \(detail)"
        case .invalidManifest(let table, let reason):
            "HappySync: SyncTable(\"\(table)\") — \(reason)"
        case .recursiveTriggersUnavailable:
            """
            HappySync: PRAGMA recursive_triggers could not be enabled on the writer connection — \
            without it a locally deleted row can lose its tombstone and survive on the server forever
            """
        }
    }
}
