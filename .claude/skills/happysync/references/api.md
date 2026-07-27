# HappySync — public API reference

All types are in the `HappySync` module. The engine is an `actor`; its async methods are
`await`ed and safe to call from any task.

## `SyncEngine`

```swift
public init(
    db: any DatabaseWriter,                                  // GRDB writer; local source of truth
    supabase: SupabaseClient,
    tables: [SyncTable],                                     // the manifest, declared once
    auth: @escaping @Sendable () async -> String,            // fresh access token per call
    scope: @escaping @Sendable () async -> String? = { nil },// partition value for scopeColumn tables
    maxOfflineGap: TimeInterval = 30 * 24 * 3600             // stale-cursor full-resync threshold
) throws
```

Migrations for the engine's internal tables (`_sync_outbox`, `_sync_state`, `_sync_meta`,
`_sync_control`) run at init. If your app owns the GRDB migrator, register them into it instead so
both share one `grdb_migrations` table:

```swift
var migrator = DatabaseMigrator()
SyncSchema.register(into: &migrator)   // adds happysync_v1..v4
// … your app migrations …
```

The per-table **write-capture triggers** are not migrations — they're derived from the `tables`
manifest, so the engine (re)installs them idempotently at every init and drops the ones for tables
you've stopped syncing. Every declared table must exist by then.

### Methods

| Method | Notes |
|---|---|
| `func start()` | Begins background sync. Idempotent. Runs an immediate convergence sync, then drives from three triggers (Realtime doorbell, periodic poll, retry backoff) through one serialized runner. |
| `func stop() async` | **Async** — awaits the in-flight pass, then quiesces (no more DB writes / network) and unsubscribes Realtime. Settles `status` to `.idle` but leaves subscriptions open. `start()` re-subscribes cleanly. Call **before** wiping/replacing the DB. |
| `func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws` | **Deprecated (issue #48).** Performs the write; the table's capture trigger records it. Write GRDB directly instead. Throws `SyncError.unknownTable` / `.missingPrimaryKey` / `.encoding`. |
| `func syncNow()` | Fire-and-forget nudge (foreground return, pull-to-refresh). Not needed after a write — the engine wakes on queued writes itself. |
| `@discardableResult func pullNow() async throws -> [String: Set<String>]` | Runs a cursor pull now; returns the server primary keys seen per table. |
| `var status: AsyncStream<SyncStatus>` (`nonisolated`) | Live status for the sync-status UI. Independent stream per access, replays the latest snapshot, survives `stop()`/`start()` — it only ends when the engine is deallocated. |
| `func deadLetters() async throws -> [DeadLetter]` | Inspect parked entries. |
| `func retryDeadLetters(_ seqs: [Int64]? = nil) async throws` | Re-queue parked writes (specific `seq`s, or all). Refreshes status immediately. |
| `func discardDeadLetters(_ seqs: [Int64]? = nil) async throws` | Drop parked writes and re-pull the affected rows so local converges to the server. |

## `SyncTable`

```swift
public init(
    name: String,                          // identical local + remote table name
    primaryKey: String = "id",
    cursorColumn: String = "updatedAt",    // change-time column the cursor orders/filters/advances by
                                           // — server-stamped: never included in an upload payload
    dependsOn: [String]? = nil,            // nil = derive from the schema's FKs; drives sync + delete ordering
    jsonColumns: [String] = [],            // JSON text locally ↔ json/jsonb remote
    serverOwnedColumns: [String] = [],     // *extra* RPC-managed columns; stripped too, applied on download
    scopeColumn: String? = nil,            // partition column when RLS is broader than the partition
    conflictColumns: [String] = [],        // secondary-unique upsert target; LEAF tables only
    serverColumns: [String] = []           // optional allow-list to survive a dropped/renamed column
)
```

Field gotchas:

- **`dependsOn`** is normally left alone: `nil` (the default) means the engine reads
  `PRAGMA foreign_key_list` at init and derives the table's FK parents itself, so a rename in a
  migration can't leave a stale manifest behind. Parents upsert/download before children; children
  tombstone before parents. Declare it only to **override** — a *logical* parent your schema has no
  FK for. An explicit list replaces the derived one rather than adding to it, every name in it must
  be a declared `SyncTable`, and a self-reference (a tree table) is dropped from either form.
- **`scopeColumn`** requires the engine's `scope:` closure to resolve the partition value per
  signed-in user. While no value is available (cold launch before session restore, or signed out),
  the pull for that table is skipped and the stale-resync check stays armed — it never wipes an
  un-pulled table.
- **`conflictColumns`** merges a fresh-pk insert onto an existing server row by a secondary
  `UNIQUE` constraint (avoids a permanent 409). The merge **re-keys the server row to the client's
  pk**, so declaring it on a non-leaf table orphans children — **leaf tables only**.
- **`cursorColumn`** is server-stamped by contract, so the engine strips it from **every** upload
  payload — you declare nothing, and there's no way to opt out. It's defence in depth for the one
  server-side requirement the engine can't verify: on a table whose `updatedAt` trigger is missing,
  an uploaded client value would stick and that device's clock would silently become the LWW
  ordering authority. Consequence: an insert-only table's stamp column needs a server
  `default now()`, since the client never supplies it. `deletedAt` is *not* stripped — an upsert
  carrying `deletedAt = null` is how a re-created row un-tombstones.
- **`serverOwnedColumns`** lists only the *extra* server-owned columns (RPC-managed counters and the
  like); the cursor column is already covered.
- **`serverColumns`** is an opt-in client-side backstop; a stale list silently stops uploading a
  real column. Prefer the operational client-first removal rule; use this only when you accept the
  maintenance cost. Downloads need no equivalent — they intersect against the local schema.

## `SyncStatus`

```swift
public struct SyncStatus {
    public enum Phase {
        case idle          // settled and clean — genuinely healthy
        case syncing
        case degraded      // settled, but writes are still failing or parked
        case failed(SyncFailure)
    }
    public var phase: Phase
    public var lastSyncedAt: Date?     // last successful pull/push, or nil
    public var failedUploads: Int      // entries retrying with backoff (attempts > 0, not parked)
    public var deadLetters: Int        // entries parked after a permanent 4xx or exhausted retries
    public var isHealthy: Bool         // phase == .idle && both counts zero
}
```

**Ask `status.isHealthy`; never `phase == .idle` alone.** The engine derives `.idle` vs `.degraded`
from the counts, so a status it broadcast can't claim idle while writes are outstanding — but
`isHealthy` is the read that stays correct on a `SyncStatus` you construct yourself, and it says what
you mean. Don't render a green checkmark on `.degraded`.

## `SyncFailure`

The classified cause carried by `Phase.failed` and `DeadLetter.failure` — branch on it instead of
substring-matching PostgREST prose.

```swift
public enum SyncFailure: Error, Sendable, Equatable {
    case network                            // URLError, dropped connection, Postgres 08xxx
    case authExpired                        // 401/403, PGRST301/302 — prompt re-auth
    case permissionDenied                   // 42501 — RLS rejected the write
    case constraintViolation(code: String)  // 23xxx — unique, FK, not-null, check
    case schemaMismatch(column: String?)    // 42703 / PGRST204 — prompt an app update
    case server(status: Int)                // the server answered, including 5xx
    case other(String)                      // unrecognised — original text preserved
}
```

The kind says *what to show*, not whether to retry: `failedUploads` vs `deadLetters` is what tells
you whether a write is still being retried or has been parked.

## `DeadLetter`

```swift
public struct DeadLetter {
    public let seq: Int64        // pass to retryDeadLetters / discardDeadLetters
    public let table: String
    public let pk: String
    public let op: SyncOp        // .upsert | .delete
    public let attempts: Int     // upload attempts charged before parking
    public let failure: SyncFailure // classified cause — branch on it to offer the right repair
    public let lastError: String?// the raw breadcrumb behind `failure`, for logs and bug reports
    public let queuedAt: Date
}
```

## `SyncOp` / `SyncError`

```swift
public enum SyncOp: String, Codable { case upsert; case delete }

public enum SyncError: Error, CustomStringConvertible {
    case notImplemented(String)
    case unknownTable(String)                    // deprecated enqueue on an undeclared table
    case missingPrimaryKey(table: String, column: String)
    case encoding(String)
    // Thrown at init when the manifest and the local schema disagree (issues #48/#49):
    case invalidManifest(table: String, reason: ManifestProblem)
    case recursiveTriggersUnavailable            // PRAGMA recursive_triggers could not be enabled
}

public enum ManifestProblem: Equatable, CustomStringConvertible {
    case duplicateDeclaration                            // the same table declared twice
    case noSuchTable                                     // no local table of that name
    case noSuchColumn(field: String, column: String)     // a field names a column the table lacks
    case unknownDependency(String)                       // dependsOn names an undeclared SyncTable
    case conflictColumnsOnParentTable(referencedBy: String)
    case serverColumnsOmitPrimaryKey(String)
}
```

### Manifest validation at init

`SyncEngine.init` checks every `SyncTable` against the local schema before installing a single
trigger, and the error names the table, the field and the value. What's checked:

- `name` is a local table; `primaryKey`, `cursorColumn`, `scopeColumn`, `jsonColumns`,
  `conflictColumns` and `serverOwnedColumns` each name a column on it (case-insensitively, as SQLite
  matches identifiers).
- `serverColumns` is **not** checked against the local schema — it describes the server's schema,
  which is allowed to differ — but it may not omit the primary key.
- `conflictColumns` is rejected on a table anything else foreign-keys (the leaf-only rule, now
  enforced instead of merely documented). Unsynced children count.
- `dependsOn` entries name declared tables; no table is declared twice.

Both enums print a full sentence, so the message stands on its own in a crash log.

## Writing (no API)

There is no write method. Each declared table gets `AFTER INSERT` / `AFTER UPDATE` / `AFTER DELETE`
triggers that append to `_sync_outbox` inside the caller's own transaction, so every GRDB write
syncs — and a multi-row, multi-table transaction queues its uploads atomically with its rows.

```swift
try await db.write { try recipe.save($0) }                                   // upsert
try await db.write { try $0.execute(sql: "UPDATE recipes SET title = ? WHERE id = ?", arguments: [t, id]) }
try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = ?", arguments: [id]) }
```

An update that changes a row's primary key queues both an upsert of the new key and a tombstone for
the old. Declare `ON DELETE CASCADE` on child foreign keys to have a parent delete tombstone its
children. Internally, the engine suppresses the triggers (via a one-row `_sync_control.applying`
flag) while it writes server state locally, so downloads never re-enqueue themselves.
