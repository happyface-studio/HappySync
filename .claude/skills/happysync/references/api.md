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
| `func stop() async` | **Async** — awaits the in-flight pass, then quiesces (no more DB writes / network) and unsubscribes Realtime. `start()` re-subscribes cleanly. Call **before** wiping/replacing the DB. |
| `func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws` | **Deprecated (issue #48).** Performs the write; the table's capture trigger records it. Write GRDB directly instead. Throws `SyncError.unknownTable` / `.missingPrimaryKey` / `.encoding`. |
| `func syncNow()` | Fire-and-forget nudge (foreground return, pull-to-refresh). Not needed after a write — the engine wakes on queued writes itself. |
| `@discardableResult func pullNow() async throws -> [String: Set<String>]` | Runs a cursor pull now; returns the server primary keys seen per table. |
| `var status: AsyncStream<SyncStatus>` (`nonisolated`) | Live status for the sync-status UI. |
| `func deadLetters() async throws -> [DeadLetter]` | Inspect parked entries. |
| `func retryDeadLetters(_ seqs: [Int64]? = nil) async throws` | Re-queue parked writes (specific `seq`s, or all). Refreshes status immediately. |
| `func discardDeadLetters(_ seqs: [Int64]? = nil) async throws` | Drop parked writes and re-pull the affected rows so local converges to the server. |

## `SyncTable`

```swift
public init(
    name: String,                          // identical local + remote table name
    primaryKey: String = "id",
    cursorColumn: String = "updatedAt",    // change-time column the cursor orders/filters/advances by
    dependsOn: [String] = [],              // FK parents; drives sync + delete ordering
    jsonColumns: [String] = [],            // JSON text locally ↔ json/jsonb remote
    serverOwnedColumns: [String] = [],     // RPC-managed; stripped from every upsert, applied on download
    scopeColumn: String? = nil,            // partition column when RLS is broader than the partition
    conflictColumns: [String] = [],        // secondary-unique upsert target; LEAF tables only
    serverColumns: [String] = []           // optional allow-list to survive a dropped/renamed column
)
```

Field gotchas:

- **`dependsOn`** must reflect real FK dependencies — parents upsert/download before children;
  children tombstone before parents.
- **`scopeColumn`** requires the engine's `scope:` closure to resolve the partition value per
  signed-in user. While no value is available (cold launch before session restore, or signed out),
  the pull for that table is skipped and the stale-resync check stays armed — it never wipes an
  un-pulled table.
- **`conflictColumns`** merges a fresh-pk insert onto an existing server row by a secondary
  `UNIQUE` constraint (avoids a permanent 409). The merge **re-keys the server row to the client's
  pk**, so declaring it on a non-leaf table orphans children — **leaf tables only**.
- **`serverColumns`** is an opt-in client-side backstop; a stale list silently stops uploading a
  real column. Prefer the operational client-first removal rule; use this only when you accept the
  maintenance cost. Downloads need no equivalent — they intersect against the local schema.

## `SyncStatus`

```swift
public struct SyncStatus {
    public enum Phase { case idle; case syncing; case failed(String) }
    public var phase: Phase
    public var lastSyncedAt: Date?     // last successful pull/push, or nil
    public var failedUploads: Int      // entries retrying with backoff (attempts > 0, not parked)
    public var deadLetters: Int        // entries parked after a permanent 4xx or exhausted retries
}
```

**Health = `phase == .idle && failedUploads == 0 && deadLetters == 0`.** An idle status can still
carry failing or parked uploads — surface them; don't render a green checkmark.

## `DeadLetter`

```swift
public struct DeadLetter {
    public let seq: Int64        // pass to retryDeadLetters / discardDeadLetters
    public let table: String
    public let pk: String
    public let op: SyncOp        // .upsert | .delete
    public let attempts: Int     // upload attempts charged before parking
    public let lastError: String?// the repair breadcrumb — why it parked
    public let queuedAt: Date
}
```

## `SyncOp` / `SyncError`

```swift
public enum SyncOp: String, Codable { case upsert; case delete }

public enum SyncError: Error {
    case notImplemented(String)
    case unknownTable(String)                    // deprecated enqueue on an undeclared table
    case missingPrimaryKey(table: String, column: String)
    case encoding(String)
    // Thrown at init when the manifest and the local schema disagree (issue #48):
    case missingLocalTable(String)               // declared SyncTable has no local table
    case missingPrimaryKeyColumn(table: String, column: String)
    case recursiveTriggersUnavailable            // PRAGMA recursive_triggers could not be enabled
}
```

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
