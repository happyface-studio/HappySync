# API Reference

All types are in the `HappySync` module. `SyncEngine` is an `actor`; its async methods are `await`ed
and safe to call from any task.

## SyncEngine

```swift
public init(
    db: any DatabaseWriter,                                   // GRDB writer; local source of truth
    supabase: SupabaseClient,
    tables: [SyncTable],                                      // the manifest, declared once
    auth: @escaping @Sendable () async -> String,             // fresh access token per call
    scope: @escaping @Sendable () async -> String? = { nil }, // partition value for scopeColumn tables
    maxOfflineGap: TimeInterval = 30 * 24 * 3600              // stale-cursor full-resync threshold
) throws
```

| Method | Behavior |
|---|---|
| `func start()` | Begins background sync. Idempotent. Immediate convergence sync, then drives from the Realtime doorbell, a periodic poll, and retry backoff through one serialized runner. |
| `func stop() async` | **Async.** Awaits the in-flight pass, quiesces (no more DB writes / network), unsubscribes Realtime. Settles `status` to `.idle` but leaves subscriptions open. `start()` re-subscribes cleanly. Call **before** wiping/replacing the DB. |
| `func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) throws` | **Deprecated.** Performs the write; the table's capture trigger queues it. Write GRDB directly instead. Throws `SyncError`. |
| `func syncNow()` | Fire-and-forget nudge (foreground, pull-to-refresh). Not needed after a write — the engine wakes on queued writes itself. |
| `@discardableResult func pullNow() async throws -> [String: Set<String>]` | Cursor pull now; returns server primary keys seen per table. |
| `var status: AsyncStream<SyncStatus>` | `nonisolated`. Live status for the UI. Each access is an independent stream replaying the latest snapshot; it survives `stop()`/`start()` and only ends when the engine is deallocated. |
| `func deadLetters() async throws -> [DeadLetter]` | Inspect parked entries. |
| `func retryDeadLetters(_ seqs: [Int64]? = nil) async throws` | Re-queue parked writes (specific `seq`s, or all). |
| `func discardDeadLetters(_ seqs: [Int64]? = nil) async throws` | Drop parked writes and re-pull affected rows so local converges to the server. |

## SyncTable

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

| Field | Notes |
|---|---|
| `dependsOn` | FK dependencies. Parents upsert/download before children; children tombstone before parents. Leave it `nil` (the default) and the engine reads `PRAGMA foreign_key_list` at init and uses the table's real FK parents. Declare it only to **override** that for a *logical* parent with no FK constraint — an explicit list replaces the derived one rather than adding to it, and every name in it must be a declared `SyncTable`. A self-reference (a tree table) is dropped from either form. |
| `scopeColumn` | Requires the engine `scope:` closure. While no partition value is available (cold launch, signed out), the table's pull is skipped and the stale-resync check stays armed — never wipes an un-pulled table. |
| `conflictColumns` | Merges a fresh-pk insert onto an existing server row by a secondary `UNIQUE` (avoids a permanent 409). The merge **re-keys the server row to the client's pk** → **leaf tables only**, or you orphan children. |
| `serverColumns` | Opt-in backstop; a stale list silently stops uploading a real column. Prefer the client-first removal rule. Downloads need no equivalent (they intersect against the local schema). |
| `serverOwnedColumns` | Stripped from every upsert so a stale client value never clobbers the authoritative one; still arrive on download. Declare only the *extra* ones — the `cursorColumn` is always stripped. |
| `cursorColumn` | Server-stamped by contract (§1/§4), so the engine **never uploads it** — nothing to declare, and no way to opt out. Defence in depth for the one server-side requirement the engine can't verify: on a table whose trigger is missing, a client-sent value would silently make that device's clock the LWW authority. `deletedAt` is *not* stripped — an upsert carrying `deletedAt = null` is how a re-created row un-tombstones. |

## SyncStatus

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

**Ask `status.isHealthy`.** `.idle` on a status the engine broadcast already implies it — the engine
derives `.idle` vs `.degraded` from the counts — but `isHealthy` is the one read that stays correct
on a `SyncStatus` you construct yourself, and it's the one that documents the intent.

## SyncFailure

The classified cause carried by `Phase.failed` and `DeadLetter.failure` — so an app can branch on
*why* instead of substring-matching PostgREST prose.

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
you a write is still being retried or has been parked.

## DeadLetter

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

## SyncOp / SyncError

```swift
public enum SyncOp: String, Codable { case upsert; case delete }

public enum SyncError: Error, CustomStringConvertible {
    case notImplemented(String)
    case unknownTable(String)                      // deprecated enqueue on an undeclared table
    case missingPrimaryKey(table: String, column: String)
    case encoding(String)
    case invalidManifest(table: String, reason: ManifestProblem)   // the manifest doesn't match the schema (init)
    case recursiveTriggersUnavailable              // PRAGMA recursive_triggers could not be enabled
}

public enum ManifestProblem: Equatable, CustomStringConvertible {
    case duplicateDeclaration                             // the same table declared twice
    case noSuchTable                                      // no local table of that name
    case noSuchColumn(field: String, column: String)      // a field names a column the table lacks
    case unknownDependency(String)                        // dependsOn names an undeclared SyncTable
    case conflictColumnsOnParentTable(referencedBy: String)
    case serverColumnsOmitPrimaryKey(String)
}
```

### Manifest validation

`SyncEngine.init` checks every declared `SyncTable` against the local schema before it installs a
single trigger, and throws `SyncError.invalidManifest` naming the table, the field and the value.
Each of these used to fail silently and late — the wrong partition downloads, a column corrupts in
both directions, an FK-order violation surfaces days later as a dead letter on another device.

- `name` must be a local table; `primaryKey`, `cursorColumn`, `scopeColumn`, `jsonColumns`,
  `conflictColumns` and `serverOwnedColumns` must each name a column on it (matched
  case-insensitively, as SQLite does).
- `serverColumns` is **not** checked against the local schema — it describes the *server's* schema,
  which is allowed to differ. It may not omit the primary key, since the payload is intersected
  against it.
- `conflictColumns` is rejected on a table anything else foreign-keys, enforcing the leaf-only rule
  that was previously documentation alone. The scan covers unsynced children too.
- `dependsOn` entries must name declared tables.
- The same table may not be declared twice.

`SyncError` and `ManifestProblem` both print a full sentence, so the message stands on its own in a
crash log.

## Writing

There is no write API. Each declared table gets `AFTER INSERT` / `AFTER UPDATE` / `AFTER DELETE`
triggers that append to `_sync_outbox` inside your own transaction, so any GRDB write syncs:

```swift
try await db.write { try recipe.save($0) }
try await db.write { try $0.execute(sql: "UPDATE recipes SET title = ? WHERE id = ?", arguments: [t, id]) }
try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = ?", arguments: [id]) }
```

Declare `ON DELETE CASCADE` on child foreign keys to have a parent delete tombstone its children.
The engine enables `PRAGMA recursive_triggers` on the writer connection at init.

## SyncSchema (migrator sharing)

```swift
static func migrator() -> DatabaseMigrator                 // standalone; used when the engine owns the DB
static func register(into migrator: inout DatabaseMigrator) // prefer this when the app owns the migrator
// The capture triggers are NOT migrations — the engine installs them idempotently at init.
```

Registers `happysync_v1..v3` — the `_sync_outbox`, `_sync_state`, and `_sync_meta` internal tables.
