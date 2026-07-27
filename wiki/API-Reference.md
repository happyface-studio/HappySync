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
| `func stop() async` | **Async.** Awaits the in-flight pass *and any in-flight `enqueue` transaction*, quiesces (no more DB writes / network), unsubscribes Realtime. Settles `status` to `.idle` but leaves subscriptions open. `start()` re-subscribes cleanly. Call **before** wiping/replacing the DB. |
| `func enqueue(_ op: SyncOp, table: String, row: some Encodable & Sendable) async throws` | Domain row + outbox entry in one transaction; wakes the runner. `.delete` cascades to FK children. Throws `SyncError`. |
| `func syncNow()` | Fire-and-forget nudge (foreground, pull-to-refresh). Not needed after `enqueue`. |
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
    dependsOn: [String] = [],              // FK parents; drives sync + delete ordering
    jsonColumns: [String] = [],            // JSON text locally ↔ json/jsonb remote
    serverOwnedColumns: [String] = [],     // *extra* RPC-managed columns; stripped too, applied on download
    scopeColumn: String? = nil,            // partition column when RLS is broader than the partition
    conflictColumns: [String] = [],        // secondary-unique upsert target; LEAF tables only
    serverColumns: [String] = []           // optional allow-list to survive a dropped/renamed column
)
```

| Field | Notes |
|---|---|
| `dependsOn` | Real FK dependencies. Parents upsert/download before children; children tombstone before parents. |
| `scopeColumn` | Requires the engine `scope:` closure. While no partition value is available (cold launch, signed out), the table's pull is skipped and the stale-resync check stays armed — never wipes an un-pulled table. |
| `conflictColumns` | Merges a fresh-pk insert onto an existing server row by a secondary `UNIQUE` (avoids a permanent 409). The merge **re-keys the server row to the client's pk** → **leaf tables only**, or you orphan children. |
| `serverColumns` | Opt-in backstop; a stale list silently stops uploading a real column. Prefer the client-first removal rule. Downloads need no equivalent (they intersect against the local schema). |
| `serverOwnedColumns` | Stripped from every upsert so a stale client value never clobbers the authoritative one; still arrive on download. Declare only the *extra* ones — the `cursorColumn` is always stripped. |
| `cursorColumn` | Server-stamped by contract (§1/§4), so the engine **never uploads it** — nothing to declare, and no way to opt out. Defence in depth for the one server-side requirement the engine can't verify: on a table whose trigger is missing, a client-sent value would silently make that device's clock the LWW authority. `deletedAt` is *not* stripped — an upsert carrying `deletedAt = null` is how a re-created row un-tombstones. |

## SyncStatus

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
carry failing or parked uploads — surface them.

## DeadLetter

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

## SyncOp / SyncError

```swift
public enum SyncOp: String, Codable { case upsert; case delete }

public enum SyncError: Error {
    case notImplemented(String)
    case unknownTable(String)                      // enqueue on an undeclared table
    case missingPrimaryKey(table: String, column: String)
    case encoding(String)
}
```

## SyncSchema (migrator sharing)

```swift
static func migrator() -> DatabaseMigrator                 // standalone; used when the engine owns the DB
static func register(into migrator: inout DatabaseMigrator) // prefer this when the app owns the migrator
```

Registers `happysync_v1..v3` — the `_sync_outbox`, `_sync_state`, and `_sync_meta` internal tables.
