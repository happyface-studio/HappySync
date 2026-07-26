# Getting Started

This walks you from zero to a working sync loop. Before your first sync completes, you also need the
server schema in **[[Server Setup]]** — the engine cannot correct a server that lacks the
`updatedAt` trigger.

## 1. Add the package

In `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/happyface-studio/HappySync.git", from: "0.4.0"),
],
targets: [
    .target(name: "MyApp", dependencies: ["HappySync"]),
]
```

Or in Xcode: **File → Add Package Dependencies →** `https://github.com/happyface-studio/HappySync`.

## 2. Declare your tables

Each synced table is a `SyncTable`, declared once, in FK-dependency order (parents before children).
Only `name` is required.

```swift
import HappySync

let tables = [
    SyncTable(name: "recipes", jsonColumns: ["nutrition"]),
    SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]),
    SyncTable(name: "recipeSteps", dependsOn: ["recipes"], jsonColumns: ["temperature"]),
]
```

See **[[API Reference]]** for every `SyncTable` field (`scopeColumn`, `conflictColumns`,
`serverOwnedColumns`, `serverColumns`, `cursorColumn`).

## 3. Create and start the engine

```swift
let engine = try SyncEngine(
    db: databaseQueue,                    // any GRDB DatabaseWriter — your local source of truth
    supabase: client,                     // your SupabaseClient
    tables: tables,
    auth: { await session.accessToken }   // return a FRESH access token each call
)

await engine.start()
```

`start()` is idempotent. It runs an immediate convergence sync, then keeps syncing from three
triggers — the Realtime doorbell (debounced), a periodic poll (converges even if Realtime drops),
and retry backoff — all funnelled through one serialized runner.

### Sharing a migrator

The engine's internal tables (`_sync_outbox`, `_sync_state`, `_sync_meta`) migrate at init. If your
app owns the GRDB `DatabaseMigrator`, register HappySync's migrations into it so both share one
`grdb_migrations` table:

```swift
var migrator = DatabaseMigrator()
SyncSchema.register(into: &migrator)   // happysync_v1..v3
// … your app migrations …
try migrator.migrate(databaseQueue)
```

## 4. Write

A write goes to GRDB **and** the outbox in one transaction, then returns optimistically. `enqueue`
wakes the uploader itself, so there's **no `syncNow()` needed after a write** — a burst debounces
into one drain pass.

```swift
try await engine.enqueue(.upsert, table: "recipes", row: recipe)   // recipe: some Encodable & Sendable
```

### Delete (with automatic cascade)

`enqueue(.delete, …)` removes the row locally and queues a tombstone. When the row is a **parent**
with children enforced by local foreign keys, the engine **cascades** — deletes the child rows too,
deepest-first, in the same transaction, and queues a tombstone for each. You don't enqueue child
deletes yourself, and the UI never shows orphaned children.

```swift
try await engine.enqueue(.delete, table: "recipes", row: ["id": recipeID])
```

## 5. Pull

Downloads happen automatically (doorbell + periodic). Call `pullNow()` for app-driven nudges —
returning to the foreground, pull-to-refresh:

```swift
try await engine.pullNow()
```

## 6. Show sync status

An `idle` status can still carry failing or parked uploads, so derive one health boolean:

```swift
for await status in engine.status {
    let healthy = status.phase == .idle
        && status.failedUploads == 0
        && status.deadLetters == 0
    // status.phase: .idle | .syncing | .failed(reason)
}
```

See **[[Operations and Troubleshooting]]** for what `failedUploads` / `deadLetters` mean and how to
repair parked writes.

## 7. Tear down on sign-out (order matters)

`stop()` is **async** and awaits the in-flight pass. Always `await engine.stop()` **before** wiping
or replacing the database on sign-out / account switch — otherwise an in-flight pass could write to
the store you're about to delete, or upload the old user's rows with the new user's token.

```swift
await engine.stop()          // engine is quiesced here
try await wipeLocalDatabase()
```

Status subscriptions survive teardown: `stop()` broadcasts a settled `.idle` status but doesn't end
the streams, so a `for await status in engine.status` loop keeps delivering after the next `start()`
— no re-subscribing needed.

## Next

- **[[Server Setup]]** — required before the first successful sync.
- **[[Operations and Troubleshooting]]** — health, dead letters, schema evolution.
- **[[Claude Skill]]** — let your AI assistant wire this correctly for you.
