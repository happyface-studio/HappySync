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
Only `name` is required. Every declared table must **already exist** in your local schema when the
engine is constructed — it installs write-capture triggers on each one, and throws
`SyncError.missingLocalTable` if the manifest and the schema disagree. Run your migrations first.

Declare `ON DELETE CASCADE` on child foreign keys if you want a parent delete to remove (and
tombstone) its children — see [Delete](#delete-with-cascade) below.

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

The engine's internal tables (`_sync_outbox`, `_sync_state`, `_sync_meta`, `_sync_control`) migrate
at init. If your app owns the GRDB `DatabaseMigrator`, register HappySync's migrations into it so
both share one `grdb_migrations` table:

```swift
var migrator = DatabaseMigrator()
SyncSchema.register(into: &migrator)   // happysync_v1..v4
// … your app migrations …
try migrator.migrate(databaseQueue)
```

The **write-capture triggers** are not part of that migrator. They're derived from your `tables`
manifest — source code, which ships without a schema version bump — so the engine (re)installs them
idempotently at every init and drops the ones for tables you've stopped syncing.

## 4. Write

Write GRDB **normally**. Each declared table carries `AFTER INSERT` / `UPDATE` / `DELETE` triggers
that append to the outbox in your transaction, so there is no second API to call and nothing to
forget. The engine notices the queued write and uploads promptly, so there's **no `syncNow()` needed
after a write** — a burst debounces into one drain pass.

```swift
try await db.write { try recipe.save($0) }                       // PersistableRecord

try await db.write { db in                                        // partial / expression updates
    try db.execute(sql: "UPDATE recipes SET title = ? WHERE id = ?", arguments: [title, id])
    try db.execute(sql: "UPDATE recipes SET cookCount = cookCount + 1 WHERE id = ?", arguments: [id])
}

try await db.write { db in                                        // one transaction = one user intent
    try recipe.save(db)
    for ingredient in ingredients { try ingredient.save(db) }
}
```

That last one matters: the rows and their queued uploads commit together, or neither does. A crash
part-way through leaves no half-recipe *and* no half-batch in the outbox.

> `engine.enqueue(op:table:row:)` still works but is **deprecated**. It now just performs the write
> and lets the trigger queue it. Replace `enqueue(.upsert, table: t, row: r)` with
> `try await db.write { try r.save($0) }`, and `enqueue(.delete, …)` with a plain `DELETE`.

### Delete (with cascade)

A plain `DELETE` removes the row locally and queues a tombstone. For a **parent** row, declare
`ON DELETE CASCADE` on the child foreign keys — SQLite then walks the graph and each removed child's
own trigger queues its tombstone, so you never delete children by hand and the UI never shows
orphans.

```swift
// In your migration:
try db.create(table: "recipeIngredients") { t in
    t.column("id", .text).primaryKey()
    t.column("recipeId", .text).references("recipes", onDelete: .cascade)
    // …
}

// Then:
try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = ?", arguments: [recipeID]) }
```

A table that only `dependsOn` a parent without an FK action is **not** cascaded — `dependsOn` orders
uploads, it doesn't delete rows. Its orphans reconcile on the next pull via the server tombstone.

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

## Next

- **[[Server Setup]]** — required before the first successful sync.
- **[[Operations and Troubleshooting]]** — health, dead letters, schema evolution.
- **[[Claude Skill]]** — let your AI assistant wire this correctly for you.
