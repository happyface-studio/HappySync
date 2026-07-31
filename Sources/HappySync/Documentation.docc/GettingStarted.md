# Getting Started

This walks you from zero to a working sync loop. Before your first sync completes, you also need the
server schema in **<doc:ServerSetup>** — the engine cannot correct a server that lacks the
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

Each synced table is a `SyncTable`, declared once, **in any order** — the engine sorts the manifest
by FK dependency itself. Only `name` is required.

Every declared table must **already exist** in your local schema when the engine is constructed, so
run your migrations first. `SyncEngine.init` validates the whole manifest against that schema and
throws `SyncError.invalidManifest(table:reason:)` — naming the table, the field and the value — if
anything disagrees: a table that isn't there, or a `cursorColumn` / `scopeColumn` / `jsonColumns` /
`conflictColumns` / `serverOwnedColumns` entry naming a column the table doesn't have. Each of those
used to fail silently and late, so it's worth reading the message rather than working around it.

You don't declare `dependsOn`: the engine reads your foreign keys at init and derives it. Pass one
explicitly only for a *logical* parent your schema has no FK for (see
[Cascade](#delete-with-cascade)).

Declare `ON DELETE CASCADE` on child foreign keys if you want a parent delete to remove (and
tombstone) its children — see [Delete](#delete-with-cascade) below.

```swift
import HappySync

let tables = [
    SyncTable(name: "recipes", jsonColumns: ["nutrition"]),
    SyncTable(name: "recipeIngredients"),                            // dependsOn derived from the FK
    SyncTable(name: "recipeSteps", jsonColumns: ["temperature"]),
]
```

``SyncTable`` documents every field it takes — `scopeColumn`, `conflictColumns`,
`serverOwnedColumns`, `serverColumns`, `cursorColumn` — including when to reach for each and what
breaks if you get it wrong.

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

Ask `isHealthy` — never `phase == .idle` alone. A settled pass with writes still failing or parked
is `.degraded`, so switching on the phase forces you to confront it:

```swift
for await status in engine.status {
    guard !status.isHealthy else { return showSynced() }
    switch status.phase {
    case .idle, .syncing: break                     // .idle here means healthy; .syncing is in flight
    case .degraded: showPending(status.failedUploads + status.deadLetters)
    case .failed(.authExpired): promptReauth()      // classified — no string matching
    case .failed(.network): break                   // recovers on its own; show nothing
    case .failed(let failure): showBanner(failure)
    }
}
```

See **<doc:Operations>** for what `failedUploads` / `deadLetters` mean and how to
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

- **<doc:ServerSetup>** — required before the first successful sync.
- **<doc:Operations>** — health, dead letters, schema evolution.
- **[Claude Skill](https://github.com/happyface-studio/HappySync/wiki/Claude-Skill)** — let your AI assistant wire this correctly for you.
