# HappySync

A small, single-user-multi-device sync engine for **GRDB** ⇄ **Supabase**, built on the
enabling constraint that personal data is *not collaborative*: one user edits their own rows,
occasionally from two devices. That makes **last-write-wins by server timestamp** correct — no
CRDTs needed.

> Status: **M1 complete.** Upload (trigger-captured writes + FK-ordered, idempotent, retrying
> outbox drain — APPS-413), download (`pullNow`: tuple `(updated_at, id)` cursor, last-write-wins
> with dirty-row protection, tombstones, pagination — APPS-414), and the scheduler that drives them
> (`start`: debounced Realtime doorbell, periodic fallback, status stream, exponential backoff —
> APPS-415) are all live. Next is M2 server prep (server-side `updated_at` triggers + `deleted_at`
> tombstones) before the M3 CookThis cutover.

## Model

Local GRDB SQLite is the source of truth for reads (observed with `ValueObservation`). You write
GRDB **normally** — `save`, a partial `UPDATE`, raw SQL, a whole multi-table transaction — and
per-table SQLite triggers record each change in an outbox **in that same transaction**, then the
write returns optimistically. A background uploader drains the outbox via PostgREST upsert. A
downloader pulls rows changed since a per-table `(updated_at, id)` cursor, RLS-scoped to the user,
applied last-write-wins — behind a control flag the triggers read, so downloaded rows don't queue
themselves straight back for upload. **Supabase
Realtime is a doorbell only** — a change event triggers a debounced `pullNow()`; payloads are
never applied directly, so all correctness lives in the idempotent cursor-pull.

Server-authoritative `updated_at` (a Postgres `BEFORE INSERT/UPDATE` trigger → `now()`) is
required for LWW correctness. Deletes propagate as `deleted_at` tombstones.

HappySync owns the write-capture triggers, the outbox drain, cursor pull, tombstones, FK ordering,
Realtime doorbell, status, and retry/backoff. It does **not** own reads or your schema — it only
installs its own triggers on the tables you declare.

The full, language-neutral contract every client and the server must honor (server conventions,
wire semantics, field mapping, and the per-table manifest) lives in
[docs/SYNC-CONTRACT.md](docs/SYNC-CONTRACT.md).

## Usage

```swift
import HappySync

let engine = try SyncEngine(
    db: databaseQueue,
    supabase: client,
    tables: [
        SyncTable(name: "recipes", primaryKey: "id", jsonColumns: ["nutrition"]),
    ],
    auth: { await session.accessToken }
)

await engine.start()

// Just write. The table's capture trigger queues the upload in the same transaction — including
// a multi-row, multi-table write, which uploads as one atomic user intent.
try await databaseQueue.write { db in
    try recipe.save(db)
    for ingredient in ingredients { try ingredient.save(db) }
}
// No syncNow() needed: the engine notices the queued writes and uploads promptly (debounced, so a
// burst coalesces into one drain pass). Call syncNow() only for app-driven nudges like returning
// to the foreground or pull-to-refresh.
try await engine.pullNow()

for await status in engine.status {
    // drive sync-status UI: .idle / .syncing / .degraded / .failed(SyncFailure).
    // Ask `status.isHealthy` — .degraded is a settled pass with writes still failing or parked,
    // and .failed carries a classified cause you can branch on (APPS-470).
}
```

For a table whose RLS is broader than the sync partition (e.g. a shared `recipes` table readable as
`isPublic OR userId = auth.uid()`), declare a `scopeColumn` and supply the partition value so the
engine downloads only the user's rows instead of the whole catalog:

```swift
SyncTable(name: "recipes", jsonColumns: ["nutrition"], scopeColumn: "userId")
// …and on the engine:
SyncEngine(db:, supabase:, tables:, auth: { await session.accessToken },
           scope: { await session.user?.id.uuidString })
```

For a table with a **secondary unique constraint** on the server (beyond its primary key), declare
`conflictColumns` so the engine upserts with that constraint as the PostgREST conflict target. A
device that mints a fresh primary key for a row the server already holds under the unique
constraint (e.g. created on another device, or server-side, and not yet pulled) would otherwise
409 on every retry and poison its outbox forever:

```swift
// userRecipeInteractions has UNIQUE(userId, recipeId) on the server:
SyncTable(name: "userRecipeInteractions", conflictColumns: ["userId", "recipeId"])
```

The merge re-keys the server row to the client's primary key, so **only declare `conflictColumns`
on a leaf table** — one whose primary key nothing else foreign-keys — or you orphan its children.

## Writes

Every table you declare gets three SQLite triggers (`AFTER INSERT` / `UPDATE` / `DELETE`) that append
to HappySync's outbox. So **any** write syncs, from any source, with no parallel API to remember:

```swift
try await db.write { try recipe.save($0) }                       // PersistableRecord
try await db.write { try $0.execute(sql:                          // partial update
    "UPDATE recipes SET title = ? WHERE id = ?", arguments: [title, id]) }
try await db.write { try $0.execute(sql:                          // expression update
    "UPDATE userRecipeInteractions SET cookedCount = cookedCount + 1 WHERE id = ?", arguments: [id]) }
```

Because the outbox entries are written by the database, in your transaction, a multi-row write is one
unit: "create a recipe + 12 ingredients" commits the rows *and* their 13 queued uploads together, or
neither. A crash halfway leaves no half-recipe and no half-batch.

The engine declares its manifest against your schema, so **the tables you declare must exist** when
you construct the engine (run your migrations first). A `SyncTable` naming a table — or a
`primaryKey` column — that isn't there throws at init rather than silently syncing nothing.

> **Migrating from `enqueue`.** `engine.enqueue(op:table:row:)` still works and is deprecated for one
> release; it now just performs the write and lets the trigger queue it. Replace
> `enqueue(.upsert, table: t, row: r)` with `try await db.write { try r.save($0) }` and
> `enqueue(.delete, …)` with a plain `DELETE`. **This changes behaviour for anyone who was
> deliberately writing outside the engine** — those writes now upload.

## Deletes

A plain `DELETE` removes the row locally and queues a tombstone that soft-deletes it server-side on
the next drain.

For a **parent** row, declare `ON DELETE CASCADE` on the child foreign keys and the database does the
rest: it walks the graph, and each removed child's own delete trigger queues its tombstone. So you
don't delete children yourself, a parent delete never throws a raw SQLite FK error mid-flow, and
there's no window where the UI shows orphaned children of a recipe that no longer exists. This
mirrors the server's child-tombstone trigger, so local and server converge on the same deleted set
with no round-trip.

```swift
// In your migration:
try db.create(table: "recipeIngredients") { t in
    t.column("id", .text).primaryKey()
    t.column("recipeId", .text).references("recipes", onDelete: .cascade)
    // …
}

// Then deleting a recipe removes its recipeIngredients / recipeSteps / recipeStepIngredients
// locally and queues a tombstone for each:
try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = ?", arguments: [recipeID]) }
```

A table that only *logically* `dependsOn` a parent, with no FK action declared, is **not** cascaded —
`dependsOn` orders uploads, it doesn't delete rows. Its orphans reconcile on the next pull via the
server tombstone instead.

The engine enables `PRAGMA recursive_triggers` on the writer connection at init (and throws if it
can't). That's what makes an `INSERT OR REPLACE` which displaces a row by a unique index queue that
row's tombstone, rather than dropping it locally and leaving the server's copy alive forever.

## Teardown

`stop()` is **async and awaits the in-flight sync pass** before returning — after it returns the
engine has quiesced (no further DB writes, no network calls). A consumer that wipes or replaces the
database on sign-out / account switch **must `await engine.stop()` before touching the database
file**, or an in-flight pass could write to the store you're about to delete (and, mid-account-
switch, upload the old user's rows with the new user's token). `stop()` also unsubscribes the
Realtime channel; `start()` re-subscribes cleanly.

```swift
await engine.stop()   // engine is quiesced here
try await wipeLocalDatabase()
```

**Status streams survive a stop/start cycle.** `stop()` broadcasts a settled `.idle` status (keeping
`failedUploads`/`deadLetters`, which live in the outbox) but does **not** end the streams — so the
usual long-lived consumer keeps working across sign-out → wipe → sign-in, and doesn't need to be
re-subscribed after `start()`:

```swift
.task { for await status in engine.status { self.status = status } }  // survives stop() → start()
```

## Repairing dead letters

A write that fails permanently (an RLS reject, a constraint violation) — or exhausts its retries —
is **dead-lettered**: parked in the outbox so it stops retrying and no longer blocks downloads for
its row. `SyncStatus.deadLetters` counts them; these three methods let you inspect and repair them
instead of hand-editing `_sync_outbox`:

```swift
let parked = try await engine.deadLetters()
for letter in parked {
    // letter.table / .pk / .op — which write parked, and .lastError — why.
    print("\(letter.op) \(letter.table)/\(letter.pk) failed: \(letter.lastError ?? "unknown")")
}

// After fixing the cause (an RLS/policy change, a schema migration, an app update), re-queue the
// parked writes so the drain uploads them again. Pass specific seqs, or omit for all.
try await engine.retryDeadLetters()

// Or abandon the local write and accept the server's version. Discard drops the entries, then
// re-pulls the affected rows so the local copy converges back to what the server holds — including
// re-pulling changes the row missed while it was parked (APPS-505). Pass specific seqs, or all.
try await engine.discardDeadLetters([badSeq])
```

Both mutations refresh the status stream immediately, so `deadLetters` drops as soon as they return.

## Testing your integration

Sync is where bugs are silent and data-losing, and it's usually untested — because testing it looks
like it needs a live Supabase project. It doesn't. The `SyncRemote` / `SyncDoorbell` seams the
engine's own tests run on are public, and the `HappySyncTestSupport` product ships the fakes:

```swift
// In your test target's dependencies: .product(name: "HappySyncTestSupport", package: "HappySync")
import HappySync
import HappySyncTestSupport

let remote = InMemorySyncRemote()      // a server in the test process: seedable, recording, failable
let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: MyApp.syncTables)
await engine.start()

// One plain delete — SQLite cascades, and each child's capture trigger queues its own tombstone.
try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = 'r1'") }
#expect(await eventually { await remote.deleteCalls.count == 3 })
```

Everything but the network is the code that ships, so what the test proves is what the app does.
Inject failures with `InMemorySyncRemote(failUpserts: 1, upsertFailure: .permanent)` to reach the
dead-letter path, ring a `ManualDoorbell` to drive convergence on demand, and hold a call in flight
with `onUpsert`/`onFetch` to race a local write against an upload. See the
[Testing](https://github.com/happyface-studio/HappySync/wiki/Testing) wiki page.

## Requirements

- Swift 6, iOS 16+ / macOS 13+
- [GRDB.swift](https://github.com/groue/GRDB.swift) 7.11+
- [supabase-swift](https://github.com/supabase/supabase-swift) 2.x

## Consumer #1

[CookThis](https://github.com/happyface-studio/CookThis) is the first consumer. The API stays
deliberately generic, but is pressure-tested against one real app before it's treated as stable.

## Documentation & Claude skill

- **[Wiki](https://github.com/happyface-studio/HappySync/wiki)** — Getting Started, Server Setup,
  API Reference, Operations & Troubleshooting, and Architecture. (Sources live in [`wiki/`](wiki).)
- **[Sync contract](docs/SYNC-CONTRACT.md)** — the language-neutral contract every client and the
  server must honor.
- **Claude skill** — [`.claude/skills/happysync/`](.claude/skills/happysync) is an installable
  [Claude Code](https://claude.com/claude-code) skill that teaches an AI assistant to integrate and
  operate HappySync correctly. Copy it into your app's `.claude/skills/` (or `~/.claude/skills/`)
  and invoke `/happysync`. See the [Claude Skill wiki page](https://github.com/happyface-studio/HappySync/wiki/Claude-Skill).

## License

MIT — see [LICENSE](LICENSE).
