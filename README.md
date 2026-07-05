# HappySync

A small, single-user-multi-device sync engine for **GRDB** ⇄ **Supabase**, built on the
enabling constraint that personal data is *not collaborative*: one user edits their own rows,
occasionally from two devices. That makes **last-write-wins by server timestamp** correct — no
CRDTs needed.

> Status: **M1 complete.** Upload (transactional `enqueue` + FK-ordered, idempotent, retrying
> outbox drain — APPS-413), download (`pullNow`: tuple `(updated_at, id)` cursor, last-write-wins
> with dirty-row protection, tombstones, pagination — APPS-414), and the scheduler that drives them
> (`start`: debounced Realtime doorbell, periodic fallback, status stream, exponential backoff —
> APPS-415) are all live. Next is M2 server prep (server-side `updated_at` triggers + `deleted_at`
> tombstones) before the M3 CookThis cutover.

## Model

Local GRDB SQLite is the source of truth for reads (observed with `ValueObservation`). Writes
go to GRDB **and an outbox in the same transaction**, then return optimistically. A background
uploader drains the outbox via PostgREST upsert. A downloader pulls rows changed since a
per-table `(updated_at, id)` cursor, RLS-scoped to the user, applied last-write-wins. **Supabase
Realtime is a doorbell only** — a change event triggers a debounced `pullNow()`; payloads are
never applied directly, so all correctness lives in the idempotent cursor-pull.

Server-authoritative `updated_at` (a Postgres `BEFORE INSERT/UPDATE` trigger → `now()`) is
required for LWW correctness. Deletes propagate as `deleted_at` tombstones.

HappySync owns the outbox drain, cursor pull, tombstones, FK ordering, Realtime doorbell,
status, and retry/backoff. It does **not** own reads or schema.

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
        SyncTable(name: "recipes", primaryKey: "id", dependsOn: [], jsonColumns: ["nutrition"]),
    ],
    auth: { await session.accessToken }
)

await engine.start()
try await engine.enqueue(.upsert, table: "recipes", row: recipe)
// No syncNow() needed after a write: enqueue wakes the runner itself, so the change uploads
// promptly (debounced, so a burst coalesces into one drain pass). Call syncNow() only for
// app-driven nudges like returning to the foreground or pull-to-refresh.
try await engine.pullNow()

for await status in engine.status {
    // drive sync-status UI: .idle / .syncing / .failed.
    // Health is `phase == .idle && failedUploads == 0 && deadLetters == 0` — an idle status can
    // still carry failing/parked uploads (APPS-470).
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

## Requirements

- Swift 6, iOS 16+ / macOS 13+
- [GRDB.swift](https://github.com/groue/GRDB.swift) 7.11+
- [supabase-swift](https://github.com/supabase/supabase-swift) 2.x

## Consumer #1

[CookThis](https://github.com/happyface-studio/CookThis) is the first consumer. The API stays
deliberately generic, but is pressure-tested against one real app before it's treated as stable.

## License

MIT — see [LICENSE](LICENSE).
