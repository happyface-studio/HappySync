---
name: happysync
description: >-
  Integrate and operate HappySync — the single-user-multi-device GRDB ⇄ Supabase
  sync engine (outbox + cursor + last-write-wins, Realtime doorbell) — in a Swift
  app. Use when adding offline sync to an iOS/macOS app backed by Supabase, wiring
  a SyncEngine, declaring the SyncTable manifest, setting up the required server
  schema (updatedAt trigger, deletedAt tombstones, RLS, Realtime publication),
  driving sync-status UI, or diagnosing failed/dead-lettered uploads. Triggers on:
  "HappySync", "sync engine", "offline sync", "GRDB Supabase sync", "outbox",
  "dead letter", "SyncEngine", "SyncTable", "last-write-wins sync".
---

# HappySync

HappySync keeps a local **GRDB SQLite** store in sync with **Supabase Postgres** for
apps where the data is **single-user, multi-device — not collaborative**. That enabling
constraint makes **last-write-wins (LWW) by server timestamp** correct, so there are no
CRDTs and no conflict-resolution RPC.

**Requirements:** Swift 6, iOS 16+ / macOS 13+, GRDB 7.11+, supabase-swift 2.x.

## Mental model (read before wiring anything)

- **Local GRDB is the source of truth for reads.** Observe it with `ValueObservation`.
- **Writes go to GRDB *and* an outbox in the same transaction**, then return optimistically.
  A background drain uploads the outbox via PostgREST upsert (idempotent by primary key,
  retried with per-entry backoff, FK-ordered).
- **Downloads are a cursor pull:** `SELECT … WHERE updatedAt > :cursor ORDER BY (updatedAt, id)`,
  RLS-scoped, applied last-write-wins with a dirty-row guard (a pending local edit is never
  clobbered).
- **Supabase Realtime is a *doorbell only*.** A change event triggers a debounced `pullNow()`;
  payloads are **never** applied directly. All correctness lives in the idempotent cursor pull,
  so sync converges even if Realtime drops.
- **HappySync owns** the outbox drain, cursor pull, tombstones, FK ordering, the Realtime
  doorbell, status, and retry/backoff. It does **not** own reads or your schema.

## The four things you must get right

1. **Server schema** — every synced table needs a server-stamped `updatedAt` trigger, a
   `deletedAt` tombstone column, RLS scoped to `auth.uid()`, and Realtime publication. This is
   non-negotiable: LWW compares a *server* clock. See `references/server-setup.md`.
2. **The table manifest** — declare every synced table as a `SyncTable`, in FK dependency order.
   See "Declaring tables" below and `references/api.md`.
3. **Lifecycle** — `await engine.start()` after sign-in; **`await engine.stop()` before wiping
   or replacing the database** on sign-out / account switch. `stop()` is async and awaits the
   in-flight pass.
4. **Surfacing health** — an `idle` status can still carry failing or parked uploads. Health is
   `phase == .idle && failedUploads == 0 && deadLetters == 0`. See `references/operations.md`.

## Minimal integration

Add the package (`Package.swift`):

```swift
.package(url: "https://github.com/happyface-studio/HappySync.git", from: "0.4.0"),
// then add "HappySync" to your target's dependencies
```

Wire the engine:

```swift
import HappySync

let engine = try SyncEngine(
    db: databaseQueue,                       // any GRDB DatabaseWriter
    supabase: client,                        // your SupabaseClient
    tables: [
        SyncTable(name: "recipes", jsonColumns: ["nutrition"]),
        SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]),
    ],
    auth: { await session.accessToken }      // fresh access token per call
)

await engine.start()

// Write: goes to GRDB + outbox in one txn, uploads promptly (no syncNow() needed —
// enqueue wakes the runner; a burst debounces into one drain pass).
try await engine.enqueue(.upsert, table: "recipes", row: recipe)

// Delete: removes locally + queues a tombstone; cascades to FK children automatically.
try await engine.enqueue(.delete, table: "recipes", row: ["id": recipeID])

// App-driven nudge (foreground, pull-to-refresh):
try await engine.pullNow()
```

Drive status UI:

```swift
for await status in engine.status {
    let healthy = status.phase == .idle && status.failedUploads == 0 && status.deadLetters == 0
    // .idle / .syncing / .failed(reason); failedUploads = retrying, deadLetters = parked
}
```

Teardown on sign-out (**order matters**):

```swift
await engine.stop()        // quiesced here: no more DB writes, no network calls
try await wipeLocalDatabase()
```

## Declaring tables

`SyncTable` is the per-table descriptor. Only `name` is required; everything else is defaulted.
Common fields:

- `dependsOn: [String]` — parent tables referenced by FK. Drives upload/download/delete ordering.
- `jsonColumns: [String]` — columns stored as JSON text locally ↔ `json`/`jsonb` in Postgres.
- `scopeColumn: String?` — set **only** when RLS is broader than the sync partition (e.g. a
  public catalog readable as `isPublic OR userId = auth.uid()`); pair with the engine's `scope:`
  closure so the pull and doorbell filter to the user's rows instead of the whole catalog.
- `conflictColumns: [String]` — a server-side secondary `UNIQUE` constraint used as the upsert
  conflict target. **Leaf tables only** (the merge re-keys the server row to the client's pk).
- `serverOwnedColumns: [String]` — RPC-managed columns stripped from every upsert.
- `serverColumns: [String]` — optional allow-list to survive a dropped/renamed server column.

Full field semantics and gotchas: `references/api.md`. The complete, language-neutral wire
contract: the repo's `docs/SYNC-CONTRACT.md`.

## Reference files (load as needed)

- `references/server-setup.md` — the required Supabase schema: `updatedAt` trigger, `deletedAt`
  tombstone + child-cascade trigger, RLS, Realtime publication, tombstone purge, and the
  `maxOfflineGap` ≤ purge-retention invariant. **Read this before the first sync.**
- `references/api.md` — every public type and method (`SyncEngine`, `SyncTable`, `SyncStatus`,
  `DeadLetter`, `SyncOp`, `SyncError`), signatures, and per-field behavior.
- `references/operations.md` — status UI, dead-letter inspect/retry/discard, teardown/account
  switch, stale-cursor full resync, schema evolution, and troubleshooting.

## Rules of thumb

- **Never trust a client `updatedAt`.** The server trigger stamps it; LWW depends on it.
- **Don't apply Realtime payloads.** The doorbell only triggers a pull.
- **`await stop()` before touching the DB file** on sign-out/account switch, or an in-flight pass
  writes to the store you're deleting (or uploads the old user's rows with the new user's token).
- **A nonzero `failedUploads`/`deadLetters` on an idle status means degraded, not broken** —
  surface it; don't show a green checkmark.
- **Schema migrations are additive and staggered** (App Store lag): add columns server-first,
  remove them client-first. See `references/operations.md` → schema evolution.
