# HappySync — operating the engine

## Sync-status UI

Consume `engine.status` and ask `isHealthy`. `.idle` means settled **and** clean; a pass that
completed while writes are still failing or parked settles as `.degraded`.

```swift
for await status in engine.status {
    switch status.phase {
    case .idle:     showSynced()                    // settled and clean
    case .syncing:  showSpinner()
    case .degraded: showPending(status.failedUploads + status.deadLetters)
    case .failed(let failure): showBanner(failure)  // classified SyncFailure — see below
    }
    // failedUploads > 0  → some local changes haven't reached the server yet (retrying)
    // deadLetters   > 0  → some writes parked and need inspection/repair (see below)
}
```

## Failure model

A failed upload is classified, not swallowed:

- **Permanent** (constraint `23xxx`, RLS `42501`, undefined-column `42703`, other 4xx) →
  dead-lettered immediately.
- **Transient** (network, 5xx, 408/429, transient Postgres `40001`/`40P01`/`53xxx`/`08xxx`, unknown
  codes) → retried with per-entry exponential backoff (with jitter) until a cap, then dead-lettered.
- **Auth-shaped** (401/403, PostgREST `PGRST301`/`PGRST302`) → transient **and exempt from the retry
  budget**, so an expired-token stretch never dead-letters the whole outbox; it recovers when the
  token refreshes out-of-band.

A dead-lettered entry stops retrying **and** stops counting as a dirty row, so it never permanently
blocks downloads for its key.

### What the app sees

The classification is public as `SyncFailure`, on `Phase.failed` and on `DeadLetter.failure`. It
answers *what to show the user*; the retry decision is already made, and is visible as
`failedUploads` (still retrying) vs `deadLetters` (parked):

| `SyncFailure` | Raised by | What the app should do |
|---|---|---|
| `.network` | `URLError`, Postgres `08xxx` | Nothing — it recovers |
| `.authExpired` | 401/403, `PGRST301`/`PGRST302` | Nothing at first; prompt re-auth if it persists |
| `.permissionDenied` | `42501` | Surface it — a policy bug or a real permissions problem |
| `.constraintViolation(code:)` | `23xxx` | Inspect the row; the write will never land as-is |
| `.schemaMismatch(column:)` | `42703`, `PGRST204` | Prompt an app update (or restore the server column) |
| `.server(status:)` | any other HTTP status, incl. 5xx | Nothing — retried; surface if it persists |
| `.other(String)` | anything unrecognised, transient Postgres states | Log the text |

`.network` means the request got no answer at all; a server that answered with a 5xx is
`.server(status:)`, so the status code isn't thrown away. Never substring-match `lastError` — that's
what the enum exists to replace.

The kind and the retry decision agree everywhere except `PGRST204`: it classifies as
`.schemaMismatch` but is **retried** rather than parked immediately, because PostgREST also raises it
from a stale schema cache, which clears on its own.

Every case renders via `CustomStringConvertible`, so `"\(failure)"` is safe to put in a banner. The
classified cases carry no server prose of their own — the full text lives in the engine's log, and on
`DeadLetter.lastError` for a parked write.

## Repairing dead letters

```swift
let parked = try await engine.deadLetters()
for letter in parked {
    switch letter.failure {
    case .permissionDenied:              reportPolicyBug(letter) // fix RLS, then retryDeadLetters()
    case .schemaMismatch(let column):    promptAppUpdate(column)
    case .constraintViolation(let code): inspect(letter, code)
    default: log("\(letter.op) \(letter.table)/\(letter.pk): \(letter.lastError ?? "unknown")")
    }
}

// After fixing the cause (an RLS/policy change, a schema migration, an app update):
try await engine.retryDeadLetters()          // re-queue all (or pass [seq] for specific ones)

// Or abandon the local write and accept the server's version — discard drops the entries and
// re-pulls the affected rows so local converges back to the server (including changes the row
// missed while parked):
try await engine.discardDeadLetters([badSeq]) // specific seqs, or omit for all
```

Both mutations refresh the status stream immediately, so `deadLetters` drops as soon as they return.

## Teardown / account switch (order matters)

`stop()` is async and awaits the in-flight pass. A consumer that wipes or replaces the database on
sign-out / account switch **must `await engine.stop()` before touching the DB file** — otherwise an
in-flight pass could write to the store you're about to delete, or (mid-account-switch) upload the
old user's rows with the new user's token.

```swift
await engine.stop()          // engine is quiesced here
try await wipeLocalDatabase()
// build a fresh engine for the new account, then start()
```

`stop()` settles the status stream (`.idle`, keeping the outbox counts) but leaves subscriptions open,
so a `for await status in engine.status` loop resumes delivering after the next `start()` — reusing
the same engine across sign-out → sign-in needs no re-subscription.

## Stale-cursor full resync

A device offline past `maxOfflineGap` (default 30 days) can't trust its cursors — a tombstone it
never saw may already be purged server-side. Once `now − lastSyncedAt > maxOfflineGap` the engine
clears cursors, re-pulls, and reconciles purged deletes instead of resurrecting them. Keep
`maxOfflineGap` **≤ the server's tombstone-purge retention** (see `server-setup.md`). For a table
with a `scopeColumn`, the resync is deferred until a partition value resolves, so it never wipes an
un-pulled table.

## Schema evolution (staggered clients)

Shipped clients and the server are **not lockstep** — App Store review + staggered updates mean old
builds run against a newer server for weeks. The engine tolerates drift both ways, but follow these
rules so any single migration is safe for every client in the field:

- **Additive-only.** Never drop-and-recreate or repurpose a column in place.
- **Never rename in place** (a rename breaks clients in both directions). Add the new name,
  dual-write server-side, wait for adoption, then remove the old one by the removal rule.
- **Adding a column — server-first.** Deploy it (nullable/default) before any client reads/writes
  it. Old clients drop the unknown column on download and simply don't send it on upload.
- **Removing a column — client-first.** Drop it server-side **only after no shipped client still
  writes it**: (1) ship a build that stops sending it — optionally declaring `serverColumns` so the
  payload is intersected immediately; (2) wait out the update-lag window; (3) then drop it. Skipping
  step 2 dead-letters every write from clients still sending the column.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Writes never reach the server; `deadLetters` climbing | RLS rejects (`42501`) or a constraint violation | `deadLetters()` → switch on `failure` (`lastError` for the raw text); fix the policy/data, then `retryDeadLetters()`. |
| A device never sees the server's version of its **own** write | Missing `updatedAt` trigger (the engine already strips the client's `updatedAt` from every upload, so a stale client value can't be the cause) | Add the server trigger. |
| Rows on an insert-only table never download to other devices | The table's `cursorColumn` has no server default — the engine never uploads it, so it lands null | `default now()` (or a trigger) on that column server-side. |
| Whole public catalog downloads to every device | RLS broader than the partition, no `scopeColumn` | Declare `scopeColumn` + supply the engine `scope:` closure. |
| A fresh-pk insert 409s forever | Row already exists under a secondary `UNIQUE` | Declare `conflictColumns` (leaf tables only). |
| Every write on a table dead-letters after a server migration | Dropped/renamed server column (`PGRST204`) | Restore the column until clients stop sending it; or declare `serverColumns` as a backstop. |
| Deleted parent leaves orphaned children in the UI | Child FK not declared `ON DELETE CASCADE` locally, or server child-tombstone trigger missing | Declare `ON DELETE CASCADE` on the child FKs (`dependsOn` alone deletes nothing) and add the server child-tombstone trigger. |
| `SyncEngine.init` throws `missingLocalTable` / `missingPrimaryKeyColumn` | The `tables` manifest names a table (or key column) the local schema doesn't have | Run your app's migrations **before** constructing the engine; fix the manifest name. It throws rather than silently syncing nothing for that table. |
| Some writes sync, one code path never does | A write that bypasses the declared tables — writing a table not in the manifest, or a `DatabasePool` reader-side hack | Add the table to the manifest. Writes to declared tables are captured by triggers, so there is no way to "forget" to enqueue one. |
| Offline-long device resurrects deleted rows | `maxOfflineGap` > server tombstone retention | Set `maxOfflineGap` ≤ retention. |
| Idle status shows healthy but changes aren't syncing | Reading `phase == .idle` as health | Ask `status.isHealthy`. The engine now settles such a pass as `.degraded`, so a `switch` over the phase surfaces it. |
