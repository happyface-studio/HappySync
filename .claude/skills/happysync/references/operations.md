# HappySync — operating the engine

## Sync-status UI

Consume `engine.status` and derive a single health boolean. An `idle` status is **not** the same as
"everything uploaded".

```swift
for await status in engine.status {
    switch status.phase {
    case .idle:     break
    case .syncing:  showSpinner()
    case .failed(let reason): showBanner(reason)
    }
    let healthy = status.phase == .idle && status.failedUploads == 0 && status.deadLetters == 0
    // failedUploads > 0  → some local changes haven't reached the server yet (degraded, retrying)
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

## Repairing dead letters

```swift
let parked = try await engine.deadLetters()
for letter in parked {
    print("\(letter.op) \(letter.table)/\(letter.pk) failed: \(letter.lastError ?? "unknown")")
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
| Writes never reach the server; `deadLetters` climbing | RLS rejects (`42501`) or a constraint violation | `deadLetters()` → read `lastError`; fix the policy/data, then `retryDeadLetters()`. |
| A device never sees the server's version of its **own** write | Missing `updatedAt` trigger, or a client-sent `updatedAt` | Add the server trigger; never send `updatedAt` from the client. |
| Whole public catalog downloads to every device | RLS broader than the partition, no `scopeColumn` | Declare `scopeColumn` + supply the engine `scope:` closure. |
| A fresh-pk insert 409s forever | Row already exists under a secondary `UNIQUE` | Declare `conflictColumns` (leaf tables only). |
| Every write on a table dead-letters after a server migration | Dropped/renamed server column (`PGRST204`) | Restore the column until clients stop sending it; or declare `serverColumns` as a backstop. |
| Deleted parent leaves orphaned children in the UI | No local FK constraint, or server child-tombstone trigger missing | Add real FK constraints (local cascade) and the server child-tombstone trigger. |
| Offline-long device resurrects deleted rows | `maxOfflineGap` > server tombstone retention | Set `maxOfflineGap` ≤ retention. |
| Idle status shows healthy but changes aren't syncing | Reading `phase` only | Health must include `failedUploads == 0 && deadLetters == 0`. |
