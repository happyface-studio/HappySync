# ``HappySync``

Sync one user's data between their devices, over GRDB and Supabase, without pretending it's a
collaborative editing problem.

## Overview

Personal data is not collaborative: one user edits their own rows, occasionally from two devices.
That single constraint is what makes **last-write-wins by a server timestamp** correct — no CRDTs,
no conflict-resolution RPC — and it's what the whole engine is built on.

The model in five sentences:

1. **Local GRDB SQLite is the source of truth for reads.** You keep observing it with
   `ValueObservation`; HappySync never sits between your app and its data.
2. **You write GRDB normally** — `save`, a partial `UPDATE`, raw SQL, a whole multi-table
   transaction — and per-table SQLite triggers record each change into an outbox *in that same
   transaction*, so a write can't be half-committed and half-queued.
3. **A background drain uploads the outbox** via PostgREST upsert, parents before children, batched
   per table, idempotent by primary key, retrying with per-entry backoff.
4. **A pull downloads rows changed since a per-table `(updatedAt, id)` cursor** and applies them
   last-write-wins, never clobbering a local row with a pending write of its own.
5. **Supabase Realtime is a doorbell only** — an event triggers a debounced pull; payloads are never
   applied directly, so all the correctness lives in the idempotent cursor pull.

Server-authoritative `updatedAt` (a Postgres `BEFORE INSERT/UPDATE` trigger → `now()`) is required
for that to be correct, and deletes propagate as `deletedAt` tombstones rather than vanishing. See
<doc:ServerSetup> — the engine cannot correct a server that skips it.

```swift
let engine = try SyncEngine(
    db: databaseQueue,
    supabase: client,
    tables: [SyncTable(name: "recipes", jsonColumns: ["nutrition"])],
    auth: { await session.accessToken }
)
await engine.start()

// That's the whole write API: there isn't one.
try await databaseQueue.write { try recipe.save($0) }
```

There is a **runnable example** — a two-table SwiftUI app wired to the in-memory fake, needing no
Supabase project at all — in [`Examples/`](https://github.com/happyface-studio/HappySync/tree/main/Examples).

The full, language-neutral contract every client *and the server* must honor lives in
[`docs/SYNC-CONTRACT.md`](https://github.com/happyface-studio/HappySync/blob/main/docs/SYNC-CONTRACT.md).
These pages are the Swift practitioner's guide; the contract is the source of truth.

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:ServerSetup>
- ``SyncEngine``
- ``SyncTable``

### Watching sync

- ``SyncStatus``
- ``SyncFailure``

### Repairing writes the server refused

- <doc:Operations>
- ``DeadLetter``

### Testing your integration

- <doc:TestingYourIntegration>
- ``SyncRemote``
- ``SyncDoorbell``
- ``SilentDoorbell``
- ``ClassifiedSyncError``
- ``RemoteFailure``

### How it works

- <doc:Architecture>
- ``SyncCursor``
- ``ScopeFilter``
- ``SyncOp``

### Configuration errors

- ``SyncError``
- ``ManifestProblem``

### Sharing a migrator

- ``SyncSchema``
