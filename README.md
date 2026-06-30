# HappySync

A small, single-user-multi-device sync engine for **GRDB** ⇄ **Supabase**, built on the
enabling constraint that personal data is *not collaborative*: one user edits their own rows,
occasionally from two devices. That makes **last-write-wins by server timestamp** correct — no
CRDTs needed.

> Status: **M1 in progress.** This is the package scaffold (public API + internal-table
> migrations). The outbox drain, cursor pull, and Realtime doorbell are stubbed and land next.

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
try await engine.pullNow()

for await status in engine.status {
    // drive sync-status UI: .idle / .syncing / .failed
}
```

## Requirements

- Swift 6, iOS 16+ / macOS 13+
- [GRDB.swift](https://github.com/groue/GRDB.swift) 7.11+
- [supabase-swift](https://github.com/supabase/supabase-swift) 2.x

## Consumer #1

[CookThis](https://github.com/happyface-studio/CookThis) is the first consumer. The API stays
deliberately generic, but is pressure-tested against one real app before it's treated as stable.

## License

MIT — see [LICENSE](LICENSE).
