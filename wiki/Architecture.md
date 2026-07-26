# Architecture

HappySync is deliberately small. Its correctness rests on one enabling constraint and a handful of
mechanisms that compose cleanly.

## The enabling constraint

Personal data is **single-user, multi-device — not collaborative**. One user edits their own rows,
occasionally from two devices. Because two devices rarely edit the *same* row at the *same* instant,
**last-write-wins (LWW) by server timestamp** is correct. No CRDTs, no operational transforms, no
conflict-resolution RPC. Revisit only if the data ever becomes collaborative.

## The pieces

```
        write                          read
          │                             ▲
          ▼                             │
   ┌──────────────┐   ValueObservation  │
   │  GRDB SQLite │───────────────────► UI
   │ (local truth)│
   └──────┬───────┘
          │ same transaction
          ▼
   ┌──────────────┐    drain (upsert)    ┌───────────────┐
   │   _sync_outbox│───────────────────► │   Supabase    │
   └──────────────┘                      │   Postgres    │
          ▲            cursor pull (LWW)  │  (server truth│
          └───────────────────────────── │   for writes) │
                                          └──────┬────────┘
                                Realtime doorbell │ (debounced pullNow)
                                          ◄───────┘
```

### Upload — outbox drain

A write appends to `_sync_outbox` **in the same transaction** as the domain write, then returns
optimistically. A background drain processes entries in `seq` order:

- **PostgREST upsert** with `Prefer: return=representation` for `.upsert`; soft-delete for
  `.delete`. Both are **idempotent by primary key**, so retries are safe.
- **The cursor column never leaves the device.** It's server-stamped by contract, so the payload
  drops it (along with any declared `serverOwnedColumns`) with nothing declared — a table whose
  `updatedAt` trigger is missing can't quietly promote a device's clock to the LWW authority.
  `deletedAt` still ships, as `null`, which is how a re-created row un-tombstones.
- **The full server representation is written back locally** on success — column defaults,
  trigger-normalized fields, recomputed server-owned columns, and (for a `conflictColumns` upsert)
  the merged row re-keyed to the client's pk. Otherwise the writing device would be the one device
  that never sees the server's version of its own write (its local `updatedAt` would equal the
  server's, so LWW would skip the row forever). The write-back is skipped for a row that gained a
  newer outbox entry mid-flight — the pending edit wins.
- **FK ordering:** upsert parents before children; tombstone children before parents.
- **Per-entry backoff** with jitter; failures are classified permanent vs transient (see
  [[Operations and Troubleshooting]]).

### Download — cursor pull with LWW

Per table: `SELECT * WHERE updatedAt > :cursor [AND scopeColumn = :partition] ORDER BY (updatedAt,
id)`, RLS-scoped.

- **Tuple cursor `(updatedAt, id)`** — not a bare timestamp — so rows sharing a millisecond at a
  page boundary aren't dropped.
- **LWW apply:** apply a remote row only if `remote.updatedAt > local.updatedAt` **and** the local
  row is not dirty (a pending local edit is never clobbered). The compare is at **microsecond**
  precision — Postgres `now()` is µs-precise, so two writes inside the same millisecond don't tie.
- **Tombstones** (`deletedAt` set) arrive through the same pull and delete locally.
- **Schema-drift tolerance:** each wire row is intersected against the local table's columns, so a
  server that migrated ahead doesn't brick an old client's pulls.

### Realtime is a doorbell

A Realtime change event triggers a **debounced `pullNow()`** — it never applies payloads directly.
All correctness lives in the idempotent cursor pull, so sync converges even if Realtime drops; the
doorbell just makes it feel instant. Foreground and periodic pulls converge on their own.

### Deletes and cascades

`enqueue(.delete)` soft-deletes locally and queues a tombstone. For a parent whose children are
enforced by **local foreign keys**, the engine cascades — deletes the children deepest-first in the
same transaction and queues a tombstone for each — mirroring the server's child-tombstone trigger.
Result: the local and server deleted sets stay symmetric, with no orphan window and no round-trip. A
visited-key guard makes each row delete at most once, so a self-referential cycle terminates.

## What HappySync owns (and doesn't)

**Owns:** the outbox drain, cursor pull, tombstones, FK ordering, the Realtime doorbell, status, and
retry/backoff.

**Does not own:** your reads (observe GRDB directly) or your schema (you declare tables; the server
holds the authoritative schema).

## The contract

Every mechanism here has a precise, language-neutral spec — server conventions, wire semantics,
field mapping, schema-evolution rules — in
[`docs/SYNC-CONTRACT.md`](https://github.com/happyface-studio/HappySync/blob/main/docs/SYNC-CONTRACT.md).
A future non-Swift client would implement that contract, not reuse this engine's code.
