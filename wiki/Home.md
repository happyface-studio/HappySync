# HappySync

A small, single-user-multi-device sync engine for **GRDB** ⇄ **Supabase**, built on one enabling
constraint: personal data is *not collaborative*. One user edits their own rows, occasionally from
two devices. That makes **last-write-wins by server timestamp** correct — no CRDTs, no
conflict-resolution RPC.

> **Latest release: [v0.4.0](https://github.com/happyface-studio/HappySync/releases/tag/v0.4.0)** —
> dead-letter repair API, local cascade deletes, schema-drift tolerance, and a batch of correctness
> and resilience hardening. See the [release notes](https://github.com/happyface-studio/HappySync/releases/tag/v0.4.0).

## What it does

Local GRDB SQLite is the source of truth for reads. Writes go to GRDB **and an outbox in the same
transaction**, then return optimistically. A background uploader drains the outbox via PostgREST
upsert. A downloader pulls rows changed since a per-table `(updatedAt, id)` cursor, RLS-scoped to
the user, applied last-write-wins. **Supabase Realtime is a doorbell only** — a change event
triggers a debounced `pullNow()`; payloads are never applied directly, so all correctness lives in
the idempotent cursor pull.

HappySync owns the outbox drain, cursor pull, tombstones, FK ordering, the Realtime doorbell,
status, and retry/backoff. It does **not** own reads or schema.

## Start here

The developer documentation is **[built from the source](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync)** — one copy, next to the symbols it
describes. These wiki pages point at it:

| Page | What's on it |
|---|---|
| **[Getting Started](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync/gettingstarted)** | Add the package, wire a `SyncEngine`, first write and pull. |
| **[Server Setup](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync/serversetup)** | The required Supabase schema: `updatedAt` trigger, `deletedAt` tombstones, RLS, Realtime. |
| **[API Reference](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync)** | Every public type and method, generated from the source. |
| **[Operations and Troubleshooting](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync/operations)** | Status UI, dead-letter repair, teardown, schema evolution, a troubleshooting table. |
| **[Testing](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync/testingyourintegration)** | Test your sync integration offline with the shipped fakes — no Supabase project. |
| **[Architecture](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync/architecture)** | How upload, download, LWW, the doorbell, and cascades fit together. |
| **[[Claude Skill]]** | Install the HappySync Claude skill so your AI assistant integrates it correctly. |

Want to *see* it work first? [`Examples/`](https://github.com/happyface-studio/HappySync/tree/main/Examples)
is a two-table SwiftUI app wired to the in-memory fake — no Supabase project needed.

## Requirements

- Swift 6, iOS 16+ / macOS 13+
- [GRDB.swift](https://github.com/groue/GRDB.swift) 7.11+
- [supabase-swift](https://github.com/supabase-community/supabase-swift) 2.x

## The contract

The full, language-neutral contract every client and the server must honor lives in
[`docs/SYNC-CONTRACT.md`](https://github.com/happyface-studio/HappySync/blob/main/docs/SYNC-CONTRACT.md).
This wiki is the practitioner's guide; the contract is the source of truth.

## License

MIT.
