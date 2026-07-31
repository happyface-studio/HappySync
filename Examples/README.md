# HappySync example — a two-table app you can actually run

A SwiftUI demo of the whole engine, wired to the **in-memory fake** from `HappySyncTestSupport`. No
Supabase project, no schema to stand up, no credentials: clone and run.

```bash
open Examples/Package.swift                  # then run the DemoApp scheme (My Mac, or an iOS sim)
swift test --package-path Examples           # the same behaviours, asserted headlessly
swift run  --package-path Examples DemoApp   # or straight from the terminal, on macOS
```

## What it shows

Two tables — `lists` → `items`, with a real foreign key — is enough for every behaviour that matters:

| In the app | What it demonstrates |
|---|---|
| Type a list or item name and hit Add | A **plain GRDB write**. No engine call: the table's capture trigger queues the upload in the same transaction. |
| Tick an item | An **expression update** (`SET done = NOT done`) — the kind of write a `save`-only sync API can't see. |
| **Delete list (cascades)** | One `DELETE`. SQLite removes the items too, and each removed item's own trigger queues its tombstone — children before parent, on the wire. |
| The status dot | `status.isHealthy`, **not** `phase == .idle`. The distinction is the point: a settled pass with writes still parked is `.degraded`. |
| **Change from device B** | Realtime as a doorbell: the ring triggers a pull, and the row arrives through the cursor fetch — never from the event payload. |
| **Make the server reject writes** | An RLS-shaped rejection: the write parks on its first attempt instead of retrying forever. |
| **Parked writes → Retry / Discard** | The dead-letter repair API. Fix the cause, then retry — or discard and take the server's version. |
| **Sign out → wipe → sign in** | The one order that's safe: `await stop()`, *then* replace the store. And replacing it, not `DELETE`-ing rows — a delete would fire the capture triggers and queue a tombstone for everything the user owns. |

Reads are plain `ValueObservation`. HappySync never sits between the app and its data: a pulled row
lands in SQLite and the observation fires, exactly as it does for a local write.

## Layout

```
Sources/DemoCore/     Schema, manifest, sync wiring, view state — no SwiftUI, so CI can drive it
Sources/DemoApp/      The SwiftUI app
Tests/DemoCoreTests/  The table above, as assertions
supabase/migrations/  The server half of the contract, for when you point it at a real project
```

`DemoCore` is where to look first: [`Schema.swift`](Sources/DemoCore/Schema.swift) is the entire
manifest, and [`DemoSync.swift`](Sources/DemoCore/DemoSync.swift) is the entire integration.

## Pointing it at a real Supabase project

One initializer changes — `SyncEngine(db:supabase:tables:auth:)` instead of
`SyncEngine.forTesting(db:remote:tables:)` (see the comment at the bottom of `DemoSync.swift`). The
server side is
[`supabase/migrations/`](supabase/migrations): `supabase start && supabase db reset` applies the
`updatedAt` trigger, the tombstone trigger, RLS, and the Realtime publication that the fake can't
check for you. The [Server Setup](https://github.com/happyface-studio/HappySync/wiki/Server-Setup)
wiki page explains why each of the four is non-negotiable.
