# Testing

Sync is the part of an app where bugs are silent and data-losing, and it is usually the part with no
tests — because testing it seems to require a live Supabase project.

It doesn't. `SyncRemote` and `SyncDoorbell` are the seams the engine's own 2,700 lines of tests run
on, and both are public. The `HappySyncTestSupport` product ships the fakes, so a test can answer
questions like *does deleting a recipe queue tombstones for all its children?* in milliseconds, with
no network.

## Add the test-support product

Add it to your **test** target only — it contains no production code:

```swift
.testTarget(
    name: "MyAppTests",
    dependencies: [
        "MyApp",
        .product(name: "HappySync", package: "HappySync"),
        .product(name: "HappySyncTestSupport", package: "HappySync"),
    ]
),
```

## Build an engine with no Supabase

`SyncEngine.forTesting(db:remote:tables:…)` is the production engine with the network seams
injected. Everything else — drain, pull, retry classification, dead-lettering, cascade ordering — is
the code that ships, so what a test proves here is what the app does.

```swift
import HappySync
import HappySyncTestSupport

let db = try DatabaseQueue()
try MyApp.migrator.migrate(db)          // your migrations, as at launch

let remote = InMemorySyncRemote()
let engine = try SyncEngine.forTesting(
    db: db,
    remote: remote,
    tables: MyApp.syncTables,           // the real manifest — a typo in it fails here, too
    pollInterval: 999,                  // only the passes the test triggers
    debounceInterval: 0.02
)
```

The parameters worth knowing: `pageSize` (shrink to `1` to force pagination), `pollInterval` (raise
to silence the background poll), `debounceInterval` (shrink so a doorbell ring converges promptly),
`deadLetterAfter` (lower to reach the parked state without driving eight retries), and `scope` (the
partition value, to test a user switch).

## `InMemorySyncRemote`

A server that lives in the test process. It records every upload, serves downloads from a seedable
per-table row store honouring the `(cursorColumn, primaryKey)` tuple cursor and the page limit, and
can be told to fail.

```swift
// What the server holds — rows are [String: AnyJSON], exactly what PostgREST returns.
let remote = InMemorySyncRemote(dataset: [
    "recipes": [["id": "r1", "title": "Soup", "updatedAt": "2026-06-30T12:00:00.000Z"]],
])

// What the client sent.
await remote.upsertCalls    // [(table, row, onConflict)] in order
await remote.deleteCalls    // [(table, pk)] — the tombstones
await remote.fetchCalls     // download pages requested; the usual "a pass ran" signal
await remote.lastScope      // the partition filter the engine resolved

// Publish a change "another device" made, between two pulls.
await remote.seed("recipes", rows: [updatedRow])
```

### Failure injection

The one knob that matters is whether the drain **retries** the write or **parks** it:

```swift
InMemorySyncRemote(failUpserts: 1, upsertFailure: .transient)   // retried, then succeeds
InMemorySyncRemote(failUpserts: 1, upsertFailure: .permanent)   // dead-lettered on the first attempt
InMemorySyncRemote(failUpserts: 1, upsertFailure: .error(HTTPError(…)))  // real error, real classification
```

`.error` wraps a genuine transport error (`HTTPError`, `PostgrestError`, `URLError`) with the
classification the production remote applies — so *"an RLS reject parks, an expired token doesn't"*
is asserted against the shipped table rather than a hand-picked answer. `failFetches` /
`fetchFailure` do the same for downloads, which drives a whole pass to `.failed(SyncFailure)`.

Use `99` for "always", and `failNextUpserts(_:with:)` / `failNextFetches(_:with:)` to change the
injection mid-test — e.g. fail, assert parked, then let the retry succeed.

### Simulating server-side computation

`representation` maps an uploaded row to the row the server returns. The drain writes the whole
representation back locally, so this is how you test server-normalized values, defaulted columns, or
a `conflictColumns` merge:

```swift
let remote = InMemorySyncRemote(representation: { row in
    var server = row
    server["cookedCount"] = .integer(7)   // the merged server total, not what this client sent
    server["updatedAt"] = .string("2026-06-30T12:00:00.000Z")
    return server
})
```

### Holding a call in flight

`onUpsert` / `onFetch` run at the start of each call with its 1-based number. Because the remote
suspends there, this is how a test races a local write against an in-flight upload, or stops the
engine mid-pull:

```swift
let started = AsyncSignal(), gate = AsyncSignal()
await remote.onUpsert { _ in await started.fire(); await gate.wait() }

let pass = Task { await engine.syncNow() }
await started.wait()                       // the upload is blocked in the remote
try await db.write { … }                   // a competing local edit lands behind it
await gate.fire()                          // let the upload finish
```

## `ManualDoorbell`

The Realtime channel, made deterministic — it rings only when you say so.

```swift
let doorbell = ManualDoorbell()
let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: tables, doorbell: doorbell)
await engine.start()

doorbell.fire()                            // a remote change lands
#expect(await eventually { await remote.fetchCalls >= 2 })

doorbell.ringScopes                        // [nil, "u1"] proves a sign-in re-scoped the subscription
doorbell.liveSubscriptions                 // settles to 1 — the old subscription was torn down
doorbell.fire(subscription: 0)             // ring a torn-down one; it must not poke the engine
```

`SilentDoorbell` (the default) never rings, so convergence comes only from the periodic poll and the
passes you trigger.

## Async helpers

`eventually` polls until a condition holds or the timeout elapses. Reach for it any time the engine
converges in the background — a fixed sleep plus one assertion is the alternative, and it flakes
under a loaded CI runner:

```swift
#expect(await eventually { await remote.fetchCalls >= 1 })
```

For a *negative* assertion ("no further pull happened"), poll until the count stops moving, then
assert it doesn't move again.

`AsyncSignal` is a one-shot rendezvous — `fire()` releases every `wait()`, now and later, so a wait
can't miss a signal that arrived first.

## Three tests worth having

These are the ones that catch real bugs, and none of them need a network.

**A cascade delete queues every child's tombstone.** The app deletes one row; SQLite cascades and
each child's capture trigger queues its own tombstone. If a child table is missing from your
manifest, this test fails and the field doesn't.

```swift
try await db.write { try $0.execute(sql: "DELETE FROM recipes WHERE id = 'r1'") }
#expect(await eventually { await remote.deleteCalls.count == 3 })
```

**The status UI shows degraded when a write parks.** `.degraded` is the case everyone gets wrong: the
pass completed, so nothing *looks* broken, but the user's write never reached the server. Assert on
`isHealthy`, never on the phase alone.

```swift
let remote = InMemorySyncRemote(failUpserts: 99, upsertFailure: .permanent)
…
#expect(await eventually { await latestStatus(engine)?.phase == .degraded })
#expect(try await engine.deadLetters().count == 1)
```

**Sign-out quiesces the engine before the wipe.** After `await engine.stop()` returns, nothing may
still be in flight — a late pull writes into the store you're about to delete, and mid-account-switch
uploads the previous user's rows under the new user's token.

```swift
await engine.stop()
let atStop = await remote.fetchCalls
doorbell.fire()                            // a Realtime event racing the sign-out
try await Task.sleep(for: .milliseconds(200))
#expect(await remote.fetchCalls == atStop) // nothing restarted
```

A worked version of all three lives in
[`Tests/HappySyncTests/PublicSeamsTests.swift`](https://github.com/happyface-studio/HappySync/blob/main/Tests/HappySyncTests/PublicSeamsTests.swift),
which imports HappySync the way your app does — no `@testable` — so it stops compiling if these
seams ever regress.

## Writing your own remote

`SyncRemote` and `SyncDoorbell` are protocols, so a bespoke double is three methods. Signal whether a
failure is retryable by conforming your error to `ClassifiedSyncError`, or wrap a real transport
error with `RemoteFailure(classifying:)` to get the production classification:

```swift
struct RejectingRemote: SyncRemote {
    func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        throw RemoteFailure(classifying: PostgrestError(code: "42501", message: "rls"))
    }
    func delete(table: String, primaryKey: String, pk: String) async throws {}
    func fetch(table: String, cursorColumn: String, since cursor: SyncCursor?, primaryKey: String,
               scope: ScopeFilter?, limit: Int) async throws -> [[String: AnyJSON]] { [] }
}
```

An error the engine can't classify is treated as **transient** — safer to retry than to silently drop
a user's write.
