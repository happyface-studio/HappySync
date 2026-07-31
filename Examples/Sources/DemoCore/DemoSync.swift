import Foundation
import GRDB
import Supabase
import HappySync
import HappySyncTestSupport

/// The demo's sync stack: a local GRDB store, a HappySync engine, and — instead of a network — the
/// in-memory fake from `HappySyncTestSupport`. That's the point of the example: **no Supabase project
/// is needed to watch sync work**, and the same object drives both the SwiftUI app and the
/// integration tests in CI (issue #58).
///
/// Everything except the transport is the shipping engine: the same capture triggers, the same
/// FK-ordered drain, the same cursor pull and LWW, the same dead-letter classification. Swapping in
/// the real backend is one initializer — see `productionEngine` at the bottom of this file.
@MainActor
public final class DemoSync {
    /// The server, in-process. Seedable (a change "another device" made) and failable (a write the
    /// server refuses), which is how the demo reaches the interesting states on command.
    public let remote: InMemorySyncRemote
    /// Stands in for the Supabase Realtime channel: `fire()` is a change event arriving.
    public let doorbell: ManualDoorbell

    /// The local source of truth. Replaced wholesale on sign-out — see `signOutAndBackIn`.
    public private(set) var db: DatabaseQueue
    public private(set) var engine: SyncEngine

    /// Every row the fake server holds for `lists`. The fake serves downloads from what it was last
    /// seeded with, so publishing a second remote change means re-seeding the accumulated set.
    private var serverLists: [[String: AnyJSON]] = []

    public init() throws {
        remote = InMemorySyncRemote()
        doorbell = ManualDoorbell()
        db = try Self.makeDatabase()
        engine = try Self.makeEngine(db: db, remote: remote, doorbell: doorbell)
    }

    private static func makeDatabase() throws -> DatabaseQueue {
        // In-memory, so every launch of the demo starts from nothing. A real app points GRDB at a
        // file; nothing else about the wiring changes.
        let db = try DatabaseQueue()
        try DemoSchema.migrator().migrate(db)
        return db
    }

    private static func makeEngine(
        db: DatabaseQueue, remote: InMemorySyncRemote, doorbell: ManualDoorbell
    ) throws -> SyncEngine {
        // The manifest is validated against the schema here, which is why the migrator ran first.
        try SyncEngine.forTesting(
            db: db,
            remote: remote,
            tables: DemoSchema.tables,
            doorbell: doorbell,
            pollInterval: 5,        // short, so the demo converges while you watch it
            debounceInterval: 0.2,
            deadLetterAfter: 2      // reach the parked state in two failures rather than eight
        )
    }

    public func start() async {
        await engine.start()
    }

    // MARK: - Writes (there is no write API — that's the point)

    @discardableResult
    public func addList(title: String) async throws -> TodoList {
        let list = TodoList(title: title)
        try await db.write { try list.insert($0) }
        // No engine call. The `lists` capture trigger queued the upload inside that transaction, and
        // the engine's outbox observer wakes the drain.
        return list
    }

    @discardableResult
    public func addItem(title: String, to listID: String) async throws -> TodoItem {
        let item = TodoItem(listId: listID, title: title)
        try await db.write { try item.insert($0) }
        return item
    }

    /// An expression update — the kind of write a `save`-only sync API can't see, and the capture
    /// triggers do.
    public func toggle(itemID: String) async throws {
        try await db.write { db in
            try db.execute(
                sql: "UPDATE items SET done = NOT done WHERE id = ?",
                arguments: [itemID]
            )
        }
    }

    /// One plain `DELETE`. SQLite cascades to the list's items, and each removed item's own trigger
    /// queues its tombstone — the app never enumerates children.
    public func deleteList(id: String) async throws {
        try await db.write { db in
            try db.execute(sql: "DELETE FROM lists WHERE id = ?", arguments: [id])
        }
    }

    // MARK: - The other device

    /// Publishes a list "another device" created and rings the doorbell, exactly as Supabase Realtime
    /// would. The engine debounces the ring into one `pullNow()`; the row arrives through the cursor
    /// pull, never from the event payload.
    public func publishFromAnotherDevice(title: String) async {
        serverLists.append([
            "id": .string(UUID().uuidString),
            "title": .string(title),
            "updatedAt": .string(Self.serverTimestamp()),
            "deletedAt": .null,
        ])
        await remote.seed("lists", rows: serverLists)
        doorbell.fire()
    }

    // MARK: - The server saying no

    /// Makes the server reject every write from now on, the way an RLS policy does: permanently, so
    /// the drain parks the entry instead of retrying it forever.
    public func rejectWrites() async {
        await remote.failNextUpserts(99, with: .permanent)
    }

    /// Stops rejecting. Parked writes don't retry on their own — that's what `retryDeadLetters()` is
    /// for, and it's the repair flow this demo exists to show.
    public func acceptWrites() async {
        await remote.failNextUpserts(0)
    }

    // MARK: - Teardown, in the order that matters

    /// Sign-out → **stop** → wipe → sign-in. `stop()` awaits the in-flight pass, so after it returns
    /// nothing is still writing to the store being thrown away. Doing this in any other order is how
    /// an app uploads the previous user's rows with the new user's token.
    ///
    /// The wipe replaces the whole database rather than `DELETE`-ing rows: a delete would fire the
    /// capture triggers and queue a tombstone for every row the user owns, which the next sign-in
    /// would dutifully upload.
    public func signOutAndBackIn() async throws {
        await engine.stop()
        db = try Self.makeDatabase()
        engine = try Self.makeEngine(db: db, remote: remote, doorbell: doorbell)
        await engine.start()
    }

    /// ISO-8601 with fractional seconds — what a Postgres `timestamptz` serializes to, and what the
    /// tuple cursor orders by.
    private static func serverTimestamp() -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(identifier: "UTC")
        return formatter.string(from: Date())
    }
}

// MARK: - What this would be against a real backend

// The demo's only concession to running offline is *which* initializer builds the engine. Against a
// real project it is this instead, with `Examples/supabase/migrations/` standing the schema up:
//
//     let engine = try SyncEngine(
//         db: databaseQueue,
//         supabase: SupabaseClient(supabaseURL: url, supabaseKey: anonKey),
//         tables: DemoSchema.tables,
//         auth: { await session.accessToken },
//         scope: { await session.user?.id.uuidString }   // only if RLS is broader than the partition
//     )
//
// Everything above this comment — the writes, the cascade, the status, the dead-letter repair —
// is unchanged.
