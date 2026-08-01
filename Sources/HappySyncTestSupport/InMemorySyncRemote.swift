import Foundation
import Supabase
import HappySync

/// How a simulated remote call fails — the one knob that decides whether the drain retries the
/// write or parks it.
///
/// The distinction is the whole point of injecting a failure: a `.transient` upload is retried with
/// backoff and eventually succeeds, a `.permanent` one dead-letters on its first attempt, and
/// `.error` classifies a **real** transport error exactly as the production remote would — so a test
/// asserting "an RLS reject parks; an expired token doesn't" is asserting against the shipped
/// classification rather than a hand-picked answer.
public enum SimulatedFailure: @unchecked Sendable {
    /// Retryable: the drain backs off and tries again, so the write eventually lands once the
    /// injected failures run out.
    case transient
    /// Non-retryable: the drain dead-letters the entry on its first attempt, the way an RLS reject
    /// or a constraint violation does.
    case permanent
    /// A real transport error (`HTTPError`, `PostgrestError`, `URLError`, …), wrapped with the
    /// production classification — permanence and auth-transience both come out as they would in
    /// the field.
    case error(any Error)

    /// The error to throw for this failure, carrying the classification the drain will read.
    func makeError() -> any Error {
        switch self {
        case .transient: return SimulatedTransientFailure()
        case .permanent: return SimulatedPermanentFailure()
        case .error(let underlying): return RemoteFailure(classifying: underlying)
        }
    }
}

/// The error a `.transient` `SimulatedFailure` raises: unclassified, which the drain reads as
/// "retry" — the same default it applies to an unrecognized network error.
public struct SimulatedTransientFailure: Error, CustomStringConvertible {
    public init() {}
    public var description: String { "HappySyncTestSupport: simulated transient failure" }
}

/// The error a `.permanent` `SimulatedFailure` raises: classified permanent, so the drain parks the
/// entry on its first attempt instead of charging it retries.
public struct SimulatedPermanentFailure: ClassifiedSyncError, CustomStringConvertible {
    public let isPermanent = true
    public init() {}
    public var description: String { "HappySyncTestSupport: simulated permanent failure" }
}

/// An in-memory `SyncRemote`: a server that lives in the test process.
///
/// It records every upload call, serves downloads from a seedable per-table row store honouring the
/// `(cursorColumn, primaryKey)` tuple cursor and the page limit, and can be told to fail a run of
/// calls — so ordering, retry, dead-lettering, last-write-wins, tombstones and pagination are all
/// observable with no network and no Supabase project (issue #53).
///
/// ```swift
/// let remote = InMemorySyncRemote(dataset: ["recipes": [
///     ["id": "r1", "title": "Soup", "updatedAt": "2026-06-30T12:00:00.000Z"],
/// ]])
/// let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: manifest)
/// try await engine.pullNow()
/// ```
///
/// Rows are `[String: AnyJSON]`, the same shape the engine exchanges with PostgREST, so a seeded row
/// is literally what the server would return.
public actor InMemorySyncRemote: SyncRemote {
    /// The value stamped into the cursor column of every successful upsert's representation, standing
    /// in for the server's `BEFORE UPDATE` trigger. Read it to assert a row was marked clean at the
    /// server's time rather than the client's.
    ///
    /// `nonisolated` so `#expect(stamped == remote.serverUpdatedAt)` needs no `await`: the value is
    /// immutable and `Sendable`, but a cross-module actor `let` is isolated unless it says otherwise.
    public nonisolated let serverUpdatedAt: String
    /// Which column `serverUpdatedAt` is stamped into — match the manifest's `cursorColumn` when a
    /// table cursors on something other than `updatedAt`.
    public nonisolated let cursorColumn: String

    /// Every **row** the drain has upserted, in order — table, uploaded payload, and the PostgREST
    /// conflict target (nil = conflict on the primary key). A batched upload records one entry per
    /// row it carried, so this counts writes regardless of how they were packed; `upsertRequests`
    /// counts the requests.
    public private(set) var upsertCalls: [(table: String, row: [String: AnyJSON], onConflict: String?)] = []
    /// How many upsert **requests** were issued — the number of network round trips, which batching
    /// makes smaller than `upsertCalls.count` (issue #54). A rejected batch counts here too, then the
    /// drain's per-row re-run adds one request each.
    public private(set) var upsertRequests = 0
    /// Every tombstone the drain has propagated, in order — one entry per row, as for `upsertCalls`.
    public private(set) var deleteCalls: [(table: String, pk: String)] = []
    /// How many delete **requests** were issued. See `upsertRequests`.
    public private(set) var deleteRequests = 0
    /// How many download pages have been requested. One pull of a single-table manifest that fits in
    /// one page is one call, so this is the usual way to observe "a sync pass ran".
    public private(set) var fetchCalls = 0
    /// The scope passed to the most recent `fetch` — asserts the engine forwarded the resolved
    /// partition filter (APPS-469).
    public private(set) var lastScope: ScopeFilter?

    private var dataset: [String: [[String: AnyJSON]]]
    private var remainingUpsertFailures: Int
    private var upsertFailure: SimulatedFailure
    private var remainingFetchFailures: Int
    private var fetchFailure: SimulatedFailure
    private var representation: (@Sendable ([String: AnyJSON]) -> [String: AnyJSON])?
    private var upsertHook: (@Sendable (Int) async -> Void)?
    private var fetchHook: (@Sendable (Int) async -> Void)?

    /// - Parameters:
    ///   - dataset: Server rows per table, as PostgREST would return them. Downloads are served from
    ///     here, filtered by scope and cursor and truncated to the page limit.
    ///   - failUpserts: How many upload **requests** throw before one succeeds — a rejected batch
    ///     spends one, as does each row of the per-row re-run that follows it. `99` stands in for
    ///     "always".
    ///   - upsertFailure: What those failures are — the difference between a write that retries and
    ///     one that parks.
    ///   - failFetches: How many downloads throw before one succeeds.
    ///   - fetchFailure: What those failures are. Drives a whole sync *pass* to a chosen failure, so
    ///     a test can assert the `SyncFailure` the status carries.
    ///   - representation: Maps an uploaded row to the row the server returns. Use it to simulate
    ///     server-normalized values, defaulted columns, or a `conflictColumns` merge — the drain
    ///     writes the whole representation back locally. Defaults to echoing the upload with the
    ///     cursor column stamped.
    ///   - serverUpdatedAt: The stamped cursor value.
    ///   - cursorColumn: Which column it is stamped into.
    public init(
        dataset: [String: [[String: AnyJSON]]] = [:],
        failUpserts: Int = 0,
        upsertFailure: SimulatedFailure = .transient,
        failFetches: Int = 0,
        fetchFailure: SimulatedFailure = .transient,
        representation: (@Sendable ([String: AnyJSON]) -> [String: AnyJSON])? = nil,
        serverUpdatedAt: String = "2026-06-30T12:00:00.000Z",
        cursorColumn: String = "updatedAt"
    ) {
        self.dataset = dataset
        self.remainingUpsertFailures = failUpserts
        self.upsertFailure = upsertFailure
        self.remainingFetchFailures = failFetches
        self.fetchFailure = fetchFailure
        self.representation = representation
        self.serverUpdatedAt = serverUpdatedAt
        self.cursorColumn = cursorColumn
    }

    // MARK: - Seeding & injection (after construction)

    /// Replaces the server's rows for one table — e.g. to publish a change "another device" made
    /// between two pulls.
    public func seed(_ table: String, rows: [[String: AnyJSON]]) {
        dataset[table] = rows
    }

    /// The server's current rows for a table, so a test can assert what an upload actually stored.
    /// Note that `upsert` records the call but does **not** write into `dataset` — seeded downloads
    /// and recorded uploads are deliberately independent.
    public func rows(in table: String) -> [[String: AnyJSON]] {
        dataset[table] ?? []
    }

    /// Makes the next `count` upload requests throw. Replaces any run still outstanding.
    public func failNextUpserts(_ count: Int, with failure: SimulatedFailure = .transient) {
        remainingUpsertFailures = count
        upsertFailure = failure
    }

    /// Makes the next `count` downloads throw. Replaces any run still outstanding.
    public func failNextFetches(_ count: Int, with failure: SimulatedFailure = .transient) {
        remainingFetchFailures = count
        fetchFailure = failure
    }

    /// Runs `hook` at the start of every upsert, passing the 1-based call number, **before** the
    /// failure check. Because the actor suspends at the hook's `await`, this is how a test holds an
    /// upload in flight — block on a signal here, make a competing local write, then release it.
    public func onUpsert(_ hook: @escaping @Sendable (Int) async -> Void) {
        upsertHook = hook
    }

    /// Runs `hook` at the start of every download, passing the 1-based call number. Gate the *n*-th
    /// call to hold a multi-page pull open mid-flight.
    public func onFetch(_ hook: @escaping @Sendable (Int) async -> Void) {
        fetchHook = hook
    }

    /// Sets (or replaces) the server's response to an upload. See `representation` in `init`.
    public func respond(with representation: @escaping @Sendable ([String: AnyJSON]) -> [String: AnyJSON]) {
        self.representation = representation
    }

    // MARK: - SyncRemote

    public func upsert(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        upsertRequests += 1
        return try await store(table: table, row: row, onConflict: onConflict)
    }

    /// The batched upload the drain prefers (issue #54). One request, so an injected failure rejects
    /// the **whole** batch before any of its rows is recorded — which is how PostgREST answers a
    /// rejected array body, and what makes the drain fall back to one row at a time.
    public func upsert(table: String, rows: [[String: AnyJSON]], onConflict: String?) async throws -> [[String: AnyJSON]] {
        upsertRequests += 1
        if remainingUpsertFailures > 0 {
            remainingUpsertFailures -= 1
            throw upsertFailure.makeError()
        }
        var representations: [[String: AnyJSON]] = []
        for row in rows {
            representations.append(try await store(table: table, row: row, onConflict: onConflict))
        }
        return representations
    }

    /// Records one uploaded row and answers with the server's representation of it. Shared by the
    /// single-row and batched entry points, which own the request accounting.
    private func store(table: String, row: [String: AnyJSON], onConflict: String?) async throws -> [String: AnyJSON] {
        upsertCalls.append((table, row, onConflict))
        if let upsertHook { await upsertHook(upsertCalls.count) }
        if remainingUpsertFailures > 0 {
            remainingUpsertFailures -= 1
            throw upsertFailure.makeError()
        }
        if let representation { return representation(row) }
        var server = row
        server[cursorColumn] = .string(serverUpdatedAt) // the server's BEFORE UPDATE trigger, in miniature
        return server
    }

    public func delete(table: String, primaryKey: String, pk: String) async throws {
        deleteRequests += 1
        deleteCalls.append((table, pk))
    }

    public func delete(table: String, primaryKey: String, pks: [String]) async throws {
        deleteRequests += 1
        for pk in pks { deleteCalls.append((table, pk)) }
    }

    public func fetch(
        table: String,
        cursorColumn: String,
        since cursor: SyncCursor?,
        primaryKey: String,
        scope: ScopeFilter?,
        limit: Int
    ) async throws -> [[String: AnyJSON]] {
        fetchCalls += 1
        lastScope = scope
        if let fetchHook { await fetchHook(fetchCalls) }
        if remainingFetchFailures > 0 {
            remainingFetchFailures -= 1
            throw fetchFailure.makeError()
        }
        let scoped = (dataset[table] ?? []).filter { row in
            guard let scope else { return true }
            return row[scope.column]?.stringValue == scope.value
        }
        let sorted = scoped.sorted { tuple($0, cursorColumn, primaryKey) < tuple($1, cursorColumn, primaryKey) }
        let filtered = cursor.map { position in
            sorted.filter { tuple($0, cursorColumn, primaryKey) > (position.updatedAt, position.id) }
        } ?? sorted
        return Array(filtered.prefix(limit))
    }

    /// The `(cursorColumn, primaryKey)` tuple a row sorts and resumes by — the same ordering the
    /// server applies, so pagination boundaries land where the engine expects them.
    private func tuple(_ row: [String: AnyJSON], _ cursorColumn: String, _ primaryKey: String) -> (String, String) {
        (row[cursorColumn]?.stringValue ?? "", row[primaryKey].flatMap(Self.keyText) ?? "")
    }

    /// The text form of a scalar key — an integer key renders to the same text the engine's outbox
    /// and cursor store (`7` → `"7"`), so an integer-keyed dataset pages and resumes like a
    /// string-keyed one. (The tie-break *within* one cursor instant is still a text compare, where a
    /// real server compares integers numerically — seed distinct timestamps if that distinction ever
    /// matters to a test.)
    private static func keyText(_ value: AnyJSON) -> String? {
        switch value {
        case .string(let string): return string
        case .integer(let int): return String(int)
        default: return nil
        }
    }
}
