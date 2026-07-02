import Foundation
import GRDB

/// Per-table download position: the `(updated_at, id)` of the last applied row. Stored as a
/// **tuple**, not a bare timestamp, so resuming a pull never skips rows that share a millisecond
/// across a page boundary.
struct SyncCursor: Sendable, Equatable {
    var updatedAt: String
    var id: String
}

/// One pending upload, as read back from `_sync_outbox`.
struct OutboxEntry: FetchableRecord, Sendable {
    var seq: Int64
    var tableName: String
    var pk: String
    var op: SyncOp
    var attempts: Int
    /// When this entry last failed an upload — gates the per-entry backoff window. Nil until the
    /// first failure (APPS-470).
    var lastAttemptAt: Date?

    init(row: Row) {
        seq = row["seq"]
        tableName = row["table_name"]
        pk = row["pk"]
        op = SyncOp(rawValue: row["op"]) ?? .upsert
        attempts = row["attempts"]
        lastAttemptAt = row["last_attempt_at"]
    }
}

/// One (table, pk) collapsed to its net effect (APPS-472): the highest-`seq` entry — whose op is
/// the net op — plus every `seq` in the group, all cleared together once that net op is applied.
struct CollapsedOp: Sendable {
    let net: OutboxEntry
    let seqs: [Int64]
}

/// Collapses same-`(table, pk)` outbox entries to their net effect in `seq` order, so a
/// delete-then-recreate (or any run of edits) on one row uploads once as its final state instead of
/// replaying every op. Without this, `orderForUpload` runs all upserts before all deletes, so an
/// offline `delete(pk)` then `upsert(pk)` uploads the row *then* soft-deletes it — the tombstone
/// propagates back and hard-deletes the re-created row everywhere (APPS-472 data loss).
///
/// The net op is simply the op of the highest-`seq` entry (the last thing the user did to the row);
/// the local DB already holds that net state, so an `.upsert` net reads a fresh payload (which
/// carries `deletedAt = null`, un-tombstoning the server row) and a `.delete` net soft-deletes.
func collapseOutbox(_ entries: [OutboxEntry]) -> [CollapsedOp] {
    var groups: [String: [OutboxEntry]] = [:]
    var order: [String] = []
    for entry in entries {
        let key = "\(entry.tableName)\u{0}\(entry.pk)"
        if groups[key] == nil { order.append(key) }
        groups[key, default: []].append(entry)
    }
    return order.map { key in
        let group = groups[key]!
        let net = group.max { $0.seq < $1.seq }!
        return CollapsedOp(net: net, seqs: group.map(\.seq))
    }
}

/// Orders pending uploads so foreign keys are never violated: upserts run parents-before-children,
/// then deletes run children-before-parents; `seq` breaks ties so writes to one table stay ordered.
func orderForUpload(_ entries: [OutboxEntry], tables: [SyncTable]) -> [OutboxEntry] {
    let rank = Dictionary(
        uniqueKeysWithValues: topologicalOrder(tables).enumerated().map { ($1, $0) }
    )
    func tableRank(_ name: String) -> Int { rank[name] ?? Int.max }
    return entries.sorted { a, b in
        let phaseA = a.op == .delete ? 1 : 0
        let phaseB = b.op == .delete ? 1 : 0
        if phaseA != phaseB { return phaseA < phaseB } // all upserts before all deletes
        // Upserts ascend the FK order (parents first); deletes descend it (children first).
        let rankA = a.op == .delete ? -tableRank(a.tableName) : tableRank(a.tableName)
        let rankB = b.op == .delete ? -tableRank(b.tableName) : tableRank(b.tableName)
        if rankA != rankB { return rankA < rankB }
        return a.seq < b.seq
    }
    // Same-`(table, pk)` mis-ordering (delete-then-recreate) is handled upstream by
    // `collapseOutbox`, which nets each key to one op before this FK ordering runs (APPS-472).
}

/// Exponential backoff between drain passes for an entry that has failed `attempts` times.
/// Capped so a permanently-failing entry doesn't push the retry interval to infinity.
func backoffDelay(attempts: Int) -> TimeInterval {
    let capped = min(attempts, 6) // 2^6 = 64s ceiling
    return pow(2.0, Double(max(capped, 1))) // 1→2s, 2→4s, 3→8s … 6→64s
}

/// How long the scheduler waits before the next automatic sync: the steady-state `pollInterval`
/// while healthy, or exponential `backoffDelay` once syncs start failing so a flapping network
/// isn't hammered.
func nextDelay(consecutiveFailures: Int, pollInterval: TimeInterval) -> TimeInterval {
    consecutiveFailures == 0 ? pollInterval : backoffDelay(attempts: consecutiveFailures)
}

/// Orders tables so a parent always precedes its children (Kahn's algorithm over `dependsOn`).
/// Used to upload parents before children; reverse the result to delete children before parents.
/// `dependsOn` entries naming tables outside the set are ignored.
func topologicalOrder(_ tables: [SyncTable]) -> [String] {
    let known = Set(tables.map(\.name))
    var emitted: [String] = []
    var done: Set<String> = []
    var remaining = tables

    while !remaining.isEmpty {
        let ready = remaining.filter { table in
            table.dependsOn.filter(known.contains).allSatisfy(done.contains)
        }
        guard !ready.isEmpty else {
            // ponytail: FK graph is a DAG; this guard only stops an infinite loop on a cycle —
            // emit the stragglers in declared order rather than hanging.
            emitted.append(contentsOf: remaining.map(\.name))
            break
        }
        for table in ready { emitted.append(table.name); done.insert(table.name) }
        remaining.removeAll { done.contains($0.name) }
    }
    return emitted
}
