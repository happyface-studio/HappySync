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

    init(row: Row) {
        seq = row["seq"]
        tableName = row["table_name"]
        pk = row["pk"]
        op = SyncOp(rawValue: row["op"]) ?? .upsert
        attempts = row["attempts"]
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
    // ponytail: a delete then re-upsert of the *same* pk inside one drain window is mis-ordered
    // (upsert wins). Collapse same-pk outbox entries if that case ever shows up.
}

/// Exponential backoff between drain passes for an entry that has failed `attempts` times.
/// Capped so a permanently-failing entry doesn't push the retry interval to infinity.
func backoffDelay(attempts: Int) -> TimeInterval {
    let capped = min(attempts, 6) // 2^6 = 64s ceiling
    return pow(2.0, Double(max(capped, 1))) // 1→2s, 2→4s, 3→8s … 6→64s
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
