import Foundation
import GRDB

/// The SQLite triggers that capture **every** local write into the outbox, from any source — issue
/// #48.
///
/// Before this, a synced write had to go through `SyncEngine.enqueue`. A plain
/// `try db.write { try recipe.save($0) }` compiled, succeeded, updated the UI, and *never uploaded* —
/// with no assertion, no log, no status change and no dead letter. Nothing in the engine could see
/// it, so nothing could report it. Capturing writes in the database itself removes the whole failure
/// mode: the app writes GRDB however it likes (`PersistableRecord.save`, partial updates,
/// `UPDATE … SET count = count + 1`, raw SQL, multi-table transactions) and the outbox entries land
/// in the *same* transaction as the domain rows, so the store and the queue still can't disagree —
/// now per user intent, not merely per row.
///
/// Three triggers per synced table (`AFTER INSERT` / `AFTER UPDATE` / `AFTER DELETE`), each guarded
/// by a `WHEN` clause reading `_sync_control.applying` so the engine's own apply-downloads writes
/// don't queue themselves straight back for upload.
enum SyncTriggers {
    /// Prefix identifying a HappySync-owned trigger, so `install` can find the ones it created
    /// previously and drop the stale ones without touching an app's own triggers.
    static let namePrefix = "_sync_trg_"

    // MARK: - Installation

    /// Creates the capture triggers for `tables` and drops any HappySync trigger that no longer
    /// belongs — the table left the manifest, or its primary key changed.
    ///
    /// The manifest is *code*, not a numbered migration: adding a `SyncTable` is a source change that
    /// ships without a schema version bump. So this runs on **every** engine init rather than inside
    /// `SyncSchema.migrator()`. It compares the SQL SQLite has stored against the SQL we'd generate
    /// and only issues DDL when they differ, so the steady-state launch does one `sqlite_master` read
    /// and no schema churn.
    ///
    /// Expects a manifest `SyncManifest.resolve` has already checked against the schema — a declared
    /// table missing locally, or a `primaryKey` naming no column on it, is faulted there (issue #49)
    /// so every manifest problem reports through one error with one shape.
    static func install(_ db: Database, tables: [SyncTable]) throws {
        var wanted: [String: String] = [:] // trigger name → the exact CREATE statement
        for spec in tables {
            for (name, sql) in statements(for: spec) { wanted[name] = sql }
        }

        var installed: [String: String] = [:]
        for row in try Row.fetchAll(db, sql: "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'") {
            let name: String = row["name"]
            guard name.hasPrefix(namePrefix) else { continue } // an app's own trigger — not ours to manage
            let sql: String? = row["sql"]
            installed[name] = sql
        }

        // Tables dropped from the manifest (or re-keyed) leave their triggers behind; those would keep
        // queueing uploads for a table the engine no longer syncs.
        for name in installed.keys where wanted[name] == nil {
            try db.execute(sql: "DROP TRIGGER \(quoted(name))")
        }
        for (name, sql) in wanted where installed[name] != sql {
            // SQLite stores a trigger's CREATE statement verbatim, so an exact match means the
            // installed trigger is already the one we want. Anything else is re-created from scratch.
            try db.execute(sql: "DROP TRIGGER IF EXISTS \(quoted(name))")
            try db.execute(sql: sql)
        }
    }

    /// The three capture triggers for one table, keyed by trigger name.
    ///
    /// The `UPDATE` trigger also queues a tombstone for the **old** primary key when a write changes
    /// it: the server still holds a row under the old key, and nothing else would ever remove it.
    ///
    /// `queued_at` is stamped by SQLite in GRDB's own `Date` storage format
    /// (`YYYY-MM-DD HH:MM:SS.SSS`, UTC), so `DeadLetter.queuedAt` decodes it like any other date the
    /// engine writes. The primary key needs no such handling: `_sync_outbox.pk` has TEXT affinity, so
    /// SQLite renders an integer or UUID key to the same text the drain looks the row back up by.
    private static func statements(for spec: SyncTable) -> [(name: String, sql: String)] {
        let table = quoted(spec.name)
        let pk = quoted(spec.primaryKey)
        let name = literal(spec.name)
        let queuedAt = "strftime('%Y-%m-%d %H:%M:%f', 'now')"
        let columns = "INSERT INTO \(SyncSchema.outboxTable) (table_name, pk, op, queued_at, attempts)"
        // Inert while the engine is applying downloaded rows — otherwise every pulled row would queue
        // itself right back for upload, and the sync would never quiesce.
        let unlessApplying = "WHEN (SELECT applying FROM \(SyncSchema.controlTable) WHERE id = 1) = 0"

        return [
            (triggerName(spec.name, "ins"), """
                CREATE TRIGGER \(quoted(triggerName(spec.name, "ins"))) AFTER INSERT ON \(table)
                \(unlessApplying)
                BEGIN
                  \(columns)
                  VALUES (\(name), NEW.\(pk), 'upsert', \(queuedAt), 0);
                END
                """),
            (triggerName(spec.name, "upd"), """
                CREATE TRIGGER \(quoted(triggerName(spec.name, "upd"))) AFTER UPDATE ON \(table)
                \(unlessApplying)
                BEGIN
                  \(columns)
                  VALUES (\(name), NEW.\(pk), 'upsert', \(queuedAt), 0);
                  \(columns)
                  SELECT \(name), OLD.\(pk), 'delete', \(queuedAt), 0 WHERE OLD.\(pk) <> NEW.\(pk);
                END
                """),
            (triggerName(spec.name, "del"), """
                CREATE TRIGGER \(quoted(triggerName(spec.name, "del"))) AFTER DELETE ON \(table)
                \(unlessApplying)
                BEGIN
                  \(columns)
                  VALUES (\(name), OLD.\(pk), 'delete', \(queuedAt), 0);
                END
                """),
        ]
    }

    private static func triggerName(_ table: String, _ suffix: String) -> String {
        "\(namePrefix)\(table)_\(suffix)"
    }

    // MARK: - Connection configuration

    /// Enables `PRAGMA recursive_triggers` on this connection and verifies it took.
    ///
    /// Two things depend on it. `INSERT OR REPLACE` (and any `ON CONFLICT REPLACE`) deletes the rows
    /// it displaces, and SQLite fires delete triggers for those rows *if and only if* recursive
    /// triggers are on — without it, a REPLACE that evicts a different row by a unique index leaves
    /// the server's copy of that row alive forever. Foreign-key `ON DELETE CASCADE` is the second: a
    /// cascade must reach the child tables' own delete triggers or the child tombstones are never
    /// queued, which would swap a visible problem for an invisible one. Current SQLite fires the
    /// cascade's triggers either way, but the pragma is what the behaviour is documented against, so
    /// the engine sets it rather than depending on the version it happens to be linked against.
    ///
    /// Applied to the writer connection, which is the only one triggers ever fire on. An app using a
    /// `DatabasePool` can also set it in `Configuration.prepareDatabase` — belt and braces, and this
    /// check then just confirms it.
    static func enableRecursiveTriggers(_ db: Database) throws {
        try db.execute(sql: "PRAGMA recursive_triggers = ON")
        let enabled = try Int.fetchOne(db, sql: "PRAGMA recursive_triggers")
        guard enabled == 1 else { throw SyncError.recursiveTriggersUnavailable }
    }

    // MARK: - Re-entrancy

    /// Runs `body` with the capture triggers suppressed — for writes that carry *server* state into
    /// the local store (applying a pulled page, a tombstone, the upload's server representation,
    /// reconciling a full resync, discarding a dead letter). Those are the engine converging on what
    /// the server already has; queueing them for upload would be a loop that never settles.
    ///
    /// The flag is a row in `_sync_control`, so callers must already be inside the `db.write`
    /// transaction whose writes they're suppressing: the raise/lower is then part of that transaction
    /// and a rollback restores it, and GRDB's single writer means no app write can interleave and be
    /// swallowed. The previous value is restored rather than assumed 0, so nesting is safe.
    static func applyingRemoteState<T>(_ db: Database, _ body: () throws -> T) throws -> T {
        let previous = try Int.fetchOne(db, sql: "SELECT applying FROM \(SyncSchema.controlTable) WHERE id = 1") ?? 0
        try setApplying(db, 1)
        do {
            let result = try body()
            try setApplying(db, previous)
            return result
        } catch {
            try? setApplying(db, previous) // best effort — the enclosing transaction rolls back anyway
            throw error
        }
    }

    static func setApplying(_ db: Database, _ value: Int) throws {
        try db.execute(
            sql: "UPDATE \(SyncSchema.controlTable) SET applying = ? WHERE id = 1",
            arguments: [value]
        )
    }

    // MARK: - Quoting

    /// Quotes an identifier (table, column, trigger) for interpolation into DDL.
    private static func quoted(_ identifier: String) -> String {
        "\"\(identifier.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    /// Renders a string as a SQL literal — the table name each trigger writes into `table_name`.
    private static func literal(_ text: String) -> String {
        "'\(text.replacingOccurrences(of: "'", with: "''"))'"
    }
}

/// Wakes the engine as soon as a local write lands in the outbox.
///
/// `enqueue` used to poke the runner itself; with triggers doing the capture there's no engine call
/// on the write path at all, so the engine listens to the database instead — otherwise a plain GRDB
/// write would sit unsent until the next periodic poll. Observing **inserts into `_sync_outbox`**
/// specifically (not the domain tables) means one hook covers every synced table, and the drain's own
/// bookkeeping — the `UPDATE`s that record a failure, the `DELETE`s that clear a confirmed entry —
/// isn't an insert and so can't feed itself another pass.
final class OutboxObserver: TransactionObserver, @unchecked Sendable {
    /// Assigned once by `SyncEngine.init` — after the engine is fully initialized, and before the
    /// observer is registered, so no commit can ever observe it half-wired.
    var onNewEntries: (@Sendable () -> Void)?
    /// Set by the change callback, consumed on commit. GRDB drives both on the writer's serialized
    /// queue, so this is single-threaded despite the `@unchecked` conformance.
    private var sawInsert = false

    func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool {
        if case .insert(let table) = eventKind { return table == SyncSchema.outboxTable }
        return false
    }

    /// GRDB opts an observer into a whole statement's events once `observes(eventsOfKind:)` matches any
    /// of them, so re-check the table here: a write that queues an outbox row also reports the domain
    /// row's own event, and only the outbox insert means "there's something new to upload".
    func databaseDidChange(with event: DatabaseEvent) {
        guard event.tableName == SyncSchema.outboxTable, event.kind == .insert else { return }
        sawInsert = true
    }

    func databaseDidCommit(_ db: Database) {
        guard sawInsert else { return }
        sawInsert = false
        onNewEntries?()
    }

    func databaseDidRollback(_ db: Database) { sawInsert = false }
}
