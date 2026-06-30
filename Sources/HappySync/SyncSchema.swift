import GRDB

/// Internal bookkeeping tables HappySync owns. The app never reads or writes these directly.
enum SyncSchema {
    /// One row per pending upload, drained in `seq` order (APPS-413).
    static let outboxTable = "_sync_outbox"
    /// One row per synced table, holding the last applied `(updated_at, id)` cursor (APPS-414).
    static let stateTable = "_sync_state"

    /// Registers HappySync's internal tables. Run once on `SyncEngine` init.
    static func migrator() -> DatabaseMigrator {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("happysync_v1") { db in
            try db.create(table: outboxTable) { t in
                t.autoIncrementedPrimaryKey("seq")
                t.column("table_name", .text).notNull()
                t.column("pk", .text).notNull()
                t.column("op", .text).notNull()
                t.column("queued_at", .datetime).notNull()
                t.column("attempts", .integer).notNull().defaults(to: 0)
            }
            try db.create(table: stateTable) { t in
                t.column("table_name", .text).notNull().primaryKey()
                // Tuple cursor: last applied (updated_at, id). NULL until the first pull.
                t.column("updated_at", .datetime)
                t.column("last_id", .text)
            }
        }
        return migrator
    }
}
