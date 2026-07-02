import GRDB

/// Internal bookkeeping tables HappySync owns. The app never reads or writes these directly.
enum SyncSchema {
    /// One row per pending upload, drained in `seq` order (APPS-413).
    static let outboxTable = "_sync_outbox"
    /// One row per synced table, holding the last applied `(updated_at, id)` cursor (APPS-414).
    static let stateTable = "_sync_state"
    /// Key/value engine metadata (e.g. `last_synced_at` for the stale-cursor resync, APPS-471).
    static let metaTable = "_sync_meta"

    /// A standalone migrator for HappySync's internal tables. Used when the engine owns the DB.
    static func migrator() -> DatabaseMigrator {
        var migrator = DatabaseMigrator()
        register(into: &migrator)
        return migrator
    }

    /// Registers HappySync's internal tables into an existing migrator. Prefer this when the app
    /// runs its own `DatabaseMigrator` on the same database — GRDB tracks every migration in one
    /// shared `grdb_migrations` table, so the app and HappySync must share one migrator.
    static func register(into migrator: inout DatabaseMigrator) {
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

        // APPS-470: per-entry retry bookkeeping. `last_attempt_at` gates exponential backoff so a
        // failing entry isn't retried on every drain; `dead_lettered` parks a poison entry so it
        // stops retrying and no longer wedges the LWW dirty gate for its row; `last_error` is a
        // telemetry breadcrumb the consumer can surface/repair from.
        migrator.registerMigration("happysync_v2_outbox_retry") { db in
            try db.alter(table: outboxTable) { t in
                t.add(column: "last_attempt_at", .datetime)
                t.add(column: "last_error", .text)
                t.add(column: "dead_lettered", .integer).notNull().defaults(to: 0)
            }
        }

        // APPS-471: durable engine metadata. `last_synced_at` lets the engine detect a device that
        // has been offline past the server's tombstone-purge horizon and full-resync on reconnect.
        migrator.registerMigration("happysync_v3_meta") { db in
            try db.create(table: metaTable) { t in
                t.column("key", .text).notNull().primaryKey()
                t.column("value", .text).notNull()
            }
        }
    }
}
