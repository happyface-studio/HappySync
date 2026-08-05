import Foundation
import GRDB
import HappySync

// The smallest schema that still shows every interesting HappySync behaviour: a parent, a child, and
// a real foreign key between them, so a parent delete cascades locally *and* queues a tombstone per
// removed child — the behaviour an app most often gets wrong by hand-deleting children.

/// A list of things to do. The parent row.
public struct TodoList: Codable, Identifiable, Hashable, Sendable, FetchableRecord, PersistableRecord {
    public static let databaseTableName = "lists"

    public var id: String
    public var title: String

    public init(id: String = UUID().uuidString, title: String) {
        self.id = id
        self.title = title
    }
}

/// One entry in a list. `listId` carries `ON DELETE CASCADE`, which is the whole trick: deleting a
/// list removes its items in the same transaction and each item's own capture trigger queues its
/// tombstone.
public struct TodoItem: Codable, Identifiable, Hashable, Sendable, FetchableRecord, PersistableRecord {
    public static let databaseTableName = "items"

    public var id: String
    public var listId: String
    public var title: String
    public var done: Bool

    public init(id: String = UUID().uuidString, listId: String, title: String, done: Bool = false) {
        self.id = id
        self.listId = listId
        self.title = title
        self.done = done
    }
}

// Neither record declares `updatedAt` or `deletedAt`. They exist as columns — the sync contract
// requires them — but they belong to the *server*: it stamps `updatedAt`, the drain writes it back,
// and a domain type that carried them would write nulls over both on every `save`.

public enum DemoSchema {
    /// The demo's migrator, with HappySync's internal tables registered into it — the shape an app
    /// that owns its own `DatabaseMigrator` should use, so both share one `grdb_migrations` table.
    ///
    /// The write-capture triggers are deliberately *not* here: they're derived from the manifest
    /// below, which is source code, and the engine (re)installs them idempotently at every init.
    public static func migrator() -> DatabaseMigrator {
        var migrator = DatabaseMigrator()
        SyncSchema.register(into: &migrator)
        migrator.registerMigration("demo_v1") { db in
            try db.create(table: TodoList.databaseTableName) { t in
                t.column("id", .text).primaryKey()
                t.column("title", .text).notNull()
                t.column("updatedAt", .text)  // server-stamped; never written by this app
                t.column("deletedAt", .text)  // tombstone marker, likewise
            }
            try db.create(table: TodoItem.databaseTableName) { t in
                t.column("id", .text).primaryKey()
                t.column("listId", .text)
                    .notNull()
                    .references(TodoList.databaseTableName, onDelete: .cascade)
                t.column("title", .text).notNull()
                t.column("done", .boolean).notNull().defaults(to: false)
                t.column("updatedAt", .text)
                t.column("deletedAt", .text)
            }
        }
        return migrator
    }

    /// The whole manifest. Declared in any order — the engine sorts it — and with no `dependsOn`:
    /// the engine reads `items`' foreign key at init and derives that lists upload first (issue #49).
    public static let tables: [SyncTable] = [
        SyncTable(name: TodoItem.databaseTableName),
        SyncTable(name: TodoList.databaseTableName),
    ]
}
