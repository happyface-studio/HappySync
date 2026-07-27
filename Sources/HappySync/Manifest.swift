import Foundation
import GRDB

/// What is wrong with one `SyncTable` declaration — the `reason` carried by
/// `SyncError.invalidManifest` (issue #49).
///
/// Every case names the field it faults, because the whole point of validating the manifest is that
/// the failures it prevents are otherwise *silent and delayed*: a mistyped `scopeColumn` downloads
/// the wrong partition, a mistyped `jsonColumn` corrupts a column in both directions, a stale
/// `dependsOn` surfaces days later as a constraint dead-letter on a different device. A message that
/// says "which table, which field, which value" turns each of those into a one-line fix at launch.
public enum ManifestProblem: Sendable, Equatable, CustomStringConvertible {
    /// Two `SyncTable`s name the same table. The second would install identical triggers and drain
    /// the same rows twice under whichever spec `drainOutbox` happened to look up.
    case duplicateDeclaration
    /// No local table of that name — the manifest and the schema disagree.
    case noSuchTable
    /// A field naming a column names one the local table doesn't have.
    case noSuchColumn(field: String, column: String)
    /// `dependsOn` names a table that isn't itself declared, so the ordering it asks for can't be
    /// applied — the entry is dead configuration.
    case unknownDependency(String)
    /// `conflictColumns` is declared on a table something else foreign-keys. The merge re-keys the
    /// server row to this client's primary key, which orphans the children of the row it merged onto.
    case conflictColumnsOnParentTable(referencedBy: String)
    /// `serverColumns` is non-empty but omits the primary key, so the payload intersection would
    /// strip the key from every upload.
    case serverColumnsOmitPrimaryKey(String)

    public var description: String {
        switch self {
        case .duplicateDeclaration:
            "declared more than once in `tables`"
        case .noSuchTable:
            "there is no local table of that name — run the app's migrations before constructing the engine"
        case .noSuchColumn(let field, let column):
            "`\(field)` names \"\(column)\", which is not a column on this table"
        case .unknownDependency(let name):
            "`dependsOn` names \"\(name)\", which is not a declared SyncTable"
        case .conflictColumnsOnParentTable(let child):
            """
            `conflictColumns` is only safe on a leaf table, but \"\(child)\" foreign-keys this one — \
            merging on a secondary constraint re-keys the server row and orphans its children
            """
        case .serverColumnsOmitPrimaryKey(let primaryKey):
            "`serverColumns` is non-empty but omits the primary key \"\(primaryKey)\", so every upload would be keyless"
        }
    }
}

/// Checks the declared manifest against the local schema and fills in what the schema already knows
/// — issue #49.
///
/// `SyncTable` takes eight strings and, before this, nothing checked any of them: not the compiler,
/// not the engine, not the first sync pass. Each typo had its own delayed, hard-to-attribute failure,
/// and the engine already did all the introspection needed to catch them — it just never pointed it
/// at the manifest. So this runs once at `SyncEngine.init` (which already throws), before any
/// trigger is installed, and reports the first problem it finds by table, field and value.
///
/// It also *derives* `dependsOn` from `PRAGMA foreign_key_list` when the consumer didn't declare it.
/// The FK graph is in the database already; asking the app to restate it — and then not checking the
/// answer — was the weakest part of the interface.
enum SyncManifest {
    /// Validates `tables` and returns the manifest the engine runs on: identical, except that every
    /// spec's `dependsOn` is resolved to a concrete list (declared, or introspected from the schema).
    ///
    /// Read-only — the caller decides which connection it runs on.
    static func resolve(_ db: Database, tables: [SyncTable]) throws -> [SyncTable] {
        var declared: [String: String] = [:] // lowercased name → the spelling the manifest used
        for spec in tables {
            // SQLite matches identifiers case-insensitively, so `dependsOn: ["Recipes"]` against
            // `SyncTable(name: "recipes")` is the same table and two specs differing only in case are
            // the same table twice.
            guard declared.updateValue(spec.name, forKey: spec.name.lowercased()) == nil else {
                throw SyncError.invalidManifest(table: spec.name, reason: .duplicateDeclaration)
            }
        }

        let graph = try ForeignKeyGraph(db)

        return try tables.map { spec in
            guard try db.tableExists(spec.name) else {
                throw SyncError.invalidManifest(table: spec.name, reason: .noSuchTable)
            }
            try checkColumns(db, spec)
            try checkLeafRule(spec, graph: graph)
            return spec.dependingOn(try resolveDependencies(spec, declared: declared, graph: graph))
        }
    }

    /// Every field that names a column must name one the table has. `serverColumns` is deliberately
    /// exempt: it describes the *server's* schema, which is allowed to differ from the local one —
    /// that's the whole reason it exists (APPS-504). The one thing about it that is checkable locally
    /// is that it can't omit the primary key, since the payload is intersected against it.
    private static func checkColumns(_ db: Database, _ spec: SyncTable) throws {
        let columns = try RowCoding.tableColumns(db, table: spec.name)
        let known = Set(columns.map { $0.lowercased() })

        func require(_ column: String, _ field: String) throws {
            guard known.contains(column.lowercased()) else {
                throw SyncError.invalidManifest(
                    table: spec.name, reason: .noSuchColumn(field: field, column: column)
                )
            }
        }

        try require(spec.primaryKey, "primaryKey")
        try require(spec.cursorColumn, "cursorColumn")
        if let scope = spec.scopeColumn { try require(scope, "scopeColumn") }
        for column in spec.jsonColumns { try require(column, "jsonColumns") }
        for column in spec.conflictColumns { try require(column, "conflictColumns") }
        for column in spec.serverOwnedColumns { try require(column, "serverOwnedColumns") }

        if !spec.serverColumns.isEmpty,
           !spec.serverColumns.contains(where: { $0.lowercased() == spec.primaryKey.lowercased() }) {
            throw SyncError.invalidManifest(
                table: spec.name, reason: .serverColumnsOmitPrimaryKey(spec.primaryKey)
            )
        }
    }

    /// `conflictColumns` documents itself as "only safe on a leaf table … or you orphan its
    /// children". That was a doc-only rule with a data-loss consequence, and it is mechanically
    /// checkable: if any local table foreign-keys this one's primary key, it isn't a leaf.
    ///
    /// The scan covers *every* local table, not just the synced ones — an unsynced child is orphaned
    /// by the re-key just the same.
    private static func checkLeafRule(_ spec: SyncTable, graph: ForeignKeyGraph) throws {
        guard !spec.conflictColumns.isEmpty else { return }
        if let child = graph.firstChild(of: spec.name, referencing: spec.primaryKey) {
            throw SyncError.invalidManifest(
                table: spec.name, reason: .conflictColumnsOnParentTable(referencedBy: child)
            )
        }
    }

    /// The concrete `dependsOn` the engine orders by: the declared list when there is one, otherwise
    /// the table's foreign-key parents.
    ///
    /// A declared list is kept as-is (minus self, below) rather than merged with the FK graph, because
    /// it's an *override* — the documented use is a logical dependency with no real FK constraint, and
    /// silently unioning would make it impossible to say "order me before this parent only".
    ///
    /// Derived parents are narrowed to declared tables; declared ones must already be, since naming a
    /// table the engine doesn't sync asks for an ordering it can't apply.
    ///
    /// **Self-references drop out of both.** A tree table (`parentId` → its own `id`) foreign-keys
    /// itself, and a table cannot be uploaded before itself: left in, it's a one-node cycle that
    /// `topologicalOrder` can only escape through its straggler guard. SQLite's own cascade walks the
    /// tree at delete time, so nothing needs the edge.
    ///
    /// A *mutual* cycle between two tables (legal in SQLite with deferred FKs) is left alone — it's a
    /// real schema shape, and `topologicalOrder` already emits such a pair in declared order rather
    /// than hanging. Derivation only makes it reachable; it isn't new.
    private static func resolveDependencies(
        _ spec: SyncTable, declared: [String: String], graph: ForeignKeyGraph
    ) throws -> [String] {
        guard let names = spec.dependsOn else {
            return graph.parents(of: spec.name)
                .compactMap { declared[$0.lowercased()] }
                .filter { $0.lowercased() != spec.name.lowercased() }
        }
        return try names
            .filter { $0.lowercased() != spec.name.lowercased() }
            .map { name in
                guard let canonical = declared[name.lowercased()] else {
                    throw SyncError.invalidManifest(table: spec.name, reason: .unknownDependency(name))
                }
                return canonical
            }
    }
}

/// The local database's foreign-key graph, read once per engine init.
///
/// Both directions are needed and both come from the same `PRAGMA foreign_key_list` pass: parents to
/// derive `dependsOn`, children to enforce the `conflictColumns` leaf rule. Every user table is
/// scanned, not just the synced ones — an unsynced child still makes a synced table a non-leaf.
private struct ForeignKeyGraph {
    /// child table → the tables it references, in `foreign_key_list` order, de-duplicated.
    private var parentsByChild: [String: [String]] = [:]
    /// parent table → (child table, referenced column) for every FK pointing at it. The column is nil
    /// when the FK omits it, which in SQLite means "the parent's primary key".
    private var referencesByParent: [String: [(child: String, column: String?)]] = [:]

    init(_ db: Database) throws {
        // Filtered in Swift rather than with `NOT LIKE`, where `_` is itself a single-character
        // wildcard and `'_sync_%'` would quietly match more than the engine's own tables.
        let tables = try String
            .fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'table'")
            .filter { !$0.hasPrefix("sqlite_") && !$0.hasPrefix(SyncSchema.tablePrefix) }

        for child in tables {
            var parents: [String] = []
            for row in try Row.fetchAll(db, sql: "PRAGMA foreign_key_list(\(Self.quoted(child)))") {
                let parent: String = row["table"]
                let column: String? = row["to"]
                if !parents.contains(where: { $0.lowercased() == parent.lowercased() }) {
                    parents.append(parent) // a composite FK lists one row per column; one edge is enough
                }
                referencesByParent[parent.lowercased(), default: []].append((child: child, column: column))
            }
            parentsByChild[child.lowercased()] = parents
        }
    }

    func parents(of table: String) -> [String] {
        parentsByChild[table.lowercased()] ?? []
    }

    /// The first table foreign-keying `table`'s `primaryKey`, if any. A reference with no column named
    /// targets the parent's primary key by definition, so it counts.
    func firstChild(of table: String, referencing primaryKey: String) -> String? {
        referencesByParent[table.lowercased()]?
            .first { $0.column == nil || $0.column?.lowercased() == primaryKey.lowercased() }?
            .child
    }

    private static func quoted(_ identifier: String) -> String {
        "\"\(identifier.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}
