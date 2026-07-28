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
    /// No local table of that name — the manifest and the schema disagree. `didYouMean` carries a
    /// table that differs only in case, which SQLite would resolve but PostgREST would not.
    case noSuchTable(didYouMean: String?)
    /// A field naming a column names one the local table doesn't have. `didYouMean` carries a column
    /// that differs only in case — the likeliest form of this mistake, and the one that reads as a
    /// false alarm until you know these names are sent to Postgres verbatim.
    case noSuchColumn(field: String, column: String, didYouMean: String?)
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
        case .noSuchTable(nil):
            "there is no local table of that name — run the app's migrations before constructing the engine"
        case .noSuchTable(.some(let actual)):
            """
            there is no local table spelled that way — did you mean \"\(actual)\"? \(Self.verbatimNote)
            """
        case .noSuchColumn(let field, let column, nil):
            "`\(field)` names \"\(column)\", which is not a column on this table"
        case .noSuchColumn(let field, let column, .some(let actual)):
            """
            `\(field)` names \"\(column)\", but the column is spelled \"\(actual)\". \(Self.verbatimNote)
            """
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

    /// Why a case-only difference is still an error, which is the least obvious of these and the one
    /// most likely to be read as a false alarm.
    private static let verbatimNote = """
        SQLite would resolve either spelling, but these names are sent to PostgREST verbatim — and a \
        camelCase Postgres identifier is quoted, so the server matches it case-sensitively.
        """
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

        let schema = try LocalSchema(db)

        return try tables.map { spec in
            guard schema.tables.contains(spec.name) else {
                throw SyncError.invalidManifest(
                    table: spec.name,
                    reason: .noSuchTable(didYouMean: nearMiss(spec.name, among: schema.tables))
                )
            }
            try checkColumns(db, spec)
            try checkLeafRule(spec, schema: schema)
            return spec.dependingOn(try resolveDependencies(spec, declared: declared, schema: schema))
        }
    }

    /// Every field that names a column must name one the table has, **spelled identically**.
    ///
    /// Exact, not case-insensitive, even though SQLite resolves identifiers case-insensitively — and
    /// that's the whole point. `primaryKey`, `cursorColumn`, `scopeColumn` and `conflictColumns` are
    /// handed to PostgREST verbatim (`.eq`, `.order`, `on_conflict=`), where a camelCase Postgres
    /// column is a quoted identifier and matches case-sensitively; `jsonColumns` and
    /// `serverOwnedColumns` are matched by exact string equality against the local row's column names
    /// in `RowCoding.payload`. So `scopeColumn: "userID"` against a `userId` column is a real defect —
    /// the local half works and the wire half doesn't — and a check that accepted it would wave
    /// through the likeliest typo of the set.
    ///
    /// `serverColumns` is deliberately exempt from the existence check: it describes the *server's*
    /// schema, which is allowed to differ from the local one — that's the whole reason it exists
    /// (APPS-504). The one thing about it that is checkable locally is that it can't omit the primary
    /// key, since the payload is intersected against it.
    private static func checkColumns(_ db: Database, _ spec: SyncTable) throws {
        let known = try RowCoding.tableColumns(db, table: spec.name)

        func require(_ column: String, _ field: String) throws {
            if known.contains(column) { return }
            throw SyncError.invalidManifest(
                table: spec.name,
                reason: .noSuchColumn(field: field, column: column, didYouMean: nearMiss(column, among: known))
            )
        }

        try require(spec.primaryKey, "primaryKey")
        try require(spec.cursorColumn, "cursorColumn")
        if let scope = spec.scopeColumn { try require(scope, "scopeColumn") }
        for column in spec.jsonColumns { try require(column, "jsonColumns") }
        for column in spec.conflictColumns { try require(column, "conflictColumns") }
        for column in spec.serverOwnedColumns { try require(column, "serverOwnedColumns") }

        if !spec.serverColumns.isEmpty, !spec.serverColumns.contains(spec.primaryKey) {
            throw SyncError.invalidManifest(
                table: spec.name, reason: .serverColumnsOmitPrimaryKey(spec.primaryKey)
            )
        }
    }

    /// The one name in `candidates` that differs from `name` only in case, if there is one — the
    /// "did you mean" on a `noSuchTable` / `noSuchColumn` fault.
    private static func nearMiss(_ name: String, among candidates: Set<String>) -> String? {
        candidates.first { $0.lowercased() == name.lowercased() }
    }

    /// `conflictColumns` documents itself as "only safe on a leaf table … or you orphan its
    /// children". That was a doc-only rule with a data-loss consequence, and it is mechanically
    /// checkable: if any local table foreign-keys this one's primary key, it isn't a leaf.
    ///
    /// The scan covers *every* local table, not just the synced ones — an unsynced child is orphaned
    /// by the re-key just the same.
    private static func checkLeafRule(_ spec: SyncTable, schema: LocalSchema) throws {
        guard !spec.conflictColumns.isEmpty else { return }
        if let child = schema.firstChild(of: spec.name, referencing: spec.primaryKey) {
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
        _ spec: SyncTable, declared: [String: String], schema: LocalSchema
    ) throws -> [String] {
        guard let names = spec.dependsOn else {
            return schema.parents(of: spec.name)
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

/// What the local schema says, read once per engine init: the app's table names as SQLite spells
/// them, plus the foreign-key graph in both directions.
///
/// Both FK directions come from the same `PRAGMA foreign_key_list` pass: parents to derive
/// `dependsOn`, children to enforce the `conflictColumns` leaf rule. Every user table is scanned, not
/// just the synced ones — an unsynced child still makes a synced table a non-leaf.
private struct LocalSchema {
    /// Every app table, spelled as `CREATE TABLE` spelled it. The manifest must match exactly: the
    /// name goes to PostgREST verbatim, where it is case-sensitive.
    private(set) var tables: Set<String> = []
    /// child table → the tables it references, in `foreign_key_list` order, de-duplicated.
    private var parentsByChild: [String: [String]] = [:]
    /// parent table → (child table, referenced column) for every FK pointing at it. The column is nil
    /// when the FK omits it, which in SQLite means "the parent's primary key".
    private var referencesByParent: [String: [(child: String, column: String?)]] = [:]

    init(_ db: Database) throws {
        // Filtered in Swift rather than with `NOT LIKE`, where `_` is itself a single-character
        // wildcard and `'_sync_%'` would quietly match more than the engine's own tables.
        let names = try String
            .fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'table'")
            .filter { !$0.hasPrefix("sqlite_") && !$0.hasPrefix(SyncSchema.tablePrefix) }
        tables = Set(names)

        for child in names {
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
