import Foundation
import GRDB

// Issue #50: the manifest was eight stringly-typed parameters, so a rename in a GRDB migration or a
// Swift record left it stale with no build error. `SyncEngine.init` catches that at launch (#49);
// this catches it at compile time, which is the difference between a crash on the tester's device
// and a red squiggle.

public extension SyncTable {
    /// Declares a synced table from its **GRDB record type**, reading the table name off the record
    /// instead of restating it as a string.
    ///
    /// ```swift
    /// let tables = [
    ///     SyncTable.table(Recipe.self, jsonColumns: [Recipe.Columns.nutrition]),
    ///     SyncTable.table(RecipeIngredient.self),   // dependsOn derived from the FK (issue #49)
    /// ]
    /// ```
    ///
    /// Renaming the record's table — in the record or in the migration that created it — is now a
    /// build error at the manifest rather than a table that silently stops syncing. The same holds
    /// for `dependsOn`, which takes record types rather than names.
    ///
    /// **Columns are as checked as your record makes them.** Passing GRDB's conventional
    /// `enum Columns` (`Recipe.Columns.nutrition`) ties each name to a declaration you rename in one
    /// place; passing a bare `Column("nutrition")` is exactly as unchecked as the string form was, and
    /// still validated against the schema at init. Column names can't be derived from key paths
    /// without a `CodingKeys` bridge the app would have to write anyway, so the choice of how much
    /// checking to buy stays with the consumer.
    ///
    /// Every parameter means what it means on ``SyncTable/init(name:primaryKey:cursorColumn:dependsOn:jsonColumns:serverOwnedColumns:scopeColumn:conflictColumns:serverColumns:)``
    /// — this is that initializer with the strings replaced, not a second set of semantics. The
    /// string form stays for tables with no Swift record type (a join table written only in SQL, or
    /// one owned by another module).
    ///
    /// - Parameters:
    ///   - record: The GRDB record whose `databaseTableName` names the table.
    ///   - dependsOn: Records this one references. `nil` (the default) derives the dependency from
    ///     the schema's own foreign keys — prefer it; pass a list only for a *logical* parent the
    ///     schema has no FK for.
    static func table<Record: TableRecord>(
        _ record: Record.Type,
        primaryKey: any ColumnExpression = Column("id"),
        cursorColumn: any ColumnExpression = Column("updatedAt"),
        dependsOn: [any TableRecord.Type]? = nil,
        jsonColumns: [any ColumnExpression] = [],
        serverOwnedColumns: [any ColumnExpression] = [],
        scopeColumn: (any ColumnExpression)? = nil,
        conflictColumns: [any ColumnExpression] = [],
        serverColumns: [any ColumnExpression] = []
    ) -> SyncTable {
        SyncTable(
            name: Record.databaseTableName,
            primaryKey: primaryKey.name,
            cursorColumn: cursorColumn.name,
            dependsOn: dependsOn.map { records in records.map { $0.databaseTableName } },
            jsonColumns: jsonColumns.map { $0.name },
            serverOwnedColumns: serverOwnedColumns.map { $0.name },
            scopeColumn: scopeColumn.map { $0.name },
            conflictColumns: conflictColumns.map { $0.name },
            serverColumns: serverColumns.map { $0.name }
        )
    }
}
