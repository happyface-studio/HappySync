import Testing
import Foundation
import GRDB
@testable import HappySync

// Issue #49: the manifest is eight strings per table, and every typo in one used to fail silently and
// late — the wrong partition downloads, a column corrupts in both directions, an FK-order violation
// surfaces days later as a dead letter on a different device. `SyncEngine.init` now checks each field
// against the schema it's about to sync, and fills in `dependsOn` from the schema's own foreign keys
// rather than asking the app to restate what SQLite already knows.

// MARK: - Harness

/// Resolves `tables` against `db` and reports the manifest problem it faulted, or nil if the manifest
/// validated. Rethrows anything that isn't a manifest fault, so a real failure never reads as a pass.
private func problem(
    _ db: DatabaseQueue, _ tables: [SyncTable]
) throws -> (table: String, reason: ManifestProblem)? {
    do {
        _ = try db.read { try SyncManifest.resolve($0, tables: tables) }
        return nil
    } catch let error as SyncError {
        guard case .invalidManifest(let table, let reason) = error else { throw error }
        return (table, reason)
    }
}

/// The resolved `dependsOn` of each table, keyed by name — what the engine actually orders by.
private func resolvedDependencies(_ db: DatabaseQueue, _ tables: [SyncTable]) throws -> [String: [String]] {
    let resolved = try db.read { try SyncManifest.resolve($0, tables: tables) }
    return Dictionary(uniqueKeysWithValues: resolved.map { ($0.name, $0.dependsOn ?? []) })
}

/// The HappySync-owned triggers currently installed — proof that a faulted manifest installed none.
private func installedTriggerCount(_ db: DatabaseQueue) throws -> Int {
    try db.read { db in
        try String
            .fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type = 'trigger'")
            .filter { $0.hasPrefix(SyncTriggers.namePrefix) }
            .count
    }
}

/// `recipes` with the `userId` partition column, plus a child that foreign-keys it — enough to
/// exercise the column checks and the `conflictColumns` leaf rule from one fixture.
private func manifestDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("nutrition", .text)
            t.column("userId", .text)
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes", onDelete: .cascade)
            t.column("updatedAt", .text)
        }
    }
    return db
}

// MARK: - Every field that names a column is checked

@Test func initRejectsAScopeColumnTheTableDoesNotHave() throws {
    // `userID` vs `userId`. Unchecked, the filter matches nothing (every pull comes back empty) or
    // PostgREST 400s — and on a table whose RLS is deliberately broader than the partition, an
    // unfiltered pull downloads the whole public catalog to every device.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", scopeColumn: "userID")]))
    #expect(fault.table == "recipes")
    #expect(fault.reason == .noSuchColumn(field: "scopeColumn", column: "userID"))
}

@Test func initRejectsAJSONColumnTheTableDoesNotHave() throws {
    // The quietest of them all: the real column stores escaped JSON *text* instead of a JSON value,
    // and corrupts in both directions with nothing to report it.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", jsonColumns: ["nutritian"])]))
    #expect(fault.reason == .noSuchColumn(field: "jsonColumns", column: "nutritian"))
}

@Test func initRejectsACursorColumnTheTableDoesNotHave() throws {
    // Every pull orders by a column that isn't there — a hard failure, or a cursor that never advances.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", cursorColumn: "modifiedAt")]))
    #expect(fault.reason == .noSuchColumn(field: "cursorColumn", column: "modifiedAt"))
}

@Test func initRejectsAPrimaryKeyColumnTheTableDoesNotHave() throws {
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", primaryKey: "uuid")]))
    #expect(fault.reason == .noSuchColumn(field: "primaryKey", column: "uuid"))
}

@Test func initRejectsAConflictColumnTheTableDoesNotHave() throws {
    let fault = try #require(
        try problem(manifestDB(), [SyncTable(name: "recipeIngredients", conflictColumns: ["recipe_id"])])
    )
    #expect(fault.reason == .noSuchColumn(field: "conflictColumns", column: "recipe_id"))
}

@Test func initRejectsAServerOwnedColumnTheTableDoesNotHave() throws {
    // Not data loss, but dead configuration: the column it was meant to protect is uploaded on every
    // write, clobbering the server's authoritative value.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", serverOwnedColumns: ["cookedCount"])]))
    #expect(fault.reason == .noSuchColumn(field: "serverOwnedColumns", column: "cookedCount"))
}

@Test func initRejectsATableTheDatabaseDoesNotHave() throws {
    // `recipe` (singular) simply never synced — no error, no log, no status change.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipe")]))
    #expect(fault.table == "recipe")
    #expect(fault.reason == .noSuchTable)
}

@Test func columnChecksAreCaseInsensitiveLikeSQLite() throws {
    // SQLite resolves identifiers case-insensitively, so `UserId` really is the partition column and
    // faulting it would be a false alarm on a working manifest.
    let fault = try problem(manifestDB(), [SyncTable(name: "recipes", scopeColumn: "UserId")])
    #expect(fault == nil)
}

@Test func serverColumnsAreNotCheckedAgainstTheLocalSchema() throws {
    // `serverColumns` describes the *server's* schema, which is allowed to differ from the local one —
    // that divergence is the whole reason it exists (APPS-504), so a name the local table lacks is not
    // a fault.
    let fault = try problem(manifestDB(), [SyncTable(name: "recipes", serverColumns: ["id", "title", "calories"])])
    #expect(fault == nil)
}

@Test func initRejectsServerColumnsThatOmitThePrimaryKey() throws {
    // The one thing about the list that *is* checkable locally, and always wrong: the payload is
    // intersected against it, so every upload would go out without a key.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", serverColumns: ["title"])]))
    #expect(fault.reason == .serverColumnsOmitPrimaryKey("id"))
}

// MARK: - Manifest-level rules

@Test func initRejectsATableDeclaredTwice() throws {
    let fault = try #require(
        try problem(manifestDB(), [SyncTable(name: "recipes"), SyncTable(name: "recipes", primaryKey: "id")])
    )
    #expect(fault.reason == .duplicateDeclaration)
}

@Test func initRejectsDependsOnNamingAnUndeclaredTable() throws {
    // The ordering it asks for can't be applied, so the entry is dead configuration — and reads like a
    // guarantee the engine never made.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipeIngredients", dependsOn: ["recipes"])]))
    #expect(fault.table == "recipeIngredients")
    #expect(fault.reason == .unknownDependency("recipes"))
}

@Test func initRejectsConflictColumnsOnATableSomethingForeignKeys() throws {
    // Documented as "only safe on a leaf table … or you orphan its children" — a doc-only rule with a
    // data-loss consequence, and mechanically checkable from the FK graph.
    let fault = try #require(try problem(manifestDB(), [
        SyncTable(name: "recipes", conflictColumns: ["userId"]),
        SyncTable(name: "recipeIngredients"),
    ]))
    #expect(fault.table == "recipes")
    #expect(fault.reason == .conflictColumnsOnParentTable(referencedBy: "recipeIngredients"))
}

@Test func theLeafRuleCountsChildrenTheManifestDoesNotSync() throws {
    // An unsynced child is orphaned by the re-key exactly the same way, so the scan covers every local
    // table rather than only the declared ones. Here only `recipes` is in the manifest.
    let fault = try #require(try problem(manifestDB(), [SyncTable(name: "recipes", conflictColumns: ["userId"])]))
    #expect(fault.reason == .conflictColumnsOnParentTable(referencedBy: "recipeIngredients"))
}

@Test func conflictColumnsOnALeafTableAreAccepted() throws {
    // The sanctioned case: a table nothing references, merging on its secondary unique constraint.
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "userRecipeInteractions") { t in
            t.column("id", .text).primaryKey()
            t.column("userId", .text)
            t.column("recipeId", .text)
            t.column("updatedAt", .text)
        }
    }
    let fault = try problem(db, [SyncTable(name: "userRecipeInteractions", conflictColumns: ["userId", "recipeId"])])
    #expect(fault == nil)
}

// MARK: - dependsOn derived from the schema

@Test func dependsOnIsDerivedFromTheSchemasForeignKeys() throws {
    // Nothing declares dependsOn; every edge below comes from `PRAGMA foreign_key_list`.
    let resolved = try resolvedDependencies(recipeGraphDB(), [
        SyncTable(name: "recipeStepIngredients"),
        SyncTable(name: "recipeIngredients"),
        SyncTable(name: "recipeSteps"),
        SyncTable(name: "recipes"),
    ])
    #expect(resolved["recipes"] == [])
    #expect(resolved["recipeIngredients"] == ["recipes"])
    #expect(resolved["recipeSteps"] == ["recipes"])
    #expect(Set(resolved["recipeStepIngredients"] ?? []) == ["recipeSteps", "recipeIngredients"])
}

@Test func derivedDependenciesIgnoreTablesOutsideTheManifest() throws {
    // `recipeIngredients` foreign-keys `recipes`, but a manifest that doesn't sync `recipes` can't be
    // ordered against it — and the engine shouldn't invent an edge to a table it never uploads.
    let resolved = try resolvedDependencies(recipeGraphDB(), [SyncTable(name: "recipeIngredients")])
    #expect(resolved["recipeIngredients"] == [])
}

@Test func anExplicitDependsOnOverridesTheSchema() throws {
    // The carve-out the docs promise: a *logical* parent with no FK constraint. Derivation finds
    // nothing here, so the declared edge has to survive.
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("updatedAt", .text)
        }
        try db.create(table: "mealPlans") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text) // logical reference only — no .references(...)
            t.column("updatedAt", .text)
        }
    }
    let resolved = try resolvedDependencies(db, [
        SyncTable(name: "recipes"),
        SyncTable(name: "mealPlans", dependsOn: ["recipes"]),
    ])
    #expect(resolved["mealPlans"] == ["recipes"])
    #expect(resolved["recipes"] == [])
}

@Test func aSelfReferentialTableDoesNotDependOnItself() throws {
    // A folder tree foreign-keys its own primary key. Left in, that's a one-node cycle the topological
    // sort can only escape through its straggler guard — and nothing needs the edge, because SQLite's
    // cascade walks the tree at delete time. Dropped from the derived and the declared form alike.
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "nodes") { t in
            t.column("id", .text).primaryKey()
            t.column("parentId", .text).references("nodes", onDelete: .cascade)
            t.column("updatedAt", .text)
        }
    }
    let derived = try resolvedDependencies(db, [SyncTable(name: "nodes")])
    let declaredSelf = try resolvedDependencies(db, [SyncTable(name: "nodes", dependsOn: ["nodes"])])
    #expect(derived["nodes"] == [])
    #expect(declaredSelf["nodes"] == [])
}

@Test func derivedDependenciesOrderTheUploadParentsFirst() throws {
    // End to end: no dependsOn anywhere in the manifest, and the sort still puts parents first — the
    // ordering the consumer used to have to restate by hand.
    let resolved = try recipeGraphDB().read {
        try SyncManifest.resolve($0, tables: [
            SyncTable(name: "recipeStepIngredients"),
            SyncTable(name: "recipeIngredients"),
            SyncTable(name: "recipeSteps"),
            SyncTable(name: "recipes"),
        ])
    }
    let order = topologicalOrder(resolved)
    func rank(_ name: String) -> Int { order.firstIndex(of: name)! }
    #expect(rank("recipes") < rank("recipeIngredients"))
    #expect(rank("recipes") < rank("recipeSteps"))
    #expect(rank("recipeSteps") < rank("recipeStepIngredients"))
    #expect(rank("recipeIngredients") < rank("recipeStepIngredients"))
}

// MARK: - Wiring

@Test func initFaultsTheManifestBeforeInstallingAnyTrigger() throws {
    // The check runs at construction, so a bad manifest stops the app at launch rather than syncing
    // wrong — and it runs *before* the DDL, so no half-installed set of triggers is left behind.
    let db = try manifestDB()
    #expect(throws: SyncError.self) {
        _ = try SyncEngine(db: db, remote: FakeRemote(), tables: [SyncTable(name: "recipes", scopeColumn: "userID")])
    }
    let triggers = try installedTriggerCount(db)
    #expect(triggers == 0)
}

@Test func aValidManifestBuildsAnEngineAndInstallsItsTriggers() throws {
    // The negative space: none of the above fires on a manifest that matches its schema.
    let db = try manifestDB()
    _ = try SyncEngine(
        db: db,
        remote: FakeRemote(),
        tables: [
            SyncTable(name: "recipes", jsonColumns: ["nutrition"], scopeColumn: "userId"),
            SyncTable(name: "recipeIngredients"),
        ]
    )
    let triggers = try installedTriggerCount(db)
    #expect(triggers == 6) // insert/update/delete per table
}

@Test func theErrorMessageNamesTheTableTheFieldAndTheValue() {
    // These are configuration errors read off a crash log at launch, so the description has to stand on
    // its own — a bare `invalidManifest(table:reason:)` in a stack trace is not actionable.
    let message = String(
        describing: SyncError.invalidManifest(
            table: "recipes", reason: .noSuchColumn(field: "scopeColumn", column: "userID")
        )
    )
    #expect(message.contains("recipes"))
    #expect(message.contains("scopeColumn"))
    #expect(message.contains("userID"))
}
