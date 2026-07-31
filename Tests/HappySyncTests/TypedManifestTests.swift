import Testing
import Foundation
import GRDB
import HappySyncTestSupport
@testable import HappySync

// Issue #50: `SyncTable.table(Record.self, …)` reads the table name off the GRDB record, so a rename
// is a build error at the manifest rather than a table that quietly stops syncing. It produces the
// same `SyncTable` the string form does — these tests hold that equivalence down, because the moment
// the two diverge the typed form becomes a second set of semantics to learn.

// MARK: - Fixtures

/// A record with GRDB's conventional `Columns` namespace — passing those instead of bare `Column`s is
/// what buys the column names their compile-time check.
private struct Recipe: TableRecord {
    static let databaseTableName = "recipes"

    enum Columns {
        static let id = Column("id")
        static let nutrition = Column("nutrition")
        static let userId = Column("userId")
    }
}

private struct RecipeIngredient: TableRecord {
    static let databaseTableName = "recipeIngredients"

    enum Columns {
        static let amount = Column("amount")
        static let userId = Column("userId")
    }
}

/// A record whose table the schema doesn't have — the case compile-time checking can't catch.
private struct Ghost: TableRecord {
    static let databaseTableName = "ghost"
}

// MARK: - Tests

@Test func typedTableReadsItsNameFromTheRecord() {
    let table = SyncTable.table(Recipe.self)

    #expect(table.name == "recipes")
    #expect(table.primaryKey == "id")
    #expect(table.cursorColumn == "updatedAt")
    #expect(table.dependsOn == nil) // nil still means "derive it from the schema's FKs" (issue #49)
}

@Test func typedTableTakesColumnsAndDependenciesAsDeclarations() {
    let table = SyncTable.table(
        RecipeIngredient.self,
        dependsOn: [Recipe.self],
        jsonColumns: [RecipeIngredient.Columns.amount],
        scopeColumn: RecipeIngredient.Columns.userId
    )

    #expect(table.name == "recipeIngredients")
    #expect(table.dependsOn == ["recipes"]) // a record type, not a string that can rot
    #expect(table.jsonColumns == ["amount"])
    #expect(table.scopeColumn == "userId")
}

@Test func typedAndStringManifestsProduceTheSameTable() {
    let typed = SyncTable.table(
        Recipe.self,
        primaryKey: Recipe.Columns.id,
        dependsOn: [RecipeIngredient.self],
        jsonColumns: [Recipe.Columns.nutrition],
        scopeColumn: Recipe.Columns.userId
    )
    let strings = SyncTable(
        name: "recipes",
        primaryKey: "id",
        dependsOn: ["recipeIngredients"],
        jsonColumns: ["nutrition"],
        scopeColumn: "userId"
    )

    #expect(typed == strings) // one manifest type, two ways to spell it
}

@Test func aTypedManifestDrivesTheEngine() async throws {
    // The point of the equivalence: everything downstream — validation, trigger install, the drain —
    // is the same code path, so a typed manifest is a spelling choice rather than a mode.
    let db = try recipesDB()
    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(
        db: db,
        remote: remote,
        tables: [SyncTable.table(Recipe.self, jsonColumns: [Recipe.Columns.nutrition])]
    )

    try await write(db, "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
    try await engine.drainOutbox()

    #expect(await remote.upsertCalls.map(\.table) == ["recipes"])
    #expect(try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM _sync_outbox") } == 0)
}

@Test func aTypedManifestIsStillCheckedAgainstTheSchema() throws {
    // Compile-time checking is not a substitute for the launch check: the record's table name is
    // checked against the *record*, and the record can be ahead of the migration that creates it.
    let db = try recipesDB()

    #expect(throws: SyncError.self) {
        _ = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: [.table(Ghost.self)])
    }
}
