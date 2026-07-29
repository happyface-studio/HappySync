import Testing
import Foundation
import GRDB
import Supabase
import HappySyncTestSupport
@testable import HappySync

// recipeGraphDB / recipeGraphTables / write / queuedOps live in TestSupport.swift.
//
// Issue #48 retired the engine's hand-rolled cascade (~60 lines of recursive FK walking, a composite-FK
// exclusion, and a visited-set cycle guard). `ON DELETE CASCADE` in the app's schema now does the graph
// walk the database was built for, and each child table's own delete trigger queues its tombstone — so
// local and server still converge on the same deleted set, with no orphan window and no round-trip.

private func queuedDeletes(_ db: any DatabaseReader) async throws -> [String] {
    try await db.read { db in
        try Row.fetchAll(db, sql: "SELECT table_name, pk FROM \(SyncSchema.outboxTable) WHERE op = 'delete' ORDER BY seq")
            .map { "\($0["table_name"] as String)/\($0["pk"] as String)" }
    }
}

/// Seeds the graph and drains the inserts, so each test starts with an empty outbox.
private func seedGraph(_ db: any DatabaseWriter, engine: SyncEngine, _ statements: [String]) async throws {
    try await db.write { db in for sql in statements { try db.execute(sql: sql) } }
    try await engine.drainOutbox()
}

// MARK: - Cascade

@Test func deletingParentCascadesToChildrenAndQueuesTombstones() async throws {
    let db = try recipeGraphDB()
    let engine = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: recipeGraphTables)
    try await seedGraph(db, engine: engine, [
        "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')",
        "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1'), ('i2', 'r1')",
        "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')",
        "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1')",
        // A second recipe whose subtree must survive the delete of r1.
        "INSERT INTO recipes (id, title) VALUES ('r2', 'Salad')",
        "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i9', 'r2')",
    ])

    // A plain DELETE. SQLite walks the FK graph; each child's delete trigger queues its own tombstone.
    try await write(db, "DELETE FROM recipes WHERE id = 'r1'")

    let counts = try await db.read { db in
        (
            recipes: try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipes")!,
            ingredients: try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipeIngredients")!,
            steps: try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipeSteps")!,
            stepIngredients: try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipeStepIngredients")!
        )
    }
    #expect(counts.recipes == 1)     // only r2 remains
    #expect(counts.ingredients == 1) // only r2's i9 remains
    #expect(counts.steps == 0)
    #expect(counts.stepIngredients == 0)

    // r2's untouched child survived — the cascade only follows FKs to the deleted parent.
    let survivor = try await db.read { try String.fetchOne($0, sql: "SELECT id FROM recipeIngredients") }
    #expect(survivor == "i9")

    // A tombstone was queued for the parent and every removed child — including x1, reached through
    // both recipeSteps and recipeIngredients, which SQLite deletes (and so tombstones) exactly once.
    let queued = try await queuedDeletes(db)
    #expect(Set(queued) == [
        "recipes/r1", "recipeIngredients/i1", "recipeIngredients/i2",
        "recipeSteps/s1", "recipeStepIngredients/x1",
    ])
    #expect(queued.count == 5) // no duplicate tombstone for the doubly-reachable step-ingredient
}

@Test func cascadeDrainsTombstonesChildrenBeforeParent() async throws {
    let db = try recipeGraphDB()
    let remote = InMemorySyncRemote()
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: recipeGraphTables)
    try await seedGraph(db, engine: engine, [
        "INSERT INTO recipes (id) VALUES ('r1')",
        "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')",
        "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')",
        "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1')",
    ])

    try await write(db, "DELETE FROM recipes WHERE id = 'r1'")
    try await engine.drainOutbox()

    let order = await remote.deleteCalls.map(\.table)
    #expect(Set(order) == ["recipes", "recipeIngredients", "recipeSteps", "recipeStepIngredients"])
    func rank(_ t: String) -> Int { order.firstIndex(of: t)! }
    // Deepest child first, parent last — the FK-safe delete order (reverse of the upsert order).
    #expect(rank("recipeStepIngredients") < rank("recipeSteps"))
    #expect(rank("recipeStepIngredients") < rank("recipeIngredients"))
    #expect(rank("recipeSteps") < rank("recipes"))
    #expect(rank("recipeIngredients") < rank("recipes"))

    let pending = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM \(SyncSchema.outboxTable)") }
    #expect(pending == 0) // every tombstone drained
}

@Test func deletingLeafParentQueuesOnlyItsOwnTombstone() async throws {
    let db = try recipeGraphDB()
    let engine = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: recipeGraphTables)
    try await seedGraph(db, engine: engine, ["INSERT INTO recipes (id) VALUES ('r1')"])

    try await write(db, "DELETE FROM recipes WHERE id = 'r1'")

    #expect(try await queuedDeletes(db) == ["recipes/r1"]) // no children — nothing to cascade
    let recipes = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes")! }
    #expect(recipes == 0)
}

@Test func deletingIntermediateParentCascadesOnlyItsOwnSubtree() async throws {
    // Deleting a recipeStep (a child that is itself a parent) removes its recipeStepIngredients but
    // leaves the recipe and its recipeIngredients intact.
    let db = try recipeGraphDB()
    let engine = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: recipeGraphTables)
    try await seedGraph(db, engine: engine, [
        "INSERT INTO recipes (id) VALUES ('r1')",
        "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')",
        "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1'), ('s2', 'r1')",
        "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1'), ('x2', 's2', 'i1')",
    ])

    try await write(db, "DELETE FROM recipeSteps WHERE id = 's1'")

    let (steps, stepIngredients, ingredients) = try await db.read { db in
        (
            try String.fetchAll(db, sql: "SELECT id FROM recipeSteps ORDER BY id"),
            try String.fetchAll(db, sql: "SELECT id FROM recipeStepIngredients ORDER BY id"),
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipeIngredients")!
        )
    }
    #expect(steps == ["s2"])            // only the deleted step is gone
    #expect(stepIngredients == ["x2"])  // only s1's step-ingredient cascaded away
    #expect(ingredients == 1)           // the shared recipeIngredient i1 is untouched
    #expect(Set(try await queuedDeletes(db)) == ["recipeSteps/s1", "recipeStepIngredients/x1"])
}

@Test func aTableWithoutAnFKConstraintIsNotCascaded() async throws {
    // A child that `dependsOn` a parent but declares no `ON DELETE CASCADE` isn't touched by the
    // parent's delete — the database has nothing to walk. Its rows survive locally and reconcile on the
    // next pull via the server's own child tombstone. Declare the FK action if you want the local
    // cascade; `dependsOn` alone only orders uploads.
    let db = try DatabaseQueue()
    try await db.write { db in
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
    let engine = try SyncEngine.forTesting(
        db: db,
        remote: InMemorySyncRemote(),
        tables: [SyncTable(name: "recipes"), SyncTable(name: "mealPlans", dependsOn: ["recipes"])]
    )
    try await seedGraph(db, engine: engine, [
        "INSERT INTO recipes (id) VALUES ('r1')",
        "INSERT INTO mealPlans (id, recipeId) VALUES ('m1', 'r1')",
    ])

    try await write(db, "DELETE FROM recipes WHERE id = 'r1'")

    let mealPlans = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM mealPlans")! }
    #expect(mealPlans == 1) // not cascaded — no FK action to follow
    #expect(try await queuedDeletes(db) == ["recipes/r1"])
}

@Test func selfReferentialCascadeTombstonesTheWholeChain() async throws {
    // A self-referential table (a folder tree). Deleting the root cascades down every level and each
    // removed row's own delete trigger queues its tombstone — no recursion guard needed on our side,
    // because SQLite's cascade, not the engine, walks the graph.
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "nodes") { t in
            t.column("id", .text).primaryKey()
            t.column("parentId", .text).references("nodes", onDelete: .cascade)
            t.column("updatedAt", .text)
        }
    }
    let engine = try SyncEngine.forTesting(db: db, remote: InMemorySyncRemote(), tables: [SyncTable(name: "nodes", dependsOn: ["nodes"])])
    try await seedGraph(db, engine: engine, [
        "INSERT INTO nodes (id, parentId) VALUES ('n1', NULL), ('n2', 'n1'), ('n3', 'n2')",
    ])

    try await write(db, "DELETE FROM nodes WHERE id = 'n1'")

    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM nodes")! }
    #expect(remaining == 0)
    #expect(Set(try await queuedDeletes(db)) == ["nodes/n1", "nodes/n2", "nodes/n3"])
    #expect(try await queuedDeletes(db).count == 3) // one tombstone per row, no duplicates
}

// MARK: - Server tombstones don't bounce back

@Test func aPulledParentTombstoneCascadesLocallyWithoutQueuingUploads() async throws {
    // The server already tombstoned the children (its own trigger did, per contract §1/§2). Applying
    // the parent's tombstone cascades locally, but none of it may be queued back for upload.
    let db = try recipeGraphDB()
    let remote = InMemorySyncRemote(dataset: [
        "recipes": [[
            "id": "r1", "title": "Soup",
            // Strictly newer than the `serverUpdatedAt` the seeding drain stamped on the local row.
            "updatedAt": "2026-07-01T00:00:00.000Z", "deletedAt": "2026-07-01T00:00:00.000Z",
        ]],
    ])
    let engine = try SyncEngine.forTesting(db: db, remote: remote, tables: recipeGraphTables)
    try await seedGraph(db, engine: engine, [
        "INSERT INTO recipes (id, title, updatedAt) VALUES ('r1', 'Soup', '2026-01-01T00:00:00.000Z')",
        "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')",
    ])

    try await engine.pullNow()

    let counts = try await db.read { db in
        (
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipes")!,
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM recipeSteps")!
        )
    }
    #expect(counts.0 == 0) // the tombstone applied…
    #expect(counts.1 == 0) // …and cascaded to the child locally
    #expect(try await queuedOps(db).isEmpty) // but nothing bounced back at the server
}
