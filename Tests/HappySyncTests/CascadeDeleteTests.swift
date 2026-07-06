import Testing
import Foundation
import GRDB
import Supabase
@testable import HappySync

// FakeRemote and makeEngine live in TestSupport.swift.
//
// Local cascade delete (APPS-510): deleting a parent row removes its children locally — deepest-first
// so enforced FKs never RESTRICT — and queues a tombstone for each, mirroring the server trigger.

// MARK: - Fixture

/// CookThis's recipe graph with **real SQLite FK constraints** (GRDB enforces them by default), so a
/// bare parent delete would RESTRICT without the engine's local cascade.
private func recipeGraphDB() throws -> DatabaseQueue {
    let db = try DatabaseQueue()
    try db.write { db in
        try db.create(table: "recipes") { t in
            t.column("id", .text).primaryKey()
            t.column("title", .text)
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes")
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeSteps") { t in
            t.column("id", .text).primaryKey()
            t.column("recipeId", .text).references("recipes")
            t.column("updatedAt", .text)
        }
        try db.create(table: "recipeStepIngredients") { t in
            t.column("id", .text).primaryKey()
            t.column("stepId", .text).references("recipeSteps")
            t.column("ingredientId", .text).references("recipeIngredients")
            t.column("updatedAt", .text)
        }
    }
    return db
}

/// Declared child-first to prove the cascade doesn't rely on input order.
private let recipeGraphTables = [
    SyncTable(name: "recipeStepIngredients", dependsOn: ["recipeSteps", "recipeIngredients"]),
    SyncTable(name: "recipeIngredients", dependsOn: ["recipes"]),
    SyncTable(name: "recipeSteps", dependsOn: ["recipes"]),
    SyncTable(name: "recipes"),
]

private func queuedDeletes(_ db: any DatabaseReader) async throws -> [String] {
    try await db.read { db in
        try Row.fetchAll(db, sql: "SELECT table_name, pk FROM \(SyncSchema.outboxTable) WHERE op = 'delete' ORDER BY seq")
            .map { "\($0["table_name"] as String)/\($0["pk"] as String)" }
    }
}

// MARK: - Cascade

@Test func deletingParentCascadesToChildrenAndQueuesTombstones() async throws {
    let db = try recipeGraphDB()
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r1', 'Soup')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1'), ('i2', 'r1')")
        try db.execute(sql: "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1')")
        // A second recipe whose subtree must survive the delete of r1.
        try db.execute(sql: "INSERT INTO recipes (id, title) VALUES ('r2', 'Salad')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i9', 'r2')")
    }
    let engine = try SyncEngine(db: db, remote: FakeRemote(), tables: recipeGraphTables)

    // Under enforced FKs this bare DELETE would RESTRICT; the cascade removes the whole subtree.
    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])

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

    // A tombstone was queued for the parent and every removed child (no duplicate for x1, which is
    // reachable via both recipeSteps and recipeIngredients).
    let queued = try await queuedDeletes(db)
    #expect(Set(queued) == [
        "recipes/r1", "recipeIngredients/i1", "recipeIngredients/i2",
        "recipeSteps/s1", "recipeStepIngredients/x1",
    ])
    #expect(queued.count == 5) // no duplicate tombstone for the doubly-reachable step-ingredient
}

@Test func cascadeDrainsTombstonesChildrenBeforeParent() async throws {
    let db = try recipeGraphDB()
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id) VALUES ('r1')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1')")
    }
    let remote = FakeRemote()
    let engine = try SyncEngine(db: db, remote: remote, tables: recipeGraphTables)

    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])
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
    try await db.write { db in try db.execute(sql: "INSERT INTO recipes (id) VALUES ('r1')") }
    let engine = try SyncEngine(db: db, remote: FakeRemote(), tables: recipeGraphTables)

    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])

    // No children exist — the cascade is a no-op, exactly the pre-APPS-510 single-row behavior.
    #expect(try await queuedDeletes(db) == ["recipes/r1"])
    let recipes = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM recipes")! }
    #expect(recipes == 0)
}

@Test func deletingIntermediateParentCascadesOnlyItsOwnSubtree() async throws {
    // Deleting a recipeStep (a child that is itself a parent) removes its recipeStepIngredients but
    // leaves the recipe and its recipeIngredients intact.
    let db = try recipeGraphDB()
    try await db.write { db in
        try db.execute(sql: "INSERT INTO recipes (id) VALUES ('r1')")
        try db.execute(sql: "INSERT INTO recipeIngredients (id, recipeId) VALUES ('i1', 'r1')")
        try db.execute(sql: "INSERT INTO recipeSteps (id, recipeId) VALUES ('s1', 'r1'), ('s2', 'r1')")
        try db.execute(sql: "INSERT INTO recipeStepIngredients (id, stepId, ingredientId) VALUES ('x1', 's1', 'i1'), ('x2', 's2', 'i1')")
    }
    let engine = try SyncEngine(db: db, remote: FakeRemote(), tables: recipeGraphTables)

    try await engine.enqueue(.delete, table: "recipeSteps", row: ["id": "s1"])

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
    let queued = try await queuedDeletes(db)
    #expect(Set(queued) == ["recipeSteps/s1", "recipeStepIngredients/x1"])
}

@Test func cascadeSkipsLogicalDependencyWithoutFKConstraint() async throws {
    // A child that `dependsOn` a parent but has NO real SQLite FK constraint is not cascaded here
    // (there's no enforcement to violate, and no declared column to follow). Its rows survive locally
    // and reconcile on the next pull via the server tombstone.
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
        try db.execute(sql: "INSERT INTO recipes (id) VALUES ('r1')")
        try db.execute(sql: "INSERT INTO mealPlans (id, recipeId) VALUES ('m1', 'r1')")
    }
    let engine = try SyncEngine(
        db: db,
        remote: FakeRemote(),
        tables: [SyncTable(name: "recipes"), SyncTable(name: "mealPlans", dependsOn: ["recipes"])]
    )

    try await engine.enqueue(.delete, table: "recipes", row: ["id": "r1"])

    let mealPlans = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM mealPlans")! }
    #expect(mealPlans == 1) // not cascaded (no FK constraint to follow)
    #expect(try await queuedDeletes(db) == ["recipes/r1"])
}

@Test func cascadeTerminatesOnASelfReferentialDataCycle() async throws {
    // A self-referential table holding a data cycle (n1 → n2 → n1) must not recurse unbounded into a
    // stack overflow: the visited-key guard follows each row at most once (APPS-510).
    let db = try DatabaseQueue()
    try await db.write { db in
        try db.create(table: "nodes") { t in
            t.column("id", .text).primaryKey()
            t.column("parentId", .text).references("nodes")
            t.column("updatedAt", .text)
        }
        // Build the cycle in two steps so the immediate FK check never sees a dangling reference.
        try db.execute(sql: "INSERT INTO nodes (id, parentId) VALUES ('n1', NULL), ('n2', 'n1')")
        try db.execute(sql: "UPDATE nodes SET parentId = 'n2' WHERE id = 'n1'") // now n1 → n2 → n1
    }
    let engine = try SyncEngine(
        db: db, remote: FakeRemote(), tables: [SyncTable(name: "nodes", dependsOn: ["nodes"])]
    )

    try await engine.enqueue(.delete, table: "nodes", row: ["id": "n1"])

    // Both rows are gone and each is tombstoned exactly once — the cycle terminated cleanly.
    let remaining = try await db.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM nodes")! }
    #expect(remaining == 0)
    #expect(Set(try await queuedDeletes(db)) == ["nodes/n1", "nodes/n2"])
    #expect(try await queuedDeletes(db).count == 2) // no duplicate tombstone from the cycle
}
