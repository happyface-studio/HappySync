# HappySync sync contract

The language-neutral contract every HappySync client (the Swift engine today, a possible
TypeScript web client later) and the Supabase backend must honor. The engine implementation is
per-platform; **this contract is what's shared across platforms** — not code.

Enabling constraint: personal data is **single-user, multi-device — not collaborative**. So
**last-write-wins (LWW) by server timestamp is correct** and no CRDTs are needed. Revisit only if
data ever becomes collaborative.

---

## 1. Server contract (Supabase Postgres)

Per synced table:

- **`updatedAt timestamptz` + a `BEFORE INSERT/UPDATE` trigger stamping `now()`.**
  Non-negotiable: LWW compares `updatedAt`, and a *server* clock is what prevents two devices'
  clock skew from silently losing writes. Never trust a client-sent `updatedAt`.
- **`deletedAt timestamptz` tombstone** (nullable). Deletes are soft — set `deletedAt` instead of
  removing the row — so deletions propagate on the next cursor pull. Deleting a parent must
  **tombstone its children too** (don't hard-`ON DELETE CASCADE`); a server trigger is the clean
  way. Purge old tombstones server-side on a schedule.
- **RLS scoped to `auth.uid()`** — every row read/written is filtered by RLS: it is the **security
  boundary**. It is *not* necessarily the **sync partition**. RLS may legitimately be broader than
  what a device should download: CookThis's `recipes` SELECT policy is `isPublic = true OR userId =
  auth.uid()`, so an unfiltered pull would download the entire public catalog to every device. When
  RLS is broader than the partition, the table declares a `scopeColumn` (§5) and the engine filters
  the download (and the Realtime doorbell) to `scopeColumn = <the user's partition value>`. Leave
  `scopeColumn` unset only when RLS already scopes the table to exactly the synced partition. See
  APPS-469.
- **Realtime publication** — the table is in `supabase_realtime`. Realtime is a **doorbell only**:
  an event triggers a debounced `pullNow()`; payloads are never applied directly.
- **Server-owned columns are never written by clients** — RPC-managed values (counters, clone
  counts) are excluded from client upserts and only ever arrive on download. See §4.

No conflict-resolution RPC. For single-user, the later write to reach the server simply gets a
newer `now()` and wins; a plain PostgREST upsert is sufficient.

## 2. Upload (outbox → server)

- Writes append to a local **outbox** in the same transaction as the domain write, then return
  optimistically. A background drain processes the outbox in `seq` order.
- **PostgREST upsert** with `Prefer: return=representation` (returns the server-stamped
  `updatedAt`) for `.upsert`; soft-delete for `.delete`. Both are **idempotent by primary key**, so
  retries are safe; back off exponentially and count `attempts`.
- **FK ordering:** upsert parents before children; tombstone children before parents.
- The upsert payload **excludes** `serverOwnedColumns` (§4) and re-encodes `jsonColumns` to JSON.

## 3. Download (cursor pull → local, LWW)

- Per table: `SELECT * WHERE updatedAt > :cursor [AND scopeColumn = :partition] ORDER BY
  (updatedAt, id)`, RLS-scoped. The `scopeColumn` predicate is added only for tables that declare
  one (§1, §5); it is orthogonal to the `(updatedAt, id)` cursor.
- **Tuple cursor `(updatedAt, id)`** — not a bare timestamp — so rows sharing a millisecond at a
  page boundary aren't dropped. Advance it past the last applied row.
- **LWW apply:** apply a remote row only if `remote.updatedAt > local.updatedAt` **and** the local
  row is not dirty (a pending local edit is never clobbered — its queued upload wins).
- **Tombstones** (`deletedAt` set) arrive through the same pull; apply by deleting locally.
- Convergence does not depend on Realtime: foreground + periodic pulls converge even if Realtime
  drops. Realtime only makes it feel instant.

## 4. Field-mapping conventions

| Concern | Convention |
|---|---|
| Column names | **camelCase, identical** in local SQLite and Postgres — no snake_case mapping layer. |
| Dates | ISO-8601 **with fractional seconds** (`.withInternetDateTime, .withFractionalSeconds`); fall back to non-fractional on read for legacy rows. |
| UUID | stored as text locally. |
| Bool | integer `0/1` locally ↔ `boolean` in Postgres. |
| Enum | `rawValue` string. |
| JSON columns | JSON **text** locally ↔ `json`/`jsonb` in Postgres; re-parsed to a JSON value on upload. Declared per table (`jsonColumns`). |
| Server-owned columns | declared per table (`serverOwnedColumns`); excluded from upserts, applied only on download. |

## 5. Table descriptor

Each synced table is declared once with these fields (the Swift `SyncTable`; a future TS client
declares the same shape):

- `name` — identical local + remote table name
- `primaryKey` — default `id`
- `dependsOn` — tables referenced by FK; drives sync ordering
- `jsonColumns` — columns needing JSON encode/decode
- `serverOwnedColumns` — RPC-managed columns stripped from upserts
- `scopeColumn` — partition column (e.g. `userId`) when RLS is broader than the sync partition;
  the engine filters downloads + the doorbell to `scopeColumn = <partition value>` (§1). Omit when
  RLS already scopes the table to exactly the partition.

---

## 6. Consumer #1 manifest — CookThis (9 tables)

Derived from `CookThis/powersync/sync-config.yaml` (stream list) and the iOS
`jsonEncodedColumnsByTable` registry. `updatedAt?` / `deletedAt?` mark M2 server gaps still to add.

| table | primaryKey | dependsOn | jsonColumns | serverOwned | updatedAt | deletedAt |
|---|---|---|---|---|---|---|
| `profiles` | `id` (= auth uid) | — | dietaryRestrictions, dislikedIngredients | — | **add** | **add** |
| `recipes` | `id` | — | cuisine, dishTypes, tags, equipment, nutrition, detailedNutrition, tasteProfile, estimatedCost | _verify clone/cook counters_ | ✓ | **add** |
| `recipeIngredients` | `id` | recipes | — | — | **add** | **add** |
| `recipeSteps` | `id` | recipes | temperature | — | **add** | **add** |
| `recipeStepIngredients` | `id` | recipeSteps, recipeIngredients | — | — | **add** | **add** |
| `recipe_translations` | `id` | recipes | cuisine, dishTypes, tags, equipment, ingredients, steps | — | n/a¹ | **add** |
| `cookingSessions` | `id` | recipes | completedSteps, activeTimers, substitutions | — | **add** | **add** |
| `mealPlans` | `id` | recipes | suggestionReasons | — | **add** | **add** |
| `userRecipeInteractions` | `id` | recipes | — | **cookedCount**² | ✓ | **add** |

¹ `recipe_translations` is insert/delete-only (immutable once written, stamped `translatedAt`) — no
update trigger needed; cursor on `translatedAt`. ² `cookedCount` is RPC-managed
(`rpcIncrementCookedCount`) — it must never be in an upsert payload. Verify whether `recipes`
carries any server-owned counter (clone/cook counts) before cutover.

Already satisfied server-side: `supabase_realtime` publication covers all 9, denormalized `userId`
partition key on the recipe-child tables (COOK-328), uuid PKs.

**RLS ≠ partition (APPS-469).** RLS scopes reads/writes on all 9, but on 5 tables it is *broader*
than the sync partition, so those must declare a `scopeColumn` or the engine downloads the whole
public catalog to every device:

| table | RLS SELECT policy | `scopeColumn` |
|---|---|---|
| `recipes` | `isPublic = true OR userId = auth.uid()` | `userId` |
| `recipeIngredients` | readable with parent recipe | `userId`¹ |
| `recipeSteps` | readable with parent recipe | `userId`¹ |
| `recipeStepIngredients` | readable with parent recipe | `userId`¹ |
| `recipe_translations` | readable with parent recipe | `userId`¹ |
| `profiles`, `cookingSessions`, `mealPlans`, `userRecipeInteractions` | `userId`/`id = auth.uid()` — RLS already equals the partition | — (omit) |

¹ Uses the denormalized `userId` partition column added in COOK-328. Consumer-side adoption
(declaring these `scopeColumn`s in CookThis's `SyncTable` list + supplying the `scope` uid closure)
is ticketed in "Cook This - Release Ready".

---

## 7. Web client (deferred)

No web consumer exists today (the team's web/Expo apps are online-direct via `@supabase/supabase-js`
+ TanStack Query, no local-first). When one is built it should be **online + optimistic** — same
contract (LWW, field mapping, server-owned columns, Realtime doorbell) but **no outbox / offline
SQLite**: read/write Postgres directly, optimistic UI, Realtime for live updates. It shares this
contract, not the Swift engine's code. Promote the §6 manifest to a generated JSON/YAML source only
when that second consumer makes the duplication real.
