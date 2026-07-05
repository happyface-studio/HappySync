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
  way. Purge old tombstones server-side on a schedule. **Invariant (APPS-471): the purge retention
  must exceed the engine's `maxOfflineGap`.** A device offline longer than the retention would never
  see a purged tombstone; the engine defends against this by full-resyncing + reconciling once
  `now − lastSyncedAt > maxOfflineGap`, so that window must be set ≤ the server's retention (CookThis
  purges at 90 days; the engine defaults `maxOfflineGap` to 30). When a table declares a
  `scopeColumn` (§5) and no partition value has resolved yet (cold launch before session
  restoration, or signed out), the resync is deferred — reconciling a table that wasn't pulled
  would wipe its local rows — and the stale check stays armed until a value is available
  (APPS-501).
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
  retries are safe; back off exponentially **per entry** (`last_attempt_at` gates the window) and
  count `attempts`.
- **Failures are visible, not swallowed (APPS-470).** A failed upload surfaces in `SyncStatus`
  (`failedUploads` while retrying, `deadLetters` once parked) so a user whose writes are all failing
  never sees a healthy idle. Classify failures (APPS-502): **permanent** (constraint `23xxx`, RLS
  `42501`, undefined-column `42703`, and other 4xx) dead-letter immediately; **transient** (network,
  5xx, 408/429, transient Postgres states `40001`/`40P01`/`53xxx`/`08xxx`, and any unknown code)
  retry with backoff until a cap, then dead-letter. **Auth-shaped** failures (401/403, PostgREST
  `PGRST301`/`PGRST302`) are transient *and* exempt from the retry budget — a stale token recovers
  out-of-band, so an expired-token stretch must not dead-letter the whole outbox. A dead-lettered
  entry stops retrying **and** stops counting as a dirty row, so
  it never permanently blocks downloads for its key (§3 LWW). Health = `phase == .idle &&
  failedUploads == 0 && deadLetters == 0`.
- **FK ordering:** upsert parents before children; tombstone children before parents.
- The upsert payload **excludes** `serverOwnedColumns` (§4) and re-encodes `jsonColumns` to JSON.
- **Schema-drift tolerance (APPS-504).** A column the server has dropped or renamed but a shipped
  client still sends makes PostgREST reject the whole upsert (`PGRST204`), which classifies permanent
  and dead-letters every write on the table. The primary defense is operational — the §8 client-first
  removal rule keeps the column until no client sends it. As an optional client-side backstop a table
  may declare `serverColumns` (§5); when set, the upload payload is intersected against it so a
  removed column is dropped locally instead of poisoning the drain.

## 3. Download (cursor pull → local, LWW)

- Per table: `SELECT * WHERE updatedAt > :cursor [AND scopeColumn = :partition] ORDER BY
  (updatedAt, id)`, RLS-scoped. The `scopeColumn` predicate is added only for tables that declare
  one (§1, §5); it is orthogonal to the `(updatedAt, id)` cursor.
- **Tuple cursor `(updatedAt, id)`** — not a bare timestamp — so rows sharing a millisecond at a
  page boundary aren't dropped. Advance it past the last applied row.
- **LWW apply:** apply a remote row only if `remote.updatedAt > local.updatedAt` **and** the local
  row is not dirty (a pending local edit is never clobbered — its queued upload wins).
- **Tombstones** (`deletedAt` set) arrive through the same pull; apply by deleting locally.
- **Schema-drift tolerance (APPS-504).** A server that migrated ahead of an older app build sends
  columns the local schema doesn't have yet. The apply **intersects each wire row against the local
  table's columns** (introspected once per table per pass) and silently drops the unknowns — an old
  client just doesn't see the new column until it updates, instead of throwing `table … has no column
  named …` and bricking every subsequent pull. Tombstone/LWW detection still reads `deletedAt` and
  the cursor column from the wire row, so dropping unknown columns never affects ordering or deletes.
- Convergence does not depend on Realtime: foreground + periodic pulls converge even if Realtime
  drops. Realtime only makes it feel instant.

## 4. Field-mapping conventions

| Concern | Convention |
|---|---|
| Column names | **camelCase, identical** in local SQLite and Postgres — no snake_case mapping layer. |
| Dates | ISO-8601 **with fractional seconds** (`.withInternetDateTime, .withFractionalSeconds`); fall back to non-fractional on read for legacy rows. This is the **canonical** form: the LWW gate canonicalizes both sides (PostgREST `…+00:00`/microseconds, client `…Z`, legacy non-fractional) to it before the lexicographic compare, so mixed formats/zones still order chronologically (APPS-474). Codable `Date`s encode to it too (APPS-475). |
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
- `conflictColumns` — columns of a **secondary unique constraint** to use as the PostgREST upsert
  conflict target (e.g. `["userId", "recipeId"]`), so a fresh-primary-key insert merges onto the
  existing server row instead of 409-ing on the duplicate. The merge re-keys the row to the
  client's primary key, so only declare it on a **leaf** table (no FK children). Omit when the
  primary key is the only uniqueness the upsert can hit.
- `serverColumns` — optional allow-list of the columns the server's schema has; when non-empty the
  upload payload is intersected against it so a dropped/renamed server column can't `PGRST204` the
  upsert (§2, §8). Omit to upload every non-server-owned local column and rely on the §8 removal
  rule instead. Downloads need no equivalent — they intersect against the local schema, introspected
  per pass (§3).

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

---

## 8. Schema evolution

The server (Supabase) and shipped clients are **not lockstep** — App Store review plus staggered
user updates mean older builds run against a newer server schema for weeks. A migration that assumes
lockstep is an outage for those clients (APPS-504): before this section, an added server column threw
`table … has no column named …` on download and bricked every pull, and a dropped server column
`PGRST204`-rejected every upload and dead-lettered the table. The rules below keep any single
migration safe for every client version in the field.

**Additive-only.** Never drop-and-recreate or repurpose a column in place. Every change is either an
add or a (deferred) remove — never a mutation of an existing column's meaning or type.

**Never rename in place.** A rename is a drop + an add, and it breaks clients in *both* directions at
once (old clients download the new name they can't store and upload the old name the server no longer
has). To rename `a` → `b`: add `b`, dual-write `a` and `b` server-side, wait for client adoption of
`b`, then remove `a` by the removal rule below.

**Adding a column — server-first.** Deploy the column server-side (nullable, or with a default)
*before* any client build reads or writes it. Older clients tolerate it automatically:

- *Download*: the apply intersects wire rows against the local schema and drops the unknown column
  (§3) — the old client simply doesn't see it until it updates.
- *Upload*: the old client doesn't have the column, so it never sends it; the server's nullable/
  default value stands.

**Removing a column — client-first.** A column may be dropped server-side **only after no shipped
client still writes it.** Until then it must remain on the server (nullable, ignored). The ordering:

1. Ship a client build that no longer sends the column (stop writing it; if needed declare the
   remaining `serverColumns` so the payload is intersected — §2, §5).
2. Wait out the update-lag window (weeks for an App Store app) until that build's predecessors are
   below your support floor.
3. Only then drop the column server-side.

Skipping step 2 dead-letters every write from clients still sending the column. `serverColumns` is a
client-side backstop that lets a build stop sending a column immediately, but it does not remove the
*need* for the server to keep the column until the sending builds are gone — it only bounds the blast
radius if the ordering slips.

**Server-owned and JSON columns** follow the same rules; a new `serverOwnedColumn` is an additive
server-first change (old clients never wrote it anyway), and a new `jsonColumn` is additive but also
needs the client build that knows to encode/decode it before rows depend on the nesting.
