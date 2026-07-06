# HappySync — Supabase server setup

Every synced table needs four things server-side. These are **non-negotiable**: LWW compares a
*server* clock, and deletes/scoping/live-updates all depend on this contract. The engine will not
correct a server that violates it.

Run this per synced table (adjust `recipes` → your table). Requires the `moddatetime`-style trigger
below — implemented inline so you don't need an extension.

## 1. `updatedAt` — server-stamped change time

LWW compares `updatedAt`, and a *server* clock is what stops two devices' clock skew from silently
losing writes. **Never trust a client-sent `updatedAt`.**

```sql
alter table public.recipes
  add column if not exists "updatedAt" timestamptz not null default now();

create or replace function public.stamp_updated_at()
returns trigger language plpgsql as $$
begin
  new."updatedAt" := now();   -- server clock, always; ignore any client value
  return new;
end $$;

drop trigger if exists stamp_updated_at on public.recipes;
create trigger stamp_updated_at
  before insert or update on public.recipes
  for each row execute function public.stamp_updated_at();
```

> An **insert-only / immutable** table (rows never change after write) needs no update trigger.
> Give it its own monotonic stamp column (e.g. `translatedAt`) and set the table's
> `cursorColumn` to it in the `SyncTable` descriptor.

## 2. `deletedAt` — soft-delete tombstones

Deletes are **soft**: set `deletedAt` instead of removing the row, so deletions propagate on the
next cursor pull. Deleting a parent must **tombstone its children too** — use a trigger, *not*
`ON DELETE CASCADE` (a hard cascade leaves no tombstone for other devices to pull).

```sql
alter table public.recipes
  add column if not exists "deletedAt" timestamptz;
```

Child-cascade trigger (soft-delete children when a parent is soft-deleted):

```sql
create or replace function public.tombstone_recipe_children()
returns trigger language plpgsql as $$
begin
  if new."deletedAt" is not null and old."deletedAt" is null then
    update public."recipeIngredients"
       set "deletedAt" = new."deletedAt"
     where "recipeId" = new.id and "deletedAt" is null;
    -- repeat for each child table (recipeSteps, recipeStepIngredients, …)
  end if;
  return new;
end $$;

drop trigger if exists tombstone_recipe_children on public.recipes;
create trigger tombstone_recipe_children
  after update on public.recipes
  for each row execute function public.tombstone_recipe_children();
```

The engine **also** cascades child deletes locally (deepest-first, in one transaction) for children
enforced by local foreign keys, so the local and server deleted sets stay symmetric with no orphan
window and no round-trip. The server trigger is what makes *other* devices converge.

### Purge old tombstones — and the `maxOfflineGap` invariant

Purge tombstones on a schedule (e.g. `deletedAt < now() - interval '90 days'`), but respect this
**invariant**: the purge retention must exceed the engine's `maxOfflineGap` (default 30 days). A
device offline longer than the retention would never see a purged tombstone and would resurrect the
row. The engine defends by full-resyncing + reconciling once `now − lastSyncedAt > maxOfflineGap`,
so set `maxOfflineGap` **≤ your server's tombstone retention**.

```sql
-- e.g. a pg_cron job; retention (90d) > engine maxOfflineGap (30d default)
delete from public.recipes where "deletedAt" < now() - interval '90 days';
```

## 3. RLS scoped to `auth.uid()`

RLS is the **security boundary** — every row read/written is filtered by it. Enable RLS and add
policies keyed on `auth.uid()`.

```sql
alter table public.recipes enable row level security;

create policy "own rows: select" on public.recipes
  for select using ("userId" = auth.uid());
create policy "own rows: write" on public.recipes
  for all using ("userId" = auth.uid()) with check ("userId" = auth.uid());
```

**RLS is not necessarily the sync *partition*.** If a policy is deliberately broader than what a
device should download — e.g. `isPublic = true OR userId = auth.uid()` — an unfiltered pull drags
the whole public catalog to every device. In that case declare a `scopeColumn` (e.g. `userId`) on
the `SyncTable` and supply the engine's `scope:` closure; the engine filters the pull **and** the
Realtime doorbell to `scopeColumn = <partition value>`. Omit `scopeColumn` when RLS already equals
the partition.

## 4. Realtime publication (the doorbell)

Add each synced table to the `supabase_realtime` publication. Realtime is a **doorbell only** — an
event triggers a debounced `pullNow()`; payloads are never applied directly.

```sql
alter publication supabase_realtime add table public.recipes;
```

## Field-mapping conventions

| Concern | Convention |
|---|---|
| Column names | **camelCase, identical** in SQLite and Postgres — no snake_case mapping layer. |
| Dates | ISO-8601 with fractional seconds; the LWW gate compares instants at **microsecond** precision. |
| UUID | text locally ↔ `uuid` in Postgres. |
| Bool | integer `0/1` locally ↔ `boolean` in Postgres. |
| Enum | `rawValue` string. |
| JSON columns | JSON **text** locally ↔ `json`/`jsonb`; declare in `SyncTable.jsonColumns`. |
| Server-owned | RPC-managed columns; declare in `serverOwnedColumns`, never in an upsert payload. |

## Server-side checklist

- [ ] `updatedAt timestamptz not null default now()` + `BEFORE INSERT/UPDATE` trigger on every table.
- [ ] `deletedAt timestamptz` on every table + a child-tombstone trigger on every parent.
- [ ] Tombstone purge scheduled, retention **>** engine `maxOfflineGap`.
- [ ] RLS enabled + `auth.uid()` policies on every table.
- [ ] `scopeColumn` declared for any table whose RLS is broader than the sync partition.
- [ ] Every synced table in the `supabase_realtime` publication.
- [ ] Schema changes are additive; removals are client-first (see `operations.md`).
