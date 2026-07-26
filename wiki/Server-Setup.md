# Server Setup

Every synced table needs four things on the Supabase side. These are **non-negotiable** — LWW
compares a *server* clock, and deletes, scoping, and live updates all depend on this contract. The
engine will not paper over a server that violates it.

Run the SQL below per synced table (swap `recipes` for your table).

## 1. `updatedAt` — server-stamped change time

LWW compares `updatedAt`, and a *server* clock is what stops two devices' clock skew from silently
losing writes. **Never trust a client-sent `updatedAt`.** The engine holds up its half unasked: a
table's `cursorColumn` is stripped from every upload payload, so no HappySync client ever sends one
— you don't declare anything for this. The trigger below is still required (it's what advances the
column on update, and what the cursor pull depends on); the stripping just means a table that lost
its trigger can't quietly promote a device's clock to the ordering authority.

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

> An **insert-only / immutable** table needs no update trigger. Give it a monotonic stamp column
> (e.g. `translatedAt`) and set that table's `cursorColumn` in its `SyncTable` descriptor. Because
> the engine never uploads the cursor column, that column must be `default now()` server-side —
> otherwise inserts land with a null stamp the cursor can't order.

## 2. `deletedAt` — soft-delete tombstones

Deletes are **soft**: set `deletedAt` instead of removing the row, so deletions propagate on the
next cursor pull. Deleting a parent must **tombstone its children too** — use a trigger, *not*
`ON DELETE CASCADE` (a hard cascade leaves no tombstone for other devices to pull).

```sql
alter table public.recipes
  add column if not exists "deletedAt" timestamptz;

create or replace function public.tombstone_recipe_children()
returns trigger language plpgsql as $$
begin
  if new."deletedAt" is not null and old."deletedAt" is null then
    update public."recipeIngredients"
       set "deletedAt" = new."deletedAt"
     where "recipeId" = new.id and "deletedAt" is null;
    -- repeat per child table
  end if;
  return new;
end $$;

drop trigger if exists tombstone_recipe_children on public.recipes;
create trigger tombstone_recipe_children
  after update on public.recipes
  for each row execute function public.tombstone_recipe_children();
```

The engine **also** cascades child deletes locally (deepest-first, one transaction) for children
enforced by local foreign keys, keeping the local and server deleted sets symmetric with no orphan
window. The server trigger is what makes *other* devices converge.

### Tombstone purge and the `maxOfflineGap` invariant

Purge old tombstones on a schedule, but respect this **invariant**: the purge retention must exceed
the engine's `maxOfflineGap` (default 30 days). A device offline longer than the retention would
never see a purged tombstone and would resurrect the row. Set `maxOfflineGap` **≤ your server's
retention**.

```sql
-- e.g. a pg_cron job; retention (90d) > engine maxOfflineGap (30d default)
delete from public.recipes where "deletedAt" < now() - interval '90 days';
```

## 3. RLS scoped to `auth.uid()`

RLS is the **security boundary**. Enable it and add policies keyed on `auth.uid()`.

```sql
alter table public.recipes enable row level security;

create policy "own rows: select" on public.recipes
  for select using ("userId" = auth.uid());
create policy "own rows: write" on public.recipes
  for all using ("userId" = auth.uid()) with check ("userId" = auth.uid());
```

**RLS is not necessarily the sync partition.** If a policy is deliberately broader than what a
device should download — e.g. `isPublic = true OR userId = auth.uid()` — an unfiltered pull drags
the whole public catalog to every device. Declare a `scopeColumn` (e.g. `userId`) on the `SyncTable`
and supply the engine's `scope:` closure; the engine filters the pull **and** the doorbell to
`scopeColumn = <partition value>`. Omit `scopeColumn` when RLS already equals the partition.

## 4. Realtime publication (the doorbell)

```sql
alter publication supabase_realtime add table public.recipes;
```

Realtime is a **doorbell only** — an event triggers a debounced `pullNow()`; payloads are never
applied directly. Sync converges without it; Realtime just makes it feel instant.

## Field-mapping conventions

| Concern | Convention |
|---|---|
| Column names | **camelCase, identical** in SQLite and Postgres — no snake_case mapping. |
| Dates | ISO-8601 with fractional seconds; the LWW gate compares instants at **microsecond** precision. |
| UUID | text locally ↔ `uuid` in Postgres. |
| Bool | integer `0/1` locally ↔ `boolean`. |
| Enum | `rawValue` string. |
| JSON columns | JSON **text** locally ↔ `json`/`jsonb`; declare in `jsonColumns`. |
| Server-owned | RPC-managed columns; declare in `serverOwnedColumns`, never in an upsert. The `cursorColumn` is server-owned too and the engine strips it with nothing declared. |

## Checklist

- [ ] `updatedAt` + `BEFORE INSERT/UPDATE` trigger on every table (`default now()` on an
      insert-only table's stamp column — the client never sends it).
- [ ] `deletedAt` on every table + a child-tombstone trigger on every parent.
- [ ] Tombstone purge scheduled, retention **>** engine `maxOfflineGap`.
- [ ] RLS enabled + `auth.uid()` policies on every table.
- [ ] `scopeColumn` declared for any table whose RLS is broader than the partition.
- [ ] Every synced table in the `supabase_realtime` publication.

See **[[Operations and Troubleshooting]] → Schema evolution** before you change a live schema.
