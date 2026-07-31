-- The demo's server side, in one file: `supabase start && supabase db reset` stands it up.
--
-- The app in Examples/ runs against an in-process fake and needs none of this. It's here because the
-- fake cannot check the half of the contract that lives on the server — the `updatedAt` trigger LWW
-- depends on, the tombstone propagation deletes depend on, RLS, and the Realtime publication. This
-- is what those four requirements look like for a two-table schema (see the Server Setup wiki page).

-- ---------------------------------------------------------------------------
-- Tables. Column names are camelCase and identical to the local SQLite schema —
-- HappySync does no name mapping, by contract.
-- ---------------------------------------------------------------------------

create table if not exists public.lists (
    id          uuid primary key,
    "userId"    uuid not null default auth.uid() references auth.users (id) on delete cascade,
    title       text not null,
    "updatedAt" timestamptz not null default now(),
    "deletedAt" timestamptz
);

create table if not exists public.items (
    id          uuid primary key,
    "listId"    uuid not null references public.lists (id) on delete cascade,
    "userId"    uuid not null default auth.uid() references auth.users (id) on delete cascade,
    title       text not null,
    done        boolean not null default false,
    "updatedAt" timestamptz not null default now(),
    "deletedAt" timestamptz
);

create index if not exists lists_cursor_idx on public.lists ("updatedAt", id);
create index if not exists items_cursor_idx on public.items ("updatedAt", id);

-- ---------------------------------------------------------------------------
-- 1. updatedAt — server-stamped, always. This is what makes last-write-wins
--    correct: two devices' clocks never enter the ordering. A client-sent value
--    is ignored (and HappySync strips it from every upload anyway).
-- ---------------------------------------------------------------------------

create or replace function public.stamp_updated_at()
returns trigger language plpgsql as $$
begin
  new."updatedAt" := now();
  return new;
end $$;

drop trigger if exists stamp_updated_at on public.lists;
create trigger stamp_updated_at
  before insert or update on public.lists
  for each row execute function public.stamp_updated_at();

drop trigger if exists stamp_updated_at on public.items;
create trigger stamp_updated_at
  before insert or update on public.items
  for each row execute function public.stamp_updated_at();

-- ---------------------------------------------------------------------------
-- 2. deletedAt — deletes are soft, so they can propagate on the next cursor
--    pull. A parent's children are tombstoned by trigger, NOT by a hard
--    `ON DELETE CASCADE`: a hard cascade removes the rows with no tombstone
--    left for other devices to pull, and they keep the children forever.
-- ---------------------------------------------------------------------------

create or replace function public.tombstone_list_items()
returns trigger language plpgsql as $$
begin
  if new."deletedAt" is not null and old."deletedAt" is null then
    update public.items
       set "deletedAt" = new."deletedAt"
     where "listId" = new.id and "deletedAt" is null;
  end if;
  return new;
end $$;

drop trigger if exists tombstone_list_items on public.lists;
create trigger tombstone_list_items
  after update on public.lists
  for each row execute function public.tombstone_list_items();

-- Purge tombstones on a schedule if you like — but the retention must exceed the engine's
-- `maxOfflineGap` (30 days by default), or a device offline longer than the retention never sees the
-- tombstone and resurrects the row:
--
--   delete from public.items where "deletedAt" < now() - interval '90 days';
--   delete from public.lists where "deletedAt" < now() - interval '90 days';

-- ---------------------------------------------------------------------------
-- 3. RLS — the security boundary. Here it equals the sync partition exactly,
--    so the demo's SyncTable declares no `scopeColumn`. Declare one when a
--    policy is deliberately broader than what a device should download.
-- ---------------------------------------------------------------------------

alter table public.lists enable row level security;
alter table public.items enable row level security;

drop policy if exists "own lists" on public.lists;
create policy "own lists" on public.lists
  for all using ("userId" = auth.uid()) with check ("userId" = auth.uid());

drop policy if exists "own items" on public.items;
create policy "own items" on public.items
  for all using ("userId" = auth.uid()) with check ("userId" = auth.uid());

-- ---------------------------------------------------------------------------
-- 4. Realtime — the doorbell. An event triggers a debounced pull; the payload
--    is never applied directly, so sync converges with or without this.
-- ---------------------------------------------------------------------------

alter publication supabase_realtime add table public.lists;
alter publication supabase_realtime add table public.items;
