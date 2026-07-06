# Claude Skill

HappySync ships an installable **Claude skill** that teaches Claude Code (or any Claude Agent SDK
harness) how to integrate and operate the engine correctly — the mental model, the required server
schema, the table manifest, lifecycle, and dead-letter repair. Install it and your AI assistant
wires HappySync the right way instead of guessing.

The skill lives in the repo at
[`.claude/skills/happysync/`](https://github.com/happyface-studio/HappySync/tree/main/.claude/skills/happysync):

```
.claude/skills/happysync/
├── SKILL.md                    # entry point (mental model + minimal integration)
└── references/
    ├── server-setup.md         # the required Supabase schema
    ├── api.md                  # every public type and method
    └── operations.md           # status, dead letters, teardown, schema evolution
```

## Install it in your app's repo (project skill)

From the root of the app that consumes HappySync:

```bash
mkdir -p .claude/skills
# from a checkout of HappySync:
cp -R /path/to/HappySync/.claude/skills/happysync .claude/skills/happysync
```

Or pull just the skill without cloning the whole repo:

```bash
mkdir -p .claude/skills/happysync/references
base=https://raw.githubusercontent.com/happyface-studio/HappySync/main/.claude/skills/happysync
curl -sSL $base/SKILL.md -o .claude/skills/happysync/SKILL.md
for f in server-setup api operations; do
  curl -sSL $base/references/$f.md -o .claude/skills/happysync/references/$f.md
done
```

Commit `.claude/skills/happysync/` and everyone on the team gets it automatically.

## Install it globally (available in every project)

```bash
mkdir -p ~/.claude/skills
cp -R /path/to/HappySync/.claude/skills/happysync ~/.claude/skills/happysync
```

## Use it

Once installed, Claude discovers the skill by its description and loads it when you're working on
HappySync integration. You can also invoke it explicitly:

```
/happysync
```

Then ask, for example:

- "Add HappySync to this app and wire the SyncEngine for my `recipes` and `recipeIngredients` tables."
- "Write the Supabase migration for the `updatedAt` trigger and `deletedAt` tombstones."
- "My writes are dead-lettering with a 42501 — help me diagnose and repair."

## Keep it current

The skill is versioned with the engine. After upgrading the HappySync package, re-copy the skill so
its API reference matches the release you're on.
