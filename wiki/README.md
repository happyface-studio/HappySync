# Wiki sources

These Markdown files are the source for the [HappySync GitHub Wiki](https://github.com/happyface-studio/HappySync/wiki).
GitHub wikis are a **separate git repository** (`HappySync.wiki.git`) with their own credentials, so
they're authored here and published with the steps below. Page links use `[[Page Name]]` syntax;
GitHub resolves `Getting Started` → `Getting-Started.md`, so keep filenames hyphenated.

## Pages

| File | Wiki page |
|---|---|
| `Home.md` | Landing page |
| `Getting-Started.md` | Getting Started |
| `Server-Setup.md` | Server Setup |
| `API-Reference.md` | API Reference |
| `Operations-and-Troubleshooting.md` | Operations and Troubleshooting |
| `Testing.md` | Testing |
| `Architecture.md` | Architecture |
| `Claude-Skill.md` | Claude Skill |
| `_Sidebar.md` | Sidebar nav (all pages) |

## Publish

The wiki must be **enabled** once (repo **Settings → Features → Wikis**) and initialized with any
first page via the web UI, which creates the `.wiki.git` repo. Then, from a machine authenticated to
GitHub (the wiki repo is not reachable through the sandboxed CI/agent proxy):

```bash
git clone https://github.com/happyface-studio/HappySync.wiki.git
cp wiki/*.md HappySync.wiki/
cd HappySync.wiki
git add .
git commit -m "Sync wiki from repo wiki/ sources"
git push
```

Re-run after editing any page here to keep the published wiki in sync.
