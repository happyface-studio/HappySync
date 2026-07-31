# Wiki sources

These Markdown files are the source for the [HappySync GitHub Wiki](https://github.com/happyface-studio/HappySync/wiki).
GitHub wikis are a **separate git repository** (`HappySync.wiki.git`) with their own credentials, so
they're authored here and published with the steps below. Page links use `[[Page Name]]` syntax;
GitHub resolves `Getting Started` → `Getting-Started.md`, so keep filenames hyphenated.

## Pages

**The developer documentation lives in DocC**, not here — `Sources/HappySync/Documentation.docc/`,
published by [Swift Package Index](https://swiftpackageindex.com/happyface-studio/HappySync/documentation/happysync)
and readable in Xcode. It sits next to the symbols it documents, which is the only arrangement that
keeps the two from drifting (issue #57). The pages below that used to hold that content are now
one-paragraph pointers to it; edit the DocC article instead.

| File | Wiki page | |
|---|---|---|
| `Home.md` | Landing page | index |
| `Claude-Skill.md` | Claude Skill | the only full article left here |
| `_Sidebar.md` | Sidebar nav | index |
| `Getting-Started.md` | Getting Started | → DocC |
| `Server-Setup.md` | Server Setup | → DocC |
| `API-Reference.md` | API Reference | → DocC |
| `Operations-and-Troubleshooting.md` | Operations and Troubleshooting | → DocC |
| `Testing.md` | Testing | → DocC |
| `Architecture.md` | Architecture | → DocC |

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
