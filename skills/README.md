# Publisher skills

Task-specific guides for working with Malloy through this Publisher deployment. Claude Code auto-discovers them via the `.claude/skills/` symlinks; other hosts pull the same content as MCP prompts from the Publisher endpoint. Start with [`malloy-getting-started`](malloy-getting-started/SKILL.md); use `malloy-modeling` to build a model, `malloy-analysis` to answer questions, and `malloy-review` to check Malloy for correctness.

[`packages/skills`](../packages/skills) publishes this directory to npm, for consumers that need the files themselves without cloning. That is the channel the `reference/` directories reach: the MCP prompts carry each `SKILL.md` body and nothing else. It copies this tree in when it is packed, so adding a skill here needs no extra step to ship it.

## Where these come from

Most of these skills are **shared, open-source Malloy skills** kept in sync with the Credible source-of-truth repo (`ms2data/agent-skills`, `skills/`). The intent is that the shared skills are the *same text* in both repos, copyable verbatim in either direction. Automation will come later; for now the copy is manual (`cp`). Two rules make it work:

- **`credible-*` skills never land here.** Anything named `credible-*` in the upstream repo is Credible-platform-specific and is never copied into this open-source repo. The copy keys off the `credible-` prefix. If you ever see a `credible-*` file under this tree, it is a stray — it should be git-ignored, not committed (`git ls-files | grep credible-` must stay empty).
- **Shared skills carry no Credible-platform-specific answers.** They describe generic Malloy and the open-source Publisher only — no hosted draft/publish flow, retrieval-engine annotations (`#(index)`/`#(agent-hidden)`), or platform tools like `search_database_schema`/`execute_query_draft`. Open-source Publisher features (`publisher.json` `explores`/`queryableSources`, `export {}`) are fair game. The Publisher-only authoring tools `malloy_compile` / `malloy_reloadPackage` stay in the host/router skills, not the shared set (see the tool-names section below).

## Shared vs Publisher-specific

- **Shared engine skills** (identical to upstream): `malloy-model`, `malloy-analyze`, `malloy-analysis`, `malloy-charts`, `malloy-queries`, `malloy-debug`, `malloy-define`, `malloy-discover`, `malloy-notebooks`, `malloy-review`, `malloy-scope`, `malloy-gotchas-*`, `malloy-notebook-chat`, `malloy-phrase-detection`, `malloy-analysis-pitfalls`, `malloy-analysis-report`, `malloy-html-data-app*`, `malloy-lookml-review`, `malloy-patterns`.
- **Publisher-specific skills** (not shared): `malloy-modeling`, `malloy-publish`, `malloy-document`, `malloy-getting-started`, and the root `malloy` index (Publisher's own host/router entry points), plus `malloy-materialization-tuning` (a tuning skill built on the `malloy-pub` CLI). These name Publisher's own tools directly and are never synced upstream to `ms2data/agent-skills`.

## Tool names in shared skills

Shared skills refer to MCP tools by **bare name** — `get_context`, `execute_query`, `search_malloy_docs` — plus a note that the exact prefixed name depends on the host. This Publisher server exposes them as **`malloy_getContext`**, **`malloy_executeQuery`**, and **`malloy_searchDocs`** (and adds `malloy_compile` / `malloy_reloadPackage`, which are Publisher-only and appear only in the host/router skills). When a shared skill says `get_context`, use `malloy_getContext`; match each bare name to the tool you actually have. The Publisher-specific host/router skills and `AGENTS.md` name the `malloy_*` tools directly.

## Adding or updating a skill

- Update a shared skill **upstream first** (in `ms2data/agent-skills`), then copy it here — that keeps the two byte-identical. Editing only this copy makes the next sync a conflict.
- A new skill directory needs a `.claude/skills/<name>` symlink (`ln -s ../../skills/<name> .claude/skills/<name>`) so Claude Code discovers it.
- A shared skill may only `skill:`-reference other shared skills; refer to a host wrapper in neutral prose so a verbatim copy never leaves a dangling reference.
