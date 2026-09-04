<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Publisher skills

Task-specific guides for working with Malloy through this Publisher deployment. Claude Code auto-discovers them via the `.claude/skills/` symlinks; other hosts pull the same content as MCP prompts from the Publisher endpoint. Start with [`malloy-getting-started`](malloy-getting-started/SKILL.md); use `malloy-modeling` to build a model, `malloy-analysis` to answer questions, and `malloy-review` to check Malloy for correctness.

[`packages/skills`](../packages/skills) publishes this directory to npm, for consumers that need the files themselves without cloning. The MCP prompts carry the same tree: each `SKILL.md` body as one prompt, plus every `reference/*.md` as its own prompt named `<skill>/<file stem>`. It copies this tree in when it is packed, so adding a skill here needs no packaging step. It does need a version bump: `skills-npm.yml`'s PR check requires the version in [`packages/skills/package.json`](../packages/skills/package.json) to be ahead of what is on npm whenever a PR touches this directory, because a published version can never be replaced. A PR that adds or edits a skill without that bump goes red.

## What ships: `manifests/publisher-local.json`

[`manifests/publisher-local.json`](../manifests/publisher-local.json) decides which skills a deployment ships, and **every channel resolves it: none globs this directory.** The npm pack, the MCP prompt bundle, the `.claude/skills` symlinks, and the scaffolder all read the same list. The pack and the bundle agree with it by construction, because filtering on it is how they choose what to copy, and the scaffolder inherits that through the packed directory; `packages/skills/src/manifest.spec.ts` asserts the two that could still drift -- the manifest against this directory, and against the `.claude/skills` symlinks.

That matters because the four channels used to take "everything under `skills/` minus `credible-*`" independently, so a skill added here shipped everywhere by default and there was nowhere to say otherwise. Registering a skill is now one line in the manifest, and forgetting to is a red build rather than a silent non-ship.

`groups` names the two roles a consumer can take on its own: `analysis` (11 skills) is what an agent answering questions over a published model loads, `modeling` (31) what an agent building or editing a model loads. An eval that measures one of those agents installs the matching group rather than the whole set, because an answerer holding the whole library is a different system from the one a customer's analysis agent is. Groups may overlap, and a skill in neither ships anyway; a group is a curated install set, not a partition.

**A group is installable on its own**, which is a property `manifest.spec.ts` holds it to: a member never `skill:`-references a skill outside its group, so nothing tells the agent to read what it does not have. That is why `malloy-getting-started` and `malloy-analysis-report` name `malloy-gotchas-modeling` and `malloy-model` in prose rather than as `skill:` references. Both are modeling doctrine, and an answerer that followed the reference would hold exactly what the `analysis` group exists to withhold.

The `malloy` index is the case that forces the distinction, and it follows the same rule. It is the catalogue of every Malloy skill, so its table has a row per skill by definition and necessarily names skills outside the group it ships in. Those rows are plain names rather than `skill:` references. A catalogue row is not an instruction to go read something, and stating it as a bare name is how the file says so, which is why the index needs no exemption from the closure test.

`supporting` stays empty on purpose: agents discover a second skills directory poorly, and an SDK `Skill` tool cannot invoke from one at all.

## Where these come from

Most of these skills are **shared, open-source Malloy skills**, and **this repository is their source of truth.** The mechanism that carries them to `ms2data/agent-skills` is being settled in `ms2data/service#6177`; edit them here either way.

Two rules make it work:

- **`credible-*` skills never land here.** Anything named `credible-*` in the upstream repo is Credible-platform-specific and is never copied into this open-source repo. The copy keys off the `credible-` prefix. If you ever see a `credible-*` file under this tree, it is a stray: it should be git-ignored, not committed (`git ls-files | grep credible-` must stay empty).
- **Shared skills carry no Credible-platform-specific answers.** They describe generic Malloy and the open-source Publisher only, with no hosted draft/publish flow, retrieval-engine annotations (`#(index)`/`#(agent-hidden)`), or platform tools like `execute_query_draft`. Open-source Publisher features (`publisher.json` `explores`/`queryableSources`, `export {}`) are fair game. The Publisher-only authoring tools `malloy_compile` / `malloy_reloadPackage` stay in the host/router skills, not the shared set (see the tool-names section below).

## Shared vs Publisher-specific

- **Shared engine skills** (identical to upstream): `malloy-model`, `malloy-model-as-you-go`, `malloy-materialization`, `malloy-analyze`, `malloy-analysis`, `malloy-charts`, `malloy-queries`, `malloy-debug`, `malloy-define`, `malloy-discover`, `malloy-notebooks`, `malloy-review`, `malloy-scope`, `malloy-gotchas-*`, `malloy-notebook-chat`, `malloy-phrase-detection`, `malloy-analysis-pitfalls`, `malloy-analysis-report`, `malloy-html-data-app*`, `malloy-lookml-review`, `malloy-patterns`.
- **Publisher-specific skills** (not shared): `malloy-modeling`, `malloy-publish`, `malloy-document`, `malloy-getting-started`, and the root `malloy` index (Publisher's own host/router entry points), plus `malloy-materialization-tuning` (a tuning skill built on the `malloy-pub` CLI) and `malloy-dashboards` (dashboards are a Publisher surface). These name Publisher's own tools directly and are never synced upstream to `ms2data/agent-skills`.

## Evaluation skills

`eval-loop`, `eval-answer`, `eval-diagnose` and `eval-improve` are the model-evaluation loop: a set
of questions with goldens computed from raw tables, a blind answerer over the model, a judge, a
diagnosis of each failure, and one smallest model edit gated by a re-run. They are shared skills
(upstream: `ms2data/agent-skills`) and ship in the `eval` group. Their Python scripts import each
other by path from `skills/eval-answer/scripts`, so they run in place from a checkout, not from the
pack. `manifests/publisher-local.json`'s groups are what the loop installs for the
agents it spawns: the blind answerer, the agent under measurement, loads `analysis`, and the
improver loads `eval-improve` plus `modeling`. Neither loads the `eval` group, which is what keeps
the judge's rubric and the acceptance check away from the agents they score. The engine-side evaluation of `get_context` itself (fixed-term replay,
contract probes) is deliberately **not** here: it is Credible's question about its hosted engine and
lives in an unlisted skill upstream. `credibledata/malloy-samples#23` is a set anyone can run the
loop on.

The five eval skills are mirrored FROM here to `ms2data/agent-skills`, like every other shared
skill. The upstream copy has drifted before and it matters more here than elsewhere, because the
scripts are the harness: a run made with a stale copy produces a ledger that reads as current and
is not. Three files exist only upstream and are not part of the set: `eval-loop/scripts/run_all.py`
(a sequencing orchestrator, which `skill:eval-loop` forbids), `eval-answer/reference/judge.md` (now
`skill:eval-judge`) and `eval-answer/scripts/mcp_client.py`. Delete them when mirroring rather than
copying them back.

## Tool names in shared skills

Shared skills refer to MCP tools by **bare name** (`get_context`, `execute_query`, `search_malloy_docs`), plus a note that the exact prefixed name depends on the host. `search_database_schema` maps the same way if a shared skill starts using it. This Publisher server exposes them as **`malloy_getContext`**, **`malloy_executeQuery`**, **`malloy_searchDocs`**, and **`malloy_searchDatabaseSchema`** (and adds `malloy_compile` / `malloy_reloadPackage`, which are Publisher-only and appear only in the host/router skills). When a shared skill says `get_context`, use `malloy_getContext`; match each bare name to the tool you actually have. The Publisher-specific host/router skills and `AGENTS.md` name the `malloy_*` tools directly.

## Adding or updating a skill

- **Edit a shared skill here.** This repo is the source of truth for them. The mechanism that carries them to Credible is being settled in `ms2data/service#6177`; **until it lands, mirror a shared-skill edit into `ms2data/agent-skills` by hand**, or the two copies drift.
- **Register it in [`manifests/publisher-local.json`](../manifests/publisher-local.json).** An unregistered skill ships through no channel, and `manifest.spec.ts` fails rather than letting that pass quietly.
- **`malloy-dashboards` is not a shared skill.** `agent-skills` carries a file by the same name written for its own surfaces; the two describe the same feature and are not copies of each other. Do not copy it in either direction.
- **Any edit under `skills/` means regenerating the MCP bundle** (`cd packages/server && bun run src/mcp/skills/build_skills_bundle.ts ../../skills`) and committing the resulting `src/mcp/skills/skills_bundle.json`. It is a committed generated asset, and `skills_bundle.spec.ts` fails the build when it drifts from this tree. The bundle is committed indented so that two PRs touching different skills merge cleanly; if you do hit a conflict in it, resolve it by regenerating from the merged `skills/` tree, never by editing the JSON by hand.
- A new skill directory needs a `.claude/skills/<name>` symlink (`ln -s ../../skills/<name> .claude/skills/<name>`) so Claude Code discovers it.
- A shared skill may only `skill:`-reference other shared skills; refer to a host wrapper in neutral prose so a verbatim copy never leaves a dangling reference.
