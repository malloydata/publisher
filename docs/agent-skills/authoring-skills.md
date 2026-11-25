<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Authoring skills

How skills are structured in this repository, how they reference each other, how manifests
compose them for a use case, and how they reach different hosts. For the why behind the
shape of a skill, read [design principles](./design-principles.md) first.

## Repository layout

```
skills/
  malloy-model/
    SKILL.md
    reference/
      access-modifiers.md
      ...
  malloy-gotchas-modeling/
    SKILL.md
manifests/
  publisher-local.json
```

`skills/` is a flat collection of self-contained skills. A skill has no knowledge of which use
case it belongs to. `manifests/` defines skill compositions for each use case and tells the
host how to deploy them. This repository ships one manifest today, and a deployment that wants a
narrower set names a `groups` entry in it rather than adding a second file.

## A skill

Each skill is a directory `skills/<skill-name>/` with a `SKILL.md` at its root. The directory is
the skill: every file under it ships, laid out as the [Agent Skills spec](https://agentskills.io)
names:

```
SKILL.md
scripts/        programs the body runs as commands (python3 scripts/check.py)
references/     Markdown the body points to
assets/         files a skill copies into what it builds
```

`references/` is the folder to use for new skills. The skills here keep the `reference/` they
already have, and nothing moves. Both are served, and a body names a file by its path from the
skill root, such as `reference/joins.md`; `reference/` keeps the allowances it always had (see
[Budgets](#budgets)). Other folders (`templates/`, `data/`) work too.

The built-ins and `.skillignore` decide which files make up a skill for every channel (the npm
pack, the MCP bundle, a package's own skills):

- Dotfiles and dot-folders, `node_modules/`, `__pycache__/` and `*.pyc` are never part of a skill.
  `.skillignore` cannot bring them back.
- A `.skillignore` at the skill's root drops more, in gitignore syntax matched without regard to
  case. The skills that carry tests or fixtures use it: `*_test.py` keeps a test out of the pack
  and the bundle. `SKILL.md` is always kept.

A bundled skill here carries every file the skill's own `.skillignore` leaves. When Publisher
serves a package's skills or an agent's skills, its reader also skips a `SKILL.md` below the root,
`CLAUDE.md`, `CLAUDE.local.md`, `AGENTS.md` and `publisher.json` wherever they sit, because a host
reads those files with authority. In this repository that is a rule for authors: the pack and the
bundle do not filter them.

`.skillignore` curates; it does not hide anything. An ignored file stays in the repository and in
any package copy, and a client that reads the directory itself (Claude Code through
`.claude/skills`) sees it. [Packages](../packages.md#which-files-a-skill-holds) has the rule as a
package author meets it, with how `.skillignore` differs from `.credignore` and how to preview
a skill's files.

`SKILL.md` opens with YAML frontmatter, then the body:

```
---
name: malloy-model
description: Author Malloy semantic models: sources, dimensions, measures, joins, access modifiers.
---

Body in Markdown.
```

Those two keys are the whole header you author. Packing `@malloy-publisher/skills` adds a third,
`version:`, to its copy, so an installed skill says on disk which release it came from; the files
under `skills/` stay unstamped. Do not add a `version:` of your own — the pack fails rather than
write a second one.

The `description` is what a host reads on initialization to decide when the skill is relevant,
so write it as a precise trigger, not a summary. Keep the frontmatter valid YAML; a stray
unquoted colon in `description` will break the parse (quote the value if it contains a colon).
The repository's tests parse every `SKILL.md` frontmatter, so a malformed header fails CI.

Keep the body lean. Carry prevention content (the guidance that prevents errors the agent
cannot self-correct) and leave reference content to on-demand lookups. Put long reference
material in `reference/` files the body points to, rather than inline.

Keep a skill self-contained: do not reference the use case or manifest it belongs to, and do
not prescribe which skill loads next (Principle 1). An orchestrating skill may propose a
sequence and point each step at a focused skill; a focused skill must not name a follow-up.

## Cross-skill references

Refer to another skill with the `skill:` prefix, for example `skill:malloy-model` or
`skill:malloy-gotchas-modeling`. This is a logical name, not a file path. The host resolves it to
wherever that skill lives in the deployed layout. Reference-file paths inside a skill should
be relative to the skill, for example `reference/access-modifiers.md`, not an absolute or
host-specific path.

## Manifests

A manifest groups skills into a use case and tells the host how to deploy them.

This repository ships one, [`manifests/publisher-local.json`](../../manifests/publisher-local.json):

```json
{
  "name": "publisher-local",
  "description": "Every skill a local Malloy Publisher ships: ...",
  "trigger_hint": "when the user asks about modeling data, building a semantic model, ...",
  "auto_discovered": [
    "malloy-modeling",
    "malloy-model",
    "malloy-gotchas-modeling"
  ],
  "supporting": [],
  "groups": { "analysis": ["malloy-analysis", "malloy-queries"] }
}
```

The skill list above is abridged; the real file names every directory under `skills/`.

- `auto_discovered`: skills the host places where it discovers them automatically (the primary
  workflow skills to load when relevant).
- `supporting`: skills the host keeps on-demand, read when an auto-discovered skill directs the
  agent to them. **Empty here on purpose**: agents discover a second skills directory poorly, and
  an SDK `Skill` tool cannot invoke from one at all, so Publisher ships one flat set.
- `trigger_hint` (optional): text the host uses to generate rule files for IDEs that drive
  skill loading from rules; falls back to `description` if omitted.
- `groups` (optional): a role a consumer can take on its own, so someone who wants fewer
  skills gets a filter rather than a second directory. This repo names two, `analysis` and
  `modeling`, being what an answering agent and a modeling agent each load. Every member must
  also appear in `auto_discovered` or `supporting`. Groups may overlap.
- **A group is installable on its own.** A member never `skill:`-references a skill outside
  its group, so installing the group leaves nothing telling the agent to read what it does
  not have. Where a skill needs to mention another role's doctrine, it names it in prose
  instead of as a `skill:` reference. `manifest.spec.ts` holds groups to this, with one
  documented exception: the catalogue in `malloy-getting-started` names every skill by
  definition, so it names them as plain text rather than `skill:` references.

**Every channel resolves the manifest; none globs `skills/`.** The npm pack, the MCP prompt
bundle, the `.claude/skills` symlinks, and the scaffolder all read the same list. The pack and
the bundle agree with it by construction, because filtering on it is how they choose what to
copy, and the scaffolder inherits that through the packed directory;
`packages/skills/src/manifest.spec.ts` asserts the two that could still drift -- the manifest
against `skills/`, and against the `.claude/skills` symlinks. A manifest is only worth
having if it is the single answer to "what ships"; before it existed each channel answered that
question independently, and a skill added to the tree shipped everywhere by default.

How a host maps `auto_discovered` versus `supporting` onto disk (which directory, which rule
file) is the host's concern, not this repository's. The repository serves content and manifest
metadata; directory conventions live in the host, so a new host or editor only needs host-side
changes.

## Reaching different hosts

Skills reach hosts through two channels, and they do not have equal fidelity.

- Native skill files. Skill-aware hosts read the `SKILL.md` files directly. This is full
  fidelity: descriptions load on initialization, prevention content can be always-on, and the
  manifest's auto-discovered and supporting split is honored.
- MCP prompts and resources. Hosts that surface MCP but do not load skill files the same way
  reach the same content as MCP prompts and resources, which are on-demand by nature.

The consequence for authoring: always-on prevention content degrades to on-demand on a host
that only has the MCP channel, which weakens its first-pass-correctness purpose. When a piece
of prevention guidance is load-bearing (a syntax gotcha that otherwise causes a cascading
error), make sure it is also reachable on demand and worth surfacing as such, rather than
assuming it is always loaded.

## Budgets

A skill's files are read, hashed and listed when Publisher loads a package. Most of that has a
bound, set out below; the number of skills under a skills root, and the Markdown a package's own
skills carry inline, have none. Each crossing is a load warning on the package, and for an agent
also in `get_agent`'s `warnings`; `cred publish --dry-run` shows the upload set. A crossing skips
the one file it concerns, unless the row says it drops the agent. Inline files are a skill's
Markdown outside `assets/`, carried in the agent definition; listed files are every other file,
which Publisher lists by path, size and sha256 and reads on request.

| Budget | Limit | When it is crossed |
| --- | --- | --- |
| An agent's `instructions` and each task file | 16 KiB each | Drops the agent |
| A `SKILL.md` | 256 KiB | The skill is not served, and an agent that names it is dropped |
| An agent's inline Markdown | 256 KiB; every `SKILL.md` is counted first | `SKILL.md` files alone over it drop the agent; other Markdown that does not fit is listed instead, with a warning |
| One package skill's Markdown file | 256 KiB per file. A package's own skills have no total | The file is listed instead of inlined |
| Skills under a skills root, and under `skills/` | No limit. Each skill has its own `reference/` allowance | Nothing; the reload cost grows with them |
| One listed file | Under 1 MiB (1,048,576 bytes is out) | The file is skipped |
| Listed files per agent | 4 MiB and 200 files | Later files are skipped |
| Listed files per package, across agents | 8 MiB, each path counted once, agents charged in manifest order | Later files are skipped |
| Listed files of a package's own `skills/` | 4 MiB and 200 files per package, apart from the agents' 8 MiB | Later files are skipped |
| A listed path | Segments of `[A-Za-z0-9_][A-Za-z0-9._-]*`, at most 200 characters with the skill's directory name, at most 8 levels deep, no control character, no `.malloy` or `.malloynb`, a regular file | The file is skipped. Inline Markdown keeps lenient names, but not a path over 200 characters from the skill's root (next row) |
| A path in the walk | At most 200 characters from the skill's root, inline Markdown included. A directory over it is not descended | The entry is skipped, with one warning naming the first |
| Entries walked | Per agent, 1,000 under `assets/` and 1,000 elsewhere, shared by its skills. The package's `skills/` has the same pair. Entries `.skillignore` matches count | The walk stops there, and the rest are not included |
| `reference/` | At most 1,000 Markdown files per skill, whether or not `.skillignore` matches them, read no further than 10,000 entries of any kind. It is outside the entries walked, is followed when it is a link, and is read before other Markdown. `references/` has none of this and counts against the entries walked | The rest are not read |
| `.skillignore` | 16 KiB; at most 50 patterns, of at most 200 characters each and 1,024 together (comments and blank lines are free); two `*` or `**` segments a pattern, a run of `**` counting once | The pattern, or the file, is skipped |
| Frontmatter | YAML 1.2; at most 100 references to one anchor; output at most 5 times the source plus 10,000 characters | An empty `frontmatter` and a line read of `name` and `description` |
| `get_skill` result | 35,000 characters, Cursor's limit | Keeps the body and trims `files` with a count, then drops the parsed frontmatter, then the body |

A Markdown file of a bundled skill must come back whole from `get_skill` at that limit, so keep each one under it and split a long reference file in two; a bundle test holds every file to it.

Two budgets belong to the host that lists skills, not to Publisher: an agent computer lists every
skill's name and description in each model call against one shared budget (16 skills and 4,000
characters), and it refuses an agent past it. A `description` written as a YAML block scalar
(`>-` or `|`) counts its whole text against that budget.

### What a reload costs

`Package.refreshServingIdentity` walks and hashes a package's skills and agents synchronously, on
creation and on every reload, so the event loop does nothing else for as long as it takes. A host
that reloads on every turn pays it every turn. `bun run perf:skills-reload` in `packages/server`
builds a package with 8 agents and a package skill, each with one skill, and times it. The first
row is the largest each budget allows for those nine skills. A package with more skills costs more,
since nothing bounds their number. On a development machine with a warm page cache, the median of
5 reloads:

| Package | Blocks the loop for |
| --- | --- |
| One agent, `SKILL.md` only | 2 ms |
| Every budget spent in nine skills: about 11.7 MiB in 1,800 listed files, 1,000 entries under `assets/`, 999 files in `references/` and 907 in `reference/` each | 1.3 s |
| The 300 large listed files alone (about 11.7 MiB) | 109 ms |
| The 1,000 `assets/` entries alone | 257 ms |
| The 999 `references/` files alone | 616 ms |
| The 907 `reference/` files alone | 581 ms |

A Markdown file costs about 0.07 ms to read and digest and a walked entry about 10 microseconds, so
file count moves the time most. Keep a skill to the files an agent uses.

## Authoring a new skill

1. Create `skills/<skill-name>/` with a `SKILL.md` and any `references/` files.
2. Keep the skill self-contained: no reference to its use case or manifest.
3. Use `skill:<other-skill>` to point at other skills.
4. Add the skill to the relevant manifest(s) under `auto_discovered` or `supporting`.
5. Cut a release per the repository's release mechanism (the host serves content at a tagged
   version).
