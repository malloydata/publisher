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
serves a package's skills, its reader also skips a `SKILL.md` below the root,
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
so write it as a precise trigger, not a summary. Keep the frontmatter valid YAML and under 4 KiB; a
stray unquoted colon in `description` will break the parse (quote the value if it contains a colon).
Publisher reads a frontmatter that does not parse, or is over 4 KiB, line by line for `name` and
`description` only, and warns.
The repository's tests parse every `SKILL.md` frontmatter, so a malformed header fails CI.

Keep the body lean. Carry prevention content (the guidance that prevents errors the agent
cannot self-correct) and leave reference content to on-demand lookups. Put long reference
material in `reference/` files the body points to, rather than inline.

Keep a skill self-contained: do not reference the use case or manifest it belongs to, and do
not prescribe which skill loads next (Principle 1). An orchestrating skill may propose a
sequence and point each step at a focused skill; a focused skill must not name a follow-up.

### A skill in a package

A package carries its own skills in its `default` skill set: a folder of skills that
`publisher.json` declares. The shortest layout is a `skills/` folder at the package root:

```
publisher.json
skills/
  margin-rules/
    SKILL.md
```

```json
{ "name": "storefront", "skillSets": { "default": { "path": "skills" } } }
```

`skills/margin-rules/SKILL.md` is a complete skill:

```markdown
---
name: margin-rules
description: Use when a question involves margin, so net revenue is not mistaken for gross.
---

Margin is net revenue less cost, never gross revenue less cost. `margin_pct` in `orders.malloy` already does this; use it.
```

Every session on the package sees the `default` set's skills beside the bundled ones. A package
adds named sets for guidance that belongs to one job, `"analyst": { "path": "skill_sets/analyst" }`
with `skill_sets/analyst/<skill>/SKILL.md`, and a workflow lists the sets it needs in `skillSets`.
Each set is read once per load and served whole by name; see
[Packages](../packages.md#skill-sets-and-workflows) for sets, workflows, refusals and the codes a
finding carries.

A `default` skill whose `name` matches a bundled skill replaces it in Publisher, for that package,
only if the `default` set's `overrides` lists the name; otherwise the bundled skill is served and
the set carries a finding that names the fix. Credible's agent skips such a package skill and
keeps the bundled one, so give a package skill a name no bundled skill has if it must behave the
same in both. The `malloy-` and `eval-` prefixes belong to bundled skills.

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

A skill's files are read, hashed and listed when Publisher loads a package. Each bound is set out
below, and each skill set is measured on its own.
Each crossing is a load warning on the package, and a set's own findings also travel on the set
(`get_skill_set`'s `warnings`); `cred publish --dry-run` shows the upload set. A crossing skips the one file it
concerns, unless the row says it sets the skill aside or refuses a set or workflow. Inline files are
a skill's Markdown outside `assets/`, carried in the set's reply; listed files are every other
file, which Publisher lists by path, size and sha256 and reads on request.

| Budget | Limit | When it is crossed |
| --- | --- | --- |
| A workflow's `instructions` | 16 KiB | Refuses the workflow |
| Skill sets a workflow lists | 8 | Refuses the workflow |
| Skill sets and workflows a package declares | 32 and 64 | Refuses the whole `skillSets` or `workflows` declaration, so none of its entries is served |
| Names in a set's `overrides` | 64, and read only on `default` | Reads as empty, with a finding; on any other set a non-empty `overrides` is ignored, with a finding |
| A `SKILL.md` | 256 KiB | The skill is not served |
| A skill set's inline Markdown | 256 KiB per set; every `SKILL.md` is counted first | A `SKILL.md` that does not fit is skipped with its skill; other Markdown that does not fit is listed instead, with a warning |
| Inline Markdown files per skill set | 1,000, `SKILL.md` included. A zero-byte file passes the byte budget, so without this a set could inline about 64 skills of 1,000 files each | The file is skipped, with a warning |
| One skill Markdown file | 256 KiB per file | The file is listed instead of inlined |
| Skills in a skill set | 64 skill directories, counted in name order as the walk reaches them. Each skill has its own `reference/` and `references/` allowance | The first 64 by name are served and the rest are not read, with one finding on the set |
| Skill findings named per set | 20 per set, then one line counting the rest. Skipped assets are capped the same way | The remaining findings are counted, not named, so a reply that carries the set stays small |
| One listed file | Under 1 MiB (1,048,576 bytes is out) | The file is skipped |
| Listed files per skill set | 4 MiB and 200 files | Later files are skipped |
| Listed files per package, across sets | 8 MiB, each path counted once, sets charged whole in name order | The set that would cross it is refused, and so is every workflow that lists it |
| A listed path | Segments of `[A-Za-z0-9_][A-Za-z0-9._-]*`, at most 200 characters with the skill's directory name, at most 7 directories deep below the skill's root (a path under `assets/` counts its depth from `assets/`, so it may go 7 below `assets/`, 8 counting `assets/` itself), no control character, no `.malloy` or `.malloynb`, a regular file | The file is skipped. Inline Markdown keeps lenient names, but not a path over 200 characters from the skill's root (next row) |
| A path in the walk | At most 200 characters from the skill's root, inline Markdown included. A directory over it is not descended | The entry is skipped, with one warning naming the first |
| Entries walked | Per skill set, 1,000 under `assets/` and 1,000 elsewhere, shared by its skills. Entries `.skillignore` matches count | The walk stops there, and the rest are not included |
| The search for a nested `publisher.json` | Spends the same 1,000 entries, since a skill's own `publisher.json` has to be ruled out | A skill whose search runs out of budget is set aside, with a finding, as is each later skill of the set once the budget is spent |
| A skill directory's name | Segments of `[A-Za-z0-9_][A-Za-z0-9._-]*`, no `..` and no control character, and no two skills of a set that differ only by case | The skill is skipped, with a finding (the second of a case collision) |
| `reference/` and `references/` | At most 1,000 Markdown files per skill, whether or not `.skillignore` matches them, read no further than 10,000 entries of any kind. Both are outside the entries walked, are followed when they are a link, and are read before other Markdown | The rest are not read |
| `.skillignore` | 16 KiB; at most 50 patterns, of at most 200 characters each and 1,024 together (comments and blank lines are free); two `*` or `**` segments a pattern, a run of `**` counting once | The pattern, or the file, is skipped |
| Frontmatter | YAML 1.2; at most 4 KiB; at most 100 references to one anchor; output at most 5 times the source plus 10,000 characters | An empty `frontmatter` and a line read of `name` and `description`, with a warning |
| `get_skill` result | 35,000 characters, Cursor's limit | Keeps the body and trims `files` with a count, then drops the parsed frontmatter, then the body |
| `get_skill_set` and `get_workflow` result | 35,000 characters | Steps down from the full set to a summary of its files, then to the set's `name`, `digest` and `source` (a workflow keeps its `instructions` and each set's `name` and `digest` one step longer), with a message saying where to read the rest. REST has no such limit |

A Markdown file of a bundled skill must come back whole from `get_skill` at that limit, so keep each one under it and split a long reference file in two; a bundle test holds every file to it.

Two budgets belong to the host that lists skills, not to Publisher: an agent computer lists every
skill's name and description in each model call against one shared budget (16 skills and 4,000
characters), and it refuses a workflow past it. A `description` written as a YAML block scalar
(`>-` or `|`) counts its whole text against that budget.

### What a reload costs

The package-load worker walks and hashes a package's skill sets and workflows, on creation and on
every reload, in the same job that compiles its models, so the server's event loop does not wait
for it. A load bound to a build manifest walks twice, both times in the worker. The walk counts
against that job's timeout (`packageLoad.jobTimeoutMs`, 120 s). Every declared set is read once,
however many workflows list it. The main thread pays only to receive the result and adopt it:
rebuild each set's readers from the rows and shas the worker shipped, reading no file and hashing nothing. `bun run perf:skills-reload` in `packages/server` builds a package with the most
sets it may declare (32), each spending every per-set cap, and times both sides. A package with up to 64 skills in each set
costs more than the bench's, which keeps each set's 1,000 inline files in one skill. On a development
machine with a warm page cache, the median of 5 reloads, the bench's default, in milliseconds:

| Package | Worker: walk | Worker: hash | Worker: serialize | Main: receive | Main: adopt |
| --- | --- | --- | --- | --- | --- |
| One set, `SKILL.md` only | 2 | under 1 | under 1 | under 1 | under 1 |
| 32 sets, every per-set cap spent: 32,000 inline Markdown files and 6,400 listed files (6.3 MiB) | 2,147 | 94 | 85 | 17 | 3 |
| The listed files alone (6,400 files, 200 per set) | 389 | 15 | 18 | 4 | under 1 |
| The inline Markdown alone (32,000 files, 1,000 per set) | 1,907 | 81 | 67 | 14 | 3 |

A walked file costs about 60 microseconds in the worker, so file count moves the time most. Keep a
skill to the files an agent uses. The main thread's share is the structured-clone receive of the
walk and the adoption of its rows, which grow with the number of files and inline Markdown
bytes, not with their content: about 20 ms at the most a package may declare.

If the walk cannot finish (it throws, or a listed set or an instructions file exists but cannot be
read), the package's models still load, one `skill-sets-unavailable` warning names the cause, and
every set and workflow route and `get_skill_set` and `get_workflow` answer 409
`SKILL_SETS_UNAVAILABLE` with the reason, until the package reloads. So do the skills routes and `get_skill` when
the package declares a `default` set, even for a bundled skill's name, because a `default` skill can replace a bundled one; without one the bundled skills serve as usual. A name the
manifest never declares is still a 404.

## Authoring a new skill

1. Create `skills/<skill-name>/` with a `SKILL.md` and any `references/` files.
2. Keep the skill self-contained: no reference to its use case or manifest.
3. Use `skill:<other-skill>` to point at other skills.
4. Add the skill to the relevant manifest(s) under `auto_discovered` or `supporting`.
5. Cut a release per the repository's release mechanism (the host serves content at a tagged
   version).
