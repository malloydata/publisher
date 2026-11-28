---
name: malloy-define-workflow
description: Define a package's skill sets (named folders of skills, including `default`, the package's own skills) and workflows (a named job with instructions that lists skill sets), declared in a package's publisher.json. Use when the user asks to create, add, change, fix or migrate a workflow or skill set in a package, to give a package its own skills, or to turn an old package agent into workflows. Not for running a workflow that already exists (that is malloy-run-workflow), and not for a one-off question, which you just answer.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Define package skill sets and workflows

> **Tool names** are written bare here - `get_workflow`, `get_skill_set`, `reload_package`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

A **skill set** is a named folder of skills a package ships. A **workflow** is a named job an author ships inside a package: **instructions** (the brief to follow) and the skill sets to use. Publisher parses and serves both and runs nothing; a caller adopts a workflow with `get_workflow` and reads a set with `get_skill_set` (see `malloy-run-workflow`). The files are ordinary package files, so you write them like any other model change. A package does not need models to carry them: one that holds only `publisher.json` and its skill folders is valid.

## When to create one

Create a workflow only when the user asks for one or asks for a job that other people or runs will repeat. Create a skill set when the user wants the package to carry its own skills. Do not create either to answer a question or make a model change; do that directly. A workflow is a standing definition others rely on, so one nobody asked for is clutter.

## The layout

The shortest package declares one set, `default`, at a folder of skills. Its skills are the package's own, and every session on the package sees them:

```
publisher.json
skills/
  margin-rules/
    SKILL.md
```

```json
{ "name": "storefront", "skillSets": { "default": { "path": "skills" } } }
```

`skills/margin-rules/SKILL.md` is a complete skill (`name` equals the directory's name; `description` is one sentence saying when to read it):

```markdown
---
name: margin-rules
description: Use when a question involves margin, so net revenue is not mistaken for gross.
---

Margin is net revenue less cost, never gross revenue less cost. `margin_pct` in `orders.malloy` already does this; use it.
```

A package adds a named set and a workflow that lists it. A set's name is its key under `skillSets`, whatever its folder is called; the convention for several sets is `skill_sets/<set>/`:

```json
{
  "name": "storefront",
  "skillSets": {
    "default": { "path": "skill_sets/default", "description": "House conventions" },
    "analyst": { "path": "skill_sets/analyst" }
  },
  "workflows": {
    "weekly-report": {
      "description": "Writes the Monday revenue summary for the storefront package",
      "instructions": "workflows/weekly-report/instructions.md",
      "skillSets": ["analyst", "default"]
    }
  }
}
```

```
skill_sets/
  default/storefront-conventions/SKILL.md
  analyst/analyst-conventions/SKILL.md
workflows/
  weekly-report/instructions.md
```

| Key | Rule |
| --- | --- |
| name (the key, of a set or a workflow) | `^[a-z0-9]+(-[a-z0-9]+)*$`, at most 64 characters. A workflow named `default` is an ordinary workflow. |
| `skillSets.<name>.path` | Required. A package-relative folder whose subfolders are skills. Two sets may not declare one path. |
| `skillSets.<name>.description` | Optional. A non-empty line saying what the set holds. |
| `skillSets.<name>.overrides` | Optional, and read only on `default`: up to 64 distinct skill names that replace a bundled skill of the same name. On any other set it is ignored, with a finding. |
| `workflows.<name>.description` | Optional. A non-empty line saying what the workflow is for. |
| `workflows.<name>.instructions` | Required. Package-relative path of a `.md` file. At most 16 KiB. |
| `workflows.<name>.skillSets` | Optional. At most 8 distinct names of declared sets, in the order an installer reads them. |

A package declares at most 32 sets and 64 workflows. Every declared set is read once per load and served at its own route whether or not a workflow lists it, so a chat can pick a set without running any workflow.

### Skill sets

A set is a folder whose subfolders are skills (`<path>/<skill>/SKILL.md`). Any folder inside the package works (`"analyst": { "path": "analyst" }` reads `analyst/<skill>/SKILL.md`, and `"path": "skills"` reads a root `skills/`). Several workflows can list one set; it is read once.

- A folder that holds a `SKILL.md` itself is a skill, not a set: declare its parent folder (or move it into a folder and declare that), or, if its subfolders are the skills, remove its `SKILL.md`; otherwise it loads nothing.
- A skill is its whole directory: `SKILL.md`, other Markdown (`references/` is the default folder for it; an existing `reference/` keeps working), `scripts/` the skill runs as commands, and `assets/` or any other folder of files it copies. Frontmatter `name` must equal the directory name.
- A skill name that two sets of one workflow both hold is in both, each served whole, and the workflow's warnings say so. An installer keeps the first-listed set's copy, so order the sets so the one that should win is first.
- Dot-named folders are skipped, and a `.skillignore` at a skill's root (gitignore syntax) keeps tests and fixtures out of it. An ignored file is still in the package, so it hides nothing; keep secrets out of the package.
- The `malloy-` and `eval-` prefixes belong to the skills Publisher bundles, and a future bundled skill may take any name under them. Give a package skill a name outside both.

### The `default` set

The set named `default` holds the package's own skills. Its skills, beside the bundled ones, are what `get_skill` and `get_context` advertise to every session on the package, with no one naming a workflow. So put there only guidance that applies to every use of the package (what a measure counts, vocabulary, house rules). Anything for one job goes in a set that job's workflow lists.

A `default` skill named like a bundled skill replaces it, for `get_skill`, `get_context` and the skills routes, **only if `default`'s `overrides` lists the name**; otherwise the bundled skill is served and the set carries a finding that names the fix. Set reads and workflow reads serve every skill of a set whatever `overrides` says. Credible's agent keeps its own copy of a bundled skill and skips a package skill of that name, so a skill that must behave the same everywhere gets a name no bundled skill has.

A package's `agents` key is **no longer read**, and neither is a root `skills/` directory unless a declared skill set is at or under it. The one-line fix moves no file: `"skillSets": { "default": { "path": "skills" } }`, if the folder holds `<skill>/SKILL.md`. Two things change with it: a skill named like a bundled one is served only if `default`'s `overrides` lists it, and every file of a skill is now served, so put what it must not serve in a `.skillignore`. A package with either left undeclared loads with a warning (`package-agents-not-read`, `package-skills-dir-not-read`) that names the fix.

## Instructions or skills

- **The job** (what to do now, the inputs, what done looks like, what to report) goes in `instructions`: one file of a few KB.
- **Standing guidance** (conventions, procedure shared by several jobs, tone, rules, what never to do) goes in a skill in a set. The instructions name it: "use the `weekly-metrics` skill". Instructions have no reference files, so a job that has outgrown 16 KiB is split by moving procedure into a skill, never into more instruction files.
- **Files a run copies or executes go in the skill's folders, never in Markdown**: a chart module, a template, a script. Put them in `assets/` (copied) or `scripts/` (run as `python3 scripts/check.py`) and point at them by skill-relative path. Publisher lists such files without contents and the caller installs them, so they cost no context and are never retyped.
- **Markdown is what the model reads, so keep it short.** One-sentence skill `description`s: every caller lists all skills' descriptions against one budget. Keep each `SKILL.md` under 500 lines, link references one level deep, and start any reference over 100 lines with a table of contents.
- **A `SKILL.md` is served as written, frontmatter included.** Do not put `allowed-tools`, `hooks` or other settings in a package skill's frontmatter expecting them to be honored: an installer writes only `name` and `description` and tells the user about any other key.

## The budgets

Each skill set is measured on its own:

- Inline Markdown (every `SKILL.md` and other Markdown outside `assets/`): 256 KiB and at most 1,000 files per set. Every `SKILL.md` is counted first; a skill whose `SKILL.md` does not fit is skipped, and other Markdown past the byte budget is listed instead of inlined, each with a warning.
- At most 64 skills per set.
- Listed files (everything else): each under 1 MiB, at most 4 MiB and 200 files per set, path segments of letters, digits, `.`, `_` and `-`, none starting with `.` or `-`. A binary file is listed with its size and never served as text; prefer SVG for images.
- 8 MiB of listed files across the whole package.
- A file that breaks a limit, a `.malloy` file, or one named `CLAUDE.md`, `CLAUDE.local.md`, `AGENTS.md`, `publisher.json` or (below the skill's root) `SKILL.md`, in any letter case, is skipped with a warning.
- A single skill file over the 35,000-character reply budget of `get_skill` cannot be returned whole; keep each under it.

Every set has a `digest` (SHA-256 over what it serves: each file's path and as-written SHA-256, each skill's applied `.skillignore`, and its sorted `overrides`), and every workflow a `definitionSha` over its name, description, instructions and its sets' names and digests. Editing another set or another workflow moves neither, so a caller can prove which bytes ran. One exception: sets are charged to the 8 MiB package cap in name order, so a package that crosses it refuses the later set whole.

## Refusal versus skip

The manifest is an allowlist, and Publisher never takes the package offline for a skill set or workflow mistake. What breaks decides how much is lost; every finding is in the package's load warnings, with a `code` (`workflow-not-served`, `skill-set-not-served`, `skill-shadowed`, `skill-file-skipped`, `skill-finding`) and a `subject` (the workflow's or set's name). A refused set's or workflow's read answers not-found with a `reason`: `SKILL_SET_NOT_SERVED` or `WORKFLOW_NOT_SERVED` (the message is the refusal; a `skillSets` or `workflows` value refused whole gives it for every name), or `SKILL_SET_NOT_DECLARED` or `WORKFLOW_NOT_DECLARED`.

- **A workflow is refused** (not served; `Workflow '<name>' is not served: <why>`): a bad name, a key Publisher does not know, a non-object entry, an empty `description`, an `instructions` file that is missing, not `.md`, over 16 KiB or under a nested `publisher.json`, or a bad `skillSets` list (not a list, over 8, a name listed twice, a name no set declares, or a set that is itself refused). Use an `x-` prefix (`"x-owner": "finance"`) for notes you want ignored. `tools`, `hooks`, `permission-mode`, `model`, `budget` and similar are not keys: a declaration never names a tool or a permission. A skill's own `SKILL.md` can still carry `allowed-tools` or `hooks` in its frontmatter, which is why an installer writes only `name` and `description` from it (see `malloy-run-workflow`).
- **A set is refused** (not served; `Skill set '<name>' is not served: <why>`): a bad name, a key Publisher does not know, a `path` that is missing, outside the package, a file, or a link loop, an empty `description`, a path another set also declares, a `publisher.json` at or above its folder (below the package root, never the root itself), or a set that would take the package past 8 MiB of listed files. Every workflow that lists it is refused too.
- **The whole declaration is refused**: `skillSets` or `workflows` that is not an object, more than 32 sets or 64 workflows, or the two together over 1 MiB.
- **One skill is skipped** (the set still serves, with a finding on it): an unreadable `SKILL.md`, a `name` that is not its directory name, an `assets/` that is a link or leaves the skill, a symlink, `..` or case-colliding names under `assets/`, a `SKILL.md` under `assets/`, a `SKILL.md` that does not fit the set's budget, or a `publisher.json` at the skill's root, in its `reference/` or `references/`, or anywhere under `assets/` (it would make the directory a second package root). A `publisher.json` elsewhere in a skill (say `scripts/`) only skips that file.
- A set that holds no skills is a warning, not a refusal.
- A set folder or instructions file that exists but cannot be read (`EACCES`, `EIO`) refuses nothing and makes every set and workflow answer 409 `SKILL_SETS_UNAVAILABLE` until the package reloads.

## Migrating a package from agents

1. **Each old agent task becomes its own workflow**, with the task file as that workflow's `instructions`. A task's name is the workflow's name (`weekly-report.md` becomes `workflows.weekly-report`). Move the file to `workflows/<name>/instructions.md`.
2. **An old agent's standing guidance (its `instructions`) moves into a skill**, in a set of its own (`skill_sets/<set>/<skill>/SKILL.md`, declared under `skillSets`), with a one-sentence `description` saying when to read it; its old `skills/<x>/` folders move into that set too. List the set in each workflow that needs it.
3. **Root `skills/<x>/` stay where they are** when `default` is declared with `"skillSets": { "default": { "path": "skills" } }`, the one-line fix. Moving them to `skill_sets/default/<x>/` is only for tidiness.
4. `schedules`, `budget`, `model` and tasks are gone: nothing runs a workflow on a schedule, and nothing here sets a model or a budget. Drop them, and tell the user a recurring job now needs a scheduler of their own that fetches the workflow.
5. Delete `agents/` and the old root `skills/` in the same change (unless `default` is declared at `skills`); leaving them behind keeps the load warnings.

**Always-on guidance becomes on-demand.** An agent's `instructions` reached every session on the package. A skill is read when its `description` matches the job, and a workflow's `instructions` only when a user names the workflow. Tell the user which guidance now needs to be always on, put it in a skill of the `default` set with a `description` that says it always applies, and put guidance for one named job in that workflow's `instructions`.

**An agent that had no tasks** has no workflow to declare. Its `instructions` file becomes `skills/analyst-guide/SKILL.md` (frontmatter `name` and a one-sentence `description`, then the old text), its own `skills/<x>/` folders move beside it, and `"skillSets": { "default": { "path": "skills" } }` declares the set.

## Writing instructions that work unattended

The reader of `instructions` has no one to ask. Write for a run nobody is watching:

- **State the goal and the done-condition.** "Done when the summary file exists and every number in it came from a query you ran" beats "write a good report".
- **Write it to run with no saved state.** Say what to do with no prior run or earlier output (start from the full period rather than "since the last check"), and checkpoint progress in a file so a run that stops can resume.
- **Start with a `# Title` heading.**
- **No shell preprocessing**: no exclamation mark directly before a backtick, and no code fence whose opening backticks are followed by an exclamation mark. Some hosts run such text as a shell command.
- **Prefer drafts and pull requests to publishing.** The run proposes; a person publishes, unless the user said otherwise.
- **Say what to do when blocked** (a source that will not compile, a missing permission, an empty result): usually "write down what you tried and stop", not "keep retrying" or "guess".

## Steps

1. Confirm the job, and that the user wants a workflow or skill set rather than an answer.
2. Pick names that meet the regex and a one-line description for each.
3. Write `workflows/<name>/instructions.md` (a `# Title`, a few KB) for a workflow.
4. Add skills only for knowledge the job needs and the package does not already carry, in a set under `skill_sets/`; put any file a run copies under that skill's `assets/`. Guidance for every use of the package goes in the `default` set.
5. Add the `skillSets.<name>` entries and the `workflows.<name>` entry to `publisher.json`.
6. Call `reload_package`, read its load warnings, then `get_skill_set` and `get_workflow` with `scopes` alone to confirm each is listed; one missing from the list was refused. Sets and workflows are read only when the package loads, so reload after every edit.

If `reload_package`, `get_skill_set` or `get_workflow` is not among your tools, do not assume the edit worked. Publish or reload the package through whatever the surface offers, then read the load warnings (over REST, `GET /api/v0/environments/{env}/packages/{pkg}` returns them, and `GET .../packages/{pkg}/skill-sets` and `.../workflows` list what is served). If the surface offers neither, tell the user you could not confirm the set or workflow is served.
