---
name: malloy-define-workflow
description: Define a package workflow - a named job with instructions and skill sets (folders of skills), or the package's default skills - declared in a package's publisher.json. Use when the user asks to create, add, change, fix or migrate a workflow in a package, to give a package its own skills, or to turn an old package agent into workflows. Not for running a workflow that already exists (that is malloy-run-workflow), and not for a one-off question, which you just answer.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Define a package workflow

> **Tool names** are written bare here - `get_workflow`, `reload_package`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

A workflow is a named job an author ships inside a package: optional **instructions** (the brief to follow) and **skill sets** (folders of skills to use). Publisher parses and serves the definition and runs nothing; a caller adopts it with `get_workflow` (see `malloy-run-workflow`). The files are ordinary package files, so you write them like any other model change. A package does not need models to carry workflows: one that holds only `publisher.json` and its workflow files is valid.

## When to create one

Create a workflow only when the user asks for one, asks for a job that other people or runs will repeat, or wants the package to carry its own skills. Do not create one to answer a question or make a model change; do that directly. A workflow is a standing definition others rely on, so one nobody asked for is clutter.

## The layout

Workflows live under the `workflows` object of the package's `publisher.json`, keyed by name:

```json
{
  "name": "storefront",
  "workflows": {
    "default": {
      "skill_sets": ["workflows/default/skill_sets/house"]
    },
    "weekly-report": {
      "description": "Writes the Monday revenue summary for the storefront package",
      "instructions": "workflows/weekly-report/instructions.md",
      "skill_sets": [
        "workflows/weekly-report/skill_sets/analyst",
        "workflows/default/skill_sets/house"
      ]
    }
  }
}
```

```
workflows/
  default/skill_sets/house/storefront-conventions/SKILL.md
  weekly-report/
    instructions.md
    skill_sets/analyst/analyst-conventions/SKILL.md
```

| Key | Rule |
| --- | --- |
| name (the key) | `^[a-z0-9]+(-[a-z0-9]+)*$`, at most 64 characters. |
| `description` | Optional. A non-empty line saying what the workflow is for. |
| `instructions` | Optional. Package-relative path of a `.md` file. At most 16 KiB. |
| `skill_sets` | Optional. At most 8 package-relative folders, in the order that decides a name clash. |

A workflow with neither `instructions` nor `skill_sets` has nothing to serve and is refused. One with `skill_sets` and no `instructions` is only a bundle of skills; `get_workflow` reports `has_instructions: false` and there is nothing to run.

### Skill sets

A **skill set** is a folder whose subfolders are skills (`<set>/<skill>/SKILL.md`). Its name is its folder's name, which is what a chat picks it by and what `skill_set=` takes. Convention: `workflows/<workflow>/skill_sets/<set>/`. A set can be listed by several workflows (list the path again); it is read once.

- The folder name must meet the same regex as a workflow name, and two entries of one workflow may not share a name or a path.
- A skill is its whole directory: `SKILL.md`, other Markdown (`references/` is the default folder for it; an existing `reference/` keeps working), `scripts/` the skill runs as commands, and `assets/` or any other folder of files it copies. Frontmatter `name` must equal the directory name.
- A skill name that two sets of one workflow both hold is served from the **first-listed** set, and the workflow's warnings say so. Order the sets so the one that should win is first.
- Dot-named folders are skipped, and a `.skillignore` at a skill's root (gitignore syntax) keeps tests and fixtures out of it. An ignored file is still in the package, so it hides nothing; keep secrets out of the package.

### The `default` workflow

`workflows.default` holds what used to be the package's root `skills/`. Its skills, beside the bundled ones, are what `get_skill` and `get_context` advertise to every session on the package, with no one naming a workflow. So put there only guidance that applies to every use of the package (what a measure counts, vocabulary, house rules). Anything for one job goes in that job's own workflow.

A package's `agents` key is **no longer read**, and neither is a root `skills/` directory on its own: it is read only when a workflow lists it as a skill set (`"skill_sets": ["skills"]` migrates with no file moved, if the folder holds `<skill>/SKILL.md`). A package with either left unlisted loads with a warning (`PACKAGE_AGENTS_NOT_READ`, `PACKAGE_SKILLS_DIR_NOT_READ`) naming the move below.

## Instructions or skills

- **The job** (what to do now, the inputs, what done looks like, what to report) goes in `instructions`: one file of a few KB.
- **Standing guidance** (conventions, procedure shared by several jobs, tone, rules, what never to do) goes in a skill in a set. The instructions name it: "use the `weekly-metrics` skill". Instructions have no reference files, so a job that has outgrown 16 KiB is split by moving procedure into a skill, never into more instruction files.
- **Files a run copies or executes go in the skill's folders, never in Markdown**: a chart module, a template, a script. Put them in `assets/` (copied) or `scripts/` (run as `python3 scripts/check.py`) and point at them by skill-relative path. Publisher lists such files without contents and the caller installs them, so they cost no context and are never retyped.
- **Markdown is what the model reads, so keep it short.** One-sentence skill `description`s: every caller lists all skills' descriptions against one budget. Keep each `SKILL.md` under 500 lines, link references one level deep, and start any reference over 100 lines with a table of contents.

## The budgets

Each skill set is measured on its own:

- Inline Markdown (every `SKILL.md` and other Markdown outside `assets/`): 256 KiB per set. Every `SKILL.md` is counted first; a skill whose `SKILL.md` does not fit is skipped, and other Markdown past the budget is listed instead of inlined, each with a warning.
- Listed files (everything else): each under 1 MiB, at most 4 MiB and 200 files per set, path segments of letters, digits, `.`, `_` and `-`, none starting with `.` or `-`. A binary file is listed with its size and never served as text; prefer SVG for images.
- 8 MiB of listed files across the whole package.
- A file that breaks a limit, a `.malloy` file, or one named `CLAUDE.md`, `CLAUDE.local.md`, `AGENTS.md`, `publisher.json` or (below the skill's root) `SKILL.md`, in any letter case, is skipped with a warning.
- A single skill file over the 35,000-character reply budget of `get_skill` cannot be returned whole; keep each under it.

Every set has a `digest` (SHA-256 over its path and what it serves: each inline file's text, each listed file's SHA-256, each skill's applied `.skillignore`), and every workflow a `definitionSha` over its name, description, instructions and its sets' paths and digests. Editing another set or another workflow moves neither, so a caller can prove which bytes ran. One exception: sets are charged to the 8 MiB package cap in sorted path order, so a package that crosses it can couple sets.

## Refusal versus skip

The manifest is an allowlist, and Publisher never takes the package offline for a workflow mistake. What breaks decides how much is lost; every finding is in the package's load warnings.

- **The whole workflow is refused** (not served; `Workflow '<name>' is not served: <why>`): a bad name, a key Publisher does not know, a non-object entry, an empty `description`, an `instructions` file that is missing, not `.md`, over 16 KiB or under a nested `publisher.json`, a bad `skill_sets` list (not a list, over 8, a folder that is missing, outside the package, badly named or listed twice), a set folder that cannot be read, or nothing to serve. Use an `x-` prefix (`"x-owner": "finance"`) for notes you want ignored. `tools`, `hooks`, `permission-mode` and similar are not keys: a definition adds words to a session and never widens what the session may do.
- **One skill is skipped** (the workflow still serves, with a set finding): an unreadable `SKILL.md`, a `name` that is not its directory name, an `assets/` that is a link or leaves the skill, a symlink, `..` or case-colliding names under `assets/`, a `SKILL.md` under `assets/`, a `SKILL.md` that does not fit the set's budget, or a `publisher.json` at the skill's root, in its `reference/` or `references/`, or anywhere under `assets/` (it would make the directory a second package root). A `publisher.json` elsewhere in a skill (say `scripts/`) only skips that file.
- **A `publisher.json` at or above a set's folder refuses the workflow** for the same reason (below the package root, never the root itself); one inside a skill (the places above) only sets that skill aside; one in a dot-named directory is not read at all.
- A set that holds no skills is a warning, not a refusal.

## Migrating a package from agents

1. **Each old agent task becomes its own workflow**, with the task file as that workflow's `instructions`. A task's name is the workflow's name (`weekly-report.md` becomes `workflows.weekly-report`). Move the file to `workflows/<name>/instructions.md`.
2. **An old agent's standing guidance (its `instructions`) moves into a skill**, in a set of its own (`workflows/<name>/skill_sets/<set>/<skill>/SKILL.md`), with a one-sentence `description` saying when to read it; its old `skills/<x>/` folders move into that set too. List the set in each workflow that needs it.
3. **Root `skills/<x>/` move to `workflows/default/skill_sets/<set>/<x>/`**, and `default` is declared with that set. (Declaring `"default": { "skill_sets": ["skills"] }` instead keeps the folder where it is; the move is only for tidiness.)
4. `schedules`, `budget`, `model` and tasks are gone: nothing runs a workflow on a schedule, and nothing here sets a model or a budget. Drop them, and tell the user a recurring job now needs a scheduler of their own that fetches the workflow.
5. Delete `agents/` and the old root `skills/` in the same change (unless `default` lists `skills`); leaving them behind keeps the load warnings.

## Writing instructions that work unattended

The reader of `instructions` has no one to ask. Write for a run nobody is watching:

- **State the goal and the done-condition.** "Done when the summary file exists and every number in it came from a query you ran" beats "write a good report".
- **Write it to run with no saved state.** Say what to do with no prior run or earlier output (start from the full period rather than "since the last check"), and checkpoint progress in a file so a run that stops can resume.
- **Start with a `# Title` heading.**
- **No shell preprocessing**: no exclamation mark directly before a backtick, and no code fence whose opening backticks are followed by an exclamation mark. Some hosts run such text as a shell command.
- **Prefer drafts and pull requests to publishing.** The run proposes; a person publishes, unless the user said otherwise.
- **Say what to do when blocked** (a source that will not compile, a missing permission, an empty result): usually "write down what you tried and stop", not "keep retrying" or "guess".

## Steps

1. Confirm the job, and that the user wants a workflow rather than an answer.
2. Pick a name that meets the regex and a one-line description.
3. Write `workflows/<name>/instructions.md` (a `# Title`, a few KB), if the workflow has a job.
4. Add skills only for knowledge the job needs and the package does not already carry, in a set under `workflows/<name>/skill_sets/`; put any file a run copies under that skill's `assets/`. Shared guidance goes in `workflows/default/skill_sets/<set>/`.
5. Add the `workflows.<name>` entry to `publisher.json`, and `default` if it has skills.
6. Call `reload_package`, read its load warnings, then `get_workflow` with `scopes` alone to confirm the workflow is listed; one missing from the list was refused. Workflows are read only when the package loads, so reload after every edit.

If `reload_package` or `get_workflow` is not among your tools, do not assume the edit worked. Publish or reload the package through whatever the surface offers, then read the load warnings (over REST, `GET /api/v0/environments/{env}/packages/{pkg}` returns them and `GET .../packages/{pkg}/workflows` lists the served workflows). If the surface offers neither, tell the user you could not confirm the workflow is served.
