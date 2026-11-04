---
name: malloy-define-agent
description: Define a package agent - a named brief, with its own skills, optional schedules and a run budget, declared in a package's publisher.json. Use when the user asks to create, add, change or fix an agent in a package, or wants a recurring or unattended job. Not for adopting an agent that already exists (that is malloy-run-agent), and not for a one-off question, which you just answer.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Define a package agent

> **Tool names** are written bare here - `get_agent`, `reload_package`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

A package agent is a named brief an author ships inside a package: instructions, optional skills of its own, optional scheduled tasks and an optional run budget. Publisher parses and serves the definition; a caller adopts it with `get_agent` (see `malloy-run-agent`). The files are ordinary package files, so you write them like any other model change. A package does not need models to carry agents: one that holds only `publisher.json` and its agent files is valid.

## When to create one

Create an agent only when the user asks for one, or asks for work that has to run **without them watching**: a recurring report, a monitor, a long job that should resume where it stopped. Do not create one to answer a question or make a model change; do that directly. An agent is a standing definition other people and runs will rely on, so one that nobody asked for is clutter.

## The layout

Agents live under the `agents` object of the package's `publisher.json`, keyed by name:

```json
{
  "name": "storefront",
  "agents": {
    "weekly-revenue": {
      "description": "Writes the Monday revenue summary for the storefront package",
      "model": "inherit",
      "instructions": "agents/weekly-revenue/instructions.md",
      "skills": ["agents/weekly-revenue/skills"],
      "schedules": [
        {
          "cron": "0 13 * * MON",
          "task": "agents/weekly-revenue/tasks/weekly-report.md"
        }
      ],
      "budget": { "turns": 30, "wall_clock_minutes": 60 }
    }
  }
}
```

| Key | Rule |
| --- | --- |
| name (the key) | `^[a-z0-9]+(-[a-z0-9]+)*$`, at most 64 characters. |
| `description` | Required. One line saying what the agent is for. |
| `instructions` | Required. Package-relative path of a `.md` file with no frontmatter. At most 16 KB. |
| `model` | Optional. A model name, or `inherit` (the default) for the caller's own. |
| `skills` | Optional. Package-relative directories, each holding `<skill>/SKILL.md` (and optionally `reference/`). Frontmatter `name` must match the directory name. All of the agent's skill files together are at most 256 KB; over that, the agent is dropped, not truncated. |
| `schedules` | Optional. A list of `{ "cron", "task" }`: a 5-field cron expression in UTC, and the package-relative path of a `.md` task file of at most 16 KB. |
| `budget` | Optional. An object whose optional fields are `turns`, `wall_clock_minutes`, `llm_tokens` and `job_minutes`, each a positive integer. Set only the ones the work needs; an unset field keeps the platform's default. |

Put each agent's files under `agents/<name>/` so they stay together. Never point an agent at a path outside the package.

Two layout rules drop the agent:

- **No nested `publisher.json`.** An `instructions` or task file may not sit under a directory (below the package root) that holds its own `publisher.json`, and neither may an agent's skills directory, its skill directories or their `reference/`. A nested manifest would make that directory a second package root.
- **No duplicate skill paths.** If two entries in `skills` hold the same `<skill>/SKILL.md` (or reference file), they collide. Give each skill one directory in one place.

## What is live today

- Schedules and budgets are **parsed, validated and shown** (`get_agent`, the agent REST endpoints) and nothing more. **Scheduled execution is not live yet**: a `schedules` entry does not make anything run. A task runs only when someone asks for it. Say so to the user rather than promising a recurring job. Keep the `cron` field real anyway: Publisher requires it and a future scheduler will read it.
- **Nothing enforces a budget yet either.** Once a platform does, it clamps each field to its own maximum, so a larger number is not an error but is not honored. Set the budget the work needs, not the biggest value.

## Strict keys: a typo drops the whole agent

The manifest is an allowlist. An agent with a key Publisher does not know, an unknown key inside `schedules` or `budget`, a bad name, a missing file, a bad cron, a non-positive-integer budget value or an over-cap file is **not served at all**. The package still loads, and the reason appears in its load warnings. Use an `x-` prefix (`"x-owner": "finance"`) for notes you want ignored. `budget` is new, so a server older than the release that added it drops an agent that sets one; a load warning saying the server does not know `budget` means it is too old. `tools`, `hooks`, `permission-mode` and similar are not keys: a definition adds words to a session and never widens what the session may do.

Agents are read only when the package loads, so after editing call `reload_package` first: its response carries the load warnings, including any agent that was dropped and why. Then call `get_agent` with `scopes` alone to list what is served; an agent missing from the list was dropped.

If `reload_package` or `get_agent` is not among your tools, do not assume the edit worked. Publish or reload the package through whatever the surface offers, then read the package's load warnings (over REST, `GET /api/v0/environments/{env}/packages/{pkg}` returns them and `GET .../packages/{pkg}/agents` lists the served agents). If the surface offers neither, tell the user you could not confirm the agent is served.

## Task files: one brief, one run

A task file is the brief for one run: the goal, the inputs, what done looks like and what to report, in a few KB. The instructions file and each task file are capped at 16 KB, and **a single file over the cap drops the whole agent**: it is not served and every use of it disappears, leaving only a package load warning. Call `reload_package` and read its warnings after every edit to an instructions or task file.

Where things go:

- **Standing behavior** (tone, rules, what to never do) goes in `instructions`.
- **Procedure shared by several tasks, or by ad-hoc asks,** goes in one of the agent's own skills (`SKILL.md`, with `reference/*.md` files for the long parts; the agent's skill files total at most 256 KB). The task names it: "use the `weekly-metrics` skill". A task file has no reference files of its own, so a task that has grown too big is split by moving procedure into a skill, never into more task files.
- **Write each task to run with no saved state.** Say what to do when there is no prior run, no earlier output and no saved state (for example, start from the full period rather than "since the last check").
- **Start every task file with a `# Title` heading;** clients index and route by it.
- **Name the file so it reads as a short label** (`weekly-report.md`, under about 24 characters): a client may build a label for the task from its file name.
- **No shell preprocessing** in task text or in the task path: no exclamation mark directly before a backtick, and no code fence whose opening backticks are followed by an exclamation mark. Some hosts run such text as a shell command, so clients skip a task that has it.

## Writing instructions that work unattended

The reader of `instructions` and task files has no one to ask. Write for a run nobody is watching:

- **State the goal and the done-condition.** "Done when the summary file exists and every number in it came from a query you ran" beats "write a good report".
- **Checkpoint progress** in a file in the package or a draft, so a run that stops can resume rather than restart.
- **Keep a todo list** and update it as steps finish.
- **Prefer drafts and pull requests to publishing.** The agent proposes; a person publishes, unless the user explicitly said otherwise.
- **Say what to do when blocked**: a source that will not compile, a missing permission, an empty result. Usually that is "write down what you tried and stop", not "keep retrying" or "guess".
- Keep it to what the agent needs. Conventions that apply to every use of the package belong in a package skill, not repeated in each agent.

## Steps

1. Confirm the job, and that the user wants an agent rather than an answer.
2. Pick a name that meets the regex, and a one-line description.
3. Write `agents/<name>/instructions.md`, and a task file per schedule (a `# Title` heading, a short file name, a few KB).
4. Add skills only for knowledge the agent needs and the package does not already carry.
5. Add the `agents.<name>` entry to `publisher.json`, with a `budget` if the work has natural limits, setting only the fields that matter (`{ "wall_clock_minutes": 60 }` alone is fine).
6. Call `reload_package`, read its load warnings, then `get_agent` to confirm the agent is served. Tell the user plainly that schedules do not run and budgets are not enforced yet.
