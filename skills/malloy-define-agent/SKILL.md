---
name: malloy-define-agent
description: Define a package agent - a named brief, with its own skills, optional schedules and a run budget, declared in a package's publisher.json. Use when the user asks to create, add, change or fix an agent in a package, or wants a recurring or unattended job such as a weekly report. Not for adopting an agent that already exists (that is malloy-run-agent), and not for a one-off question, which you just answer.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Define a package agent

> **Tool names** are written bare here - `get_agent`, `reload_package`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

A package agent is a named brief an author ships inside a package: instructions, optional skills of its own, optional scheduled tasks and an optional run budget. Publisher parses and serves the definition; a caller adopts it with `get_agent` (see `malloy-run-agent`). The files are ordinary package files, so you write them like any other model change.

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
| `schedules` | Optional. A list of `{ "cron", "task" }`: a 5-field cron expression in UTC, and the package-relative path of a Markdown task file of at most 16 KB. |
| `budget` | Optional. An object whose optional fields are `turns`, `wall_clock_minutes`, `llm_tokens` and `job_minutes`, each a positive integer. |

Put each agent's files under `agents/<name>/` so they stay together. Never point an agent at a path outside the package.

## What is live today

- Schedules and budgets are **parsed, validated and shown** (`get_agent`, the agent REST endpoints) and nothing more. **Scheduled execution is not live yet**: a `schedules` entry does not make anything run. A task runs when someone asks for it. Say so to the user rather than promising a recurring job.
- **Nothing enforces a budget yet either.** Once a platform does, it clamps each field to its own maximum, so a larger number is not an error but is not honored, and a draft version's budget is ignored in favor of the published version's. Set the budget the work needs, not the biggest value.

## Strict keys: a typo drops the whole agent

The manifest is an allowlist. An agent with a key Publisher does not know, an unknown key inside `schedules` or `budget`, a bad name, a missing file, a bad cron, a non-positive-integer budget value or an over-cap file is **not served at all**. The package still loads, and the reason appears in its load warnings. Use an `x-` prefix (`"x-owner": "finance"`) for notes you want ignored. `budget` is new, so a server older than the release that added it drops an agent that sets one; a load warning saying the server does not know `budget` means it is too old. `tools`, `hooks`, `permission-mode` and similar are not keys: a definition adds words to a session and never widens what the session may do.

Agents are read only when the package loads, so after editing call `reload_package` first: its response carries the load warnings, including any agent that was dropped and why. Then call `get_agent` with `scopes` alone to list what is served; an agent missing from the list was dropped.

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
3. Write `agents/<name>/instructions.md`, and a task file per schedule.
4. Add skills only for knowledge the agent needs and the package does not already carry.
5. Add the `agents.<name>` entry to `publisher.json`, with a `budget` if the work has natural limits.
6. Call `reload_package`, read its load warnings, then `get_agent` to confirm the agent is served. Tell the user plainly that schedules do not run and budgets are not enforced yet.
