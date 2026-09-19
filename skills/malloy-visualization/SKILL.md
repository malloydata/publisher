---
name: malloy-visualization
description: Show data rather than answer with a number - a notebook, a Publisher dashboard, an in-package HTML app, or a chart on a view. Decides which fits before building, then follows its procedure.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Building a visualization

> **Tool names** are written bare here - `get_context`, `execute_query`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

"Build me a dashboard" names an outcome, not an artifact. Four different things produce it, they are authored in different files with different runtimes, and picking the wrong one is expensive to undo. Decide first.

## 1. Decide which artifact

Ask what the user needs to do with it, not what they called it.

| They want | Build | Read |
|---|---|---|
| To read an analysis, re-run it, and share it | a notebook (`.malloynb`) | `reference/notebooks.md` |
| Several validated queries written up as a report | a notebook report | `reference/reports.md` |
| A filterable operational view Publisher serves, no code | a dashboard (tagged `.malloy` in `dashboards/`) | `reference/dashboards.md` |
| A hand-authored page the package serves, with its own layout and controls | an HTML data app (`public/`) | `reference/html-data-apps.md` |
| One chart on a view they already have | nothing new - annotate the view | `reference/charts.md` |

Two questions settle almost every case:

- **Is any code wanted?** No means a dashboard. A notebook if they want to read the analysis alongside it.
- **Does it have to look a particular way, or embed somewhere?** That is an HTML data app; nothing else gives layout control.

If the answer is genuinely unclear, ask, naming two options and what each gets them. Do not build the most general one as a hedge - an HTML app built where a dashboard was wanted is a week of maintenance nobody asked for.

## 2. Ground it before you build

Every artifact here renders a query, so a wrong query is a wrong visualization that looks finished. Before building:

- Confirm the entities exist with `get_context` rather than assuming names.
- Run the query and check the numbers with `skill:malloy-analysis`'s `reference/pitfalls.md` - grain, fan-out, and filters are the three that survive into a chart and mislead silently.
- Know the row count and the time range. A chart of eleven rows and a chart of eleven million are different artifacts.

`skill:malloy-analysis` owns answering the question. This skill owns showing it.

## Reference

Paths below are relative to this skill. Read one at the step that calls for it, not up front; your host states where this skill lives.

| Read this | When |
|---|---|
| `reference/notebooks.md` | building or editing a `.malloynb` |
| `reference/reports.md` | combining validated queries into a written-up report |
| `reference/dashboards.md` | building a Publisher dashboard, filter controls, `# drill` |
| `reference/html-data-apps.md` | building a page in `public/` |
| `reference/html-data-app-runtime.md` | writing the JavaScript that drives that page |
| `reference/html-data-app-embedding.md` | embedding a Publisher page in a host application |
| `reference/charts.md` | choosing a chart type or adding a render annotation |
| `reference/render-gotchas.md` | a render tag is not doing what you expect |
| `reference/lazy-load.md` | a page has to defer work until it is visible |
| `reference/verification-harness.md` | checking an HTML app actually works before handing it over |

## Next steps

Say which artifact you built and where it lives, so the user can find it without asking. If the analysis behind it rested on a judgment call, write that down too; your host's model-as-you-go workflow covers where it belongs.
