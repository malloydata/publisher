---
name: malloy-dashboards
description: "Renamed to malloy-reports. A dashboard is now a report with layout=grid: a .malloy file with an ## artifact tag, written at the package root. Load malloy-reports instead when the user asks for a dashboard, a filterable view, or drill-through between views."
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy dashboards: renamed

This skill was merged into `skill:malloy-reports`. Read that skill instead; it holds everything this one did.

What changed, in one place:

- A dashboard is a **report** with `layout=grid`. A notebook is the same report with `layout=linear`.
- A report can live anywhere in the package. Write a new one at the package root, not in `dashboards/`.
- Write `layout=grid` in the `## artifact` tag. `kind=dashboard` is the deprecated spelling.
- A `# drill { to=… }` target is the report's package path without `.malloy`, not a bare name.

This name stays only so that an install that already holds `malloy-dashboards` has it replaced with this pointer rather than keeping the old folder rules.
