---
name: malloy-notebooks
description: "Renamed to malloy-reports. A notebook is now a report with layout=linear: a .malloy file with an ## artifact tag, written at the package root. Load malloy-reports instead when the user asks for a notebook, a data story, or a report that reads top to bottom."
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy notebooks: renamed

This skill was merged into `skill:malloy-reports`. Read that skill instead; it holds everything this one did.

What changed, in one place:

- A notebook is a **report** with `layout=linear`. A dashboard is the same report with `layout=grid`.
- A report can live anywhere in the package. Write a new one at the package root, not in `notebooks/`.
- Write `layout=linear` in the `## artifact` tag. `kind=notebook` is the deprecated spelling.
- A `# drill { to=… }` target is the report's package path without `.malloy`.

This name stays only so that hosts and installs that still ask for `malloy-notebooks` find the new skill.
