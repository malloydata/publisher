---
name: malloy
description: Index of all Malloy skills. Use when user asks "malloy help", "what malloy skills are available", "how do I use malloy", or needs guidance on which Malloy skill to use.
---

# Malloy Skills Index

## First-Time Setup

**Tools missing or server not running?**
Load `skill:malloy-publisher-setup`.

**No .malloy files in workspace?**
Say "model my data" and the agent will orchestrate the full modeling workflow automatically. Make sure the Malloy Publisher MCP tools are configured first.

## Skill Reference

Every skill in this deployment, by what it is for. Start at a driver; it routes to the rest.

**Start here**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-publisher-setup` | Diagnose missing Malloy Publisher MCP tools and get from nothing to a running server. Use when the tools are missing, when asking how to start the server, or when setting up Publisher for the first time. |
| `skill:malloy-getting-started` | First steps for discovering data and running a grounded query against a Malloy Publisher deployment. Use when you do not yet know the available environments, packages, or models, or when a user asks what data they can explore. |
| `skill:malloy-modeling` | Build semantic models with Malloy. Read this skill whenever the user asks about modeling data or specifically mentions Malloy. |
| `skill:malloy-analysis` | Workflow for answering data questions and exploring Malloy semantic models over MCP - structured discovery with get_context, query construction with execute_query, verification, and answer delivery. Use whenever the user asks a data question, wants a metric, a breakdown, a trend, or a chart over a model, or asks to analyze this data, find insights, explore for patterns, or what's interesting. |

**Modeling phases**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-discover` | Silent data discovery for Malloy modeling. Used at Step 1 of the modeling workflow. Scans tables, columns, distributions, and relationships without user interaction. The agent builds an internal picture before presenting anything. |
| `skill:malloy-scope` | Present discovery findings and propose an analytical scope before modeling. Use after inspecting a package's model and data, to classify tables and recommend an analytical focus the user can pick from. |
| `skill:malloy-define` | Propose a source plan and field definitions for a Malloy semantic model. Covers picking which sources to model and at what grain, then proposing the specific renames, dimensions, and measures per source, every proposal backed by querying the data. |
| `skill:malloy-model` | Build Malloy semantic models with base source and joined source files. Use when creating or modifying .malloy files, user asks to "create a malloy model", "add dimensions", "add measures", "create a source", or any Malloy model authoring task. |
| `skill:malloy-document` | Add documentation with #(doc) tags to Malloy models so fields and sources are described in plain language. Use when user asks to "add documentation", "add doc tags", "document the model", or wants fields and sources described for natural-language search and discovery. For declaring parameterizable filters with #(filter), see the malloy-model skill. Filters are a runtime/modeling construct (governance, latency, correctness), not a documentation tag. |
| `skill:malloy-lookml-review` | Analyze LookML files as prior art for Malloy modeling. Used during Step 1 (DISCOVER) when .lkml files are present. Coordinates reference files that extract business logic, relationships, and curation decisions. Works with or without a database connection. |
| `skill:malloy-model-as-you-go` | After answering a data question, write down what the answer assumed so the next reader can trust the number. A field with a #(doc) in the model when you can edit it, an extend in the notebook when you can only author reports, or a stated assumption plus a Malloy snippet when you can only chat. Use after every answered question that rested on a judgment call, and whenever a question is asked against tables that have no model yet. |

**Analysis and presentation**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-charts` | Chart selection guidance and renderer reference for Malloy views. Use when choosing visualization types, adding chart annotations, user asks "what chart should I use", "how should I visualize this", or when deciding between bar_chart, line_chart, scatter_chart, etc. |
| `skill:malloy-dashboards` | Build or modify a Malloy Publisher dashboard, a tagged .malloy file in a package's dashboards/ directory, with auto-rendered filter controls, a grid layout, and # drill click-through. Use when the user asks for a dashboard, a filterable operational view, or drill-through between views, and no code is wanted. |
| `skill:malloy-notebooks` | Create Malloy notebooks (.malloynb) for interactive dashboards and data stories. Use when user asks to "create a notebook", "make a dashboard notebook", "write a malloynb", "data story", or needs to build reports/visualizations. |
| `skill:malloy-analysis-report` | Combine validated Malloy queries into a notebook report or dashboard. Use when the user asks to "create a report", "build a dashboard", "combine these into a report", or wants a persistent multi-query artifact. |
| `skill:malloy-analysis-pitfalls` | Common data analysis pitfalls to watch for during query construction and result interpretation. Reference this checklist when verifying queries and results to catch errors before presenting an answer. |
| `skill:malloy-notebook-chat` | Steps to follow when the chat is bound to a notebook or saved report. The notebook's cells are the agent's primary context, answer from it, run its queries, and only reach for get_context when the user asks about something outside it. |
| `skill:malloy-phrase-detection` | How to construct search targets for the get_context tool. Covers target-type classification and non-obvious decomposition patterns. Read the tool description for field definitions and the end-to-end workflow. |

**Writing correct Malloy**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-queries` | Malloy query patterns, syntax rules, and chart annotation reference. Consult before writing or debugging any query. Covers dates, aggregates vs dimensions, join paths, filters, string matching, and common error patterns. |
| `skill:malloy-gotchas-modeling` | Common Malloy modeling mistakes and how to avoid them. Read BEFORE writing source definitions, dimensions, measures, or joins. Covers reserved words, NULL checks, date functions, type casts, field management (extend except/accept/rename vs include public/internal/private), and query-based source gotchas. |
| `skill:malloy-gotchas-queries` | Common Malloy query and view mistakes. Read BEFORE writing views, queries, or notebooks. Covers chart constraints, aggregate filters, joined field aliasing, method syntax, and time truncation vs extraction. |
| `skill:malloy-gotchas-rendering` | Common Malloy renderer annotation mistakes. Read BEFORE adding chart annotations, formatting tags, or building dashboards. Covers tag syntax, scale rules, sparkline setup, and big_value patterns. |
| `skill:malloy-debug` | Fix Malloy compile errors and understand error messages. Use when encountering errors in .malloy files, user says "fix this error", "malloy error", "compile error", "syntax error", or sees 20+ cascading errors. |
| `skill:malloy-patterns` | Index of Malloy documentation topics. Use to discover what's available in search_malloy_docs. Covers language reference (sources, queries, views, fields, aggregates, joins, filters, expressions, functions), common patterns (YoY, cohorts, percent of total), rendering, and experimental features. |
| `skill:malloy-review` | Malloy semantic-model code review. Invoke when the user asks to review, audit, or critique a `.malloy` file, a folder of Malloy models, or a GitHub PR that touches Malloy. Enforces project modeling standards and emits a navigable review file. |

**Serving and operating a package**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-publish` | Package Malloy models for serving by Malloy Publisher. Use when user asks to "publish", "package", "deploy", or wants to share models with others. |
| `skill:malloy-html-data-apps` | Build or modify an in-package HTML data app for a Malloy Publisher package (a public/ directory the package serves). Use when the user wants a hand-authored HTML dashboard or web page backed by a package's Malloy models, with no build step. |
| `skill:malloy-html-data-app-runtime` | Write the JavaScript that drives an in-package HTML data app, calling Publisher.query, building queries from filter state, and handling results and errors. Read before writing the page's data code. |
| `skill:malloy-html-data-app-embedding` | Embed an in-package HTML data app into a host page or another application, including auto-sizing and auth. Read when embedding a Publisher page via Publisher.embed. |
| `skill:malloy-materialization` | Add and debug Malloy Persistence materializations in a package - persist an expensive source so queries read a pre-built table. Read this whenever the user wants to materialize a source, add a persist annotation, speed up a slow source, or asks why a persist source isn't building. |
| `skill:malloy-materialization-tuning` | Optimize a package's Malloy Persistence materializations for cost and performance using the malloy-pub CLI and the materialization history. Recommend what to persist, what to stop persisting, and how to schedule/scope it. Use when the user asks to make a package cheaper or faster, tune persistence, decide what to materialize, or review persist/schedule choices. |

> **Adapter pattern:** Each prior art adapter (LookML, future dbt) follows the same structure: a coordinator SKILL.md plus reference files under `reference/` dispatched by phase skills.

## Workflows

Two top-level workflows orchestrate the phase and support skills above:

- **Model data from scratch:** load `skill:malloy-modeling`. It drives the full pipeline (discover, scope, define, build, review, curate) and routes to the phase skills.
- **Answer a data question or explore:** load `skill:malloy-analysis`. It drives discovery, query construction, verification, and open-ended exploration. Use `skill:malloy-charts` for visualization and `skill:malloy-notebooks` or `skill:malloy-dashboards` to persist the result.

Publishing is out of scope for open-source Publisher v1. Self-hosters move a finished model into a served package via git and the host's publish path; see `skill:malloy-publish`.

## Syntax Help

Call `malloy_searchDocs` with your question. Use `skill:malloy-patterns` to discover available topics.
