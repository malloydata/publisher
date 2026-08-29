---
name: malloy
description: Index of all Malloy skills. Use when user asks "malloy help", "what malloy skills are available", "how do I use malloy", or needs guidance on which Malloy skill to use.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy Skills Index

## First-Time Setup

**No .malloy files in workspace?**
Say "model my data" and the agent will orchestrate the full modeling workflow automatically. Make sure the Malloy Publisher MCP tools are configured first.

## Skill Reference

Every skill in this deployment, by what it is for. Start at a driver; it routes to the rest.

**Start here**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-getting-started` | First contact with a Publisher: confirming the tools, finding what data exists, running a first grounded query |
| `skill:malloy-modeling` | Building a semantic model from scratch (the modeling workflow driver) |
| `skill:malloy-analysis` | Answering a data question or exploring data (the analysis workflow driver) |

**Modeling phases** (driven by `skill:malloy-modeling`)

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-discover` | Silent data discovery: tables, schemas, distributions, prior art |
| `skill:malloy-scope` | Presenting findings and proposing an analytical focus |
| `skill:malloy-define` | Proposing the source plan and field definitions |
| `skill:malloy-model` | Writing base and joined source .malloy files, review, curate (includes normalized schema support) |
| `skill:malloy-document` | Adding `#(doc)` tags for discoverability |
| `skill:malloy-lookml-review` | Prior-art adapter for LookML (field extraction, derived tables, visibility, docs) |

**Analysis and presentation**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-model-as-you-go` | After answering a question, writing down what it assumed: a `#(doc)`'d field in the model, an `extend` in the notebook, or a stated assumption plus snippet, depending on what the session can write |
| `skill:malloy-analyze` | Open-ended exploration with no intent to keep anything: profiling, hypotheses, views |
| `skill:malloy-charts` | Chart selection and renderer reference for Malloy visualizations |
| `skill:malloy-notebooks` | Building Malloy notebooks (.malloynb) |
| `skill:malloy-analysis-report` | Combining validated queries into a notebook report or dashboard |
| `skill:malloy-analysis-pitfalls` | Checking a query and its results before presenting an answer |
| `skill:malloy-notebook-chat` | The chat is bound to a notebook or saved report; answer from its cells |
| `skill:malloy-phrase-detection` | Turning a plain-English question into search targets for the context tool |

**Writing correct Malloy** (read before writing, not after failing)

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-queries` | Query and view syntax: dates, aggregates, join paths, filters |
| `skill:malloy-gotchas-modeling` | Before writing sources, dimensions, measures, joins |
| `skill:malloy-gotchas-queries` | Before writing views, queries, notebooks |
| `skill:malloy-gotchas-rendering` | Before adding chart annotations or formatting tags |
| `skill:malloy-debug` | Fixing compile errors and interpreting diagnostics |
| `skill:malloy-patterns` | Finding syntax/pattern docs: YoY, cohorts, percent-of-total, window functions |
| `skill:malloy-review` | Reviewing, auditing, or critiquing existing Malloy |

**Serving and operating a package**

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-publish` | Moving a finished model into a served package (local-to-served handoff) |
| `skill:malloy-dashboards` | Building a dashboard: a tagged `.malloy` file in a package's `dashboards/` directory, with filter controls and drill-through |
| `skill:malloy-html-data-apps` | Building an in-package HTML data app (a `public/` directory the package serves) |
| `skill:malloy-html-data-app-runtime` | Writing the JavaScript that drives that app |
| `skill:malloy-html-data-app-embedding` | Embedding a served page into a host application |
| `skill:malloy-materialization` | Persisting an expensive source so queries read a pre-built table |
| `skill:malloy-materialization-tuning` | Tuning what to persist, and on what schedule, for cost and speed |

> **Adapter pattern:** Each prior art adapter (LookML, future dbt) follows the same structure: a coordinator SKILL.md plus reference files under `reference/` dispatched by phase skills.

## Workflows

Two top-level workflows orchestrate the phase and support skills above:

- **Model data from scratch:** load `skill:malloy-modeling`. It drives the full pipeline (discover, scope, define, build, review, curate) and routes to the phase skills.
- **Answer a data question or explore:** load `skill:malloy-analysis`. It drives exploratory analysis, views, and notebooks, using `skill:malloy-analyze` and `skill:malloy-charts`.

Publishing is out of scope for open-source Publisher v1. Self-hosters move a finished model into a served package via git and the host's publish path; see `skill:malloy-publish`.

## Syntax Help

Call `malloy_searchDocs` with your question. Use `skill:malloy-patterns` to discover available topics.
