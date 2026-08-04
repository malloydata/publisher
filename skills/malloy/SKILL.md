---
name: malloy
description: Index of all Malloy skills. Use when user asks "malloy help", "what malloy skills are available", "how do I use malloy", or needs guidance on which Malloy skill to use.
---

# Malloy Skills Index

## First-Time Setup

**No .malloy files in workspace?**
Say "model my data" and the agent will orchestrate the full modeling workflow automatically. Make sure the Malloy Publisher MCP tools are configured first.

## Skill Reference

| Skill | Use when... |
|-------|-------------|
| `skill:malloy-modeling` | Building a semantic model from scratch (the modeling workflow driver) |
| `skill:malloy-analysis` | Answering a data question or exploring data (the analysis workflow driver) |
| `skill:malloy-discover` | Silent data discovery: tables, schemas, distributions, prior art |
| `skill:malloy-scope` | Presenting findings and proposing an analytical focus |
| `skill:malloy-define` | Proposing the source plan and field definitions |
| `skill:malloy-model` | Writing base and joined source .malloy files, review, curate (includes normalized schema support) |
| `skill:malloy-analyze` | Exploratory data analysis: profiling, building views and dashboards |
| `skill:malloy-charts` | Chart selection and renderer reference for Malloy visualizations |
| `skill:malloy-html-data-apps` | Building a data app: a hand-authored HTML page in a package's `public/`, served with no build step |
| `skill:malloy-notebooks` | Building Malloy notebooks (.malloynb) for an exploratory or analysis narrative |
| `skill:malloy-debug` | Fixing compile errors and interpreting diagnostics |
| `skill:malloy-patterns` | Finding syntax/pattern docs: YoY, cohorts, percent-of-total, window functions |
| `skill:malloy-document` | Adding `#(doc)` tags for discoverability |
| `skill:malloy-publish` | Moving a finished model into a served package (local-to-served handoff) |
| `skill:malloy-lookml-review` | Prior-art adapter for LookML (field extraction, derived tables, visibility, docs) |

> **Adapter pattern:** Each prior art adapter (LookML, future dbt) follows the same structure: a coordinator SKILL.md plus reference files under `reference/` dispatched by phase skills.

## Workflows

Two top-level workflows orchestrate the phase and support skills above:

- **Model data from scratch:** load `skill:malloy-modeling`. It drives the full pipeline (discover, scope, define, build, review, curate) and routes to the phase skills.
- **Answer a data question or explore:** load `skill:malloy-analysis`. It drives exploratory analysis, views, and notebooks, using `skill:malloy-analyze` and `skill:malloy-charts`.

### "Build a data app" means HTML, not a notebook

These two deliverables are easy to confuse, and reaching for the wrong one costs a rewrite:

- **A data app** is a hand-authored HTML page in the package's `public/` directory, backed by that package's models and served by Publisher with no build step. This is what "build a data app", "build a dashboard for users", or "make a page" asks for. Load `skill:malloy-html-data-apps`.
- **A notebook** (`.malloynb`) is an exploratory or analytical narrative: cells of Malloy with prose, for someone reading the analysis. It is not the default form of an app. Load `skill:malloy-notebooks`.

When the ask is ambiguous, the audience decides: a page someone *uses* is a data app, a document someone *reads* is a notebook.

Publishing is out of scope for open-source Publisher v1. Self-hosters move a finished model into a served package via git and the host's publish path; see `skill:malloy-publish`.

## Syntax Help

Call `malloy_searchDocs` with your question. Use `skill:malloy-patterns` to discover available topics.
