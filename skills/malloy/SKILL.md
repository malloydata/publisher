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
| `skill:malloy-notebooks` | Building Malloy notebooks (.malloynb) |
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

Publishing is out of scope for open-source Publisher v1. Self-hosters move a finished model into a served package via git and the host's publish path; see `skill:malloy-publish`.

## Syntax Help

Call `malloy_searchDocs` with your question. Use `skill:malloy-patterns` to discover available topics.
