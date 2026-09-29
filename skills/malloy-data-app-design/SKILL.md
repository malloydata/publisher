---
name: malloy-data-app-design
description: Decide what a data app should be before building it - who opens it, the decision it serves, which archetype it is, the forms that answer its questions, and the depth (drill-down, shared scope, linkable state) that makes it a tool rather than a page of charts. Read at the START of any data-app request, before scaffolding a package or writing a tile, and when an existing app feels flat, generic, or like a list of every view in the model.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Designing a Data App

> An app that replaces a BI dashboard is shaped by a **job**, not by a model. The model tells you what is *available*; the job tells you what to build. Enumerating a model produces the same KPI-row-plus-chart-grid every time, which is the single most common failure of a generated data app.

> **Tool names** are written bare here - `get_context`, `execute_query`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

This skill produces a **design brief**: the audience, the decision, the archetype, the tile forms, and the depth plan. That brief is the input to building the app. It does not cover wiring, queries, or the runtime.

## Why this comes first

A data app is asked for in one line ("build a dashboard for X") and the model is right there, so the tempting move is to read the model and turn each view into a tile. That path is what makes every app look alike, and the sameness is structural, not cosmetic: a tile per view yields a KPI row, a line chart, a bar chart, and a table, in sections named after the model's subject areas, whatever the domain.

The apps that hold up against Looker or Sigma are not better-decorated versions of that. They are shaped differently because they answer a specific recurring question for a specific person. So spend the first few minutes on the brief. It is cheap, and it is the difference between a page someone screenshots once and a tool they open on Monday.

**The anti-pattern, stated plainly:** a tile per view is enumeration, not design. If your tile list can be derived mechanically from the model's view list, you have not designed anything yet.

## 1. Establish the job

Answer these four before choosing anything. Ask the user what you cannot infer - this is a **decision**, not a rule: propose what you believe from the model and the request, and confirm the parts that change the build.

| Question | Why it changes the app |
|---|---|
| **Who opens this, and how often?** | An exec opening it monthly wants a verdict. An analyst in it daily wants pivots and exports. The same numbers, two different apps. |
| **What decision does it serve?** | "Which accounts do I call this week", "do we ship or hold", "where did margin go". A named decision tells you what belongs on the first screen. |
| **What 2-3 numbers settle that decision?** | These lead. Everything else is supporting evidence or a drill target. An app with fifteen equal-weight tiles has no answer. |
| **What does the user do next, in the app?** | The answer is the depth plan (§4). "Nothing" is a legitimate answer only for a true monitor. |

If the user cannot name the decision, that is worth surfacing rather than papering over: an app without a decision behind it becomes a data dump, and no amount of design rescues it. Offer to build the most defensible general view for the audience and say plainly that is what you are doing.

### If the app already exists

Most requests are "make this better", not "build me one". The brief is the same, but an existing app carries constraints a new one does not, and you find them by looking rather than by asking:

- **Find the tests and docs that describe the current app first.** A test that asserts exact label text, a README that names a section, a screenshot in a doc: any of these turns a wording change into a broken build. Search for the app's strings before you change them.
- **Read the app against the brief you just wrote, not against your taste.** Say which tiles serve the decision, which do not, and which are missing. That list is the change; anything else is redecoration.
- **Say what you are keeping.** A redesign that silently drops a tile someone relies on is worse than one that keeps it and explains why.

### When nobody can answer

You may be running unattended, or the person who asked is not reachable. Do not stall, and do not quietly skip the brief: **write it down, state the assumption it rests on, and build.** Put the brief where the work lands (a comment at the top of the tile definitions, a note in the app, the handover message), so the first reviewer sees what you assumed and can correct it in one reply. An unanswered question is a reason to record the assumption, not to abandon the step.

## 2. Pick the archetype

Archetype drives layout, navigation, and how much depth is warranted. Pick one deliberately; do not default.

| Archetype | The job | Shape | Depth |
|---|---|---|---|
| **Monitor** | Is anything wrong right now? | One screen, scannable, status-forward. Thresholds and deltas, not raw levels. | Shallow by design. Drill only into an anomaly. |
| **Scorecard** | Are we hitting the target? | Metric per row: actual, target, variance, trend. Period selector. | Drill from a miss to its drivers. |
| **Explorer** | What is going on in this population? | Ranked lists and distributions, an entity detail view, comparison. | Deep. Drill-down and compare are the point. |
| **Workbench** | Do the recurring work of a role. | Multi-page behind nav, one page per subject. Shared period/unit controls. | Deep. Cross-page state, entity drawers, export. |
| **Briefing** | Explain what happened and why. | Narrative order, prose between the numbers, few tiles, each carrying an argument. | Shallow. Links out rather than drills in. |

Notes that matter in practice:

- **A monitor is not a small workbench.** Do not grow one into the other by adding sections. If the user needs both, they are two apps, or two pages with different designs.
- **Explorer and workbench are where BI tools are actually replaced.** They are also where the archetype gets skipped and a flat one-pager appears instead. If the job is "explore the population" or "do my job here", commit to the depth.
- **Briefing is legitimate and under-used.** When the ask is "explain the quarter", prose with four well-chosen numbers beats twenty tiles.

`reference/archetypes.md` has a layout sketch for each, what leads the page, the depth that belongs, the failure mode that turns it back into a generic dashboard, and a table for deciding between two that seem close. Read the entry for the one you picked before laying anything out - "multi-page behind nav" and "narrative order" are easy to nod at and hard to produce from the phrase alone, and an archetype that stays abstract collapses back into a card grid.

Say which archetype you picked and why, in one line, before you build. It is the sentence the user can most usefully disagree with.

## 3. Choose forms by question, not by data shape

A form is chosen by the **question it answers**. Reaching for line/bar/table by default is how a rich model comes out flat. The table below is the vocabulary to choose from; pick the row whose question matches, then check the misuse column before committing.

### First, inventory the model. The table alone will not change what you build.

A vocabulary list is a lookup, and nobody looks up what they think they already know. Measured on a real build: an agent that had this exact table in context still shipped an app with a scatter, a sparkline and a table - three forms out of eighteen. It reached for more only when told to, and then found them immediately, because **the forms were sitting in fields it had not enumerated**.

So before you choose a single tile, write the inventory down:

1. **List every dimension and measure the model exposes - and then look at the underlying table columns too.** Not the named views: views are someone else's earlier answer to a different question. And a model's `extend` block is a curated subset, so the columns it did not promote are exactly where an unused form hides. On a real build the single best tile came from a `release_date` column the model never declared as a dimension; a model-first reading would have missed it entirely. Where your host gives you a schema tool, run it; otherwise `run: <source> -> { select: *; limit: 1 }` shows you the columns.
2. **Check the model's own measures against the raw values before you trust one.** A declared measure is someone's earlier reading of the data, and it can be wrong in a way that inverts your headline. Profile the column a measure depends on - its distinct values, its null count - and confirm the measure means what its name says. Pay particular attention to any predicate of the form `!= null`, `is not null` or a boolean derived from one: a column whose "absent" case is a **sentinel string** rather than a null breaks them silently. On a real build, `cleared_count is count() { where: clearance_status != null }` counted every row whose status was the literal string `'Not cleared'`, reporting 95.6% of cases cleared where the truth was 16.6%. Nothing in the app would have looked broken. If a measure is wrong, do not use it: define what you need in the app's queries, say in the app that you did, and tell whoever owns the model.
3. **Mark the ones no line, bar or table can show well.** A part-to-whole breakdown (several measures that sum to a total), a paired value per entity (home/away, before/after, actual/target), a per-row distribution, a second entity reference on the same row (an opponent, a referrer, a parent), a date that supports a calendar or a cohort, a flag that splits a population.
4. **For each, name the question it answers** and take the form from the table below.

That is the step that changes the output. A field like `points_second_chance` next to three sibling scoring columns *is* a composition bar; `home_wins` and `away_wins` on one row *is* a dumbbell; a margin column *is* a distribution. None of these are visible if you start from the view list.

**The test:** if your app has three or fewer distinct forms and the model exposes more than about fifteen fields, you enumerated rather than designed. Go back to step 3.

These forms assume the app draws its own charts with a vendored chart library, where any form is available. An app that renders through the Malloy renderer's chart tags instead is working with a narrower vocabulary - it has no native treemap, funnel, or bullet, and a chart-tag skill will offer approximations for them. Design to whichever vocabulary the app actually uses, and do not carry the renderer's substitutions into an app that could draw the real form.

| The question | Form | Misused when |
|---|---|---|
| How did this move over time? | Line, or area for one cumulative series | Categories on the x-axis. That is a bar chart. |
| Which categories are biggest? | Ranked horizontal bars | More than ~15 bars, or a category axis with an inherent order (use a line). |
| How does one entity score across many criteria? | Radar | Axes on different scales. Normalize, or do not use it. |
| Where did the total go, step by step? | Waterfall | Steps that are not additive to the total. |
| How does actual sit against target? | Bullet, or bar with target marker | A second bar for target, which reads as a category rather than a threshold. |
| What is the distribution, not the average? | Histogram, box, or strip | Small n, where individual points are more honest. |
| How do two measures relate, and who are the outliers? | Scatter, bubble for a third | Dense overplotting with no encoding of density. |
| How does a cohort behave over its life? | Retention/cohort heatmap | Cohorts too small to read as rates. |
| Where do people drop out of a sequence? | Funnel | Steps that are not strictly sequential or not a subset of the prior. |
| How did rank change between two points? | Slope, or bump for many periods | More than ~20 entities, which becomes spaghetti. |
| How does a total break into parts, with magnitude? | Treemap | Parts that do not sum to a meaningful whole. |
| Where does volume flow between stages? | Sankey | Cycles, or more than a few stages. |
| How do two groups compare on one axis? | Diverging bars | **Disjoint category vocabularies.** See the trap below. |
| Composition as a share of a countable whole | Waffle | Continuous quantities, where it implies false discreteness. |
| How much do two or three sets overlap? | Venn / upset | More than three sets, where upset is the only readable option. |
| The same small chart across many facets | Small multiples | Facets with wildly different y-ranges and a shared axis. |
| One number, in context | Big value with sparkline and delta | A number with no comparison, which carries no information. |
| The records themselves | Table, sortable, with inline bars | Used as the default because no form was chosen. |

Two traps worth naming, because they produce charts that are *readable and wrong*:

- **Diverging bars on disjoint vocabularies.** If the positive and negative sides are drawn from different taxonomies (an NLP model whose praise labels and complaint labels do not overlap), a diverging chart keyed on a shared label renders nearly every row one-sided and implies a balance the data does not contain. Use two ranked lists side by side.
- **A shared axis across different scales.** Mixing a 0-7 rating with a 0-10 rating on one radar, or two measures with different units on one y-axis, silently lets the larger scale dominate. Normalize, use a second axis explicitly labeled, or split the chart.

Pie and donut are absent from that table on purpose. They answer "what is the composition" worse than a ranked bar or a waffle at almost every n. Use one only when parts-of-whole is the entire message and there are two or three parts.

## 4. Plan the depth

This is what separates a tool from a page. Decide which of these the app gets, from the "what does the user do next" answer in §1. A monitor may take none; an explorer or workbench should take most.

| Pattern | What it gives the user | Decide it when |
|---|---|---|
| **Drill-down** | A number leads to the rows behind it | Any tile where "why is that number that?" is the obvious next question |
| **Shared scope** | Clicking a category filters the other tiles | The app has a dimension the whole page should agree on |
| **Linkable state** | A view someone can paste into Slack | Always, unless the app has no state at all |
| **Entity detail** | One row's profile, trend, and actions | The population has entities a user cares about individually |
| **Comparison** | Two or more entities side by side, same definitions | Users pick between things, rather than just monitoring them |
| **Provenance** | The query behind a number, on demand | The number will be challenged by someone who did not build the app |

The one that shapes the model rather than the page is **drill-down**. A drill target is a parameterised source that extends the spine, so every inherited measure comes back already filtered to that entity and the panel names a *view* rather than recomputing anything. That keeps one definition of every number, so a drawer and a question asked of an agent answer the same way, and it keeps drill logic out of hand-built query strings in the page, which is where drift starts. Plan it while you are still deciding the tiles: retrofitting drill onto an app whose numbers were computed in JavaScript means rewriting both.

`reference/depth-patterns.md` carries the mechanics for all six - the state bus, URL-hash rules, the parameterised-source traps, the five parts of a correct drawer, and safe cross-filtering. Read it before building any of them: each has a failure that is quiet rather than obvious, and the two that bite hardest (a stale drill response painting one entity's numbers under another's title, and a filtered dashboard with no visible chips) look like bad data rather than bugs.

Also decide what the app does **not** do. An explorer that cannot export, or a monitor with no link to the runbook, is usually a gap rather than a choice.

## 5. Design for consistency without a shared kit

There is no shipped component library. Consistency comes from a **token contract**: define the tokens in CSS, and have every chart read them at runtime rather than hardcoding color. That is what lets one app carry a client's identity while still looking like it was built on purpose.

Three rules do the load-bearing work here:

- **Chart code reads tokens at runtime; never a hex literal in a chart config.** `getComputedStyle(document.documentElement).getPropertyValue('--c1')` and friends. The payoff is that a client re-skin or a theme toggle is a token swap and the charts follow. If you do support a theme toggle, re-push chart options after the swap rather than reloading the page, since most chart libraries bake resolved colors at construction time and will otherwise keep the old palette.
- **The same rule covers type, spacing and motion, not just color.** A font-size, spacing value, radius or duration that is not a token is a bug, exactly like a hex literal in a chart config. This is the half of the contract that rots when nobody states it: measured across three shipped data apps, each had 12 to 17 distinct ad-hoc font sizes and no type tokens at all. Sizes a half-point apart (10.5px, 11.5px, 13.5px) are the symptom - nobody designs those, you arrive at them by nudging one label at a time. Define a **closed**, ratio-based scale of eight type steps and pick the nearest one. Unlike a stray hex literal, a 12.5px is invisible on sight, so this rule only holds if you actually check it - grep the stylesheet for raw `font-size:` values before you call the app done, and expect the answer to be zero. Grep the JavaScript too: `style.fontSize`, `style.padding` and a hex literal in a chart config are the same violation wearing different clothes, and a chart-heavy app drifts there first because the stylesheet stays clean.
- **Hierarchy through size and weight, not boxes.** The 2-3 numbers from §1 should be visibly primary, and the gap between them and everything else should come from the type scale rather than from per-tile judgment. If every tile is the same card at the same size, you have built a grid, not a hierarchy, and the reader has no way to tell what the app is for.

`reference/token-contract.md` has the full list of tokens to define, a worked example, and the craft rules (domain conventions, formatting at the edge, tile captions) to follow when writing the stylesheet.

**The contract is the floor, not the ceiling.** It buys consistency, and an app can satisfy every rule in it and still look like a generated admin panel: one surface colour used for every tile, a uniform grid, library-default charts, an accent that never appears at scale. That has been measured on a real build - every token rule green, and the page still read as generic. `reference/visual-craft.md` is the difference between consistent and designed: surface elevation rather than one flat plane, an accent that carries something structural, charts with direct labelling and annotation instead of library defaults, density that earns its space, motion as feedback, and type with enough contrast to separate the answer from the interface. Read it whenever the app is meant to feel like a product someone chose rather than a dashboard someone generated - which, for anything replacing a BI tool, is always.

## What "done" means for the design

Before building, you should be able to state, in a few lines:

- who opens it, how often, and the decision it serves
- the archetype, and why that one
- the 2-3 numbers that lead, and what is supporting
- for each tile: the question it answers and the form that answers it, with any default form you rejected and why
- the depth plan: what drills into what, what shares scope, what goes in the URL
- the token contract the app will use

Hand that brief over before scaffolding. It is short, the user can correct it in one message, and correcting it then is far cheaper than rebuilding a finished app that answered the wrong question.
