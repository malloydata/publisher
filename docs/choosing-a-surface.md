<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Notebooks, dashboards, or an HTML data app?

> What this is: how to pick between Publisher's three in-package analytics surfaces, **notebooks**
> (`.malloynb`), **dashboards** (`dashboards/*.malloy`), and **HTML data apps** (`public/`), with
> the pros, cons, and decision rules for each.

All three are artifacts that live _inside a package_, ship with the model, and run on the same
engine: the same governed query endpoints, the same [givens](givens.md), the same
`@malloydata/render` renderer, the same query caps. Ask any of them the same question and the numbers
come back the same. What changes is the **reading mode** you're authoring
for, **how much control you take on**, and one thing that is easy to miss:

> **The three differ in whether an author ships code.** An HTML data app is HTML, CSS and
> JavaScript you write. A notebook is markdown and Malloy cells; a dashboard is Malloy plus
> renderer tags. That difference has security consequences, and they are worked out in
> [security-posture.md](security-posture.md), including the gaps still open there. Read it before
> deciding this axis does not matter to you.

The one-line version:

- **Notebook.** You're telling a story. Prose and queries in author order, read top to bottom.
- **Dashboard.** You're monitoring. One grid behind a shared filter row, read at a glance.
- **HTML data app.** You're shipping a product. Custom page, total control, built with AI.

## Side by side

|                      | Notebook                                                                                                       | Dashboard                                                                                                       | HTML data app                                                                                                  |
| -------------------- | -------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| **What it is**       | A `.malloynb` file: markdown prose + live query cells                                                          | A `dashboards/*.malloy` file: a tagged Malloy query that _is_ the dashboard                                     | A `public/` directory of HTML/CSS/JS, served as-is                                                             |
| **Reading mode**     | Narrative: a data story, read top to bottom in author order                                                    | Operational: one grid behind a filter row, scanned at a glance                                                  | Whatever you design                                                                                            |
| **Layout**           | Vertical document flow                                                                                         | Column grid via tags (`# colspan`, `# break`)                                                                   | Fully custom                                                                                                   |
| **Authoring**        | Zero code: Malloy + markdown                                                                                   | Zero code: Malloy + layout tags (`# artifact`, `# dashboard {columns}`)                                         | Code: HTML/CSS/JS, hand-written or agent-written, no build step. The `malloy-html-data-apps` skills guide an agent through it |
| **Portability**      | Malloy's notebook format: the same file the VS Code extension authors and runs                                 | Plain Malloy, near-identical to [Malloyyo](https://github.com/malloydata/malloyyo)'s (the grid width differs)   | Standard web page; the `Publisher.*` runtime is Publisher-specific                                             |
| **Filters**          | Auto-rendered from the givens the file declares or imports: select, slider, date picker                                    | Auto-rendered from the givens the query references: select, slider, date picker                                 | You build the controls and pass givens through `Publisher.query` yourself                                      |
| **Interactivity**    | URL-addressable filter state, Apply batching, `# drill` click-through and drill-in-place                       | URL-addressable filter state, Apply batching, `# drill` click-through and drill-in-place                        | Anything the web platform can do                                                                               |
| **Embedding**        | SDK `<Notebook>` for React hosts ([internal][sdk-internal]); iframe embedding is a [follow-up][embed-followup] | SDK `<Dashboard>` for React hosts ([internal][sdk-internal]); iframe embedding is a [follow-up][embed-followup] | `Publisher.embed`: auto-resizing iframe in any host page                                                       |
| **Maintenance cost** | Low: the model does the work                                                                                   | Low: the model does the work                                                                                    | You own layout, state, and error handling                                                                      |

[embed-followup]: https://github.com/malloydata/publisher/issues/931
[sdk-internal]: embedded-data-apps.md

Read the table this way: notebooks and dashboards differ in the rows above **Filters**, and from
**Filters** down they behave the same, because the two surfaces run the same code. The one
difference is which givens get a control. A dashboard is one query, so it can tell: it renders a
control for each given that query references. A notebook is many queries, so it renders one per
given the file imports, and importing a given no cell filters by leaves a control that moves
nothing. Import the ones you filter by.

## Notebooks: the data story

A notebook interleaves markdown prose with live query cells, in the order the author wants them
read. A cell tagged `# dashboard` can render a KPI grid inline, and a model's givens surface as a
Parameters panel above the cells. Try
`http://localhost:4000/examples/storefront/storefront.malloynb`.

Interactivity is not the axis to choose on: a notebook and a dashboard run the same givens code,
so both get URL-addressable parameters, the same controls, starting values and Apply batching (a
notebook asks for those with file-level `## givens { … }` and `## autorun=false`, a dashboard with
the same two properties on its artifact tag), and `# drill`. Nor is polish: both are listed by
title rather than filename, and the same load-time lint covers the drill tags either one fires.
Pick on the shape of the deliverable instead.

**Pros**

- Fastest surface to author: no tags to learn beyond what the queries already use, no layout
  decisions. Agents produce them well (the `malloy-analysis-report` skill targets them).
- Prose is a first-class citizen: context, caveats, and interpretation live next to the numbers.
- The natural output of an analysis session: a sequence of validated queries becomes a report.
- `.malloynb` is Malloy's notebook format, not a Publisher one: the same file opens in the Malloy
  VS Code extension, so an author can write and run it locally and Publisher serves it unchanged.
- Its opening heading titles it in the package listing, so a notebook reads as a document there
  without carrying a tag for it. `## title="…"` or a `#" ` doc comment override.

**Cons**

- Vertical flow only; there is no grid of the whole document, no shared at-a-glance layout.
- Embedding into a non-React host page is a follow-up
  ([#931](https://github.com/malloydata/publisher/issues/931)), the same one dashboards are
  waiting on. React hosts have the SDK `<Notebook>` component today, with the internal-API caveat
  in [embedded-data-apps.md](embedded-data-apps.md).

**Choose it when** the deliverable is an _analysis_: a question answered, a finding explained, a
sequence a reader should follow in order. If you'd naturally write paragraphs between the
queries, it's a notebook.

## Dashboards: the operational grid

A dashboard is a self-contained `.malloy` file in the package's `dashboards/` directory: it
imports the model, declares one query (or composes tiles), applies its filters, and tags the
layout. Filter controls render automatically from the givens the query references; `# drill`
makes dimension cells navigate between dashboards; filter state lives in the URL. The package
page lists them, and the Console renders them at `dashboards/<name>`. How to write one:
[dashboards.md](dashboards.md); the design behind it:
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md). The `storefront` example ships one
at [`dashboards/overview.malloy`](../examples/storefront/dashboards/overview.malloy), which is a
complete file to read or copy.

**Pros**

- Built for the _recurring look_: same numbers, same layout, different day, one grid behind a
  shared filter row.
- Still zero code, still plain Malloy: versioned, reviewed, and linted with the model, and
  near-identical to Malloyyo's, the grid width aside (the migration story).
- A real grid: `# colspan` and `# break` lay tiles out across columns, which vertical cell flow
  cannot do.

**Cons**

- No prose to speak of: a title and a doc comment, not a narrative.
- Wants a modeled foundation: controls come from `given:` declarations, so ad-hoc one-offs fit
  notebooks better.
- No code escape hatch: whatever renderer tags can express is the ceiling. When a page needs to
  go past it, that is an HTML data app, not a bespoke tile
  ([design note](malloyyo-dashboards-design.md#custom-jsx-components-cut)).
- Embedding into a non-React host page is still a follow-up
  ([#931](https://github.com/malloydata/publisher/issues/931)). React hosts have the SDK
  `<Dashboard>` component today, under the same internal-API caveat; `Publisher.embed` cannot
  usefully target a dashboard route yet, so an HTML data app remains the surface with the complete
  embedding story.

**Choose it when** the deliverable is a _view you return to_ (business health, a funnel, a team's
KPIs) filtered live by whoever's looking. If the reader scans rather than reads, it's a dashboard.

## HTML data apps: the product

An HTML data app is a custom page in the package's `public/` directory, served by Publisher with
no build step, calling `Publisher.query` for data. You write the HTML, CSS and JavaScript, and an
AI agent is a well-supported way to write it: the bundled skills (`malloy-html-data-apps`, plus its
runtime and embedding companions) teach an agent the page structure, the `Publisher.*` runtime,
filter wiring, and error handling. Guide:
[html-data-apps.md](html-data-apps.md). Try
`http://localhost:4000/environments/examples/packages/storefront/`.

**Pros**

- Total control: your layout, your branding, your chart library (Chart.js, D3, anything), your
  interaction patterns.
- Cheap to author despite the control: an agent following the skills produces the page, and the
  no-build loop (edit the file, reload; live-reload under watch mode) makes iteration fast.
- The strongest embedding story today: `Publisher.embed` drops it into any host page with
  auto-resizing and cross-origin support.
- Given-scoped filtering and query caps apply as usual, because the page asks the same endpoints
  every other surface asks. **`#(authorize)` is the exception, and it follows from the author-code
  property rather than from the endpoint:** because the page controls its own requests, it can send
  `x-publisher-bypass-authorize: true` and skip gate evaluation on any deployment that does not strip
  that header at its edge ([authorize.md](authorize.md#authorize-bypass-for-trusted-data-management-callers),
  [authorize-bypass-deployment.md](authorize-bypass-deployment.md)). Sending a custom header takes
  JavaScript, which neither format gives an author a file for;
  [security-posture.md](security-posture.md) covers where that boundary is and is not absolute.

**Cons**

- It's still code, even when an agent writes it. The filter widgets, loading states, error
  handling, and responsive layout that notebooks and dashboards give you for free are the app's
  to get right. The skills cover them, but they live in your page, not the platform.
- Nothing is derived from the model: add a given, and the UI has to be updated to match (another
  agent pass, but a pass someone has to remember to make).
- Highest maintenance cost of the three, and quality depends on the author, human or agent.

**Choose it when** the deliverable is a _product experience_: customer-facing, branded,
pixel-controlled, or interactive beyond what renderer tags express. If a designer has opinions
about it, it's an HTML data app.

## Decision guide

1. **Is there a narrative?** Prose between the numbers, an order the reader should follow →
   **notebook**.
2. **Is it a recurring, at-a-glance view behind shared filters?** → **dashboard**.
3. **Does it need custom design, branding, or interactions beyond renderer tags?** → **HTML data
   app**.
4. **Is the host an application of your own?** The supported paths are an HTML data app dropped in
   with `Publisher.embed`, or the [REST and MCP APIs](api-overview.md). A React host can also
   render the Console's own components directly, `<Dashboard>` included, but that is an internal
   building block whose API changes without notice: read the heads-up at the top of
   [embedded-data-apps.md](embedded-data-apps.md) before choosing it.

Two rules of thumb cut through most cases: _read top-to-bottom → notebook; scanned at a glance →
dashboard; owned by a designer → HTML app._ And when in doubt, start with a notebook: it's the
cheapest to author, and the queries it validates can be promoted into a dashboard or wired into
an HTML app later. The surfaces share the engine, so nothing is thrown away when you move up.
