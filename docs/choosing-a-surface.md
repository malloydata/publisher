<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A report or an HTML data app?

> What this is: how to pick between Publisher's two in-package analytics surfaces, a **report** (a
> `.malloy` file with an `## artifact` tag, in a grid or a linear layout) and an **HTML data app**
> (`public/`), and, for a report, which layout. Pros, cons, and decision rules for each.

Both live _inside a package_, ship with the model, and run on the same engine: the same governed query
endpoints, the same [givens](givens.md), the same `@malloydata/render` renderer, the same query caps.
Ask either the same question and the numbers come back the same. What changes is **how much control you
take on**, and one thing that is easy to miss:

> **The two differ in whether an author ships code.** An HTML data app is HTML, CSS and JavaScript you
> write. A report is Malloy plus markdown and renderer tags. That difference has security consequences,
> and they are worked out in [security-posture.md](security-posture.md), including the gaps still open
> there. Read it before deciding this axis does not matter to you.

The one-line version:

- **Report.** Zero code, derived from the model. Pick its layout by how it is read:
  - **Linear**: you are telling a story. Prose and queries in author order, read top to bottom.
  - **Grid**: you are monitoring. One grid behind a shared filter row, read at a glance.
- **HTML data app.** You are shipping a product. Custom page, total control, built with AI.

A report's layout is one property in its tag (`layout=linear` or `layout=grid`), not a choice of
document type: both layouts get the same filters, drill, builder, lint and API, and switching one to the
other is a one-property edit in the Console builder or the file. So the real decision is report or data
app; the layout can change later at no cost.

If you have built dashboards in a classic BI tool, a grid report is the surface that will feel familiar:
[the Console's builder](reports.md#editing-in-the-console) is drag-to-move, drag-to-resize, pick a tile
and its chart from a menu. The difference is what it writes. There is no proprietary layout document
behind it: it edits the report's `.malloy` file in your package, which you can read, diff, review and
commit like any other source file, and which an agent can write just as well as the builder can.

## Side by side

|                      | Report                                                                                                                                                                      | HTML data app                                                                                                                 |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **What it is**       | A `.malloy` file with an `## artifact { layout=… tiles=[…] }` tag, anywhere in the package (at the root unless you choose otherwise)                                        | A `public/` directory of HTML/CSS/JS, served as-is                                                                            |
| **Reading mode**     | Linear: a data story, read top to bottom in author order. Grid: one grid behind a filter row, scanned at a glance                                                           | Whatever you design                                                                                                           |
| **Layout**           | Linear: one column, in `tiles=[…]` order. Grid: columns via tags (`dashboard { columns }`, `# colspan`, `# break`)                                                          | Fully custom                                                                                                                  |
| **Authoring**        | Zero code: drag tiles in the Console's builder, or write Malloy, markdown and layout tags by hand                                                                           | Code: HTML/CSS/JS, hand-written or agent-written, no build step. The `malloy-html-data-apps` skills guide an agent through it |
| **Portability**      | Plain Malloy. A grid report in `dashboards/` written with `kind=` and bare drill names is near-identical to [Malloyyo](https://github.com/malloydata/malloyyo)'s dashboards | Standard web page; the `Publisher.*` runtime is Publisher-specific                                                            |
| **Filters**          | Auto-rendered from the givens the tiles reference: select, slider, date picker                                                                                              | You build the controls and pass givens through `Publisher.query` yourself                                                     |
| **Interactivity**    | URL-addressable filter state, Apply batching, `# drill` click-through and drill-in-place                                                                                    | Anything the web platform can do                                                                                              |
| **Embedding**        | SDK `<Report>` for React hosts ([internal][sdk-internal]); iframe embedding is a [follow-up][embed-followup]                                                                | `Publisher.embed`: auto-resizing iframe in any host page                                                                      |
| **Maintenance cost** | Low: the model does the work                                                                                                                                                | You own layout, state, and error handling                                                                                     |

[embed-followup]: https://github.com/malloydata/publisher/issues/931
[sdk-internal]: embedded-data-apps.md

## Reports: the zero-code surface

A report is a `.malloy` file whose `## artifact` tag lists its tiles: views on a source, run as separate
queries, and markdown text tiles between them. Filter controls render automatically from the givens the
tiles reference, `# drill` makes dimension cells navigate between reports or filter in place, and filter
state lives in the URL. A report can live anywhere in the package; put a new one at the package root. The
package page lists every report in one Reports section, and the Console serves it at its package path. How
to write one: [reports.md](reports.md). The `storefront` example package ships one of each layout.

**Pros**

- Zero code and plain Malloy: versioned, reviewed, and linted with the model.
- Derived from the model: add a given and the control appears; rename a view and the load-time lint
  names every tile that broke.
- The natural output of an analysis session: a sequence of validated queries becomes a linear report
  (the `malloy-analysis-report` skill targets it), and the same tiles become a grid by changing one
  property.
- Prose is first-class in both layouts, as text tiles.

**Cons**

- Wants a modeled foundation for its filters: controls come from `given:` declarations.
- No code escape hatch: whatever renderer tags can express is the ceiling. When a page needs to go past
  it, that is an HTML data app, not a bespoke tile
  ([design note](malloyyo-dashboards-design.md#custom-jsx-components-cut)).
- Embedding into a non-React host page is a follow-up
  ([#931](https://github.com/malloydata/publisher/issues/931)). React hosts have the SDK `<Report>`
  component today, with the internal-API caveat in [embedded-data-apps.md](embedded-data-apps.md).

### Which layout

- **Linear** when the deliverable is an _analysis_: a question answered, a finding explained, a sequence
  a reader should follow in order. If you would naturally write paragraphs between the queries, it is
  linear.
- **Grid** when the deliverable is a _view you return to_ (business health, a funnel, a team's KPIs),
  filtered live by whoever is looking. If the reader scans rather than reads, it is a grid: `# colspan`
  and `# break` lay tiles out across columns, which one column cannot do.

Interactivity is not the axis: both layouts run the same givens code, so both get URL-addressable
parameters, the same controls, starting values and Apply batching, and `# drill`. When in doubt, start
linear; switching to grid later is one edit.

## HTML data apps: the product

An HTML data app is a custom page in the package's `public/` directory, served by Publisher with no build
step, calling `Publisher.query` for data. You write the HTML, CSS and JavaScript, and an AI agent is a
well-supported way to write it: the bundled `malloy-html-data-apps` skill (with its design, runtime and
embedding references) teaches an agent the page structure, the `Publisher.*` runtime, filter wiring, and
error handling. Guide: [html-data-apps.md](html-data-apps.md). Try
`http://localhost:4000/environments/examples/packages/storefront/`.

**Pros**

- Total control: your layout, your branding, your chart library (Chart.js, D3, anything), your
  interaction patterns.
- Cheap to author despite the control: an agent following the skills produces the page, and the no-build
  loop (edit the file, reload; live-reload under watch mode) makes iteration fast.
- The strongest embedding story today: `Publisher.embed` drops it into any host page with auto-resizing
  and cross-origin support.
- Given-scoped filtering and query caps apply as usual, because the page asks the same endpoints every
  other surface asks. **`#(access_filter)` is the exception, and it follows from the author-code property
  rather than from the endpoint:** because the page controls its own requests, it can send
  `x-publisher-bypass-authorize` carrying the configured `PUBLISHER_BYPASS_AUTHORIZE_SECRET` and skip gate
  evaluation, on any deployment that has set that secret and does not strip the header at its edge
  ([authorize.md](authorize.md#authorize-bypass-for-trusted-data-management-callers),
  [authorize-bypass-deployment.md](authorize-bypass-deployment.md)). Sending a custom header takes
  JavaScript, which a report gives an author no file for; [security-posture.md](security-posture.md)
  covers where that boundary is and is not absolute.

**Cons**

- It is still code, even when an agent writes it. The filter widgets, loading states, error handling, and
  responsive layout that a report gives you for free are the app's to get right. The skills cover them,
  but they live in your page, not in Publisher.
- Nothing is derived from the model: add a given, and the UI has to be updated to match (another agent
  pass, but a pass someone has to remember to make).
- Higher maintenance cost, and quality depends on the author, human or agent.

**Choose it when** the deliverable is a _product experience_: customer-facing, branded,
pixel-controlled, or interactive beyond what renderer tags express. If a designer has opinions about it,
it is an HTML data app.

## Decision guide

1. **Does it need custom design, branding, or interactions beyond renderer tags?** → **HTML data app**.
2. **Otherwise it is a report.** Prose between the numbers, an order the reader should follow →
   **`layout=linear`**. A recurring, at-a-glance view behind shared filters → **`layout=grid`**.
3. **Is the host an application of your own?** The supported paths are an HTML data app dropped in with
   `Publisher.embed`, or the [REST and MCP APIs](api-overview.md). A React host can also render the
   Console's own components directly, `<Report>` included, but that is an internal building block whose
   API changes without notice: read the heads-up at the top of
   [embedded-data-apps.md](embedded-data-apps.md) before choosing it.

Rule of thumb: _owned by a designer → HTML app; otherwise a report, linear if it is read, grid if it is
scanned._ When in doubt, start with a linear report: it is the cheapest to author, it becomes a grid with
one edit, and the queries it validates can be wired into an HTML app later. The surfaces share the
engine, so nothing is thrown away when you move up.
