<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Dashboards

> **What this is:** how to write a `dashboards/*.malloy` file (a filterable, clickable, grid-laid-out
> dashboard declared entirely in Malloy tags, with no code and no build step).

A dashboard is a self-contained `.malloy` file in a package's `dashboards/` directory. The file _is_
the dashboard: it imports the model parts it needs, names the views to show, and tags the layout.
Publisher discovers it at package load, lists it on the package page, and serves it at
`/<env>/<package>/dashboards/<name>`.

**One form:** `## artifact { tiles=[…] }` at model level, one tile per named view. The controls at the
top are not written anywhere in the page; they are rendered from the `given:` declarations the tiles
filter by. Cells are clickable where the model's dimension carries a `# drill` tag.
[`examples/storefront/dashboards/overview.malloy`](../examples/storefront/dashboards/overview.malloy)
is the shipped one.

Publisher also serves `# artifact` on a `query:`, and it is worth knowing what that is: **a rendered
Malloy query** — one result that `@malloydata/render` lays out from the query's own `# dashboard`
tag, the same thing a notebook cell or the VS Code extension shows. That is Malloy's rendering
feature, and this page covers it under
[a dashboard from one query](#a-dashboard-from-one-query) because the layout tags are shared. It is
not a second way to build a dashboard, and one thing it can never do is span sources: a nest's
pipeline starts from its own query's source, and there is no way to combine two, so a page over
unrelated sources has to be tiles.

The format is the one [Malloyyo](https://github.com/malloydata/malloyyo) uses, so a model repo with
a `dashboards/` directory largely works unchanged in either. One property differs: Publisher spells
the grid width `# dashboard { columns=N }` rather than `dashboard_columns=N`, and reports the old
name as a property it does not read rather than laying out at the default in silence. See
[Where Publisher diverges](malloyyo-dashboards-design.md#where-publisher-diverges).

## Where the pieces live

```
storefront/
  publisher.json           # package manifest
  storefront.malloy        # sources, measures, reusable views, # drill tags
  givens.malloy            # given: declarations, the filter controls
  dashboards/
    overview.malloy        # a dashboard: imports the model, names its tiles
    category.malloy
    regions.malloy
    _shared.malloy         # no artifact tag ⇒ a shared include, not a dashboard
```

Two conventions carry most of the weight:

- **The filename is the dashboard's name.** `overview.malloy` is the slug in its URL, the name in
  the package listing, and what a `# drill { to=overview }` elsewhere in the model points at. What
  the views inside it are called is up to you.
- **A file in `dashboards/` with no `# artifact` tag is a shared include.** Discovery skips it. It
  is where to put anything more than one dashboard needs.

Each dashboard file compiles **as its own entry**, which is why it imports what it uses rather than
inheriting it: model-level annotations do not cross an import, so the `# artifact` tag is only
readable when the file is the thing being compiled.

<a id="a-dashboard-from-one-query"></a>

## A dashboard from one query

Before the tiles, the layout tags, because a dashboard's tiles inherit them from the views they name
and this is where they are easiest to read. Everything below is `@malloydata/render`'s grammar,
rendering ONE Malloy result — the form a notebook cell shows. Publisher serves it, and the section
after this one is the form to author a dashboard in.

```malloy
##! experimental.givens
import { order_items, products } from '../storefront.malloy'
import { CATEGORY, MIN_SALE } from '../givens.malloy'

#" Revenue and margin at a glance, and where they come from.
# artifact { title="Business Overview" } dashboard { columns=12 }
query: overview is order_items -> {
  where: category ~ $CATEGORY and sale_price ~ $MIN_SALE

  aggregate:
    # label="Revenue"
    # currency
    # colspan=3
    total_sales
    # label="Gross margin"
    # currency
    # colspan=3
    total_margin
    # label="Orders"
    # colspan=3
    order_count
    # label="Avg order value"
    # currency
    # colspan=3
    avg_order_value

  nest:
    # break
    # colspan=6
    # label="Revenue by month"
    sales_by_month
    # colspan=6
    # label="Revenue by state"
    sales_by_state
  nest:
    # colspan=12
    # label="Category performance"
    by_category
}
```

- `# artifact { … }` is what makes the file a dashboard. `title=` names it; without one the title
  falls back to the `#"` doc comment above, then to the slug.
- `# dashboard { columns=N }` is the renderer's grid: a standard `@malloydata/render` tag, not a
  Publisher one.
- `where:` naming a given is what puts a control on the page. Two names here, so two controls.
- In a `# dashboard` view, fields render **by role**: a top-level `aggregate:` measure becomes a KPI
  card, and each `nest:` becomes a tile.

### Laying out the grid

Cards and tiles share one grid, so a page that lines up is a matter of four tags, and the same four
work on both forms: on a `# dashboard` query the renderer reads them off each nest, and on a
dashboard's `tiles=[…]` Publisher reads them off the view each tile names. (`# subtitle` and
`# borderless` are read the same way on both, and are the rest of that set.) Using one recipe across
a package's dashboards is what makes them read as one product rather than as several pages:

- **`columns=12`.** Twelve divides by 2, 3, 4 and 6, so a row comes out even whether it holds three
  cards or four. Pick one number and use it on every dashboard in the package.
- **A colspan on every card and every tile, summing to `columns` per row.** Four cards at 3, three at
  4, two tiles at 6, a full-width table at 12. Leave them off and each item takes one column. A
  colspan wider than `columns` is clamped, and said so in the package warnings.
- **Do not forget `columns=` itself.** What you get without it differs by form, and neither is the
  page you drew. On a dashboard the page falls back to a narrow default and every colspan is clamped
  to it, which is the usual reason a laid-out grid comes out one item per row, and the package
  warnings name it. On a `# dashboard` query there is no grid at all: the cards flow side by side at
  their natural widths and every colspan is ignored, which the renderer reports in that query's
  `renderLogs` and the package warnings never mention.
- **`# break` on the first tile after the cards.** Without it the first tile flows into whatever
  columns are left beside the cards and the next one wraps. A break is for interrupting a row that
  would otherwise be shared, not for every new row: once a row's colspans sum to `columns`, the next
  item wraps on its own with the same gap.
- **`# label="…"` on every nest or tile view.** The tile's heading is otherwise derived from the
  view's name: `sales_by_month` over a chart, which reads like a database column. Labelling at the
  point of use — the nest, or a thin re-declaration of the view — lets the same modelled view carry
  different words on different pages.

**A KPI row is authored differently on the two forms, and this is the one place they genuinely
diverge.** On a `# dashboard` query a top-level `aggregate:` measure _is_ the card, so do not nest a
`# big_value` view to get one: nested there it renders embedded, and each measure becomes a
full-width bar inside a single tile instead of a row of cards. A dashboard has no top-level
aggregates — a tile is one whole result — so there a `# big_value` view IS the KPI row, and it
renders as one. `dashboards/overview.malloy` is that tile, at `# colspan=12`.

Either way, a card's label is one line that ellipses rather than wrapping, so a narrow card truncates
it silently: "Orders / customer" reads as "ORDERS / CUSTOMEI" at 1 column of 6.

Tiles size themselves to the width their colspan gives them, so leave `# size=fill` off. Inside a
dashboard, fill measures against the container the whole grid was handed rather than against the
tile, which produces a chart thousands of pixels tall from a tag that reads like "fit the tile."

### Labels the renderer takes from somewhere else

Two places read a field's **name** rather than its `# label`, so a labelled model still shows
`total_sales` on the page:

- **A `# shape_map`'s color legend** is titled with the measure's field name. Rename the measure in
  the view (`aggregate: revenue is total_sales`) rather than only labelling it.
- **A chart's series legend** reserves its width from the longer of the series label and its widest
  value, then truncates both to fit. A 4-character label over 4-digit years clips to `20…`; naming
  the series `# label="Order year"` instead of `"Year"` buys the column enough room. A legend showing
  `…` is this, not a data problem.

Both are upstream renderer behavior, not Publisher's, and both are cheap to work around in the
model.

All of this applies equally to a `# dashboard` **view** run in a notebook cell: the two surfaces
render through the same code, so a view laid out with these tags looks the same in a cell as on a
dashboard page. The storefront model's `business_overview` view is the shipped `# dashboard` view,
and the notebook's Business overview cell runs that view. Read it as a working example of the two
surfaces agreeing, not of the layout recipe above, which it predates: it carries no `columns=`, no
colspans, and it nests a `# big_value` for its KPIs, which is the one thing this page says not to do.
`dashboards/overview.malloy` next to it is the same figures as a dashboard, laid out with the recipe.

Height is the one thing the surface decides rather than the tags: a one-query page renders at its
natural height when the result reports one, which a `# dashboard` grid does, a dashboard caps each
tile so one long table cannot set its row's height, and a notebook caps a chart cell and lets a table
cell hug its rows so one long result cannot push the prose off the page.

One exception, and it argues for always tagging the grid. A one-query page whose top-level
result is a bare chart has no height of its own to report: a chart with no grid around it sizes to
whatever container it is handed, so it stretches to the first-paint height. Measured on a two-row bar
chart: 1992px bare, against 227px for the same query under a `# dashboard` tag.

### Tag reference

| Construct                                                | What it does                                                                                                                   |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `## artifact { title= tiles=[…] givens{…} autorun= }`     | Declares the dashboard, model-level. `title` falls back to the `#"` doc comment; `givens` sets starting control values; see [Apply](#apply) |
| `# artifact { title= givens{…} autorun= }` on a `query:`  | Serves ONE query's result as the page. Malloy's rendering feature, not a second dashboard form; see [above](#a-dashboard-from-one-query)    |
| `# dashboard { columns=N }`                              | Grid width, beside the artifact tag on either form. One spelling                                                                            |
| `# colspan=K`, `# break`, `# label="…"`, `# subtitle="…"`, `# borderless` on a view | Per-tile presentation, read the same whichever way the view is consumed. See [Laying out the grid](#laying-out-the-grid)                    |
| `# label="…"` on an aggregate                             | What the KPI card is headed. Without it a card reads `total_sales`, which is a column name, not a number a reader came for                  |
| `# drill { to=[…] given=… }` on a source `dimension:`     | Makes cells that group by it clickable, see [Drill](#drill)                                                                                 |
| A `dashboards/*.malloy` with **no** artifact tag          | A shared include, skipped by discovery                                                                                                      |

Anything else inside the artifact tag is a package warning naming it, because the reader looks
properties up by name and would otherwise serve the page as though the line were not written.
`dashboard_columns=N`, which earlier versions of this grammar accepted, is that warning's main
customer: write `# dashboard { columns=N }`.

Two spellings that bite:

- **A model-level `##` tag has to be on one line.** Wrapping a long `## artifact { … }` across lines
  is a compile error, and it fails the whole package rather than the one file.
- **`# artifact` is read off a `query:`, not off a `view:`.** A source-level view carrying the tag is
  not discovered, and nothing says so: the file is treated as a shared include and quietly produces
  no dashboard. Name the view in `tiles=[…]` instead, which is what that list is for.

## Filter controls

Controls are not declared on the dashboard. They come from the `given:` declarations the query
references, and the tags on each declaration are its control contract:

```malloy
##! experimental.givens

# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''

# label="Brand" control=multiselect suggest { query=brand_suggest dimension=brand }
given: BRAND :: filter<string> is f''

# label="Minimum line total" range_min=0 range_max=250
given: MIN_SALE :: filter<number> is f''

# label="Ordered since"
given: SINCE :: date is @2023-01-01
```

| Tag                                             | Renders as                                         |
| ----------------------------------------------- | -------------------------------------------------- |
| `control=select` + `suggest { … }`              | A dropdown whose options are queried from the data |
| `control=multiselect` + `suggest { … }`         | The same, taking several values                    |
| `range_min=` / `range_max=` on `filter<number>` | A slider instead of a text box                     |
| none, on a `date` or `timestamp`                | A date picker                                      |
| none, on a `filter<string>`                     | A text box taking Malloy filter syntax             |

A `suggest` reads either a `source=` and `dimension=` pair, or a named `query=` when the option list
needs its own ordering or filtering. **The source or query has to resolve in the dashboard file**,
so import it there: a dashboard that surfaces a given whose suggest names something it cannot see
is a package warning at load, not a surprise when someone opens the dropdown.

In a package that [curates its surface](discovery-and-access.md) (`explores` plus
`queryableSources: "declared"`), resolving is not enough: an option list is an ordinary query, so
the source or query behind it must also be _queryable_ from the dashboard file, which under curation
means exported from it. Re-export what the controls read, and note that an explicit `export { … }`
replaces the default "everything top-level", so the dashboard's own query belongs on the list too:

```malloy
export { governed_overview, region_suggest, status_suggest }
```

Leave a suggest off and only that dropdown comes up empty; leave the dashboard's own query off and
the grid stops loading. A package with no `explores` has curation off, so importing what the suggest
names is enough for a `source=`.

It is not enough for a `query=`, and this is the one failure in this family that nothing reports. An
import is not transitive, so a suggest query resolves by _name_ while the source it reads does not:
the file compiles, the package loads with no warning, the manifest lists the control, and the picker
answers `400 Undefined source '…'` the moment a reader opens it. Import that source too.

Which controls appear is decided per dashboard, by which givens its query references. Declaring ten
and referencing two shows two. That is what lets one `CATEGORY` declaration scope revenue on one
dashboard and margin on another without either redeclaring it.

**Importing a given is what makes it bindable.** Malloy's given namespace is per-file, so a
dashboard can only be _run_ with the givens its own file imports, even when the `where:` that
references one lives up an import chain. A given the file does not import gets no control, and
sending it at run time fails with "unknown given". Everything about givens themselves
(declaration, types, defaults, access control) is in [givens.md](givens.md).

<a id="apply"></a>

### Apply, starting values, and the URL

```malloy
#" Sales by region
# artifact { autorun=false givens { REGION=f'West' } }
# dashboard { columns=12 }
query: regional_sales is order_items -> { … }
```

- **`autorun=false`** puts an Apply button on the control row and batches changes behind it, so
  moving a date and picking a region re-runs the page once instead of twice. Worth it as soon as a
  page is slow enough that a reader notices.
- **`givens { … }`** sets this dashboard's _starting_ values. It is an opening position, not a
  redeclaration.
- **Control state lives in the URL**, so a filtered dashboard is a shareable link. A URL parameter
  beats the dashboard's own starting values.

All three behave identically in a notebook, which spells them at the file level (`## autorun=false`
and `## givens { REGION=f'West' }`) and gets the same controls, the same URL state, and the same
Apply button from the same code.

## A dashboard: `tiles=[…]`

The form to author. The model-level tag names the views to show, and Publisher runs each one and lays
the results out:

```malloy
##! experimental.givens
## artifact { title="Seasonality" tiles=["scoped_sales -> sales_by_month", "scoped_sales -> seasonality"] } dashboard { columns=12 }
import { scoped_sales } from './_shared.malloy'
import { products } from '../storefront.malloy'
import { CATEGORY, SINCE } from '../givens.malloy'
```

Model-level because there is no query of its own to hang a `#` tag on, and model-level for a second
reason: tiles run as **separate queries**, which is the only way a page can span unrelated sources.

Presentation is per-tile and lives on the **view**: `# colspan`, `# break`, `# label`, `# subtitle`
and `# borderless`, which is exactly the set `@malloydata/render` resolves for a `# dashboard` nest
child. One view therefore presents identically whether it is named as a tile here or nested under a
`# dashboard` query, and there is no second grammar to learn:

```malloy
source: overview is scoped_sales extend {
  # colspan=8
  # break
  # label="Revenue by month"
  view: revenue_trend is sales_by_month

  # colspan=4
  # label="Revenue by state"
  view: revenue_by_state is sales_by_state
}
```

Tagging a thin re-declaration like that, rather than the shared view itself, is what lets one modelled
view sit at different widths on different pages. `# colspan` is clamped to `columns` and a colspan
wider than the grid is a package warning, exactly as the renderer treats the same tag.

What tiles buy, and it applies to every dashboard: a broken tile shows its error in place instead of
blanking the page, a control change re-runs only the tiles that reference it, and each tile gets its
own row and byte budget. The control row is the union across the tiles, with one exception worth
knowing: if a tile cannot be resolved, the row widens to every given the entry file surfaces rather
than narrowing to the tiles that did resolve. The unresolvable tile is a package warning of its own,
so the state is visible, but the control row is usually where it is noticed first.

Three things it costs, none of them fixable by tagging differently:

- **A tile expression is a string in an annotation, so the compiler never checks it.** Rename a view
  and the dashboard still compiles; the tile fails at package load, where the lint names it.
- **No per-parent-row grouping.** A `# dashboard` nest can repeat its whole grid once per row of a
  parent query; tiles have no parent query, so there is nothing to repeat over.
- **Filtering lives in what the tiles name, not on the page.** There is no page-level `where:`.

A dashboard has no query of its own, so the filtering it applies has to live in what it composes.
That is the job the shared include does here. `_shared.malloy` scopes the source once:

```malloy
// dashboards/_shared.malloy: no artifact tag, so an include rather than a dashboard.
##! experimental.givens
import { order_items } from '../storefront.malloy'
import { CATEGORY, SINCE } from '../givens.malloy'

source: scoped_sales is order_items extend {
  where: category ~ $CATEGORY and created_at >= $SINCE
}
```

Note the imports in the dashboard file itself. Nothing in that file mentions `CATEGORY` or `SINCE` (the
`where:` that does is one file over), but the given namespace is per-file, so without importing them
the control row would be empty and the tiles would silently run at their defaults.

<a id="drill"></a>

## Drill: making cells clickable

`# drill` is declared on a model **dimension**, not on a dashboard:

```malloy
source: order_items is duckdb.table('data/order_items.parquet') extend {
  # drill { to=["category", "self"] given=CATEGORY }
  dimension: category is products.category

  # drill { to=self given=BRAND }
  dimension: brand is products.brand

  # drill { to=regions given=REGION }
  dimension: region is regions.region
}
```

- `to=<slug>` navigates to that dashboard with the clicked value written into the named given. The
  value is written as **filter syntax**, because a click cannot know what it is aiming at, so the
  destination's given should be a `filter<…>` one. Seeding a plain `string` given across dashboards
  delivers the escaped spelling (`Ben\ &\ Jerry` for a cell reading `Ben & Jerry`), which matches
  nothing. `to=self` is exempt: there the surface knows the declared type and re-encodes for it.
- `to=self` filters wherever the click came from, without leaving the page. Under `autorun=false` it
  stages the value as a draft instead: neither the page nor the URL changes until Apply is pressed.
  Worth knowing because the same menu's `to=<slug>` entry navigates and arrives already applied, so
  one menu offers two entries with different timing. Measured on `regions`: with two regions applied,
  choosing "Filter this dashboard" on one of them left the URL alone and enabled Apply.
- More than one destination pops a menu, because a choice is not a guess.
- `given=` names the given to seed. **Write it whenever the dimension is not named after the
  given.** Without it the given is the dimension name exactly as the model spells it, so
  `dimension: brand_name` looks for a given called `brand_name` and a model declaring `BRAND` does
  not match. The failure is quiet on the dashboard side: the cell still reads as clickable and the
  click lands on an unfiltered page. A difference of CASE alone is forgiven **only for `to=self`**,
  where the surface resolves the name against the givens it declares and folds case doing it; a
  notebook and a dashboard fold it the same way. A `to=<slug>` drill has no such lookup: the name
  goes into the destination's URL exactly as the tag spells it, and the destination binds only the
  parameters it declares, spelled identically. So `given=brand` into a dashboard declaring `BRAND`
  opens it unfiltered. The load-time lint reports the case it can see: a `to=self` drill seeding a
  given no model in the package declares is an error at load.

**What a reader sees.** Cells in a drillable column take a pointer cursor, and turn blue and
underlined under the pointer: plain text at rest, a link when you reach for them. They carry a button
role and are reachable from the keyboard, with focus styled the way hover is, and Enter or Space
fires the drill. That matters because a pointer cursor and a hover colour are the two things a
keyboard or touch user can never produce.

**Tab reaches the column; the arrows move within it.** A drillable column puts ONE cell in the tab
order, not one per row, so Tab does not walk a reader through a thousand cells to reach whatever
follows the result. From a cell in the column, ArrowDown and ArrowUp step a row, Home and End jump to
the ends, and the movement stops at the ends rather than wrapping. The tab stop follows the focused
row however you got there, clicking included, so leaving the result and tabbing back returns to the
row you were on.

That is one stop per drillable COLUMN, scoped to its table, which is not the same as one per result
in two cases. A table grouping by two drilled dimensions gets a stop for each: measured, two drillable
columns in one table produce two stops. And a drill inside a `nest:` gets one per parent row, because
the renderer draws a nested table per row, each with its own columns: measured, 2 parent rows give 2
stops against 1 for the same dimension grouped flat. Sharing a stop across those tables needs the
marking to match on field identity rather than on a rendered column, which is the limitation
`markDrillableCells` already records.

Those signals are the only thing saying a cell does anything, so it is worth knowing what turns them
off: a destination the surface cannot reach is not offered and not marked, deliberately, since a dead
link is worse than none. A `to=self` drill read in a document that declares no control for its given
is the usual case, and it stays plain text rather than swallowing the click. The same rule decides
the menu, so on an ordinary table what looks clickable and what a click does agree.

**One layout where they do not agree: `# transpose`.** The renderer lays a transposed table out
without the per-cell `grid-column` the marking reads, so no cell in one is marked: no pointer cursor,
no hover colour, no button role, no tab stop. The renderer still routes the click, so the drill FIRES
if a reader hits the cell anyway. A transposed tile therefore navigates on a click that looked like
plain text. Marking it needs a second strategy keyed on that layout, and until then a `# drill`
dimension is better kept out of a `# transpose` tile. The same gap applies, for a different reason, to
a drillable column whose header text is also rendered by a non-drillable field: the marking cannot
tell the two columns apart, so it leaves both unmarked rather than painting a dead link.

Declaring it on the dimension is what makes it work everywhere: any result that groups by that
dimension is clickable, in a dashboard tile and in a **notebook cell** alike, with no per-document
wiring. A notebook cell that groups by a tagged dimension is clickable for exactly this reason, while
saying nothing about drill itself: the notebook imports the given for its own controls, which is all
`to=self` needs. Both surfaces run the same
implementation, down to the hover styling and the menu, so a reader cannot tell from the
interaction which kind of document they are in. Only the menu's `to=self` entry names it ("Filter
this notebook" against "Filter this dashboard").

A drill only lands somewhere useful if the destination declares a control for the given being
seeded, since that is where the value goes. **Publisher does not check that half.** The load-time
lint checks that the destination slug is a dashboard in the package, and for `to=self` that some
model in the package declares the given at all; nothing reads the destination dashboard's own
givens. So a drill can pass the lint and still land on a page with no control for the value it
carried.

Two practical notes. Drill fires from **table cells** reliably; chart marks depend on the renderer's
own hit testing, and carry no hover affordance either way, so a reader has no way to know a bar is
clickable. If a dashboard is meant to be drilled, give it at least one untagged (table) tile. And a
dimension that only exists to be grouped by is worth declaring for its own sake: group by a local
`category` rather than by `products.category`, so the cells carry the tag, with identical output
field names and identical numbers.

## What Publisher checks at load

Every package load lints the dashboards and reports findings as package warnings, visible on the
package page and in the server log. They catch the failures that are otherwise silent: a control
that never appears, a click that goes nowhere. Broadly, they cover:

- **Drill targets.** A `# drill { to=… }` naming a dashboard that does not exist in the package, or
  one that exists but is not served; a `# drill` with no destination at all; and a `to=self` drill
  whose given no model in the package declares, so the clicked value has nowhere to land.
- **Controls.** A given surfaced by a dashboard whose `suggest` names a source, query or dimension
  that file cannot see, or declares a `suggest` in a form that cannot fetch options at all.
- **Layout and tiles.** A tile that does not resolve to a real view; a `# dashboard { columns= }`
  that is not a positive integer; a `# colspan` on a tile's view that is not a positive integer, or
  that is wider than the grid and therefore clamped; and any property inside the artifact tag that
  Publisher does not read, `dashboard_columns=` included.
- **Tags that did not parse**, on the dashboard or on a `given:` declaration, which otherwise lose
  their whole line in silence.
- **Curation.** A dashboard whose entry file is not listed in `explores` under
  `queryableSources: "declared"`, so its queries would be refused. It is not served.
- **Renderer tags the validator rejects.**

That is the shape of the list rather than the whole of it: the findings on the package page are the
authoritative set, and they are worth reading directly rather than counting against this page. Note
too that a tag which does not parse is reported for **syntax** only. An unresolved reference inside a
tag that parses is not a parse finding, and no finding carries a position, so an empty result is not
proof that a tag is well formed.

The drill checks read **every** model in the package, not just the files under `dashboards/`, since
a tag reachable only from a notebook is exactly as breakable.

**A file that does not compile fails the package, rather than appearing as a broken dashboard.**
Loading aborts on the first model error, so the package answers `424` and no dashboard from it is
served, including the ones that were fine. A reload that fails to compile is refused the same way and
leaves the previously compiled package serving, which is the behaviour to rely on while editing: fix
the file, reload again.

## Serving, URLs, and the API

| Path                                                       | What it is                                                  |
| ---------------------------------------------------------- | ----------------------------------------------------------- |
| `/<env>/<pkg>/dashboards/<name>`                           | The Console page                                            |
| `/<env>/<pkg>/dashboards/<name>?CATEGORY=Outerwear`        | The same page, filtered: control state is URL state         |
| `GET /api/v0/environments/<env>/packages/<pkg>/dashboards` | List them                                                   |
| `GET …/dashboards/<name>`                                  | The manifest: title, autorun, columns, control specs, tiles |

A dashboard's query runs through the ordinary query endpoint against
`dashboards/<name>.malloy`, with givens in the request body. There is no dashboard-specific
execution path, so authorization, row limits, and query caps behave exactly as they do everywhere
else. [ai-agents.md](ai-agents.md) has the REST playbook.

After editing a dashboard file, `GET …/packages/<pkg>?reload=true` recompiles the package in place,
and a reload that fails to compile leaves the previously compiled model serving.
[AGENTS.md](../AGENTS.md) §6 covers the edit loop and watch mode.

## Rendering one in your own React app

`<Dashboard>` is a public export of `@malloy-publisher/sdk`, and the Publisher Console is one
consumer of it rather than its home. It takes props instead of reading a router, so the host decides
what a drill and a filter change mean:

`<Dashboard>` reads its server through context, so it has to sit inside a `<ServerProvider>`; without
one it throws `useServer must be used within a ServerProvider`. Mount the provider once, near the root
of your app, not per dashboard.

```tsx
import {
  Dashboard,
  encodeResourceUri,
  ServerProvider,
} from "@malloy-publisher/sdk";

<ServerProvider baseURL="https://publisher.example.com/api/v0">
  <Dashboard
    resourceUri={encodeResourceUri({
      environmentName: "examples",
      packageName: "storefront",
    })}
    dashboard="overview"
    givens={Object.fromEntries(searchParams)}
    // MERGE, do not replace. `managed` is every given this dashboard declares,
    // including the ones holding no value right now, and it is there because
    // `next` alone cannot tell a control the reader CLEARED from a parameter
    // that was never yours. Replacing the whole query string drops an unrelated
    // parameter as soon as the manifest arrives, before the reader touches
    // anything.
    onGivensChange={(next, managed) =>
      setSearchParams(
        (current) => {
          for (const name of managed) {
            if (!Object.prototype.hasOwnProperty.call(next, name)) {
              current.delete(name);
            }
          }
          for (const [name, value] of Object.entries(next)) {
            current.set(name, value);
          }
          return current;
        },
        // Filtering is not a navigation step.
        { replace: true },
      )
    }
    // The slug is ENCODED, because it is a filename: it can hold a character
    // that would read as structure in a path or start the query string early.
    onNavigate={(target) =>
      navigate(
        `/dashboards/${encodeURIComponent(target.dashboard)}` +
          `?${new URLSearchParams(target.givens)}`,
      )
    }
  />
</ServerProvider>;
```

A host that keeps its own names in the query string should also remember the ones
it has written, so a given the dashboard stops declaring is still cleaned up;
`DashboardPage` does that with a `writtenRef`.

Without `onNavigate`, drilling to another dashboard is inert, and by the rule above those cells do
not read as clickable either, so a host that has not wired navigation shows no affordance rather than
a dead one. `to=self` still works, since it never leaves the component. The Console's own
`DashboardPage` (`packages/app/src/components/pages/DashboardPage/DashboardPage.tsx`) is the worked
example of both handlers; [`examples/data-app`](../examples/data-app) is a standalone SDK app, but it
predates this component and builds its pages from `EmbeddedQueryResult` rather than rendering
`<Dashboard>`.

Embedding into a **non-React** host page is a follow-up
([#931](https://github.com/malloydata/publisher/issues/931)); `Publisher.embed` cannot usefully
target a dashboard route yet, so an [HTML data app](html-data-apps.md) remains the surface with the
complete embedding story.

## Where dashboards stop

Whatever renderer tags can express is the ceiling. There is deliberately no code escape hatch (no
custom component, no authored JavaScript) because a page that needs to go past the tags is an
[HTML data app](html-data-apps.md), which already does that job at whole-page scope with a real
runtime and a real embedding story. The reasoning, including why the sandboxed-component approach
was built and then cut, is in
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md#custom-jsx-components-cut).

## See also

- [choosing-a-surface.md](choosing-a-surface.md): whether this is the surface you want, against a
  notebook and an HTML data app
- [givens.md](givens.md): the parameter mechanism the controls are built on
- [malloyyo-dashboards-design.md](malloyyo-dashboards-design.md): the design, and the grammar's provenance
- [console.md](console.md): the rest of the Publisher Console
- [`examples/storefront`](../examples/storefront), the ecommerce model this page's examples build on
