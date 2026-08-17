# Dashboards

> **What this is:** how to write a `dashboards/*.malloy` file — a filterable, clickable, grid-laid-out
> dashboard declared entirely in Malloy tags, with no code and no build step.

A dashboard is a self-contained `.malloy` file in a package's `dashboards/` directory. The file _is_
the dashboard: it imports the model parts it needs, declares its query, applies its filtering, and
tags the layout. Publisher discovers it at package load, lists it on the package page, and serves it
at `/<env>/<package>/dashboards/<name>`.

Everything on the page comes from one tagged query. The controls at the top are not written anywhere
in the page; they are rendered from the `given:` declarations the query filters by. Cells are
clickable where the model's dimension carries a `# drill` tag.

> **No bundled example ships a dashboard yet.** The `dashboards/` directory in the `storefront`
> example, and the screenshots of it, arrive with the examples change that follows this one. Until
> then this page is the reference and the quickest way to see one is to write it, in any package.

The format is the one [Malloyyo](https://github.com/malloydata/malloyyo) uses, kept byte-compatible on purpose, so
a model repo with a `dashboards/` directory works unchanged in either.

## Where the pieces live

```
storefront/
  publisher.json           # package manifest
  storefront.malloy        # sources, measures, reusable views, # drill tags
  givens.malloy            # given: declarations — the filter controls
  dashboards/
    overview.malloy        # a dashboard: imports the model, declares its query
    category.malloy
    regions.malloy
    seasonality.malloy     # a composite: a list of views, no query of its own
    _shared.malloy         # no artifact tag ⇒ a shared include, not a dashboard
```

Two conventions carry most of the weight:

- **The filename is the dashboard's name.** `overview.malloy` is the slug in its URL, the name in
  the package listing, and what a `# drill { to=overview }` elsewhere in the model points at. The
  query inside it can be called anything — `regions.malloy` names its query `regional_sales`,
  because the `regions` source it imports already owns that name in the file.
- **A file in `dashboards/` with no `# artifact` tag is a shared include.** Discovery skips it. It
  is where to put anything more than one dashboard needs.

Each dashboard file compiles **as its own entry**, which is why it imports what it uses rather than
inheriting it: model-level annotations do not cross an import, so the `# artifact` tag is only
readable when the file is the thing being compiled.

## A first dashboard

The whole grammar in one file:

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
    category_performance
}
```

- `# artifact { … }` is what makes the file a dashboard. `title=` names it; without one the title
  falls back to the `#"` doc comment above, then to the slug.
- `# dashboard { columns=N }` is the renderer's grid — a standard `@malloydata/render` tag, not a
  Publisher one.
- `where:` naming a given is what puts a control on the page. Two names here, so two controls.
- In a `# dashboard` view, fields render **by role**: a top-level `aggregate:` measure becomes a KPI
  card, and each `nest:` becomes a tile.

### Laying out the grid

Cards and tiles share one grid, so a page that lines up is a matter of four tags. Using one recipe
across a package's dashboards is what makes them read as one product rather than as several pages:

- **`columns=12`.** Twelve divides by 2, 3, 4 and 6, so a row comes out even whether it holds three
  cards or four. Pick one number and use it on every dashboard in the package.
- **A colspan on every card and every tile, summing to `columns` per row.** Four cards at 3, three at
  4, two tiles at 6, a full-width table at 12. Leave them off and each item takes its natural width,
  which is how a page ends up looking accidental. A `# colspan` without `columns=` on the
  `# dashboard` tag is ignored with a warning — the usual reason a grid comes out as one column.
- **`# break` on the first tile after the cards.** Without it the first tile flows into whatever
  columns are left beside the cards and the next one wraps. A break is for interrupting a row that
  would otherwise be shared, not for every new row: once a row's colspans sum to `columns`, the next
  item wraps on its own with the same gap.
- **`# label="…"` on every nest.** The tile's heading is otherwise the view's name —
  `sales_by_month` over a chart, which reads like a database column. Labelling on the nest rather
  than the view lets the same view carry different words on different pages.

Two things about cards specifically. A top-level `aggregate:` measure _is_ the card, so do not nest a
`# big_value` view to get one: nested in a dashboard it renders embedded, and each measure becomes a
full-width bar inside a single tile instead of a row of cards. And a card's label is one line that
ellipses rather than wrapping, so a narrow card truncates it silently — "Orders / customer" reads as
"ORDERS / CUSTOMEI" at 1 column of 6.

Tiles size themselves to the width their colspan gives them, so leave `# size=fill` off. Inside a
dashboard, fill measures against the container the whole grid was handed rather than against the
tile, which produces a chart thousands of pixels tall from a tag that reads like "fit the tile."

### Labels the renderer takes from somewhere else

Two places read a field's **name** rather than its `# label`, so a labelled model still shows
`total_sales` on the page:

- **A `# shape_map`'s color legend** is titled with the measure's field name. Rename the measure in
  the view — `aggregate: revenue is total_sales` — rather than only labelling it.
- **A chart's series legend** reserves its width from the longer of the series label and its widest
  value, then truncates both to fit. A 4-character label over 4-digit years clips to `20…`; naming
  the series `# label="Order year"` instead of `"Year"` buys the column enough room. A legend showing
  `…` is this, not a data problem.

Both are upstream renderer behavior, not Publisher's, and both are cheap to work around in the
model.

All of this applies equally to a `# dashboard` **view** run in a notebook cell — the two surfaces
render through the same code, so a view laid out with these tags looks the same in a cell as on a
dashboard page. The storefront model's `business_overview` view is the worked case, and the notebook's
first cell is that view. Height is the one thing the surface decides rather than the tags: a dashboard
renders at its natural height, while a notebook caps a chart cell and lets a table cell hug its rows,
so one long result cannot push the prose off the page.

### Tag reference

| Construct                                                        | What it does                                                                                                                   |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `# artifact { title= givens{…} autorun= }` on a `query:`                | Declares the dashboard. `title` falls back to the `#"` doc comment; `givens` sets starting control values; see [Apply](#apply) |
| `## artifact { title= tiles=[…] dashboard_columns=N }`           | Declares a **composite** — model-level, because there is no query of its own to hang a `#` tag on                              |
| `# dashboard { columns=N }`, `# colspan=K`, `# break`            | The renderer grid — see [Laying out the grid](#laying-out-the-grid). `# colspan` does nothing without `columns=`               |
| `# label="…"` on an aggregate                                    | What the KPI card is headed. Without it a card reads `total_sales`, which is a column name, not a number a reader came for     |
| `# drill { to=[…] given=… }` on a source `dimension:`            | Makes cells that group by it clickable — see [Drill](#drill)                                                                   |
| A `dashboards/*.malloy` with **no** artifact tag                 | A shared include, skipped by discovery                                                                                         |

Two spellings that bite:

- **A model-level `##` tag has to be on one line.** Wrapping a long `## artifact { … }` across lines
  is a compile error, and it fails the whole package rather than the one file.
- **`# artifact` is read off a `query:`, not off a `view:`.** A source-level view carrying the tag is
  not discovered, and nothing says so: the file is treated as a shared include and quietly produces
  no dashboard. Give the dashboard its own `query:`, as every example here does.

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
so import it there — a dashboard that surfaces a given whose suggest names something it cannot see
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
the grid stops loading. A package with no `explores` has curation off, so an import is all it
needs.

Which controls appear is decided per dashboard, by which givens its query references. Declaring ten
and referencing two shows two. That is what lets one `CATEGORY` declaration scope revenue on one
dashboard and margin on another without either redeclaring it.

**Importing a given is what makes it bindable.** Malloy's given namespace is per-file, so a
dashboard can only be _run_ with the givens its own file imports, even when the `where:` that
references one lives up an import chain. A given the file does not import gets no control, and
sending it at run time fails with "unknown given". Everything about givens themselves —
declaration, types, defaults, access control — is in [givens.md](givens.md).

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

All three behave identically in a notebook, which spells them at the file level — `## autorun=false`
and `## givens { REGION=f'West' }` — and gets the same controls, the same URL state, and the same
Apply button from the same code.

## Composite dashboards

When the views are already modelled and the dashboard is a matter of choosing which to show
together, there is nothing left to write a query for. A composite names them instead:

```malloy
##! experimental.givens
## artifact { title="Seasonality" tiles=["scoped_sales -> sales_by_month", "scoped_sales -> sales_by_year", "scoped_sales -> seasonality"] dashboard_columns=3 }
import { scoped_sales } from './_shared.malloy'
import { products } from '../storefront.malloy'
import { CATEGORY, SINCE } from '../givens.malloy'
```


Each tile runs as its own query, which means a broken tile shows its error in place instead of
blanking the page, and a control change re-runs only the tiles that reference it. The control row is
the union across the tiles.

A composite's tiles are equal-width — `dashboard_columns` is the whole layout, and there is no
per-tile colspan, since each tile is a separate result rather than a field in one. So choose a column
count the tile list divides evenly (three tiles at 3, four at 2 or 4), and expect each chart to have
`1/N` of the page: a monthly trend at a third of the width gets crowded x labels, which is a reason
to prefer the single-query form when one tile deserves more room than the others.

A composite has no query of its own, so the filtering it applies has to live in what it composes.
That is the job the shared include does here — `_shared.malloy` scopes the source once:

```malloy
// dashboards/_shared.malloy — no artifact tag, so an include rather than a dashboard.
##! experimental.givens
import { order_items } from '../storefront.malloy'
import { CATEGORY, SINCE } from '../givens.malloy'

source: scoped_sales is order_items extend {
  where: category ~ $CATEGORY and created_at >= $SINCE
}
```

Note the imports in the composite itself. Nothing in that file mentions `CATEGORY` or `SINCE` — the
`where:` that does is one file over — but the given namespace is per-file, so without importing them
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
- `to=self` filters wherever the click came from, without leaving the page.
- More than one destination pops a menu, because a choice is not a guess.
- `given=` names the given to seed. **Write it.** Without it the given is the dimension name
  exactly as the model spells it, so a lowercase `dimension: category` looks for a given called
  `category`. A dashboard whose model declares `CATEGORY` does not match it, and the failure is
  quiet: the cell still reads as clickable, and the click lands on an unfiltered page with an
  empty control. Note the load-time lint does not catch this, because it upper-cases the
  dimension name before checking while the click does not.

**What a reader sees.** Cells in a drillable column take a pointer cursor, and turn blue and
underlined under the pointer: plain text at rest, a link when you reach for them. They are also in
the tab order and carry a button role, so a keyboard reaches them and Enter or Space fires the drill,
with focus styled the way hover is. That matters because a pointer cursor and a hover colour are the
two things a keyboard or touch user can never produce.

Those signals are the only thing saying a cell does anything, so it is worth knowing what turns them
off: a destination the surface cannot reach is not offered and not marked, deliberately, since a dead
link is worse than none. A `to=self` drill read in a document that declares no control for its given
is the usual case, and it stays plain text rather than swallowing the click. The same rule decides
the menu, so what looks clickable and what a click does cannot drift apart.

Declaring it on the dimension is what makes it work everywhere: any result that groups by that
dimension is clickable, in a dashboard tile and in a **notebook cell** alike, with no per-document
wiring. A notebook cell grouping by a tagged dimension is clickable for exactly this reason, and
says nothing about drill itself: it imports the given for its own Parameters panel, which is all
`to=self` needs. Both surfaces run the same
implementation, down to the hover styling and the menu, so a reader cannot tell from the
interaction which kind of document they are in — only the menu's `to=self` entry names it ("Filter
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
package page and in the server log. They catch the failures that are otherwise silent — a control
that never appears, a click that goes nowhere. Broadly, they cover:

- **Drill targets.** A `# drill { to=… }` naming a dashboard that does not exist in the package, or
  one that exists but is not served; a `# drill` with no destination at all; and a `to=self` drill
  whose given no model in the package declares, so the clicked value has nowhere to land.
- **Controls.** A given surfaced by a dashboard whose `suggest` names a source, query or dimension
  that file cannot see, or declares a `suggest` in a form that cannot fetch options at all.
- **Layout and tiles.** A composite tile that does not resolve to a real view, and a `columns=` or
  `dashboard_columns=` that is not a positive integer.
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
| `/<env>/<pkg>/dashboards/<name>?CATEGORY=Outerwear`        | The same page, filtered — control state is URL state        |
| `GET /api/v0/environments/<env>/packages/<pkg>/dashboards` | List them                                                   |
| `GET …/dashboards/<name>`                                  | The manifest: title, autorun, columns, control specs, tiles |

A dashboard's query runs through the ordinary query endpoint against
`dashboards/<name>.malloy`, with givens in the request body — there is no dashboard-specific
execution path, so authorization, row limits, and query caps behave exactly as they do everywhere
else. [ai-agents.md](ai-agents.md) has the REST playbook.

After editing a dashboard file, `GET …/packages/<pkg>?reload=true` recompiles the package in place,
and a reload that fails to compile leaves the previously compiled model serving.
[AGENTS.md](../AGENTS.md) §6 covers the edit loop and watch mode.

## Rendering one in your own React app

`<Dashboard>` is a public export of `@malloy-publisher/sdk`, and the Publisher Console is one
consumer of it rather than its home. It takes props instead of reading a router, so the host decides
what a drill and a filter change mean:

```tsx
import { Dashboard, encodeResourceUri } from "@malloy-publisher/sdk";

<Dashboard
  resourceUri={encodeResourceUri({
    environmentName: "examples",
    packageName: "storefront",
  })}
  dashboard="overview"
  givens={Object.fromEntries(searchParams)}
  onGivensChange={(next) => setSearchParams(next, { replace: true })}
  onNavigate={(target) =>
    navigate(
      `/dashboards/${target.dashboard}?${new URLSearchParams(target.givens)}`,
    )
  }
/>;
```

Without `onNavigate`, drilling to another dashboard is inert — and, by the rule above, those cells do
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

Whatever renderer tags can express is the ceiling. There is deliberately no code escape hatch — no
custom component, no authored JavaScript — because a page that needs to go past the tags is an
[HTML data app](html-data-apps.md), which already does that job at whole-page scope with a real
runtime and a real embedding story. The reasoning, including why the sandboxed-component approach
was built and then cut, is in
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md#custom-jsx-components-cut).

## See also

- [givens.md](givens.md) — the parameter mechanism the controls are built on
- [malloyyo-dashboards-design.md](malloyyo-dashboards-design.md) — the design, and the grammar's provenance
- [console.md](console.md) — the rest of the Publisher Console
- [`examples/storefront`](../examples/storefront), the ecommerce model this page's examples build on
