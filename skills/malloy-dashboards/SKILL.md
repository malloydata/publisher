---
name: malloy-dashboards
description: "Build or modify a Malloy Publisher dashboard, a tagged .malloy file in a package's dashboards/ directory, with auto-rendered filter controls, a grid layout, and # drill click-through. Use when the user asks for a dashboard, a filterable operational view, or drill-through between views, and no code is wanted."
---

# Publisher Dashboards

> A `dashboards/*.malloy` file **is** a dashboard. It imports the model, declares one query (or names existing views), applies its filtering, and tags the layout. Publisher discovers it at package load, renders the filter controls from the givens it references, and serves it at `/<env>/<pkg>/dashboards/<name>`. No code, no build step.

## When this is the right tool

| The user wants                                       | Use                                              |
| ---------------------------------------------------- | ------------------------------------------------ |
| A recurring, at-a-glance view behind shared filters  | this skill (a dashboard)                         |
| A narrative, with prose between the numbers          | a notebook (`skill:malloy-notebooks`)            |
| Custom design, branding, or interactions beyond tags | an HTML data app (`skill:malloy-html-data-apps`) |
| The model itself: sources, measures, joins           | `skill:malloy-modeling`                          |

Notebooks and dashboards run the same engine, so **interactivity is not the axis**: both get filter
controls, URL-addressable state, Apply batching, and `# drill`. Pick on the shape of the document.
Scanned at a glance is a dashboard; read top to bottom is a notebook.

## Build sequence

1. **READ THE MODEL FIRST.** Get the real source, view, dimension, and given names from the package:
   `malloy_getContext` if you have it, otherwise the REST model endpoint or the `.malloy` files.
   Never guess a name. A guessed field in a query fails the whole package load, not just that one
   dashboard; a guessed tile or suggest source is quieter, and only shows up in the package warnings.
2. **DECIDE THE FORM** (below): single-query if the page is one filtered result with parts;
   composite if the views already exist and the job is choosing which to show together.
3. **DECLARE THE GIVENS** the dashboard will filter by, in the model (usually `givens.malloy`), with
   their control tags. Skip if they already exist, since a given is a model concern and dashboards
   share them.
4. **COMPOSE THE FILE** for `dashboards/`, following the template below, but do not save it yet.
   Import every given it filters by, and every source or query any of those givens names in a
   `suggest`. Both are per-file, and getting the suggest wrong does not error: the control still
   looks like a picker but has no options, and only the package warnings name it.
5. **COMPILE IT** with `malloy_compile` (or `POST …/models/<path>/compile`), against the source text,
   before you save. Cheaper than a reload, and it catches a wrong field or source name outright. A
   clean compile is not a working dashboard: some tag mistakes surface at step 6, and some only when
   you look at the page in step 7.
6. **SAVE IT, THEN RELOAD AND READ THE WARNINGS.** `malloy_reloadPackage`, or
   `GET …/packages/<pkg>?reload=true`. Check the status the reload returns as well as the warnings:
   a 424 means the package did not load and your edit is not live. See "Read the lint" below.
7. **OPEN IT AND LOOK.** Not optional; see "What 'done' means".

Compiling before you save is worth the extra step: compile against a path that already holds the
file and every source in it collides with itself, which reports as a wall of redefinition errors that
look like a problem with your Malloy rather than with the order you did things in.

## The two forms

**Single-query:** one query whose result is the dashboard. Reach for this by default.

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

The `#"` line above the tag is a doc comment, and it is the page's description. If you leave `title=`
off the artifact tag, it becomes the title instead, so write it as one, not as a sentence about the
page.

**Composite:** a list of views that already exist, each run separately into one grid. The tag is
model-level (`##`) because there is no query of its own to hang a `#` tag on.

```malloy
##! experimental.givens
## artifact { title="Seasonality" tiles=["scoped_sales -> sales_by_month", "scoped_sales -> sales_by_year", "scoped_sales -> seasonality"] dashboard_columns=3 }
import { scoped_sales } from './_shared.malloy'
import { products } from '../storefront.malloy'
import { CATEGORY, SINCE } from '../givens.malloy'
```

A composite has no query, so the filtering it applies must live in what it composes: a source that
already has the givens applied. Put it in an untagged `dashboards/_shared.malloy`, which discovery
treats as a shared include rather than a dashboard:

```malloy
##! experimental.givens
import { order_items } from '../storefront.malloy'
import { CATEGORY } from '../givens.malloy'

source: scoped_sales is order_items extend {
  where: products.category ~ $CATEGORY
}
```

Its tiles are equal-width (there is no per-tile colspan), so pick a `dashboard_columns` the tile list
divides evenly, and use the single-query form when one tile deserves more room than the others.

## Layout: the four tags that make a page line up

Cards and tiles share one grid. Copy this recipe, and use the same column count on every dashboard in
the package so they read as one product:

1. **`columns=12`** on the `# dashboard` tag. Twelve divides by 2, 3, 4 and 6, so a row is even with
   three cards or four.
2. **A `# colspan` on every card and tile, summing to 12 per row.** Four cards at 3, three at 4, two
   tiles at 6, a full-width table at 12. Omit them and each item takes its natural width, which is
   what "the dashboard looks janky" almost always is.
3. **`# break` on the first tile after the cards.** Otherwise it flows into the columns left beside
   the cards and the next tile wraps. Not needed per row: once a row sums to 12 the next item wraps
   on its own.
4. **`# label="…"` on every nest and every aggregate.** The heading is otherwise the view's or
   field's name.

Then the traps:

- **No `# size=fill` on a dashboard tile.** Inside a dashboard it measures against the container the
  whole grid was handed, not the tile, so it yields a chart thousands of pixels tall. Tiles already
  size to their colspan.
- **A KPI card's label is one line that ellipses.** "Orders / customer" reads as "ORDERS / CUSTOMEI"
  in a 2-of-12 card. Widen the card or shorten the label.
- **A ratio needs a number format.** `order_count / customer_count` renders as `10.695` on a card;
  `# number="#,##0.0"` is the precision it actually carries.
- **A `# shape_map` legend is titled with the measure's field _name_, not its `# label`.** Rename it
  in the view: `aggregate: revenue is total_sales`.
- **A series legend sizes itself from the longer of the series label and its widest value**, then
  truncates both. A 4-character label over 4-digit years clips to `20…`; `# label="Order year"`
  instead of `"Year"` buys the room. A legend showing `…` is this, not a data problem.

The last two are upstream renderer behavior, cheap to work around in the model.

The same tags govern a `# dashboard` **view** run in a notebook cell, since both surfaces render
through the same code, so a view laid out this way looks the same in a cell as on a dashboard page.
Height is the one thing the surface decides: a single-query dashboard renders at its natural height,
a composite's tiles are each capped, and in a notebook a chart cell is capped and a table cell hugs
its rows.

## The rules that actually bite

- **The filename is the dashboard's name:** its URL slug, its listing name, and its `# drill`
  target. The query inside can be called anything, and sometimes must be (a query named `regions`
  collides with an imported `regions` source).
- **Importing a given is what makes it bindable.** Malloy's given namespace is per-file. A given the
  dashboard file does not import gets no control and cannot be sent to it, even when the `where:`
  that references it lives up an import chain. A composite must import the givens its tiles use.
- **A suggest's source or query has to resolve in the dashboard file too.** `suggest { source=products … }`
  means the dashboard imports `products`.
- **`# colspan` does nothing without `# dashboard { columns=N }`.** This is the usual reason a grid
  comes out as one column. It does warn ("Ignored # colspan on 'x': colspan only applies in columns
  mode"), but on the query response as a render log, not in the package warnings, so step 6 will not
  show it. See "Layout" above.
- **A model-level `##` tag must be on one line.** Wrapping one always breaks it, but how you find
  out depends on what follows. If the continuation is not valid Malloy you get a compile error. If it
  happens to be, an `import` say, the file compiles clean, quietly stops being a dashboard and
  becomes a shared include, and only the package warnings tell you: "Tag does not parse (Unclosed
  '{')". That second case is why step 5 is not the last step.
- **In a `# dashboard` view, fields render by role.** A top-level `aggregate:` measure is a KPI card,
  so do not nest a `# big_value` view to get one. Each `nest:` is a tile. Give every KPI a
  `# label=`, or the card is headed `total_sales`.
- **Only table cells drill.** A chart mark emits no click, so a chart is not clickable however it is
  tagged. If a dashboard is meant to be clicked, give it at least one untagged (table) tile.

`skill:malloy-gotchas-rendering` covers the renderer tags in depth; `skill:malloy-charts` covers
choosing them.

## Filter controls

Controls come from the `given:` declarations the query references, and the tags on the declaration
are the control contract, declared once and identical on every dashboard and in every notebook that
uses them:

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

`control=select`/`multiselect` with a `suggest` renders a picker filled from the data;
`range_min`/`range_max` on a `filter<number>` renders a slider; a `date` or `timestamp` renders a
date picker. Which controls appear is per-dashboard, decided by which givens the query references.
`skill:malloy-modeling` and `docs/givens.md` cover givens themselves.

Two per-dashboard options on the artifact tag:

- `autorun=false` batches control changes behind an Apply button. Add it once a page is slow enough
  that a reader notices two round trips.
- `givens { CATEGORY=f'Outerwear' }` sets starting values, not a redeclaration. A URL parameter wins.

A notebook takes both at the file level, as `## autorun=false` and `## givens { CATEGORY=f'Outerwear' }`,
and behaves identically.

## Drill

`# drill` goes on a model **dimension**, never on a dashboard:

```malloy
# drill { to=["category", "self"] given=CATEGORY }
dimension: category is products.category
```

`to=<slug>` navigates to that dashboard with the clicked value written into the named given;
`to=self` filters in place; two or more destinations pop a menu.

Declaring it on the dimension is the point: every result that groups by it becomes clickable, in a
dashboard tile and in a notebook cell alike. So when a view is meant to be drilled, group by the
tagged dimension. Declaring `dimension: category is products.category` and grouping by `category`
gives the identical output field name and the identical numbers, and carries the tag.

**Always write `given=`.** Without it the given name is the dimension name **verbatim**, so a
`dimension: category` seeds a given called `category`, which does not match a declared
`given: CATEGORY`. Nothing errors: the `to=self` is quietly not offered, and a `to=<slug>` still
navigates and still looks like it worked, but arrives as `?category=…`, which the destination drops,
so you land on an unfiltered page. The lint upper-cases when it checks, so it stays green.

A drill only lands somewhere useful if the destination declares a control for the given being
seeded. **No lint checks that.** It verifies that the target slug is a dashboard in the package, and
for `to=self` that some model declares the given, and stops there. Nothing reads the destination's
own givens, so click it and look.

Cells in a drillable **table** column show it: pointer cursor, and a blue underline on hover. Chart
marks get no such affordance in either Publisher or Malloyyo, so a dashboard meant to be drilled
wants at least one table tile. A destination the surface cannot honor is not marked and not offered,
which is why a `to=self` reads as plain text in a document that declares no control for its given.

## Read the lint

Package warnings after a reload are the dashboard's test suite. Fix all of them:

- `# drill … targets "x", which is not a dashboard in this package`: a dead click.
- `to=self, but no model in this package declares a given "X"`: the clicked value has nowhere to go.
- `given "X" suggests options from source "y", which this file does not define`: the dropdown will
  be empty, so import it.
- A tile that does not resolve to a real view, or a non-positive `dashboard_columns`.

**Read the status the reload itself returns, not the listing.** One dashboard that fails to compile
fails the whole package load, and the reload answers **424** with the compile error. A package that
was already serving then keeps serving its previous version, so the listing still answers 200 and
looks perfectly healthy while your edit has silently not taken effect. That is the usual case and the
one to watch for: a 424 you did not read, and a page that has not changed. Only a package that never
loaded at all, on a fresh boot or when newly added, answers 424 on the listing too.

If the reload is 200 and the others are listed but yours is not, discovery skipped the file instead,
usually a missing or misspelled `# artifact` tag, which is the same mechanism that deliberately skips
an untagged shared include.

**A clean reload is not proof the tags are right.** The tag lint is syntax only: it carries no
position and says nothing about a name that does not resolve. It catches *a* malformed tag; its
absence is not evidence there are none. That is why the last step is opening the page, not reading
the warning list.

## What "done" means

- Every source, view, and field name came from the model you read in step 1.
- The reload returned **200**, not 424. A 424 means the page you are about to look at is the old one.
- The package reloads with **zero** dashboard warnings.
- You opened the page and every tile shows real numbers: not stuck loading, not an error, not an
  empty state you did not intend.
- Each control renders as the widget you intended (a select shows options; a slider is a slider),
  and changing one changes the numbers.
- If you added a `# drill`, you clicked it and landed where you meant to, with the given seeded.
- Every card and tile carries a colspan, each row's colspans sum to `columns`, and the rows end flush
  with each other. Nothing is clipped, no tile is thousands of pixels tall, and no legend or card
  label ends in `…`.

## Reference

- `docs/dashboards.md`: the full guide this skill condenses.
- `docs/givens.md`: the givens the controls are generated from.
