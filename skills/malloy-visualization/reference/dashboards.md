<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Publisher Dashboards

## Contents

- [When this is the right tool](#when-this-is-the-right-tool)
- [Build sequence](#build-sequence)
- [The form](#the-form)
- [artifact { title="Storefront overview" tiles=["overview -> kpis", "overview -> revenue_trend", "overview -> revenue_by_state"] } dashboard { columns=12 }](#artifact--titlestorefront-overview-tilesoverview---kpis-overview---revenuetrend-overview---revenuebystate--dashboard--columns12-)
  - [Also served: `# artifact` on a `query:`](#also-served--artifact-on-a-query)
  - [Where the filtering goes](#where-the-filtering-goes)
- [Layout: the four tags that make a page line up](#layout-the-four-tags-that-make-a-page-line-up)
- [The rules that actually bite](#the-rules-that-actually-bite)
- [Filter controls](#filter-controls)
- [Drill](#drill)
- [Losing the grid](#losing-the-grid)
- [Read the lint](#read-the-lint)
- [What "done" means](#what-done-means)
- [Reference](#reference)

> A `dashboards/*.malloy` file **is** a dashboard. It imports the model, either declares one query and applies its own filtering or names views that already do, and tags the layout. Publisher discovers it at package load, renders the filter controls from the givens it references, offers `# drill` click-through between pages, and serves it at `/<env>/<pkg>/dashboards/<name>`. No code, no build step.

## When this is the right tool

| The user wants                                       | Use                                              |
| ---------------------------------------------------- | ------------------------------------------------ |
| A recurring, at-a-glance view behind shared filters  | this skill (a dashboard)                         |
| A narrative, with prose between the numbers          | a notebook (``reference/notebooks.md``)            |
| Custom design, branding, or interactions beyond tags | an HTML data app (``reference/html-data-apps.md``) |
| The model itself: sources, measures, joins           | the modeling workflow                            |

Notebooks and dashboards run the same engine, so **interactivity is not the axis**: both get filter
controls, URL-addressable state, Apply batching, and `# drill`. Pick on the shape of the document.
Scanned at a glance is a dashboard; read top to bottom is a notebook.

## Build sequence

1. **READ THE MODEL FIRST.** Get the real source, view, dimension, and given names from the package:
   `get_context` if you have it, otherwise the REST model endpoint or the `.malloy` files.
   Never guess a name. A guessed field in a query fails the whole package load, not just that one
   dashboard; a guessed tile or suggest source is quieter, and only shows up in the package warnings.
2. **PICK THE VIEWS TO SHOW.** A dashboard is `## artifact { tiles=[…] }` naming existing views, so
   this is the design step: which views, how wide each sits, what each is called. There is one form,
   so there is no form to choose.
3. **DECLARE THE GIVENS** the dashboard will filter by, in the dashboard file itself, with their
   control tags: see "Filter controls" below for the syntax and what each tag renders as. That is
   the convention because the dashboard builder edits the dashboard file and nothing else, so a
   filter it can add, change or remove is a declaration in that file. Bind them on the tiles (step
   4). Reuse the package's `givens.malloy` only for a control several surfaces genuinely share, and
   then import it whole, knowing the builder can bind those givens but not edit them.
4. **COMPOSE THE FILE** for `dashboards/`, following the template below, but do not save it yet.
   Name the sources you need: `import { order_items, products } from '../storefront.malloy'`.
   Each tile binds its controls with a refinement, `view: t is v + { where: category ~ $CATEGORY }`,
   one clause per given it answers to; a `filter<...>` binds with `~`, a plain `date` or `number`
   given is a value and binds with `>=`, `<=` or `=`. Only the givens some tile references become
   controls, so a declaration nothing binds shows nothing. Then import every
   source or query any referenced given names in a `suggest`. Both are per-file, and getting the
   suggest wrong does not error: the control still looks like a picker but has no options, and says
   so underneath, "Could not load the options for this control". The package warnings name it too. **A `suggest` naming a `query=` needs that
   query's own source imported as well**, because an import is not transitive: the query resolves by
   name, so the file compiles and the package loads, but running the picker fails with
   `Undefined source '<name>'`. The package warnings name this one too, saying which source to
   import. Import the source that suggest query reads, not just the query.
5. **COMPILE IT** with `compile_model` (or `POST …/models/<path>/compile`), against the source text,
   before you save, at the path the file will have. **Editing one that already exists needs
   `"scope": "file"`**, which compiles your source AS that file; the default appends it instead, so
   every imported name and the query name collide with the saved copy and you get a wall of
   already-defined errors that reads as broken Malloy rather than a wrong scope. **Editing a shared
   include wants `"scope": "package"`**, which recompiles every file as saved: `file` only checks the
   one you are editing, so renaming a source in `_shared.malloy` passes it while breaking every
   dashboard that imports it. A clean compile is not a working dashboard: some tag mistakes surface
   at step 6, and some only when you look at the page in step 7. (The third scope, `append`, is the
   default and is what a not-yet-saved file gets.)
6. **SAVE IT, RELOAD, AND READ THE MANIFEST AND THE WARNINGS.** `reload_package`, or
   `GET …/packages/<pkg>?reload=true`. Check the status the reload returns as well as the warnings:
   a 424 means the package did not load and your edit is not live. **The `warnings` key is absent
   when there are none**, so an empty response is the pass, not a sign you are reading the wrong
   field. Then read `GET …/packages/<pkg>/dashboards/<name>`: its `givens` are exactly the controls
   that will render, which catches a given you imported but never referenced before you open the page,
   and its `query` is the name to run in step 7. See "Read the lint" below.
7. **OPEN IT AND LOOK.** Not optional; see "What 'done' means".

## The form

A dashboard is `## artifact { tiles=[…] }` at model level: named views, each run as its own query, laid
out by Publisher into the grid `# dashboard { columns=N }` names.

```malloy
##! experimental.givens
## artifact { title="Storefront overview" tiles=["overview -> kpis", "overview -> revenue_trend", "overview -> revenue_by_state"] } dashboard { columns=12 }
import { order_items, products } from '../storefront.malloy'

// The controls, declared here: the tags are each one's control contract.
# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''
# label="Ordered since"
given: SINCE :: date is @2023-01-01

// Layout goes on the VIEW, and a thin re-declaration is the place to put it: the
// modelled view keeps its chart tag, this decides how wide it sits here, and the
// `+ { where: ... }` says which controls the tile answers to.
source: overview is order_items extend {
  # colspan=12
  # label="Key figures"
  # big_value
  view: kpis is key_figures + { where: category ~ $CATEGORY, where: created_at >= $SINCE }

  # colspan=8
  # break
  # label="Revenue by month"
  view: revenue_trend is sales_by_month + { where: category ~ $CATEGORY, where: created_at >= $SINCE }

  # colspan=4
  # label="Revenue by state"
  view: revenue_by_state is sales_by_state + { where: category ~ $CATEGORY, where: created_at >= $SINCE }
}
```

Model-level because there is no query of its own to hang a `#` tag on, and model-level for a second
reason: tiles run as separate queries, which is the only way a page can span unrelated sources. A
nest's pipeline starts from its own query's source and there is no way to combine two.

Three things the form costs, so you are not surprised by them:

- **A tile expression is a string in an annotation, so the compiler never checks it.** Rename a view
  and the file still compiles; the tile fails at package load. Read the lint (step 6).
- **No per-parent-row grouping.** There is no parent query to repeat a grid over.
- **Filtering lives on the tiles**, not on the page: each view's `+ { where: ... }` names the
  controls it answers to. Below is why, and the one thing that does not work.

### Also served: `# artifact` on a `query:`

One query whose result is the whole page, laid out by `@malloydata/render` from the query's own
`# dashboard` tag. This is Malloy's rendering feature, the same thing a notebook cell shows, not a
second way to build a dashboard, and it cannot span sources. Publisher serves it, so you will meet it
in existing packages, and the layout tags below are shared with it. Do not author a new dashboard
this way.

```malloy
##! experimental.givens
import { order_items, products } from '../storefront.malloy'
import '../givens.malloy'

#" Revenue and margin at a glance, and where they come from.
# artifact { title="Business Overview" } dashboard { columns=12 }
query: overview is order_items -> {
  where: products.category ~ $CATEGORY and sale_price ~ $MIN_SALE

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

The `#"` line above the tag is a doc comment, and it is the page's description. If you leave `title=`
off the artifact tag, it becomes the title instead, so write it as one, not as a sentence about the
page. **It belongs to a query, so it only works on the one-query form.** Putting a `#"` above a
model-level `##` tag fails the whole package load with "Object annotation not connected to any
object", so a dashboard has no description as a result.

### Where the filtering goes

A dashboard has no query, so its filtering lives on the views it names: a `+ { where: field ~ $GIVEN }`
refinement on each tile, one clause per control the tile answers to. A tile without the clause does
not move when the control does, which is how a page keeps one tile fixed while the rest filter. The
givens those clauses read are declared in the same file (step 3).

**Binding is per declaration, not per name.** Measured: a dashboard declaring its own `CATEGORY`
over a source whose model-level `where:` reads the model's `CATEGORY` compiles, shows the control,
and filters nothing when it moves, because the two declarations only share a name. So do not mix
the two designs on one given. The other design still works on its own: a source with the givens
already applied in an untagged `dashboards/_shared.malloy`, reading `import '../givens.malloy'`,
which discovery treats as a shared include rather than a dashboard. Then the dashboard imports both
and the controls render for the givens its tiles reach. A given the dashboard imports but nothing
references gets no control, silently, at reload 200 with no warning. Save the include before you
compile the dashboard that imports it, since an importer compiled against a sibling that is not on
disk fails with an `import-error`. The builder can bind such a control but not add, change or
remove it, since it never edits imports or model files.

Note `SINCE` is a `date` rather than a `filter<>`, so it compares with `>=` rather than `~`.

**`# dashboard { columns=N }` is the one spelling of the grid width**, on both forms, beside the
artifact tag. Anything else inside the artifact tag is a package warning naming it, which is what you
will see if you write `dashboard_columns=N`: nothing reads it, and the grid would otherwise fall back
to the default width in silence.

A tile keeps its view's own field names on axes and column headers. `# label` titles the tile; to label
what is inside it, label the fields in the view.

## Layout: the four tags that make a page line up

Cards and tiles share one grid, and the same four tags work on both forms: on a `# dashboard` query
the renderer reads them off each nest, and on a dashboard's `tiles=[…]` Publisher reads them off the
view each tile names. Copy this recipe and use the same count on every dashboard in the package so
they read as one product:

1. **`columns=12`** on the `# dashboard` tag. Twelve divides by 2, 3, 4 and 6, so a row is even with
   three cards or four.
2. **A `# colspan` on every card and tile, summing to 12 per row.** Four cards at 3, three at 4, two
   tiles at 6, a full-width table at 12. Omit them and every item falls to a single column, a twelfth
   of the width, which is too narrow for a line chart to draw in at all. A colspan wider than
   `columns` is clamped, and the package warnings say so.
3. **`# break` on the first tile after the cards.** Otherwise it flows into the columns left beside
   the cards and the next tile wraps. Not needed per row: once a row sums to 12 the next item wraps
   on its own.
4. **`# label="..."` on every nest, tile view, and aggregate**, including the aggregates inside a table
   nest, whose column headers are field names too. The heading is otherwise derived from the view's or
   field's name, and a wide table full of `total_sales` and `order_item_count` is the most visible
   thing between a rough page and a finished one. A view referenced **by name** is the exception: you
   can label the tile, but its own field names still reach the chart axes and the column headers, so
   label the fields in the view if you want those too.

On a dashboard, all four go on the view a tile names, which is why a thin re-declaration
(`view: revenue_trend is sales_by_month`) is the place to put them: the modelled view stays reusable
and each page decides its own widths. `# subtitle="..."` and `# borderless` go there too and are the
rest of the set; a tile reads all five exactly as a `# dashboard` nest child does.

**Do not forget `columns=` itself.** What you get without it differs by form, and neither is the page
you drew. On a dashboard it falls back to a narrow default and every colspan is clamped to it, which
is the usual reason a page you laid out comes out one item per row, and the package warnings name it.
On a `# dashboard` query there is no grid at all: the cards flow side by side at their natural widths
and every colspan is ignored. No package warning says so, because the renderer reports it at query
time. See "Losing the grid".

Then the traps:

- **A KPI row is authored differently on the two forms.** On a `# dashboard` query a top-level
  `aggregate:` measure IS the card, and nesting a `# big_value` view to get one renders it embedded,
  as full-width bars in a single tile. A dashboard has no top-level aggregates, since a tile is one
  whole result, so there a view of only measures IS the KPI row: a tile that comes back as one row
  of measures renders as big-value cards on its own (`# big_value` on the view says so explicitly;
  `# table` opts out). This is the only place the two forms need different Malloy for the same
  picture.
- **No `# size=fill` on a dashboard tile.** Inside a dashboard it measures against the container the
  whole grid was handed, not the tile, so it yields a chart thousands of pixels tall. Tiles already
  size to their colspan.
- **A KPI card's label is one line that ellipses** rather than wrapping, so a long label in a narrow
  card is truncated with no other sign. Widen the card or shorten the label.
- **A ratio needs a number format.** `order_count / customer_count` renders as `10.695` on a card;
  `# number="#,##0.0"` is the precision it actually carries.
- **A `# shape_map` legend is titled with the measure's field _name_, not its `# label`.** Rename it
  in the view: `aggregate: revenue is total_sales`. Renaming drops the measure's own format tags
  though, so `# currency` becomes plain digits unless you re-tag it at the rename. Prefer `# label=`
  anywhere the legend is not the problem.
- **A series legend sizes itself from the longer of the series label and its widest value**, then
  truncates both. A 4-character label over 4-digit years clips to `20…`; `# label="Order year"`
  instead of `"Year"` buys the room. A legend showing `…` is this, not a data problem.

The last two are upstream renderer behavior, cheap to work around in the model.

The same tags govern a `# dashboard` **view** run in a notebook cell, since both surfaces render
through the same code, so a view laid out this way looks the same in a cell as on a dashboard page.
Height is the one thing the surface decides: a one-query page renders at its natural height, a
dashboard's tiles are each capped, and in a notebook a chart cell is capped and a table cell hugs its
rows.

## The rules that actually bite

- **The filename is the dashboard's name:** its URL slug, its listing name, and its `# drill`
  target. The query inside can be called anything, and sometimes must be (a query named `regions`
  collides with an imported `regions` source).
- **A given has to be in the dashboard file's scope to be bindable**: declared there (the
  convention) or imported. Malloy's given namespace is per-file. A given the file cannot see gets
  no control and cannot be sent to it, even when a `where:` that references it lives up an import
  chain. And a given declared here does not drive a `where:` in the model: bind on the tiles.
- **A suggest's source or query has to resolve in the dashboard file too.** `suggest { source=products … }`
  means the dashboard imports `products`.
- **A model-level `##` tag must be on one line.** Wrapping one always breaks it, but how you find
  out depends on what follows. If the continuation is not valid Malloy you get a compile error. If it
  happens to be, an `import` say, the file compiles clean, quietly stops being a dashboard and
  becomes a shared include, and only the package warnings tell you. Match on the shape rather than the
  words: the message may say a tag "does not parse", or was "refused" or "dropped rather than parsed",
  and on a file that still built it opens "Annotation" rather than "Tag". See "Losing the grid" below.
- **In a `# dashboard` view, fields render by role.** A top-level `aggregate:` measure is a KPI card,
  so do not nest a `# big_value` view to get one. Each `nest:` is a tile. Give every KPI a
  `# label=`, or the card is headed `total_sales`.
- **Only table cells are marked drillable.** See "Drill".

``reference/render-gotchas.md`` covers the renderer tags in depth; ``reference/charts.md`` covers
choosing them.

## Filter controls

Controls come from the `given:` declarations the tiles reference. Declare them in the dashboard
file, which is what the builder edits; a package `givens.malloy` is for controls the data app and
notebooks share, and a dashboard importing it whole gets the same controls but cannot edit them.
The tags on the declaration are the control contract:

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

A sixth tag, `description`, is part of the contract and has two spellings that do different things:
`# description="…"` publishes to the API but Publisher's own UI does not render it, while
`#(description="…")` renders as helper text under the control but complains about any multi-word
value, that the prefix "is not a well-formed route", because a route ends at the first space. That
complaint is a **compile** diagnostic on a compile that still succeeds, not a package warning, so
step 6 will not show it. Pick by which reader you care about.

`control=select`/`multiselect` with a `suggest` renders a picker filled from the data;
`range_min`/`range_max` on a `filter<number>` renders a two-handled range slider; a `filter<date>` or
`filter<timestamp>` renders a time-range control with preset windows and a custom day range; a bare
`date` or `timestamp` renders a date picker. Which controls appear is per-dashboard, decided by which
givens the query references.
The modeling workflow and `docs/givens.md` cover givens themselves.

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
`dimension: category` seeds a given called `category` rather than a declared `given: CATEGORY`. A
`to=self` survives that, because a surface folds case when it looks up its own given. A `to=<slug>`
does not: it navigates, still looks like it worked, and arrives as `?category=…`, which the
destination drops by exact match, so you land on an unfiltered page. Nothing errors, and the lint
folds case when it checks, so it stays green too. That silence is specific to a name that folds onto
a declared given. A `to=self` whose name matches nothing at all is caught loudly and is not offered;
a `to=<slug>` is not checked either way.

A drill only lands somewhere useful if the destination declares a control for the given being
seeded. **No lint checks that.** It verifies that the target slug is a dashboard in the package, and
for `to=self` that some model declares the given, and stops there. Nothing reads the destination's
own givens, so click it and look.

Cells in a drillable **table** column show it: pointer cursor, and a blue underline on hover. They are
in the tab order and carry a button role too, so a keyboard reaches them, focus is styled the way
hover is, and Enter or Space fires the drill. Chart marks get no such affordance in either Publisher
or Malloyyo, so a dashboard meant to be drilled wants at least one untagged (table) tile. A destination the
surface cannot honor is not marked and not offered, which is why a `to=self` reads as plain text in a
document that declares no control for its given.

**One thing quietly switches the marking off, per column: another tile rendering a column with the
same header.** Put a "revenue by category" chart beside a drillable `category` table, which is the
obvious thing to build, and that column's cells stop being marked. Marking matches columns by their
rendered header text, so a name a non-drillable field also shows is dropped rather than risk painting
a dead link. Only a **non-drillable** column suppresses: a second tile that groups by the same tagged
dimension is fine, which is the arrangement the paragraph above already recommends. **Other drillable
columns on the same page keep their marking**, so counting marked cells will not tell you: look at the
column you care about. The clicks still work, so this is invisible
unless you hover. Give the two different headings with `# label=`. A transposed table is never marked
either, for a different reason.

## Losing the grid

A one-query page can come out with its layout wrong in two visibly different ways, and the reload is
200 and the manifest reports the column count you asked for in both.

**Not a dashboard at all: one plain nested table**, every `# colspan` and `# break` dropped, because a
`f'…'` filter literal in a `givens { … }` block shares a line with `# dashboard`. Put `# dashboard` on
its own line: writing it first on the line does not help, a plain `'Outerwear'` or a bare date is
fine, and a `tiles=[…]` dashboard is immune because its layout comes from the manifest rather than a
re-parse.

**A dashboard, but nothing lines up**: you wrote `# colspan` without `columns=N`, so the items flow
side by side at their natural widths instead of aligning to a grid.

To tell them apart, run the dashboard's own query, `{"queryName": "<the manifest's query>"}`, and read
`renderLogs` on the response. Like `warnings`, the key is absent when there is nothing to say.
Single-query dashboards have no `tiles` in their manifest, so there is no tile query to run:

| render log                                         | what it means                                                                                       |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Unknown render tag 'colspan'`                     | the renderer never saw a `# dashboard` tag. It does **not** say which of the two causes; check both |
| `Ignored # colspan … only applies in columns mode` | it saw the tag but there is no count                                                                |

Neither reaches the package warnings, so step 6 will not show either. A **wrapped `##` tag** is the
one failure in this family that does: the file is absent from the listing and the package warnings
say "Tag does not parse (Unclosed '{')".

## Read the lint

Package warnings after a reload are the dashboard's test suite. Fix all of them:

- `# drill … targets "x", which is not a dashboard in this package`: a dead click.
- `to=self, but no model in this package declares a given "X"`: the clicked value has nowhere to go.
- `given "X" suggests options from source "y", which this file does not define`: the dropdown will
  be empty, so import it.
- `filters by given "X", which this file does not import, so no control is shown for it`: the trap
  under "Importing a given is what makes it bindable", which the lint now names for you, with the file
  to fix.
- A tile that does not resolve to a real view; a non-positive `# dashboard { columns }`; a tile view's
  `# colspan` that is not a positive integer or is wider than the grid; and any property inside the
  artifact tag Publisher does not read, `dashboard_columns=` included.

Findings carry a `severity`, but `warn` is the ordinary default and tells you nothing about how bad
one is. Read the text, not the severity and not the count. One message is worth recognising because
it changes what the rest of the list means: **"Dashboard lint stopped early, so this list is
incomplete"**. A dashboard withheld from `explores` also loses its own findings, so a short list for a
withheld file is not a clean bill of health.

**Read the status the reload itself returns, not the listing.** One dashboard that fails to compile
fails the whole package load, and the reload answers **424** with the compile error. A package that
was already serving then keeps serving its previous version, so the listing still answers 200 and
looks perfectly healthy while your edit has silently not taken effect.

**If you did not catch the 424, `GET /api/v0/status` still knows.** A package serving an older model
than its files appears in `loadErrors` with **`stale: true`**, the compile message, and the time it
failed, and the entry clears on the next reload that compiles. That is the one check that works after
the fact, so make it the first thing you run when a page will not change. A package that never loaded
at all appears there too, without `stale`, and is absent from the listing entirely.

If the reload is 200 and the others are listed but yours is not, discovery skipped the file instead,
usually a missing or misspelled `# artifact` tag, which is the same mechanism that deliberately skips
an untagged shared include. There is a second cause if the package's `publisher.json` carries an
`explores` list: a dashboard whose file is missing from it is withheld rather than served, and the
warning says so and names the fix. The list is what matters, not the `queryableSources` setting, which
is `declared` by default; a package with no `explores` list withholds nothing. Where there is one, a
`suggest` source has to be queryable as well as resolvable, so it needs to be on the list too.

**A clean reload is not proof the tags are right.** The checks above read names and resolve them; the
separate warning for a tag that does not _parse_ is syntax only: it carries no
position and says nothing about a name that does not resolve. It catches _a_ malformed tag; its
absence is not evidence there are none. That is why the last step is opening the page, not reading
the warning list.

## What "done" means

- Every source, view, and field name came from the model you read in step 1.
- The reload returned **200**, not 424. A 424 means the page you are about to look at is the old one.
- The package reloads with **zero** dashboard warnings, and your dashboard is in the listing at all.
- **The page is a dashboard and its items line up.** This is the check the others cannot make for
  you: all of them pass both on a page that has become one plain nested table and on a dashboard whose
  items do not align. See "Losing the grid" for which you have.
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
