---
name: malloy-reports
description: "Build or modify a Malloy Publisher report: a .malloy file with an ## artifact tag, written at the package root unless asked otherwise, laid out as a grid (a dashboard, scanned at a glance) or linear (a notebook or data story, read top to bottom), with filter controls rendered from givens and # drill click-through. Use when the user asks for a dashboard, a notebook, a report, a data story, a filterable view, or drill-through between views, and no code is wanted. Legacy .malloynb files are read, not authored."
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Publisher Reports

> A `.malloy` file with an `## artifact { layout=… tiles=[…] }` tag **is** a report. It imports the model, names the views to show, binds the filtering on those views, and says how to lay them out. Publisher discovers it at package load, renders the filter controls from the givens its tiles reference, offers `# drill` click-through, and serves it at `/<env>/<pkg>/<path>`, its package path without `.malloy`. No code, no build step.

What other tools call a dashboard and a notebook are both reports here. The **layout** is the only difference:

- **`layout=grid`**: tiles across a grid behind one filter row, scanned at a glance (a dashboard). Read `reference/grid.md` before you lay one out.
- **`layout=linear`**: tiles and prose top to bottom, read in order (a notebook, a data story). Read `reference/linear.md` before you write one, and before you edit a report written as `run:` cells.

Everything else (filters, text tiles, drill, the builder, the lint, the write path) is the same for both, and switching layout later is a one-property edit.

## Where the file goes

- **Write a new report at the package root**, as `<slug>.malloy`, unless the author asks for another location. No directory is required, and none is a convention: a report can live anywhere in the package except `public/`, and the root `index.malloy` is never a report. Do not create a `dashboards/` or `notebooks/` directory for it.
- **Always write `layout=grid` or `layout=linear`** in the tag. A tag without one is read by a fallback rule, and a linear report that lists tiles without `layout=linear` is read as a grid. `kind=` is no longer read: a tag that sets it makes no report (`report-kind-removed`), so replace it with `layout=`.
- **The package path is the report's identity.** `sales/q3.malloy` is the report `sales/q3`: its URL, its `GET …/reports/{path}` address, and its `# drill` target. Two reports may share a basename in different directories.
- **Imports are relative to the report's own location.** At the root: `import { order_items } from "storefront.malloy"`. In a subdirectory: `"../storefront.malloy"`.
- **An untagged `.malloy` file is an ordinary model**, wherever it lives. Shared definitions several reports import go in one.

## When this is the right tool

| The user wants                                       | Use                                                          |
| ---------------------------------------------------- | ------------------------------------------------------------ |
| A recurring, at-a-glance view behind shared filters  | this skill, `layout=grid`                                    |
| A narrative, with prose between the numbers          | this skill, `layout=linear`                                  |
| A quick write-up of queries you already ran          | `skill:malloy-analysis-report`, which writes a linear report |
| Custom design, branding, or interactions beyond tags | an HTML data app                                             |
| The model itself: sources, measures, joins           | `malloy-model`                                               |

Interactivity is not the axis: both layouts get filter controls, URL-addressable state, Apply batching, and `# drill`. Pick on the shape of the document: scanned at a glance is grid, read top to bottom is linear. When in doubt, linear.

## Build sequence

Tool names below are bare (`get_context`); match each against the tools you actually have, since a host may prefix or rename them. A host names its compile and reload tools its own way. Where you have none, use the REST endpoint named beside the step.

1. **READ THE MODEL FIRST.** Get the real source, view, dimension, and given names from the package: `get_context` if you have it, otherwise the REST model endpoint for `index.malloy` (it answers 404 for a file off the surface) or the `.malloy` files. Never guess a name. A guessed field in a query fails the whole package load, not just that one report; a guessed tile or suggest source is quieter, and only shows up in the package warnings. If the package root holds an `index.malloy` (every scaffolded package does), a tile reads only the sources that file exports.
2. **PICK THE LAYOUT AND THE TILES.** Which views, in what order, and on a grid how wide each sits. A query tile is a `view:` you declare in the report, named as `"source -> view"`.
3. **DECLARE THE GIVENS** the report will filter by, in the report file itself, with their control tags (see "Filter controls"). The builder edits the report file and nothing else, so a filter it can add, change or remove is a declaration in that file. A given the model already declares and reads is imported by name, never declared again (see "Givens that stay in the model").
4. **COMPOSE THE FILE** at its path (the package root unless asked otherwise), following the template below, but do not save it yet. Name the sources you need in the import. Each tile binds its controls with a refinement, `view: t is v + { where: category ~ $CATEGORY }`, one clause per given it answers to; a `filter<...>` binds with `~`, a plain `date` or `number` given is a value and binds with `>=`, `<=` or `=`. Only the givens some tile references become controls. Then import every source or query any referenced given names in a `suggest`; **a `suggest` naming a `query=` needs that query's own source imported as well**, because an import is not transitive.
5. **COMPILE IT** with your compile tool (`POST …/models/<path>/compile` over REST) at the path the file will have, with **`"scope": "file"`**: a report opens with an `import`, which the default `append` scope refuses, and an existing file compiled at `append` collides with itself. **`"scope": "file"` does not check tiles, drills or suggest queries.** To see those before you save, compile the same text at `"scope": "package"` with `modelPath` set to the report's path: it runs the lint step 6 reads and returns its findings with code `dashboard-lint` (or `render-tag`). Editing an untagged model several reports import also wants `"scope": "package"`, which recompiles every file as saved.
6. **SAVE IT, RELOAD, AND READ THE MANIFEST AND THE WARNINGS.** Save by writing the file, or with `PUT …/models/<path>` over REST (omit `expectedHash` to create; it creates at any new path and replaces only a file the package already serves as a report). Then your reload tool, or `GET …/packages/<pkg>?reload=true`. A 424 means the package did not load and your edit is not live. **The `warnings` key is absent when there are none.** Then read `GET …/packages/<pkg>/reports/<path>`: its `givens` are exactly the controls that will render, and its `layout` is the layout Publisher read. See "Read the lint".
7. **OPEN IT AND LOOK.** Not optional; see "What done means".

## The form

```malloy
##! experimental.givens
##" One line saying what this report is for.
##| artifact { layout=grid title="Storefront overview"
  tiles=[
    intro { kind=text colspan=12 },
    "overview -> kpis",
    "overview -> revenue_trend",
    "overview -> revenue_by_state"
  ]
} dashboard { columns=12 }
|##
import { order_items, products } from "storefront.malloy"

# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''
# label="Ordered since"
given: SINCE :: date is @2023-01-01

##|(markdown) intro
Revenue and margin for the selected category.
|##

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

The same file with `layout=linear` and no `dashboard { … }` or `colspan` is a linear report.

- **The tag is model-level**, one line (`## artifact { … }`) or a `##|` … `|##` block with the same text inside, the form to use once the list is long: one tile per line, `}` on its own line, `|##` closing at the opener's column. A one-line `##` tag must stay on one line: wrapping it is a compile error, or worse, compiles clean and quietly stops being a report.
- **The header** above the tag holds only `##!` flags, `//` comments and the unnamed `"` notes that are the report's description. A statement or a `#` tag above `## artifact` is an error.
- **Quote every query entry.** `"overview -> kpis"` is a string; unquoted, the tag does not parse (`notebook-artifact-unparsed`). A bare quoted name (`"top_products"`) names a model-level `query:` in the file.
- **A query tile is a thin `view:` on an extension the report declares** (`source: overview is order_items extend { … }`): the modelled view keeps its chart tag, the wrapper carries the tile's `# label`, its width on a grid, and its binding.
- **A text tile is a `##|(markdown) name` block** listed as `name { kind=text }`. The name is one bare word on the opener line, the body starts on the next line (a heading goes inside the block), and `|##` closes it at the opener's column. A bare `intro` without `{ kind=text }` is the query-tile form for a `query:`, so the block is not shown. On a grid the entry also takes `colspan` and `break`, and nothing else.
- **File order:** header, imports and givens, the prose blocks in tile order, then the extension that defines the tile views. That is the order the builder writes.
- **A `run:` in a report that lists `tiles` is never shown** (`notebook-layout-run`). Define a view, list it, delete the `run:`.
- **A tile expression is a string, so the compiler never checks it.** Rename a view and the file still compiles; the tile fails at package load, where the lint names it.
- **Chart tags go on the view** (`# line_chart`, `# bar_chart`, `# big_value`, `# label="…"`). `skill:malloy-charts` covers choosing them.

## Filter controls

Controls come from the `given:` declarations the tiles reference. The tags on the declaration are the control contract:

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

`control=select`/`multiselect` with a `suggest` renders a picker filled from the data; `range_min`/`range_max` on a `filter<number>` renders a two-handled range slider; a `filter<date>` or `filter<timestamp>` renders a time-range control with preset windows and a custom day range; a bare `date` or `timestamp` renders a date picker; a `filter<string>` with no tag renders a text box taking filter syntax. Which controls appear is per report, decided by which givens its tiles reference.

- `#(description="…")` renders as helper text under the control and draws a cosmetic "not a well-formed route" compile diagnostic for a multi-word value; `# description="…"` publishes to the API but the UI does not render it.
- `dimension=` can name a field of a directly joined source, quoted: `suggest { source=sales dimension="products.department" }`. On the `query=` form, `dimension` names a column of that query's output; match it to the query's `group_by`.
- **A suggest's source or query has to resolve in the report file**, so import it there. Getting it wrong does not error: the control still looks like a picker and says "Could not load the options for this control"; the package warnings name it.
- Two options on the artifact tag, either layout: `autorun=false` batches control changes behind an Apply button; `givens { CATEGORY=f'Outerwear' }` sets starting values (a URL parameter wins). A file-level `## autorun=false` does nothing on a `.malloy` report.

### Binding, and givens that stay in the model

**Binding is per declaration, not per name.** A report declaring its own `CATEGORY` over a source whose model-level `where:` reads the model's `CATEGORY` compiles, shows the control, and filters nothing. So bind on the tiles, with `+ { where: … }`. The other design, model-level scoping, still works: put a scoped source in an untagged model that imports the givens it reads, and import that model and those givens into the report. Do not mix the two on one given.

A given stays in the model, declared once there, when a model source, view or measure, an `#(authorize)` or `#(access_filter)` gate, or an HTML data app reads it. The report imports it by name (`import { CATEGORY } from "givens.malloy"`) and never re-declares it:

- **Import a name and also declare it: a compile error.** Drop the local declaration.
- **Declare a name the model already reads, without importing it: no error at all.** The control renders and moves, and the model's own `where:` or gate never sees the value.

A given has to be in the report file's own scope to be bindable: declared there or imported. Never add a `#(filter)` annotation: it is deprecated, a report renders no control for it, and every use has a `given:` form (`malloy-model` covers the migration). `malloy-model` and `docs/givens.md` cover givens themselves.

## Drill

`# drill` goes on a model **dimension**, never on a report:

```malloy
# drill { to=["category", "self"] given=CATEGORY }
dimension: category is products.category

# drill { to="regions/west" given=REGION }
dimension: region is regions.region
```

- **`to=<path>`** navigates to a report, of either layout, by its package path without `.malloy`, read from the package root: `to=category` is `category.malloy` at the root, `to="regions/west"` is `regions/west.malloy`. Quote a path with a `/`. **Always write the path from the package root**: a bare name is a root file, never a report of that name elsewhere.
- **`to=self`** filters in place; two or more destinations pop a menu.
- **Always write `given=`.** Without it the given is the dimension name verbatim, so `dimension: category` seeds `category`, not `CATEGORY`. `to=self` survives that (it folds case); `to=<path>` does not, and lands on an unfiltered page with nothing erroring.
- **The destination needs a control for the given**, declared as `filter<T>`. No lint checks that half, so click it and look.
- **Only table cells are marked drillable.** A report meant to be drilled wants at least one untagged (table) tile, grouped by the tagged dimension. A column whose rendered header another, non-drillable tile also shows loses its marking: give the two different `# label=`s.

## Read the lint

Package warnings after a reload are the report's test suite. Fix all of them, and read the text rather than the severity or the count. **"Dashboard lint stopped early, so this list is incomplete"** means the rest of the list is partial.

| Finding                                                                       | Fix                                                                                                                               |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `# drill … targets "x", which is not a report in this package`                | A dead click. Write the target's path                                                                                             |
| `to=self, but no model in this package declares a given "X"`                  | The clicked value has nowhere to go. Declare the given, or fix `given=`                                                           |
| `given "X" suggests options from source "y", which this file does not define` | Import `y` into the report                                                                                                        |
| `filters by given "X", which this file does not import`                       | No control is shown for it. Import the given                                                                                      |
| A tile that does not resolve to a real view                                   | Fix the view name, or declare the view                                                                                            |
| `notebook-artifact-unparsed`                                                  | The tag does not parse, usually an unquoted `source -> view` entry                                                                |
| `notebook-layout-run`                                                         | A `run:` beside `tiles`. Make it a view, list it, delete the `run:`                                                               |
| `notebook-tile-layout-ignored`                                                | `colspan` or `break` on a linear report. Remove them, or switch to `layout=grid`                                                  |
| `report-kind-removed`                                                         | The tag sets `kind=`, which is no longer read, so the file is no report. Write the `layout=` the finding gives                    |
| `report-layout-implicit`                                                      | A tag with no tiles and no `layout=`. Add `layout=`                                                                               |
| `report-on-surface`                                                           | A model `index.malloy` imports (or `explores` lists) is tagged, so its sources left the surface. Untag it, or move the report out |
| `index-is-not-a-report`                                                       | The root `index.malloy` carries a tag. Remove it; put the report in its own file                                                  |
| `report-name-ambiguous`                                                       | Two grid reports share a file name, so the deprecated by-name dashboards route answers 409 for it. Rename one of the files        |
| `Tile … reads <source>, which index.malloy doesn't export`                    | Add the source to the `export { ... }` in `index.malloy`. Never add an `explores` to `publisher.json`                             |
| A grid's `columns`, `colspan`, or `dashboard_columns=` findings               | `reference/grid.md`                                                                                                               |
| A statement or tag above the artifact tag; prose the reader cannot place      | `reference/linear.md`, the cell rules                                                                                             |

**Read the status the reload itself returns, not the listing.** One report that fails to compile fails the whole package load (424), and a package already serving keeps its previous version, so the listing still looks healthy. `GET /api/v0/status` still knows: a package serving an older model than its files appears in `loadErrors` with `stale: true`. If the reload is 200 and your report is not listed, discovery skipped it: usually a missing or misspelled artifact tag, or a file under `public/`.

**A clean reload is not proof the tags are right.** The parse warning is syntax only and carries no position. That is why the last step is opening the page.

## Template

```malloy
##! experimental.givens
##" [One-line description of the report.]
##| artifact { layout=linear title="[Title]"
  tiles=[
    intro { kind=text },
    "main_source_tiles -> broad_tile",
    "main_source_tiles -> drill_tile"
  ]
}
|##
import { main_source } from "model.malloy"

##|(markdown) intro
[Framing question: what are we trying to understand?]
|##

source: main_source_tiles is main_source extend {
  view: broad_tile is [broad view]
  view: drill_tile is [drill view]
}
```

For a grid, write `layout=grid`, add `dashboard { columns=12 }` after the tag's closing `}`, and give every tile view a `# colspan` (`reference/grid.md`).

## Editing in the Console

A person can open any report in the Console's builder (**Edit** on the report page; **New report** on the package page writes `<slug>.malloy` at the package root). The builder splices the file rather than regenerating it, so comments and anything it does not model survive; its layout switch rewrites `layout=` and nothing else. A report written as `run:` cells opens converted to tiles, and the first Save asks before it rewrites the file (`reference/linear.md`). A save whose text declares a real `#(authorize)` or `#(access_filter)` gate outside prose is refused with a 400: gates live in a model file the report imports.

## What "done" means

- The file is at the package root (or where the author asked), and its tag says `layout=`.
- Every source, view, and field name came from the model you read in step 1.
- The reload returned **200**, not 424, with **zero** report warnings, and your report is in `GET …/reports`.
- You opened the page and every tile shows real numbers: not stuck loading, not an error, not an empty state you did not intend.
- Each control renders as the widget you intended, and changing one changes the numbers.
- If you added a `# drill`, its `to=` is a path, and you clicked it and landed where you meant to, with the given seeded.
- On a grid: every tile carries a colspan, each row sums to `columns`, rows end flush, nothing is clipped, and no legend or card label ends in `…`.

## Reference

- `reference/grid.md`: the grid recipe, its traps, and one query as the whole page.
- `reference/linear.md`: linear reports, `run:` cells, legacy `.malloynb`, style, and common mistakes.
- `docs/reports.md`: the full guide this skill condenses.
- `docs/givens.md`: the givens the controls are generated from.
