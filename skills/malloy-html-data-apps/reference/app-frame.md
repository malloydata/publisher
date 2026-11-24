<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The dashboard kit

> Every data app starts from this skill's `assets/data-app/`: a kit of blocks (a subject with its figures, ranked
> lists, shares, lines over time, tables, grids, watchlists, entity pages) and a worked example app. You write one
> config and the pages; the kit draws them to Credible's design (`reference/dashboard-design.md`), keeps the state
> in the address, makes every chart clickable and every section explain itself.

## What is in `assets/data-app/`

| File | What it is | Copy into `public/` |
|---|---|---|
| `kit/` | The kit: `kit.js` (import everything from it), its modules, `kit.css`, `themes.css` (nine themes, light and dark), `charts.js`/`charts.css` (more chart forms), `fonts.css` (Geist, inlined) and `fonts/` (each theme's display face, loaded only by its theme), `vendor/` (d3, topojson-client, the font licences) | Whole; never edit, copy again to take a fix |
| `index.html` | The page: links the kit's styles, loads `/sdk/publisher.js` and `app.js` | Once, then it is the app's own |
| `app.js` | A worked dashboard over an online store's orders: three pages and three entity pages | Once, then rewrite it for your data |
| `themes.py` | Writes and checks `kit/themes.css`; where a theme is added or changed | Never: run it from here |

Run the copy from the package's own directory, the one holding `publisher.json`. The first line finds where the
skill is installed: in this directory or one above it (a workspace's `.claude/skills` or `.agents/skills`), else in
your home directory:

```sh
T=$(d=$PWD; while :; do for s in "$d/.claude/skills" "$d/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done; [ "$d" = / ] && break; d=$(dirname "$d"); done; for s in "$HOME/.claude/skills" "$HOME/.agents/skills"; do [ -d "$s/malloy-html-data-apps/assets/data-app" ] && { echo "$s/malloy-html-data-apps/assets/data-app"; exit; }; done)
test -f publisher.json || { echo "run this from the package directory, the one with publisher.json"; exit 1; }
test -n "$T" || { echo "assets/data-app is not on disk: build from the minimal scaffold in SKILL.md"; exit 1; }
mkdir -p public && rm -rf public/kit && cp -R "$T/kit" public/kit
for f in index.html app.js; do [ -e "public/$f" ] || cp "$T/$f" public/; done
chmod -R u+w public
```

When the skill reached you only as text, `assets/` is not on disk: say so, and build from the minimal scaffold in
`SKILL.md` to the same design.

## The model the kit needs

Read the model first and query it. The kit asks for:

- **One source at record grain** (one row per order line, flight, claim, pull request), with its joins. Every query
  starts from it. Name it in `source`.
- **A measure for every number on the page.** Use the model's; when you need one it lacks (net of returns, a rate),
  define it in `extend` (`measure: net_sales is total_sales { where: status != 'Cancelled' }`) and give it a `doc`
  in `metrics`. Ratios must be measures (`a / nullif(b, 0)`) so the kit can filter them to a period. Check a measure
  against the raw values before it leads a page (`reference/design.md`, the inventory).
- **The date** every period filters on (`date: { field, label }`), a date or timestamp dimension. Data with no time
  dimension sets `date: null`: the period controls go, the subject stands alone, and `subject({ by })` ranks it.
- **The entities**, each a dimension whose values a reader wants to open one at a time.

A package with an `index.malloy` at its root answers queries only from the sources that file exports. Put the app's
source in its own file, export it from `index.malloy`, and point the config's `model` at `index.malloy`.

Query the model once for each measure and dimension you plan to use, and look at the rows.

## Make it fast: store the source, roll up what it adds

Every dashboard over a warehouse table, or a file over about 100,000 rows, is materialized: Credible stores the
app's source as a table and keeps rollups of the figures it adds up, and each block's info mark says which one
answered and how it was built ("a summary Credible keeps of the order lines: one row for each category, channel and
day, with net revenue already added up, last built 24 November"). A small file in the package needs neither.

Write the app's source in the model, not in the config's `extend`, which a rollup cannot read:

```malloy
import "ecommerce.malloy"

#(doc) Order lines for the sales dashboard, with the dimensions it filters by stored beside each line.
#(materialize)
source: sales is order_items extend {
  dimension:
    order_date is created_at.day                       // the dashboard's date: a day, so a rollup can filter on it
    category is inventory_items.product_category       // what it filters and ranks by, as the source's own fields
    channel is users.traffic_source
  measure:
    #(rollup) grain=["order_date", "category", "channel", "status"]
    net_sales is total_sales { where: status != 'Cancelled' and status != 'Returned' }
    #(rollup) grain=["order_date", "category", "channel", "status"]
    items is count()
    #(rollup) grain=["order_date", "category", "channel", "status"]
    returned is count() { where: status = 'Returned' }
    #(rollup) grain=["order_date", "category", "channel", "status"]
    return_rate is returned / nullif(items, 0)        // a ratio of rolled-up sums rolls up when it says so too
    aov is net_sales / nullif(order_count, 0)          // a distinct count does not roll up: from the stored source
}
```

- **`#(materialize)` on the source** the app queries (`source: 'sales'` in the config). Everything is answered from
  its table at least.
- **The dimensions the app filters, ranks and splits by are the source's own**, and the config names them
  (`field: 'category'`, not `inventory_items.product_category`); the date is a day dimension
  (`date: { field: 'order_date' }`).
- **`#(rollup) grain=[…]` on every measure that sums or counts, and on ratios of them.** The grain is the date and
  the four to seven dimensions the app filters by most; leave out one with thousands of values (a brand, a
  customer), which makes the rollup nearly as big as the source.
- **No rollup on a distinct count** (`count(order_id)`) **or on a measure that reads a joined column**
  (`inventory_items.cost.sum()`): Publisher refuses it (`join_not_in_grain`), and the measure is answered from the
  stored source, which is still fast.
- **Build it and check it.** On Credible, publishing builds both. Locally, `POST …/packages/{pkg}/materializations`,
  then `GET …/packages/{pkg}/status`: every `serveVerdicts` entry says `serves`. Then open a ranked list's info mark:
  it should name the summary. The kit asks for a period and its comparison as two queries, each with the dates in
  its own `where`, because a rollup answers those and never a measure that carries its own filter; a block of your
  own should do the same (`S.kpiQueries`, `S.breakdownCompareQuery` with `S.withCompare`).

## The config

```js
import { dashboard, subject, rank, share, trend, table, heat, watch, entityHead, pageHead, aboutPage, blocks, block, S, F, C } from './kit/kit.js'

dashboard({
  title: 'Sales performance', sub: 'Online store',          // the masthead; keep `sub` to a few words
  model: 'sales.malloy', source: 'sales',                    // a materialized source (above)
  extend: '',                                                // measures the model lacks; a rollup cannot read these
  date: { field: 'order_date', label: 'Order date' },        // a day dimension, or null
  recordNoun: 'order lines',                                 // what a row is, for counts in menus
  theme: 'bloom',                                            // from the subject: reference/dashboard-design.md
  metrics: {
    net_sales: { label: 'Net revenue', format: 'usd', good: 'up', doc: 'What customers paid, less cancellations.' },
    return_rate: { label: 'Return rate', format: 'percent', good: 'down' },
    on_time_rate: { label: 'On-time rate', format: 'percent', good: 'up', thresholds: { watch: 0.8, breach: 0.7 } },
    flight_count: { label: 'Flights', format: 'number', good: null },
  },
  filters: [{ key: 'channel', field: 'channel', label: 'Channel', plural: 'channels' }],
  entities: {
    category: { field: 'category', label: 'Category', plural: 'categories' },
    customer: { field: 'user_id', label: 'Customer', plural: 'customers', nameField: 'users.full_name' },
  },
  defaults: { period: 'last_91', compare: 'previous' },
  sources: {                                                 // where the data comes from; on every block's back
    store: { name: 'Store orders', by: "The shop's order system", what: 'Every order line since the store opened.' },
  },
  glossary: [{ term: 'Net revenue', match: ['net'], plain: 'What customers paid, less cancellations and returns.' }],
  search: 'customer',                                        // find an entity by name from the masthead
  pages: [
    { id: 'overview', title: 'Overview', render: overview },
    { id: 'products', title: 'Products', lists: 'category', render: products },
    { id: 'about', title: 'About the data', period: false, filters: false, render: () => aboutPage({ lede: '…' }) },
    { id: 'category', title: 'Category', entity: 'category', render: category },
  ],
  footer: 'Where the data comes from, in one line.',
})
```

- **`metrics`**: every measure a block shows. `format`: `usd` (or any currency code in lower case), `number`,
  `percent` (a fraction), `decimal`, or `{ unit: 'min' }`; copy it from the model's tag (`# currency=usd` is `usd`,
  `# percent` is `percent`). `good`: `'up'`, `'down'` or `null`, which colours every change. `thresholds` give a
  monitor its statuses. `doc` is what "How it's made" says the number means, when the model has no `#(doc)` for it.
- **`filters`**: the fields a reader can filter by, offered in the Filter menu with their values and counts: the
  500 most common, or `limit` of them (`{ key, field, label, limit: 2000 }`) when a reader needs the rare ones.
- **`entities`**: things with a page. An entity is also a filter, so its values are known. For an entity keyed by an
  id (a hospital's provider number), `nameField` names the dimension holding what to call it: the kit loads the
  names with the ids and uses them in every list, chip and title. `name: (id) => text` formats an id that needs no
  lookup.
- **`pages`**: each `render(state)` returns its blocks. A page with `entity` is that entity's page, reached at
  `#/<id>/<value>` and not listed in the navigation; every query on it is scoped to the entity. `lists: 'category'`
  marks the page that lists an entity, which its pages' breadcrumbs return to. `period: false` and `filters: false`
  hide those controls on a page they do not apply to, such as the About page.
- **`defaults.filters`**: values a reader starts filtered to, when one slice is the whole story by default (regular
  gasoline in the US as a whole, `{ grade: ['Regular'], area: ['U.S.'] }`). A reader can clear them, and a shared
  link keeps what they chose.
- **`sources`**: where the data comes from (`name`, `by`, `what`, `license`, `url`). Every block's info mark lists
  them, or the ones it names in its own `sources: ['eia']`; the footer credits them; `aboutPage()` lists them with
  their licences. An app on public data has `sources` and an About page.
- **`search`**: an entity a reader looks up by name more often than they browse to it (a customer, a hospital, a
  ZIP code). The masthead gets a search box over its names; picking one opens its page.
- **`articles`** (`{ title, outlet, date, url }`): the stories an app made from the news responds to. `news()` lists
  them as a section, and `aboutPage()` lists them too. Never quote their text.
- **`credit: true`** puts "Made by" and Credible's logo in the masthead, for an app Credible itself publishes.
- **`glossary`**: the words a newcomer may not know (`term`, the words that mean it in `match`, `plain`). An info
  mark explains the ones its own text uses.
- **Periods** (`defaults.period`): `last_7`, `last_28`, `last_91`, `last_364` (days counted back from the data's
  latest day, so weeks are whole and line up with the comparison), `mtd`, `qtd`, `ytd`, `last_month`,
  `last_quarter`, `last_year`, `all`. **Compare**: `previous`, `year`, `none`. Pick the default from how often the
  reader opens it: weekly readers `last_91`, daily ones `last_28`.

## The blocks

Shares of a total, in the kit's headlines and on entity pages, are said only for metrics that add up (sums and
counts); the kit checks, so an average or a rate never reads as "180% of the total".

Every block takes `kicker` (what is measured; the kit writes one when you leave it out), `headline` (a function of
its rows returning a sentence or `prose()` parts; the kit writes a standard one; `null` for none), and `span` (12,
the full width, or 6, half). Every block loads as it nears the screen, holds its height while loading, says when
nothing matches, and has an info mark that turns it over to "How it's made".

| Block | What it draws | A click |
|---|---|---|
| `subject({ metrics, by, layout })` | The first metric (or the one the reader picked) large, its change, a sentence, and its line over the period with the comparison; the rest in a ledger under it. `layout`: `split` (default), `band` (on a dark plane, for a monitor) or `board` (every figure large in a row, for a scorecard). With no date, `by` ranks it beside the figure | A ledger figure becomes the subject; a point zooms the period to it; a drag zooms to a range |
| `rank({ field \| entity, metric, top, extra, order })` | A ranked list: name, a thin bar over a pale one for the comparison, value, change. `extra` adds metric columns. `order` sorts by another expression (`'on_time_rate asc'`) | Opens the entity's page, or filters to the value |
| `share({ field \| entity, metric, top })` | One whole split into parts: a segmented bar, then each part's share, value and change | As rank |
| `trend({ metric, by, top })` | The metric over time; with `by`, one line per top value, the leader in the accent | A point zooms; with `by`, a line opens or filters |
| `table({ kicker, query, columns, open, sort })` | Dense rows, sortable, searchable, 25 a page, CSV of all. `columns`: `{ key, label, metric \| format, bar, status }`. `open: { entity, key }` makes rows open that entity's page | Opens the row's entity |
| `heat({ rows, cols, metric, volume, min, period })` | A metric across two dimensions (weekday by hour). `rows`/`cols`: `{ expr, label, name, count }`. `period: false` spans the whole data (year by week of year) | Filters to the row's value when it is a filter |
| `watch({ field \| entity, metric, volume, min, extra, limit })` | A monitor's list: each value's status against the metric's thresholds, the worst first, values under `min` of `volume` counted and left out | As rank |
| `entityHead({ metric })` | An entity page's top: breadcrumb, name, and its rank among its peers by the metric, its share and its change | (Breadcrumb back) |
| `pageHead(title, lede)` | A page's title and a sentence under it | |
| `aboutPage({ title, lede, notes })` | Where the data comes from, each source with its licence, then `notes` (`{ title, text }`: what was reshaped, what is left out), then how fresh it is | |
| `news()` | The config's `articles`, each linked with its outlet and date | Opens the article |
| `blocks(...)` | Lays blocks out: span-12 ones take the row, span-6 ones pair | |

`rank`, `share`, `trend`, `watch` and `heat` take `where`, a Malloy condition that narrows that block alone
(`where: "area_type = 'State'"` ranks the states and leaves out the regions beside them).

`S.run(view, state, opts)` writes a query over the source with the state's period and filters: `view` is a view the
model names or a block in braces. `S.breakdownQuery`, `S.kpiQuery` and `S.seriesQuery` are what the blocks use.

### Page plans, as examples of a shape

These are shapes that have worked, not templates. Choose sections from what the data shows and the reader needs,
give every dashboard at least one block of your own with the form native to its field, and do not copy a plan
section for section (`reference/dashboard-design.md`, "Make it about this subject").

- **Overview (performance)**: `subject` with 4 to 6 metrics; `rank` of the main entity and `share` of how it splits,
  side by side; `rank` of a second entity and of a place, side by side; `trend` of the subject by the main entity.
- **Monitor**: `subject` with `layout: 'band'` and the status metrics (they carry thresholds); `watch` for each entity that can breach,
  side by side; `heat` of when it happens; `trend` of the status metric by the entity.
- **An area's page** (`lists: '<entity>'`): `pageHead`; a full-width `rank` of every entity with `extra` columns;
  two `rank`s by the other metrics that matter, side by side; a `table` of the rows below.
- **An entity's page**: `entityHead`; `subject` scoped to it; `rank`s of the entities inside it (a category's brands)
  and a `share` of how it splits; a `table` of its rows that opens the next level down.

## A block of your own

When a question needs a form the blocks do not have (a dumbbell, a slope, a bump chart, a map), use `block()` with
a chart from `charts.js` (`reference/charts.md` lists them, `reference/maps.md` the maps). It must say what it shows
and it must do something when clicked:

```js
block({
  kicker: 'Margin against volume by category', span: 12, reserve: 340, metrics: ['margin_pct', 'net_sales'],
  what: 'Each category placed by its sales and its margin, so the big low-margin ones stand out.',
  queries: (s) => [S.breakdownQuery('inventory_items.product_category', ['net_sales', 'margin_pct'], s, { top: 30 })],
  draw: ([rows], body) => C.scatterChange(body.appendChild(document.createElement('div')), rows.map((r) => ({ id: r.product_category, label: r.product_category, x: r.net_sales, y: r.margin_pct })), {
    onPick: (id) => store.open('category', id),   // every chart does something when clicked
  }),
  headline: ([rows]) => [/* a sentence from the rows */],
})
```

`block()` takes `kicker`, `what`, `metrics`, `queries(state)`, `draw(results, body, state)`, `headline(results,
state)`, `of(results)` (a few words after the kicker), `span`, `reserve` (its height while loading), `ignore`
(filter keys its queries leave out), `compare: true` when it shows the comparison period (so "How it's made" says
what it is compared with) and `period: false` when its queries ignore the period. Import `store` from the kit for `store.open(page, id)`,
`store.toggleFilter(key, value)` and `store.setPeriod(...)`.

## Before you call it done

Serve it and look at every page, at 1280 and 390 wide, light and dark (`?mode=dark`):

- Open each page from its address alone, and an entity page for a real value and for a value that does not exist.
- Click through every path in the plan: a row to its page, that page's row to the next, the breadcrumb back. Pick a
  filter, change the period, make another figure the subject: every block redraws and the address keeps it.
- Open "How it's made" on a block of each kind: the definitions are filled and the query runs.
- `?check=1` puts a banner on the page saying whether any block failed or shows `undefined`, `NaN` or `null`.
- `body[data-ready="true"]` is set once every block on the page has drawn. Blocks below the fold load only as they
  near the screen, so on a short window it waits for a scroll: scroll to the bottom first, then wait on it. Inside
  Credible's frame the host page is what scrolls.
- No console errors, no horizontal scroll at 390, layout shift under 0.1 (`reference/performance.md`). Measure layout
  shift on a cold load left still, before any scroll: a scroll while blocks load counts their arrival as shift.
  Then scroll to the bottom to load the rest and check those blocks drew.
