<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The data app kit

> Every data app starts from this skill's `assets/data-app/`: a kit of blocks (a subject with its figures, ranked
> lists, shares, lines over time, tables, grids, watchlists, entity pages) and a worked example app. You write one
> config and the pages; the kit draws them to one design (`reference/data-app-design.md`), keeps the state
> in the address, makes every chart clickable and every section explain itself.

## What is in `assets/data-app/`

| File | What it is | Copy into `public/` |
|---|---|---|
| `kit/` | The kit: `kit.js` (import everything from it), its modules, `kit.css`, `themes.css` (nine themes, light and dark), `charts.js`/`charts.css` (more chart forms), `fonts.css` (Geist, inlined) and `fonts/` (each theme's display face, loaded only by its theme), `vendor/` (d3, topojson-client, the font licences) | Whole; never edit, copy again to take a fix |
| `index.html` | The page: links the kit's styles, loads `/sdk/publisher.js` and `app.js` | Once, then it is the app's own |
| `app.js` | A worked data app over an online store's orders: three pages and three entity pages | Once, then rewrite it for your data |

`kit/themes.css` and the fonts are written by this skill's `scripts/themes.py`, which pins each font by its
address and checksum. A theme is added or changed there, in the skill, never in an app's copy.

Run the copy from the package's own directory, the one holding `publisher.json`. `SKILL_DIR` is the directory you
read this skill's `SKILL.md` from, wherever your host installed it:

```sh
T="$SKILL_DIR/assets/data-app"
test -f publisher.json || { echo "run this from the package directory, the one with publisher.json"; exit 1; }
test -d "$T/kit" || { echo "assets/data-app is not on disk: build from the minimal scaffold in SKILL.md"; exit 1; }
mkdir -p public && rm -rf public/kit && cp -R "$T/kit" public/kit
for f in index.html app.js; do [ -e "public/$f" ] || cp "$T/$f" public/; done
chmod -R u+w public
```

An app built earlier carries its kit's version as `KIT_VERSION`, exported from `public/kit/kit.js`, and on the
running page (`<meta name="data-app-kit">`). Copying a newer kit over it is meant to be safe within one major
version; across one, compare the app's config with the new kit's worked `app.js` before copying.

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

A data app over a warehouse table, or a file over about 100,000 rows, is materialized: its source stored as a table
and the figures it adds up kept as rollups, so blocks answer from them. Read `skill:malloy-materialization` first:
it says which spelling the server reads (`#(materialize)` and `#(rollup)`, or `#@ persist` and `#@ preaggregate`
where the server's materialization switch is off and the newer tags would be ignored), and that a source with a row
gate (`#(authorize)`, `#(access_filter)`) is built past its gate and must not be stored where every caller reads it.
A small file in the package needs neither. What the kit adds to that skill:

```malloy
import "ecommerce.malloy"

#(doc) Order lines for the sales app, with the dimensions it filters by stored beside each line.
#(materialize)
source: sales is order_items extend {
  dimension:
    order_date is created_at.day                       // the app's date: a day, so a rollup can filter on it
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

- **The app's source lives in the model**, not in the config's `extend`, which a rollup cannot read, and the config
  names it (`source: 'sales'`).
- **The dimensions the app filters, ranks and splits by are the source's own**, and the config names them
  (`field: 'category'`, not `inventory_items.product_category`); the date is a day dimension
  (`date: { field: 'order_date' }`).
- **The rollup's grain is the date and the four to seven dimensions the app filters by most**; leave out one with
  thousands of values (a brand, a customer), which makes the rollup nearly as big as the source.
- **The kit asks for a period and its comparison as two queries**, each with its dates in its own `where`, because a
  rollup answers those and never a measure that carries its own filter; a block of your own does the same
  (`S.kpiQueries`, `S.breakdownCompareQuery` with `S.withCompare`).
- **Check it** on a ranked list's info mark: it names the summary that answered, its grain, and how fresh its rows
  are. "Worked out from the raw data just now" means the source was not stored or the query missed the rollup.

## The config

```js
import { dataApp, subject, rank, share, trend, table, heat, watch, entityHead, pageHead, aboutPage, blocks, block, S, F, C } from './kit/kit.js'

dataApp({
  title: 'Sales performance', sub: 'Online store',          // the masthead; keep `sub` to a few words
  model: 'sales.malloy', source: 'sales',                    // a materialized source (above)
  extend: '',                                                // measures the model lacks; a rollup cannot read these
  date: { field: 'order_date', label: 'Order date' },        // a day dimension, or null
  recordNoun: 'order lines',                                 // what a row is, for counts in menus
  theme: 'bloom',                                            // from the subject: reference/data-app-design.md
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
  `#/<page id>/<value>` and not listed in the navigation; every query on it is scoped to the entity. `lists: 'category'`
  marks the page that lists an entity, which its pages' breadcrumbs return to. `period: false` and `filters: false`
  hide those controls on a page they do not apply to, such as the About page.
- **`defaults.filters`**: values a reader starts filtered to, when one slice is the whole story by default (regular
  gasoline in the US as a whole, `{ grade: ['Regular'], area: ['United States'] }`). A reader can clear them, and a shared
  link keeps what they chose. Copy each value from a query's rows: a value the data does not have is dropped, and
  `?check=1` fails on it.
- **`sources`**: where the data comes from (`name`, `by`, `what`, `license`, `url`). Every block's info mark lists
  them, or the ones it names in its own `sources: ['eia']`; the footer credits them; `aboutPage()` lists them with
  their licences. An app on public data has `sources` and an About page.
- **`search`**: an entity a reader looks up by name more often than they browse to it (a customer, a hospital, a
  ZIP code). The masthead gets a search box over its names; picking one opens its page.
- **`articles`** (`{ title, outlet, date, url }`): the stories an app made from the news responds to. `news()` lists
  them as a section, and `aboutPage()` lists them too. Never quote their text.
- **`credit`** (`{ name, logo }`): "Made by" and who made the app in the masthead, their logo when the app ships one
  as an image (`logo: './logo.svg'`), else their name.
- **`glossary`**: the words a newcomer may not know (`term`, the words that mean it in `match`, `plain`). An info
  mark explains the ones its own text uses.
- **Periods** (`defaults.period`): `last_7`, `last_28`, `last_91`, `last_364` (days counted back from the data's
  latest day, so weeks are whole and line up with the comparison), `mtd`, `qtd`, `ytd`, `last_month`,
  `last_quarter`, `last_year`, `all`. **Compare**: `previous`, `year`, `none`. Pick the default from how often the
  reader opens it: weekly readers `last_91`, daily ones `last_28`.

Two things come from the page that frames the app, not the config:

- **Light and dark.** A host tells the page the reader's mode (`publisher:color-scheme`, through `/sdk/publisher.js`);
  the kit follows it and answers that it drew it. Publisher's own viewer sends it, and so may any host.
- **Asking the host.** `setHost({ name, ask(prompt, context) })` registers a host that takes a reader's question;
  every block's info mark then offers "Ask <name> about this", with what the block covers and its queries in
  `context`. Register it only where the app knows it runs inside that host; the kit knows no host of its own.

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
give every data app at least one block of your own with the form native to its field, and do not copy a plan
section for section (`reference/data-app-design.md`, "Make it about this subject").

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
  near the screen, so on a short window it waits for a scroll: scroll to the bottom first, then wait on it. In a host
  frame as tall as its content, the host page is what scrolls.
- No console errors, no horizontal scroll at 390, layout shift under 0.1 (`reference/performance.md`). Measure layout
  shift on a cold load left still, before any scroll: a scroll while blocks load counts their arrival as shift.
  Then scroll to the bottom to load the rest and check those blocks drew.
