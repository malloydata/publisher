<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->
# How a Credible dashboard looks and works, and why

> Read this before building any dashboard. The kit in `assets/data-app/` draws every element described here, so
> most of it is already true of what you build. Your job is what the kit cannot do: decide who it is for and what
> leads, plan the pages and the paths between them, write the findings, and leave things out.

## The idea

A Credible dashboard is a set of pages from a well-run business's own report, not a control panel. It is printed on
warm paper in ink, the way Credible itself is. Each page has one subject, stated as a number and a sentence at the
top; every other element is evidence for it or a way deeper into it. Sections are separated by space and hairline
rules, never boxed. Headings say what the data shows, not what the chart is. Colour is spent on meaning.

A reader who opens it knows in two seconds what it is about and whether things are going well. From there every
chart takes them somewhere: a click filters the dashboard, opens the page of the thing they clicked, or zooms the
period. And on every section they can ask how it is made, and get the period, the filters, what each number means in
the model's own words, how fresh it is, and the query.

## Decide, then build

Do not stop to ask what the dashboard should show. Read the model (its sources, measures, dimensions and docs) and
the request, and decide:

- **Who opens it and how often.** An owner checking weekly, an operator watching daily, an analyst digging.
- **The subject.** The number that reader asks for first: revenue for a sales team, the on-time rate for operations,
  active users or conversion for a product. It leads the first page.
- **The archetype** (`reference/archetypes.md`): an overview of performance, a monitor of what is breaking, an
  explorer of a population. It sets what the first page leads with.
- **The entities.** The things a reader wants to look at one at a time: a category, a brand, a customer, a carrier, an
  airport, a hospital, a repository. Each gets its own page, reached by clicking it anywhere it appears.
- **The pages and the paths.** Usually three to five pages in the navigation and a page per entity. Write the plan
  down before building (below).

Write what you decided at the top of `app.js` and in your hand-over, in a few lines, so the reader can correct it in
one reply. An app built on a stated guess is useful; an app waiting for an answer is not.

## Pages, and the paths between them

A dashboard is never one long page. Plan it as pages a reader moves between:

- **The first page** answers the reader's first question. It leads with the subject and the supporting figures,
  then four to six sections of evidence: where the number comes from, what is moving it, what needs attention.
- **One page per area the reader works in**, named for it: Products, Customers, Carriers, Airports. Each opens with a
  title and a sentence, then a full list of that area's entities (every category, not the top ten), then the
  sections that compare them, then the rows.
- **One page per entity.** It says what the entity is and where it stands among its peers ("3rd of 26 categories by
  net revenue, 16% of the total, up 19%"), then the same subject and figures for the entity alone, then the entity's
  own parts (a category's brands, a carrier's airports), then its rows. Every query on the page is scoped to it
  automatically.
- **Paths go down and across.** Overview → a category → one of its brands → one of that brand's customers. A ranked
  list on any page opens its entities' pages; an entity's page lists the entities inside it. Plan at least two levels
  of depth for the main entity.

Write the plan as a short list before building: each page, what leads it, its sections in order, and for each list
which page a click opens. If a page has nothing a reader would click into, it is a dead end: give it a path or cut it.

## Every chart does something when clicked

This is not optional. A chart that only shows is half a chart.

- A row of a ranked list, a part of a share bar, a line of a multi-line chart: opens that value's page when it is an
  entity, filters the whole dashboard to it otherwise.
- A point on a line over time: zooms the period to that week or month. Dragging across several zooms to those.
- A cell of a grid: filters to its row's value.
- A figure in the ledger: makes it the subject.
- A table row: opens its entity's page when it names one.

The kit does all of these. A custom chart you draw yourself (`reference/app-frame.md`, "A block of your own") must
do one of them too, and say so in its tooltip.

## Every section explains itself

Each block has "How it's made". It turns the section over and says: what it shows (one sentence you write for a
reader who does not know the data), what it covers (the period, the comparison and the filters, in words, written by
the kit), what each number means (from the model's `#(doc)`, or the metric's `doc` when the app defines it), how
fresh and how fast (from Publisher's answer), and the query and its rows with a CSV. Write `what` for every block
you add, and a `doc` for every metric the app defines itself.

## What makes a dashboard look generated, so you can avoid it

- A row of equal boxes, each with one number. It says the page has no subject.
- Every chart in its own bordered card with a shadow, an info icon and a title like "Revenue by category".
- Pills: green and red badges on every change, status chips in every row.
- The system font, the default blue, rainbow categories, library chart defaults.
- A row of dropdowns reading "Category: All", "Brand: All".
- One long page with everything on it, and charts that do nothing when clicked.

None of these are in the kit. Do not add them back.

## The page, top to bottom

1. **The masthead.** The dashboard's name, a few words on what it covers, its pages, and how fresh the data is.
2. **The controls**, read as a sentence: *Last 13 weeks compared with the 13 weeks before · by week*. Filters are
   added with Filter (a field, then its values) and shown as tokens. Copy link copies this exact view.
3. **The subject.** One large number, its change in words and colour, the period it is compared with, a sentence
   written from the data, and beside it the same number over the period with the comparison in grey.
4. **The ledger.** Three to five other figures in one ruled row, smaller on purpose. Clicking one makes it the
   subject.
5. **Sections.** A kicker (what is measured, small), a headline (what it shows, a sentence from the data), the
   evidence. Two may sit side by side.
6. **Rows.** Tables at the bottom or on their own page: dense, ruled, sortable, searchable.

## Each element

**Type.** Geist for everything, Geist Mono for axes, dates in menus and queries. Weight 400 to read, 500 for labels
and names, 600 only for the subject. Large numbers use proportional figures; columns use tabular ones. Currency
symbols and units sit a step smaller than their number.

**Colour.** Paper, ink and four ink tones carry nearly everything. One accent marks the subject, the selection and
focus; choose it from the subject (`accent`: steel by default, indigo, teal, plum, ochre, graphite). Good and bad
appear only on changes and status. The comparison is always grey.

**Changes.** Text, not badges: "+16.2%", "−0.4 pts", "no change", coloured by what the change means for the metric
(`good: 'down'` makes a fall green). Percentages change in points.

**Headlines.** Written by code from the rows. The kit writes a standard one for each block ("Jeans leads; Fashion
Hoodies grew fastest, +29.1%"). Write your own when you know what matters in the domain: a monitor's "Two carriers
are past the line", a cost explorer's "Three hospitals charge more than twice the median". Never type a number.

**Status (monitors).** A coloured dot and a word. Counts lead: "2 breaching, 8 to watch, 3 on track". Thresholds
belong to the metric, so every list and figure of it uses the same lines.

**Motion.** Only for a change the reader caused. The old chart stays dimmed until the new one is ready.

## Leave things out

Four well-chosen sections beat eight. A section whose headline the kit cannot write anything interesting for (every
value the same, one value only, a split no one acts on) is cut, not padded. A filter nobody would use is cut.
