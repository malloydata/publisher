<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The d3 Chart Module

Every chart is drawn with d3 into SVG by one chart module, which you copy whole into the app and never read: bars,
lines, areas, scatters, heatmaps, maps, the ranked table, and the forms a specialist reaches for (slope, bump,
beeswarm, flow, treemap, waterfall and the rest). Each sizes itself to its box, redraws when the box changes width,
takes a click (`onPick`; not `sparkline`, `waterfall`, `multiples` or `connectedScatter`, and `rankTable` opens a row
with `onOpen`), shows a tooltip on hover and on keyboard focus, and writes under itself what it could not
draw. It ships inside the dashboard kit, `assets/data-app/kit/charts.js` (`reference/app-frame.md`), and the kit's own
blocks cover the common forms (the subject's line, ranked lists, shares, lines by a field, tables, grids, watchlists).
Reach for this module for the rest, inside a `block()` of your own.

It is the exception to `SKILL.md`'s "trim the chart library" rule: it is part of the kit, copied whole and never
edited, so a fixed version can be copied over it. The kit loads d3 beside its first queries (`reference/performance.md`).

## Using it from the kit

The kit's copy step copies it with d3 and the styles it reads; `index.html` already links `kit/charts.css`, and
`kit.css` maps the module's colour names onto the kit's palette. Import it from the kit, `import { C } from
'./kit/kit.js'`. To learn a function's arguments, read only its comment and signature, never the code:
`awk -v f=bump '/^\/\*\*/{b="";c=1} c{b=b $0 "\n"; if (/\*\//) c=0; next} $0 ~ "^export (function|const) " f "[ (=]" {i=index($0, "){"); printf "%s%s\n", b, substr($0, 1, i && i < 200 ? i + 1 : 200); exit} {b=""}' public/kit/charts.js`.
Its helpers are `C.fmt`, `C.pct`, `C.token`, `C.loadTopo` (boundary files) and `C.tercile`; `C.ABBR` maps state
names to postal codes. Its `rankTable` is superseded by the kit's `rank()` block; use that.

## Choose the form by the question

| The question | The form | In the module |
|---|---|---|
| How has it changed over time? | A line, the selected period shaded; drag to choose a period | `C.lines` with `band` and `onBrush` |
| How do a few groups compare over time? | A line per group, named at its end | `C.lines` |
| How did this year's season run against earlier ones? | Periods overlaid on one axis (Jan to Dec, one line per era) | `C.lines`, labels are months, a series per era |
| What is it made of, over time? | Stacked areas, each named inside its band | `C.area` |
| Which share of each group is which part? | Stacked bars to 100% | `C.stackedShare` |
| Who has the most? | A ranked list with bars and the change | The kit's `rank()` block |
| Which items, by one measure? | Bars with their values at the end, no value axis | `C.bars` with `direct` |
| How big was each period, and which stand out? | Columns, a reference line where a number means something | `C.bars` with `refs` |
| Which places lead, and by how much? | Not a map: sorted bars, a dumbbell, a slope, or a ranked list | `C.bars`, `C.dumbbell`, `C.slope`, the kit's `rank()` |
| Where, when the pattern in space is the finding (neighbours, a gradient, a cluster)? | A choropleth of the places | `C.choropleth` (`reference/maps.md`) |
| Where, for points with coordinates and no boundaries? | Hexagons binned from the points, the unfiltered points grey underneath | `C.hexMap` |
| Where, when every state should read equally? | A tile-grid map of US states, each tile with its trend | `C.tileMap` |
| Where, as an amount rather than a rate? | Circles on the map, area to the amount | `C.circleMap` |
| Where, by two measures at once? | A bivariate map, three classes each | `C.bivariateMap` |
| What moved, then against now? | A scatter of then (x) against now (y), with the line where nothing changed | `C.scatterChange` |
| How does it differ with and without something? | A dot for each, joined, the difference written beside it | `C.dumbbell` |
| When in the week, or the year, does it happen? | A heatmap, or a clock of rings (days) by hours | `C.heatmap`, `C.clock` |
| What happens next: from one grouping to the next? | A flow, bands as wide as their count | `C.flow` |
| How did each item move between two dates? | A slope: a line per item from then to now | `C.slope` |
| Who led, year by year? | A bump chart of ranks | `C.bump` |
| How are many items spread along one measure? | A beeswarm, a dot per item | `C.beeswarm` |
| Which parts drove each period's change? | Contribution columns: gains above zero, losses below, the net marked | `C.contrib` |
| How did a total get from one value to another? | A waterfall of the contributors | `C.waterfall` |
| Who gained and who lost? | Diverging bars either side of zero | `C.divergingBars` |
| What is one whole made of? | A waffle of a hundred squares (few parts) or a treemap (many, or many in groups) | `C.waffle`, `C.treemap` |
| What is each group made of, and how big is each group? | A marimekko: widths are the groups, bands the parts | `C.marimekko` |
| How did a few items change, in the same unit? | Arrows from the old value to the new | `C.arrows` |
| How do many series compare in shape? | Small multiples, each on its own scale | `C.multiples` |
| How did two measures move together over time? | A connected scatter, one point per period | `C.connectedScatter` |

Pick the form a specialist in the field would draw, not the first one the columns suggest and not the one the
template already holds. This table is where you start, not the whole choice: when a finding deserves a form it lacks,
draw it in a `block()` of your own (`reference/app-frame.md`, "A block of your own"). Never the same form for two questions next to each other.

## The ranked list

Most apps have one list readers come for: the categories, the carriers, the hospitals, ranked. Draw it with the
kit's `rank()` block: each row carries its value as a thin bar over a pale one for the comparison, its number and
its change, and opens the entity's page (or filters) on a click. Use `extra` for a second metric column.

## Everything clicks

- **A click does what the kit's blocks do.** A mark that stands for an entity opens its page (`store.open`); any
  other filters the dashboard to it (`store.toggleFilter`); a period zooms to it. Pass `onPick`, and `selected` where the chart takes it, to
  every chart that stands for something in the scope: a bar, a period, a place, a part, a district. The selected
  mark keeps its colour; the rest fade. Time charts take `onBrush(from, to)` to set the scope's period, and show it,
  and a picked period, with `band`.
- **The tooltip is built in**: the value, its share, the total it is part of, and what a click does. Hover and
  keyboard focus both show it.
- **Marks move.** A chart drawn again into the same box moves each mark from where it was, and a first chart rises in;
  `C.MOTION` is false when the reader asks for less motion or the address has `?motion=0`, and `C.DUR` (380 ms) is
  the time a mark takes to move between two answers. That is longer than a UI transition (`reference/visual-craft.md`)
  on purpose: the reader is following a mark, not waiting for a control. `C.countTo`
  (node, from, to) counts a number in text to its new value. Hovering a mark lights the marks and ranked rows that stand for
  the same thing in every chart, by the key `onPick` gets (a map's `linkOf` names its shapes the same way).
- **Marks are named buttons** a keyboard reaches, and focus comes back after a pick. Large sets (counties, hexagons,
  clock cells, a beeswarm of more than 60 dots) are clicked rather than tabbed to: put a ranked table beside them.

## Using the module

- Draw into a box you append to the block's body, `body.appendChild(document.createElement('div'))`: no fixed height, the chart fills it at its own height. Give the block a `reserve` close to that height.
- Colours are token names (`'--accent'`, `'--c2'`), so a part keeps one colour on every chart. The bivariate map's
  nine colours mix `--accent` and `--c2`, so the app's accent reaches them.
- Changes are neutral unless `good` ('rise' or 'fall') says which way is good news.
- The maps need topojson-client and a boundary file from `reference/maps.md`; `C.hexMap` and `C.tileMap` need neither.
- With nothing to draw, a chart says so in the card; what it cannot draw (a row with no value, zero on a log scale, a
  place with no shape, steps that miss their total) it says under the drawing, never drops in silence.
- A chart prints only numbers it is given. It places marks by arithmetic (a band's height, where a waterfall step
  starts), but a share, a net or a total it prints comes from the model: pass contrib its `nets` and the marimekko
  each part's `share`, or they draw the marks and print no number.

Each form needs its numbers ready from the model, in the shape the last column gives.

| Form | Not for | The model gives |
|---|---|---|
| `hexMap` | Places with boundaries (use a choropleth), or a rate: a hexagon sums what falls in it | One row per location, `group_by: lat, lon`, with a count and a `limit` above the number of locations; the same with no filters as `base` |
| `clock` | Fewer than three rings, or a cycle that is not a clock (use a heatmap) | One row per ring and slot, such as day of week and hour of day, with a count |
| `flow` | More than about twelve names in a stage, or stages that do not follow from each other | One row per combination of the stages' fields, with a count |
| `tileMap` | Places other than US states, or a map where area is the point | One value per state, and a nested trend if the tiles carry one |
| `circleMap` | Rates and shares: a circle's area reads as an amount | One amount per place, matched to the boundary file's names |
| `bivariateMap` | A single measure, or two that move together | Two measures per place; the classes are thirds of the places shown |
| `slope` | More than about twelve items, or more than two dates | A first-date and a last-date measure per item (filtered measures such as `m_first`, `m_latest`) and the change |
| `bump` | Fewer than three periods, items that leave the top for long, or ranks that hardly change: flat lines say only that, and a ranked table says it better | `calculate: m_rank is rank() { partition_by: period; order_by: m desc }`, one row per item and period; a tie draws on one spot |
| `beeswarm` | A count that takes only a few values, which stacks into towers; under 30 or over about 400 items | One continuous value per item; `log: true` for values spread over decades |
| `contrib` | Fewer than three periods, or parts that do not add up to the total | `calculate: change is m - lag(m) { partition_by: part }`, one row per part and period, and the net per period, `change` with no part |
| `waterfall` | A total that is not the sum of its contributors | Each contributor's change, the two totals, and the change for "All others" so the steps add up |
| `divergingBars` | Values that are all on one side of zero, or more than about 30 items (it draws the first 40) | A signed change per item, computed in the model |
| `waffle` | More than six parts, or parts of different wholes | Each part's value and its share, `m / nullif(all(m), 0)`; a top few that are not the whole leave the rest as "Everything else" |
| `marimekko` | More than about ten groups, or parts that overlap | One row per group and part, with the part's share of its group, `m / nullif(all(m, group), 0)`; the group's total; and what of the total no part holds (withheld) |
| `arrows` | Items in different units, or more than about eight | A first and a last value per item, and the change |
| `multiples` | Series meant to be compared by height: each panel has its own scale | One row per series and period |
| `connectedScatter` | Fewer than about ten periods, or two measures that bounce from year to year (a crop and its price): the path tangles, and two lines read better | Two measures per period, in time order |
| `treemap` | Comparing items that are close in size | One row per item with its value; its `group`, `share` (`m / nullif(all(m), 0)`) or `change` when the tiles show them |

## Rules

- **Label directly.** A line is named at its end, an area inside its band, a bar by its value at its end, a
  notable point beside it. A key is only for marks that cannot carry a name: a map's or heatmap's ramp, and parts too
  thin to name inside them.
- **Show the scope on the chart.** When the app's scope selects a period, every time chart shades it (`band`).
  Where a number has a meaning (a target, the national rate, last year's level), draw it as a labelled reference
  line (`refs`).
- **Colours come from the kit's tokens** (`reference/dashboard-design.md`, "Colour"), each with one meaning across the app.
  Neutral bars use `--c-bar`; the accent is for the lead number and the selection.
- **Every chart's finding is also a sentence**: the block's headline, written from its rows, for readers who cannot
  see the chart.
- **Every block says what it shows**, in its `what`, so its info mark reads as a definition, then the query and its
  rows.
- **Verification reads SVG, not pixels.** The module draws SVG, so a check counts marks (`svg path`, `svg rect`,
  `svg circle`) and reads their labels, where a canvas library's check reads pixels
  (`reference/verification-harness.md`).
