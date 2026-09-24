<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# DAX Measure Translation (Step 5)

> Route every DAX measure to a recipe before translating it. Emitting Malloy is the
> easy part; knowing which measures change meaning on the way across is the work.

Read `_concepts.md` for the syntax mapping tables. This file is about the semantic
traps the mapping tables cannot express, and about **where to send each measure**.

The recipes are in three companion files, and they are the deliverable:

| File | Covers |
|---|---|
| `cookbook-filter-context.md` | `CALCULATE`, `ALL`, `ALLSELECTED`, `ALLEXCEPT`, `REMOVEFILTERS`, `RANKX`, top-N |
| `cookbook-time.md` | YTD, prior period, growth, date spines, semi-additive, `CALENDAR()` |
| `cookbook-structure.md` | `USERELATIONSHIP`, many-to-many, bidirectional, what-if parameters, calculation groups, `PATH` |

**"Untranslatable" does nothing for the customer.** "Here is the Malloy, and here is
what you lose" has done the migration. Route to a recipe; only the four constructs
marked STOPGAP in the cookbook are genuinely short of a first-class answer, and even
those ship working Malloy.

## The Rule That Makes Migrations Wrong

**DAX Boolean filter arguments overwrite. Malloy's `where:` intersects.**

In DAX, this:

```dax
Shipped Revenue = CALCULATE([Total Revenue], Orders[Status] = "Shipped")
```

is shorthand for `CALCULATE([Total Revenue], FILTER(ALL(Orders[Status]), Orders[Status] = "Shipped"))`. The `ALL` is implicit and it is the whole problem: the filter **replaces** whatever filter was already on `Orders[Status]`.

In Malloy, this:

```malloy
measure: shipped_revenue is total_revenue { where: status = 'Shipped' }
```

**adds** a condition to whatever the query already filtered.

(Written against the leaf aggregate, `sum(revenue) { where: status = 'Shipped' }`, this also stays usable by preaggregation, which refuses a filter refining an already-derived measure. Prefer the leaf form wherever the underlying aggregate is available.)

Now put a report filter on `Status = "Cancelled"` and ask for both:

| | DAX | Malloy |
|---|---|---|
| Filter context | `Status = "Cancelled"` | `where: status = 'Cancelled'` |
| Measure's own filter | overwrites it: `Status = "Shipped"` | intersects: `Cancelled AND Shipped` |
| Result | **shipped revenue** | **zero** |

Neither errors. Both look like a correct transcription. One of them is on a slide in front of a customer. `cookbook-filter-context.md#fc1` works this against real data, with the numbers.

**`KEEPFILTERS` is the tell.** A DAX author who wrote `CALCULATE([Total], KEEPFILTERS(Orders[Status] = "Shipped"))` asked for intersection, which is what Malloy does natively. That measure is safe. A measure without `KEEPFILTERS` is only safe if nothing ever filters the same column, and you cannot know that from the model file alone.

**Expect the tell to be absent.** `KEEPFILTERS` is rare in practice: Microsoft's published `PBIASEngine` model uses it in **0 of its 79 `CALCULATE` calls**. So divergence is the default case, not the exception. Do not approach this as hunting for a few bad measures among many safe ones; approach it as establishing which handful are safe.

## Routing Procedure

Run the steps in order. Step 0 is not optional and is not in the DAX.

### 0. Build the dependency graph, and read `relationships.tmdl`

**A measure is no better than its worst dependency.** Measures reference measures;
route the leaves and walk up. A direct-looking wrapper around a divergent leaf is
divergent.

**Bidirectional, many-to-many and inactive flags live outside every measure's DAX**,
so a text-only pass cannot see them. Read the relationships first. The blast radius
is not theoretical:

| model | measures | bidirectional relationships | measures affected |
|---|---:|---:|---:|
| `PBIASEngine` | 126 | 0 | 0 |
| `FabricASEngineAnalytics` | 117 | 2, both onto the fact table | **111** |

Same publisher, same domain. TMDL writes only non-default properties, so an absent
`crossFilteringBehavior` means single-direction - read absent as the default, not as
missing data. `CROSSFILTER(..., BOTH)` sets the same thing inside one measure and
leaves no trace in `relationships.tmdl` at all, so scan the measures for it too.

### 1. Does it return a string? Route to **skip**.

Only that. Not "references the viewer's selection" - that test swallows legitimate
denominators, and `ALLSELECTED` is never a reason to skip anything.

A large share of the measures in a real model exist only to drive the report canvas:
button captions, tooltips, dynamic titles, navigation paths, conditional-format
colors, SVG sparklines. They return text, not numbers, and no one wants them in a
semantic model. Skip them the way you skip `report.json`.

**Type the return value; do not look for a quote character.** The canonical example
in `PBIASEngine` has no string literal at all:

```dax
Selected page = SELECTEDVALUE('Current page'[Current page])
```

It is a label because `'Current page'[Current page]` is `dataType: string`. A test
that keys on a `"` in the body files this as a translatable measure. The tells that
do work: a string-returning function (`FORMAT`, `CONCATENATE`, `UNICHAR`, `LEFT`,
`PATH`, …), `&` concatenation, a string literal in a *value* position rather than a
comparison, `SELECTEDVALUE`/`VALUES`/`MIN`/`MAX` **over a text column**, or a
reference to a measure that is itself a label.

Three DAX lexing traps that produce wrong answers here: `&&` is logical AND, not
concatenation; `IN ({"a","b"})` is a comparison against a literal set, not a value
position; and DAX has **two** line-comment forms, `//` and `--`. Real models carry
whole superseded measures commented out, so a scan that misses `--` reads dead code
as live.

Report the skipped count separately so the user sees the true size of the job.

### 2. Does it need a concept outside the model? **Name the recipe.**

Before the widen test, because these are not filter-context problems and treating
them as such is what stalls the routing.

| Concept | Recipe |
|---|---|
| inactive relationship / `USERELATIONSHIP` | `cookbook-structure.md#s1` |
| many-to-many | `cookbook-structure.md#s2` |
| bidirectional / `CROSSFILTER` | `cookbook-structure.md#s3` |
| disconnected parameter table, `GENERATESERIES` + `SELECTEDVALUE` | `cookbook-structure.md#s4` |
| calculation item / `SELECTEDMEASURE` | `cookbook-structure.md#s5` **(stopgap)** |
| `PATH` family | `cookbook-structure.md#s6` **(stopgap)** |
| marked date table / time intelligence | `cookbook-time.md#t1` to `#t6` |
| ranking, top-N | `cookbook-filter-context.md#fc6`, `#fc7` |

### 3. Does it touch filter context?

Including **a measure reference inside an iterator** - `SUMX(T, [Some Measure])` is
context transition and belongs here, not in step 4. (An iterator over a plain
row-level expression is not: `SUMX(Sales, Sales[Qty] * Sales[Price])` is
`sum(qty * price)`, and it is one of the commonest measures in any model.)

- **Widens** - `CALCULATE` with a Boolean filter and no `KEEPFILTERS`;
  `FILTER(ALL(T), …)`; `ALL`; `REMOVEFILTERS`; `ALLEXCEPT` - route to the divergent
  recipe: `#fc1`, `#fc2`, `#fc4`, `#fc5`.
- **Narrows** - `KEEPFILTERS`, or plain `FILTER(T, …)` over the table, which is
  evaluated in the current filter context and therefore preserves existing filters -
  translate directly.
- **Neither** - `ALLSELECTED` (an exact match, `#fc3`), `USERELATIONSHIP` (a join
  path). **Fall through to step 4. Do not stall.**

### 4. Otherwise, translate directly.

Then **parity-test anyway**.

### Then: say what the routing is, and is not

**Step 3 can rarely prove a measure safe.** Whether anything ever filters the
overwritten column lives in `report.json`, which this skill deliberately does not
read. So the output is a **priority order, not a verdict** - which measures to
parity-test first, and at which filter context. Present it that way, or the user
reads "direct" as "checked".

## Counts, and what they cost to get wrong

**Count measures as measures.** A model file also carries calculation items,
user-defined functions, calculated columns, calculated tables and RLS role
predicates. All of them are DAX and all of them route, but none of them is a
measure, and summing them under one heading once published 1,622 measures for a
corpus that held 1,406. Every table below is measures only unless it says
otherwise.

For calibration, `PBIASEngine` (126 measures), routed by
`scripts/classify_measures.py`:

| | measures |
|---|---:|
| report-layer (return a label) | 42 |
| translate directly | 32 |
| need a recipe | 52 |
| of those, can return a different number silently | 47 |
| of those, land on a stopgap recipe | 0 |

It carries 15 calculated columns and 6 calculated tables besides.

Function frequency in the same model, as occurrences / measures containing:
`CALCULATE` 79/50, `ALLSELECTED` 33/25, `RANKX` 8/8, `ALLEXCEPT` 7/4,
`REMOVEFILTERS` 5/5, `KEEPFILTERS` **0/0**. `ALLSELECTED` is the fourth most common
function by measures containing it, behind `CALCULATE`, `IF` and `MAX` - common
enough to budget for, and it needs no budget, because it maps exactly.

**Name the model when you quote a number.** The same repository holds
`FabricASEngineAnalytics` (117 measures), whose profile is completely different: 4
report-layer, 2 direct, 111 gated on bidirectional cross-filtering.

**Across a wider corpus the untranslatable count stays at zero.** Fifty public
TMDL models, every one under a permissive license, listed with its commit in
`corpus.md` so the number can be re-run:

| | measures |
|---|---:|
| models routed | 50 |
| measures | 1,881 |
| report-layer (return a label) | 217 |
| translate directly | 606 |
| need a recipe | 1,058 |
| of those, can return a different number silently | 937 |
| of those, land on a stopgap recipe | 0 |
| **untranslatable** | **0** |

Those 50 models carry a further **363 definitions that are DAX and are not
measures** - 129 calculated columns, 115 calculated tables, 86 user-defined
functions, 17 calculation items, 16 RLS role predicates - plus 126 auto date
tables, which are skipped (`S7`).

Recipe demand across the measures, which is what says where to reach first:
`FC1` 791, `S3` 251, `S4` 155, `FC5` 147, `T2` 73, `T3` 64, `FC2` 44, `FC7` 28,
`FC3` 26, `T1` 23, `S1` 12, `FC6` 5, `FC4` 5.

Five things only a wider corpus shows:

- **`FC5` (`ALL(T[c])` / `REMOVEFILTERS` on one column) is fourth at 147**, though it
  appears 5 times in `PBIASEngine`. A single-model sample under-ranks it badly. It
  was briefly published as third at 216, because `FILTER(ALL(T[c]), pred)` matched
  the same `ALL(` - and that shape *re-filters* the column rather than removing it
  from the grouping, so it is `FC1`'s expanded spelling. The Malloy `FC5` points at
  compiles and answers a different question.
- **Three recipes fire zero times in any measure and are not rare at all** - they
  live somewhere else in the file. `T6` fires 11 times across 9 models, every one
  a **calculated-table partition**, which is where `CALENDAR()` actually is. `S5`
  fires 17 times, all **calculation items**; `RLS` 16 times, all
  **`roles/*.tmdl`**, and not one `USERPRINCIPALNAME` in the corpus is in a table
  file. A pass that reads only `measure` declarations reports all three as absent.
- **`S4` is third at 155, and it was nearly missed the same way.** Detecting a
  what-if parameter by `GENERATESERIES()` finds only the tables that are
  *generated*, which are 10 calculated tables and 12 user-defined functions and no
  measure at all. What a measure actually does is read an **unjoined** table with
  `SELECTEDVALUE`/`MIN`/`MAX`, and that is the test: 155 measures across 14
  models, in one of which it drives 81 of 108.
- **`S2`, `S6` and `T5` fire zero times, and that one is measured.** Not one
  many-to-many relationship in 50 `relationships.tmdl` files, and no `PATH` or
  `CLOSINGBALANCE*` anywhere in 2.5M characters of live DAX. Those shapes are real
  but rare - do not lead a customer conversation with them, and weigh it before
  citing them as upstream evidence.
- **`S5` is concentrated rather than spread.** Like bidirectional
  cross-filtering, a model either builds on calculation groups or has none.

`T4` and `FC8` are **teaching recipes with no trigger**: no DAX function requests
either, so the router cannot emit them and their zero says nothing. Do not report
them among measured results - a previous revision of this file did, inside a
"fires zero times" claim that was vacuous for exactly that one route.
`reference/limitations.md` has the full inventory.

**Three ways earlier revisions of this skill got its own numbers wrong**, all worth
avoiding in yours:

- A substring match on `CALCULATE` also matched the column name
  `CPUTime (calculated)`, inflating the count. Match `\bCALCULATE\s*\(` against a
  body with bracketed column references blanked out - and note the same pattern
  correctly does *not* match `CALCULATETABLE(`.
- A report-layer test that required a `"` in the body filed `Selected page` as
  translatable, though this file uses it as *the* report-layer example. See step 1.
- A report-layer test that read a string anywhere in the body, rather than the
  value the measure returns, filed numbers as labels. The tells are a column
  *name* argument to `ADDCOLUMNS`/`SUMMARIZE`/`SELECTCOLUMNS`/`ROW`; a text column
  named only to tell an iterator which table to walk; a `VAR` the `RETURN` never
  reaches; and a `//` inside a string literal, which is not a comment - the one in
  an SVG measure's `http://www.w3.org/2000/svg` left the literal unterminated and
  typed five sparklines as numbers. **DAX types on the `RETURN`.**

**The untranslatable count for `PBIASEngine` is approximately zero.** Twelve measures
were once reported untranslatable; all twelve were `ALLSELECTED`-triggered, eight of
them rankings, and not one exercises a real gap. `ALLSELECTED` maps exactly, and
`RANKX` maps to `calculate: rank()`, which orders by any expression independently of
the query's own ordering. If your run produces a large untranslatable bucket, check
those two mappings before reporting it.

## Translation Notes That Still Bite

**Watch the counts.** `DISTINCTCOUNT(T[c])` is `count(c)`, because `count(field)` is already distinct in Malloy and `count(distinct c)` is a parse error. DAX `COUNT(T[c])` is **not** `count(c)`; it is `count() { where: c is not null }`. Getting these backwards compiles and returns a different number. `COUNTA` behaves like `COUNT` here.

**The `BLANK()` caveat.** DAX treats blank as zero in addition: `BLANK() + 1` is `1`. Malloy and SQL propagate null: `null + 1` is `null`. Any measure that sums or subtracts other measures can diverge wherever one side is empty. Wrap with `??` where the DAX relied on it, and validate a filter context where one term has no rows. The idiom `[Some Measure] + 0`, common in real models, is exactly this reliance written out.

**`EARLIER` / `EARLIEST`** are row-context constructs with no equivalent. These are
the one shape with no recipe: ask what the number means and rewrite the intent.

## Reporting

Produce a table with a row per measure:

| Measure | Recipe | Malloy | Diverges when | Power BI value | Malloy value | Match |
|---------|--------|--------|---------------|----------------|--------------|-------|

For a direct translation, "Diverges when" is empty and the values should match. For
a divergent recipe, "Diverges when" is mandatory and both values are measured at
that context, **not at the grand total** - the overwrite-versus-intersect divergence
is invisible at the grand total by construction. For a stopgap, record what the
workaround costs as well as what it returns.

**A migration report with no divergent rows is a red flag, not a clean bill of
health.** Any Power BI model with real `CALCULATE` usage has them. If you found
none, you did not look at `KEEPFILTERS`.

## Running the classifier

```
python3 scripts/classify_measures.py <model>/definition
python3 scripts/classify_measures.py <model>/definition --format json
python3 scripts/classify_measures.py <model>/definition --functions
python3 scripts/classify_measures.py --json measures.json      # the .pbix path
```

The script needs `definition/tables/*.tmdl` **and** `definition/relationships.tmdl`.
On the `.pbix` path there is no TMDL at all, so `--json` takes extracted records;
supply `columns[]` with their `dataType` or step 1 under-detects labels, and
`relationships[]` or step 0 is skipped entirely. The script reports which of those
it did not have rather than passing quietly.

It is a scale tool, not a substitute for this file - it handles the dependency
graph, the return-type inference and the relationship flags, none of which survive
reading measures one at a time at 5,000 of them. Read the routing it produces and
hand-check a sample: on `PBIASEngine`, a 25-measure hand-check agreed with the
script on all 25.

**It will not run on every surface.** Skills ship their scripts to npm and into
Claude Code and Cursor, but the Credible app's agent has no shell tool, and the MCP
skills bundle carries markdown only. The prose above has to stand on its own, and
does.
