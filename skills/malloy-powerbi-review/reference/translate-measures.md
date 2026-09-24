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

## Sizing the job, and what it costs to get wrong

The user wants to know how big this is before they commit. Two numbers make that
estimate honest, and both are easy to get wrong.

**Count measures as measures.** A model file also carries calculation items,
user-defined functions, calculated columns, calculated tables and RLS role
predicates. All of them are DAX and all of them route, but none of them is a
measure. Summing them under one heading overstates the job and then understates
your own progress against it.

**Count the report layer separately, for *this* model.** Its share swings more
than anything else here - a third of the measures in Microsoft's `PBIASEngine`,
a tenth across a fifty-model sample - so it is not a ratio you can assume. These
measures are captions and colors; they belong to the canvas and do not need to
become Malloy at all, so they are the fastest part of the estimate to retire.

`PBIASEngine` (126 measures) is the worked calibration:

| | measures |
|---|---:|
| report-layer (return a label) | 42 |
| translate directly | 32 |
| need a recipe | 52 |
| of those, can return a different number silently | 43 |
| of those, land on a stopgap recipe | 0 |

**Name the model when you quote a number**, because the profile is not portable.
The same repository holds `FabricASEngineAnalytics` (117 measures): 4
report-layer, 2 direct, and 111 gated on bidirectional cross-filtering. Same
publisher, same domain, opposite shape. Across fifty public models the
untranslatable count stays at zero and the stopgap count stays at zero; that is
the claim worth making to a customer, not any particular ratio.

Four things worth knowing before you quote an estimate:

- **`KEEPFILTERS` is rare** - 0 of `PBIASEngine`'s 79 `CALCULATE` calls. So the
  §"Rule That Makes Migrations Wrong" divergence is the default case, not an edge
  one, and the work is establishing which measures are *safe* rather than hunting
  a few bad ones.
- **Three recipes fire zero times in any measure and are not rare at all** - they
  live elsewhere in the file. `CALENDAR()` is only ever in a calculated-table
  partition, calculation groups carry a model's time intelligence, and RLS
  predicates live in `roles/*.tmdl`. Match only `measure` declarations and you
  will report a model as having no calculation groups, no date spine and no RLS.
- **`ALLSELECTED` is common and costs nothing.** It is among the most frequent
  functions by measures containing it, and it maps exactly. Do not budget for it.
- **Many-to-many, parent-child hierarchies and semi-additive measures are real
  but rare** - zero occurrences across the fifty-model sample. Do not lead a
  customer conversation with them.

`T4` and `FC8` are **teaching recipes with no trigger**: no DAX function requests
either, so the router cannot emit them. Their zero says nothing, and it should
never be reported among measured results. `reference/limitations.md` is the full
inventory of what the router can and cannot decide.

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

**If your run produces a large untranslatable bucket, suspect your run.** Twelve
`PBIASEngine` measures were once reported untranslatable; all twelve were
`ALLSELECTED`-triggered, eight of them rankings, and not one exercised a real gap.
`ALLSELECTED` maps exactly and `RANKX` maps to `calculate: rank()`. Check those two
mappings before reporting a gap to a customer.

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
