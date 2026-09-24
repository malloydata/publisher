<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# DAX Measure Translation (Step 5)

> Classify every DAX measure before translating it. Emitting Malloy is the easy part; knowing which measures change meaning on the way across is the work.

Read `_concepts.md` for the syntax mapping tables. This file is about the semantic traps that the mapping tables cannot express.

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

Neither errors. Both look like a correct transcription. One of them is on a slide in front of a customer.

**`KEEPFILTERS` is the tell.** A DAX author who wrote `CALCULATE([Total], KEEPFILTERS(Orders[Status] = "Shipped"))` asked for intersection, which is what Malloy does natively. That measure is safe. A measure without `KEEPFILTERS` is only safe if nothing ever filters the same column, and you cannot know that from the model file alone.

## The Three Classes

Every measure lands in exactly one. Classify first, translate second.

### Class 1: Translatable

Translate directly. Validate anyway, but expect a match.

- Plain aggregates: `SUM`, `AVERAGE`, `MIN`, `MAX`, `COUNTROWS`, `DISTINCTCOUNT`
- **Iterators over a row-level expression**: `SUMX(Sales, Sales[Qty] * Sales[Price])` is `sum(qty * price)`. Same for `AVERAGEX`, `MINX`, `MAXX`, `COUNTX`. There is no context transition unless a measure reference or `CALCULATE` appears *inside* the iterator, so most `SUMX` is Class 1 and this is one of the commonest measures in any real model.
- Arithmetic over other translatable measures, with the `BLANK()` caveat below
- `DIVIDE(a, b)` to `a / nullif(b, 0)`
- `CALCULATE` with `KEEPFILTERS` on every Boolean filter argument
- `RELATED(Other[c])` to a join path `other.c`
- `IF` / `SWITCH` over row-level conditions, to `pick ... when ... else`

**Watch the counts.** `DISTINCTCOUNT(T[c])` is `count(c)`, because `count(field)` is already distinct in Malloy and `count(distinct c)` is a parse error. DAX `COUNT(T[c])` is **not** `count(c)`; it is `count() { where: c is not null }`. Getting these backwards compiles and returns a different number.

**The `BLANK()` caveat.** DAX treats blank as zero in addition: `BLANK() + 1` is `1`. Malloy and SQL propagate null: `null + 1` is `null`. Any measure that sums or subtracts other measures can diverge wherever one side is empty. Wrap with `??` where the DAX relied on it, and validate a filter context where one term has no rows.

### Class 2: Silently Divergent

**These are the ones that cost the migration.** They translate to valid Malloy that returns a different number under some filter contexts and the same number under others. They cannot be caught by reading the output.

Flag every one. Never resolve it quietly.

- **`CALCULATE` with a Boolean filter and no `KEEPFILTERS`**, where the filtered column is one a report or a user can also filter. This is the common case and there will be many.
- **`CALCULATE(expr, FILTER(ALL(T), cond))` or `FILTER(ALL(T[c]), cond)`**: this is the explicit spelling of the sugar above, and it is the overwriting form. Search for `FILTER(ALL(` specifically; it is the shape that does the damage. (Plain `CALCULATE(expr, FILTER(T, cond))` over the table is evaluated in the current filter context and therefore *preserves* existing filters, which is the safe, intersecting shape.)
- **Every `ALL` / `REMOVEFILTERS` translation.** DAX `ALL` removes *filters*; Malloy `all()` removes *grouping* and still obeys the query's `where:`. They agree when the grouping is the only filter, which is the unfiltered grand total, and diverge as soon as anything else is filtered. See the worked table in `_concepts.md`. Percent-of-total built on `ALL` inherits this.
- **`ALLEXCEPT`**: keeps filters on the listed columns and removes the rest, so it is `all(expr, kept...)`, **not** `exclude(expr, kept...)`. `exclude()` is the `ALL(T[c])` analog. The two read alike and fail silently.
- **Measures referencing measures that are themselves Class 2.** Divergence propagates. Classify the leaves first and walk up; a Class 1 wrapper around a Class 2 measure is Class 2.
- **Any measure over a table reachable by a bidirectional relationship.** The filter propagation differs before the measure is even evaluated.
- **`BLANK()`-dependent arithmetic**, as above, when the author clearly relied on it.

For each, record: the measure, the column whose filter is overwritten, the filter context in which it diverges, and the number from both sides at that context. That table is what you hand the user.

### Class 3: Untranslatable

No Malloy equivalent. Do not fake one. Each needs a rewrite of the intent or an explicit decision to drop it.

- **Time intelligence**: `TOTALYTD`, `SAMEPERIODLASTYEAR`, `DATEADD`, `DATESYTD`, `PARALLELPERIOD`, `PREVIOUSMONTH`. These depend on a marked date table with a contiguous date column. The intent translates; the function does not.
- **Context transition inside an iterator**: `SUMX(T, [Some Measure])`, where a measure reference or `CALCULATE` inside the iterator transitions row context into filter context. Note the narrowness: an iterator over a plain row-level expression is Class 1 (above), and `SUMX(FILTER(T, cond), <row expr>)` is usually just `sum(expr) { where: cond }`. Only the nested-measure form belongs here. Often the intent is a simple filtered aggregate and the DAX is more complicated than the question; ask what the number means before translating the code.
- **`EARLIER` / `EARLIEST`**: row-context constructs with no equivalent.
- **`RANKX`, `TOPN`**: ranking is a query in Malloy, not a measure.
- **`ALLSELECTED`**: depends on the visual's own filter scope, a concept that exists only inside a report.
- **`USERELATIONSHIP`**: switches to an inactive relationship for one measure. In Malloy this is a second join path or a second source.
- **Calculation groups**: a Power BI object that rewrites measures at query time. There is no equivalent; each generated combination has to be considered on its own.
- **Field parameters**: a report-layer construct that swaps which measure or column a visual shows.
- **Parent-child hierarchy functions**: `PATH`, `PATHCONTAINS`, `PATHITEM`. Usually a recursive org structure, which wants flattening upstream.

## Working Order

1. **Build the dependency graph first.** Measures reference measures. Classify leaves, then walk up. A wrapper is no better than its worst dependency.
2. **Classify every measure before translating any.** The counts change the conversation: "310 of 340 are Class 1" and "180 are Class 2" are different projects.
3. **Translate Class 1 in bulk.** They are mechanical.
4. **Translate Class 2 one at a time, each with a parity check at a filter context that exercises the divergence.** Unfiltered totals will match and prove nothing.
5. **Take Class 3 to the user as a list of intents**, not a list of failures. "Here are 48 time-intelligence measures; they express year-to-date, prior-year, and rolling-window comparisons. Here is how Malloy expresses each of those three shapes."

## Reporting

Produce a table with a row per measure:

| Measure | Class | Malloy | Diverges when | Power BI value | Malloy value | Match |
|---------|-------|--------|---------------|----------------|--------------|-------|

For Class 1, "Diverges when" is empty and the values should match. For Class 2, "Diverges when" is mandatory and both values are measured at that context, not at the grand total. For Class 3, only the intent is recorded.

**A migration report with no Class 2 rows is a red flag, not a clean bill of health.** Any Power BI model with real `CALCULATE` usage has them. If you found none, you did not look at `KEEPFILTERS`.
