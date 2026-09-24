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
- Arithmetic over other translatable measures, with the `BLANK()` caveat below
- `DIVIDE(a, b)` to `a / nullif(b, 0)`
- `CALCULATE` with `KEEPFILTERS` on every Boolean filter argument
- `CALCULATE(expr, ALL(T))` to `all(expr)`, and `CALCULATE(expr, ALL(T[c]))` or `REMOVEFILTERS(T[c])` to `exclude(expr, c)`
- Percent-of-total built from those: `expr / CALCULATE(expr, ALL(T))`. Search the Malloy docs for the percent-of-total pattern rather than inventing one.
- `IF` / `SWITCH` over row-level conditions, to `pick ... when ... else`

**The `BLANK()` caveat.** DAX treats blank as zero in addition: `BLANK() + 1` is `1`. Malloy and SQL propagate null: `null + 1` is `null`. Any measure that sums or subtracts other measures can diverge wherever one side is empty. Wrap with `??` where the DAX relied on it, and validate a filter context where one term has no rows.

### Class 2: Silently Divergent

**These are the ones that cost the migration.** They translate to valid Malloy that returns a different number under some filter contexts and the same number under others. They cannot be caught by reading the output.

Flag every one. Never resolve it quietly.

- **`CALCULATE` with a Boolean filter and no `KEEPFILTERS`**, where the filtered column is one a report or a user can also filter. This is the common case and there will be many.
- **`CALCULATE` with `FILTER(T, cond)` over a whole table**: a table filter, which keeps other columns' filters but replaces the table's own row set. Whether it matches `where:` depends on the condition and the query.
- **`ALLEXCEPT`**: removes filters from everything except the listed columns. Expressible with `exclude()` but easy to get backwards, and the error is silent.
- **Measures referencing measures that are themselves Class 2.** Divergence propagates. Classify the leaves first and walk up; a Class 1 wrapper around a Class 2 measure is Class 2.
- **Any measure over a table reachable by a bidirectional relationship.** The filter propagation differs before the measure is even evaluated.
- **`BLANK()`-dependent arithmetic**, as above, when the author clearly relied on it.

For each, record: the measure, the column whose filter is overwritten, the filter context in which it diverges, and the number from both sides at that context. That table is what you hand the user.

### Class 3: Untranslatable

No Malloy equivalent. Do not fake one. Each needs a rewrite of the intent or an explicit decision to drop it.

- **Time intelligence**: `TOTALYTD`, `SAMEPERIODLASTYEAR`, `DATEADD`, `DATESYTD`, `PARALLELPERIOD`, `PREVIOUSMONTH`. These depend on a marked date table with a contiguous date column. The intent translates; the function does not.
- **Context transition over a filtered table**: `SUMX(FILTER(T, cond), expr)` and friends, where the iterator establishes row context that a nested measure then transitions. Often the intent is a simple filtered aggregate and the DAX is more complicated than the question. Ask what the number means before translating the code.
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
