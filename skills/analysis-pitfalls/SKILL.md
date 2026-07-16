---
name: analysis-pitfalls
description: Common data analysis pitfalls to watch for during query construction and result interpretation. Reference this checklist when verifying queries and results to catch errors before presenting an answer.
---
# Data Analysis Pitfalls

Watch for these common mistakes throughout the analysis workflow. When you encounter one, fix it before presenting results.

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Query Construction

### Wrong grain / fan-out
Using dimensions or measures from a joined source that has a finer grain than the base source can silently multiply rows, inflating aggregates. For example, aggregating revenue while grouping by a line-item field may double- or triple-count totals. If your query touches fields from a joined source, compare `count(key_field)` to `count()`: if the row count is significantly higher than the distinct key count, you likely have fan-out.

### Invented entity names
Never guess field names. Use only the exact field paths defined in the model (find them with `get_context`). A plausible-sounding name that does not exist in the model will produce an error, or worse, silently reference the wrong field.

### Mismatched filter values
Dimensional values are case-sensitive and format-specific. Common mismatches include case differences ("Nike" vs "NIKE" vs "nike, inc."), partial matches ("New York" when the data has "New York City"), and aliased values ("USA" vs "United States"). A filter on a value that doesn't exist in the data silently returns zero rows without erroring. Always use the exact dimensional values from retrieval results, and if in doubt, run a distinct-values query on the dimension to confirm.

### Filtering on the wrong field
If the user asks to filter by "brand", confirm which dimension corresponds to "brand" in the model. There may be multiple fields with similar names at different levels of the hierarchy.

### Missing filters
If the user asks about "last quarter" but you don't apply a time filter, you are returning all-time data. Always check whether the question implies filters you have not yet applied.

### Misinterpreted entities
A field can exist in the model and still be the wrong choice. For example, using `revenue` (which may be gross) when the question asks about profit, or treating a count measure as if it were a sum. Cross-reference the field definitions and `#(doc)` descriptions in the model to confirm a field means what you think it means.

### Semantic ambiguity
Field names or descriptions sometimes suggest one interpretation while the actual values tell a different story, for example, a field labeled "annual ridership" containing values that look like average weekday traffic, or a field named `revenue` that appears to represent net revenue in practice. When values don't match expectations, note whether the ambiguity affects the answer and call it out.

### Fragile ad-hoc definitions
When defining a new measure or dimension inline, common mistakes include assuming a field is numeric when it's actually a string, ignoring nulls in arithmetic (e.g., `a - b` yields null if either is null), and building logic around values that only cover a subset of the data.

Pay special attention to value coverage when defining a dimension that categorizes or subsets data. It's easy to capture too few values (missing categories that should be included) or too many (grouping in values that don't belong). For example, a `pick` expression that maps tier names to "Premium" and "Standard" might miss a tier that should be Premium, or a filter meant to isolate one product line might inadvertently include related but distinct products. Before relying on such a definition, run a distinct-values query on the underlying field to see the full set of values and confirm your logic handles them all correctly. This applies equally whether you define the logic as a new dimension or apply it directly as a `where` clause: the risk of incomplete value coverage is the same either way.

### Hidden filters in views
Views can have built-in `where` clauses that pre-filter the data. A view named `recent_orders` might only include the last 90 days, or `active_customers` might exclude certain statuses. If the view's definition is available, read it to understand what filters are baked in; they may conflict with what the user is asking for. When in doubt, query the base source directly and apply filters explicitly.

## Result Interpretation

### Implausible magnitudes
If a "total" is suspiciously small or large, question it. Common causes: missing filters (too large), over-filtering (too small), wrong unit (dollars vs. cents), or fan-out from joins (inflated).

### Nulls distorting aggregations
Null values are silently excluded from `avg()` and can make `sum()` results lower than expected. If a significant portion of a field's data is null, aggregations over that field may be misleading. When results seem off, compare total row count to a count of non-null values for the key field to gauge how much data is missing.

### Confusing count vs. count distinct
`count()` counts rows while measures defined with `count(field)` count distinct values of that field. Using a row count when you need distinct values (or vice versa) is a frequent source of inflated or deflated numbers, especially when the query touches joined sources.

### Percentage of what?
When computing percentages or shares, be explicit about the denominator. "30% of revenue" means nothing if you don't confirm what the total revenue is and whether it's filtered the same way.

### Time period mismatches
Comparing metrics across different time periods without normalizing (e.g., comparing a full year to a partial quarter) produces misleading conclusions.

## Verification Signals

### Parts don't sum to the whole
If you break down a total by category, the categories should sum to the total (or close to it, accounting for nulls). If they don't, something is wrong with the grain or filters.

### Row count surprises
Before interpreting results, check whether the row count makes sense. An unexpectedly high row count often indicates fan-out from a join. An unexpectedly low count may mean an overly restrictive filter.

### Zero or empty results
If a query returns no rows, don't report "there is no data." First verify that your filters are correct and the field names are right. The absence of results is usually a query problem, not a data problem.
