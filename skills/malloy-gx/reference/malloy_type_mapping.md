<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy -> Great Expectations: what to check and why

## Why we profile query results, not `.malloy` source

A Malloy `dimension:` or `measure:` declaration doesn't carry an explicit
type in the source text most of the time -- `dimension: gross_margin is
sale_price - products.cost` gets its type from the expression and the
underlying table's schema, which lives in the database, not the model file.
Parsing `.malloy` text for types means re-implementing enough of Malloy's
type inference to get it right, and it would still miss the thing you
actually care about for a data-quality suite: what values a field *really*
takes. Running the query and profiling the result is both simpler and more
honest about what's being checked.

The one thing worth grepping the model for is `primary_key: <field>` inside
a `source:` block -- if you know a query result includes that field, treat
it as a stronger uniqueness signal than the name-based heuristic in
`generate_expectations.py` (which just looks for `id`, `_id`, `key`,
`_key` suffixes).

## Heuristics `generate_expectations.py` uses, and when to override them

| Signal | Expectation added | Override when |
|---|---|---|
| Column has zero nulls in the sample | `ExpectColumnValuesToNotBeNull` | The sample got lucky and the field is legitimately sometimes null (e.g. an optional join) -- loosen with `mostly=` or drop it |
| Column has some nulls | `ExpectColumnValuesToNotBeNull(mostly=...)` at the observed rate | You know the *real* acceptable null rate differs from this sample's -- set `mostly` by hand |
| Column name looks like a key (`id`, `*_id`, `*key`) and every value in the sample is distinct | `ExpectColumnValuesToBeUnique` | The name is a false positive (e.g. `valid` isn't a key despite containing no `_id`... this rarely fires wrongly, but double check on a source you don't recognize) |
| Numeric column | `ExpectColumnValuesToBeBetween(min, max)` padded ±10% | The metric is monotonically growing (running totals, cumulative counts) -- a fixed range will eventually fail on schedule, not on a real anomaly. Consider dropping this expectation or checking day-over-day delta instead |
| String/categorical column with <=20 distinct values, and distinct count is <=5% of row count | `ExpectColumnValuesToBeInSet` | The category list is expected to grow (e.g. `product_sku` looks categorical in a small sample but isn't closed) -- drop it, or re-generate periodically instead of treating it as fixed |
| Row count | `ExpectTableRowCountToBeBetween` padded ±25% | The query result size varies a lot by design (e.g. a `where:` filter driven by a given) -- widen the pad or drop this one |

The paddings (10% for numeric ranges, 25% for row count) are deliberately
loose. A freshly generated suite is meant to catch gross breakage (a join
that silently drops rows, a metric that goes negative when it shouldn't,
a category that starts returning `NULL` instead of a real value) --
not to encode this exact sample as gospel. Tighten specific expectations
by hand once you know which ones represent a real invariant.

## Malloy value types -> pandas/GX correspondence

Publisher's REST/MCP query endpoints return JSON, which `malloy_client.py`
loads into a pandas DataFrame with `pandas.DataFrame(rows)` -- so the
*effective* type of each column, for GX's purposes, is whatever pandas
inferred from the JSON values, not Malloy's own type system. Rough mapping,
for when you're deciding whether a generated expectation makes sense:

| Malloy type | Typical JSON shape | pandas dtype | Relevant GX expectations |
|---|---|---|---|
| `string` | string | `object` | `ExpectColumnValuesToBeOfType`, `ExpectColumnValuesToBeInSet`, `ExpectColumnValueLengthsToBeBetween` |
| `number` (integer) | number | `int64` | `ExpectColumnValuesToBeBetween`, `ExpectColumnValuesToBeOfType` |
| `number` (float) | number | `float64` | `ExpectColumnValuesToBeBetween` |
| `boolean` | true/false | `bool` | `ExpectColumnValuesToBeInSet(value_set=[True, False])` |
| `date` / `timestamp` | ISO string | `object` (unless you `pd.to_datetime` it first) | `ExpectColumnValuesToBeBetween` after parsing to datetime, or `ExpectColumnValuesToMatchStrftimeFormat` |

If a date/timestamp column matters for validation, parse it explicitly
(`df[col] = pd.to_datetime(df[col])`) before profiling or validating --
pandas will otherwise treat it as an opaque string column and you'll only
get string-shaped expectations out of it.

## Measures vs. dimensions

A query result mixes grouped dimensions with aggregated measures. Row-level
expectations (uniqueness, null checks, categorical sets) are most meaningful
on dimension columns -- a measure is already a summary, so "not null" or
"in this set" rarely says anything useful about it. Range checks
(`ExpectColumnValuesToBeBetween`) are the one kind that's usually still
worth having on a measure: they catch a metric going negative, exploding,
or collapsing to zero when it shouldn't.
