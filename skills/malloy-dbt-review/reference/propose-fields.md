# Propose Fields from dbt (Step 4)

> Turn dbt's entities, dimensions, and metrics into field proposals the user can confirm. dbt
> supplies the candidate list and the business language; the data supplies the evidence.

Read `_concepts.md` for the mapping table before proposing.

## What dbt has already decided for you

dbt's semantic layer is reviewed prior art, so most of the usual proposal work is done:

- **Keys and joins** come from entities, with cardinality declared rather than guessed
  (`discover.md` § 3).
- **Measures** come from metrics, with a business description and often a display label.
- **Views** come from saved queries.

That makes the proposal step shorter, but not automatic. dbt's definitions are hypotheses to
verify by query, not facts to paste. Metadata drifts; the data is the authority.

## Verify before proposing

For each proposed field, run the query that proves it:

| Proposal | Evidence to gather |
|---|---|
| `primary_key` from a `primary` entity | Row count equals distinct key count, and the key is never null |
| `join_one` from a `foreign` entity | Local key's distinct count against the target's; orphan rate. A `relationships` test in dbt is supporting evidence, not a substitute |
| A measure over a column | The column is populated and its type aggregates as expected; a `sum` over a string is a cast waiting to fail |
| A filtered measure | The filter matches a non-trivial share of rows. A filter matching 0 rows usually means an unresolved dimension name |
| A ratio | The denominator is non-zero often enough to be meaningful |

If a dbt metric's definition and the data disagree, the data wins and the disagreement is an item
for the user, not something to quietly reconcile in the model.

## Naming

Keep dbt's metric names. They are the vocabulary the business and its agents already use, and
changing them silently breaks the continuity that makes the conversion worth doing. When a metric
name collides with a column name, move the *column* in the binding layer, not the metric
(`_concepts.md` § Naming, `build-bindings.md` rule 4).

Do not carry over MetricFlow's mechanical names: `order_id__order_total_dim` is a qualified
internal reference, not a business term.

## Present the proposal

Group by source, and for each field give the dbt origin, the proposed Malloy, and the evidence.
Flag anything that needs a decision instead of burying it:

```
orders (dbt mart `orders`, 9,568 rows, one row per order_id - verified unique and not-null)

  primary_key: order_id                      dbt entity `order_id` (primary)
  join_one: customers with customer_id       dbt entity `customer` (foreign); 0 orphans
  join_one: locations with location_id       dbt entity `location` (foreign); 0 orphans

  measure: order_total is order_total_raw.sum()   dbt metric `order_total` (sum)
      NOTE: column `order_total` renamed to `order_total_raw` in the binding so the
            metric can keep dbt's name.
  measure: large_orders is count() { where: order_total_raw >= 20 }
      dbt metric `large_orders`, filter resolved from Dimension('order_id__order_total_dim')
      1,141 of 9,568 orders match.

  DEFERRED: median_revenue - no scalar median in this Malloy build. Not substituting avg.
  AS A VIEW, NOT A MEASURE: revenue_growth_mom (dbt offsets an input by 1 month)
```

Every threshold that came from dbt is still a business convention: `>= 20` for a "large" order is
dbt's choice, and it belongs in the measure's `#(doc)` as a stated convention rather than an
unexplained constant. See `skill:malloy-document`.
