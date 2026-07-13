---
name: malloy-define
description: Propose a source plan and field definitions for a Malloy semantic model. Covers picking which sources to model and at what grain, then proposing the specific renames, dimensions, and measures per source, every proposal backed by querying the data.
---

# Propose sources and definitions

This skill covers two consecutive activities when building or extending a Malloy semantic model:

- **Propose sources**: the architectural blueprint (which sources, what grain).
- **Propose definitions**: the specific fields per base source (renames, dimensions, measures).

Both happen in conversation. Propose, let the user confirm or adjust, then carry the confirmed plan forward into the actual `.malloy` model. There is no separate plan-file store: keep the source plan and field proposals in the conversation, and write the model itself when the user has confirmed. The broader modeling workflow lives in `skill:malloy-modeling`.

Read the existing model first so you propose against what is really there. Use `malloy_getContext` with a plain-English description to inspect the current sources and fields and find the most relevant existing sources. Confirm the scope (which tables are in play) before proposing the source plan.

## Propose a source plan

**Goal:** Propose the full source architecture for the tables in scope.

### Base sources

One base source per table in scope. For each, specify:

| Source | Table | Grain | Primary Key | Role |
|--------|-------|-------|-------------|------|
| orders | sales.orders | one row per order | order_id | Fact, transactions |
| customers | sales.customers | one row per customer | customer_id | Dimension, who |
| products | sales.products | one row per product | product_id | Dimension, what |

### Computed sources

Computed sources are created from queries, not physical tables. Propose them when:

1. **Grain mismatch**: the analytical scope requires a grain that no physical table provides (e.g., customer-level metrics from an order-grain table).
2. **Repeated aggregation patterns**: the same group-by plus aggregate pattern would be used in multiple places.
3. **Cross-entity aggregations**: inspecting the model and querying the data shows that an aggregate rolled up to a different entity would be reused.

For each computed source, explain:

| Source | Source Query | Grain | Rationale |
|--------|-------------|-------|-----------|
| user_order_facts | orders grouped by customer_id | one row per customer | Need customer-level order metrics (LTV, order count, recency) for customer health analysis |

### Dependencies

Show which sources depend on which:

```
customers (physical) ← user_order_facts (derived, sources from orders)
orders (physical) → user_order_facts (derived)
products (physical): independent
```

### Deferred sources

List sources considered but not included, with reasoning:

- **order_items**: bridge table, defer until line-item analysis is needed.
- **monthly_product_facts**: derived, defer until product trend analysis is requested.

### User interaction

The user will:
- **Confirm** the source plan as-is.
- **Add** missing sources (physical or derived).
- **Remove** unnecessary sources.
- **Validate** grain assignments.
- **Defer** sources to later iterations.

Once the source plan is confirmed, carry it forward into the definitions step below. Keep the confirmed map in the conversation rather than persisting it to a separate file.

## Propose definitions

**Goal:** Propose specific fields per base source with data evidence, working from the confirmed source plan.

### For each base source

Present a table of proposed fields.

**Renames (schema cleanup):**

| Raw Column | Proposed Name | Reason |
|-----------|---------------|--------|
| `Order Date` | order_date | Whitespace in column name |
| `Type` | order_type | Reserved word |
| `number` | item_number | Reserved word |

**Dimensions:**

| Field | Logic | Data Evidence | Priority |
|-------|-------|---------------|----------|
| order_status | status column | 5 distinct values: pending, processing, shipped, delivered, cancelled | must-have |
| order_month | submitted_at.month | Time trending | must-have |
| order_size | total buckets (data-driven) | Distribution: min $5, p25 $35, median $85, p75 $150, p95 $450, max $2,400. Proposed breaks at p25/p75: <$35, $35-$150, >$150 | nice-to-have |
| is_returned | returned_at is not null | 8% of orders have non-null returned_at | nice-to-have |

**Data-driven tiers:** For bucketed dimensions like `order_size`, always derive boundaries from the actual data distribution (percentiles, natural breaks, clustering). Query `min`, `max`, `p25`, `p50`, `p75`, `p95` and propose boundaries based on the distribution. Show the evidence so the user can confirm or adjust. Never use arbitrary hardcoded thresholds unless the user explicitly provides them.

**Measures:**

| Field | Logic | Data Evidence | Priority |
|-------|-------|---------------|----------|
| order_count | count() | Basic metric | must-have |
| revenue | sum(total) | Total column includes tax. Range: $5 - $2,400 | must-have |
| avg_order_value | revenue / nullif(order_count, 0) | Derived from above | must-have |
| return_rate | returned_count / nullif(order_count, 0) | 8% overall return rate | nice-to-have |

### For each computed source

Show the source query and additional fields.

**`user_order_facts`**, derived from `orders` grouped by `customer_id`:

| Aggregated Field | Logic |
|-----------------|-------|
| total_orders | count() |
| total_revenue | sum(total_price) |
| first_order_date | min(submitted_at) |
| last_order_date | max(submitted_at) |

**Additional dimensions on top:**

| Field | Logic | Evidence |
|-------|-------|----------|
| days_since_last_order | days(now - last_order_date) | Recency metric |
| is_repeat_buyer | total_orders > 1 | 62% of customers are repeat |
| buyer_frequency | total_orders buckets | Distribution: 1 (38%), 2-4 (35%), 5-19 (22%), 20+ (5%) |

### Business logic questions

Flag decisions the agent can't make from data alone. Be specific and data-grounded:

> **Q1:** Your `orders` table has both `created_at` and `submitted_at`. 87% of rows have them within 1 minute, but 13% differ by 1-3 days. Which should be the canonical order date?
>
> **Q2:** I'm proposing `order_size` tiers based on the data distribution: small (<$35, below p25), medium ($35-$150, p25-p75), large (>$150, above p75). Do these data-driven breaks work for you, or do you have specific business thresholds?
>
> **Q3:** The `status` column has 5 values. Should "cancelled" orders be excluded from revenue calculations, or included with a separate measure?

### Priority ranking

Group proposals into:
- **Must-have**: core metrics that every analyst needs (counts, sums, primary dimensions).
- **Nice-to-have**: useful but not critical (bucketed dimensions, rates).
- **Value-add**: new insights the data supports but may not be asked for yet (computed sources, complex measures).

### User interaction

The user will:
- **Confirm** business logic decisions.
- **Adjust** thresholds and bucket boundaries.
- **Add** missing fields.
- **Remove** fields they don't need.
- **Change** priorities.

Once the definitions are confirmed, write them into the `.malloy` model (see `skill:malloy-modeling`). Use `#(doc)` annotations to document sources and fields, and `#(filter)` annotations to declare server-side filterable dimensions where appropriate. Keep the confirmed definitions in the conversation; there is no separate plan-file store.

## Data-driven proposals

**Every recommendation must be backed by a query result.** Do not propose based on column names or schema structure alone. Always run `malloy_executeQuery` to check the actual data before presenting. To learn what sources and fields exist, ground yourself with `malloy_getContext`: it returns the model's sources, views, and fields, so there is no separate schema-search step.

| Proposal Type | What to query first |
|--------------|---------------------|
| Dimension (bucketed) | Distribution: min, p25, median, p75, p95, max. Propose boundaries from natural breaks, not arbitrary values. |
| Dimension (categorical) | Distinct values and frequencies. Show the actual categories and their counts. |
| Measure (sum/avg) | Sample values: min, max, avg. Verify the column contains what you think (e.g., is `total` gross or net?). |
| Measure (rate/ratio) | Query both numerator and denominator. Verify they make sense together. |
| Denormalized field vs join | Compare the pre-computed column against the joined aggregate. Report match rate. Recommend whichever is more reliable. |
| Computed source | Run the proposed group-by plus aggregation. Verify the grain collapses as expected and the result is useful. |
| Date field selection | Query all candidate date columns. Show % of rows where they differ and by how much. |
| Column rename | Verify the column has data worth exposing (not 100% NULL). |

**Example, denormalized vs joined:**

> "Your `customers` table has an `order_count` column. I compared it against `count()` from the `orders` table:
> - 94% of customers match exactly
> - 6% have stale counts (the denormalized value is lower than the actual count)
> - The max discrepancy is 12 orders
>
> I'd recommend using the joined count from `orders` rather than the denormalized `order_count`. Want to keep the denormalized column as internal, or drop it?"

## Tips

- **Show data, not assumptions:** every proposed dimension or measure should have evidence (distinct values, distributions, ranges).
- **Use `malloy_executeQuery`** to verify any data questions before presenting to the user.
- **Don't over-propose:** 5-8 dimensions and 4-6 measures per base source is usually enough to start.
- **Rank everything:** users appreciate knowing what's essential vs. optional.
- **Business logic questions must be specific.** "What date should I use?" is bad. "Your table has `created_at` and `submitted_at` that differ by 1-3 days in 13% of rows, which is canonical?" is good.

## Output

A confirmed source architecture and a confirmed set of field definitions (renames, dimensions, measures, business decisions), held in the conversation and ready to write into the `.malloy` model via `skill:malloy-modeling`.
