# Query-Based Sources (Derived/Pre-Aggregated)

When you need a pre-aggregated or windowed source, use a Malloy query as the source definition. **Do NOT use `conn.sql()` unless there is no Malloy equivalent.**

## Pre-aggregation (group_by + aggregate)
```malloy
source: user_facts is conn.table('orders') -> {
  where: status != 'Cancelled'
  group_by: user_id
  aggregate:
    lifetime_revenue is sum(sale_price)
    order_count is count(order_id)
} extend {
  primary_key: user_id
  dimension: is_vip is lifetime_revenue > 500
}
```

## Window functions (calculate)
```malloy
source: order_sequence is conn.table('orders') -> {
  group_by: order_id, user_id
  aggregate: order_time is min(created_at)
  calculate: sequence is row_number() {
    partition_by: user_id
    order_by: order_time
  }
} extend {
  primary_key: order_id
  dimension: is_first_order is sequence = 1
}
```

## Pipeline (window + filter)
```malloy
source: first_touch is conn.table('events') -> {
  where: user_id is not null
  group_by: user_id, channel
  aggregate: first_at is min(created_at)
  calculate: rank is row_number() {
    partition_by: user_id
    order_by: first_at
  }
} -> {
  where: rank = 1
  select: user_id, first_channel is channel
} extend {
  primary_key: user_id
}
```

## Key Rules

- **Cannot redefine** columns from query-based sources, they already exist as fields. Add only NEW derived dimensions in `extend {}`.
- To add `#(doc)` tags to existing query columns, use `include {}` between the query and extend.
- **Use the RAW TABLE** in query-based sources, not a modeled source, when the modeled source would create a circular dependency.
- **Never use `conn.sql()`** when Malloy has a native pattern. `conn.sql()` is a last resort for UNNEST, PIVOT, or dialect-specific functions only. Call `malloy_searchDocs` first.
