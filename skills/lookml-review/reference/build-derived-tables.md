# LookML Derived Table Conversion (Step 5)

> Classify LookML derived tables and convert them to Malloy patterns. Reference `_concepts.md` for syntax translation. This runs during Step 5 (BUILD) when the prior-art notes flag derived tables.

## Decision Tree

```
derived_table:
├── explore_source: → NDT path
│   ├── Simple aggregation → Malloy from() extend {}
│   ├── With derived_column: (window functions) → Malloy window function patterns
│   ├── Chained NDTs → dependency ordering, multi-stage computed source
│   └── With bind_filters → flag, no direct Malloy equivalent
└── sql: → PDT path
    ├── Performance-only → strip to base table
    └── Transformation → recommend base table + Malloy dims or upstream dbt
```

## NDT Path (`explore_source:`)

### Simple Aggregation NDT

Express the aggregation as a Malloy query, then build a source from it:

```malloy
source: source_name is from(
  base_source -> {
    group_by: group_field
    aggregate:
      metric_one is count()
      metric_two is sum(amount_field)
  }
) extend {
  primary_key: group_field
  dimension: derived_dim is metric_one > 1
}
```

### NDT with `derived_column:` (Window Functions)

LookML `derived_column:` adds window functions on top of the NDT result. Map to Malloy window function patterns. Call `malloy_searchDocs("window functions")` for current syntax.

### Chained NDTs

When one NDT references another's explore_source, determine the dependency order. Build the upstream computed source first, then reference it in the downstream one. Each becomes its own `.malloy` file.

### NDTs with `bind_filters`

No direct Malloy equivalent. Flag for user and propose alternatives:
- Parameterized source (if Malloy supports it; check `malloy_searchDocs`)
- Pre-filtered views
- Document the intent and let the user decide

## PDT Path (`sql:`)

### Performance-Only PDT

**Signal:** SQL is essentially `SELECT *, generate_uuid() as pk FROM table WHERE increment_condition` with partitioning/clustering/incremental keys.

**Action:** Use the base table directly. The PDT optimization is Looker-specific. Strip to the actual `FROM` table, resolve any manifest constants (`@{TABLE_NAME}`), and use `conn.table('resolved_table')`.

**Synthetic PK investigation:** If the PDT generates a synthetic PK (`generate_uuid()`, `concat(field_a, field_b)`), the actual grain is unknown. Run queries to determine it:

```malloy
// Check if candidate columns form a unique grain
run: source -> {
  group_by: candidate_pk_field
  aggregate: row_count is count()
  having: row_count > 1
  order_by: row_count desc
  limit: 10
}
```

### Transformation PDT

**Signal:** SQL has JOINs, CTEs, complex WHERE clauses, or computed columns.

**Action:** Flag for user. Recommend:
1. Use the base table + Malloy dimensions for the computed columns
2. Push the transformation to upstream dbt/SQL
3. Only carry forward verbatim if user insists (last resort)

**CRITICAL: Never use `conn.sql()` when Malloy has a native pattern.** For aggregation, window functions (`calculate`), and filtering, use Malloy query-based sources (`table -> { group_by, aggregate } extend {}`). `conn.sql()` is a last resort for patterns with NO Malloy equivalent (UNNEST, PIVOT, dialect-specific functions). Call `malloy_searchDocs` before writing any SQL block.

## Examples

Grounded in real patterns from `lookml/block-google-cloud-billing/` and `lookml/bq_thelook/`.
