---
name: malloy-discover
description: Silent data discovery for Malloy modeling. Used at Step 1 of the modeling workflow. Scans tables, columns, distributions, and relationships without user interaction. The agent builds an internal picture before presenting anything.
---

# Data Discovery (Step 1, Silent)

> **CRITICAL**: Read the model before writing ANY Malloy code. The model defines the sources, connection names, and fields. Never guess connection names.

> **PREREQUISITE:** Make sure the Publisher MCP tools (`malloy_packageGet`, `malloy_modelGetText`, `malloy_executeQuery`, `malloy_searchDocs`) are configured and reachable. If they are not, stop and resolve the MCP connection before continuing.

**This step is silent.** The agent does not present findings to the user yet. That happens in the next step (PROPOSE SCOPE).

## Tools

- **`malloy_packageGet`** / **`malloy_modelGetText`**: Read the model to see the sources, connection names, and fields it already defines. Call FIRST. The sources and their join paths are the schema you build on.
- **`malloy_executeQuery`**: Run ad-hoc queries to preview data, verify values, check NULLs, validate assumptions.
- **`malloy_searchDocs`**: Get Malloy syntax help when needed.

## Workflow

```
1. Check for prior art signals                 → If found, ask user: "I found [LookML/dbt] files, use as prior art?"
2. If user confirms: read adapter reference        → Follow skill:lookml-review, keep prior-art notes in-conversation
3. malloy_packageGet / malloy_modelGetText     → Read the model: sources, connection names, fields
4. Inspect source definitions                  → See ALL fields and join paths for key sources
5. Derive candidate joins/dimensions/measures  → Read them off the model and the data, not a suggestion tool
6. Define a minimal source if one is missing   → Just enough to run malloy_executeQuery for previews
7. malloy_executeQuery(query)                  → Preview data, verify values, check NULLs, check duplicates
8. malloy_searchDocs(query)                    → Get syntax help when needed
9. Proceed to Step 2 (PROPOSE SCOPE)
```

**If the model has no sources defined** and no LookML files are present, do NOT silently retry or proceed without data. Tell the user: "No model sources were found. Please check that the package points at a connected data source, then try again."

**If the model has no sources defined** but LookML files ARE present (LookML-only mode), skip steps 3-7. Use connection name and table paths from the LookML review. Flag all proposals as unvalidated.

**Key principle:** Query data to verify assumptions. Don't ask the user to confirm values you can check yourself.

**Search docs proactively.** If you discover patterns that need derived/pre-aggregated sources, window functions, or unfamiliar features, call `malloy_searchDocs` BEFORE writing code, not just when you hit errors.

## Query File for Discovery

**In the schema-first workflow:** Run ad-hoc queries with `malloy_executeQuery`. If the source you want to preview is not yet defined in the model, define a minimal one against the connection and table so you can run previews. The real model fields are built in later steps.

```malloy
// minimal source for previewing data during discovery
source: explore is my_conn.table('schema.table') extend {}
```

**In analysis-first mode:** There is no temp file. The analysis `.malloy` file IS your working file. It grows throughout the session and becomes the input for formalizing into a model. See `skill:malloy-analyze` for that workflow.

## What to Capture

When reviewing tables and columns, capture:

### Table-Level
- All tables with row counts
- Connection name and schema (CRITICAL, never guess)
- Table roles: fact, dimension, bridge, lookup, staging, operational
- Join relationships (FK → PK mappings)

### Column-Level
- Primary key and foreign key columns
- Data types (watch for string dates, arrays, JSON)
- Reserved word columns that need backticking (`Date`, `Type`, `number`, `source`, etc.)
- Column cardinality and NULL rates (via `malloy_executeQuery`)
- Data distributions for key numeric and categorical columns

### Data Quality
- **Check for duplicate rows** on primary keys. Run `group_by: pk, aggregate: count(), having: count() > 1` on each key table. Duplicates cause `sum()` to return nonsensical values.
- **Denormalized count columns**: beware pre-aggregated fields (e.g., `order_count` in a customer table) that may conflict with joined counts.
- **Delimited list columns**: flag string columns containing comma-separated values.

### Data-Driven Validation

**Every recommendation must be grounded in queried data, not schema inference.** During discovery, run `malloy_executeQuery` to validate assumptions before proposing anything in later steps.

| What to validate | Query to run |
|-----------------|-------------|
| **Denormalized vs joined values** | Compare pre-computed columns (e.g., `customers.order_count`) against the actual joined aggregate (`count()` from `orders`). Report discrepancy rate. If >0%, flag for user decision. |
| **Candidate date fields** | When multiple date/timestamp columns exist, query both. What % of rows differ? By how much? This informs which is canonical. |
| **Numeric column distributions** | Query min, max, avg, percentiles (p25, p50, p75, p95). These inform tier boundaries and detect outliers. |
| **Categorical column cardinality** | Query distinct values. A `status` column with 5 values behaves differently from one with 500. |
| **Column usefulness** | Query NULL rates. Columns that are >95% NULL are candidates for `internal`. |
| **Join cardinality** | Query FK uniqueness: `group_by: fk_col, aggregate: row_count is count(), having: row_count > 1`. Determines `join_one` vs `join_many`. |
| **Revenue/amount columns** | When multiple money columns exist (`total`, `subtotal`, `amount`, `price`), query a sample to understand how they relate (does `total = subtotal + tax`?). |
| **Join key value compatibility** | For every proposed join, sample 5-10 actual values from each side. Check for format mismatches: abbreviations ("4th Av" vs "4 Avenue"), ordinals ("23 St" vs "23rd St"), casing, prefixes. Mismatched values mean the join won't work even if column names match. |
| **Mixed-grain rows** | For each key table, run top-N and bottom-N by primary metric. Look for summary/aggregate rows mixed with detail data (e.g., "System Total" rows in a station-level table). These corrupt measures if not filtered out. |

**Never assume from column names.** Always query the data to confirm. A column named `total` could include or exclude tax. A `status` column could have unexpected values. A FK could have orphaned references.

### Example Queries

**Tier boundaries**: query distribution, propose breaks from percentiles:
```malloy
run: orders -> {
  aggregate:
    min_val is min(sale_price), p25 is sale_price.percentile(25)
    median_val is sale_price.percentile(50), p75 is sale_price.percentile(75)
    p95 is sale_price.percentile(95), max_val is max(sale_price)
}
```

**Denormalized vs joined**: compare pre-computed column against real aggregate, report match rate:
```malloy
run: customers -> {
  join_many: orders on customer_id = orders.customer_id
  aggregate:
    total is count()
    match is count() { where: order_count = count(orders.order_id) }
}
```

**Canonical date**: when multiple date columns exist, check how often they differ:
```malloy
run: orders -> {
  aggregate:
    total is count()
    same_date is count() { where: created_at::date = submitted_at::date }
    max_gap_days is max(days(submitted_at - created_at))
}
```

**Revenue columns**: when multiple money columns exist, verify their relationship:
```malloy
run: orders -> {
  aggregate:
    total_eq_parts is count() { where: abs(sale_price - (subtotal + tax)) < 0.01 }
    total is count()
}
```

### Schema Shape
- Is this a star/snowflake schema (use base + joined source layers) or normalized/ER-style (may need 3-stage pattern)?
- Combined vs split tables: prefer filtered/split tables over combined when both exist.

## Computed Source Detection

Flag potential computed sources when:

1. **Grain mismatch**: the analytical scope requires a grain that no physical table provides (e.g., customer-level metrics from an order-grain table)
2. **Repeated aggregation patterns**: the same GROUP BY + aggregate pattern would be needed in multiple analyses
3. **Cross-entity aggregations**: the model or the data implies cross-entity aggregations that require a pre-aggregated entity

## Prior Art Detection

Check for prior art signals at the start of discovery. If a signal is found and the user confirms, **you MUST read** the corresponding reference skill and follow its instructions.

| Signal | Source Type | Reference to Read |
|--------|------------|-------------------|
| `.lkml` files in project or subdirectories | lookml | `skill:lookml-review` |
| `dbt_project.yml` in project or parent dirs | dbt | dbt review (future) |

The reference handles inventory, classification, and produces prior-art notes. Keep those notes in-conversation, then continue with normal discovery below.

**If DB connection available (LookML + DB mode):**
- Read the model and run `malloy_executeQuery` as normal
- Use prior art as additional context, not a replacement for data validation
- **The LookML connection name is NOT the Malloy connection name.** Always use the connection name from the model.

**If no DB connection (LookML-only mode):**
- Skip the model-read and `malloy_executeQuery` steps
- Use connection name and table paths extracted from prior art source files
- Flag all proposals in Steps 2-4 as **unvalidated**
- Proceed directly to Step 2 (PROPOSE SCOPE)

**Prior art findings enhance discovery, they don't replace it.** When a DB connection is available, always validate assumptions against the actual data.

## After Discovery

Do NOT present findings to the user yet.

## Done

Step complete. Output: discovery findings (internal: tables, columns, relationships, data quality, prior art). Continue to the next modeling step (see `skill:malloy-modeling`).

## Verify Source Joins

When reading joins off the model or the data, watch for `join_many` where the actual relationship is many-to-one. Always verify cardinality. Prefer `join_one` when each row in the primary table matches at most one row in the joined table.
