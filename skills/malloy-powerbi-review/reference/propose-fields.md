<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Field Proposals from Power BI (Step 4)

> Turn tables, columns, and relationships into Malloy source, dimension, and join proposals. Measures are handled separately in `translate-measures.md`.

Read `_concepts.md` for the type and relationship mapping tables.

## 1. One Source per Table

Each non-hidden, non-auto-date table becomes a source pointing at the real table from the M partition (see `discover.md` step 4).

```malloy
source: sales is conn.table('dbo.FactSales') extend {
  primary_key: sales_key
}
```

Use the connection name from the Malloy model. **The Power BI data source name is not the Malloy connection name**; treat it as a hint only, and in Power BI-only mode flag it as unverified.

## 2. Columns Need No Declaration

A plain column is already reachable in Malloy. Do not emit a `dimension:` that restates a column under its own name. Declare a dimension only when it does something:

- **A rename.** Power BI column names are often display names with spaces (`'Net Revenue'`). Where the warehouse column differs from the name the business uses, declare `dimension: net_revenue is NetRevenue` and consider `internal:` on the raw column.
- **A calculated column.** Translate the DAX with `translate-measures.md`, then decide: a Malloy dimension if it is row-level and cheap, a computed source if it aggregates, or upstream work if it is doing a job the warehouse should do.
- **A type correction.** A `dateTime` column used only as a date, a `decimal` that should be typed for money.

## 3. Naming

Power BI names are display names: spaces, title case, sometimes punctuation. Malloy identifiers are not. Propose snake_case, and keep a mapping table so the parity report can refer to measures by the name the business knows.

Where the display name carries real information the identifier loses, keep it with a `# label=` tag rather than mangling the identifier.

## 4. Joins from the Relationship Graph

Relationships are declared once at model level, not per table, so build the graph first (`discover.md` step 7) and then attach joins to the fact sources.

A standard active many-to-one relationship, where `fromColumn` is the fact side:

```malloy
source: sales is conn.table('dbo.FactSales') extend {
  join_one: customer with customer_key
  join_one: product with product_key
}
```

Three cases need a decision rather than a translation:

- **Inactive relationships.** They exist to be switched on by a specific measure. Do not emit a join for one until you know which measures use it; then it is usually a second source or a second join with a different name, not a replacement.
- **Bidirectional.** Ask what it was for. Usually a many-to-many bridge, a slicer that needed to filter backwards, or an accident. Only the first survives translation, as an explicit bridge source.
- **Many-to-many.** Find the bridge table that should exist and model it.

## 5. Grain and Primary Keys

Power BI does not require a declared primary key. The one side of a relationship implies a key column, which is usually the real one. Propose `primary_key:` from the relationship graph, and verify against the data where a connection exists:

```
run: source -> { aggregate: rows is count(), keys is count(distinct key_col) }
```

If those differ, the relationship was working on a non-unique column, which in Power BI silently fans out. Flag it: the Power BI numbers may already be wrong, which is a finding worth delivering carefully.

## 6. Order of Proposals

Follow the fact and dimension split from discovery. Propose dimension sources first, since fact sources join them, and confirm each dimension's grain before building anything on it.

## 7. What Not to Propose

- A dimension per column just because the column exists
- Anything from an auto date table
- Hierarchy objects: Malloy has none, and the level order is drill intent, not structure
- Display folders, lineage tags, ordinals
