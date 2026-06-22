# Normalized Schemas (3-Stage Pattern)

Use this pattern when data is in normalized/relational form, ER-diagram style schemas, application databases (CRM, ERP, OLTP), many tables linked by foreign keys, no clear single fact table, or many-to-many relationships.

For most star/snowflake schemas, the base source + joined source layers are sufficient.

**Why multiple sources?** Different sources answer different questions.
- `deals` with `join_one: company` → counts only companies WITH deals
- `companies` with `join_many: deals` → counts ALL companies

## The 3 Stages

| Stage | Purpose | Naming |
|-------|---------|--------|
| 1. Base | Raw tables + single-table dimensions | `_xxx_base` |
| 2. Modeled | Forward joins (each relationship defined once) | `_xxx_modeled` |
| 3. Query Sources | Reverse joins + measures (public) | `xxx` (no prefix) |

## Example

```malloy
// Stage 1: Base sources
source: _companies_base is duckdb.table('company') extend {
  primary_key: _id
  dimension:
    company_name is name
    size_bucket is employee_count ?
      pick 'Small' when < 50 pick 'Medium' when < 500 else 'Large'
}
source: _deals_base is duckdb.table('deal') extend { primary_key: _id }

// Stage 2: Modeled (forward joins, defined once)
source: _companies_modeled is _companies_base extend {}
source: _deals_modeled is _deals_base extend {
  join_one: company is _companies_modeled on company_id = company._id
}

// Stage 3: Query sources (reverse joins + measures)
source: companies is _companies_modeled extend {
  join_many: deals is _deals_modeled on _id = deals.company_id
  measure:
    company_count is count()
    deal_count is count(deals._id)
}
source: deals is _deals_modeled extend {
  measure:
    deal_count is count()
    total_value is sum(amount)
}
```

## CRITICAL: Reuse Modeled Sources

In query sources, join to MODELED sources, not raw tables:

```malloy
// WRONG - joins raw table, loses dimensions/measures
source: accounts is _accounts extend {
  join_many: trans is conn.table('financial.trans') on ...
}

// RIGHT - joins the modeled source
source: accounts is _accounts extend {
  join_many: trans is transactions on account_id = trans.account_id
}
```
