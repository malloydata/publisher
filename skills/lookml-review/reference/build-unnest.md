# LookML UNNEST & Struct Conversion (Step 5)

> Convert LookML UNNEST joins and struct field access patterns to Malloy. This runs during Step 5 (BUILD) when the prior-art notes flag UNNEST joins or struct access.

## UNNEST Join Patterns

LookML joins containing `UNNEST()` in the `sql:` parameter are unnesting nested/repeated fields (common in BigQuery). Example from `gcp_billing_export.explore.lkml`:

```
# LookML: unnests a repeated field
join: gcp_billing_export__labels {
  sql: LEFT JOIN UNNEST(${gcp_billing_export.labels}) as gcp_billing_export__labels ;;
  relationship: one_to_many
}
```

Malloy handles nested structures natively. The conversion depends on whether you want to:
1. **Keep nested**: access nested fields directly via the parent source
2. **Flatten**: create a separate source from the unnested data

Call `malloy_searchDocs("nested repeated")` for current Malloy syntax for nested/repeated field handling.

## Struct Field Access

LookML accesses struct fields via `${TABLE}.struct.field` syntax:

```
# LookML
dimension: project_id { sql: ${TABLE}.project.id ;; }
dimension: adjustment_description { sql: ${TABLE}.adjustment_info.description ;; }
```

In Malloy, struct field access uses dot notation directly. Check `malloy_searchDocs("struct")` for the current syntax.

## Multiple Views from One Table

LookML often creates separate views for each unnested array (e.g., `gcp_billing_export__labels`, `gcp_billing_export__credits`, `gcp_billing_export__system_labels`). These all derive from the same base table.

**Options:**
1. **Consolidate**: model as nested sources within the parent source
2. **Keep separate**: create individual sources if they have distinct analytical value

Present options to the user with the tradeoffs.

## Flattened Naming Conventions

LookML flattens nested field names with double underscores: `project__id`, `labels__key`, `labels__value`. In Malloy, use cleaner names that reflect the actual structure. Propose renames during Step 4 field proposals.
