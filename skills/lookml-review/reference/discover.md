# LookML Discovery (Step 1)

> Inventory a LookML project, classify its contents, extract architecture-level candidates, and capture prior-art notes in the conversation. Does NOT extract individual field definitions; that's deferred to `propose-fields.md`.

## 1. Locate `.lkml` Files

Scan the project directory and immediate subdirectories (especially `lookml/`, `lkml/`, `looker/`, or any directory containing `.lkml` files). Ask the user to confirm: "I found LookML files. Use as prior art?"

## 2. Categorize by File Type

| File Pattern | Type | Priority |
|-------------|------|----------|
| `manifest.lkml` | Manifest | Read FIRST: contains constants |
| `*.model.lkml` | Model | Connection name, includes, datagroups |
| `*.view.lkml` | View | Source-level knowledge (dimensions, measures) |
| `*.explore.lkml` | Explore | Relationship-level knowledge (joins) |
| `*.dashboard.lookml` | Dashboard | **SKIP entirely**: analysis is a separate workflow |

## 3. Resolve Manifest Constants

Read `manifest.lkml` and build a lookup table. LookML references like `@{BILLING_TABLE}` must be resolved to actual table names before analysis.

```
constant: BILLING_TABLE {
  value: "instance.billing.gcp_billing_export_public"
}
# → @{BILLING_TABLE} resolves to "instance.billing.gcp_billing_export_public"
```

## 4. Extract Connection Info

From model files, note the `connection:` value. This is the **LookML connection name**; it may differ from the connection name used in the Malloy model. In LookML-only mode, use as fallback and flag as unverified.

## 5. Extract Source Candidates from Views

For each `.view.lkml` file, extract architecture-level info only:

- **Table reference:** `sql_table_name:` → direct table, `derived_table:` → classify (see `build-derived-tables.md`)
- **Primary key:** field with `primary_key: yes`
- **Grain:** inferred from PK and table name
- **Role:** Fact (has measures, transactional) or Dimension (lookup, one row per base source)

### Identify Refinements

- **Base view:** `view: view_name { ... }`
- **Refinement:** `view: +view_name { ... }`: extends an existing view

Present the refinement structure to the user for a consolidate/extend decision.

## 6. Extract Source Candidates from Explores

For each explore, extract:

- **Base view** and **joined views** with cardinality (`relationship:` → `join_one:` / `join_many:` / `join_cross:`)
- **Join conditions** (`sql_on:`): translate `${view.field}` references
- **UNNEST joins**: flag any `sql:` containing `UNNEST()` (BigQuery nested fields)
- **`sql_always_where:`**: document as context, do NOT bake into Malloy
- **`hidden: yes`** on explores: flag for scope decisions

## 7. Quality Evaluation: Flag Situations

Classify patterns into KEEP, SKIP, and FLAG categories:

**KEEP:** dimension/measure names, aggregation formulas, PKs, join relationships, simple SQL refs, CASE/WHEN logic, filtered aggregates, value_format patterns, yesno dimensions.

**SKIP:** `link:`, `drill_fields:`, `html:`, `action:`, `parameter:` (note intent), Liquid templates (strip, note intent), `datagroup_trigger:`, PDT optimization keys, `convert_tz:`, dashboards, `view_label:`, `group_item_label:`.

**FLAG for user decision:**

| Pattern | Default Recommendation |
|---------|----------------------|
| Complex SQL dimensions (50+ lines) | Simplify or push upstream |
| `derived_table { sql: ... }` with complex SQL | Classify as performance-only vs transformation |
| `sql_always_where:` constraints | Document as context, don't bake in |
| Synthetic primary keys (`generate_uuid()`, `concat()`) | Ask about actual grain |
| Refinement structure (`+view` across files) | Consolidate or preserve layering? |

## 8. Extract Visibility Seeds

Scan for fields with non-default visibility. Capture a compact summary (not full field extraction):

- `hidden: yes` fields: note whether they're join keys, intermediate calcs, or UI clutter
- `fields` parameter exclusions (explore/join level)
- `required_access_grants`: flag for user decision

## 9. Extract Documentation Seeds

Scan for fields with high-quality `description:` values worth preserving as `#(doc)` tags. Capture source, field name, and the description text.

## 10. Capture Prior-Art Notes

Hold a lean routing summary in the conversation with these sections:

- **Source**: Type, Location, Mode, Confidence
- **Source Candidates**: table of sources with table, grain, PK, role
- **Source Candidates**: table of sources with base source, joins, notes
- **Flags**: numbered list of situations requiring attention
- **Visibility Seeds**: compact table of non-default visibility fields
- **Documentation Seeds**: compact table of fields with good descriptions
- **Decisions Made During Discovery**: refinement choice, connection name resolution, etc.

No field-level detail beyond seeds. Architecture + flags + decisions only.
