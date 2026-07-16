# LookML Field Extraction (Step 4)

> Read `.lkml` view files and extract field proposals for the Malloy model. Reference `_concepts.md` for type mapping. This runs during Step 4 (PROPOSE DEFINITIONS) to provide LookML-sourced field proposals alongside schema-derived proposals.

## Prerequisites

- Prior-art notes exist with Type: lookml and a Location path
- Read `reference/_concepts.md` for the LookML → Malloy type mapping table

## Extract Fields from Views

Read every `.view.lkml` file at the location from the prior-art notes. For each view in scope (including refinements merged onto their base view):

### For Each Field, Capture:

| Attribute | What to Capture | Maps to |
|-----------|----------------|---------|
| Field name | The LookML field name | Dimension/measure name candidate |
| `type:` | Data type (string, number, date, yesno, etc.) | Malloy type hint |
| `sql:` | The SQL expression | Dimension/measure logic |
| `description:` | Business description | `#(doc)` tag content |
| `label:` | Display name (if different from field name) | `internal:` + `dimension:` or `# label=` candidate |
| `hidden: yes` | Hidden from Explore picker | Classify per `curate-visibility.md` |
| `group_label:` | Logical grouping | Source organization hint |
| `primary_key: yes` | Primary key | `primary_key:` |
| `value_format:` / `value_format_name:` | Display formatting | `# currency`, `# percent`, `# number` |
| `filters:` (on measures) | Filtered aggregate | `{ where: ... }` syntax |

### Handle `dimension_group` (Time Dimensions)

LookML `dimension_group` with `type: time` generates multiple dimensions from one timestamp column. Malloy handles time natively. Extract:
- The **base column** from `sql:` (e.g., `${TABLE}.created_at`)
- Note declared timeframes (signals which granularities were used)
- No explicit dimension needed for each timeframe in Malloy

### Handle `type: yesno`

Boolean flags derived from a SQL condition:
```
# LookML
dimension: is_complete { type: yesno  sql: ${status} = 'complete' ;; }

# Malloy
dimension: is_complete is status = 'complete'
```

### Handle Measure Types

| LookML Measure Type | Malloy Equivalent | Notes |
|---------------------|-------------------|-------|
| `type: count` | `count()` | Malloy `count()` is always distinct |
| `type: count_distinct` | `count(field)` | Direct mapping |
| `type: sum` | `sum(field)` | Direct mapping |
| `type: average` | `avg(field)` | Direct mapping |
| `type: min` / `max` | `min(field)` / `max(field)` | Direct mapping |
| `type: number` | Derived measure expression | Usually a ratio; use `nullif()` for division |
| `type: list` | No direct equivalent | Flag for alternative approach |
| `type: percentile` | `field.percentile(n)` | Direct mapping |

**Filtered measures:** `filters:` → Malloy `{ where: }` syntax:
```
# LookML
measure: completed_revenue { type: sum  sql: ${total} ;;  filters: [status: "completed"] }

# Malloy
measure: completed_revenue is sum(total) { where: status = 'completed' }
```

## Field-Level Visibility Classification

For each field with visibility restrictions, classify per `curate-visibility.md` rules and include the classification in the proposal's `Malloy Treatment` column.

## Present Proposals

Present field proposals with a `Provenance: lookml` column alongside schema-derived proposals. For fields flagged with Liquid/parameter usage in the prior-art notes, note the business intent without trying to convert the mechanism.
