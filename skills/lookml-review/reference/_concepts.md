# LookML → Malloy Concept Mapping

Reference table for translating LookML constructs to Malloy. Referenced by multiple reference files.

| LookML | Malloy | Notes |
|--------|--------|-------|
| `view:` | `source:` (base source file) | One source per physical table |
| `explore:` | `source:` (source file with joins) | One source per analytical domain |
| `dimension:` | `dimension:` | Direct mapping |
| `dimension_group: { type: time }` | `.month`, `.year`, `::date` (native) | Malloy handles time natively; no explicit timeframe list needed |
| `dimension: { type: yesno }` | `dimension: x is condition` | Boolean expression |
| `measure: { type: count }` | `count()` | Always distinct in Malloy |
| `measure: { type: count_distinct }` | `count(field)` | Direct mapping |
| `measure: { type: sum }` | `sum(field)` | Direct mapping |
| `measure: { type: average }` | `avg(field)` | Direct mapping |
| `measure: { type: number }` | Derived measure expression | Usually a ratio; use `nullif()` for division |
| `measure: { filters: [...] }` | `measure { where: condition }` | Filtered aggregate |
| `primary_key: yes` | `primary_key: field_name` | Direct mapping |
| `hidden: yes` | `# hidden` tag (cosmetic) | Classify reason first; see `curate-visibility.md` |
| `fields` exclusion (explore/join) | `internal:` (with access modifiers) | Structurally excluded; `internal:` candidate |
| `required_access_grants` | `private:` (with access modifiers) | Security-restricted; `private:` candidate |
| `description:` | `#(doc)` tag | Direct mapping |
| `label:` (simple rename) | `internal:` old + `dimension: new_name is old_name` | Never use `rename:` |
| `label:` (complex) | `# label="Display Name"` | When name differs from identifier |
| `sql_table_name:` | `conn.table('schema.table')` | Use the connection name from the model definition if available |
| `join: { relationship: many_to_one }` | `join_one:` | Direct mapping |
| `join: { relationship: one_to_one }` | `join_one:` | Direct mapping |
| `join: { relationship: one_to_many }` | `join_many:` | Direct mapping |
| `join: { relationship: many_to_many }` | `join_cross:` | Direct mapping |
| `sql_on: ${a.field} = ${b.field}` | `on a_field = b.b_field` | Translate `${}` references |
| `CASE WHEN ... END` (in SQL) | `pick ... when ... else` | Direct syntax translation |
| `COALESCE(a, b)` | `a ?? b` | Direct mapping |
| `IFNULL(a, b)` | `a ?? b` | Direct mapping |
| `${TABLE}.field` | `field` (direct column reference) | Malloy references columns directly |
| `${view_name.field}` | `view_name.field` (join path) | In join conditions and cross-source refs |
| `+view:` (refinement) | User decides: consolidate or `extend` | Malloy `extend` serves the same purpose |
| `derived_table: { sql: ... }` (perf-only) | Use base table directly | PDT optimization is Looker-specific |
| `derived_table: { sql: ... }` (transformation) | Flag for user | Recommend base table + dims or upstream dbt |
| `derived_table: { explore_source: ... }` (NDT) | `from(source -> { group_by:, aggregate: }) extend { }` | Computed source pattern |
| `value_format: "$#,##0.00"` | `# currency` | Map to Malloy render tags |
| `value_format: "0.00%"` | `# percent` | Map to Malloy render tags |
| `value_format_name: decimal_2` | `# number="0.00"` | Map to Malloy render tags |
