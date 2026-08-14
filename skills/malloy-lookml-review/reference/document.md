# LookML Documentation Seeds (Step 9)

> Extract LookML descriptions and formatting hints as starting material for Malloy `#(doc)` tags. This runs during Step 9 (DOCUMENT) when the prior-art notes have a Documentation Seeds section.

## Process

1. **Start from seeds**: read the Documentation Seeds captured during discovery for fields with high-quality LookML `description:` values.

2. **Read `.lkml` view files** for the full `description:` text on dimensions and measures not captured in seeds.

3. **Rewrite for retrieval**: LookML descriptions are written for Looker's field picker. Malloy `#(doc)` strings power AI search. Rewrite to match how analysts search:
   - Use business meaning, not Looker jargon
   - Include units (USD, count, percentage): a unit is part of what a number means
   - For a categorical field, listing a short set of values (roughly ten or fewer) makes the description concrete; past that, describe what the field captures instead of dumping the list, which crowds out the meaning and goes stale as the data changes
   - Avoid "filterable", "groupable", "dimension", "measure"

4. **Map formatting to render tags:**

   | LookML | Malloy |
   |--------|--------|
   | `value_format: "$#,##0.00"` | `# currency` |
   | `value_format: "0.00%"` | `# percent` |
   | `value_format_name: decimal_2` | `# number="0.00"` |
   | `value_format_name: usd` | `# currency` |
   | `value_format_name: percent_2` | `# percent` |

5. **Map labels:**
   - `label:` that simply renames → `internal:` + `dimension:` candidate
   - `label:` that differs from a clean snake_case name → `# label="Display Name"` candidate
