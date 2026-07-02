# Starting from Analysis

When you've been doing analysis, writing queries, building views, creating notebooks, and want to formalize the work into a reusable semantic model, follow this process.

**The model emerges from your analysis.** You don't design sources top-down, you identify what's worth encoding as reusable building blocks.

## Step 1: IDENTIFY, what belongs in a model?

Scan your analysis `.malloy` and `.malloynb` files. For each dimension, measure, join, or calculation, ask: **is this a reusable building block?**

A measure or dimension belongs in a model if it meets ANY of these criteria:

| Criteria | Example |
|----------|---------|
| **Reusable**, another query or user would benefit from this definition | `revenue is sum(sale_price)`, any analysis involving orders needs this |
| **Composable**, can be combined with other concepts for future analysis | `customer_tier` dimension, useful for segmenting any metric |
| **Encodes business logic**, standardizes a calculation that should be consistent | Regex that parses a messy column, status mapping, tier boundaries |
| **Enables reproducibility**, someone repeating this analysis shouldn't re-derive it | Complex window function for first-touch attribution |

**Include even if used once**, if a calculation encodes business logic (regex, tier boundaries, status mappings) or was hard to get right (window functions, multi-step derivations), it belongs in the model.

**When uncertain, propose with reasoning and ask the user.** Show what you think should be modeled and why. The user confirms, adjusts, or defers.

### What to scan for

| Pattern in analysis | Promotes to |
|---------------------|-------------|
| Tables used in queries | Base source candidates |
| Dimensions that define segments, categories, or time buckets | Dimensions in base sources |
| Measures that compute business metrics | Measures in base sources |
| Joins between tables | Joined source candidates |
| Pre-aggregated patterns at different grains | Computed source candidates |
| Complex calculations (regex, window functions) | Dimensions in base sources, especially worth modeling |
| Repeated `where:` filters that define business segments | Filtered measures or dimensions |

## Step 2: DECOMPOSE, break into composable building blocks

Take the identified elements and decompose compound analysis into atomic, reusable parts.

**Atomic measures:** Break compound insights into their building blocks.
```
Analysis: "Revenue grew 23% YoY for enterprise customers"
→ Measure: revenue is sum(sale_price)
→ Measure: enterprise_revenue is sum(sale_price) { where: segment = 'enterprise' }
→ Dimension: segment (already exists or needs creation)
```

**Reusable dimensions:** Separate bucketing logic from one-off filters.
```
Analysis: "High-value users (>$500 LTV) churn less"
→ Dimension: ltv_tier is lifetime_value ? pick 'high' when > 500 else 'standard'
→ Not: a filtered measure that bakes in the $500 threshold
```

**What to leave behind:**
- Ad-hoc calculations that answered a specific question but aren't reusable
- Filtered aggregates tied to a specific finding (unless the filter defines a business segment)
- Views that are specific to one analysis narrative

## Step 3: STRUCTURE, build the model files

Map the identified and decomposed elements to the file architecture:
- Single-table measures/dimensions → base source files (one per table)
- Cross-table measures/joins → joined source files (one per analytical domain)
- Pre-aggregated patterns at different grains → computed source files (see `reference/query-sources.md`)

**The analysis files remain as-is**, they're the record of your investigation. The model is the reusable layer extracted from them. Later, analysis files can be refactored to `import` the model and reference its sources instead of defining everything inline.

## Step 4: VALIDATE, confirm the extraction

Run key analysis queries against the new model to confirm results match. If numbers differ, a dimension or measure was extracted incorrectly.

Then apply the same curate and document standards as the rest of this skill (access modifiers, `#(doc)` tags).

## When to formalize

Don't formalize too early. Good signals:
- You've answered 3+ distinct questions and keep redefining the same measures
- You find yourself copy-pasting dimensions or joins between queries
- Someone else needs to use your analysis patterns
- You want a notebook or dashboard that should survive data refreshes
- You built something non-trivial (regex, window function, multi-step logic) that would be painful to recreate
