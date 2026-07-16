# LookML Coverage Review (Step 7)

> Compare the built Malloy model against the original LookML project. Show the user what was modeled, what was skipped, and why. This runs during Step 7 (REVIEW) when prior-art notes exist.

## Data Sources

Read these before building the coverage report:

| Source | What it tells you |
|--------|------------------|
| Original `.lkml` files (location from the prior-art notes) | Full inventory of views, explores, dimensions, measures |
| Prior-art notes | Source candidates, flags, visibility seeds |
| Definition notes | What was proposed, confirmed, and deferred |
| Built `.malloy` files in package root | What actually shipped |

## 1. Source Coverage

Compare LookML views against Malloy base source files. Every LookML view should appear in this table.

| LookML View | Malloy Source | Status | Rationale |
|-------------|--------------|--------|-----------|
| orders | orders.malloy | modeled | (none) |
| users | customers.malloy | modeled (renamed) | Renamed to match business terminology |
| order_items | (none) | deferred | Bridge table: defer until line-item analysis needed |
| admin_audit | (none) | skipped:not-analytical | Operational/ETL table |
| order_facts_pdt | user_order_facts.malloy | modeled (rearchitected) | Performance-only PDT stripped; rebuilt as computed source |

**Status values:**
- `modeled`: directly represented in a Malloy source
- `modeled (renamed)`: represented under a different name
- `modeled (rearchitected)`: LookML pattern was restructured (e.g., PDT → computed source)
- `deferred`: valid source, postponed to a later iteration
- `skipped:not-analytical`: operational, staging, or ETL table
- `skipped:pre-aggregated`: snapshot/summary table; compute fresh in Malloy
- `skipped:looker-specific`: view exists only for Looker UI purposes

## 2. Field Coverage (Per Modeled Source)

For each modeled source, walk the original LookML view's dimensions and measures. Show what mapped and what didn't.

| LookML Field | Type | Malloy Field | Status |
|-------------|------|-------------|--------|
| order_id | dimension (PK) | order_id | modeled |
| total_price | measure (sum) | revenue | modeled (renamed) |
| status | dimension | order_status | modeled (renamed) |
| created_month | dimension_group | order_month (.month) | modeled (native) |
| status_link | dimension | (none) | skipped:looker-ui |
| period_comparison | measure | (none) | skipped:liquid |
| _pk | dimension | (none) | skipped:synthetic-key |

**Status values:**
- `modeled`: direct mapping
- `modeled (renamed)`: mapped under a clearer name
- `modeled (native)`: LookML pattern replaced by native Malloy feature (e.g., dimension_group → `.month`)
- `deferred`: valid field, not included in this iteration
- `skipped:looker-ui`: `link:`, `drill_fields:`, `html:`, `action:` patterns
- `skipped:liquid`: Liquid templating with no direct equivalent (intent documented)
- `skipped:synthetic-key`: generated PK, not meaningful as a dimension
- `skipped:duplicate`: redundant field, another field covers the same data
- `skipped:upstream`: complex SQL that belongs in dbt/ETL, not the semantic layer

**Show field counts per base source** to give a quick coverage ratio:

> **orders:** 18 of 24 LookML fields modeled (75%). 6 skipped: 3 Looker UI, 2 Liquid, 1 synthetic key.

## 3. Source/Explore Coverage

Compare LookML explores against Malloy source files.

| LookML Explore | Malloy Source | Status | Rationale |
|----------------|--------------|--------|-----------|
| order_analysis | order_analysis.malloy | modeled | (none) |
| customer_health | customer_health.malloy | modeled | (none) |
| admin_overview | (none) | skipped:not-analytical | Operational dashboard explore |

Include join coverage within each modeled source: did all the LookML joins carry over?

## 4. Skipped Patterns Summary

Group all skipped items by reason with counts. This gives the user a quick sense of what categories of things were left out and whether any warrant reconsideration.

| Category | Count | Examples |
|----------|-------|---------|
| Looker UI patterns (link, drill, html, action) | 12 | link on order_id, drill on customer_name |
| Liquid templating | 3 | period_over_period parameter, dynamic_timeframe |
| Filter-only fields | 2 | status_filter, date_filter |
| Synthetic keys | 1 | _pk (generate_uuid) |
| Dashboard files | 1 | overview.dashboard.lookml |
| Upstream SQL | 2 | complex_attribution, geo_enrichment |

## 5. Presentation

Present coverage in this order:

1. **Overall summary**: "Modeled X of Y sources, covering Z% of LookML fields. N items deferred, M skipped."
2. **Source coverage table** (Section 1)
3. **Per-source field counts** with ratios (Section 2 summaries)
4. **Skipped patterns summary** (Section 4)
5. **Deferred items**: list everything marked `deferred` across sources and fields, with rationale, so the user knows what's available for future iterations
6. **Full field coverage tables** (Section 2 detail): present per base source if the user wants to drill in

**Ask the user:** "Does this coverage look right? Anything deferred that you'd like to add now, or anything modeled that should be removed?"
