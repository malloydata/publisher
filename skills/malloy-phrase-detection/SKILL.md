---
name: malloy-phrase-detection
description: How to phrase search_text on a get_context call so retrieval returns the fields you need instead of a truncated catalog. Covers target-type classification and decomposition patterns.
---
<!-- Copyright (c) Credible Data Inc. SPDX-License-Identifier: MIT -->

# Search Target Construction for `get_context`

The `get_context` tool description defines each field and what a call returns. This skill focuses on the parts you won't get right by default: classifying concepts into target types and splitting ambiguous phrases.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

**Scope of this skill:** the patterns below build `dimension` / `measure` / `view` / `dimensional_value` targets. Phrasing for `source` targets is covered at the end.

**A note on matching:** `get_context` searches over the model (sources, fields, views, and their descriptions), and `dimensional_value` targets search the categorical *values* stored in the data. Some servers don't index values; check the tool's description. On one that doesn't, find a literal value by targeting its dimension, then querying its distinct values with `execute_query` (see "Where value search isn't available" below).

## Always send `search_text`

**Do not enumerate.** Omitting `search_text` lists a catalog rather than searching it. Knowing the package narrows *where* to look; it does not substitute for saying *what* you need: if you know the package, that is a reason to scope, not a reason to skip `search_text`. Enumerated listings are capped per source and per entity type, and with no relevance signal the cap drops the fields your question is about while keeping join-path noise.

A bare listing has two legitimate uses. The first is answering "what data is here?" when the user has named no subject at all. The second is reading a specific entity you already have the exact name of: scope to its source, set `entity_name`, and pass `search_text: null`, which returns that entity's docstring and Malloy code without spending a search. Every other call carries `search_text` on every target.

## Authoring `search_text` for entity targets

Write `search_text` as a brief semantic **description** of what you're looking for, not an echo of the user's word. This applies even when you already know the entity name from a prior result: still describe it, don't just repeat the name. That is a rule about how to *phrase* a search, and it does not conflict with the exact-entity lookup above: if you want that one entity's code and docstring rather than a ranked set, pin it with `entity_name` and skip the search entirely. Sending its name back as `search_text` is the move this rule forbids, because it searches for a name instead of either describing a concept or asking for the entity.

One target per concept is enough: the tool handles phrasing variants internally. Don't pile up dimension targets that point at the same field. Use multiple targets only when they describe genuinely distinct concepts (see "Non-obvious decomposition patterns" below).

## Target-type decision guide

- **`dimension`**: categorical attribute to group, filter, or join on. Also used for time and numeric fields.
  - "region" becomes `"the geographic region"`
- **`measure`**: aggregation metric (count, sum, average, rate).
  - "total revenue" becomes `"the total revenue or sales amount"`
- **`view`**: pre-built analysis. Include one whenever the question sounds like a canned report (summary, breakdown, top-N, trend).
  - "sales summary" becomes `"a summary of sales metrics"`
- **`dimensional_value`**: a literal value the user named, stored in some dimension. `search_text` is the value itself, the one target where echoing the user's word is right.
  - "CyberArk" stays `"CyberArk"`
  - Best practice is to scope the call to the source that holds the value. An unscoped value search covers every indexed dimension in scope, so it is slow on a large package. If you truly don't know the source, you can leave the scope off, but expect a slow call.
- **`source`**: data domain, for a question that names a subject area rather than fields (phrasing below).

**Resolving categorical values.** When the user names a literal value like "premium" or "New York City", send a `dimensional_value` target for it, scoped to its source once you know the source. Filter on the exact string it returns: the data may store `"Premium"`, `"PREMIUM"`, `"NYC"`, or `"New York"`, and only the data tells you which.

**Where value search isn't available.** Some servers have no value index and return nothing for a `dimensional_value` target, and on others a particular dimension's values may not be indexed. Then target the *dimension* the value lives on (`"the subscription tier"`, `"the city where the subscriber lives"`), and confirm the exact stored string by querying that dimension's distinct values with `execute_query` before you filter on it.

## Non-obvious decomposition patterns

These are the rules you won't apply correctly by default:

1. **Adjective + noun, split.** "active users" becomes two dimension targets: one for the attribute (`"the status of the user account"`) and one for the noun (`"the user or account holder"`). Resolve the modifier ("active") to the exact stored value with a `dimensional_value` target, or the status dimension's distinct values where values aren't indexed.
2. **Ambiguous concept, cover both types.** "rating", "duration", and the like could be either a dimension or a measure: create one target of each type.
3. **Time references are dimensions.** "last year" becomes a dimension target for the relevant date field (`"the date the event occurred"`).
4. **Numeric ranges are dimensions.** "aged 50", "revenue over $1M" become dimension targets; the comparison is applied in the query, not matched as text.
5. **Categorical strings that look numeric are still dimensions.** "18-30", "<5 days", "tier 2" are stored as literal strings on a dimension. Target that dimension, then confirm the exact string with a `dimensional_value` target, or `execute_query` where values aren't indexed.
6. **"Top N" without a named measure, add a ranking measure.** "top 6 products" becomes a measure for the ranking concept (`"the performance metric for a product"`) plus a dimension for the entity. If the measure is explicit ("top products by total sales"), use it directly and skip the generic ranking measure.
7. **Multiple values for one concept, one dimension target.** Several values ("premium and basic") still map to a single dimension target for the parent field; confirm the exact stored values with `dimensional_value` targets, or `execute_query` where values aren't indexed.
8. **The quantity asked for is a measure, even when nothing names it.** "How many titles were released in 2019?" is asking for a count, but no noun in it is the count: the visible phrases are the subject ("titles") and the filter ("2019"), and mapping only those yields a `source` and a `dimension` target with no way to answer. Always add a measure target for the quantity itself (`"the number of titles"`). This holds for every interrogative that IS the aggregation -- "how many", "how much", "how often" -- and it is the pattern most easily lost when a filter or grouping is the loud part of the sentence. Measured: an answerer got this right on "How many titles are in the dataset?" and dropped it on "How many titles were released in 2019?", where the year took the attention.
9. **A population qualifier is a target, and so is the one the question omits.** Words like "real", "actual", "genuine", "live" or "production" are not filler: they name rows the model marks for exclusion. Target the flag itself (`"the flag marking synthetic, test or monitoring traffic"`), not just the noun they modify. Add one such target even when the question carries no qualifier at all, because a table of events, requests, sessions or logs usually holds test, internal or cancelled rows and nothing in the wording will say so. Resolve it to the model's own flag rather than inventing a filter on an id or a name; a hand-rolled exclusion and the documented one rarely select the same rows.

## Worked example

**User:** "Customer churn in NYC over the last year for premium and basic subscribers"

The targets for this question:

| target_type | search_text |
|---|---|
| `measure` | `"the rate at which customers leave the service"` |
| `dimension` | `"the city where the subscriber lives"` |
| `dimension` | `"the date the subscription was canceled"` |
| `dimension` | `"the tier of the subscription"` |
| `view` | `"subscriber churn or retention analysis"` |

Key moves: time ("last year") becomes a dimension on the cancellation date; "NYC" and "premium/basic subscribers" resolve to the city and tier dimensions in this first call, because the source is not known yet and an unscoped value search is slow. One `view` target is included to surface any canned churn analysis.

The response returns the source these fields live on (here `subscriptions`) with the matched fields on its card. Then send a second call scoped to `subscriptions`, with `dimensional_value` targets for `"NYC"`, `"premium"` and `"basic"`, to get the exact strings to filter on ("New York City" vs "NYC", "premium" vs "Premium"). Where values aren't indexed, run `execute_query` on the city and tier dimensions' distinct values instead.

## Authoring `search_text` for `source` targets

Aim for **3-8 words that name the entity and its business process**. Don't include filter values, time ranges, or aggregations: those belong in entity targets.

| Too vague | Over-specific (entity-shaped) | Good |
|---|---|---|
| `"orders"` | `"total order revenue by customer last year"` | `"customer order history and line items"` |
| `"customer data"` | `"premium subscribers who churned in NYC"` | `"subscriber accounts and churn"` |
| `"metrics"` | `"monthly revenue variance by account"` | `"sales pipeline and revenue forecasts"` |

Heuristics:

1. **Translate, don't echo.** "How did sales go last month?" becomes `"sales order revenue"`, not `"sales last month"`.
2. **Differentiate by data shape or business process, not by the user's industry, brand, or product category.** Prefer `"order fulfillment and shipping"` over `"ecommerce data"` when multiple commerce-ish packages exist. Do NOT add words like `"eyewear"`, `"subscription box"`, `"Acme Corp"`, or the user's specific vertical/brand. Source summaries describe data structure, not the customer's vertical, so those words add noise and can hurt matching.
3. **Retry with alternative phrasings** if the right source isn't in the results before concluding it's missing.
