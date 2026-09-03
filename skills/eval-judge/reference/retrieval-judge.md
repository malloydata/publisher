<!-- A different job from scoring an answer. Read this only when judging retrieval. -->

# Retrieval judge

Input:

- the intent row: `term`, `entityType`, `description` (the rich intent, the
  thing you actually judge against)
- the ranked entities a `get_context` call returned for that term, each with
  its within-target rank and doc

Two judgments:

1. **In scope?** Does THIS model version contain an entity representing the
   described concept at all, anywhere, regardless of whether it was returned?
   `in_scope: false` is a coverage gap, charged to the model's coverage, not
   to retrieval.
2. **Per returned entity**: `match` (represents the described intent),
   `near_match` (the concept overlaps but the intent might want something
   broader or narrower; retrieving `net_revenue` for the term "revenue" is a
   near match), or `no_match`. Confidence 1 to 10 and a one-line why, each.

Rule 7 does **not** apply to retrieval. Here `near_match` counts towards recall
and precision, and should: handing back an overlapping entity is a real
retrieval success, since the agent can read the doc and decide. The answer judge
excludes it because there the same word means "the answer might be wrong".

Output:

```json
{
  "in_scope": true,
  "judgments": [
    { "entityId": "measure:orders:total_sales", "rank": 1,
      "level": "match", "confidence": 9, "why": "..." }
  ]
}
```

The conductor computes coverage, recall, and precision by counting these
(`reference/ledger-schema.md`). The judge only judges.
