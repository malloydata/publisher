# The judge

JUDGE_VERSION: 1

This file IS the judge prompt. `eval-answer` spawns one fresh judge subagent
per attempt (answer judge) or per intent (retrieval judge) and pastes the
relevant section plus the case materials. Record `judge_version` and this
file's git blob sha (`git rev-parse HEAD:skills/eval-answer/reference/judge.md`,
or the model repo's copy) on every verdict, so a rubric change never silently
rewrites what old scores meant.

The judge is not blind. It sees the golden. It must never be the same
subagent that answered, and it never edits anything: it returns a verdict
object and stops.

## Answer judge

Input, all of it (a judge with only two row sets grades formatting, not
intent):

- the question, exactly as the answerer saw it
- the golden: rows or scalar, plus `canonicalQuery` when present
- the prediction: the rows the CONDUCTOR re-executed from the answerer's
  `final_query` (never the answerer's self-reported rows)
- the relevant source and field definitions from the model (docs, join list)

Output, exactly this shape:

```json
{
  "verdict": "match | near_match | no_match",
  "confidence": 7,
  "why": "one short paragraph",
  "column_pairing": { "gold_col": "pred_col", ... }
}
```

### Rubric

1. **Judge intent, not formatting.** The question defines what counts. A
   result that answers the question in a different but faithful shape is a
   match.
2. **Gold-subset containment.** The prediction must CONTAIN the gold answer.
   Extra columns or benign extra context downgrade to `near_match` at worst;
   they never make a containing answer `no_match`.
3. **Name the column pairing.** Pair each gold column with the prediction
   column that carries the same meaning, using names, the question's role for
   the value, and the values together. Never pair numeric columns by value
   overlap alone: a year column is not a count column even when magnitudes
   overlap. If a gold column has no counterpart, say which.
4. **Rows are a multiset.** Order matters only when the question asks for an
   order. For a "top N" with possible ties, check that the boundary value is
   right and every returned row legitimately qualifies; any valid tie-break is
   a match.
5. **Tolerances.** Numeric equality within small rounding (relative 1e-6, or
   the display precision the golden uses). A percentage and its fraction
   (50 and 0.5) are the same value in different units when the pairing says
   the column is a rate.
6. **Confidence 1 to 10.** 5 or lower means the case needs a human:
   the conductor records `needs_human`, which is neither a pass nor a fail.
   Do not inflate confidence to be helpful; a wrong confident verdict is worse
   than an abstention.
7. You MAY run the reference script
   `skills/eval-answer/scripts/match_rows.py` as an aid on large row sets.
   Its output informs you; it never overrides your judgment. Known limits:
   it zeroes on extra columns and can cross-match numeric columns.

### Anchors

- **match**: question "total sales by category"; golden 8 rows
  `(category, revenue)`; prediction 8 rows `(product_category,
  gross_revenue, order_count)`. Same categories, revenues equal within
  rounding; the extra count column does not change what the answer says.
  Verdict: match, confidence 9.
- **near_match**: question "top 5 states by returns"; golden and prediction
  agree on 4 of 5 states, and the disagreement is at rank 5 where two states
  tie exactly; the prediction chose the other tie-break. The boundary value
  is right, the membership defensible, but the golden pinned one tie-break.
  Verdict: near_match, confidence 7, why names the tie.
- **no_match**: question "revenue in 2024, completed orders only"; golden
  1.2M; prediction 1.9M and the pairing shows the prediction summed all
  statuses. Same shape, wrong population. Verdict: no_match, confidence 9.

Keep the anchor set balanced. A judge shown only matches learns a base rate,
not a rubric.

## Retrieval judge

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

## Versioning and regressions

Any change to this file is a judge change: bump JUDGE_VERSION, commit, and
re-run `evals/<set>/judge-regressions.jsonl` (the human-overruled verdicts)
before trusting new scores. Runs record `judge_version` and `rubric_sha`, so
a delta across a rubric change is attributable to the rubric, not the model.
