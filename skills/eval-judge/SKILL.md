---
name: eval-judge
description: 'Decide whether ONE answer matches its golden, and say whether you believe the golden. Read this before emitting any verdict. Covers containment, column pairing, near_match, refusals, and the gold_status judgement. Use when scoring an attempt in an evaluation run; never to conduct a run (eval-loop), diagnose a failure (eval-diagnose) or edit a model (eval-improve).'
---

# The judge

JUDGE_VERSION: 4

This skill IS the judge. One fresh judge subagent is spawned per attempt, with
this skill installed in its workspace and the case materials in its prompt. It
is loaded, not pasted -- so the prompt carries the case and this carries the
doctrine, and a judge that needs to read a Malloy query can reach for the
skills beside it rather than being handed a transcription.

Measured when it stopped being pasted, on the case that had oscillated
(a valued golden against a model with no trace of the concept):

    pasted into the prompt   match / no_match / match / match
    loaded as this skill     no_match x4, and the reasoning cites the rule

It costs about 2.5x per verdict, which is the price of the judge actually
reading its own rules.

Record `judge_version` and this file's git blob sha
(`git rev-parse HEAD:skills/eval-judge/SKILL.md`, or the model repo's copy) on
every verdict, so a rubric change never silently rewrites what old scores
meant.

The judge is not blind. It sees the golden. It must never be the same
subagent that answered, and it never edits anything: it returns a verdict
object and stops.

## Read one of these before you decide

This file is the decision procedure. Four situations have their own rules, and
each is a file beside this one. Read the file BEFORE emitting a verdict, not
after -- these are the cases where judging from the general rubric alone gets it
wrong, which is why they are called out rather than summarised.

| If | Read |
|---|---|
| the answer declines, or gives no value at all | `references/refusal.md` |
| the golden itself looks wrong to you | `references/suspect-goldens.md` |
| you are judging retrieval, not an answer | `references/retrieval-judge.md` |
| you are AUTHORING a case rather than judging one | `references/writing-rubrics.md` |

The first row is the one that catches people. A refusal is only exempt from
containment when `golden.kind` is `unanswerable`; against a golden that holds a
value, an answer containing none of it is `no_match` however well it reasons.
`references/refusal.md` is the whole rule.

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
  "column_pairing": { "gold_col": "pred_col", ... },
  "gold_status": "verified | verified_benign | suspect | verified_wrong",
  "gold_note": "why, when not verified"
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
7. **`near_match` is not a soft pass, and it is not a soft fail.** It is a
   third outcome meaning *defensibly different*: the answer took a reading the
   rubric allows but did not prefer, broke a tie the other way, or buried a
   caveat that should have been plain. It is excluded from the pass rate and
   from the acceptance check, exactly like `needs_human`.

   So do not reach for it to avoid a hard call. If the prediction contains the
   gold answer, that is `match` -- extra columns and benign extra context never
   reduce it (rule 2). If it does not, and the rubric does not sanction the
   reading that produced it, that is `no_match`. Use `near_match` only when you
   can name the rubric clause that makes the difference defensible.

   It is a third outcome because as a pass it was a large share of the measured
   noise: the same unchanged answer reads `match` in one run and `near_match`
   in the next, and the pass rate moves although nothing did. A verdict whose
   content is "this is arguable" cannot be allowed to decide anything. Its
   count is still reported, and a rising one means the rubrics are going vague.
   (What that share was for a given set is in that set's calibration record.)
8. On a large row set, compare it as a set rather than scanning pairwise: state
   how many gold rows you located in the prediction, name the ones you could
   not, and say what the mismatched values look like (uniformly scaled, off in
   one column, a different population). "I checked all 76" without that
   breakdown is not a comparison.
9. **Score the data, not the insight.** A question that asks for a figure or
   a series is judged on the figure or the series. Where the question also asks
   for an interpretation -- "when did it flatten out", "what drove the change"
   -- that interpretation is not scored unless the rubric marks it `REQUIRED`
   with a criterion that resolves from the data alone. Two analysts reading the
   same exact curve name different weeks; an eval that scores which week they
   named is measuring taste, and a run that lost a case that way (13 of 13
   weekly values exact, plateau named one week outside a window) was measuring
   nothing. Exact data with a different reading of it is `match`.
10. **Do not demand a grain the question did not fix.** When the question names
   no grain -- by medium, by week, campaign total -- a figure that is correct at
   the grain the answer states is correct. The golden's grain is `PREFERRED`,
   not the only one: an answer at another grain is `match` when the grain is
   stated and the figures are right at it; `near_match` when the grain is left
   unstated; `no_match` only when the figures are wrong at the grain claimed. An
   answer that named the right segment and showed the index split by medium,
   every number right, was once scored down for not showing the campaign
   total; the question had never asked for one. A rubric that means "campaign
   total only" must say so as `REQUIRED`, and the question should say so too.

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

### Coverage

A case may be labelled `coverage: derivable`: the model has no entity for the
concept and the answer had to be built from the parts that exist. Judge the
result exactly as the rubric says -- a derived answer that matches the golden is a
`match`, and the absence of a named measure is not a deduction. But when the
answer states what it built, say so in the why. That sentence is what tells
diagnosis the gap is real and lets `coverage_note` become a model edit rather
than a guess.

## Versioning and regressions

Any change to this file is a judge change: bump JUDGE_VERSION, commit, and
re-run `evals/<set>/judge-regressions.jsonl` (the human-overruled verdicts)
before trusting new scores. Runs record `judge_version` and `rubric_sha`, so
a delta across a rubric change is attributable to the rubric, not the model.
