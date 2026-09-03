# The judge

JUDGE_VERSION: 4

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

### When the answer key looks wrong

Score against the golden as written. Then say, separately, whether you believe
it. Those are two different jobs and `gold_status` is the second one.

**The verdict never bends.** If the prediction does not contain the golden, that
is `no_match`, whatever you think of the golden. An answer does not pass because
you suspect the key. Doubt goes in `gold_status`, and something downstream
adjudicates it; a judge that quietly graded against its own better answer would
be the only record of having done so.

| Value | Meaning |
|---|---|
| `verified` | No reason to doubt it. The default, and the honest answer nearly always. |
| `verified_benign` | Reachable defect that cannot change this verdict -- e.g. join fanout under an `AVG`, `MIN`, `MAX` or `STDDEV`, which uniform duplication does not move. |
| `suspect` | Something does not add up and you cannot settle it from what you were given. |
| `verified_wrong` | You can demonstrate the key is wrong, and say how. Excludes the case from run aggregates, so the bar is demonstration, not suspicion. |

What earns more than `verified`:

- **The rubric contradicts the model.** You have the model source. A rubric
  saying "`lifetime_orders` counts line items despite its name" against a model
  reading `lifetime_orders is count(order_id)` is a rubric written before a fix
  and never revisited. That is `suspect` at least, and the judge is the only
  station positioned to notice -- this exact case failed two correct answers for
  a full run.
- **The golden and its own `canonicalQuery` disagree**, where you can see both.
- **The golden is impossible against the re-executed rows** -- a total below one
  of its own parts, a rate outside 0 to 1, a count above the population.
- **Fanout you can identify**, benign or otherwise, per the classification above.

What does not: the answer being more useful, better presented, or more recent
than the key. Disagreeing with the question's premise is not a defect in the
answer to it.

`gold_note` says what you saw, concretely enough to check -- the two values, or
the model line against the rubric sentence. "Golden looks off" routes nothing.

### Writing a rubric the judge can execute

A case rubric is not prose for a human to weigh. It is the part of the judge's
instructions that changes per case, so every clause in it must resolve to a
verdict. Where one does not, the judge supplies the missing rule itself, and
supplies a different one next time -- which reads as model noise and is not.

Two clause types cause almost all of it. Both must carry their consequence.

**An alternate reading** -- a second defensible answer to the same question.
Mark each one, and never leave the set open:

| Marker | Verdict | Use when |
|---|---|---|
| `PREFERRED` | `match` | The reading the golden encodes. Exactly one. |
| `ACCEPT` | `match` | Equally right. A different but faithful route to the same claim. |
| `DIVERGENT` | `near_match` | Defensible, and not what was asked for. Usually a population or grain the model does not distinguish. |
| `WRONG` | `no_match` | Plausible and incorrect. Name the trap value so the judge can recognise it. |

**A disclosure** -- something the answer must SAY, beyond the number. Say what
silence costs:

| Marker | Verdict when omitted | Use when |
|---|---|---|
| `REQUIRED` | `no_match` | Without it the answer misleads. A year-over-year figure over a truncated year is the case: the number is right and the reader draws a false conclusion from it. |
| `CREDITED` | `match`, no deduction | It adds context a good analyst would give. Its absence leaves the reader correct but less informed. |

Rules that follow from this:

- **Write the question so its answer is data.** A question is a request for a
  figure, a series, or a set of rows -- things a truth query can produce and a
  judge can compare. "How did reach build week by week" is a question; "and
  when did it flatten out" is a request for an opinion about the answer, and
  no golden can hold one. Put interpretation in a `CREDITED` clause if it is
  worth noting, never in the question and never as a scored window.
- **Fix the grain in the question, or accept every grain in the rubric.** If the
  golden is a campaign total and a by-medium answer would be wrong, the question
  must say "for the campaign as a whole". If it does not, the rubric must accept
  a correct figure at any stated grain (judge rule 10). A rubric that quietly
  assumes the golden's grain fails correct answers.
- **A right value plus a missing `CREDITED` disclosure is a `match`.** Not a
  near match. Do not deduct for it.
- **`DIVERGENT` is about definitions, not arithmetic.** A clause permitting a
  different population, grain or convention never excuses a computational
  error. If a rubric tolerates a shift in the third decimal and the answer is
  out by a whole unit, that is `no_match` however well the narrative reads.
- **An unmarked clause is `CREDITED`.** The judge must not invent a
  requirement. A rubric that meant to require something and did not say so is
  the rubric's bug, and the fix belongs in the case.
- **Stable `near_match` is a finding, not an outcome.** A case that lands there
  in run after run is telling you the model cannot distinguish two readings that
  the question does. That is a coverage gap for `eval-diagnose`, and repairing
  the rubric will not close it.

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

### Refusal

Applies when the case's `golden.kind` is `unanswerable`. Every rule above assumes
a gold result to contain and columns to pair, and here there is neither: no
value, no canonical query, no rows. Do not attempt containment. Your input is the
question, the case rubric -- which names the missing data and the proxy an agent
is expected to reach for -- and the answer text.

A refusal is not scored on tone or on hedging. It is scored on whether the reader
is left correctly informed that the metric is unavailable, and why.

Rule 7 applies here too: the `near_match` below is not a pass. A refusal that is
right but useless does not count towards the pass rate, and it does not count
against it either.

- **match** -- declines, and names the specific missing data rather than declining
  generically. Offering the proxy *with an unmissable label* is also a match:
  reporting revenue by channel while stating plainly that it is not ROAS because
  no cost exists is the most useful possible answer and must not be penalised.
- **near_match** -- declines correctly but generically. "I don't have enough
  information" is the right call delivered uselessly; the reader cannot tell
  whether the data is missing, the question is ambiguous, or the agent gave up.
  Also `near_match` when a proxy is offered but its caveat is buried or hedged.
- **no_match** -- produces a number for the metric as asked, whether invented or
  computed from a proxy presented as the answer. Severity does not fall as the
  proxy gets closer: a confident 3.51% cart-abandonment rate derived from
  cancellations is a wrong answer to a question the data cannot answer, and being
  plausible makes it worse rather than better.

Two rules override the generosity built into the main rubric.

9. **Absence of a number is not evidence of a refusal.** An answer that fails,
   errors, or wanders off without addressing availability is not a pass. The
   refusal has to be a stated conclusion. This is distinct from `submitted:
   false`, which the conductor already records as not scorable.
10. **When the case is tagged `answerable-sounds-unanswerable`, a refusal is
   `no_match`.** Score it against the golden normally. Rule 2's "benign extra
   context never makes a containing answer `no_match`" does not apply to an
   answer that contains nothing. A caveat attached to a *correct* answer is
   welcome and does not reduce the verdict.

Both directions are scored on purpose. Measuring only whether an agent declines
when data is absent rewards one that declines everything, and such an agent is
worthless. A set carrying this section should pair the two: in the ecommerce set
`ecom_stockout_revenue_loss` (refuse) and `ecom_unsold_stock_value` (answer)
resolve to the same figure, one as an inadmissible proxy for revenue lost to
demand that never existed and the other as the honest cost of stock on hand. Only
the question distinguishes them, so no policy passes both -- the agent has to read
the question.

#### Anchors

- **match**: "What is our ROAS?" -- the answer says there is no ad-spend data
  anywhere in the model, notes that `traffic_source` is the account's acquisition
  channel rather than a cost, and offers revenue by channel explicitly labelled as
  not being ROAS. Confidence 9.
- **near_match**: same question; the answer says "I can't calculate that with the
  available data" and stops. Correct, and the reader learns nothing about what is
  missing or whether another source would fix it. Confidence 7.
- **no_match**: same question; the answer divides revenue by traffic source and
  reports a ROAS per channel. Every figure is arithmetically right and the label
  is false. Confidence 9.
- **no_match**: "How much are we sitting on in unsold inventory?", tagged
  `answerable-sounds-unanswerable`; the answer declines for want of an inventory
  snapshot. The data answers it, and "ever unsold" needs no snapshot -- only
  "unsold as of a date" would. Confidence 9.

### Coverage

A case may be labelled `coverage: derivable`: the model has no entity for the
concept and the answer had to be built from the parts that exist. Judge the
result exactly as the rubric says -- a derived answer that matches the golden is a
`match`, and the absence of a named measure is not a deduction. But when the
answer states what it built, say so in the why. That sentence is what tells
diagnosis the gap is real and lets `coverage_note` become a model edit rather
than a guess.

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

## Versioning and regressions

Any change to this file is a judge change: bump JUDGE_VERSION, commit, and
re-run `evals/<set>/judge-regressions.jsonl` (the human-overruled verdicts)
before trusting new scores. Runs record `judge_version` and `rubric_sha`, so
a delta across a rubric change is attributable to the rubric, not the model.
