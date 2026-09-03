<!-- How to score an answer that declines. Read this WHENEVER the answer gives no value. -->

# Refusal

**STOP. Check `golden.kind` before reading further.** This section applies ONLY
when it is `unanswerable`. If the golden carries a value or rows, close this
section and score by containment like any other answer: an answer that declines,
however well it reasons, contains none of the golden's numbers and is
`no_match`.

That the model genuinely lacks the field is NOT a reason to pass a refusal.
Whether the model should be able to answer is what `coverage` records and what
`eval-diagnose` decides. Settling it here converts a model gap into a passing
case, and the gap then never reaches the backlog.

This rule is here because refusals against a valued golden are where the judge
is least stable, and the instability has been localised rather than guessed at.
Holding the answer, the rubric and the golden fixed and varying ONLY the model
source shown to the judge, over samples of three to four:

| model source shown | verdicts |
|---|---|
| lacks the concept entirely | `match` / `no_match` / `match` / `match` -- unstable |
| defines something adjacent | `no_match` x3 -- stable |
| withheld | `no_match` x3 -- stable |

So a model with no trace of the concept is what destabilises the verdict: the
judge starts weighing whether the answerer *could* have complied instead of
whether it did. Four prompt edits were tried against it -- this rule, deleting
the Refusal section, deleting the model-beats-rubric bullet, and splitting that
bullet into "the model CONTRADICTS the rubric" versus "the model LACKS what the
rubric names" -- and none of them stabilised it.

Treat a refusal on a coverage case as unstable until that changes: score it with
`check_judge.py --repeat`, not from one verdict. The rule below is still the
rule; it is just not yet enforceable by prompt alone.

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
