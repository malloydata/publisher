# storefront-tour -- baseline-02

Second arm of this set, run on a cold clone by an agent that had not seen it
built, and the first judged under the corrected summer rubric.

## What ran

- Set `storefront-tour` v1, 12 cases, all `dev`, every golden `verified`
- Target `examples/storefront`, model `storefront.malloy`
- Truth package on a second server the answerer has no route to
- Answerer sonnet, judge sonnet, cap 40 turns, 4 at a time
- Retrieval **semantic**, confirmed ready before the first question
- Steps: run, eval, diagnose. Improve did not run -- see "What to do next"
- Cost $2.32 answerer + $1.23 judge + $2.27 diagnose = **$5.82**

## The result

**10 of 12 correct (83%).** No `near_match` (an answer the judge finds
defensibly different, which is excluded from the pass rate), no unscorable
case, no unreadable judge reply.

| qid | verdict |
|---|---|
| top-category, top-margin-brand, peak-month-2025, orders-per-customer | correct |
| net-sales-2025, signup-cohort-2025, avg-discount, top-region | correct |
| best-customers, customer-count-basis | correct |
| **top-customer-spend** | **wrong** |
| **summer-sales-2025** | **wrong** |

Against baseline-01, which read 11 of 12: same summer failure,
`top-customer-spend` flipped. **Two arms, and the rubric changed between them,
so that move is not attributable** -- this set has no measured noise band.

## Retrieval

**Entity recall 95.5%** -- of the entities an answer needed, the share
`get_context` actually handed the agent. Complete on 10 of the 11 cases that
name any. The one miss is `top-category`, whose category dimension came back
under none of its three accepted ids; the agent answered correctly by another
route.

Coverage -- whether the model can express each answer at all -- was **not
measured**; it is a separate run of `check_coverage.py`. That is an absent
measurement, not a clean bill.

Payload is heavy: **75 entities returned per attempt for the 2 an answer used.**
Nothing here shows it hurting accuracy, but it is a real cost in tokens.

## Model failures

**`top-customer-spend` -- a false top spender, twice the real one.** The answer
named "Mila Costa" at $17,988.30. The real biggest customer is Delilah Okafor,
customer_id 884, at $8,817.56. The agent grouped spend by `full_name`, and
1,000 customers hold only 769 distinct names, so three different Mila Costas
merged into one row that outranks everyone. Nothing in the model says the name
is not unique. Retrieval was not the problem: both fields came back in the same
response. `construction/WRONG-PICK`, owner **model**.

**`summer-sales-2025` -- the wrong summer, 20% low.** $213,939.34 for a
June-August window; the company's summer is 25 May to 15 September, or
$267,422.53. Identical to baseline-01. No field, given, filter or doc anywhere
in `storefront.malloy` encodes a season, so the rule exists nowhere the agent
could find it. `get_context/model/COVERAGE`, owner **model**.

## Eval failures

**None.** Nothing was truncated, contaminated or environment-failed; the run
completed and the judge doubted no golden.

## Query errors

Nine failed `execute_query` calls across five cases, all recovered from -- but
seven of them sit inside cases that PASSED, so `eval-diagnose` never sees them.

| case | errors | kind |
|---|---|---|
| summer-sales-2025 | 4 | `'month' is a reserved word`; `created_at` cannot contain a `month` (x2); internal compiler error |
| avg-discount | 2 | `something is missing before 'avg'` |
| peak-month-2025, signup-cohort-2025 | 2 | internal compiler error |
| net-sales-2025 | 1 | `'logical operator' Can't use type date` |

Four are documented traps -- the reserved word and time truncation vs
extraction are both in `malloy-gotchas-queries` -- in a case that had
`malloy-analysis` open. That is a finding about the skills, not the agent.
Three are `Internal compiler error (likely a Malloy bug)`: the engine, not the
query.

## What this run taught us

1. **The duplicate-name trap is a coin flip.** Right in one arm, wrong in the
   next, same model and question. A trap the model does not warn about is not
   closed by an agent happening to avoid it.
2. **The convention failure reproduces exactly.** Same wrong number both arms.
   It is the stable failure because its cause is an absence, not a choice.
3. **Retrieval is not the bottleneck here.** Both wrong answers had everything
   they needed in hand.

## What to do next

- **[model] Document that `full_name` does not identify a customer** -- a
  `#(doc)` on it saying names repeat, and one on `customer_id` saying it is the
  identity. Through `skill:eval-improve`, not by hand.
- **[model] Encode the season window.** `givens.malloy` already shows the shape.
- **[set] Re-split before either edit lands.** Every case is `dev`, so an
  acceptance check has no holdout to defend an accept against.
- **[set] Measure coverage**, then re-report with `--coverage` so it charges
  the two failures.
- **[set] Check `required` on `top-category`** -- a passing case at 0.5 recall
  usually means the set named one path to an answer the agent reached by
  another -- and author `acceptable` on the six cases lacking it.
- **[skills] Find why the gotchas did not reach the answerer.**
- **[engine] File the three internal compiler errors** with the queries.
