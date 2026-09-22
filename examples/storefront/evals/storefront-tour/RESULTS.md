# storefront-tour -- baseline-01

First measurement of the bundled `storefront` package against the twelve
questions authored beside it, to see what a well-documented model scores cold
and to exercise the eval loop end to end on an example anyone can re-run.

## What ran

- Set `storefront-tour` v1, 12 cases (all `dev`, nothing withheld), every golden `verified`
- Target `examples/storefront`, model `storefront.malloy`
- Truth package `storefront-tour-truth` on a second server (`:4881`) the answerer has no route to
- Answerer sonnet, judge sonnet, cap 40 turns, 4 at a time
- Retrieval: **semantic**, confirmed ready by two consecutive probes before the first question
- Steps run: run, eval, diagnose
- Cost $1.82 answerer + $1.32 judge + $0.74 diagnose = **$3.88**

## The result

**11 of 11 decided (100%). One near match.**

| qid | verdict |
|---|---|
| top-category | match |
| top-margin-brand | match |
| peak-month-2025 | match |
| orders-per-customer | match |
| top-customer-spend | match |
| net-sales-2025 | match |
| signup-cohort-2025 | match |
| avg-discount | match |
| top-region | match |
| best-customers | match |
| customer-count-basis | match |
| **summer-sales-2025** | **near_match** |

No attempt was truncated, contaminated or environment-failed.

## Retrieval and coverage

**Entity recall 100%, complete on all 11 scored cases.** Every entity a
golden depends on came back, on every case. Coverage was not measured this
run: it reads the model rather than the answers, and with one failure there
was nothing for it to charge.

**Precision is the finding here, not recall.** get_context returned about 73
entities per attempt for the 2 the answer actually named -- a mean precision
of 4.0%, ranging from 1.3% to 7.5%. Recall is free at that breadth. Whether
that costs accuracy on a harder set is the open question; on this one it
did not.

## Model failures

**summer-sales-2025** -- the only one, and the only kind that a
well-documented model cannot prevent.

The company's sales calendar defines summer as 25 May to 15 September. The
agent answered $213,939.34 for 1 June to 31 August, the meteorological
window, and **said so plainly**: "summer 2025 (June 1 - Aug 31, 2025)". The
correct figure for the company's window is $267,422.53, 25% higher.

The judge scored `near_match` rather than `no_match` on exactly the right
ground: the rubric accepts a stated window, because an answer that names its
own definition is checkable and only the convention is missing. That is the
distinction the set was built to make.

Diagnosis put it at `get_context/model/COVERAGE`, owner **model**, in a
cluster named `sales-season-calendar-unrepresented`: `storefront.malloy`
contains no entity of any kind -- dimension, named filter or given --
expressing a sales season.

### Proposed improvement (not applied)

Encode the season in the model, so the convention travels with the data
instead of living in someone's head. Either a named filter or a `given` for
the season window would do it. `storefront.malloy` is deliberately unchanged
by this run: the report proposes, and a separate improve step with an
acceptance check would decide.

## Eval failures

**One, and it cost the run's artifacts.** The run directory
`runs/baseline-01/` was untracked when an external branch switch removed it,
taking `events.jsonl`, the per-case transcripts and the diagnosis with it.

What survived, and why the numbers above are still quotable: the run package
had already been built from `events.jsonl` (`runs/run-package-baseline-01/`,
holding attempts, scores, calls, entities and retrieval as CSVs) and the
console summary was captured (`runs/baseline-01-console.log`). Every figure
in this file comes from one of those two.

What did not survive: the answerer transcripts, the judge's full reasoning
per case, and the diagnosis beyond its cluster name and component. Those
would need the arm re-running.

The lesson is one the loop's own doctrine already states -- keep durable
outputs in the repository -- and it was followed for the SET and not for the
RUN. The set was committed before the arm; the arm's output was not
committed until after it had already been destroyed once.

## Query errors

None. Every attempt produced a query that ran.

## What this run taught us

**A well-documented model plus a capable agent answers most questions, and
the predictions about which ones would fail were mostly wrong.** Four of the
five failures forecast before the run did not happen:

- **net sales** was predicted to fail because `total_sales` carries no status
  filter. The agent found `status` and filtered it.
- **the signup cohort** was predicted to fail as an unmodelled concept.
  `customers.signup_date` is exposed as a dimension, the join is declared,
  and the agent wrote the two-step route.
- **discount off list** was predicted to fail the same way.
  `products.retail_price` is a dimension too, and the agent avoided the
  grain trap that caught this set's own author.
- **the duplicate-name trap** was predicted to catch it. 1,000 customers
  hold 769 distinct names, and grouping spend by `full_name` merges three
  Mila Costas into a false top spender of $17,988. The agent grouped by
  `customer_id` and returned Delilah Okafor, $8,817.56.

Only the business convention failed, and it is the one category that no
amount of field documentation can close: the rule exists nowhere in the
data. That is a sharper claim than the run was designed to make, and it is
worth more than the 100% is.

**Two of the eval's own rubrics were wrong before the run**, both asserting
the model exposed nothing for a question when `get_context` returns the
dimension. Reading the real entity list is what caught it. A rubric that
misstates what the model holds steers a judge, and it survived two rounds of
authoring.

## What to do next

1. **Add business-convention questions.** This is the one category that
   reliably fails, and real businesses are full of them: a fiscal calendar
   that is not the calendar year, "active customer" as ordered-in-90-days,
   "repeat customer" as two or more orders, full-price against markdown.
   Each is unguessable and each has a clean proposed fix.
2. **Add multi-step analytics**, which this set barely tests: year-over-year
   growth by category, retention of a signup cohort, regional
   over-indexing against the national mix. Those stress construction rather
   than vocabulary.
3. **Commit the run directory** as soon as the arm finishes, before building
   anything from it.
4. Only then consider `improve`, and re-split the set first: with every case
   `dev` there is no holdout to defend an acceptance check.
