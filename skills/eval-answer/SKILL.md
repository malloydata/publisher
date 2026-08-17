---
name: eval-answer
description: 'Evaluate a single analytical answer against a semantic model: did this answer actually answer the question? Use when scoring an agent answer against a known-correct result, regression-testing a model after an edit, running a benchmark or eval set over a published package, building a baseline before changing a model, or when asked "did that answer correctly", "score this run", "is this model getting better". Produces one deterministic ledger record per answer. It does NOT say why an answer failed (that is eval-diagnose) or change the model (that is eval-improve).'
---

# Evaluate One Answer

An **answer** is one user intent, answered once. This skill decides whether that answer was
correct, records it in a fixed schema, and stops. It is the foundation the diagnosis and improvement
skills build on, and it is useful alone: run it over a question set before and after a model change
and you have regression testing for a semantic model.

**Scope boundary, enforced:** this skill produces a verdict and evidence. It never explains a
failure, never proposes a model edit, and never edits anything. Those are separate skills, and
keeping them separate is what makes a failure attributable.

## The unit of evaluation

**A chat is not the unit: the answer is.** One chat commonly spans several questions, and
retrieval can be right for questions 1–3 and wrong for question 4. Segment a chat into answers,
one per user intent, and evaluate each separately:

- A **new intent** starts a new answer.
- **Feedback on the current answer** ("that seems wrong", "break it out by region") is a *revision
  within* the answer. The final accepted revision is the graded unit; earlier revisions are
  evidence for diagnosis, not separate answers.

## Step 1: Pin what you are measuring

Do this before anything else, every time. Both failures below happened in a real run and neither
was noticed for hours.

1. **Take the question from data, never from context.** Read it from the eval set / ticket /
   transcript file. Never retype it from a console listing or from memory: an orchestrator once
   typed a question from output truncated at 110 characters and invented the rest. The agent then
   answered a question that does not exist, and it scored as a model failure. A fabricated question
   is indistinguishable from a real one downstream.
2. **Assert which model artifact you are measuring, by content hash.** Record the hash of the model
   file actually being served. A publisher maps a package *name* to a *directory*, and nothing in a
   query response distinguishes a served model from a same-named decoy sitting beside it: a
   confirmation run measured a 422-line stand-in instead of the 3,146-line model under test.
3. **Freeze the harness.** Round/call budget, prompt text, and model slug are held constant for a
   whole experiment and recorded. Raising an answer round cap from 16 to 50 moved mean F1 by 0.30 on
   an unchanged model: harness config dominates model quality if you let it float.

## Step 2: Get the answer's result, by running it yourself

**Never score an agent's reported result.** Take the final query it submitted, execute it yourself,
and score what comes back. An agent's claimed output is commentary; the query is the artifact.

Record `submitted: false` as its own outcome when the agent never produced a final query (budget
exhaustion, gave up, errored out). **Never conflate "no answer" with "wrong answer"**: they are
different failures with different fixes, and merging them corrupts every downstream rate.

## Step 3: Score against the oracle

**The comparison must be mechanical and identical across runs.** Not because a model cannot compare
rows, but because a comparison that drifts between the before and the after makes every delta
meaningless: and because the obvious shortcut is measurably wrong. Comparing rows as *strings* flips
the verdict on **9 of 26** recorded answers, every flip calling a *correct* answer wrong, because
`70495.18453531274` and `70495.1845353127` are the same number and different text.

### The contract

This is the normative part. Any implementation satisfying it is fine.

1. **Normalize each cell.** Trim whitespace and quotes; strip a trailing `%` and thousands commas;
   fold `null`/`none`/`nan`/`n/a`/`<na>`/`nat` and empty to *nothing* (excluded from matching); fold
   `true`/`false` to `1`/`0`.
2. **Round every numeric to 7 significant digits**: `round(x, 6 - floor(log10(abs(x))))`, and `0`
   for zero. Seven is not arbitrary: it sits just past float32's precision, so representation noise
   collapses while genuinely different answers stay distinct. Checked against 11,166,651 numeric
   cells; at 9 or 12 digits the noise stops collapsing and correct answers start failing.
3. **A row's key is its non-empty normalized values, sorted**, so column order and names don't matter.
4. **`matched` = the multiset intersection size** of the two key sets: a row pairs with at most one
   row on the other side.
5. `precision = matched / n_pred`, `recall = matched / n_gold`, `f1` their harmonic mean.

### Preferred implementation: a query

Use the query engine you already have. It does the arithmetic deterministically and needs no code
execution. Given the two row sets as `(rid, val)` cells:

```sql
WITH norm AS (
  SELECT 'g' AS side, rid, val FROM gold_cells
  UNION ALL SELECT 'p', rid, val FROM pred_cells
), clean AS (
  SELECT side, rid,
    CASE
      WHEN val IS NULL OR trim(val) = '' THEN NULL
      WHEN lower(trim(val)) IN ('null','none','nan','n/a','<na>','nat') THEN NULL
      WHEN lower(trim(val)) = 'true'  THEN '1'
      WHEN lower(trim(val)) = 'false' THEN '0'
      ELSE COALESCE(
        CASE WHEN num = 0 THEN '0'
             ELSE CAST(round(num, CAST(6 - floor(log10(abs(num))) AS INT)) AS VARCHAR) END,
        trim(val))
    END AS v
  FROM (SELECT side, rid, val,
               try_cast(replace(replace(replace(trim(val),'"',''),',',''),'%','') AS DOUBLE) AS num
        FROM norm)
), keyed AS (
  SELECT side, rid, string_agg(v, '|' ORDER BY v) AS k
  FROM clean WHERE v IS NOT NULL GROUP BY side, rid
), counts AS (
  SELECT k, COUNT(*) FILTER (WHERE side='g') AS gn,
            COUNT(*) FILTER (WHERE side='p') AS pn
  FROM keyed GROUP BY k
)
SELECT (SELECT COUNT(*) FROM keyed WHERE side='g') AS n_gold,
       (SELECT COUNT(*) FROM keyed WHERE side='p') AS n_pred,
       COALESCE((SELECT SUM(least(gn,pn)) FROM counts WHERE gn>0 AND pn>0), 0) AS matched
```

Only `abs`, `log10`, `floor`, `round` and a sorted string aggregate are required, so this ports
across dialects: but `try_cast`, the spelling of `string_agg`, and how a double renders as text all
vary, so **check it against a few known-good pairs on your engine before trusting it.** Validated on
DuckDB against 26 recorded answers: identical `f1`, `n_gold`, `n_pred` and `matched` to the reference
implementation on every one.

Result sets are usually small: median 67 rows, max 157, 4 columns in one real eval set: so inlining
both sides as literals is practical.

### If a query is awkward in your environment, write the code

A small script implementing the contract is a perfectly good option; this is a fallback, not a
failure. `scripts/match_rows.py` is a working reference implementation (stdlib only,
`--gold X.csv --pred Y.csv --json`) to run directly or port.

What is **not** acceptable is comparing by eye or by string equality. An LLM asked to judge row
correctness with no tools called **20 of 33 wrong answers correct**, then confabulated a mechanism a
downstream agent built its worst model edit on. Model judgment belongs at *why* something failed,
never at *whether* it failed.

### What the comparison must produce

| Field | Meaning |
|---|---|
| `f1`, `precision`, `recall` | Row-level partial credit. **F1 is the signal to optimize**, not binary correctness: a near-miss (115 of 117 rows) stays distinguishable from garbage, and binary metrics need ~4x the samples for equal power. |
| `ex_strict` | The `f1 == 1` case. Report it, don't optimize it. |
| `failure_bucket` | Names the failure shape: `empty_result`, `row_count_mismatch`, `value_mismatch`, `format_mismatch (…)`, `over_returns_rows (…)`, `no_result (query error)`. |
| `column_agreement` | **The most informative free signal.** Which target columns the answer reproduced exactly. If the extremes (min/max/range/count) match and the means/sums don't, the formulas were right and the *row set* was wrong: a population or filter-scope gap, not arithmetic. |

Two behaviours you do not need to defend against, both tested:

- **Sub-ULP float differences cannot fail a correct answer.** Numerics are rounded to 7 significant
  digits before comparison, never compared as text. This was raised as a concern mid-run, tested,
  and found unfounded.
- **An extra column does not silently zero a correct answer.** It buckets as
  `format_mismatch (extra columns, gold values all present)` and routes to adjudication. Scoring
  it as flat-zero once accounted for ~36% of a phantom null result.

## Step 4: Adjudicate the margin (only when strict fails and the answer may still be right)

Some answers are right in a different shape: an extra column, a valid alternative grain, an
equivalent framing. Adjudication is **verification work, not an opinion**: you may run queries :
project away the extra column, re-aggregate to the target's grain: and then rule, recording what
you ran. Store it as `f1_adjudicated` alongside the raw `f1`. Never overwrite the deterministic
score.

## Step 5: Be skeptical of the target

**A reference answer is not automatically right.** Two defect classes recur:

- **Parent-column fanout**: a parent-table stored value aggregated across a parent→child join.
- **Cross-contamination**: a join on a shared non-identifying attribute pooling other entities'
  rows into a group.

The subtlety that matters: **fan-out is not automatically a defect.** When duplication is uniform
within a group, AVG / STDDEV / VARIANCE / MIN / MAX are unchanged and only SUM / COUNT break: one
question was hand-verified correct despite 162x row inflation. So classify explicitly and keep a
registry: `verified_wrong` (excluded from scoring) vs `verified_benign` (kept). Carry the flag onto
the record as `gold_status` so nobody has to remember.

Without this step, an improvement loop will faithfully encode the benchmark's bugs into the model.

## Step 6: Write one record

One JSONL row per answer, append-only. **`reference/ledger-schema.md` is the definition** :
required fields, the fields `eval-diagnose` and `eval-improve` later append, and the rules that are
part of the schema rather than conventions. Read it before writing or reading a ledger; other
skills reach it by invoking `skill:eval-answer`, not by path.

**Check each record before you append it**: every required field present and correctly typed, and
the cross-field rules hold: `ex_strict` true implies a null `failure_bucket`; a `submitted: false`
record carries no score; `matched` never exceeds `n_gold` or `n_pred`; a `measure`-mode record
carries no diagnosis or edit fields.

This is not ceremony. Across the first seven runs of this loop the ledger accumulated **41 distinct
fields, of which only 11 appeared in every run**: `precision`, `recall`, `matched` and
`final_query` were absent from two runs entirely, and no run ever recorded the model hash. An
unenforced schema breaks cross-run comparison silently, which is how the predecessor pilot produced
a null result nobody could interpret. If checking by hand is unreliable at your run's scale, a short
validation script is a sensible thing to write.

**Store one artifact per record, keyed by attempt.** If an answer is re-answered, do not overwrite
the first attempt's result file: a re-run that reuses the filename makes the earlier ledger record
un-reproducible, and a later re-grade will silently report the second answer's score for the first
record. (Observed: a run where two records shared one file and only a `voided/` copy preserved the
truth.)

All end-of-run numbers come from the ledger by query, never from an agent's arithmetic.

## Sampling: one run is not a measurement

The same byte-identical model, same question, scored **1.0 / 1.0 / 0.0** across three runs. Roughly
half the variance in a single per-question score is noise, and about 89% of it is binary "which
source did the agent pick" coin-flips concentrated in a minority of questions.

- For a **measurement** (baseline, before/after comparison), sample **k ≥ 3** per question and
  report the mean with its spread. Reallocate samples proportional to each question's observed
  standard deviation: stable questions have sd exactly 0 and need k=1, which buys the same
  precision for roughly 40% of a uniform budget.
- For a **single loop answer**, k=1 is fine: a loop tolerates a noisy individual verdict. The
  *measurement* cannot.
- **Never gate a change on any-single-answer regression.** At an observed ~15% spontaneous flip
  rate, a binary re-answer guard fired on pure noise ~90% of the time, reverting 8 of 10 changes
  including ones whose retrieval surface provably did not change. Compare means over a fixed set,
  or replay stored queries deterministically.

## When there is no known-correct answer

The oracle is a plug-in. Highest to lowest trust:

| Evidence | Source | Trust |
|---|---|---|
| Stated answer | The user supplies the correct result, or corrects the agent's | High: may drive any downstream action |
| Resolved after iteration | Wrong answer → user pushes back → corrected answer accepted | Medium-high: treat as provisional; the *diff between attempts* is the signal |
| Accepted but unverified | User said "thanks" | **None.** Acceptance is the absence of a negative, never evidence of correctness |
| Doubt only | "Hmm, are you sure?" with no resolution | None as a verdict: it is a routing signal to verify |
| Retrieval-only | A reasonable request returned no confident match | No verdict on the answer at all |

When you must produce a verdict without a target, output
`verified | plausible-unverified | suspicious | contradicted` plus a confidence and an evidence
trail: never a bare pass/fail. Earn it with work: check the query computes the asked thing; check
internal consistency (totals reconcile with breakdowns, a filtered count ≤ its unfiltered count);
re-derive independently with a blind second agent and compare with the same matcher.

**Calibration warning.** Independent re-derivation *decorrelates* query-construction errors (two
agents rarely make the same slip) but *correlates* model-coverage errors (two blind agents facing
the same undocumented sibling tables coin-flip onto the same wrong one, for the same reason).
**Consensus detects query bugs well and model gaps poorly.** Never source confidence about a model
gap from two agents agreeing.

## Related skills

- **eval-diagnose**: why an answer failed, and whose fault (model / retrieval call / query
  construction). Consumes this skill's record.
- **eval-improve**: the smallest model edit that closes a diagnosed gap.
- `skill:malloy-analysis-pitfalls`: what to check before trusting a result you produced yourself.
