---
name: eval-answer
description: 'Score one analytical answer against a verified golden. Run the contamination check, re-execute the submitted query yourself, and compare rows with scripts/match_rows.py. Write attempt, tool_call, and score events to Publisher REST /api/v0/evals. Never explain the failure (eval-diagnose) or edit the model (eval-improve). Use when asked whether an answer was correct, to score a run, or to baseline a model.'
---

# Evaluate One Answer

One user intent, answered once. This skill decides whether that answer was correct,
records the evidence, and stops.

**Scope boundary:** verdict and events only. No diagnosis, no model edit.

## The unit

A chat is not the unit. Segment by user intent. Feedback ("break it out by region")
is a revision inside the same answer; grade the final accepted revision.

Take the question from the stored case (`GET /api/v0/evals/cases/:id`), never from
memory or a truncated console line. Record `question_sha` of the exact text the
answerer saw. Record `servedRevision` / model content hash from `get_context` or
reload, not the package name: a same-named decoy has been measured for hours.

## Step 1: Contamination check, before any score

The answerer can Read or Shell its way to gold. Publisher traces do not see that.

Write the host-side tool-use log (every tool name and its path or command) plus
the MCP call counts the answerer reported. Then run:

```bash
python skills/eval-answer/scripts/check_contamination.py \
  --log tool_uses.json \
  --gold-globs 'evals/**' 'results/gold/**' \
  --eval-paths '/api/v0/evals' 'eval_' 'publisher.db' \
  --model-path path/to/served.malloy \
  --reported-calls N
```

An attempt is contaminated if it touched an eval path, eval table, gold artifact,
or the model file under test, or if `reported MCP calls > host tool uses`
(the detectable under-report floor is `reported <= total tool_uses`).

Contaminated attempts get `score: null` and `contaminated: true`. They are
excluded from the run score. They are not "wrong answers."

If you cannot produce a host log, mark contamination `unknown` and do not treat
the attempt as a clean pass.

## Step 2: Re-run the submitted query yourself

Never score the agent's reported rows. Take its final query, execute it with
`malloy_executeQuery`, and write a prediction CSV.

`submitted: false` when there is no final query. That is not a wrong answer.
`score` is `null` when not submitted, when the golden is missing / provisional /
invalid / ambiguous, or when a verified golden has no local artifact to compare.

## Step 3: Mechanical comparison

Do not compare rows by eye, by string equality, or by asking a model.
An LLM asked to judge row correctness called 20 of 33 wrong answers correct.

Preferred: the contract SQL in this skill's notes, run on DuckDB, if you have
both sides as `(rid, val)` cells.

Fallback, and the reference implementation:

```bash
python skills/eval-answer/scripts/match_rows.py --gold GOLD.csv --pred PRED.csv --json
```

The contract, so any other implementation can be checked:

1. Normalize each cell: trim quotes and whitespace; strip a trailing `%` and
   thousands commas; fold `null` / `none` / `nan` / `n/a` / `<na>` / `nat` and
   empty to nothing; fold `true` / `false` to `1` / `0`.
2. Numerics compare under relative tolerance (the script), or render to 7
   significant digits if you use the SQL form: `round(x, 6 - floor(log10(abs(x))))`,
   and `0` for zero.
3. A row key is its non-empty normalized values, sorted. Column order and names
   do not matter.
4. `matched` is the multiset intersection size.
5. `precision = matched / n_pred`, `recall = matched / n_gold`, `f1` their
   harmonic mean. `ex_strict` is `f1 == 1`.

`f1` is the *answer* signal. Binary pass/fail needs far more samples for the same power.
`failure_bucket` names the shape (`empty_result`, `row_count_mismatch`,
`value_mismatch`, `format_mismatch`, `no_result`). `column_agreement` is the
cheapest hint: extremes matching while means do not is a row-set problem, not
arithmetic.

An extra column must not zero a correct answer. It buckets as format mismatch
when the gold values are all present.

For a scalar golden, write a one-row, one-column prediction and compare the same
way. For `unanswerable`, `submitted: false` or a refusal that names the gap is
the pass; a confident numeric answer is a fail.

When a needed-entity list is known (`golden.neededEntities` on the case, or
the set `eval-diagnose` already wrote), score retrieval the same way, with a
script, not by eye:

```bash
python skills/eval-answer/scripts/score_retrieval.py \
  --needed needed.json --ranked ranked.json --json
```

`needed.json` is the entity ids. `ranked.json` is the `rankedSummary` objects
from the attempt's `get_context` `tool_call` events (or traces). Do not invent
the needed set from the question's nouns. If it is unknown, `retrieval_score`
is `null`.

The contract:

- A needed id matches a returned id exactly, as a bare name
  (`space_floor` → `join:space_detail:space_floor`), or as kind+name
  (`join:space_floor`). Last segments must be equal: `floor` is not `floor_key`.
- Best rank is the minimum across every `get_context` call in the attempt.
- `context_recall` = fraction of needed entities that appeared (ignores rank).
- `mrr` = mean over the needed set of `1/best_rank`, or `0` if missing.
  That is per-entity MRR, not classic single-relevant-document MRR (which is
  `rr_first`: `1` over the best rank of *any* needed entity).
- `retrieval_score` is independent of `answer_score`. An attempt can find
  every entity and still miss the rows.

Contaminated attempts leave `retrieval_score` null, same as `answer_score`.

## Step 4: Adjudicate only the margin

If strict fails and the answer may still be right (extra column, equivalent grain),
you may project or re-aggregate and record `f1_adjudicated`. Never overwrite `f1`.

## Step 5: Distrust the golden

A reference answer can be wrong (parent-column fanout, a join on a shared
non-identifying key). Fanout is not automatically a defect: `AVG` / `STDDEV` /
`MIN` / `MAX` survive uniform duplication. Classify `verified_wrong` (exclude
from scoring) vs `verified_benign` (keep). Write that on the score event as
`gold_status`. Do not encode a rewrite of a bad golden into the model.

## Step 6: Write events, then stop

`POST /api/v0/evals/runs/:runId/events` with `caseId` set. See
`reference/ledger-schema.md` for payloads.

1. `attempt`: qid, sample, question_sha, submitted, final_query, served revision,
   call counts, contamination verdict.
2. `tool_call`: one per MCP `get_context` / `execute_query`, with `traceId` and
   the compact ranked summary from the response (entity ids, ranks, cutoff).
   Do not copy full traces into the event. `malloy_getTrace` holds the body.
3. `score`: `answer_score` (`f1`, `ex_strict`, `precision`, `recall`, `n_gold`,
   `n_pred`, `matched`, `failure_bucket`), `retrieval_score` when a needed
   set exists, `golden_revision`, `contaminated`, `score` null when the
   attempt is not scorable.

A stage never rewrites another stage's fields. End-of-run numbers come from
querying events, not from your arithmetic in prose.

`n_samples=1` is fine for a loop look. A measurement you will quote needs
`n_samples>=3` (see `skill:eval-loop`).

## Re-score after a golden repair

When `eval-loop` has patched a golden and opened a new run, this skill
runs again **without a new answerer**: same stored `final_query` (or its
saved prediction CSV), new gold artifact, new `golden_revision` on the
`score` event. Contamination does not need to be re-litigated if the
attempt was already clean. If you must re-execute, do it yourself; do not
ask the original answerer to "try again" with the new key in context.

## Related skills

- `skill:eval-diagnose`: why it failed, after this record exists.
- `skill:eval-improve`: smallest model edit, model-owned issues only.
- `skill:malloy-analysis-pitfalls`: checks before you trust a result you ran.
