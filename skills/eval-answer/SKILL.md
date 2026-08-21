---
name: eval-answer
description: 'Score one analytical answer against a verified golden, and retrieval against the intent dataset. Run the contamination checklist, re-execute the submitted query yourself, then spawn a judge subagent per reference/judge.md. Append attempt, tool_call, score, and retrieval_score events to the file ledger (reference/ledger-schema.md). Never explain the failure (eval-diagnose) or edit the model (eval-improve). Use when asked whether an answer was correct, to score a run, or to baseline a model.'
---

# Evaluate One Answer

One user intent, answered once. This skill decides whether that answer was
correct, records the evidence, and stops.

**Scope boundary:** verdict and events only. No diagnosis, no model edit.

## The unit

A chat is not the unit. Segment by user intent. Feedback ("break it out by
region") is a revision inside the same answer; grade the final accepted
revision.

Take the question from the stored case (`evals/<set>/cases.jsonl`), never from
memory or a truncated console line. Record `question_sha` of the exact text
the answerer saw. Record `servedRevision` from `get_context` or reload, not
the package name: a same-named decoy has been measured for hours.

## Step 1: Contamination check, before any score

The answerer can Read or Shell its way to gold. Publisher traces do not see
that, so the check runs on the HOST-side tool-use log you kept for the
answerer subagent (every tool name and its path or command), plus the MCP
call counts the answerer reported.

The checklist. An attempt is contaminated when its log shows any of:

1. a Read, Shell, or any file tool touching `evals/` or a gold artifact path;
2. any access to the model file under test through a file tool (the
   `modelPath` argument on an MCP `execute_query` is NOT contamination; the
   server resolves it, the answerer never reads the file);
3. `reported_calls` greater than `host_tool_uses` (the detectable
   under-report floor is reported at most total tool uses).

`skills/eval-answer/scripts/check_contamination.py` is a reference aid that
mechanizes the same checklist over a JSON log; your reading of the transcript
is the check, the script is a second pair of eyes.

Contaminated attempts get `verdict: null` and `contaminated: true`. They are
excluded from the run aggregates. They are not "wrong answers."

If you cannot produce a host log, mark `contaminated: "unknown"` on both the
attempt and its score event, and do not treat the attempt as a clean pass.

## Step 2: Re-run the submitted query yourself

Never score the agent's reported rows. Take its final query, execute it with
`execute_query`, and write a prediction CSV under the run's
`artifacts/` directory.

`submitted: false` when there is no final query. That is not a wrong answer.
No verdict can be issued (`verdict: null`, with the reason) when the attempt
is not submitted, when the golden is missing, provisional, invalid, or
ambiguous, or when a verified golden has no local artifact to compare.

## Step 3: Judge the answer

Spawn one fresh judge subagent per attempt, following
`reference/judge.md` (the rubric, the anchors, and the output shape live
there; this skill does not restate them). Give it the question, the golden,
your re-executed prediction rows, the canonical query when present, and the
relevant source and field definitions from the model. It returns
`{verdict, confidence, why, column_pairing}`.

- The judge sees gold. It is therefore never the answerer, and its verdict
  never leaks back to any answerer.
- Confidence 5 or lower records as `needs_human`: neither a pass nor a fail,
  excluded from gate arithmetic, queued for a human look.
- When a human overrules a verdict, append the case to
  `evals/<set>/judge-regressions.jsonl`.
- For a scalar golden, the same protocol applies to a one-value prediction.
  For `unanswerable`, a refusal that names the gap is the pass; a confident
  numeric answer is the fail.
- `match_rows.py` is an aid the judge may run on large row sets, never the
  verdict.

## Step 4: Score retrieval against the intent dataset

Retrieval is scored per intent row (`evals/<set>/intents.jsonl`), not per
case, and needs no golden entity ids. For each valid intent this run covers:

1. Get the ranked entities for the term: reuse the answerer's `get_context`
   call from its stored trace when one matches the intent, otherwise issue
   the call yourself with the intent's `term` and `entityType`.
2. Spawn a retrieval judge per `reference/judge.md`: it decides `in_scope`
   for this model version and judges each returned entity match, near match,
   or no match.
3. Append one `retrieval_score` event per intent with the judgments.

Coverage, recall, and precision fall out by counting events, as defined in
`reference/ledger-schema.md`. Do not compute them per attempt, and do not
invent intent rows from a case's nouns mid-run; curating `intents.jsonl` is
scrape-step work.

## Step 5: Distrust the golden

A reference answer can be wrong (parent-column fanout, a join on a shared
non-identifying key). Fanout is not automatically a defect: `AVG` / `STDDEV`
/ `MIN` / `MAX` survive uniform duplication. Classify `verified_wrong`
(exclude from scoring) vs `verified_benign` (keep). Write that on the score
event as `gold_status`. Do not encode a rewrite of a bad golden into the
model. A judge verdict of no_match with a why that indicts the golden rather
than the prediction is exactly the signal to route to diagnosis as a dataset
issue, not to count as a model failure.

## Step 6: Append events, then stop

Append to `evals/<set>/runs/<runId>/events.jsonl` with `caseId` set. Shapes
live in `reference/ledger-schema.md`.

1. `attempt`: qid, sample, phase, question_sha, submitted, final_query,
   served revision, call counts, contamination verdict, transcript path.
2. `tool_call`: one per MCP `get_context` / `execute_query`, with `traceId`
   and the `rankedSummary` copied from the trace (per-target ranks included).
   Do not copy full traces into the event; the trace store holds the body.
3. `score`: the judge's verdict object plus `judge_version`, `rubric_sha`,
   `golden_revision`, `contaminated`, `gold_status`, and the judge output's
   artifact path.
4. `retrieval_score`: one per intent judged (run-level, no caseId).

A stage never rewrites another stage's fields. End-of-run numbers come from
counting events, not from your arithmetic in prose.

Sample each case once. Breadth across cases beats repeats of one case; the
comparison rule for a before/after is the flip count in `skill:eval-loop`
Measurement, not a mean over samples.

## Re-score after a golden repair

When `eval-loop` has repaired a golden and opened a new run, this skill runs
again **without a new answerer**: same stored `final_query` (or its saved
prediction CSV), new gold artifact, fresh judge, new `golden_revision` on the
`score` event. Contamination does not need to be re-litigated if the attempt
was already clean. If you must re-execute, do it yourself; do not ask the
original answerer to "try again" with the new key in context.

## Related skills

- `skill:eval-diagnose`: why it failed, after this record exists.
- `skill:eval-improve`: smallest model edit, model-owned issues only.
- `skill:malloy-analysis-pitfalls`: checks before you trust a result you ran.
