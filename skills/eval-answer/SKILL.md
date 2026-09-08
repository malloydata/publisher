---
name: eval-answer
description: 'Score one analytical answer against a verified golden, and score which of the entities the golden depends on retrieval delivered to the answerer. Run the contamination checklist, re-execute the submitted query yourself, then spawn a judge subagent per skill:eval-judge. Append attempt, tool_call, score, and retrieval_score events to the file ledger (reference/ledger-schema.md). Never explain the failure (eval-diagnose) or edit the model (eval-improve). Use when asked whether an answer was correct, to score a run, or to baseline a model.'
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
   under-report floor is reported at most total tool uses). `host_tool_uses`
   is EVERY tool use the host logged, MCP calls included; while it counted only
   the non-MCP ones this comparison was true of almost every clean attempt, so
   a run whose attempts predate the split cannot be checked this way.

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

A named view is a submitted query. `execute_query` takes either ad-hoc Malloy
or a `queryName` plus `sourceName`, and its own tool description steers an
answerer to the named form; record it as the Malloy it stands for,
`run: <source> -> <view>`, which re-executes and reads the same as any other.
Capturing only the ad-hoc form recorded an attempt that did query as
`submitted: false` with no query to re-execute, and the judge then graded prose.

`submitted: false` when there is no final query. That is not a wrong answer, and
it is not by itself a reason to withhold a verdict. No verdict can be issued
(`verdict: null`, with the reason) when the attempt produced neither a query nor
any answer text, when the golden is missing, provisional, invalid, or ambiguous,
or when a verified golden has no local artifact to compare. An attempt that
wrote prose and ran nothing IS judged: against a golden holding a value, an
answer containing none of it is `no_match` however well it reasons.

## Step 3: Judge the answer

Spawn one fresh judge subagent per attempt, following
`skill:eval-judge` (the rubric, the anchors, and the output shape live
there; this skill does not restate them). Give it the question, the golden,
your re-executed prediction rows, the canonical query when present, and the
relevant source and field definitions from the model. It returns
`{verdict, confidence, why, column_pairing}`.

- The judge sees gold. It is therefore never the answerer, and its verdict
  never leaks back to any answerer.
- Confidence 5 or lower records as `needs_human`: neither a pass nor a fail,
  excluded from acceptance arithmetic, queued for a human look.
- `near_match` is also neither. It means defensibly different, not "nearly a
  pass", and it stays out of the pass rate and the acceptance check for the same reason
  `needs_human` does. Report the count; do not fold it into either column.
- When a human overrules a verdict, append the case to
  `evals/<set>/judge-regressions.jsonl`.
- For a scalar golden, the same protocol applies to a one-value prediction.
  For `unanswerable`, a refusal that names the gap is the pass; a confident
  numeric answer is the fail.
- Large row sets are still the judge's job. There is no scripted row oracle:
  a script that can pass a wrong answer is worse than none, and the rubric's
  containment and column-pairing rules are what the comparison needs.
- `golden.mustNotUse` is the exception, and it is not the judge's. It names the
  similar-but-wrong field, and using one is a failure however good the number
  looks, which is a question about query TEXT. Run
  `scripts/check_must_not_use.py` over the final query: a named field found
  there forces `no_match` and records `must_not_use_hits`, keeping the judge's
  own verdict beside it as `judge_verdict`. Only a BARE name vetoes. An entry
  with a connective (`weekly_active_users as a cumulative series`, `product.cost
  through the order_items join`) objects to a use of the field rather than to
  the field, so it goes to the judge as prose, as does an entry that names no
  field at all and a path's bare leaf. A veto that fires on a correct answer is
  worse than one that misses, and reading the head of `X as ...` as "ban X"
  failed an answer whose only sin was showing X as an extra column.

## Step 4: Score what retrieval delivered

Per attempt, mechanically, from the ledger -- `scripts/score_retrieval.py`. Each
case names the entities its answer depends on (`expectedEntities.required`, and
`requiredAnyOf` groups where the model offers more than one route). An entity
was delivered if the attempt's `get_context` calls returned it as a ranked entity
under its id, under the same type and name on a sibling source, or by name inside
a returned source's documentation -- text the answerer reads and acts on. Only
`missing` is a retrieval miss; the route per entity is recorded so the strict
count is still there.

Recall 1.0 with a wrong answer exonerates retrieval: the failure is in the query.
Recall below 1.0 and `coverage: covered` means the entity existed and search did
not surface it; `derivable` or `absent` means there was nothing to surface. Those
look identical in an answer score and have opposite owners, which is what makes
this number worth having. It uses the search terms the answerer chose, so it
attributes a failure *within* an arm and does not compare retrieval across arms
-- that is the engine-side `eval-retrieval` skill, which does not ship here.

`scripts/check_coverage.py` measures the other half, and it is worth knowing
which question each answers. Recall asks whether search surfaced the entities a
case names. Coverage asks whether the model holds the concepts at all, decided
by READING the model: no answerer, no judge, no goldens, no warehouse. A version
can score full recall on a case the model could never have answered, and then
recall is scoring search against an expectation that was never satisfiable.

Because it runs from the model text alone it is cheap enough to point at every
published version and read as a trend, which is what it is for. Its verdicts are
`eval-diagnose`'s codes verbatim, validated against that table at startup, so
`COVERAGE`, `AMBIGUOUS`, `NO-DISAMBIG` and `CONVENTION` mean there exactly what
they mean here. It writes nothing: not a golden, not the case-level `coverage`
field, not a run directory. Where its verdict and that field disagree is where a
version regressed, and the field is the standing judgement about the question
while this is a measurement against one build.

How to invoke it. Either `--model <file-or-dir>` for a local package or
`--publisher <url> --package <pkg>` for a served one, plus `--set <dir>`, and
`--version <label>` to stamp the report so a trend has an x-axis:

```
python3 check_coverage.py --set evals/ecommerce --model model.malloy --version 0.0.58
```

Two flags change what the number means, so choose them rather than inheriting
them. `--repeat N` samples each case N times and takes the majority; it defaults
to **1 for a set**, because the score is a trend over many cases rather than a
verdict on one, and to 3 for `--self-check`, where a single fixture is the whole
measurement. A case that flips between samples is arguable rather than covered,
so a tie goes to the gap, and two DIFFERENT gaps tying leaves the case undecided
and out of the denominator. `--self-check` runs the shipped fixture instead of a
set, which is how to confirm the checker still detects a gap it is known to
detect; run it after editing the prompt, the verdict vocabulary **or the fixture
itself**. The tests pin the fixture's shape and cannot pin its verdict, because
the judge needs a live model, so a fixture swap that goes unmeasured leaves the
whole metric unguarded.

Undecided cases are excluded from the percentage and reported on their own line.
Read that line: a coverage number over a handful of decided cases is not a
measurement, it is a sample size.

**Read `reference/coverage-limits.md` before quoting a coverage number.** Run
against all 49 hand-labelled cases of the `evals/ecommerce` set it disagreed
with the authored label on 17, while the two headline percentages landed two
points apart because the disagreements cancel. Checking each one against the
model found the LABEL was the stale side more often than the verdict was: four
notes call a measure missing that the model declares, and one prescribes a
filter on a field the model documents as "not evidence of a sale". So read a
coverage number as a rough signal over many cases, never as a verdict on one and
never as a small movement between versions, and use `--compare-labels` to print
the disagreements and triage them by reading the model. Agreement with the
labels is not a score for the checker; the two answer different questions on
purpose. Separately, the whole model goes into one prompt per case, so past a
few thousand lines it stops running on Linux and above about 100 KB the verdict
stops being stable. Do not take the over-size message's advice to narrow
`--model` to one file, which drops every imported source and manufactures
`COVERAGE` verdicts.

## Step 5: Distrust the golden

A reference answer can be wrong (parent-column fanout, a join on a shared
non-identifying key, or a rubric describing a model that has since been fixed).
Fanout is not automatically a defect: `AVG` / `STDDEV` / `MIN` / `MAX` survive
uniform duplication. Classify `verified_wrong` (exclude from scoring) vs
`verified_benign` (keep).

**The judge produces this, not you.** It is the only station holding the golden,
the re-executed rows and the model source at once, so it is the only one that can
see the key contradict any of them; the rules and the four values are in
`skill:eval-judge`. Carry its `gold_status` and `gold_note` onto the score
event unchanged, and where it says nothing, fall back to the case's standing
`golden.status`.

Do not encode a rewrite of a bad golden into the model. A `suspect` or
`verified_wrong`, or a no_match whose why indicts the golden rather than the
prediction, routes to the golden side door in `skill:eval-loop` as a **dataset**
issue. It is never a model failure, and it must be settled before improve runs --
otherwise a modelling agent is dispatched to fix a model that is already right.

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
- The `malloy-analysis-pitfalls` skill: checks before you trust a result you ran.
  The judge loads it by name; it is not part of the `eval` group.
