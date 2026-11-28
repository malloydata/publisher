## Events

Every line in `events.jsonl` is **flat**: `{ "kind": ..., <fields> }`, one
JSON object per line, with an optional `at` ISO timestamp. Case-scoped kinds
(`attempt`, `tool_call`, `score`) carry `qid`, `sample`, `phase` on the line;
run-level kinds do not. (An earlier draft of this document nested fields under
a `payload` with a `caseId`; no writer ever did that, and 23 run directories
exist in the flat shape, so the flat shape is the contract.) `kind` is one of:
`attempt`, `tool_call`, `score`, `retrieval_score`, `issue`, `issue_status`,
`candidate`, `acceptance_check`, `checkpoint`.

### `attempt`

| Field | Type | Notes |
|---|---|---|
| `qid` | string | |
| `sample` | int or null | Which repeat. Required even when null. |
| `phase` | string | `baseline` / `loop` / `blind_gate` / `canary` / `final`. `phase` lives here, on the attempt, not in run config. |
| `question_sha` | string | Hash of the exact text the answerer saw, so two runs can be compared on whether they asked the same thing. A case may carry `questionSha`, the seal stamped at conversion, and that wins when present; the ecommerce set writes a 16-char prefix of it, and a set without one left this null. Neither form proves the question was not edited, since both follow the case as it now reads: that comparison is check 6 of `verify_goldens.py`. |
| `submitted` | bool | False when there was no final query. Not a wrong answer. |
| `final_query` | string or null | Required to replay. A named view is recorded as the Malloy it stands for, `run: <source> -> <view>`, so every consumer sees one shape and the query re-executes. |
| `final_query_source` | string or null | How `final_query` was chosen: `declared` (the answer printed it), `last_ok` (the last call the server answered) or `last`. `last` is a warning: a trailing sanity probe may be standing in for the answer's own query. |
| `final_givens` | object or null | The `givens` (Publisher runtime parameters, `{name: value}`) the call behind `final_query` was sent with. A query written against a given returns unfiltered rows when re-run without it. Null when the call passed none. |
| `servedRevision` | string or null | From the package actually queried. |
| `n_get_context` / `n_execute` / `n_execute_errors` | int | |
| `host_tool_uses` | int | EVERY tool use the host logged, MCP calls included. It counted only the non-MCP ones until 2026-09-03, which made the under-report check below true of almost every clean attempt. |
| `mcp_tool_uses` | int or null | The MCP subset: `get_context` plus `execute_query`. Null on a run written before the split. |
| `reported_calls` | int | MCP calls the answerer claimed. `reported_calls > host_tool_uses` is the under-report floor from `skill:eval-answer`: the answerer cannot have made more calls than the host logged. `validate_run` warns on it, and only for runs that carry `mcp_tool_uses`, because the field meant something narrower before. |
| `contaminated` | bool or `"unknown"` | `"unknown"` when no host log exists. Read it with `ledger.is_contaminated`, never for truthiness: runs written before 2026-09-03 carry the strings `"true"`/`"false"`, and `bool("false")` is True. `ledger.event` now rejects those strings on write; `validate_run` grandfathers them on read as a warning. |
| `contamination_reasons` | list | Empty when clean. |
| `input_tokens` / `output_tokens` / `cache_read_tokens` / `cache_write_tokens` | int or null | Answerer token usage, the four counts that reprice a run from its own ledger. Null when the host does not report it. `cost_usd` beside them comes from the CLI's `total_cost_usd`, which prices cache reads and writes at their rates, so it was always cache-inclusive; what was missing was the breakdown that reproduces it. `cache_write_tokens` (`cache_creation_input_tokens` upstream) was never captured before 2026-09-15, and on one analysed run cache writes were 44% of the agent's cost, the single largest line. **Read `input_tokens` with care:** it counts only the tokens after the last cache breakpoint. One run recorded 224 for 24 questions beside 2.4M cache reads; the 224 is not the context volume, and a token total that omits the cache columns is not a size. |
| `cost_usd` | float or null | Answerer cost for this attempt. |
| `num_turns` / `wall_seconds` | int, float or null | |
| `answer_text` | string or null | The answer the judge scored. Kept so a verdict can be re-read without the transcript. |
| `skills_invoked` | list of string | The skills the answerer invoked through the `Skill` tool, deduplicated and sorted, counted from its own transcript. Always a list on a run that recorded it, so an EMPTY list is the finding: the arm granted a manifest and the agent reached for none of it. Absent on runs written before 2026-09-15, which is why every reader takes `or []`. Only explicit `Skill` calls are counted, and only skills the arm granted -- one outside `--answerer-skills` is a contamination breach and lands in `contamination_reasons` instead. What it is for: a manifest names what an arm GRANTED, and comparing two arms' doctrine is meaningless if the doctrine never loaded. |
| `transcriptPath` | string | The answerer's transcript under `artifacts/`. |

Token counts sit here rather than being derived later because the claim a
semantic model makes is about cost as well as correctness -- that a documented
model reaches a good answer in fewer turns and tokens than working from raw
schema. A ledger that counts calls but not tokens can state half of that.

### `tool_call`

One event per MCP `get_context` or `execute_query` the attempt made.

| Field | Type | Notes |
|---|---|---|
| `tool` | string | `get_context` or `execute_query`. |
| `traceId` | string or null | `get_context` only; look up in your host's trace store. |
| `targets` | string, list or null | **What the answerer asked for**: the search terms it sent to `get_context`. Null for `execute_query`. |
| `rankedSummary` | object | Copied at capture so evidence survives trace eviction: `entityIds` (identity, in response order), `hits` (one row per entity: `entity_id`, `source`, `relevance`, `matched_targets`, `position`, `position_in_source`), `resultCount`, `docTokens`. **There is no rank.** This field previously documented `ranks` and "per-target `targets` with within-target ranks"; no writer ever wrote either, and what `ranks` did hold was `range(1, n+1)` over a list already flattened source-card by source-card -- a position, not a rank, and not even a relevance order, since the response sorts globally and then buckets into cards. Ask relevance questions of `hits[].relevance`. |
| `rankedSummary.hits[].matched_targets` | list or null | Which of the caller's search targets matched this entity, each with the server's own relevance. **`null` means no attribution was published**, which is not the same as `[]`: the server attaches it to entities only (a source card's attribution is discarded before an entity object exists) and only on the semantic path, because a lexical score is relative to its own query and comparing two of them means nothing. A lexical run therefore cannot say which target found what, and a reader must report that as unavailable rather than as "never asked". |
| `retrieval_mode` | string or null | `get_context` only: the `retrieval` field of the response that answered, `semantic` or `lexical`. Null when the server named none, which means no embedding provider. Recorded per call because the semantic path can fall over partway through a run. |
| `query` | string or null | `execute_query` only: the Malloy the call sent, so the final query can be chosen by which call the server actually answered. |
| `modelPath` | string or null | `execute_query` only: the model file the call named. A source does not resolve outside the file that declares it, so re-execution needs this. |
| `givens` | object or null | `execute_query` only: the `{name: value}` runtime parameters the call was sent with. Part of the same fact as `query` and `modelPath`: the text alone, re-run without them, returns a different population. |
| `filterParams` | object or null | `execute_query` only: values for the model's legacy `#(filter)` source filters, a separate mechanism from `givens`. The harness captures `givens`; this stays allowed for records that carry it. |
| `error` | string or null | |

Never persist `execute_query` result rows or credentials. `givens` are recorded because they decide what the query means; they are filter values, not secrets, so do not put a credential in one.

`targets` records the request; `rankedSummary` records the response. Without
both, a low per-attempt recall has two readings that cannot be told apart: the
answerer searched for the wrong thing, or it searched well and retrieval ranked
the right entity too low. Those have opposite owners -- `agent-skill` and
`retrieval` -- so a ledger holding only the response cannot attribute the
failure, and any recall computed from it is a blend of answerer behaviour and
retrieval quality.

That blend is also why per-attempt recall is **not** comparable across arms: a
stronger answerer searches better and scores higher retrieval recall without
retrieval having changed. Use it for attribution within an arm. Comparing
retrieval itself between engine versions is `eval-retrieval`'s job, with fixed
terms, and is not something a customer run reports.

### `score`

The answer judge's verdict for one attempt (protocol in
`skill:eval-judge`). Every attempt in a scored run gets exactly one.

| Field | Type | Notes |
|---|---|---|
| `verdict` | string or null | `match` / `near_match` / `no_match` / `needs_human`; null when the attempt is not scorable. Only `match` and `no_match` are decisions; see below. |
| `reason` | string | Why, from the judge; for a null verdict, why not scorable, and it is ALWAYS one of these: `not_submitted`, `answerer_truncated`, `contaminated`, `judge_unparseable`, `no_saved_verdict`, `judge_error: <text>`, or `golden_<status>` -- `golden_missing`, `golden_provisional`, `golden_invalid`, `golden_ambiguous`, `golden_verified_wrong`. A null verdict never arrives without one: a nulling that does not say which of these it was is indistinguishable from the field being dropped on the write path, and was read as exactly that from a real run. The `golden_*` reasons and `answerer_truncated` are decided before the judge is called, so neither an unestablished key nor a cut-off attempt costs a judge call. |
| `confidence` | int or null | 1 to 10. Confidence of 5 or lower forces `needs_human`. |
| `column_pairing` | object or null | The judge's named gold-to-prediction column correspondence. |
| `judge_version` / `rubric_sha` | string | Pins which rubric produced this verdict. |
| `golden_revision` | int | From the case at score time. |
| `contaminated` | bool or `"unknown"` | Copied from the attempt; true or unknown means `verdict: null`. |
| `must_not_use_hits` | list or null | Entries of `golden.mustNotUse` a script found in the final query. A hit forces `verdict: no_match`. Decided by `check_must_not_use.py`, never by the judge: it is a question about query text. Only a BARE name vetoes; an entry with a connective (`X as ...`, `X through ...`) objects to a use of X rather than to X, and goes to the judge with the prose entries and a path's bare leaf. |
| `judge_verdict` | string or null | What the judge said when a MECHANICAL override replaced it: a `must_not_use_hits` veto, or contamination voiding the verdict. Null otherwise, so the judge's own agreement rate stays measurable across both. When a case is vetoed AND contaminated this still holds the judge's read, not the veto's `no_match`. |
| `artifactPath` | string | The full judge output under `artifacts/`. |
| `gold_status` | string | `verified` / `verified_benign` / `suspect` / `verified_wrong`, and never anything else: the case file's `golden.status` is a DIFFERENT vocabulary that shares only `verified` and `verified_wrong`, and only those two fall back into this field. A `provisional` / `invalid` / `ambiguous` key is reported through the verdict's `reason` (`golden_<status>`) instead, so nothing downstream has to match on two vocabularies in one field. **From the judge**, which scored against the golden as written and reports separately whether it believes it; falls back to the case's standing `golden.status` when the judge does not say, written onto the verdict itself so the run summary and the aggregates see it too. `verified_wrong` excludes the case from run aggregates -- from the judge OR from the case file; assigning the fallback into a copy of the verdict meant a key the set had marked wrong stayed in the aggregates unless the judge independently agreed. `suspect` and `verified_wrong` route to the golden side door as `dataset` issues, never to improve. |
| `gold_note` | string or null | The judge's evidence for a non-`verified` status: the two values, or the model line against the rubric sentence. Null when `verified`. |

A `golden_*` reason means the ANSWER KEY is not something a verdict can be
issued against. A golden the set stamps `provisional`, `invalid` or `ambiguous`
is refused, as is a case with no golden at all and one the set marks
`verified_wrong`; a golden with NO `status` is unguarded rather than
unscorable, because sets that predate the field would otherwise score zero of
zero. A golden holding no value is not refused either: that is how a
`kind: unanswerable` case and a `kind: criteria` golden are written. This
matters most for an imported set, because `skill:eval-import` stamps
`provisional` on every golden it writes and enforces it -- so a set is
unscorable by construction until its keys are re-derived, and `run_baseline`
refuses to spend at all when every case in a set is in that state.

`not_submitted` means the attempt produced NOTHING to judge: no answer text and
no query. An attempt that wrote prose without querying **is judged**, and
against a golden that holds a value an answer containing none of it is
`no_match` (`skill:eval-judge`, `reference/refusal.md` rule 10). Excusing those
as unscorable dropped a confident refusal on an answerable case out of the pass
rate, which is the one thing the answerable-sounds-unanswerable cases exist to
measure. A refusal is exempt only where `golden.kind` is `unanswerable`, and
there naming the gap is the pass and a confident number is the fail.

`answerer_truncated` means the answerer hit the run's `maxTurns` cap and the
CLI returned whatever it had, which is usually a sentence stopping mid-thought.
It is refused BEFORE the golden gate and before a saved verdict is reused, so
`--rebuild` over a run made without this re-derives the right ledger from its
transcripts. A cap is a harness setting, so a case cut off by one says nothing
about whether the model could answer it: measured on one 29-case arm, four
attempts ended at exactly 31 turns and each was judged as a wrong answer, one
of them on the text "Let me build the correct monthly trend query."

`contaminated` means the attempt breached isolation (`contamination_reasons`
names how) and the verdict is withheld rather than lost. The judge's own read
moves to `judge_verdict`. Before this reason was written, a voided verdict kept
the judge's prose in `reason` beside `verdict: null`, and a run report
reasonably concluded the ledger was dropping the field -- it recovered nine
"lost" verdicts by re-parsing the stored judge replies, every one of which had
been voided on purpose.

Both are HARNESS-owned, not model-owned, and a run containing either reports
`status: incomplete` in `run.json` and **prints no pass rate**. `unscorable`
(no established golden) and `judge_unparseable` do not suppress the rate: the
first is a dataset state no arm can fix, and the second already has a retry.

Aggregates count decided verdicts only. `match` and `no_match` are the pass and
the fail; **`near_match`, `needs_human` and null are none of the above** and stay
out of acceptance arithmetic. `near_match` is excluded because it means "defensibly
different", and an arguable verdict that moves a pass rate is a measurement
artefact rather than a result (`skill:eval-judge` rule 7). Report its count:
it rising is how a set tells you its rubrics are going vague.

This applies to `score`. On `retrieval_score` below, `near_match` **is** counted
towards recall and precision, because an overlapping entity is a genuine
retrieval success. The two are different questions that share a word.

### `retrieval_score` (written by `eval-retrieval`, not by a customer run)

One per term judged in a fixed-term replay (run-level; no `caseId`).

| Field | Type | Notes |
|---|---|---|
| `intentId` / `term` / `entityType` | string | From the term file. |
| `in_scope` | bool | Does THIS model version represent the concept? False is a coverage gap, not a retrieval failure. |
| `judgments` | list | Per returned entity: `entityId`, `rank`, `level` (`match` / `near_match` / `no_match`), `confidence`, `why`. Empty when nothing returned. |
| `judge_version` / `rubric_sha` | string | |
| `traceId` | string or null | The `get_context` call judged. |

Run-level metrics fall out by counting:

- `coverage` = in-scope terms / valid terms -- a property of the model and its
  data, reported on its own, never as a retrieval number.
- `recall` (on in-scope terms) = fraction whose judgments contain a `match`.
- `precision@N` = `match` judgments / judged, with N stated.

### `issue`

| Field | Type | Notes |
|---|---|---|
| `issue_id` | string | Stable across status events. |
| `qids` | list | Affected cases. |
| `primary_code` / `contributing_codes` | string / list | From `skill:eval-diagnose`, verbatim. An event written before the codes were renamed may carry a retired one: `COVERAGE` is now `MISSING`, `NO-DISAMBIG` is `AMBIGUOUS`, and `CONVENTION` is `RULE_UNWRITTEN`. Read those as the new name; nothing rewrites old events. |
| `component` | string | `dataset` / `agent-call` / `get_context/model` / `get_context/retrieval` / `construction` / `model-definition`. |
| `owner` | string | `model` / `retrieval` / `agent-skill` / `dataset`. Environment failures stop the run; they are never diagnosed, so there is no environment owner. |
| `severity` / `confidence` | string | |
| `sufficiency` | string | `sufficient` / `insufficient` / `unknown`. |
| `traceIds` | list | |
| `diagnosis` | string | Written before any edit exists. |

### `issue_status`

`issue_id` plus `status`: `open` / `batched` / `fixed` / `rejected` /
`deferred`. Readers take the latest event for that `issue_id`. Do not invent
a status column.

### `candidate`

Written by `eval-improve`, for every proposed edit, accepted or not. A
rejected direction keeps its record.

| Field | Notes |
|---|---|
| `issue_ids` | |
| `files` | Paths the edit touched. |
| `diffSummary` | One line per file. |
| `probes` | Query and result for each factual claim. May be empty for a `class: skill` candidate that asserts nothing about the data: a guide making no factual claim has nothing to probe. Empty is not a missing receipt there; a guide that DOES quote a number still needs one. |
| `meaningChanged` | Entities whose *meaning* the edit changed; `[]` for a docs-only edit. |
| `goldenSuspect` | Each `{qid, entity, stored, rederived}`: a golden this edit may have invalidated. Reported by the improver, never repaired by it. **Non-empty halts the acceptance check** until adjudicated through the golden side door. |
| `goldenAudit` | The set's `verify_goldens.py` run against the edited model: `{ran, clean, model, tail}`. Catches drift and rubric-vs-model contradictions only; a golden whose value silently moved is invisible to it, which is why `goldenSuspect` exists alongside. |

### `acceptance_check`

Called `gate` before 2026-09-03. `ledger.py` reads the old kind as this one, so
run directories written earlier still validate and nothing rewrites them.

Written by `eval-loop`, one per acceptance check decision, BEFORE any checkpoint commit.

| Field | Notes |
|---|---|
| `issue_ids` | |
| `decision` | `accepted` / `rejected`. |
| `class` | `docs` / `definition` / `skill` / `retrieval` / `justified`. `skill` is an edit to a package skill in the package's `default` skill set; it never takes the documentation shortcut in the acceptance check. |
| `baselineRunId` / `finalRunIds` | Plural: acceptance needs two independent runs. |
| `regressions` | Case ids whose verdict got worse vs baseline. Must be empty to accept. |
| `holdoutDelta` | Confident-verdict delta on the holdout slice. |
| `reason` | Including independent deterministic justification when that is the basis. |

### `checkpoint`

Written by `eval-loop` after an accepted acceptance check, or when a restore runs. The
model bytes live in git, not in this payload.

| Field | Notes |
|---|---|
| `action` | `created` / `restored`. |
| `label` | |
| `modelGitSha` | The commit this checkpoint names (create), or the commit restored to. |
| `issueIds` | Issues the accepted edit closed. Empty on restore. |

## `judge-regressions.jsonl`

Append a line whenever a human overrules a judge verdict: the case or intent,
the judge's verdict, the human's, and why. Re-run this file against the judge
whenever `skill:eval-judge` or the judge model changes; a rubric change that
flips old human-settled verdicts is a judge regression, not new truth.
