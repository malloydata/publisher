# Eval event records

The contract between evaluation stages is append-only events in Publisher's
eval store (`POST /api/v0/evals/runs/:runId/events`). `eval-answer` writes
`attempt`, `tool_call`, and `score`. `eval-diagnose` writes `issue` and
`issue_status`. `eval-improve` writes `candidate`. `eval-loop` writes `gate`,
further `issue_status`, and `checkpoint` (created after an accepted gate, or
restored).

Do not keep a parallel ledger.jsonl as source of truth. Export
`attempts.jsonl` / `issues.jsonl` only for review. A stage never rewrites
another stage's fields.

## Event envelope

Every event has `runId`, optional `caseId`, `kind`, and `payload`.
`kind` is one of: `attempt`, `tool_call`, `score`, `issue`, `issue_status`,
`candidate`, `gate`, `checkpoint`.

Goldens live on the case. Bump `golden_revision` on every golden change and
stamp that revision on each `score` so a re-baseline cannot silently change
an earlier run. Optional `golden.neededEntities` is the retrieval gold: entity
ids `score_retrieval.py` scores against. Leave it off until diagnose (or a
human) names the set; do not invent it from the question. A set-level integer `version` (on `eval.json` and
`eval_sets.metadata.version`) bumps when any golden in the set is repaired;
record it on the new run as `config.setVersion`. Issue status is the latest
`issue_status` for that `issue_id`.

## `attempt`

| Field | Type | Notes |
|---|---|---|
| `qid` | string | Stable case id. |
| `sample` | int or null | Which repeat. Required even when null. |
| `mode` | string | `measure` / `triage` / `improve`, or the five-step names `scrape` / `eval` / `diagnose` / `improve` / `checkpoint`. |
| `phase` | string | `baseline` / `loop` / `blind_gate` / `canary` / `final`. |
| `question_sha` | string | Hash of the exact text the answerer saw. |
| `submitted` | bool | False when there was no final query. Not a wrong answer. |
| `final_query` | string or null | The query that will be scored. Required to replay. |
| `servedRevision` | string or null | From the package actually queried. |
| `sourceContentSha` | string or null | Bytes the worker compiled. |
| `n_get_context` | int | |
| `n_execute` | int | |
| `n_execute_errors` | int | |
| `host_tool_uses` | int | Host-side count, including Read and Shell. |
| `reported_calls` | int | MCP calls the answerer claimed. |
| `contaminated` | bool or `"unknown"` | From `scripts/check_contamination.py`. |
| `contamination_reasons` | list | Empty when clean. |

## `tool_call`

One event per MCP `get_context` or `execute_query`. Link evidence; do not
embed the full trace.

| Field | Type | Notes |
|---|---|---|
| `tool` | string | `get_context` or `execute_query`. |
| `traceId` | string or null | `get_context` only. Lookup via `malloy_getTrace`. |
| `rankedSummary` | object | Entity ids, per-target ranks, `belowCutoffCount`, retrieval config hash. Copied at capture so an issue survives trace eviction. |
| `error` | string or null | |

Never persist `execute_query` result rows, givens, or credentials.

## `score`

| Field | Type | Notes |
|---|---|---|
| `golden_revision` | int | From the case at score time. |
| `contaminated` | bool | If true, `answer_score` is null and the attempt is excluded. |
| `answer_score` | object or null | Null when not submitted, not scorable, or contaminated. |
| `answer_score.f1` | float | Optimization signal. |
| `answer_score.ex_strict` | bool | `f1 == 1`. Reported, not optimized. |
| `answer_score.precision` | float | |
| `answer_score.recall` | float | |
| `answer_score.n_gold` | int | |
| `answer_score.n_pred` | int | |
| `answer_score.matched` | int | Never exceeds `n_gold` or `n_pred`. |
| `answer_score.failure_bucket` | string or null | Null iff `ex_strict`. |
| `retrieval_score` | object or null | Null when the needed set is unknown or the attempt is contaminated. |
| `retrieval_score.context_recall` | float | Fraction of needed entities that appeared in any `get_context` result. |
| `retrieval_score.mrr` | float | Mean over the needed set of `1/best_rank` (0 if missing). Per-entity MRR. |
| `retrieval_score.rr_first` | float | `1` over the best rank of any needed entity; 0 if none found. Classic first-hit RR. |
| `retrieval_score.n_needed` | int | |
| `retrieval_score.n_found` | int | |
| `retrieval_score.ranks` | object | Needed id → best 1-based rank, or `null`. |
| `f1_adjudicated` | float or null | Never overwrites `f1`. |
| `gold_status` | string | `verified` / `verified_benign` / `suspect` / `verified_wrong`. Case `golden.status` may also be `ambiguous`: not scorable until a later sample or a human picks a replacement. |
| `exclude_from_scoring` | bool | Set when `verified_wrong`. |

`ex_strict` true implies a null `failure_bucket`. A `submitted: false` attempt
carries no `answer_score`.

## `issue`

| Field | Type | Notes |
|---|---|---|
| `issue_id` | string | Stable across status events. |
| `qids` | list | Affected cases. |
| `primary_code` | string | From `skill:eval-diagnose`, verbatim. |
| `contributing_codes` | list | |
| `component` | string | `dataset` / `agent-call` / `get_context/model` / `get_context/retrieval` / `construction` / `model-definition`. |
| `owner` | string | `model` / `retrieval` / `agent-skill` / `dataset` / `environment`. |
| `severity` | string | |
| `confidence` | string | |
| `context_recall` | float or null | From `scripts/score_retrieval.py`. |
| `mrr` | float or null | Per-entity MRR from the same script. |
| `sufficiency` | string | `sufficient` / `insufficient` / `unknown`. |
| `traceIds` | list | |
| `destination` | string | Phase 1, Phase 2, model, skill, tool, or environment. |
| `diagnosis` | string | Written before any edit exists. |

## `issue_status`

| Field | Type | Notes |
|---|---|---|
| `issue_id` | string | |
| `status` | string | `open` / `batched` / `fixed` / `rejected` / `deferred`. |

Readers take the latest event for that `issue_id`.

## `candidate` and `gate`

| Field | On | Notes |
|---|---|---|
| `issue_ids` | both | |
| `files` / `servedRevision` | candidate | What changed. |
| `probes` | candidate | Query and result for each factual claim. |
| `decision` | gate | `accepted` / `rejected`. |
| `class` | gate | `docs` / `definition` / `retrieval` / `justified`. |
| `baselineRunId` / `finalRunId` | gate | |
| `reason` | gate | Including independent deterministic justification. |

## `checkpoint`

Written by `eval-loop` after an accepted gate, or when a restore runs.
The bytes live on `GET /api/v0/evals/checkpoints/:id`, not in this payload.

| Field | Notes |
|---|---|
| `action` | `created` / `restored`. |
| `checkpointId` | |
| `label` | |
| `servedRevision` | Package revision at create, or after restore reload. |
| `issueIds` | Issues the accepted edit closed. Empty on restore. |

## Rules

- One artifact directory per attempt (`qid` + `sample`). Never overwrite a
  previous attempt's prediction CSV.
- Append, never edit. To void, write a new event and mark the old payload
  `voided`.
- End-of-run numbers come from querying events, never from agent arithmetic
  in a summary paragraph.
- Cost is optional. Stage linkage (`mode`, `phase`, run config) is not.
