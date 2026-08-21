# The eval ledger: files and events

The ledger is plain files in the model package's git repository. There is no
eval API and no eval database. The conductor (`skill:eval-loop`) reads and
writes these files directly; the stages share them as their contract.
`eval-answer` writes `attempt`, `tool_call`, `score`, and `retrieval_score`.
`eval-diagnose` writes `issue` and `issue_status`. `eval-improve` writes
`candidate`. `eval-loop` writes `gate`, further `issue_status`, and
`checkpoint`.

## Layout

```
evals/<set>/
  set.json                  # set metadata (below)
  cases.jsonl               # one case per line
  intents.jsonl             # retrieval intent dataset (below)
  judge-regressions.jsonl   # judge verdicts a human overruled
  runs/<runId>/
    run.json                # run config, the attribution pins
    events.jsonl            # append-only event lines
    artifacts/              # prediction CSVs, judge outputs, transcripts
```

The set directory lives in the SAME git repository as the model it evaluates,
so a checkpoint (a git commit) pins the model and the ledger together. Never
place `evals/` inside the directory tree the answerer's package serves: gold
in the served tree is a contamination path.

Rules that make the ledger trustworthy:

- `events.jsonl` is append-only. Never edit a line. To void one, append a new
  event whose payload marks the old one `voided`.
- A stage never rewrites another stage's fields.
- One artifact directory per attempt (`qid` plus `sample`). Never overwrite a
  previous attempt's prediction CSV.
- End-of-run numbers come from counting event lines (`jq` over
  `events.jsonl`), never from arithmetic recalled in prose.

## `set.json`

| Field | Notes |
|---|---|
| `name` | Set name; also the directory name. |
| `description` | |
| `datasetVersion` | Integer. Bump on any golden repair or case change. Runs record the version they scored against. |
| `targetModelPath` | Model path within the package. |

## `cases.jsonl`

One JSON object per line:

| Field | Notes |
|---|---|
| `qid` | Stable case id. |
| `question` | Exact text the answerer will see. |
| `split` | `dev` or `holdout`. Frozen at import. Diagnose and improve read `dev` only; the gate runs both. |
| `tags` | list |
| `state` | `candidate` / `selected` / `excluded`. |
| `source` | Where the case came from. |
| `golden` | `status` (`verified` / `provisional` / `invalid` / `ambiguous`), `kind`, `value` or `path` (artifact under the set directory), `canonicalQuery`, `verifiedBy`. |
| `goldenRevision` | Integer. Bump on every golden change; `score` events stamp the revision they compared against. |

## `intents.jsonl`

The retrieval intent dataset. Rows describe what a user was looking for, not
which entities match, so the dataset survives model renames and restructuring.

| Field | Notes |
|---|---|
| `intentId` | Stable id. |
| `term` | The search term as a user or agent would issue it. |
| `entityType` | `dimension` / `measure` / `view` / `dimensional_value`. |
| `description` | A few sentences of rich natural language: what the user meant. Specific enough to judge relevance, general enough to stay valid as the model evolves. |
| `valid` | Boolean. A one-time domain filter: is this a legitimate request in this domain at all? Invalid intents never enter coverage, recall, or precision. |

Per model version, each valid intent is additionally judged in-scope (the
model represents the concept) or out-of-scope (a coverage gap). That judgment
lives on the run's `retrieval_score` events, not here, because it changes as
the model changes.

## `run.json`

The attribution pins. Two runs are comparable only when these match where it
matters:

| Field | Notes |
|---|---|
| `runId` | Directory name. |
| `mode` | Steps or alias this run walks (see `skill:eval-loop`). |
| `setName` / `datasetVersion` | What was scored. |
| `modelGitSha` | Commit of the model repo the answerers ran against. Frozen for the run. |
| `serverVersion` | Version of whatever serves the model (Publisher build, platform release). |
| `judgeVersion` / `rubricSha` | From `reference/judge.md` and its git blob sha. |
| `answererModel` | |
| `traceMode` | `retrieval` for a scored run. |
| `callBudget` | Frozen; raising it mid-run moved mean outcomes on an unchanged model. |
| `status` | `running` / `complete` / `abandoned`. Updated in place; `run.json` is the one mutable file. |

## Events

Every line in `events.jsonl`: `{ "kind": ..., "caseId": ..., "at": ISO time,
"payload": {...} }`. `caseId` is the qid, or absent for run-level events.
`kind` is one of: `attempt`, `tool_call`, `score`, `retrieval_score`,
`issue`, `issue_status`, `candidate`, `gate`, `checkpoint`.

### `attempt`

| Field | Type | Notes |
|---|---|---|
| `qid` | string | |
| `sample` | int or null | Which repeat. Required even when null. |
| `phase` | string | `baseline` / `loop` / `blind_gate` / `canary` / `final`. `phase` lives here, on the attempt, not in run config. |
| `question_sha` | string | Hash of the exact text the answerer saw. |
| `submitted` | bool | False when there was no final query. Not a wrong answer. |
| `final_query` | string or null | Required to replay. |
| `servedRevision` | string or null | From the package actually queried. |
| `n_get_context` / `n_execute` / `n_execute_errors` | int | |
| `host_tool_uses` | int | Host-side count, including Read and Shell. |
| `reported_calls` | int | MCP calls the answerer claimed. |
| `contaminated` | bool or `"unknown"` | `"unknown"` when no host log exists. |
| `contamination_reasons` | list | Empty when clean. |
| `transcriptPath` | string | The answerer's transcript under `artifacts/`. |

### `tool_call`

One event per MCP `get_context` or `execute_query` the attempt made.

| Field | Type | Notes |
|---|---|---|
| `tool` | string | `get_context` or `execute_query`. |
| `traceId` | string or null | `get_context` only; look up in your host's trace store. |
| `rankedSummary` | object | Copied at capture from the trace so evidence survives trace eviction: `entityIds`, `ranks`, `resultCount`, and per-target `targets` with within-target ranks. |
| `error` | string or null | |

Never persist `execute_query` result rows, givens, or credentials.

### `score`

The answer judge's verdict for one attempt (protocol in
`reference/judge.md`). Every attempt in a scored run gets exactly one.

| Field | Type | Notes |
|---|---|---|
| `verdict` | string or null | `match` / `near_match` / `no_match` / `needs_human`; null when the attempt is not scorable. |
| `reason` | string | Why, from the judge; for a null verdict, why not scorable (`not_submitted`, `golden_missing`, `golden_ambiguous`, `contaminated`). |
| `confidence` | int or null | 1 to 10. Confidence of 5 or lower forces `needs_human`. |
| `column_pairing` | object or null | The judge's named gold-to-prediction column correspondence. |
| `judge_version` / `rubric_sha` | string | Pins which rubric produced this verdict. |
| `golden_revision` | int | From the case at score time. |
| `contaminated` | bool or `"unknown"` | Copied from the attempt; true or unknown means `verdict: null`. |
| `artifactPath` | string | The full judge output under `artifacts/`. |
| `gold_status` | string | `verified` / `verified_benign` / `suspect` / `verified_wrong`. `verified_wrong` excludes the case from run aggregates. |

A `submitted: false` attempt gets `verdict: null, reason: "not_submitted"`,
except for an `unanswerable` golden, where a refusal that names the gap is the
pass and a confident numeric answer is the fail.

Aggregates count confident verdicts only: `needs_human` and null verdicts are
neither passes nor failures and stay out of gate arithmetic.

### `retrieval_score`

One per intent judged in this run (run-level; no `caseId`).

| Field | Type | Notes |
|---|---|---|
| `intentId` / `term` / `entityType` | string | From `intents.jsonl`. |
| `in_scope` | bool | Does THIS model version represent the concept? False is a coverage gap, not a retrieval failure. |
| `judgments` | list | Per returned entity: `entityId`, `rank`, `level` (`match` / `near_match` / `no_match`), `confidence`, `why`. Empty when nothing returned. |
| `judge_version` / `rubric_sha` | string | |
| `traceId` | string or null | The `get_context` call judged. |

Run-level metrics fall out by counting:

- `coverage` = in-scope intents / valid intents.
- `recall` (on in-scope intents) = fraction whose judgments contain at least
  one `match` or `near_match`.
- `precision` = approved judgments (`match` or `near_match`) / all judgments.

### `issue`

| Field | Type | Notes |
|---|---|---|
| `issue_id` | string | Stable across status events. |
| `qids` | list | Affected cases. |
| `primary_code` / `contributing_codes` | string / list | From `skill:eval-diagnose`, verbatim. |
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
| `probes` | Query and result for each factual claim. |

### `gate`

Written by `eval-loop`, one per gate decision, BEFORE any checkpoint commit.

| Field | Notes |
|---|---|
| `issue_ids` | |
| `decision` | `accepted` / `rejected`. |
| `class` | `docs` / `definition` / `retrieval` / `justified`. |
| `baselineRunId` / `finalRunIds` | Plural: acceptance needs two independent runs. |
| `regressions` | Case ids whose verdict got worse vs baseline. Must be empty to accept. |
| `holdoutDelta` | Confident-verdict delta on the holdout slice. |
| `reason` | Including independent deterministic justification when that is the basis. |

### `checkpoint`

Written by `eval-loop` after an accepted gate, or when a restore runs. The
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
whenever `reference/judge.md` or the judge model changes; a rubric change that
flips old human-settled verdicts is a judge regression, not new truth.
