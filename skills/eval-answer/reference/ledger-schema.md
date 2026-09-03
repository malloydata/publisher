# The eval ledger: files and events

The ledger is plain files in the model package's git repository. There is no
eval API and no eval database. The conductor (`skill:eval-loop`) reads and
writes these files directly; the stages share them as their contract.
`eval-answer` writes `attempt`, `tool_call`, and `score`. An engine-side
`eval-retrieval` skill, which does not ship here, writes `retrieval_score` and
`probe`; both stay in this contract so one validator covers every run
directory.
`eval-diagnose` writes `issue` and `issue_status`. `eval-improve` writes
`candidate`. `eval-loop` writes `gate`, further `issue_status`, and
`checkpoint`.

**The contract is code: `eval-answer/scripts/ledger.py`.** Every script that
writes `run.json` or `events.jsonl` builds its lines through that module, so a
field rename that reaches only one writer fails at write time. This document
is the human-readable rendering; where the two disagree, the module is right
and this file is the bug. Check any run directory with:

```
python3 skills/eval-answer/scripts/ledger.py validate <runDir>
```

Errors are broken identity (missing `run.json`, a missing required field, two
scores for one attempt); warnings are missing comparison pins (a run without
`skillsVersion` still measures, but cannot anchor an A/B on that pin) and
grandfathered unknown fields on old runs.

## Layout

```
evals/<set>/
  set.json                  # set metadata (below)
  cases.jsonl               # one case per line
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
  event whose payload marks the old one `voided`. One sanctioned exception: a
  re-runnable stage (diagnose, improve) replaces ITS OWN prior events, keyed by
  issue id, through `ledger.replace_events()` -- so a crash-and-rerun does not
  duplicate issues. A stage never touches another stage's lines.
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
| `golden` | `status` (`verified` / `provisional` / `invalid` / `ambiguous`), `kind`, `value` or `path` (artifact under the set directory), `canonicalQuery` (runs against the **truth** package, never the model under test), `verifiedBy`, and `verification` -- `{primaryAxis, variesAxis, note}` naming what the second derivation varied. **A golden is written only after two differently shaped derivations agree** (the truth query and a second one that varies a different axis -- group by something else, sum a different way, take a different route through the raw tables); `gold/<qid>.json` holds both (`truthRows`, `verifyRows`, `agreement`). Two derivations that vary the same axis share every other blind spot: that is how a golden that summed three overlapping time slices, 3x too high, passed its check. `verify_goldens.py` re-derives the value, reads the accepting clause of the rubric against the rows, and flags a second derivation that names no axis. **The question asks for data, never for an interpretation of it**, and either fixes the grain or the rubric accepts a correct figure at any stated grain (`judge.md` rules 9 and 10). |
| `goldenRevision` | Integer. Bump on every golden change; `score` events stamp the revision they compared against. |
| `expectedEntities` | `required`: entity ids (`kind:source:name`) the answer cannot be produced without. `requiredAnyOf`: a list of **groups**, each a list of ids of which any one suffices -- for a case the model can answer through more than one route. Naming only one route scores the other as a retrieval miss and steers diagnosis to "retrieval ranking" for a failure that was never retrieval's. `acceptable`: ids that are not noise if returned. An entity counts as delivered when returned under its exact id, under the same type and name on a sibling source, or by name in a returned source's documentation; `score_retrieval.py` records the route per entity as `delivery`. |

## Term files (`eval-retrieval` only)

A customer set has no term file. The engine-side `eval-retrieval` skill keeps
`intents.jsonl` fixtures -- search term, entity type, a description of what the
searcher meant, a validity flag -- beside its own scripts, for evaluating the
engine against fixed inputs. Their shape and the `retrieval_score` events they
produce are documented there and validated here.

## `run.json`

The attribution pins. Two runs are comparable only when these match where it
matters. Two runs with different `target` values are comparable on answer
verdicts only if the same model version reached both, which is rarely worth
assuming:

Required fields are the run's **identity** -- every run ever written carries
them. The rest are optional in the schema, but the **comparison pins** are
warned about when absent, because a run missing one cannot take part in an A/B
on that axis.

| Field | Notes |
|---|---|
| `runId` *(required)* | Directory name. |
| `target` *(required)* | Which server answered, and how it reached the data: `local`, `local-proxied`, or `platform`. Decides what an improve step is even allowed to do (see `skill:eval-loop`). |
| `answererModel` *(required)* | |
| `phase` / `started` *(required)* | |
| `judgeModel` / `judgeVersion` / `rubricSha` *(pins)* | The judge's model, and the version + content sha of `reference/judge.md`. |
| `datasetVersion` *(pin)* | From `set.json` at run time. |
| `modelSha` *(pin)* | Content sha of the `model.malloy` snapshot in the run directory -- the bytes the answerer actually queried. A git sha is not enough: a snapshot host serves a copy, often of a dirty tree no commit names. |
| `skillsVersion` *(pin)* | HEAD of the checkout the agents' skills were loaded from, dirty-marked (`ledger.skills_git_sha(root)`). The skills are the doctrine the agents load; a run that cannot name their version cannot anchor a skills A/B. |
| `skillsRoot` / `harnessVersion` | Which checkout supplied the doctrine (`--skills-root`, e.g. a Publisher checkout for the open-source skills; default this one), and this checkout's own HEAD. The eval-* skills always come from the harness checkout, whatever `skillsRoot` says. Two runs whose `skillsRoot` differ are a skills A/B only if their manifests name the same skills. |
| `diagnoserModel` / `improverModel` | Written by `diagnose.py` / `improve.py` when those stages run, so the run names every LLM that touched it. Absent on a run that was only answered and judged. |
| `modelGitSha` | Commit of the model repo, when the target served working files; `-dirty` suffix when the tree had uncommitted changes -- fine for a band measurement, not for an A/B pin. Omit for a platform target, where a local commit pins nothing. |
| `environment` / `package` / `modelPath` | What was served, and from where. On a platform target `environment` is the organization and `package` is the workspace the MCP URL is scoped to. |
| `scope` | Platform target only: the `environment/package` the answerer was told to pass as an explicit `scopes` entry on every `get_context` / `execute_query` call. A workspace can serve many packages (a personal workspace serves every package the user can read), so without this the run also measures whether retrieval picks the right package -- a different measurement. Absent means unscoped. |
| `mcpUrl` / `publisher` | The endpoints the answerer and the re-execution used. |
| `predictionsReExecuted` | Whether the judge saw re-executed rows (false when the served bytes no longer match the pinned snapshot). |
| `label` / `effort` | |
| `answererCostUsd` / `judgeCostUsd` | What the arm cost, split by role. The judge's half was discarded until 2026-09-02, so every "cost per arm" quoted before then was the answerer alone. |
| `goldenCheck` | What `verify_goldens.py` said before the run started: `N ok, M drifted, K other finding(s)`, or why it did not run (no truth server on a platform target; `--skip-golden-check`; rebuild). A run that started on a drifted set says so here rather than pretending its verdicts mean something. |
| `status` | `complete`, or `aborted` when four consecutive attempts errored or found the server dead and the harness stopped rather than spend the rest of the budget on attempts nobody will trust. |
| `doubtedGoldens` | The cases whose golden the judge did not believe: `qid`, `gold_status` (`suspect` or `verified_wrong`), `gold_note`. **Read this before diagnose.** These are dataset issues, not model failures, and they go through the golden side door in `skill:eval-loop`. Empty list when the judge believed every key. Written from the same scan that prints the end-of-run warning, because a warning that lives only in console text is one scrollback away from sending a modelling agent at a model that is already right. |
| `mode` / `setName` / `targetVersion` / `serverVersion` / `traceMode` / `callBudget` / `status` | Defined and accepted, **not yet written by any harness** -- kept in the schema for the platform target and the conductor, which need them. |

## Events

Every line in `events.jsonl` is **flat**: `{ "kind": ..., <fields> }`, one
JSON object per line, with an optional `at` ISO timestamp. Case-scoped kinds
(`attempt`, `tool_call`, `score`) carry `qid`, `sample`, `phase` on the line;
run-level kinds do not. (An earlier draft of this document nested fields under
a `payload` with a `caseId`; no writer ever did that, and 23 run directories
exist in the flat shape, so the flat shape is the contract.) `kind` is one of:
`attempt`, `tool_call`, `score`, `retrieval_score`, `issue`, `issue_status`,
`candidate`, `gate`, `checkpoint`.

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
| `input_tokens` / `output_tokens` / `cache_read_tokens` | int or null | Answerer token usage. Null when the host does not report it. |
| `cost_usd` | float or null | Answerer cost for this attempt. |
| `num_turns` / `wall_seconds` | int, float or null | |
| `answer_text` | string or null | The answer the judge scored. Kept so a verdict can be re-read without the transcript. |
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
| `rankedSummary` | object | Copied at capture from the trace so evidence survives trace eviction: `entityIds`, `ranks`, `resultCount`, and per-target `targets` with within-target ranks. |
| `error` | string or null | |

Never persist `execute_query` result rows, givens, or credentials.

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
`reference/judge.md`). Every attempt in a scored run gets exactly one.

| Field | Type | Notes |
|---|---|---|
| `verdict` | string or null | `match` / `near_match` / `no_match` / `needs_human`; null when the attempt is not scorable. Only `match` and `no_match` are decisions; see below. |
| `reason` | string | Why, from the judge; for a null verdict, why not scorable (`not_submitted`, `golden_missing`, `golden_ambiguous`, `contaminated`). |
| `confidence` | int or null | 1 to 10. Confidence of 5 or lower forces `needs_human`. |
| `column_pairing` | object or null | The judge's named gold-to-prediction column correspondence. |
| `judge_version` / `rubric_sha` | string | Pins which rubric produced this verdict. |
| `golden_revision` | int | From the case at score time. |
| `contaminated` | bool or `"unknown"` | Copied from the attempt; true or unknown means `verdict: null`. |
| `artifactPath` | string | The full judge output under `artifacts/`. |
| `gold_status` | string | `verified` / `verified_benign` / `suspect` / `verified_wrong`. **From the judge**, which scored against the golden as written and reports separately whether it believes it; falls back to the case's standing `golden.status` when the judge does not say. `verified_wrong` excludes the case from run aggregates. `suspect` and `verified_wrong` route to the golden side door as `dataset` issues, never to improve. |
| `gold_note` | string or null | The judge's evidence for a non-`verified` status: the two values, or the model line against the rubric sentence. Null when `verified`. |

A `submitted: false` attempt gets `verdict: null, reason: "not_submitted"`,
except for an `unanswerable` golden, where a refusal that names the gap is the
pass and a confident numeric answer is the fail.

Aggregates count decided verdicts only. `match` and `no_match` are the pass and
the fail; **`near_match`, `needs_human` and null are none of the above** and stay
out of gate arithmetic. `near_match` is excluded because it means "defensibly
different", and an arguable verdict that moves a pass rate is a measurement
artefact rather than a result (`reference/judge.md` rule 7). Report its count:
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
| `meaningChanged` | Entities whose *meaning* the edit changed; `[]` for a docs-only edit. |
| `goldenSuspect` | Each `{qid, entity, stored, rederived}`: a golden this edit may have invalidated. Reported by the improver, never repaired by it. **Non-empty halts the gate** until adjudicated through the golden side door. |
| `goldenAudit` | The set's `verify_goldens.py` run against the edited model: `{ran, clean, model, tail}`. Catches drift and rubric-vs-model contradictions only; a golden whose value silently moved is invisible to it, which is why `goldenSuspect` exists alongside. |

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
