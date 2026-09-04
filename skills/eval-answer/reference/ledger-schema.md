# The eval ledger: files and events

The ledger is plain files in the model package's git repository. There is no
eval API and no eval database. The conductor (`skill:eval-loop`) reads and
writes these files directly; the stages share them as their contract.
`eval-answer` writes `attempt`, `tool_call`, and `score`. An engine-side
`eval-retrieval` skill, which does not ship here, writes `retrieval_score` and
`probe`; both stay in this contract so one validator covers every run
directory.
`eval-diagnose` writes `issue` and `issue_status`. `eval-improve` writes
`candidate`. `eval-loop` writes `acceptance_check`, further `issue_status`, and
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
| `truthPackage` | Name of the package holding the semantics-free sources every golden is re-derived from -- a package NAME, not an object. `verify_goldens.py` skips every check without it, reporting "nothing to re-derive against", so a set that omits it silently has no golden verification at all. `init_truth_package.py` scaffolds the package. |
| `truthModel` | Model file inside that package. Default `truth.malloy`. |
| `truthTableRewrite` | Boolean, default false. Rewrites `duckdb.table('data/x.parquet')` refs to bare `x` in canonical queries, for a truth package whose tables are registered rather than read from files. |

## `cases.jsonl`

One JSON object per line:

| Field | Notes |
|---|---|
| `qid` | Stable case id. |
| `question` | Exact text the answerer will see. |
| `split` | `dev` or `holdout`. Frozen at import. Diagnose and improve read `dev` only; the acceptance check runs both. |
| `tags` | list |
| `state` | `candidate` / `selected` / `excluded`. |
| `source` | Where the case came from. |
| `golden` | `status` (`verified` / `provisional` / `invalid` / `ambiguous`), `kind`, `value` or `path` (artifact under the set directory), `canonicalQuery` (runs against the **truth** package, never the model under test), `verifiedBy`, and `verification` -- `{primaryAxis, variesAxis, note}` naming what the second derivation varied. **A golden is written only after two differently shaped derivations agree** (the truth query and a second one that varies a different axis -- group by something else, sum a different way, take a different route through the raw tables); `gold/<qid>.json` holds both (`truthRows`, `verifyRows`, `agreement`). Two derivations that vary the same axis share every other blind spot: that is how a golden that summed three overlapping time slices, 3x too high, passed its check. `verify_goldens.py` re-derives the value, reads the accepting clause of the rubric against the rows, and flags a second derivation that names no axis. **The question asks for data, never for an interpretation of it**, and either fixes the grain or the rubric accepts a correct figure at any stated grain (`skill:eval-judge` rules 10 and 11). |
| `golden.rubric` | **On the golden, not on the case.** The prose the judge is shown as `CASE RUBRIC`, naming what counts as correct and what does not. `run_baseline.py` reads `golden.rubric`; a rubric written at the case's top level is silently not passed, the judge is told `CASE RUBRIC: none`, and it then scores from the golden value and the answer alone -- which reads as a judge that ignores its instructions. |
| `golden.mustState` | **On the golden, not on the case.** What the answer has to say out loud. Read from `golden.mustState` by the run-package builder. |
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
| `judgeModel` / `judgeVersion` / `rubricSha` *(pins)* | The judge's model, and the version + content sha of `skill:eval-judge`. |
| `datasetVersion` *(pin)* | From `set.json` at run time. |
| `modelSha` *(pin)* | Content sha of the `model.malloy` snapshot in the run directory -- the bytes the answerer actually queried. A git sha is not enough: a snapshot host serves a copy, often of a dirty tree no commit names. |
| `skillsVersion` *(pin)* | HEAD of the checkout the agents' skills were loaded from, dirty-marked (`ledger.skills_git_sha(root)`). The skills are the doctrine the agents load; a run that cannot name their version cannot anchor a skills A/B. |
| `skillsRoot` / `harnessVersion` | Which checkout supplied the doctrine (`--skills-root`, e.g. a Publisher checkout for the open-source skills; default this one), and this checkout's own HEAD. The eval-* skills always come from the harness checkout, whatever `skillsRoot` says. Two runs whose `skillsRoot` differ are a skills A/B only if their manifests name the same skills. |
| `diagnoserModel` / `improverModel` | Written by `diagnose.py` / `improve.py` when those stages run, so the run names every LLM that touched it. Absent on a run that was only answered and judged. |
| `modelGitSha` | Commit of the model repo, from `--model-repo`; `-dirty` suffix when that path had uncommitted changes -- fine for a band measurement, not for an A/B pin. **Null unless `--model-repo` names the repo.** It cannot be inferred: Publisher serves a COPY under `publisher_data/`, so the tree around the served file is the server's storage, not the model's history, and the field named that until 2026-09-03. `modelSha` above is the pin that always works; this is the pointer to where those bytes are versioned, and absent beats wrong. |
| `modelRepo` | The path `--model-repo` named, so a reader can tell an absent pin from an unrecorded one. |
| `environment` / `package` / `modelPath` | What was served, and from where. On a platform target `environment` is the organization and `package` is the workspace the MCP URL is scoped to. |
| `scope` | Platform target only: the `environment/package` the answerer was told to pass as an explicit `scopes` entry on every `get_context` / `execute_query` call. A workspace can serve many packages (a personal workspace serves every package the user can read), so without this the run also measures whether retrieval picks the right package -- a different measurement. Absent means unscoped. |
| `mcpUrl` / `publisher` | The endpoints the answerer and the re-execution used. |
| `predictionsReExecuted` | Whether re-execution was POSSIBLE: the server was serving the bytes this run is pinned to. It is one bool for the whole run and says nothing about any given query, so it reads stronger than it is; `reExecution` below is what actually happened. |
| `reExecution` | `{attempted, ok, failed, noQuery}` over the run's predictions. `failed` means the query ran and the server returned an error, which is a different thing from an answer with no query at all. |
| `retrievalMode` | Which retriever answered: `semantic`, `lexical`, `mixed` (it changed mid-run, so nothing can be averaged across it) or `unreported` (the server named none, meaning no embedding provider). Local retrieval degrades to lexical SILENTLY without an embedding key, and a pair compared across that reads as a model change, so `flip_table.py` refuses the pair unless both sides match. |
| `retrievalCalls` | `{semantic, lexical, unreported}` call counts behind `retrievalMode`. |
| `label` | `<set>-<phase>-<nn>`, assigned by `run_baseline.py` from the set name, the run's phase and the next free number beside it (`ecommerce-baseline-01`, `ecommerce-baseline-02`, `ecommerce-blind_gate-01`). The A/A pair is two runs of the same phase; the post-edit arms are two runs of `blind_gate`. Hand-typed names do not survive one afternoon of runs -- `base`, `rejudged2`, `r3`, `post1` sort wrongly, group not at all, and cannot be matched to an arm. `--label` overrides for a run that genuinely needs a human name. |
| `effort` | |
| `answererCostUsd` / `judgeCostUsd` | What the arm cost, split by role. The judge's half was discarded until 2026-09-02, so every "cost per arm" quoted before then was the answerer alone. |
| `goldenCheck` | What `verify_goldens.py` said before the run started: `N ok, M drifted, K other finding(s)`, or why it did not run (no truth server on a platform target; `--skip-golden-check`; rebuild). A run that started on a drifted set says so here rather than pretending its verdicts mean something. |
| `status` | `complete`, or `aborted` when four consecutive attempts errored or found the server dead and the harness stopped rather than spend the rest of the budget on attempts nobody will trust. |
| `packageSha` / `servedRevision` *(pins)* | Taken from the server, not recomputed: `sourceContentSha` is a content hash over EVERY model path in the package, so an edit to an imported file moves it where a sha of the one `--model-path` does not; `servedRevision` is minted per load, so it identifies a load rather than content and is a poor pin alone. Measured: `publisher.json`'s `version` moves neither, and nothing in Publisher reads it -- it is not in the Package API schema and never returned, so it pins nothing. |
| `datasetSha` *(pin)* | Content hash of `set.json` + `cases.jsonl`. Deliberately SEPARATE from the model's sha: a golden repair is not a model change, and one pin covering both would make every answer-key fix read as an edit to the model, which is the distinction an A/B rests on. Automatic, so nobody has to remember it; `datasetVersion` stays beside it as the human-readable sequence. **Local targets only.** A hosted target that publishes IMMUTABLE versions needs none of this: the set rides inside the version, and immutability -- not hashing -- is what makes a pin trustworthy. There, `targetVersion` alone identifies model and set together. |
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
`candidate`, `acceptance_check`, `checkpoint`.

### `attempt`

| Field | Type | Notes |
|---|---|---|
| `qid` | string | |
| `sample` | int or null | Which repeat. Required even when null. |
| `phase` | string | `baseline` / `loop` / `blind_gate` / `canary` / `final`. `phase` lives here, on the attempt, not in run config. |
| `question_sha` | string | Hash of the exact text the answerer saw. |
| `submitted` | bool | False when there was no final query. Not a wrong answer. |
| `final_query` | string or null | Required to replay. A named view is recorded as the Malloy it stands for, `run: <source> -> <view>`, so every consumer sees one shape and the query re-executes. |
| `final_query_source` | string or null | How `final_query` was chosen: `declared` (the answer printed it), `last_ok` (the last call the server answered) or `last`. `last` is a warning: a trailing sanity probe may be standing in for the answer's own query. |
| `servedRevision` | string or null | From the package actually queried. |
| `n_get_context` / `n_execute` / `n_execute_errors` | int | |
| `host_tool_uses` | int | EVERY tool use the host logged, MCP calls included. It counted only the non-MCP ones until 2026-09-03, which made the under-report check below true of almost every clean attempt. |
| `mcp_tool_uses` | int or null | The MCP subset: `get_context` plus `execute_query`. Null on a run written before the split. |
| `reported_calls` | int | MCP calls the answerer claimed. `reported_calls > host_tool_uses` is the under-report floor from `skill:eval-answer`: the answerer cannot have made more calls than the host logged. `validate_run` warns on it, and only for runs that carry `mcp_tool_uses`, because the field meant something narrower before. |
| `contaminated` | bool or `"unknown"` | `"unknown"` when no host log exists. Read it with `ledger.is_contaminated`, never for truthiness: runs written before 2026-09-03 carry the strings `"true"`/`"false"`, and `bool("false")` is True. `ledger.event` now rejects those strings on write; `validate_run` grandfathers them on read as a warning. |
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
| `retrieval_mode` | string or null | `get_context` only: the `retrieval` field of the response that answered, `semantic` or `lexical`. Null when the server named none, which means no embedding provider. Recorded per call because the semantic path can fall over partway through a run. |
| `query` | string or null | `execute_query` only: the Malloy the call sent, so the final query can be chosen by which call the server actually answered. |
| `modelPath` | string or null | `execute_query` only: the model file the call named. A source does not resolve outside the file that declares it, so re-execution needs this. |
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
`skill:eval-judge`). Every attempt in a scored run gets exactly one.

| Field | Type | Notes |
|---|---|---|
| `verdict` | string or null | `match` / `near_match` / `no_match` / `needs_human`; null when the attempt is not scorable. Only `match` and `no_match` are decisions; see below. |
| `reason` | string | Why, from the judge; for a null verdict, why not scorable (`not_submitted`, `golden_missing`, `golden_ambiguous`, `contaminated`). |
| `confidence` | int or null | 1 to 10. Confidence of 5 or lower forces `needs_human`. |
| `column_pairing` | object or null | The judge's named gold-to-prediction column correspondence. |
| `judge_version` / `rubric_sha` | string | Pins which rubric produced this verdict. |
| `golden_revision` | int | From the case at score time. |
| `contaminated` | bool or `"unknown"` | Copied from the attempt; true or unknown means `verdict: null`. |
| `must_not_use_hits` | list or null | Entries of `golden.mustNotUse` a script found in the final query. A hit forces `verdict: no_match`. Decided by `check_must_not_use.py`, never by the judge: it is a question about query text. Prose entries and a path's bare leaf are not vetoed; they go to the judge instead. |
| `judge_verdict` | string or null | What the judge said when a `must_not_use_hits` veto overrode it. Null otherwise, so the judge's own agreement rate stays measurable across vetoes. |
| `artifactPath` | string | The full judge output under `artifacts/`. |
| `gold_status` | string | `verified` / `verified_benign` / `suspect` / `verified_wrong`. **From the judge**, which scored against the golden as written and reports separately whether it believes it; falls back to the case's standing `golden.status` when the judge does not say. `verified_wrong` excludes the case from run aggregates. `suspect` and `verified_wrong` route to the golden side door as `dataset` issues, never to improve. |
| `gold_note` | string or null | The judge's evidence for a non-`verified` status: the two values, or the model line against the rubric sentence. Null when `verified`. |

`not_submitted` means the attempt produced NOTHING to judge: no answer text and
no query. An attempt that wrote prose without querying **is judged**, and
against a golden that holds a value an answer containing none of it is
`no_match` (`skill:eval-judge`, `reference/refusal.md` rule 10). Excusing those
as unscorable dropped a confident refusal on an answerable case out of the pass
rate, which is the one thing the answerable-sounds-unanswerable cases exist to
measure. A refusal is exempt only where `golden.kind` is `unanswerable`, and
there naming the gap is the pass and a confident number is the fail.

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
| `class` | `docs` / `definition` / `retrieval` / `justified`. |
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
