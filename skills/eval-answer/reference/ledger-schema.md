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
evals/<set>/                # the set, in the model's repository
  set.json                  # set metadata (below)
  eval.toml                 # servers and paths for eval.py (config.py)
  cases.jsonl               # one case per line
  judge-regressions.jsonl   # judge verdicts a human overruled

<workdir>/                  # [paths] workdir, default ~/.malloy-eval/<set>-<hash>/
  runs/<runId>/
    run.json                # run config, the attribution pins
    events.jsonl            # append-only event lines
    artifacts/              # prediction CSVs, judge outputs, transcripts
  packages/                 # built report packages
  servers/<role>/           # server roots for eval.py serve
```

The set directory lives in the SAME git repository as the model it evaluates,
so a checkpoint (a git commit) pins the model and the answer key together.
The workdir is never inside the model package: a run holds a `model.malloy`
snapshot and a report is a Malloy package, and either one nested there puts
the package into `loadErrors`. For a checkpoint to pin the ledger as well,
point the workdir at a directory in the repository outside the package, and
gitignore its `servers/` and `packages/`.

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
| `truthPackage` | Name of the package holding the semantics-free sources every golden is re-derived from -- a package NAME, not an object. Without it `verify_goldens.py` re-derives nothing: it reports "nothing to re-derive against" and **exits 3**, which a caller must read as unverified rather than as a pass. The checks that need no truth server -- rubric figures, verification axis, stale rubric claims, set names -- still run and still report. `init_truth_package.py` scaffolds the package. |
| `truthModel` | Model file inside that package. Default `truth.malloy`. |
| `targetPackage` | Name of the package UNDER TEST. `verify_goldens.py` refuses a "truth" server that also serves it, because an answerer on that server can retrieve the raw truth sources beside the model and quietly change what is under test. `run_baseline.py` and `improve.py` pass the package they were given, so this field is the fallback for a direct call; without either the guard cannot fire. |
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
| `golden` | `status` (`verified` / `provisional` / `invalid` / `ambiguous` / `verified_wrong`, the last being a key a person has established is wrong, which `run_baseline.py` drops from every aggregate), `kind`, `value` or `path` (a `scalar` value is an object naming the canonical query's column, `{"net_sales": 834216}`, never a bare number; a `rows` golden's `path` is relative to the set directory and names either a CSV with a header row or a JSON array of row objects, and `value` wins when both are present -- `gold/<qid>.json`, the derivation record below, is neither and is refused; a file with no header or no data rows is an error, so a key that really is no rows is written `value: []`), `canonicalQuery` (runs against the **truth** package, never the model under test), `verifiedBy`, and `verification` -- `{primaryAxis, variesAxis, note}` naming what the second derivation varied. **A golden is written only after two differently shaped derivations agree** (the truth query and a second one that varies a different axis -- group by something else, sum a different way, take a different route through the raw tables); `gold/<qid>.json` holds both (`truthRows`, `verifyRows`, `agreement`). Two derivations that vary the same axis share every other blind spot: that is how a golden that summed three overlapping time slices, 3x too high, passed its check. `verify_goldens.py` re-derives the value, reads the accepting clause of the rubric against the rows, and flags a second derivation that names no axis. **The question asks for data, never for an interpretation of it**, and either fixes the grain or the rubric accepts a correct figure at any stated grain (`skill:eval-judge` rules 10 and 11). |
| `golden.kind` | What the key IS. A value (`scalar`, rows), `unanswerable`, or `criteria`: prose with no value, where the author's criteria are the whole key. `criteria` is the one kind that is legitimately `verified` on arrival, because there is no derivation to perform and nothing that can be numerically wrong; an imported key holding a VALUE is `provisional` until re-derived (`skill:eval-import`). **`verify_goldens.py --promote` is the only thing that writes `golden.status`**: it marks a `provisional` golden `verified` when its value re-derived cleanly from the truth package AND a second derivation exists. `--refresh` rewrites a drifted VALUE and never changes a status; `invalid` and `ambiguous` are settled by a person through the golden side door. |
| `golden.rubric` | **On the golden, not on the case.** The prose the judge is shown as `CASE RUBRIC`, naming what counts as correct and what does not. `run_baseline.py` reads `golden.rubric`; a rubric written at the case's top level is silently not passed, the judge is told `CASE RUBRIC: none`, and it then scores from the golden value and the answer alone -- which reads as a judge that ignores its instructions. |
| `golden.mustState` | **On the golden, not on the case.** What the answer has to say out loud. Read from `golden.mustState` by the run-package builder. |
| `goldenRevision` | Integer. Bump on every golden change; `score` events stamp the revision they compared against. |
| `expectedEntities` | `required`: entity ids (`kind:source:name`) the answer cannot be produced without. `requiredAnyOf`: a list of **groups**, each a list of ids of which any one suffices -- for a case the model can answer through more than one route. Naming only one route scores the other as a retrieval miss and steers the fix to the docs for an entity whose docs were never the problem. `acceptable`: ids that are not noise if returned. An entity counts as delivered when returned under its exact id, under the same type and name on a sibling source, or by name in a returned source's documentation; `score_retrieval.py` records the route per entity as `delivery`. |

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
| `judgePromptSha` *(pin)* | Content sha of the prompt the judge is SHOWN, which is the harness's own template and not the skill file `rubricSha` covers. The template carries the golden rendering, the evidence blocks and the tie-breaking rules, so two runs whose prompts differed compared as the same judge -- the blind spot that let a golden-rendering defect run through three arms with every test passing, because the tests covered the code paths and not the text the model sees. Absent on runs written before it existed. |
| `datasetVersion` *(pin)* | From `set.json` at run time. |
| `modelSha` *(pin)* | Content sha of the `model.malloy` snapshot in the run directory -- the bytes the answerer actually queried. A git sha is not enough: a snapshot host serves a copy, often of a dirty tree no commit names. |
| `skillsVersion` *(pin)* | HEAD of the checkout the agents' skills were loaded from, dirty-marked (`ledger.skills_git_sha(root)`). The skills are the doctrine the agents load; a run that cannot name their version cannot anchor a skills A/B. |
| `skillsRoot` / `harnessVersion` | Which checkout supplied the doctrine (`--skills-root`, e.g. a Publisher checkout for the open-source skills; default this one), and this checkout's own HEAD. The eval-* skills always come from the harness checkout, whatever `skillsRoot` says. Two runs whose `skillsRoot` differ are a skills A/B only if their manifests name the same skills. |
| `diagnoserModel` / `improverModel` | Written by `diagnose.py` / `improve.py` when those stages run, so the run names every LLM that touched it. Absent on a run that was only answered and judged. |
| `modelGitSha` | Commit of the model repo, from `--model-repo`; `-dirty` suffix when `--model-dir` (or the whole repo, without it) had uncommitted changes -- fine for a band measurement, not for an A/B pin. Null when git cannot answer for the scope (a path outside the repo): absent beats wrong. **Null unless `--model-repo` names the repo.** It cannot be inferred: Publisher serves a COPY under `publisher_data/`, so the tree around the served file is the server's storage, not the model's history, and the field named that until 2026-09-03. `modelSha` above is the pin that always works; this is the pointer to where those bytes are versioned, and absent beats wrong. |
| `modelRepo` | The path `--model-repo` named, so a reader can tell an absent pin from an unrecorded one. |
| `modelDir` | The package directory `--model-dir` named, when it was: the path the `-dirty` marker on `modelGitSha` was decided over. Absent means the whole repo, where any unrelated untracked file marks the model dirty. |
| `environment` / `package` / `modelPath` | What was served, and from where. On a platform target `environment` is the organization and `package` is the workspace the MCP URL is scoped to. |
| `scope` | Platform target only: the `environment/package[@version]` the answerer was told to pass as an explicit `scopes` entry on every `get_context` / `execute_query` call. A workspace can serve many packages (a personal workspace serves every package the user can read), so without this the run also measures whether retrieval picks the right package -- a different measurement. Absent means unscoped. |
| `targetVersion` *(pin)* | Platform target only: the published version the answers came from. Both hosted tools default an omitted version to the PINNED one, meaning whatever the workspace serves at the moment of the call, so this only describes the run when `--scope` carries it to every call. `--target-version` supplies it when the scope omits it; a scope naming a different version is refused rather than treated as an override, because a run cannot say which build answered if two pins disagree. |
| `mcpUrl` / `publisher` | The endpoints the answerer and the re-execution used. |
| `evalConfig` | The set's `eval.toml` (or `eval.json`) the run read, as `{path, values}`; `path` is null when the set has none. |
| `predictionsReExecuted` | Whether re-execution was POSSIBLE: the server was serving the bytes this run is pinned to. It is one bool for the whole run and says nothing about any given query, so it reads stronger than it is; `reExecution` below is what actually happened. |
| `reExecution` | `{attempted, ok, failed, noQuery, notReExecuted, notJudged, missing}` over the run's predictions. `failed` means the query ran and the server returned an error, which is a different thing from an answer with no query at all. `notJudged` is a case the judge was never handed: a golden refused before judging (`provisional`, `invalid`, `ambiguous`, `verified_wrong`, missing), an attempt that submitted nothing, or every case under `--no-judge`. Those used to land in `missing` and read as corrupt artifacts on a freshly imported set. `ok`, `failed`, `noQuery`, `notReExecuted`, `notJudged` and `missing` partition the cases, so they sum to the case count; `attempted` is `ok + failed` and sits outside that sum. |
| `retrievalMode` | Which retriever answered: `semantic`, `lexical`, `mixed` (it changed mid-run, so nothing can be averaged across it), `unavailable` (at least one search was answered `indexing` or `error`, so it returned nothing to score and reads as a miss) or `unreported` (nothing ranked, which on a configured server means no embedding provider). Only a RANKING call names a retriever, so a call that enumerates a type or looks a name up is `unreported` on a fully semantic server and must not make the run read as `mixed`. Local retrieval degrades to lexical SILENTLY without an embedding key, and a pair compared across that reads as a model change, so `flip_table.py` refuses the pair unless both sides match. |
| `retrievalCalls` | `{semantic, lexical, indexing, error, unreported}` call counts behind `retrievalMode`. `unreported` counts the calls that did not rank, which is a different thing from calls that fell back. |
| `retrievalGate` | What the pre-arm retrieval gate found, or why it did not run. A run is held until `get_context` answers semantic twice in a row, because the first calls after a restart return `retrieval: "indexing"` with no sources and an arm started immediately measures empty results as well as real ones. A `retrieval: "error"` result stops the gate with its `retrieval_reason` instead of waiting. `not run (no answering phase)` under `--rebuild`/`--rejudge`, and `skipped by --no-retrieval-gate` when the operator opted out -- which is a different fact from a gate that passed, and the field is what tells them apart. |
| `coverageReport` | The `check_coverage.py --out` report this run read, when `--coverage` named one: its path, the model version it was stamped with, its `agentModel`, and `decided` of `cases`. Coverage is a read of the MODEL, so it is measured separately and costs a judge call per case; the run does not run it, it consumes it. Its per-case verdict then beats the authored `coverage` label in retrieval attribution, and each `score_retrieval` row records `coverage_source` (`measured`, `authored`, `none`) so nobody has to guess which one charged the failure. Absent when no report was given, which is a different fact from a report that found everything covered. |
| `label` | `<set>-<phase>-<nn>`, assigned by `run_baseline.py` from the set name, the run's phase and the next free number beside it (`ecommerce-baseline-01`, `ecommerce-baseline-02`, `ecommerce-blind_gate-01`). The A/A pair is two runs of the same phase; the post-edit arms are two runs of `blind_gate`. Hand-typed names do not survive one afternoon of runs -- `base`, `rejudged2`, `r3`, `post1` sort wrongly, group not at all, and cannot be matched to an arm. `--label` overrides for a run that genuinely needs a human name. |
| `effort` | |
| `maxTurns` / `answererTimeout` *(pins)* | The answerer's turn cap and per-attempt timeout, as the arm ran them. `skill:eval-loop` step 6 lists the call budget among the pins to freeze for a whole run, and nothing wrote one, so no two runs could be compared on the setting that decides whether a case got to finish at all. Distinct from `callBudget`, which no harness has ever written. Size the cap from a pilot rather than taking the default: on a 29-case arm at the default 30, completed attempts had a median of 17 turns and a 90th percentile of 26, and the four cases that ran past it were recorded as wrong answers. |
| `truncated` / `contaminated` | The qids excluded for a HARNESS-owned reason: the cap cut the answer off, or the attempt breached isolation. Both leave the denominator, both are named here rather than only in the console, and either one makes `status` `incomplete`. |
| `answererCostUsd` / `judgeCostUsd` | What the arm cost, split by role. The judge's half was discarded until 2026-09-02, so every "cost per arm" quoted before then was the answerer alone. Per-case judge cost is on each `score` event. |
| `answererCostCopiedFrom` | The run whose attempts `answererCostUsd` was COPIED from, when this run spawned no answerer -- a `--rejudge`, a `--rebuild`, a `--from`. Those copy the source's attempt events verbatim, `cost_usd` included, so the figure is right about the attempts and wrong as this run's spend: one re-judge reported $17.56 of answering that never happened. Null when this run paid, so a total over several run files can skip the copies. |
| `goldenCheck` | What `verify_goldens.py` said before the run started: `N ok, M drifted, K other finding(s)`, or why it did not run (no truth server on a platform target; `--skip-golden-check`; rebuild). A run that started on a drifted set says so here rather than pretending its verdicts mean something. `run_baseline` calls `verify()` without a model, so the set-name audit (check 5) does not run in-arm; the trailing `N stale entity name(s)` is the in-arm lint's own count, and `entity-name lint not run` says there was no model text to lint against. Without it the field read `0 other finding(s)` for a check that never happened. |
| `staleEntityNames` | The `expectedEntities` ids the compiled served model does not declare, from the in-arm lint (bare names from a search of the model text when the compiled model could not be read). Each one depresses the run's own recall on every case listing it, so a stale set reads as a retrieval failure until someone checks by hand. `null` when there was no model text to lint against (a platform target), which is a different fact from `[]`. |
| `status` | `complete`; `aborted` when four consecutive attempts errored or found the server dead and the harness stopped rather than spend the rest of the budget on attempts nobody will trust; or `incomplete` when the arm ran to the end and still cannot report a rate, because `truncated` or `contaminated` is non-empty. `incomplete` is a third state, not a soft `complete`: the arm did everything asked of it and the number is still unearned. It does NOT stop the run being compared: `flip_table.py` scores the cases both arms did, and names the ones this arm left unscored -- an excluded case carries no verdict, so it can never be counted as a flip. |
| `packageSha` / `servedRevision` *(pins)* | Taken from the server, not recomputed: `sourceContentSha` is a content hash over EVERY model path in the package AND the `skillSets` and `workflows` declaration and every file a served set or workflow reads, so an edit to an imported file or to a skill moves it where a sha of the one `--model-path` does not; `servedRevision` is minted per load, so it identifies a load rather than content and is a poor pin alone. Measured: `publisher.json`'s `version` moves neither, and nothing in Publisher reads it -- it is not in the Package API schema and never returned, so it pins nothing. |
| `packageSkillsMode` / `packageSkillsSha` *(pins)* | How the package's own skills reached the answerer (`install` copied them into its workspace, `tool` left it to fetch them with `get_skill`, `off` withheld them, which is also what a package that ships none is recorded as), and a content hash of the skills in the `default` workflow's skill sets, which are what `install` copies, so it is set in the `tool` arm too, but only when this machine holds the skills the server serves under the same names; it is null when they are not on this machine or do not match what the server serves. A run that spawns no answerer (`--rebuild`, `--rejudge`, `--from`) carries the prior run's values, and a platform run records null. Two runs can differ only in guidance; without these the ledger cannot tell them apart, and a flip table between them would look like noise. |
| `datasetSha` *(pin)* | Content hash of `set.json` + `cases.jsonl`. Deliberately SEPARATE from the model's sha: a golden repair is not a model change, and one pin covering both would make every answer-key fix read as an edit to the model, which is the distinction an A/B rests on. Automatic, so nobody has to remember it; `datasetVersion` stays beside it as the human-readable sequence. **Local targets only.** A hosted target that publishes IMMUTABLE versions needs none of this: the set rides inside the version, and immutability -- not hashing -- is what makes a pin trustworthy. There, `targetVersion` alone identifies model and set together. |
| `doubtedGoldens` | The cases whose golden is not believed: `qid`, `gold_status` (`suspect` or `verified_wrong`), `gold_note`, and `declaredBy` (`judge` or `set`). A status the SET declared is a key somebody already settled, not an opinion the judge formed this run, and attributing both to the judge claimed a judgement that never happened. **Read this before diagnose.** These are dataset issues, not model failures, and they go through the golden side door in `skill:eval-loop`. Empty list when no key is in doubt from either source. Written from the same scan that prints the end-of-run warning, because a warning that lives only in console text is one scrollback away from sending a modelling agent at a model that is already right. |
| `mode` / `setName` / `serverVersion` / `traceMode` / `callBudget` | Defined and accepted, **not yet written by any harness** -- kept in the schema for the platform target and the conductor, which need them. (`targetVersion` and `status` were in this list and are both written now.) |

The events, and `judge-regressions.jsonl`, are in `reference/ledger-schema-events.md`.
