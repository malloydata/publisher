---
name: eval-loop
description: 'Conduct a local Publisher evaluation loop in five steps: scrape/run, eval, diagnose, improve, checkpoint. You are the conductor: import cases, spawn a blind answerer, then run eval-answer, eval-diagnose, and eval-improve. Persist through REST /api/v0/evals. Python is only match_rows.py, score_retrieval.py, and check_contamination.py. Use to score a model, diagnose failures, improve behind a gate, or roll back a bad direction.'
---

# The Evaluation Loop

You conduct this loop. There is no batch orchestrator to start, and no eval MCP tools.
Publisher stores sets, cases, runs, events, and checkpoints. You read and write them over REST.
Scoring arithmetic and contamination detection are the only scripts you run.

```
scrape/run  ->  eval  ->  diagnose  ->  improve  ->  checkpoint
```

**This skill conducts; it does not restate.** Scoring lives in `skill:eval-answer`.
Components and owners live in `skill:eval-diagnose`. Edit rules live in `skill:eval-improve`.

Do not merge **eval** into **diagnose**. Scoring by eye is how scores become fiction.
Do not skip the **gate** inside improve. The gate decides whether *this* edit stays.
**Checkpoint** decides whether a *sequence* of accepted edits can be undone.

## The five steps

| Step | Job | Writes |
|---|---|---|
| **a. scrape / run** | Put questions in the store; spawn a blind answerer | cases; `attempt` |
| **b. eval** | Mechanical score vs gold | `score` (and `retrieval_score` when `neededEntities` exists) |
| **c. diagnose** | Why it failed, who owns it | `issue` / `issue_status`. Stop. Do not edit. |
| **d. improve** | One smallest model edit, then the gate | `candidate` + `gate`. Revert on reject. |
| **e. checkpoint** | Named restore point after an accepted gate | checkpoint row + `checkpoint` event |

**scrape** and **run** share a letter but are not the same job. Scrape writes cases.
Run writes attempts. Do not invent questions and score them in one breath.

### Mode aliases

Older mode names still work as aliases for how far one run walks:

| Alias | Steps |
|---|---|
| `measure` | scrape/run + eval |
| `triage` | plus diagnose |
| `improve` | plus improve + gate + checkpoint on accept |

Say which alias (or which steps) you are running before the first question.
Record it on the run. Do not mix steps in a way that lets the answerer see gold,
issues, or the model file.

Most runs should stop after eval. Diagnose when you need a histogram of
components and owners. Improve only for diagnosed *model* gaps, one batch at a
time. Checkpoint only after the gate **accepts**.

## Roles

| Role | Sees |
|---|---|
| **Answerer** | The question and the Malloy tools. Never the golden, the eval store, the model file, `evals/`, or any hint it is being evaluated. |
| **You (conductor / grader / improver)** | Everything, including goldens and traces. |
| **Gate** | The edit and the evidence. Never the improver's self-assessment alone. |

The answerer stays blind. That is not optional. A grader-visible answerer writes toward
the expected answer, and the score is fiction.

There are no eval MCP tools on purpose. The answerer inherits your tools, including
Shell and Read, so REST convenience for you would also be a gold path for it.
Blindness is prevention plus detection, not a guarantee. `eval-answer` runs the
contamination check on every attempt.

## Before you start

1. Publisher must be up with the eval store and retrieval tracing on:

   `PUBLISHER_EVAL_STORE=on PUBLISHER_MCP_TRACE=retrieval`

   Refuse to start a scored run if either is off. Check by calling `GET /api/v0/evals/sets`
   (404 means the store is off) and confirming `malloy_getTrace` is registered
   (absent means tracing is not `retrieval`). Failures without traces cannot be attributed.

2. Health-check: `malloy_getStatus` (or `GET /api/v0/status`) until `serving`, and
   inspect `loadErrors`. A dead database that still answers HTTP is an environment
   failure, not a model failure. Stop and fix it. Four consecutive environment or
   no-result attempts means stop the run.

3. Load cases into the store. Either import a snapshot
   (`POST /api/v0/evals/import` with `directory` or `snapshot`) or create a set and
   cases by hand. Package `evals/<set>/eval.json` + `cases.jsonl` is a portable
   snapshot, not the live workspace. Do not auto-sync.
   `GET /evals/sets` returns **active** sets only. `?name=synthetic` is the
   live row with that name. `?status=all` includes archived. A second
   import of a live name is 409: rebase in place (`PATCH` goldens, bump
   `metadata.version`) or `PATCH` the old row `{ "status": "archived" }`
   first. There is no delete-set; `POST /evals/reset` wipes every set and
   every checkpoint.
   Snapshot export is optional handoff, not how you retire a generation.

4. Review goldens before you score. A verified golden with no local artifact stays
   verified by provenance and is not scorable until you have rows or a scalar to
   compare. `score` is `null` when the golden is missing, provisional, invalid,
   or ambiguous, or when a verified golden has no local artifact.
   If diagnosis later marks `BAD-REFERENCE`, follow **Repair a bad golden**.
   If it marks `AMBIGUOUS-REFERENCE`, follow **Hold an ambiguous golden**.
   Both are expected in the wild. That work is the golden side door below, not
   improve, and not a sixth step of the loop.

5. Create a run (`POST /api/v0/evals/runs`) whose `config` names: steps or alias,
   environment, package, model path, Publisher and repo SHAs if you have them,
   answerer model, call budget, trace mode, value-index mode, and skill bundle
   identity. Freeze those for the whole run. Raising a call budget mid-run moved
   mean score by 0.30 on an unchanged model.

6. Generate every answerer prompt from the stored case (`GET /api/v0/evals/cases/:id`).
   Never retype the question. A truncated retype is indistinguishable from a real
   question downstream.

## Per question

1. Health-check again.
2. Spawn a *fresh* blind subagent. Give it only the question text and the Malloy
   analysis tools. Tell it to follow `skill:malloy-analysis`. Do not mention eval,
   gold, scoring, or this skill.
3. Keep a host-side tool-use log for that subagent (name, input path or command,
   MCP tool name). Publisher traces see MCP only. Read of a gold CSV is invisible
   server-side.
4. `skill:eval-answer`: contamination first, then mechanical score, then events.
5. `skill:eval-diagnose` only when this run includes diagnose, and only after the
   score event exists.
6. `skill:eval-improve` only when this run includes improve, and only for
   `owner: model`. Then gate. On accept, checkpoint.

A benchmarks helper may import Beaver cases. It is not the product. If you have
a short list, write the cases yourself through REST.

## Persistence

Base URL: `http://127.0.0.1:4000/api/v0` (or the REST port this server bound).

| Action | Request |
|---|---|
| List / create sets | `GET` / `POST /evals/sets` (`GET` is active-only; `?name=` / `?status=all`) |
| Cases | `GET` / `POST /evals/sets/:setId/cases`, `PATCH /evals/cases/:id` |
| Import / export snapshot | `POST /evals/import`, `POST /evals/sets/:id/export` |
| Runs | `POST /evals/runs`, `PATCH /evals/runs/:id` |
| Events | `POST /evals/runs/:id/events`, `GET /evals/runs/:id/events` |
| Checkpoints | `GET` / `POST /evals/checkpoints`, `GET /evals/checkpoints/:id`, `POST /evals/checkpoints/:id/restore` |

Event `kind` values: `attempt`, `tool_call`, `score`, `issue`, `issue_status`,
`candidate`, `gate`, `checkpoint`. Schema is `skill:eval-answer`
`reference/ledger-schema.md`. Issue status is the latest `issue_status` event.
Do not invent a status column.

`--init` preserves eval tables, including checkpoints. Only `POST /evals/reset`
wipes them.

Export snapshots for review or handoff. Do not export `evals/` into the package the
answerer queries; gold in that tree is a contamination path.

## Golden side door (not a sixth step)

Bad and ambiguous goldens show up immediately. That is not improve.
A checkpoint that mixes model edits and silent golden rewrites is useless for
rollback. Keep hold and repair here, outside the five steps.

### Repair a bad golden

This is **your** job as conductor, after `eval-diagnose` writes `BAD-REFERENCE`.
It is not the answerer's job, and it is not a reason to change the model.
Wrong goldens show up in real sets. Handle them as a versioned rebase.

1. **Replay, yourself.** Take the stored `final_query` (or a query you can
   justify from the model) and run it with `malloy_executeQuery`. Write the
   rows to a gold artifact that is *not* inside the package the answerer
   queries (the eval snapshot directory is fine; `publisher_data/` is not).
   If you cannot produce a trusted key, follow **Hold an ambiguous golden**
   (or `invalid` if the question itself is unusable). Do not invent a number.
2. **Patch the case.** `PATCH /evals/cases/:id` with the new `golden`
   (status, kind, value or path, `canonicalQuery`, `verifiedBy: replay`).
   The store bumps `golden_revision` on that write. Do not edit an old
   `score` event.
3. **Bump the set version.** `PATCH /evals/sets/:id` so `metadata.version`
   is `previous + 1`. On the next snapshot export, write the same integer
   into `eval.json` `version`. `draftRevision` also ticks; that is internal.
   The integer humans compare is `version`.
4. **Close the issue as repaired, not as a model fix.**
   `issue_status: fixed` with a note that the *golden* changed.
5. **Open a new run** (`POST /evals/runs`) whose `config` records
   `setVersion` and the new `golden_revision`s. Put `phase: baseline` on
   that run. It is not comparable to scores stamped with the old revision
   without saying so.
6. **Re-score stored queries first.** For each affected case, run
   `scripts/match_rows.py` on the saved prediction (or re-execute the stored
   `final_query`) against the new gold. Write new `score` events on the
   *new* run. This is `skill:eval-answer` without a new answerer.
7. **Re-answer only if you still need a blind look** (discoverability, or
   the stored query was itself the thing under test). Fresh subagent, question
   only, same as any other run.

Never mix old-golden and new-golden scores in one mean. A before/after that
crosses a golden bump is a rebase, not a model delta.

### Hold an ambiguous golden

Use this when the current key is unusable as a score *and* you cannot justify
exactly one replacement (two honest replays disagree; a window or tie is
unspecified; later samples might confirm a convention).

Goldens are usually assumptions we want the model to encode. They are also
sometimes wrong. `ambiguous` is the holding state for "we do not know which
yet." It is not a model edit, and it is not a silent rewrite of old scores.

1. **Do not invent a key.** Leave the old artifact on the case for provenance.
2. **`PATCH` the case** `golden.status: ambiguous` with a `reason` that names
   the defect and the competing replacements (not a new number). The store
   bumps `golden_revision`.
3. **Do not score** this case until a later sample confirms a convention or
   a human picks a replacement. `score` is `null` while status is `ambiguous`.
4. **`issue_status: deferred`**, not `fixed`. Revisit when another case in
   the same neighborhood (same tables, same filter, same family split)
   confirms a convention, or the same fanout, or the same literal.
5. Old `score` events stay. They keep the previous `golden_revision` and
   must not enter a mean that claims the model failed.

If later evidence makes one replacement obvious, then Repair a bad golden
(replay, patch, bump `metadata.version`, new run).

## The gate (inside improve)

You own the gate. The improver does not accept its own edit.

Cheap and deterministic, every edit:

1. `malloy_compile` (scope `file` for an edit, `package` if importers must survive).
2. Save, then `malloy_reloadPackage`. Confirm the package is not `stale`.
3. Replay stored final queries from previously-passing cases. They must still
   execute and still match (`scripts/match_rows.py`).
4. A *fresh* blind re-answer of the affected question. The fix must be discoverable,
   not merely possible. The improver writing the query it already knows proves only
   that the edit exists.

Acceptance by change class (not "the score went up"):

- Documentation / discoverability: a deterministic `get_context` probe now returns
  the entity, and no previously-passing replay breaks. `n_samples=1` is enough.
  Record `retrieval_score.mrr` when a needed set exists; do not accept or reject
  on the MRR number alone.
- Measure, join, or definition: affected-case pass count strictly up at
  `n_samples>=3`, and no previously-passing replay or canary fails.
- Independent deterministic justification (a probed-wrong definition corrected)
  may accept without a measured win. Record that on the `gate` event.

On reject: revert the files and reload. Do not checkpoint a rejected edit.
On accept: write `candidate` and `gate` events linked to the baseline run, then
`issue_status: fixed` for what the edit actually closed, **then checkpoint**.

## Checkpoint

A checkpoint is a local restore point so a bad improve direction can be rolled
back. It is not a report, and it is not a remote publish.

After a gate **accepts**, `POST /evals/checkpoints` with:

- `label`
- `environmentName`, `packageName`, `modelPath`
- `runId` that accepted the edit
- `servedRevision` / `sourceContentSha` from the package that just reloaded
- `issueIds` the edit closed
- omit `files` to snapshot the on-disk model under `publisher_data/`; or send
  `{path, content}` yourself

Publisher stores the exact model bytes (and a copy under `.eval_checkpoints/`).
`--init` keeps the DuckDB row; restore still has the bytes.

**Restore** (`POST /evals/checkpoints/:id/restore`) copies those bytes back
onto the package, calls `malloy_reloadPackage`, and writes a `checkpoint`
event with `action: restored`. Readers return to the model that existed
before the bad direction.

Take a checkpoint of the current model *before* a new improve batch if no
checkpoint exists yet. Rolling back by hand is guesswork.

If reload reports `mode: reinstalled`, the package was re-fetched from its
install location and may have overwritten the restored files. Prefer
in-place / watch-mounted packages for this loop.

## Measurement

A single pass is a look, not a claim. If you will report a before/after delta,
use `n_samples>=3` on the compared cases and say so. Do not gate an edit on one
binary flip: the same model and question has scored 1.0 / 1.0 / 0.0.
Do not quote a set-level coverage percentage before `n_samples>=3`.

You may count retrieval in eval when a needed-entity list is already on
the case (`golden.neededEntities`) or from a prior diagnose: run
`scripts/score_retrieval.py` and record `retrieval_score.context_recall` and
`retrieval_score.mrr`. Those are mechanical counts, not a diagnosis.
Do not invent the needed set from the question, and do not assign component
codes during eval. `mrr` is the rank signal for a docs/discoverability edit;
it does not replace answer `f1`, and it is not enough by itself to accept a
join or definition change.

If the contaminated fraction of attempts exceeds 0.1 (or any contamination, on a
run smaller than 10), the run is a harness failure. Do not publish a model score.

## Out of scope

This loop is local. Publisher is the store and the three scripts. You are the
conductor. Do not:

- publish the model to remote Credible as a "true" checkpoint or learning curve
- start a Python orchestrator (`loop.py`, `improve_batch.py`); Python is only
  `match_rows.py`, `score_retrieval.py`, and `check_contamination.py`
- wait for a holdout gold set before the loop can run
- score with an LLM judge
- auto-sync package `evals/` with DuckDB
- register eval MCP tools
- encode unsettled goldens into the model

Package snapshot import/export can stay as a later convenience. It is not
required for the loop.

## Prime directives

- The model is the only thing improve edits. No question text, qids, or expected
  values in any name, doc, or comment.
- When the environment misbehaves, stop. Never diagnose a sick system.
- When a subagent disagrees with you, probe. Do not win by authority.
- When a rule here is wrong, change this file and note it on the run.

## Related skills

- `skill:eval-answer`: contamination, mechanical score, events.
- `skill:eval-diagnose`: component, owner, issue events. No edit.
- `skill:eval-improve`: smallest model edit, probe receipts, no self-accept.
- `skill:malloy-analysis`: what the blind answerer follows.
