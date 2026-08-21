---
name: eval-loop
description: 'Conduct a local Publisher evaluation loop in five steps: scrape/run, eval, diagnose, improve, checkpoint. You are the conductor: import cases into the file ledger, spawn a blind answerer, then run eval-answer, eval-diagnose, and eval-improve. Persistence is plain files under the model package''s evals/ directory; checkpoints are git commits of the model repo. Use to score a model, diagnose failures, improve behind a gate, or roll back a bad direction.'
---

# The Evaluation Loop

You conduct this loop. There is no batch orchestrator to start, no eval API,
and no eval MCP tools. The ledger is plain files in the model package's git
repository (`reference/ledger-schema.md` in `skill:eval-answer` defines every
file and event). Scoring is an LLM judge you spawn per case. There is no
scripted scorer: the one script in the tree checks contamination, which is the
one thing a judge cannot see.

```
scrape/run  ->  eval  ->  diagnose  ->  improve  ->  checkpoint
```

**This skill conducts; it does not restate.** Scoring lives in
`skill:eval-answer`. Components and owners live in `skill:eval-diagnose`.
Edit rules live in `skill:eval-improve`.

Do not merge **eval** into **diagnose**. A conductor who scores while
explaining writes the explanation into the score. Do not skip the **gate**
inside improve. The gate decides whether *this* edit stays. **Checkpoint**
decides whether a *sequence* of accepted edits can be undone.

## The five steps

| Step | Job | Writes |
|---|---|---|
| **a. scrape / run** | Put cases and intents in the ledger; spawn a blind answerer | cases, intents; `attempt` |
| **b. eval** | Judge the answer and retrieval | `score`, `retrieval_score` |
| **c. diagnose** | Why it failed, who owns it | `issue` / `issue_status`. Stop. Do not edit. |
| **d. improve** | One smallest model edit, then the gate | improve writes `candidate`; you write `gate`. Revert on reject. |
| **e. checkpoint** | Git commit after an accepted gate | `checkpoint` event, then the commit |

**scrape** and **run** share a letter but are not the same job. Scrape writes
cases and intents. Run writes attempts. Do not invent questions and score
them in one breath.

### Scrape, minimally

Importing an existing corpus IS the scrape step: copy the set from its home
(for example a benchmarks checkout) into `evals/<set>/` and convert to the
ledger shapes. While importing:

- Freeze each case's `split`: `dev` or `holdout`. Diagnose and improve read
  dev cases only; the gate runs both. A set that is all dev cannot defend an
  accept.
- Curate `intents.jsonl`: for each distinct entity-search intent the cases
  imply, write the term, the entity type, and a rich description of what the
  user meant. Descriptions, not entity ids, so the dataset survives model
  renames.
- Later, each diagnosed-and-fixed failure becomes a new frozen dev case, so a
  fixed bug cannot silently return.

Scraping from production logs (chat transcripts, retrieval traces) is the
other supported source, and usually the better one: real traffic asks what
people actually ask. `reference/log-scrape.md` is the adapter contract, and
it is where the archetypes, the session-stitching rules, and the constraints
that decide what a log-derived set can claim are written down. Where your
logs physically live is a host concern; look for a host-specific
log-fetching skill.

Prefer variety over volume when you sample, from either source. Cases that
differ in grain, source, filter shape, and phrasing are what move a
measurement; a second sample of the same case is nearly free of new
information.

### Mode aliases

Older mode names still work as aliases for how far one run walks:

| Alias | Steps |
|---|---|
| `measure` | scrape/run + eval |
| `triage` | plus diagnose |
| `improve` | plus improve + gate + checkpoint on accept |

Say which alias (or which steps) you are running before the first question.
Record it in `run.json`. Do not mix steps in a way that lets the answerer see
gold, issues, or the model file.

Most runs should stop after eval. Diagnose when you need a histogram of
components and owners. Improve only for diagnosed *model* gaps, one batch at
a time. Checkpoint only after the gate **accepts**.

## Roles

| Role | Sees |
|---|---|
| **Answerer** | The question and the Malloy tools. Never the golden, `evals/`, the model file, or any hint it is being evaluated. |
| **Judge** | The golden and the prediction. Never conducts, never answers, never edits. One fresh subagent per verdict (`reference/judge.md`). |
| **You (conductor / improver)** | Everything, including goldens and traces. |
| **Gate** | The edit and the evidence. Never the improver's self-assessment alone. |

The answerer stays blind. That is not optional. A grader-visible answerer
writes toward the expected answer, and the score is fiction.

There are no eval MCP tools on purpose. The answerer inherits your tools,
including Shell and Read, so any eval convenience surface would also be a
gold path for it. Blindness is prevention plus detection, not a guarantee:
`eval-answer` runs the contamination checklist on every attempt, which is
why you keep a host-side tool-use log per answerer.

## Before you start

1. The model package under evaluation must live in a git repository, with
   `evals/<set>/` beside the model files (same repo, never inside the served
   package tree). Git is the checkpoint mechanism; without it there is no
   rollback and no run can include improve.

2. The server must be up with retrieval tracing on, so a call's ranked results
   can be recovered afterwards (open-source Publisher: `PUBLISHER_MCP_TRACE=retrieval`).
   Confirm a trace lookup is available (absent means tracing is off).
   Refuse to start a scored run without it: failures without traces cannot be
   attributed.

3. Health-check: your host's status check until it reports serving, and inspect
   `loadErrors`. A dead database that still answers HTTP is an environment
   failure, not a model failure. Stop and fix it. Four consecutive
   environment or no-result attempts means stop the run.

4. Load the set: scrape/import as above, or reuse an existing `evals/<set>/`.
   Never keep two live copies of one set; the set directory in the model repo
   is the single source of truth, versioned by `datasetVersion` in
   `set.json`.

5. Review goldens before you score. A verified golden with no local artifact
   stays verified by provenance and is not scorable until you have rows or a
   scalar to compare (the judge needs both sides). If diagnosis later marks
   `BAD-REFERENCE`, follow **Repair a bad golden**. `AMBIGUOUS-REFERENCE`,
   follow **Hold an ambiguous golden**. Both are expected in the wild; both
   are the golden side door below, not improve, and not a sixth step.

6. Create `runs/<runId>/run.json` with the attribution pins
   (`reference/ledger-schema.md`): mode, dataset version, **model git sha**
   (commit or stash the model state first; answer from a dirty tree and the
   run pins nothing), Publisher version, judge version and rubric sha,
   answerer model, call budget, trace mode. Freeze those for the whole run.
   Raising a call budget mid-run moved mean outcomes on an unchanged model.

7. Generate every answerer prompt from the stored case in `cases.jsonl`.
   Never retype the question. A truncated retype is indistinguishable from a
   real question downstream.

## Per question

1. Health-check again.
2. Spawn a *fresh* blind subagent. Give it only the question text and the
   Malloy analysis tools. Tell it to follow `skill:malloy-analysis`. Do not
   mention eval, gold, scoring, or this skill.
3. Keep a host-side tool-use log for that subagent (name, input path or
   command, MCP tool name). Publisher traces see MCP only; a Read of a gold
   CSV is invisible server-side.
4. `skill:eval-answer`: contamination first, then re-execute, then the judge,
   then events.
5. `skill:eval-diagnose` only when this run includes diagnose, only on dev
   cases, and only after the score event exists.
6. `skill:eval-improve` only when this run includes improve, and only for
   `owner: model`. Then gate. On accept, checkpoint.

## Golden side door (not a sixth step)

Bad and ambiguous goldens show up immediately. That is not improve. A
checkpoint that mixes model edits and silent golden rewrites is useless for
rollback. Keep hold and repair here, outside the five steps.

### Repair a bad golden

This is **your** job as conductor, after `eval-diagnose` writes
`BAD-REFERENCE`. It is not the answerer's job, and it is not a reason to
change the model.

1. **Replay, yourself.** Take the stored `final_query` (or a query you can
   justify from the model) and run it with `execute_query`. Write the
   rows to a gold artifact under `evals/<set>/` (never under the served
   package tree). If you cannot produce a trusted key, follow **Hold an
   ambiguous golden** (or mark the golden `invalid` if the question itself is
   unusable). Do not invent a number.
2. **Patch the case** in `cases.jsonl`: new `golden` (status, kind, value or
   path, `canonicalQuery`, `verifiedBy: replay`) and `goldenRevision`
   incremented. Do not edit any old `score` event.
3. **Bump `datasetVersion`** in `set.json`, and commit the ledger change so
   the repair is attributable.
4. **Close the issue as repaired, not as a model fix**: `issue_status: fixed`
   with a note that the *golden* changed.
5. **Open a new run** whose `run.json` records the new `datasetVersion`. It
   is not comparable to runs on the old version without saying so.
6. **Re-score stored queries first**: `skill:eval-answer` without a new
   answerer (saved predictions, fresh judge, new `golden_revision` stamps).
7. **Re-answer only if you still need a blind look** (discoverability, or the
   stored query was itself the thing under test).

Never mix old-golden and new-golden scores in one aggregate. A before/after
that crosses a golden bump is a rebase, not a model delta.

### Hold an ambiguous golden

Use this when the current key is unusable as a score *and* you cannot justify
exactly one replacement (two honest replays disagree; a window or tie is
unspecified; later samples might confirm a convention).

1. **Do not invent a key.** Leave the old artifact on the case for
   provenance.
2. **Patch the case**: `golden.status: ambiguous` with a `reason` naming the
   defect and the competing replacements (not a new number). Increment
   `goldenRevision`.
3. **Do not score** this case until a later sample confirms a convention or a
   human picks a replacement. Its attempts get `verdict: null,
   reason: golden_ambiguous`.
4. **`issue_status: deferred`**, not `fixed`. Revisit when another case in
   the same neighborhood confirms a convention.
5. Old `score` events stay. They keep the previous `golden_revision` and must
   not enter an aggregate that claims the model failed.

If later evidence makes one replacement obvious, then Repair a bad golden.

## The gate (inside improve)

You own the gate. The improver does not accept its own edit.

Cheap and deterministic, every edit:

1. A compile check (scope `file` for an edit, `package` if importers must
   survive).
2. Save, then reload the package. Confirm it is not serving a stale model.
3. Replay stored final queries from previously-passing cases. They must still
   execute, and a judge must still call them a match.
4. A *fresh* blind re-answer of the affected question. The fix must be
   discoverable, not merely possible. The improver writing the query it
   already knows proves only that the edit exists.

Acceptance rules (replacing any vague "results improve"):

- **Per-case, not aggregate.** No previously-passing case may regress: diff
  the new run's verdicts against the baseline, case by case (`jq` over the
  two `events.jsonl` files). `regressions` on the gate event must be empty to
  accept, and the regressed qids go in the checkpoint commit message if you
  proceed anyway after a human call.
- **Confident verdicts only.** `needs_human` and null verdicts are neither
  passes nor failures; the delta is computed without them.
- **Both splits.** The gate runs the affected dev cases AND the holdout
  slice. Diagnose and improve never saw holdout; that is what makes its delta
  evidence rather than memorization.
- **Twice.** An improvement must survive a second independent run with fresh
  blind answerers before acceptance. A delta that appears once and vanishes
  on re-run was answerer or judge variance, not a fix.
- Documentation / discoverability edits may accept on a deterministic
  `get_context` probe now returning the entity, provided no replay
  regresses. Measure, join, or definition edits need the full rules above,
  including the flip-count bar in Measurement, which means enough affected
  cases to clear it.
- Independent deterministic justification (a probed-wrong definition
  corrected) may accept without a measured win. Record that as the gate
  `reason`.

Write the `gate` event (decision, class, baseline and final run ids,
regressions, holdout delta, reason) BEFORE any commit, so a rejected
direction leaves a record. On reject: revert the files (`git checkout --`
or `git restore`) and reload. On accept: `issue_status: fixed` for what the
edit actually closed, **then checkpoint**.

## Checkpoint

A checkpoint is a git commit of the model repository, taken after a gate
accepts, so a bad improve direction can be rolled back. It is not a report,
and it is not a remote publish.

1. Commit the model files AND the set's ledger in one commit; put the label
   and the closed issue ids in the message.
2. Append the `checkpoint` event (`action: created`, label, `modelGitSha`
   from the commit you just made, issueIds). The event line itself rides in
   the next commit; append-only logs trail by one commit and that is fine.
3. Confirm `git status` is clean for the model files.

**Restore**: `git checkout <sha> -- <model files>` (or `git revert` the
checkpoint commits), then reload the package, then append a `checkpoint`
event with `action: restored` and the sha. Readers return to the model that
existed before the bad direction.

Take a checkpoint of the current model *before* the first improve batch if no
commit pins it yet. Rolling back by hand is guesswork.

If reload reports `mode: reinstalled`, the package was re-fetched from its
install location and may have overwritten the restored files. Prefer in-place
/ watch-mounted packages for this loop.

## Measurement

**Sample each case once. Spend the budget on more and more varied cases
instead.** Repeats past the first buy very little: variance decompositions of
LLM evaluation put the reduction from extra repeats at a small fraction of
what extra items buy, and a set of five cases run three times cannot support
a claim that fifteen distinct cases can. If a case is genuinely borderline,
re-run that case, not the whole set.

Because a single sample cannot carry a mean, do not report before/after as a
score delta. **Count the cases whose verdict changed** between the baseline
and the post-edit run, discard the unchanged ones, and read the result off
this table:

| Cases that got worse | Cases that must get better to accept |
|---|---|
| 0 | 5 |
| 1 | 7 |
| 2 | 9 |
| 3 | 10 |

Below that bar the change is **unresolved**, not an improvement, and saying
so is the honest report. Note the consequence before you scope a run: a set
of six cases can essentially never clear this bar, so a set that small can
measure a baseline and diagnose failures but cannot defend an edit.

Do not quote a set-level coverage, recall, or precision before the intents
have been judged for THIS model version.

Retrieval metrics come from `retrieval_score` events (coverage, recall,
precision by counting; definitions in `reference/ledger-schema.md`). They are
the signal for docs and discoverability edits; they do not replace the answer
verdict, and they are not enough by themselves to accept a join or definition
change.

If the contaminated fraction of attempts exceeds 0.1 (or any contamination,
on a run smaller than 10), the run is a harness failure. Do not publish a
model score.

## Out of scope

This loop is local. The ledger is files, the checkpoints are git, you are the
conductor. Do not:

- publish the model to remote Credible as a "true" checkpoint or learning
  curve
- start a Python orchestrator (`loop.py`, `improve_batch.py`); the only
  script in the tree is the contamination check
- score by string-diffing rows instead of judging them, or reintroduce a
  scripted row oracle: one that can pass a wrong answer is worse than none
- wait for a bigger gold set before the loop can run; dev/holdout on what
  exists beats waiting
- register eval MCP tools or stand up an eval API
- encode unsettled goldens into the model

## Prime directives

- The model is the only thing improve edits. No question text, qids, or
  expected values in any name, doc, or comment.
- When the environment misbehaves, stop. Never diagnose a sick system.
- When a subagent disagrees with you, probe. Do not win by authority.
- When a rule here is wrong, change this file and note it on the run.

## Related skills

- `skill:eval-answer`: contamination, judge protocol, events. Its
  `reference/ledger-schema.md` is the file contract; `reference/judge.md` is
  the judge.
- `skill:eval-diagnose`: component, owner, issue events. No edit.
- `skill:eval-improve`: smallest model edit, probe receipts, no self-accept.
- `skill:malloy-analysis`: what the blind answerer follows.
