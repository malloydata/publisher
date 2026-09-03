---
name: eval-loop
description: 'Conduct a local Publisher evaluation loop in five steps: scrape/run, eval, diagnose, improve, checkpoint. You are the conductor: import cases into the file ledger, spawn a blind answerer, then run eval-answer, eval-diagnose, and eval-improve. Persistence is plain files under the model package''s evals/ directory; checkpoints are git commits of the model repo. Use to score a model, diagnose failures, improve behind an acceptance check, or roll back a bad direction.'
---

# The Evaluation Loop

You conduct this loop. There is no batch orchestrator to start, no eval API,
and no eval MCP tools. The ledger is plain files in the model package's git
repository (`reference/ledger-schema.md` in `skill:eval-answer` defines every
file and event). Scoring is an LLM judge you spawn per case. There is no
scripted scorer, and there will not be one: a script that can pass a wrong
answer is worse than none. The scripts under `scripts/` run the loop -- they
answer, re-execute, spawn the judge, compare runs, and write the ledger -- but
none of them decides whether an answer was right.

```
scrape/run  ->  eval  ->  diagnose  ->  improve  ->  checkpoint
```

**This skill conducts; it does not restate.** Scoring lives in
`skill:eval-answer`. Components and owners live in `skill:eval-diagnose`.
Edit rules live in `skill:eval-improve`.

Do not merge **eval** into **diagnose**. A conductor who scores while
explaining writes the explanation into the score. Do not skip the **acceptance
check** inside improve. The acceptance check decides whether *this* edit
stays. **Checkpoint** decides whether a *sequence* of accepted edits can be
undone.

## The five steps

| Step | Job | Writes |
|---|---|---|
| **a. scrape / run** | Put cases in the ledger; spawn a blind answerer | cases; `attempt`, `tool_call` |
| **b. eval** | Judge the answer; score which required entities retrieval delivered | `score` |
| **c. diagnose** | Why it failed, who owns it | `issue` / `issue_status`. Stop. Do not edit. |
| **d. improve** | One smallest model edit, then the acceptance check | improve writes `candidate`; you write `acceptance_check`. Revert on reject. |
| **e. checkpoint** | Git commit after an accepted acceptance check | `checkpoint` event, then the commit |

**scrape** and **run** share a letter but are not the same job. Scrape writes
cases. Run writes attempts. Do not invent questions and score
them in one breath.

### Scrape, minimally

Importing an existing corpus IS the scrape step: copy the set from its home
(for example a benchmarks checkout) into `evals/<set>/` and convert to the
ledger shapes. While importing:

- Freeze each case's `split`: `dev` or `holdout`. Diagnose and improve read
  dev cases only; the acceptance check runs both. A set that is all dev cannot defend an
  accept.
- Later, each diagnosed-and-fixed failure becomes a new frozen dev case, so a
  fixed bug cannot silently return.

Scraping from production logs (chat transcripts, retrieval traces) is the
other supported source, and usually the better one: real traffic asks what
people actually ask. Where your logs physically live is a host concern; look
for a host-specific log-fetching skill.

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
| `improve` | plus improve + acceptance check + checkpoint on accept |

Say which alias (or which steps) you are running before the first question.
Record it in `run.json`. Do not mix steps in a way that lets the answerer see
gold, issues, or the model file.

Most runs should stop after eval. Diagnose when you need a histogram of
components and owners. Improve only for diagnosed *model* gaps, one batch at
a time. Checkpoint only after the acceptance check **accepts**.

## Roles

| Role | Sees |
|---|---|
| **Answerer** | The question and the Malloy tools. Never the golden, `evals/`, the model file, or any hint it is being evaluated. |
| **Judge** | The golden and the prediction. Never conducts, never answers, never edits. One fresh subagent per verdict (`skill:eval-judge`). |
| **You (conductor / improver)** | Everything, including goldens and traces. |
| **Acceptance check** | The edit and the evidence. Never the improver's self-assessment alone. |

The answerer stays blind. That is not optional. A grader-visible answerer
writes toward the expected answer, and the score is fiction.

There are no eval MCP tools on purpose. The answerer inherits your tools,
including Shell and Read, so any eval convenience surface would also be a
gold path for it. Blindness is prevention plus detection, not a guarantee:
`eval-answer` runs the contamination checklist on every attempt, which is
why you keep a host-side tool-use log per answerer.

## Pick the target first

Both a local model server and a hosted platform expose the same two tools the
answerer needs, `get_context` and `execute_query`, so the loop runs against
either. What differs is which model is answering and whose data it reads, and
those are two separate axes:

| Target | Model under test | Data | Can edit and re-test? |
|---|---|---|---|
| **Local (direct)** | your working files | local (for example duckdb), or a direct warehouse connection | yes |
| **Local (proxied)** | your working files | the platform's connection, through a proxy connection type | yes |
| **Remote** | the published version, through the platform's hosted `get_context`/`execute_query` | the platform's | no, publishing is not an eval action |

The middle row is the one worth knowing about: it decouples the two axes, so you
can evaluate a model you are still editing against the customer's real data. It
is a connection configuration, not a feature.

Two rules follow, and both are the kind of mistake that produces confident
nonsense rather than an error:

- **The answerer and the conductor must hit the same target.** If the answerer
  queries the published model and you re-execute its query against your edited
  local copy, the score describes neither. Decide the target before the first
  question and record it.
- **Pin the version the target actually served, not the one you happen to have.**
  A local target pins a commit; a platform target pins the published version.
  Recording a local commit for a run that queried a published model is a pin
  that means nothing.

Which target for which job:

- **Baseline what customers experience:** Remote. It is the deployed model
  through the deployed engine, which is the thing they actually hit. The judge
  sees no re-executed rows on a Remote run (there is no local copy of the
  bytes), so its verdicts rest on the answer text and the golden; say so.
- **Improve and accept:** local, because the acceptance check needs compile,
  reload, and a fresh re-answer between edits. Publishing to a customer
  environment to score an edit is not something this loop does. Where the host
  offers draft execution, that counts as local for this purpose.
- **Measure real data without touching production:** local proxied.

So a measure-only run can use any target; a run that includes **improve** needs
a local one.

## Before you start

1. The model package under evaluation must live in a git repository, with
   `evals/<set>/` in the package, beside the model files. Git is the checkpoint
   mechanism; without it there is no rollback and no run can include improve.

   Keeping the set IN the package is what stops a model edit and its answer key
   drifting apart: they move in one commit, so fixing a measure and forgetting
   the golden that depended on it stops being possible. It is safe -- measured
   on a running server, a `cases.jsonl` inside a package appears in no model
   listing, no notebook listing, no package resource, and 404s over HTTP, so an
   MCP-only answerer has no route to it. What it does NOT get you is free
   versioning: `sourceContentSha` hashes model paths only, so the set needs its
   own `datasetSha`.

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
   (`reference/ledger-schema.md`): mode, dataset version, **the target and the
   version it served** (a local target pins a commit, so commit or stash first;
   answering from a dirty tree pins nothing), server version, judge version and
   rubric sha, answerer model, call budget, trace mode. Freeze those for the
   whole run. Raising a call budget mid-run moved mean outcomes on an unchanged
   model.

7. Generate every answerer prompt from the stored case in `cases.jsonl`.
   Never retype the question. A truncated retype is indistinguishable from a
   real question downstream.

## Running one, concretely

`scripts/run_baseline.py` does steps 3 and 7 and the whole of **Per question**:
one fresh answerer per case with only the Publisher MCP tools, a contamination
check, a judge, and a conformant `events.jsonl`.

```bash
# 1. serve the model under test -- in its own session, so the shell's exit
#    cannot take it down, and returning only once it answers a query
python3 skills/eval-loop/scripts/serve.py --publisher-dir <publisher>/packages/server \
  --server-root <root> --port 4811 --mcp-port 4040 --trace-retrieval \
  [--allow-proxy]   # required for a `publisher`-type (proxied) connection
#    a second server for the TRUTH package, on other ports, that the answerer
#    has no route to:
python3 skills/eval-loop/scripts/serve.py --publisher-dir <publisher>/packages/server \
  --server-root <truthroot> --port 4881 --mcp-port 4882 [--allow-proxy]

# 2. smoke one case first ($0.13), then the arm. Goldens are re-derived from
#    the truth server before either starts; a drifted set refuses to run.
python3 skills/eval-loop/scripts/run_baseline.py \
  --set <repo>/evals/ecommerce --out results/smoke --only <qid> --no-judge \
  --truth-publisher http://localhost:4881
python3 skills/eval-loop/scripts/run_baseline.py \
  --set <repo>/evals/ecommerce --out results/<arm> \
  --parallel 4 --truth-publisher http://localhost:4881
#    the run names itself <set>-<phase>-<nn> (ecommerce-baseline-01, then -02
#    for the second arm of the A/A). Pass --label only for a run that needs a
#    human name; hand-typed arm names stop being readable within an afternoon.

# 3. compare two arms, or two runs of one arm
python3 skills/eval-loop/scripts/flip_table.py --a results/<a> --b results/<b>

# 4. FIRST: any golden the judge did not believe. `jq .doubtedGoldens
#    results/<arm>/run.json` -- non-empty means settle those through the golden
#    side door before diagnosing, or you send a modelling agent at a model that
#    is already right.
python3 skills/eval-diagnose/scripts/diagnose.py \
  --run results/<arm> --set <repo>/evals/ecommerce --model-dir <package>
#    (cluster_failures.py gives a free mechanical first look, as
#     clusters-mechanical.jsonl; it groups by retrieval outcome and is not a
#     diagnosis)

# 5. build the browsable package
python3 skills/eval-loop/scripts/build_run_package.py \
  --run results/<a> --run results/<b> --set <repo>/evals/ecommerce --out <pkg>
```

Order of magnitude for planning, **calibrated on ecommerce over local duckdb**:
a Sonnet arm over a few dozen cases costs single-digit dollars and finishes in
minutes, at roughly a dime and a handful of turns per case. A proxied warehouse
is a different regime: the VideoAmp set ran at $0.33 per case on Sonnet and
$0.57–0.71 on Opus, ~100 s per case, driven by warehouse latency and query
errors -- budget 4x when the data is not local. Budget **five** such arms for a
defensible claim -- a baseline, two for the A/A, and two post-edit -- plus the
diagnose and improve agents, which are far cheaper per case but use a larger
model. Measured per-arm figures for a given set belong in that set's
`CALIBRATION.md`.

`--rebuild` re-derives the ledger from saved transcripts without calling a model,
and `--rebuild --rejudge` re-scores existing answers in place. `--from <run>
--out <new>` does the same into a NEW run directory -- the answers copied, the
judge fresh, the old verdicts untouched -- which is what a golden repair or a
rubric change calls for. Use them after a scoring or schema change; re-running
the answerers would confound the change you are measuring with fresh answerer
variance.

The scripts import each other by path (`ledger`, `mcp_payload`,
`score_retrieval` live in `eval-answer/scripts`; the loop scripts insert that
path). Run them **in place** from the skills checkout; a copy patched elsewhere
chases `ModuleNotFoundError` three times.

Two failure modes worth pre-empting, because both produce a clean-looking run:

- **Pre-approve the tools.** A headless answerer that has to ask permission for
  `malloy_getContext` stalls until the timeout and lands as a harness error.
- **Check the served revision is the one you edited.** Publisher serves a
  snapshot copy, so a model fix can be absent from the run that is supposed to
  measure it. Query the changed measure once before spending an arm on it.

## Per question

1. Health-check again.
2. Spawn a *fresh* blind subagent. Give it only the question text and the
   Malloy analysis tools. Tell it to follow the `malloy-analysis` skill. Do not
   mention eval, gold, scoring, or this skill.
3. Keep a host-side tool-use log for that subagent (name, input path or
   command, MCP tool name). Publisher traces see MCP only; a Read of a gold
   CSV is invisible server-side.
4. `skill:eval-answer`: contamination first, then re-execute, then the judge,
   then events.
5. `skill:eval-diagnose` only when this run includes diagnose, only on dev
   cases, and only after the score event exists.
6. `skill:eval-improve` only when this run includes improve, and only for
   `owner: model`. Then run the acceptance check. On accept, checkpoint.

## Golden side door (not a sixth step)

Bad and ambiguous goldens show up immediately. That is not improve. A
checkpoint that mixes model edits and silent golden rewrites is useless for
rollback. Keep hold and repair here, outside the five steps.

### Nothing else checks the judge

The A/A band measures whether the judge is *repeatable*. It says nothing about
whether it is *right* -- a judge answering `no_match` every time posts a perfect
band. Those come apart in practice, and when they do the loop keeps running and
every number it emits is wrong in the same direction.

So keep a small file of frozen predictions pinned to verdicts a human settled,
and re-run them after any edit to the judge prompt, a rubric, or what the judge
is given (`scripts/check_judge.py`, `judge-regressions.jsonl` in the set). Seed it
from the cases an A/A pair disagreed on: those are the contested ones, so they
are where a change will show first.

Two things about its shape:

- **The unit is a prediction, not a question.** One question earns different
  verdicts for different answers, legitimately. Key the fixture on the answer.
- **Judge through the same code path a run uses.** A reimplementation inside the
  checker can pass while the thing it stands for is broken.

A fixture that has never failed is not yet known to be a test. Break a rubric on
purpose once and confirm the right entry fails.

When a fixture fails, rule out judge nondeterminism (`--repeat`) before you
believe it. Then either you moved a verdict you did not mean to, or the fixture
was wrong -- re-settle it and record why. Deleting it throws away the only case
you had evidence about.

### Repair a bad golden

This is **your** job as conductor, after `eval-diagnose` writes
`BAD-REFERENCE`. It is not the answerer's job, and it is not a reason to
change the model.

Diagnosis is not the only way one arrives. The judge also reports a
`gold_status` on every score (`skill:eval-judge`), and a
`suspect` or `verified_wrong` comes through this same door -- earlier, because it
lands during scoring rather than after. Treat it as a `BAD-REFERENCE` with the
judge's `gold_note` as its evidence. Adjudicate it **before** improve runs: a
doubted key sends a modelling agent to fix a model that is already right, which
is the most expensive wrong turn this loop can take.

The judge scored against the golden as written even where it said `suspect`, so
its verdict is still the verdict. Do not re-open a case merely because the flag
is set; open it because you looked and agreed.

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

### A model fix can invalidate a golden, and nothing will tell you

A rubric that explains a trap usually has to quote the model -- "this measure
counts line items despite its name, so it yields the trap value". That sentence
is a claim about the model, and it is false the moment the model is fixed. The
judge keeps enforcing it and starts failing correct answers.

A truth-package check cannot catch this, structurally. It re-derives values from
sources that are independent of the model **on purpose**, so a rubric can
describe a model that no longer exists while every value still re-derives green.

Worse, a model fix can move a golden's *value* without touching the data. If a
dimension is defined in terms of the measure you fixed, the concept it names now
resolves to a different population -- same dimension, same question, different
correct answer -- while a canonical truth query still returns the old number
because it encoded the old definition.

So: **after any model edit, re-read the rubrics of every case that names an
entity you touched.** `verify_goldens.py` audits the mechanical part -- it parses
`X is <expr>` out of the model and flags any rubric asserting a different
definition -- but only for definitions it can parse. Prose claims about grain,
population, or convention are still yours to check.

When one turns up it is `BAD-REFERENCE`, and it goes through this side door.
Never let it reach improve: the model is right, and an edit would be damage.

### A golden must match the state the model is in

A case whose golden holds a value asserts that the value is obtainable. If the
model has no trace of the concept, that assertion is false, and the case is now
asking two questions at once: "did the answer contain the golden" (no) and
"should the answerer have complied" (no). Both readings are defensible, so the
verdict stops being a measurement.

Measured, holding the answer, the model and the rubric fixed and varying only
how the case was authored:

| the case says | verdicts over four samples |
|---|---|
| golden holds three counts, model defines no such concept | `match` / `no_match` / `match` / `near_match` |
| `golden.kind: unanswerable`, pass is a refusal that names what is missing | `match` x4 |

The judge is not being unreliable in the first row. It is being asked a question
with two right answers.

So a coverage case has two states and needs a golden for each:

1. **Before the model defines the concept.** `coverage: absent`,
   `golden.kind: unanswerable`. The pass is a refusal that NAMES what is
   missing; inventing boundaries and reporting them as the company's is
   `no_match`. This is the state that measures whether the model documents its
   conventions.
2. **After improve adds it.** Bump `goldenRevision`, replace the golden with the
   real value, bump `datasetVersion`. A refusal is now a failure, and the run
   measures whether the new entity is discoverable.

Never one case straddling both. The straddle is what produces an oscillating
verdict, and no amount of rubric wording fixes it -- four prompt edits were
tried against exactly this case and none of them did.

**This is the mirror of "A model fix can invalidate a golden".** That section
warns that adding a definition can move a golden nobody was working on. This one
warns of the same seam from the other side: a golden written for a model that
does not exist yet is invalid until the model catches up. Both are golden side
door work, and neither is improve.

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

## The acceptance check (inside improve)

You own the acceptance check. The improver does not accept its own edit.

**Before any of it: is the answer key still valid?** An edit that changed what
an entity means can have moved goldens for cases nobody was working on, and a
rerun against a stale key measures nothing -- it reads as a win or a regression
with equal confidence and neither is real. `skill:eval-improve` Step 4 reports
these as `golden_suspect` on the candidate; the judge reports its own doubts as
`gold_status`. **Any unadjudicated one halts the acceptance check.** Settle
each through the golden side door -- repair and bump `goldenRevision`, or
dismiss it explicitly -- and only then re-answer. Do not net a suspect golden
against the flip count; an uncertain key is not noise you can average out.

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

- **Per-case, not aggregate.** No previously-passing case may regress: diff the
  new run's verdicts against the baseline, case by case (`jq` over the two
  `events.jsonl` files). `regressions` on the acceptance check event must be
  empty to accept, and the regressed qids go in the checkpoint commit message
  if you proceed anyway after a human call.
- **Confident verdicts only.** `needs_human` and null verdicts are neither
  passes nor failures; the delta is computed without them.
- **Both splits.** The acceptance check runs the affected dev cases AND the holdout
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
  corrected) may accept without a measured win. Record that as the acceptance check
  `reason`.

Write the `acceptance_check` event (decision, class, baseline and final run ids,
regressions, holdout delta, reason) BEFORE any commit, so a rejected
direction leaves a record. On reject: revert the files (`git checkout --`
or `git restore`) and reload. On accept: `issue_status: fixed` for what the
edit actually closed, **then checkpoint**.

## Checkpoint

A checkpoint is a git commit of the model repository, taken after an acceptance check
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

### Calibrate the bar before you trust it

This table was asserted, not measured, and the number it needs is a property of
your harness and your set -- not of this skill. Measure it with an **A/A run**:
the same model, same config, same set, twice, compared with
`scripts/flip_table.py`. Every flip it reports is noise by construction, since
nothing changed. Record the result with the set, in `CALIBRATION.md`, and cite
that file when you quote a band.

Re-measure whenever the model, judge, or set changes. This is not a formality:
observed bands have moved by a factor of three across a fortnight of ordinary
work, so a band carried over from a previous configuration is a number with no
claim on the present one.

One A/A is one sample of the flip count, not a distribution. It can show a bar
is too low; it cannot show one is high enough. Treat any measured band as a
floor.

Two consequences worth separating:

- **For acceptance**, the band is the threshold untargeted flips must sit under.
- **For diagnosis**, it is a warning that a single run's failure list is partly
  luck. Pick what to fix from the failures that fail in **both** A/A runs.
  Ranking a backlog by one run's clusters partly ranks which cases were unlucky
  that afternoon.

When you inspect the flips, attribute them before you accept them as
irreducible. A band dominated by the **judge** re-reading an ambiguous rubric is
not answerer noise, and it is not a floor you have to live under: sharpening
those rubrics buys more measurement power than any change to the answerer.

An A/A is not a repeat in the sense the sampling rule forbids. It is a one-off
calibration of the instrument, and the loop's whole acceptance rule rests on
the constant it produces.

### A targeted fix needs a targeted test

The flip-count table is the right instrument for a broad change and the wrong
one for a narrow fix. A fix that repairs three cases on a 49-case set moves the
total by three -- inside the noise band an A/A already produces -- so a
mechanically-verified repair reports as no effect and gets abandoned.

This is not a hypothetical failure mode: a mechanically verified repair, where
each fixed case now matches its golden exactly, can read as "no effect" on both
of two set-total comparisons. Worked examples are in the set's `CALIBRATION.md`.

So for a narrow fix use `scripts/flip_table.py --targets --noise-band`:

1. **Name the cases before the run.** Pick them from the stable failures of the
   A/A, never from a single run. Choosing them afterwards is choosing the answer.
2. Accept on the targeted cases: they were failing, they now pass, and none of
   them broke.
3. Separately require the untargeted flips to sit **at or below the A/A band**.
   That is what rules out a fix that trades one set of cases for another --
   above the band, investigate before accepting, however good the targets look.
4. **Run the post-edit arm twice and pass both** (`--b --b2`). The band counts
   flips; it never asks which cases flipped, and that is the hole. Noise
   scatters, so an untargeted case that breaks in *both* post arms is a real
   regression however small the count is.

Report both. A targeted win with untargeted flips above the band is not a win,
and a set-total that moved by less than the band is not evidence of anything
either way.

Step 4 exists because the band alone has accepted a real regression: an edit
whose untargeted flip count sat inside the band, but where the same untargeted
case broke in every post arm. One arm cannot tell that from a coin toss.

The reason to expect this, rather than treat it as bad luck: **a correct new
entity is not a safe one.** Adding a measure changes what agents reach for on
questions nobody was thinking about, so a well-named addition can pull a
neighbouring question onto the wrong denominator. That makes the untargeted
half of the acceptance check the half that matters, and it needs two arms to be
readable at all.

The one retrieval number this loop reports is **per-question entity recall**:
of the entities each golden answer depends on, how many did the agent's own
`get_context` calls deliver (`skill:eval-answer`, `scripts/score_retrieval.py`;
delivered means returned as a ranked entity, under a sibling source, or named
in a returned source's documentation). It is measured on the agent's real
search text against real questions, so it needs no hand-written terms. Read
it within an arm, to attribute a failure; it moves with the answerer, so a
cross-arm comparison of retrieval *itself* is not this loop's job -- that is
the engine-side `eval-retrieval` skill, which ships to no customer.
Coverage (can the model answer this at all) is a property of the model and its
data and is never reported under a retrieval heading.

If the contaminated fraction of attempts exceeds 0.1 (or any contamination,
on a run smaller than 10), the run is a harness failure. Do not publish a
model score.

## Out of scope

This loop is local. The ledger is files, the checkpoints are git, you are the
conductor. Do not:

- publish the model to a hosted platform as a "true" checkpoint or learning
  curve
- start a Python orchestrator (`loop.py`, `improve_batch.py`) that runs the
  five steps end to end unattended. You conduct; the scripts are the steps,
  not the sequencing
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
  `reference/ledger-schema.md` is the file contract; `skill:eval-judge` is
  the judge.
- `skill:eval-diagnose`: component, owner, issue events. No edit.
- `skill:eval-improve`: smallest model edit, probe receipts, no self-accept.
- The `malloy-analysis` skill: what the blind answerer follows. It is installed
  from the `analysis` manifest group, not the `eval` group.
