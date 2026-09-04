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

## Where the rest of this lives

This file is the procedure. Five things it used to carry inline are files beside
it now, because each is needed at one moment rather than every run, and loading
all of them for every run is how a skill stops being read.

| When | Read |
|---|---|
| about to run one | `reference/running-a-run.md` |
| a golden is wrong, doubted, or out of step with the model | `reference/golden-side-door.md` |
| deciding whether an edit stays | `reference/acceptance-check.md` |
| about to quote a number, or set the noise band | `reference/measurement.md` |
| you changed judge doctrine or its inputs | `reference/checking-the-judge.md` |

Read the file, do not work from the summary here. The acceptance-check rules and
the golden side door are both places where acting on a half-memory of the rule
produces a confident wrong answer rather than an error.

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

Two things to check before a platform run, because neither errors and both make
the run measure something other than what it names:

- **The answerer's skills must be written for THIS host.** A shared skill names
  an MCP tool by its bare name (`get_context`) so it reads correctly anywhere,
  but a host/router skill names its own host's tools directly. Install the
  latter for the wrong host and the answerer is told to call tools it does not
  have. `run_baseline.py` warns when the manifest it loaded names Publisher-only
  tools on a platform target; point `--answerer-manifest`, or `--skills-root`,
  at the checkout that ships this host's manifest.
- **The tool names are configuration.** `--hosted-mcp-server` is both the
  `mcp__<server>__<tool>` prefix and the OAuth cache key, so it has to match the
  name the answerer authenticated under, and `--hosted-tools` lists the bare
  tools that host exposes.
- **Get the hosted tools in front of a headless answerer, one of two ways.**
  A spawned answerer cannot complete an OAuth flow, so the tools have to be
  reachable before the run starts. `run_baseline.py` proves it with one cheap
  probe and refuses to spend an arm otherwise -- a run whose answerers have no
  tools does not error, it reads as a terrible model.

  1. **Authenticate once, interactively.** Works anywhere, including a plain
     CLI install, and is the route to assume unless you know otherwise. The
     token is cached per server NAME, so authenticate under the same name the
     run passes to `--hosted-mcp-server`:

     ```bash
     claude mcp add --transport http <name> <scoped-url>
     claude          # then /mcp -> <name> -> Authenticate
     ```

     Then come back and run. This is a hand-off to a person; there is no
     headless equivalent, so plan for it rather than discovering it mid-run.

  2. **A local proxy that already holds the credential.** Some hosts ship an
     editor extension whose local MCP proxy can expose the hosted
     `get_context` / `execute_query` -- often behind a setting that is off by
     default. Where that exists, point `--mcp-url` at the proxy on localhost
     and no OAuth step is needed, because the extension holds it. Check what
     the proxy actually exposes before relying on it: the same proxy may serve
     a local Publisher's `malloy_*` tools instead, and then `--hosted-tools` is
     naming tools that are not there. This route is not available to someone
     running the CLI alone.

- **Prefer a SCOPED endpoint URL over asking for scope.** A hosted MCP is
  usually reachable two ways: a global endpoint where every call carries an
  organization and workspace, and a scoped one where the URL itself is the
  scope. `--scope` and the prompt can only ASK an answerer to stay in one
  package; a scoped URL enforces it. For an agent being measured that is the
  difference between a case answered against the package it names and one
  answered against whatever else the account can see. Authenticate once
  interactively (`claude`, `/mcp`) under the same server name the run will use;
  the token is cached per name, and a spawned headless answerer cannot complete
  an OAuth flow.

## Before you start

1. The model package under evaluation must live in a git repository, with
   `evals/<set>/` in the package, beside the model files. Git is the checkpoint
   mechanism; without it there is no rollback and no run can include improve.

   Keeping the set IN the package is what stops a model edit and its answer key
   drifting apart: they move in one commit, so fixing a measure and forgetting
   the golden that depended on it stops being possible. It is safe -- measured
   on a running server, a `cases.jsonl` inside a package appears in no model
   listing, no notebook listing, no package resource, and 404s over HTTP, so an
   MCP-only answerer has no route to it.

   What it buys differs by target. On a LOCAL Publisher it does not get you free
   versioning -- `sourceContentSha` hashes model paths only, so the set needs
   its own `datasetSha`. On a hosted target that publishes the whole package
   directory as an IMMUTABLE version, the set rides inside that version and
   `targetVersion` pins model and answer key together; nothing can be edited
   under a published version, which is what makes it a pin. Check which you have
   before deciding how much of this you need.

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
   `BAD-REFERENCE` or `AMBIGUOUS-REFERENCE`, follow
   `reference/golden-side-door.md`. Both are expected in the wild; both
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

## Out of scope

This loop is local. The ledger is files, the checkpoints are git, you are the
conductor. Do not:

- publish the model to a hosted platform as a "true" checkpoint or learning
  curve
- start a Python orchestrator (`loop.py`, `run_all.py`, `improve_batch.py`)
  that runs the five steps end to end unattended. You conduct; the scripts are
  the steps, not the sequencing. There are more than twenty of them and they
  are not the exception to this: each does one step you invoke and hands back a
  result you read. What is forbidden is a script that decides what to do next,
  because every judgement this loop protects lives in that decision
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
