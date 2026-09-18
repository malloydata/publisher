---
name: eval-report
description: 'Write the summary a person reads after an evaluation run: what ran, what it scored, which failures were the MODEL and which were the EVAL itself (out of turns, contaminated, unestablished golden, unreadable judge), what was learned, and what to do next. Every claim links to the artifact behind it. Use at the end of any run, before quoting a number to anyone. Never scores an answer (eval-answer) or decides who owns a failure (eval-diagnose).'
---

# Report a Run

A run directory is JSONL and a console summary that scrolls away. Neither is a
result. This skill turns one into something a person can open, and it is the
last step of every run, including a run that failed.

The complaint this exists to answer, in a reviewer's words: *"eval runs and a
bunch of stuff happens and it is hard to know the actual results."*

**Scope boundary:** this skill reports what the ledger already says. It does not
score an answer (`skill:eval-answer`), decide who owns a failure
(`skill:eval-diagnose`), or edit a model (`skill:eval-improve`). If a number is
not in the ledger, do not put it in the report.

**The report is about THIS RUN, not about the harness.** Defects you find in
the eval tooling while running it are real and worth filing, and they do not
belong here: the reader wants to know what their model scored and why, not what
is wrong with the thing that measured it. File those against the harness. The
one exception is anything that qualifies THIS run's number -- a truncated
attempt, a contaminated one, an unestablished answer key -- which the "Eval
failures" section exists for.

## Step 1: build the artifacts, before writing a word

```bash
python3 skills/eval-loop/scripts/build_run_package.py \
    --run <run-dir> --set <set-dir> --out /tmp/eval-<label>
curl -sS -X POST http://<publisher>/api/v0/environments/<env>/packages \
    -H 'content-type: application/json' \
    -d '{"name":"eval-<label>","location":"/tmp/eval-<label>"}'
```

That builds a Malloy package over the run's own CSVs and registers it with no
restart. It gives you two things to link:

| Artifact | What it is | URL |
|---|---|---|
| The case matrix app | Every question, its verdict, which needed entities retrieval delivered, and a drawer per case holding the reference answer, the judge's reasoning, the re-executed rows and every query the answerer ran | `<publisher>/environments/<env>/packages/eval-<label>/` |
| `eval_run.malloynb` | The aggregate tables: pass rate, effort, cost, most-missed entities, the backlog | `<publisher>/<env>/eval-<label>/eval_run.malloynb` |

**Those two URLs are in different path spaces, and guessing costs a 404.** The
app is served by the in-package `public/` handler, which owns
`/environments/<env>/packages/<pkg>/<file>`. The notebook is a MODEL, rendered
by the Console, whose routes are `/<env>/<package>/<model path>` with no
`environments` or `packages` segment at all. Putting the notebook under the
app's prefix 404s, because the public-file handler answers there and the
notebook is not in `public/`. Verified both, on a real package, by opening
them.

Pass `--run` more than once to put two arms side by side.

## Step 2: write the report

Put it in the repository beside the set, as `RESULTS.md`. Not in a chat log,
not in `~/Downloads`, both of which have lost a findings document before.

**Derive every number from `events.jsonl` and `run.json`, not from memory or
from the console.** A figure retyped from scrollback is a figure nobody can
check, and the console rounds.

### The template

```markdown
# <set> -- <label>

<one sentence: what was being measured, against what, and why now>

## What ran

- Set `<name>` v`<datasetVersion>`, N cases (N dev, N holdout)
- Target `<env>/<package>`, model `<modelPath>`, pinned `<modelSha or targetVersion>`
- Answerer `<model>`, judge `<model>`, cap `<maxTurns>` turns
- Steps run: scrape/run, eval<, diagnose><, improve>
- Cost $X answerer + $Y judge

## The result

**N of M decided (P%).**   <-- or: **No pass rate.** See "Eval failures" below.

| qid | question | verdict |
|---|---|---|

[case matrix](<url>) - [aggregate tables](<url>)

## Retrieval and coverage

Covered? -> Retrieved? -> Correct?, with the per-arm numbers under each.

## Model failures

One entry per wrong answer. What it got wrong in plain words, then the
mechanism, then the link.

## Eval failures

What went wrong with the MEASUREMENT rather than the model. Empty is a real
and good answer; say "none" rather than dropping the section.

## Query errors

Malloy the answerer wrote that would not run, and whether it recovered.

## What this run taught us

## What to do next
```

### "What to do next" is a checklist, and most of it is derived

Do not invent this section. Most of it falls out of what the run did NOT do,
and a reader should be able to see that nothing was skipped silently. Walk
these in order and put every one that fires into the list, with its command:

| If the run shows | Then the next step is |
|---|---|
| `coverage: unmeasured` | run `check_coverage.py --set <set> --model <pkg> --out coverage.json`, then re-run with `--coverage` so it charges the failures |
| `goldenCheck: skipped` or the set names no `truthPackage` | build one with `init_truth_package.py`; until then the goldens were derived through the model under test and certify themselves |
| any golden still `provisional` | re-derive and `verify_goldens.py --promote` |
| a stale entity name warning | fix `expectedEntities`; it scores as a retrieval miss on every run until you do |
| a passing case with recall below 1.0 | check whether `required` over-specifies one path |
| `truncated` non-empty | re-run those cases at a higher cap with `--from` |
| diagnose did not run | run it, or say the failures have no owner yet |
| a cluster with `owner: model` | `skill:eval-improve`, then the acceptance check |
| a cluster with `owner: agent-skill` | **edit that skill.** This is NOT a dead end |
| a cluster with `owner: dataset` | the golden side door in `skill:eval-loop` |
| only one arm exists | note that no noise band has been measured for this set yet |

**An `agent-skill` cluster is work, not an absence of work.** `eval-improve`
may not touch it, and writing "nothing to do, the model is fine" there is how a
real defect gets closed as a non-finding. Name the skill, name the rule to add
or change, and say who owns it. The model being innocent is a statement about
the model, never about the run.

### Write it for someone who was not there

The reader is a colleague who did not run this and does not know the loop's
vocabulary. Two rules, both learned by handing a report to one:

- **Never make a bare count carry the meaning.** "The only cluster is
  `owner: agent-skill`" tells a reader nothing: what is a cluster, and what
  follows from it? Write what happened and what to do: "The one wrong answer
  came from the agent picking the wrong kind of field, so the fix is in the
  analysis skill, not in the model."
- **Do not open with a negation of something you just reported.** A section
  that says "nothing to do" directly under a section reporting a wrong answer
  reads as a contradiction, and the reader stops trusting both. Lead with the
  wrong answer and what closes it; put anything genuinely needing no action
  after that, and say why.

## The two failure sections, and why they are separate

This is the part most reports get wrong. A wrong answer and a broken
measurement look identical in a pass rate and have nothing else in common: one
is work for the model owner, the other is work for whoever runs the harness.
Mixing them sends a modelling agent to fix a model that is fine.

**Model failures** are cases that were fairly asked, fairly answered and got the
wrong answer. One entry each: what the answer said, what was right, and the
mechanism in one sentence. Do not write a cluster id here; write what it got
wrong.

**Eval failures** are everything that stopped the run measuring the model.
Report every one that occurred, with its count and its qids, and say plainly
that these are NOT evidence about the model:

| What happened | How it reads in the ledger | Who fixes it |
|---|---|---|
| Answerer ran out of turns | `reason: answerer_truncated`, and `run.json` `truncated` | raise `--max-turns`, re-run those cases with `--from` |
| Isolation breached | `reason: contaminated`, `contamination_reasons` on the attempt | harness config; the answerer held a tool it should not |
| Harness or server failed | `reason: environment_failure: <what>` | fix the environment, re-run the arm |
| Answer key not established | `reason: golden_provisional` / `_invalid` / `_ambiguous` / `_missing` | derive the key (`skill:eval-import`), or the golden side door |
| Judge reply unreadable | `reason: judge_unparseable` | it already retried; read `artifacts/<qid>/judge.md` |
| Judge doubted the key | `gold_status` `suspect` / `verified_wrong`, `run.json` `doubtedGoldens` | the golden side door in `skill:eval-loop`, NOT improve |
| Rubric quoted a stale figure | `verify_goldens.py` check 2 review items | repair the rubric against its own golden rows |
| Arm stopped early | `run.json` `status: aborted` | four consecutive dead attempts; fix and re-run |

**If any of the first three occurred, there is no pass rate to quote.** The
harness prints `INCOMPLETE` and withholds the percentage; the report does the
same. Give the counts and the re-run command instead of a number with a caveat,
because the number is what gets repeated and the caveat is what gets dropped.

## Retrieval and coverage belong in every report

A pass rate says an answer was wrong. It does not say WHERE, and the three
metrics that do are already in the run summary and the notebook. Report them,
because a report that omits them makes every failure look like the model's:

- **Covered?** Can the model express a correct answer at all? Not computed by
  the run: `check_coverage.py` reads the MODEL rather than the answers, and the
  run consumes its report through `--coverage`. **If it was not run, say
  `unmeasured` rather than leaving the row out** -- an unmeasured coverage
  label is not evidence that the model covers the question.
- **Retrieved?** Of the entities the golden answer depends on, how many did
  `get_context` hand back. Quote `required_count`, `delivered_count` and how
  many were ranked rather than merely mentioned in a returned source's docs.
- **Correct?** The pass rate, which is the rung the other two qualify.

Two numbers here are routinely misread, so qualify them in the report or leave
them out:

- **Entity precision is a PAYLOAD number, not a retrieval-quality one.** Report
  it as what it measures: how much context the agent was handed against how
  much it needed. 52 entities returned per attempt for the 2 an answer used is
  a real cost, in tokens and in attention, and it is worth tracking. What it
  cannot tell you is whether retrieval worked, because the denominator is
  everything returned and it ignores rank: an entity that came back at rank 3
  of 51 scores identically to one at rank 51.
- **For quality, report rank.** "Required entities came back at median rank 3,
  9 of 16 in the top 5" is a statement about retrieval. Precision alone reads
  as an indictment of it and is mostly a statement about how many fields the
  package has.
- **Report the misses split by cause, not as one recall figure.** The run
  attributes each to `never asked` (no target of a type that could return it --
  deterministic, because `target_type` is a hard filter on the server, so no
  documentation could have delivered it) or `not retrieved` (a compatible target
  was issued and it still did not come back, which is the docs or the wording,
  and diagnose separates them). Those have different owners and one recall
  number hides which you have.
- **A miss on a case that PASSED is still a miss, and the report says so.** The
  answer was right by another route; name the route. Measured on one run, 3 of
  4 findings were on passing cases and the route was always the same: the agent
  rebuilding the model's own measure inline, which held for `count()` and failed
  on the one measure carrying a grain rule.
- **Read the entities that did NOT come back, and what was asked for.** That is
  where the retrieval signal actually is, and the misses are rarely independent.
  Measured on one run: 5 required entities never came back as ranked results,
  and 4 of the 5 had the same cause -- the agent sent only `source` and
  `dimension` targets, and `target_type` is a hard filter, so no measure could
  be returned however well the model documents it. One defect, five symptoms,
  and the same one that produced the run's only wrong answer. A per-case list of
  misses beside what was asked for would have shown it in a glance.
- **A PASSING case with recall below 1.0 is usually an expectation defect**, not
  a retrieval miss: the set named one path to an answer the agent reached by
  another. Report the count and read it as a prompt to check `required`.

## Query errors are worth a section of their own

Malloy the answerer wrote that would not compile or run, whether or not the
case passed. These sit on `tool_call` events as `error`, and nothing else
surfaces them: a case that errored twice and then recovered scores exactly like
one that got it right first time, so the effort disappears.

They are the sharpest available evidence on whether the skills and the docs are
leading agents astray, because each one names a specific thing the agent
believed and the language does not support. Report the count, the distinct
error kinds, and for each whether an existing skill already covers it. An error
whose fix IS documented, in a skill the answerer did not open, is a finding
about the skills rather than about the agent.

`eval-diagnose` does not see these today: it reads failing cases only, so an
error inside a passing case is invisible to it. Say so rather than implying
they were triaged.

## Say what did not run, and why

A step that did not run is a result, and it is indistinguishable from having
forgotten unless it is written down. The common one: `improve` does not run when
every cluster came back `owner: agent-skill` or `owner: dataset`, because
`skill:eval-improve` may only touch `owner: model`. That means the model is not
at fault, which is worth a sentence rather than a silence.

Same for diagnose. If it did not run, no failure in this report has an owner
yet, and the report says so rather than implying the failures are the model's.

## Findings and improvement ideas

Two different things, kept apart.

- **Findings** are what this run established, each with the evidence beside it.
  A finding with no artifact behind it is an opinion.
- **Improvement ideas** are what somebody might do about them, tagged with who
  owns each: the model, the answer key, the analysis skills, or the harness.
  An idea is a proposal, not a decision, and the report does not pretend a
  cluster has been triaged when it has not.

Rank by how many cases each would move, and say when the answer is "nothing":
a 90% run whose one failure is a known fan-out trap may need no action at all.

**Report facts, do not instruct the reader.** "This set has had one arm, so no
noise band exists for it" is a fact they can act on. "Do not quote 90% until X"
is a lecture about their own number, and they did not ask for one. State what
was and was not measured; what to do with it is theirs.

## Make the links clickable

A report whose evidence cannot be opened is a report nobody checks.

- **Absolute paths**, never relative ones. A reader is not in your working
  directory, and a terminal only makes an absolute path clickable.
- **The served URL** for the app and the notebook, with the real host and port
  the run used, not a placeholder.
- Per case, link the FILES, not the directory: `artifacts/<qid>/answer.md`,
  `artifacts/<qid>/judge.md`, `artifacts/<qid>/answerer.jsonl`. A `file://`
  link to a directory opens nothing in most editors, which is a dead link that
  looks live.
- **Move the run somewhere durable before you link it.** A run written under a
  session scratch directory (`/tmp/...`, a host's per-session sandbox) is
  deleted when the session ends, and many editors will not linkify a path there
  even while it exists. Copy the run directory, artifacts included, next to the
  report under the set, then link that. A report whose evidence is in a temp
  directory is a report with no evidence by next week. This was caught twice by
  a reader clicking and getting nothing.
- Write a local path bare, as `/abs/path/to/file`, not as `[text](file:///...)`.
  Terminals and editors linkify a bare absolute path; a `file://` markdown link
  is frequently inert, and an inert link is worse than the path in plain text
  because the reader has nothing to copy.
- Never link a run LABEL or a qid as though it were a path. `faa-v1-baseline-01`
  is a label; the path is the run directory. A backticked label that looks like
  a link and resolves to nothing is worse than plain text, and it has already
  wasted a reader's time.

## Related skills

- `skill:eval-loop`: conducts the run this reports on.
- `skill:eval-answer`: the verdicts and the ledger schema this reads.
- `skill:eval-diagnose`: owners and clusters, when it ran.
