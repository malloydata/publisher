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

| Artifact | What it is | Where |
|---|---|---|
| The case matrix app | Every question, its verdict, which needed entities retrieval delivered, and a drawer per case holding the reference answer, the judge's reasoning, the re-executed rows and every query the answerer ran | `<publisher>/environments/<env>/packages/eval-<label>/` |
| `eval_run.malloynb` | The aggregate tables: pass rate, effort, cost, most-missed entities, the backlog | the same package, rendered by Publisher |

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

## Model failures

One entry per wrong answer. What it got wrong in plain words, then the
mechanism, then the link.

## Eval failures

What went wrong with the MEASUREMENT rather than the model. Empty is a real
and good answer; say "none" rather than dropping the section.

## What this run taught us

## What to do next
```

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

## Make the links clickable

A report whose evidence cannot be opened is a report nobody checks.

- **Absolute paths**, never relative ones. A reader is not in your working
  directory, and a terminal only makes an absolute path clickable.
- **The served URL** for the app and the notebook, with the real host and port
  the run used, not a placeholder.
- Per case, link its `artifacts/<qid>/` directory: it holds `answer.md`,
  `judge.md` and the answerer transcript.
- Never link a run LABEL or a qid as though it were a path. `faa-v1-baseline-01`
  is a label; the path is the run directory. A backticked label that looks like
  a link and resolves to nothing is worse than plain text, and it has already
  wasted a reader's time.

## Related skills

- `skill:eval-loop`: conducts the run this reports on.
- `skill:eval-answer`: the verdicts and the ledger schema this reads.
- `skill:eval-diagnose`: owners and clusters, when it ran.
