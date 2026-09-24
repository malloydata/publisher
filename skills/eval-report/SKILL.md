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

**Do not audit the harness. You were asked to measure a model.** This is the
most common way this job goes wrong, and it does not look like going wrong: a
run turns up something odd in a script, the odd thing is genuinely a bug, and
the reply comes back as a critique of the tooling with the model's score
somewhere underneath. The reader asked what their model scored. Answer that.

So, unless the user asked you to work on the harness:

- Do not read harness source to satisfy your own curiosity about a number.
  Read it when a number you must report cannot be explained any other way, and
  stop when it can.
- Do not propose harness fixes, refactors, flags or "while I was in there"
  improvements. Not in the report, not in the chat reply.
- When a harness defect DID change this run's number, the report gets one
  sentence: what the number should be and why. Not the mechanism, not the
  file, not the fix.
- Keep a defect that changed nothing out of the report entirely. Mention it
  once in chat, in a line, and let the user decide whether they want it
  chased.

A harness bug you found and did not chase is not a loose end. It is the job
being done. If the user wants it fixed they will say so, and then it is a
different task with its own turn.

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

### Keep it under a page and a half

**Budget: about 80 lines, and 120 is the ceiling for a run with several
distinct failures.** Reports have shipped at 200 and the length is not
thoroughness -- it is the artifact links restated as prose, the cascade
explained twice, and a paragraph apologising for a figure nobody disputed.
A reader who wants the detail opens the case matrix, which is why step 1 builds
it. What cannot be recovered from the artifacts is your judgement: what broke,
why, and what to do. Spend the lines there.

What earns its place: the headline, one row per case, one short entry per
failure, the retrieval numbers, and the next steps. What does not: restating a
number you already gave, explaining what a cascade is before showing it,
defending a decision nobody questioned, or any section whose content is that
nothing happened -- except "Eval failures", where "None." is the point.

**Being brief is not being terse with the vocabulary.** The reader does not
know what `near_match`, a cluster, recall or a holdout is, so the first time
one appears, say what it means in the same sentence -- "`near_match`, which is
excluded from the pass rate" -- and then use it. Cutting the explanations is
how a short report becomes an unreadable one; cutting the restatements is how
it becomes a good one.

### The template

```markdown
# <set> -- <label>

<one sentence: what was being measured, against what, and why now>

## What ran

- Set `<name>` v`<datasetVersion>`, N cases (N dev, N holdout)
- Target `<env>/<package>`, model `<modelPath>`, pinned `<modelSha or targetVersion>`
- Answerer `<model>`, judge `<model>`, cap `<maxTurns>` turns
- Steps run: scrape/run, eval<, diagnose><, improve>
- Cost $X answerer + $Y judge, N turns median / N p90, N entities delivered per answer

## The result

**N of M decided (P%).**   <-- or: **No pass rate.** See "Eval failures" below.

| qid | question | verdict |
|---|---|---|

[case matrix](<url>) - [aggregate tables](<url>)

## Coverage, retrieval, accuracy, cost

**Coverage N of M** -- how many questions the model can express an answer to at
all. **Entity recall N%** -- one number, first, before the cascade. It is the share
of the entities an answer needed that retrieval actually handed the agent, and
it is the headline of this section for the same reason the pass rate is the
headline of the last one. Then the cascade: Covered? -> Retrieved? -> Correct?,
with the per-arm numbers under each. Say in one clause what recall counts, then
give the number.

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
| `coverage: unmeasured` | the run was given `--no-coverage`, or the measurement failed and said so. It is measured by default, so say in "Eval failures" that this score cannot tell a model gap from a bad answer, and re-run without the flag |
| `goldenCheck: skipped` or the set names no `truthPackage` | build one with `init_truth_package.py`; until then the goldens were derived through the model under test and certify themselves |
| any golden still `provisional` | re-derive and `verify_goldens.py --promote` |
| a stale entity name warning | fix `expectedEntities`; it scores as a retrieval miss on every run until you do. A next step, not a section: it goes in this list and nowhere else in the report |
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

**Eval failures** are the things that stopped the run measuring the model, and
**only** those. The test is one question: *did this cost a verdict, or make one
untrustworthy?* If no, it does not appear in the report at any length.

That rules out most of what is tempting to put here, and all of it has been put
here on a real run: a `-dirty` model pin, a stale-entity-name warning that cost
no case, a defect you hit in the harness and worked around, a setup step that
took two tries, anything you would open with "worth knowing, though it changes
no verdict". A reader wants to know what their model scored. Harness defects are
real and belong in a harness issue, filed against the harness -- that is the
skill's opening rule, and this section is where it gets broken.

The section is usually two words. "**None.**" is a complete and good answer, and
a reader who sees it learns exactly what they need to. Do not pad it into a
paragraph explaining the absence.

Report every one that DID occur, with its count and its qids, and say plainly
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

## The four measurements, and why a report carries all of them

A pass rate says an answer was wrong. It does not say where, and on its own it
makes every failure look like the model's. A run measures four things, in this
order, and each one tells you whether the next is even a fair question:

| | Measures | A failure here means |
|---|---|---|
| **a. Coverage** | can the data and the model express an answer at all | nothing downstream was winnable. Fix the model |
| **b. Retrieval** | did `get_context` hand the agent the entities it needed | the model has it and the agent never saw it: the docs, or the search wording |
| **c. Accuracy** | did the agent get the answer right | it had what it needed and still missed: the agent, or a doc that misleads |
| **d. Cost** | dollars, turns, wall-clock, entities per answer | it works and cannot be afforded, which is its own kind of not working |

**Coverage first, and a report without it is incomplete.** A model that cannot
express an answer can never succeed at that question, so a pass rate quoted
without coverage cannot distinguish a bad model from an unanswerable set.
`run_baseline.py` measures it by default; if a run skipped it, say so in
"Eval failures" and treat the score as provisional.

**Cost is a result, not an aside.** Report dollars, the turn and call counts,
and the entity payload per answer. An agent that answers correctly in 40 turns
and 75 delivered entities per question is a different product from one that
does it in 8 and 5, and only the report says which you have.

Report them all, because a report that omits them makes every failure look
like the model's:

**Copy the cascade the run printed. Do not re-derive it.** `cascade_lines()`
in `run_baseline.py` already renders every rung, and it carries one field a
hand-made table keeps losing: how many cases **answered correctly anyway**
after stopping on that rung. Paste its block into the report.

**A finer measurement must never revise the headline result.** This is the rule
the re-derivation breaks. Written as a funnel with a shrinking denominator --
`5 of 10 covered`, then `3 of the 5`, then `3 of the 3` -- the last rung reads
as "only 3 of 10 succeeded" on a run where **all ten answers were correct**.
That report went to a reviewer, and the objection was the right one: adding
detail about retrieval cannot turn a 10 of 10 into a 3.

Two things stop it:

- **Report every rung over all N cases**, never over the survivors of the rung
  above. The rungs are three measurements of the same cases, not a narrowing of
  them.
- **`delivered, right` is NOT the pass rate.** The passing cases are
  `delivered, right` plus `passed_not_covered` plus `passed_not_retrieved`. A
  report that omits those last two has dropped the only numbers that reconcile
  the cascade with the score.

**Label a rung for what it measures, not for what a reader will assume.**
"Can the model express an answer?" is wrong for most of what lands on the `no`
side of the first rung. `RULE_UNWRITTEN` means the data is present and the
model does not encode the rule for combining or filtering it; `AMBIGUOUS` means
several candidates and no doc saying which the question means. The model can
express an answer in both -- it does not say WHICH answer is meant. So the rung
asks whether the model NAMES what the question needs.

**Give the per-entity number too, and label both.**

- per CASE, the cascade: did this case get everything it needed
- per ENTITY, recall: of the N entities the answers depended on, how many were
  delivered

They answer different questions, and the entity number is the more natural read
of "how did search do". One run reported `3 of 5` cases and buried `16 required
entities, 80% delivered`, and the case number was the only thing a reader saw.

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
