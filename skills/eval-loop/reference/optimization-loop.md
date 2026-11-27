# The optimization loop

"Optimize my model" means: move some numbers about a package toward a goal,
by changing things the package controls, and prove the change did it. This
file describes the loop every kind of optimization here follows. Two kinds
use it today:

- **Quality**: how often an agent answers the package's questions correctly.
- **Speed**: how fast its queries run, how fresh their data is, and what
  that costs (materialization).

The loop has seven steps. They are not a straight line: any step can find
that an earlier one is not done, and send the work back to it, saying why.

## The steps

| # | Step | What it produces | Done when |
|---|---|---|---|
| 1 | **Objective** | The metric to move, limits on the others, the settings you may change, and the scope (which questions or queries, which time window) | It can be written as one number to improve plus limits, each with a value |
| 2 | **Ingest** | The data that measures each metric, per item (question or query) and per thing you may change | Every metric in step 1 has a source for every item, or the gap is named |
| 3 | **Model** | A Malloy model over that data whose measures are the step-1 metrics | Each metric is a measure you can query |
| 4 | **Simulate** | For each candidate change, a prediction of every metric | Each candidate has a prediction and says how it was made |
| 5 | **Strategy** | The candidates worth choosing between, each with its trade-offs, shown to a person (a data app or report) | A person can pick one without reading code |
| 6 | **Act** | The chosen change, as a draft or a branch a person lands | A person landed it, or rejected it |
| 7 | **Learn** | What happened, compared with what step 4 predicted | The gap is recorded and sent to the step it came from |

## Going back a step

Say why, as a plain question to the person or a plain line in the report:

| Found at | Back to | Example |
|---|---|---|
| Ingest | Objective | "The goal includes storage cost, but these tables report no size. Drop it from the goal, or read the warehouse's metadata?" |
| Model | Objective | "'Fast' is not a number. Which queries, and what p95?" |
| Model | Ingest | "I have the query text but not the given values, so 40% of queries cannot be replayed." |
| Simulate | Ingest or Model | "To predict which queries a rollup answers I need each query's shape, so I will replay to capture path reports." |
| Simulate | Objective | "Every option meets the goal. Tighten it, or take the cheapest?" |
| Strategy | Simulate | "These two options differ by less than run-to-run noise. Running a costlier check on both." |
| Learn | Model, Simulate or Objective | "The rollup saved 30% less than predicted; its read time was underestimated." |

## Simulate cheaply first

A prediction can come from checks that cost very different amounts. Run the
cheap ones on every candidate and the costly ones on the best few:

1. **Estimate from data already ingested.** Free.
2. **Ask the system without changing anything** (a dry run, a compile, a
   retrieval-only check). Free or nearly.
3. **Try it on a small or local copy.** Minutes.
4. **Try it for real.** Real time and money.

Backtest wherever history allows: replay the past against the candidate and
compare the prediction with what is measured. Record every prediction so
step 7 can compare it with the outcome.

## The two kinds, step by step

| Step | Quality | Speed |
|---|---|---|
| 1 Objective | Pass rate on held-out cases (`test` split). Only model docs or package skills may change | Weighted p95 latency (each query weighted by how often it runs), a freshness limit, a monthly cost limit. May change: which sources get a table (`#(materialize)`), rollup grains (`#(rollup)`), persisted views and queries, storage, refresh |
| 2 Ingest | Production turns (`eval.py ingest`) and a judged sample of them (`eval.py judge-sample`); the set (`eval-import`); each run's verdicts, transcripts, tool calls, cost | Production queries and table builds (`eval.py ingest`); a workload from them (`workload_from_logs.py`); a replay recording each query's time and path report (which table answered); prices |
| 3 Model | `history.malloy`: production quality per day and version; `eval-diagnose` clusters failures by cause and owner; `optimization.malloy` holds the eval runs | `history.malloy`: latency percentiles, serving and build cost per day, data age per table |
| 4 Simulate | Weak: an agent's behaviour decides. Cheap checks: rerun only the failing cases plus a sample; check retrieval alone. Otherwise a full run on an experiment | Strong: the router is deterministic. Estimate which queries a table answers from each query's shape (grouped-by, filtered-by, measures) against the table's grain; pick the cheapest set that meets every limit (`simulate`'s `cheapest` option); dry-run publish for serve verdicts; build and replay the best few |
| 5 Strategy | Diagnosed causes as options, each with the cases it would fix and its risk | Options with latency, freshness and cost; which queries each speeds up |
| 6 Act | `accept`: beats the noise band twice, held-out pass rate does not drop; a branch a person merges | The same, plus: the targeted queries are answered by the new table (path report), rows unchanged, cost within the limit; a person publishes |
| 7 Learn | `eval.py predict` before review, `eval.py outcomes` once published: the run's pass rate against production's judged turns on that version. `attempts.jsonl`: each edit tried, its effect, a one-line lesson | The same, with the option's mean query time and share over `max_query_ms` against production's queries on that version |

The two kinds are limits on each other. A speed change must not change
answers (rows unchanged, pass rate holds). A quality change must not make the
workload slower or stop a table from serving.

## The optimization package

Each served package gets one optimization package beside it, never inside it,
served where the answerer cannot reach it because it holds verdicts. It is at
`[optimization] package_dir` (default `<workdir>/packages/optimization-<set>`)
and holds two models, each with an app:

| | Model 1: `history.malloy` | Model 2: `optimization.malloy` |
|---|---|---|
| Answers | How did production do, per day and per model version? | What did the loop try, what did it predict, and was it right? |
| Tables | `prod_turns`, `prod_transcripts`, `prod_queries`, `prod_table_builds`, `model_versions`, `prod_judgments`, `price_card` | `runs`, `versions`, `tag_scores`, `routines`, `predictions`, `outcomes` |
| Written by | `eval.py ingest` (appends log rows once each, drops the loop's own traffic), `eval.py judge-sample collect` | `eval.py history` (rebuilt from the runs), `eval.py predict`, `eval.py outcomes` |
| App | `public/history/` | `public/optimization/` |

Model 1 knows nothing about eval sets. Every table is a CSV under `data/` in
the columns `optimization_package.py` defines; a hosted run loads the same
CSVs into a managed database, so local and hosted runs write one format.

The production quality number comes from judging a sample, because a
production turn has no answer key:

```bash
python3 eval.py ingest --set <set> --turns turns.jsonl --queries queries.jsonl --versions versions.jsonl
python3 eval.py judge-sample pick --set <set> --label prod-20261125    # draw unjudged question groups as a set
# a standalone question the set already keys takes that verified key;
# derive the rest (skill:eval-import), copy each group's key to its other turns,
# then judge the logged answers:
python3 eval.py judge-sample share-keys --set <set> --label prod-20261125
python3 eval.py run --set <workdir>/judging/prod-20261125 --label prod-20261125 --rejudge
python3 eval.py diagnose --set <workdir>/judging/prod-20261125 --label prod-20261125
python3 eval.py judge-sample collect --set <set> --label prod-20261125   # refuses an undiagnosed wrong answer
```

What `pick` draws, and what `collect` records:

- **Question groups, not turns.** Identical questions (case and spacing
  ignored; a follow-up only after the same earlier question) are one group
  with one key, derived on the case marked `sourceFields.keyHere` and copied
  to the rest by `share-keys`. Up to `--per-group` turns (default 3) are
  judged against it, because the same question can be answered right one day
  and wrong the next. `collect` shares the group's other turns out among the
  judged ones, each carrying that verdict (`sampled_because: carried`,
  `judged_turn`). A demo prompt asked 45 times costs one key and three judge
  calls, and weighs 45 in the rate.
- **User signals first.** A group whose next message corrected the answer
  (`correction`) or asked it again (`asked_again`) is drawn before the rest.
  The judge sees that next message as evidence, never as the verdict.
- **Earlier messages.** Each case carries up to three earlier user messages
  of its session, with a short excerpt of each answer, never their tool calls.
  The judge records which the question needs (`depends_on`).
- **Retrieval.** Where the key lists `expectedEntities`, `collect` records the
  recall of what `get_context` returned (`retrieval_recall`).
- **Key kind.** `key_kind` is the key's kind, so the history model can report
  coverage (the share it can answer) and correct refusals (`unanswerable`).

Each verdict records its `basis`. `answer`: the log held the prose the person
saw. `final_query`: an MCP or API client logs only its calls, so the judge read
the last query that succeeded and its re-run rows. The two are different
numbers; model 1 groups every quality view by basis and never averages them.

Model 1 also shows, per week: quality, coverage, correct refusals, retrieval
recall, and the effort a right answer took (tool calls, failed queries, tokens);
questions judged right one day and wrong another (`mixed_verdicts`); the right
answers that took the most calls; and what Credible saved (below).

The improve side then works from production rather than from questions someone
thought to ask (step O1):

```bash
python3 eval.py build-set --set <set> --label prod-20261125   # a new set version
V=<optimization package>/sets/prod-20261125                    # build-set prints it
python3 eval.py routine --kind quality --mode improve --set $V
python3 eval.py routine --kind speed --mode improve --set $V --queries $V/workloads/prod-20261125.jsonl
```

`build-set` writes a new set version each time, in the optimization package
(`sets/<label>/`), never in the model's repository: the set holds the answer
key, and it changes with production, not with each edit. The pull request names
the version it was checked against. Every version shares one attempts file in
the optimization package, so the improve step sees what earlier versions tried.

A version holds only questions judging keyed, one per question group:

- **Failures**, up to half of `--questions` (default 100): groups judged wrong,
  including mixed ones, taken round-robin across failure codes. Tagged
  `failure` and `failure:<code>`.
- **Controls**: groups judged right every time, as many as the failures and at
  least 5, round-robin across the source each answer queried. Tagged
  `control`. Every edit is re-asked on every control, and breaking one blocks
  acceptance.
- **Follow-ups** carry their earlier questions as `context.priorQuestions`. The
  run answers those first in the same session (`claude -p`, then `--resume`)
  and judges only the last. A chain of more than three, or one through a build
  request, is left out and counted.

The workload is every production query that succeeded, counted.

Improve mode stops after `--max-rejected` edits in a row are not accepted
(default 3), and writes the pull request body beside the routine record
(`<workdir>/routines/<label>.pr.md`): the production failures the edit closes
with how often each was asked, the edit, before and after on the targets and the
controls, the run ids, and what a person must settle (unverified keys,
undecided verdicts, doubted keys, failures diagnosed as the eval's own).

**Keeping the history in a database that takes no inserts.** The history
tables only grow, and a managed database cannot add rows to a table. So each
export is a batch:

```bash
python3 eval.py history-export --set <set> --label 20261127 --connection <database>
```

It writes the rows added since the last export as
`<optimization package>/export-history/<label>/<table>.csv`, to load as
`<table>__<label>` (`manifest.json` names each), and a package to publish beside
them, `<optimization package>-hosted/`, whose `history.malloy` reads each table
as the union of its loaded batches by name, cast to the local model's types.
Exporting a label again rewrites exactly its rows. Load the tables and publish
the package into an environment of its own: it holds verdicts and production
answers.

**What Credible saved.** `prod_queries.engine` says where a query ran
(`warehouse` or `credible`; empty means live ran on the warehouse and a table
read on Credible). The history model prices each where it ran
(`usd_per_compute_hour` or `usd_per_tb_scanned` with `bytes_scanned` on the
warehouse, `credible_usd_per_compute_hour` on Credible) and estimates what each
table-served query would have cost live, from the same query's live runs or
its source's. Saved is that live cost, less what serving cost, less the
warehouse cost of the builds and the cost of storing the tables
(`credible_usd_per_gb_month`). `simulate` prices an option the same way.

What the host's log skill must write, learned on real logs:

- **One turn per user message.** Fold a continuation into the message before it.
- **`kind`.** `build` for a turn that edited files or published, `question`
  otherwise. Build requests have no answer to judge.
- **`origin: draft`** for traffic against an unpublished edit. A host may log
  it under the published package's name.
- **Transcripts at tier T2** (`log_transcript.build_transcript`): the answer
  was logged, the agent's isolation was not. T3 voids every production turn,
  because the run looks for the start-up event a local answerer writes.
- **Clients with no session** (MCP connectors, API callers): group one user's
  calls to one package into a conversation, broken after 30 minutes of
  silence, and start a turn at each new `user_prompt`. A turn with no prompt
  has no question and is not judged.

Name the environments the loop itself queries in
`[optimization] exclude_environments`, or its eval runs are ingested as
production. Set `[optimization] production_environment` too: a package name is
unique only within an environment, and two environments in one organization
can each serve a package of the same name. Before a change goes to review, `eval.py predict` writes down what
it should do; after it ships, `eval.py outcomes` checks. Compare the total too,
not only whether each outcome fell in its range: when every range is wide or
starts at zero, "inside the range" teaches nothing, and the mean gap in model 2
shows a bias the ranges hide.

## Three things every loop must declare

- **Whether each candidate's effect is exact.** Decide it per change, not
  per kind. A rollup that covers a query's grain answers it, every time: the
  prediction is exact. A docs edit changes what an agent does: it is not, and
  needs a trial. One loop often has both.
- **A stand-in metric, when the real one has too little data.** If the real
  metric cannot beat its noise for weeks, name the stand-in, and the
  condition that switches to the real one. Until then a person accepts on
  judgment, and the loop says so.
- **A control, and a version for the metric.** Other things change while a
  change is measured (data, traffic, other changes). Compare against a
  control, or check only the slice the change targets (the queries a table
  was meant to answer). When the metric's definition changes, give it a new
  version, so history before and after is never compared as one.

## Five more things a loop may need

- **A map from each change to the items it targets.** What you change (a
  table, a doc, a page) and what you measure (a query, a question, a prompt)
  are often different things, many to many. Write the map down at step 1: a
  rollup targets the queries its grain covers; a doc edit targets the cases
  diagnosed to it. Acceptance and Learn read only the targeted items.
- **Trials that cannot be undone.** When every real trial is public or costly
  to reverse, there is no small private copy for step 4's third check. Lean on
  the cheaper checks, keep each change small, and say in step 5 which options
  can be taken back.
- **Learn on a schedule.** When the outcome arrives days or weeks after the
  change, step 7 runs when the outcome is due, not right after step 6. Record
  the date each prediction can be checked.
- **A measuring instrument nobody tunes.** The questions, queries or prompts
  that measure the metric are frozen within a metric version. Rewording one to
  suit a change scores a win that is not one, the same as reading the answer
  key.
- **Limits that are not numbers.** Some limits are pass or fail: a test suite,
  a style rule, a review by a person. Step 6 checks them like any other limit,
  and an option that fails one is not offered.

## Rules that hold for every kind

- **Never land your own change.** The loop proposes; a person lands it.
- **A number without its noise is not a result.** Measure the noise (a run
  against itself) before claiming a change beat it.
- **Keep the answer key away from the thing measured.** A metric the
  optimizer can read the answer to measures nothing.
- **Every assumption is written down and open to question**: prices,
  estimates, which items count.
