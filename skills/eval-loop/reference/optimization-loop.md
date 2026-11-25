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
| 2 Ingest | The set (`eval-import`, questions from request logs); each run's verdicts, transcripts, tool calls, cost | A workload from query logs; a replay recording each query's time and path report (which table answered); build time, bytes, rows, freshness per table; prices |
| 3 Model | `eval-diagnose` clusters failures by cause and owner; `quality.malloy` and `eval history` hold the trend | A performance model: queries, executions, tables, prices; measures for weighted p95, cost per day, freshness lag |
| 4 Simulate | Weak: an agent's behaviour decides. Cheap checks: rerun only the failing cases plus a sample; check retrieval alone. Otherwise a full run on an experiment | Strong: the router is deterministic. Estimate which queries a table answers from each query's shape (grouped-by, filtered-by, measures) against the table's grain; dry-run publish for serve verdicts; build and replay the best few |
| 5 Strategy | Diagnosed causes as options, each with the cases it would fix and its risk | Options with latency, freshness and cost; which queries each speeds up |
| 6 Act | `accept`: beats the noise band twice, held-out pass rate does not drop; a branch a person merges | The same, plus: the targeted queries are answered by the new table (path report), rows unchanged, cost within the limit; a person publishes |
| 7 Learn | `attempts.jsonl`: each edit tried, its effect, a one-line lesson | The same file, plus each option's projection against the next day's measurements |

The two kinds are limits on each other. A speed change must not change
answers (rows unchanged, pass rate holds). A quality change must not make the
workload slower or stop a table from serving.

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

## Rules that hold for every kind

- **Never land your own change.** The loop proposes; a person lands it.
- **A number without its noise is not a result.** Measure the noise (a run
  against itself) before claiming a change beat it.
- **Keep the answer key away from the thing measured.** A metric the
  optimizer can read the answer to measures nothing.
- **Every assumption is written down and open to question**: prices,
  estimates, which items count.
