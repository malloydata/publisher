<!-- Where runs live, why not in git, how a team shares them through a store, and how to load them into a warehouse. Read before keeping or publishing a run. -->

# Sharing runs

The set lives in git, beside the package. Runs never do.

A run is 100 to 300 KB per case, mostly answerer transcripts. A repository that
keeps runs grows with every arm, and nothing in it reads them back. So:

- **On your machine**, a run stays in the set's workdir
  (`~/.malloy-eval/<set>-<hash>/runs/<label>/` unless `eval.toml` sets
  `[paths] workdir`). `eval.py runs --set <set>` lists them,
  `eval.py versions --set <set>` shows how each package version scored, and
  `eval.py history --set <set>` builds both as a page.
- **For a team**, a store: a place every finished record goes, and every
  experiment can be compared against (below).
- **In git**, a checkpoint commit names the run's id in its message
  (`eval.py accept` prints it), and `RESULTS.md` (`skill:eval-report`) names the
  runs it reports. Neither copies the run.

## The store

One section of `eval.toml`. Leave it out and nothing is shared.

```toml
[store]
path = "/Volumes/team/evals"     # a shared drive, a synced folder, a mounted bucket
```

or, for anything a folder cannot reach, three commands the shell runs, with
`{run}`, `{run_id}`, `{dest}` and `{set}` filled in:

```toml
[store]
push = "aws s3 sync {run} s3://team-evals/{set}/runs/{run_id}"
pull = "aws s3 sync s3://team-evals/{set}/runs/{run_id} {dest}"
list = "aws s3 ls s3://team-evals/{set}/runs/ | awk '{{print $2}}' | tr -d /"
```

A brace that is part of the command, like awk's, is doubled, because single
braces mark the four placeholders. `list` prints one run id per line. Without
it the store cannot say what it holds: `pull` with no `--label`, `runs --store`
and `--baseline record` say so and use only the runs already here.

```bash
eval.py push --set <set> --label tip-01     # share one run
eval.py pull --set <set>                    # every shared run missing here
eval.py runs --set <set> --store            # what the store holds
```

`eval.py runs`, `versions` and `history` pull what the store holds first, so
they list the team's runs and not only the ones made here.

What it stores is the run directory itself: `run.json`, `events.jsonl`, the
transcripts. Pages and tables are built from it, so nothing is lost on the way
and a pulled run is the run, byte for byte.

The rules, whichever store it is:

- **A run is shared whole, once, and never changes after.** Pushing a run the
  store already holds with the same content does nothing; with other content it
  is refused. Two runs are two directories, so nothing ever appends to
  anything, and two people pushing at once cannot collide.
- **Records share themselves.** With a store set, a full answer run of the main
  line, and a query replay of it, push when they finish. A slice, an experiment
  or a partial run stays local unless run with `--share` or pushed by hand. An
  experiment is yours; a record is the team's.
- **`--baseline record` reaches the store.** It pulls whatever the store holds
  that you do not, then takes the newest full main-line run, so an experiment is
  compared against the team's record, not against whatever happens to be on
  your disk.
- **Never in the model's repository.** `check` refuses a store inside it.

### The store holds the answer key

The verdicts carry the goldens and the transcripts carry answers, so the agent
under test must never be able to read the store. On a host where a database is
a connection every model in its environment can read, that means an
environment of its own, holding nothing the agent under test is scoped to. A
run package or the history page registers on the truth server for the same
reason.

## Tables for a warehouse

To query runs in a warehouse of your own:

```bash
python3 skills/eval-loop/scripts/eval.py export --set <set> --label baseline-01
```

This writes seven CSV files and a `manifest.json` into `<run>/export/`. Every
row carries `run_id`.

| table | one row per | holds |
|---|---|---|
| `eval_runs` | run | the pins (package version, set version and sha, slice, judge, models) and the headline score |
| `eval_attempts` | attempt | verdict, `golden_revision`, `question_sha`, tags, split, cost, retrieval scores for a replay |
| `eval_events` | ledger event | the event itself, as JSON text: the line as `events.jsonl` holds it |
| `eval_transcripts` | attempt | the answerer transcript, the answer, and the judge's write-up, as text |
| `eval_cases` | case | the set as it was run, keyed by `dataset_sha`: question, tags, split, golden |
| `eval_query_timings` | query, in a query replay | median and max time, where it was served from, the rows' hash; empty for any other run |
| `eval_files` | other file of the run's record | `path` and `text`: a replay's `summary.json` and `results.jsonl`, the diagnosis, the coverage report, the model snapshot |

`manifest.json` names each file and the table name to load it under
(`eval_attempts__<run id>`). The export refuses a set whose `datasetSha` differs
from the run's, because `eval_cases` must hold the cases the verdicts were
about: check out the set's commit first.

CSV, not JSON lines, because a loader infers each file's column types on its
own: from JSON lines, a column null in every row of one run reads as JSON and
then refuses to union with the same column from another run. From CSV it reads
as text, which unions with anything. The run itself stays JSON lines; CSV is
only how it is loaded.

A loader that cannot append loads each run as its own tables. Read them
together by naming them. In DuckDB:

```sql
SELECT run_id, qid, verdict, TRY_CAST(passed AS BOOLEAN) AS passed
FROM query_table(['eval_attempts__baseline_01', 'eval_attempts__baseline_02'], true)
```

A host may ship a skill that does the upload for its own storage: look for one
before writing the commands by hand.

### The summary, for a place the agent under test can read

`eval.py export --summary --set <set> --label <run>` writes one file,
`eval_summary.csv`, into `<run>/export-summary/`: the run's row of `eval_runs`
without `run_json` and the slice's case ids, plus the pass rate on each split,
the diagnosis cost and the weighted query time. It names no case and holds no
question, golden, answer or query, so it is the one export that may be loaded
where the answerer can read, such as a database in the package's own
environment, when no separate one exists. It keeps the history of the number.
It cannot be imported back into a run.

A database table cannot have rows added to it, so for a trend over many runs
write every run's summary into one file and load it over the last one:

```bash
eval.py export --summary --all --set <set>     # <workdir>/export-summary/eval_summary.csv
```

It holds one row per run in the workdir, oldest first, under the table name
`eval_summary`. A model or report over that one table shows the number over
time, and it may live in the same environment as the package it measures.

## Reading a run back from the tables

The tables are also the run. `eval_runs.run_json` is `run.json` as written,
`eval_events.event` is each line of `events.jsonl`, and `eval_files.text` is
every other record file, so a run that lives only in a warehouse can be
rebuilt where it is needed:

```bash
eval.py import-run --set <set> --from <dir>
```

`<dir>` holds the tables of one run: the CSV files export wrote, or what a
query for them returned, as `<table>.json` (a list of row objects) or
`<table>.jsonl`. Select every column, and select `run_json`, `event` and
`text` as text: a cast or a re-format changes the bytes. `eval_runs` and
`eval_events` are needed. `eval_files` brings back a replay's results and a
run's diagnosis, and `eval_transcripts` each case's transcript, answer and
judge write-up. When the files hold rows of several runs, name one with
`--run-id`.

The run is written to `<workdir>/runs/<run id>`, where `accept`, `versions`,
`history` and `--baseline record` read. `run.json` and `events.jsonl` come
back byte for byte, measured through DuckDB 1.5.1 (export, load, select, write
as JSON, import), so a store that already holds the run sees the same run. A
replay's `rows/` and a case's other artifacts are not carried. A run already
here with the same record is left alone, and a different run under the same
name is refused.

This is how tonight's run is compared with last night's when last night's
lives in a warehouse: import the newest record, then `--baseline record`.
