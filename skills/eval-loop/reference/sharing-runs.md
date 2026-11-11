<!-- Where runs live, why not in git, and how to share them with a team. Read before keeping or publishing a run. -->

# Sharing runs

The set lives in git, beside the package. Runs never do.

A run is 100 to 300 KB per case, mostly answerer transcripts. A repository that
keeps runs grows with every arm, and nothing in it reads them back. So:

- **On your machine**, a run stays in the set's workdir
  (`~/.malloy-eval/<set>-<hash>/runs/<label>/` unless `eval.toml` sets
  `[paths] workdir`). `eval.py runs --set <set>` lists them, and
  `eval.py versions --set <set>` shows how each package version scored.
- **In git**, a checkpoint commit names the run's `runId` in its message, and
  `RESULTS.md` (`skill:eval-report`) names the runs it reports. Neither copies
  the run.
- **For a team**, export the run and load it into storage everyone can query.

## Export

```bash
python3 skills/eval-loop/scripts/eval.py export --set <set> --label baseline-01
```

This writes six CSV files and a `manifest.json` into `<run>/export/`. Every
row carries `run_id`.

| table | one row per | holds |
|---|---|---|
| `eval_runs` | run | the pins (package version, set version and sha, slice, judge, models) and the headline score |
| `eval_attempts` | attempt | verdict, `golden_revision`, `question_sha`, tags, split, cost, retrieval scores for a replay |
| `eval_events` | ledger event | the event itself, as JSON text |
| `eval_transcripts` | attempt | the answerer transcript, the answer, and the judge's write-up, as text |
| `eval_cases` | case | the set as it was run, keyed by `dataset_sha`: question, tags, split, golden |
| `eval_query_timings` | query, in a query replay | median and max time, where it was served from, the rows' hash; empty for any other run |

`manifest.json` names each file and the table name to load it under
(`eval_attempts__<run id>`). The export refuses a set whose `datasetSha` differs
from the run's, because `eval_cases` must hold the cases the verdicts were
about: check out the set's commit first.

CSV, not JSON lines: a loader infers each file's column types on its own, and
from JSON lines a column that is null in every row of one run reads as JSON,
which then refuses to union with the same column holding values from another
run. From CSV it reads as VARCHAR, which unions with anything.

## Load it where the agent under test cannot read it

**The export holds the answer key.** `eval_cases` carries every golden, and a
transcript carries an answer. An answerer that can query them is contaminated.
On a host where a database is a connection every model in its environment can
read, that means an environment of its own, holding nothing the agent under
test is scoped to.

## Read many runs as one table

Until the loader can append to an existing table, each run loads as its own
tables. Read them together by naming them, matching columns by name and casting
what you compute on. In DuckDB:

```sql
SELECT run_id, qid, verdict, TRY_CAST(passed AS BOOLEAN) AS passed
FROM query_table(['eval_attempts__baseline_01', 'eval_attempts__baseline_02'], true)
```

`query_table` takes a literal list. It cannot be given a query that finds the
tables, so the list is written out, and it grows by one name per run.

A host may ship a skill that does the upload for its own storage: look for one
before writing the commands by hand.
