<!-- Measuring and improving query speed in the same loop as correctness: the query replay, the latency band, speed findings, and accepting a materialization edit. Read before timing a model or adding a persist. -->

# Query speed in the loop

Speed is improved in the same loop as correctness, against the same set. Every
passing case already holds the query a judge accepted. Re-running those
queries gives a fixed workload whose right answers are known, so a speed edit
can be timed and also proved to have changed no answer.

## 1. Time the passing queries

```bash
python3 skills/eval-loop/scripts/eval.py query-replay --set <set> \
  --from baseline-01 --label perf-01
```

`--from` names an answer run. Each case it scored a pass is replayed with the
query its answer rested on (`final_query`, `final_givens`, `final_model_path`),
over REST, once as a warm-up and then `repeats` times. No agent and no judge
run, so it costs server time only. Per query it records:

- the median and max of the server's `executionTimeMs`;
- `servedFrom` and `queryCostBytes`;
- the source the query reads (`run: <source> -> …`);
- a hash of the rows, which ignores row order and float noise in the last
  digits.

`--from` can also name an earlier query replay, which re-runs exactly that
replay's queries. Use that for every later replay, so two replays time the same
work even after the answer run has been superseded: fresh answers word their
queries differently, and a replay of a new run's queries compared only 5 of 18
with the record it followed. `eval.py routine --kind speed` replays the last
record replay's queries for this reason, and `--baseline record` names that
replay. A replay in which every query errored is marked `aborted`: it timed
nothing, so it is never the record and is not shared.

`--target-version 1.1.0` pins a published version on every request. REST can pin
a version, unlike the MCP tools, so nothing has to be promoted to `latest`.
`--slice` and `--tag` work as on every other run.

`run.json` records the package's `materialization` state at the start: the
persist sources the build plan makes and refuses, whether their tables are
bound, and the newest build. **A reload unbinds them.** After any edit,
`eval.py publish` publishes the package and runs the build, and waits for it;
time nothing before that, or every persisted source computes live. The replay
warns when a persisted source has no bound table.

The same state carries what each built table cost, as `builds`: its rows, the
time that one table took to build, and the warehouse bytes the build scanned.
That is the other side of a speed edit. A persisted source makes reads faster
and costs a build on every refresh, so a replay prints the build cost under the
query times, with the baseline's beside it, and `summary.json` holds it as
`build`. Read the two together: 3 seconds saved across the stored queries for
a 40 second build each night is a trade, not a win. A measure the server did
not report is null, never zero: a table that was reused was not built again,
and DuckDB reports no bytes. The server reports no size on disk, so rows is
the only size, and a table built beside its source in local DuckDB reports no
rows either: measured, such a build reported its 4 ms and nothing else.

`servedFrom` reports only the `storage=` tier. A query answered from a table
built in the source's own warehouse reads null, the same as a live one, so use
the binding in `run.json`, not `servedFrom`, to say a colocated persist was in
force.

### Time what people ran

The stored queries are what the eval set asks. What costs money is what people
run, and how often. `--queries <file>` replays a workload file in place of
`--from`: one JSON object per line.

```json
{"query": "run: order_items -> by_category", "count": 412}
{"query": "run: order_items -> { group_by: status; aggregate: total_sales }", "count": 3, "modelPath": "storefront.malloy"}
```

`query` is the Malloy. `count` is how many times it ran (default 1), and
`modelPath`, `givens` and `qid` are optional. The same query on two lines is
one query whose counts add. A query with no `qid` is named by a hash of its
text, givens and model, so it has the same name in tomorrow's file and two
days' replays compare query by query. A host's logs are the usual source; a
host may ship a skill that writes the file.

Every replay reports `weighted_ms` beside `total_ms`: each query's time by its
count, in total and per source. That is the number to rank on. A 90 ms query
that ran once costs less than a 10 ms query that ran forty times.
`eval.py routine --kind speed --queries <file>` runs the whole speed routine
over the file.

## 2. What counts as slow

Set it in `eval.toml`. It is not part of the set, so it does not move
`datasetSha`:

```toml
[performance]
budget_ms    = 2000   # a query slower than this is marked slow; unset, nothing is
repeats      = 5      # timed calls per query, after one warm-up
latency_band = 0.2    # a change under 20% is noise
```

Pick `budget_ms` for the warehouse and the people waiting on it. A team that has
not decided leaves it unset, and the replay only reports times.

## 3. Compare two replays

```bash
python3 skills/eval-loop/scripts/eval.py query-replay --set <set> \
  --from perf-01 --baseline perf-01 --label perf-02
# or, for any two replays already on disk:
python3 skills/eval-loop/scripts/flip_table.py --a <runs>/perf-01 --b <runs>/perf-02
```

Rows first. A query whose rows changed, or that ran before and now errors,
**blocks**: the exit status is 1, and each one goes to the judge against its
golden before anything is accepted. A query whose rows already moved between
calls of one replay is marked unstable (a tie under a limit, say), and a
changed hash on it is listed but does not block.

Then speed, only over queries whose rows held. A query is faster or slower when
its median moved by more than the band AND by more than `--min-delta-ms`
(default 10 ms). Below that floor, a 2 ms query doubling is jitter.

**Measure the band before trusting a small speedup.** Two replays of one
version, with nothing changed, show how far timings move on their own. The same
rule as an A/A for verdicts (`measurement.md`). Shared warehouses vary more than
a laptop.

`eval.py versions` shows each version's most recent replay under its score: the
median and total query time, and against the previous version's replay, how many
queries got faster or slower and how many changed rows.

## 4. Diagnose

`skill:eval-diagnose` reads a query replay as well as an answer run. Its
`performance` component turns slow queries into findings, grouped by the source
they read, so one materialization fixes one group.

With no `budget_ms` nothing is slow, and one finding is still made:
`HOT-SOURCE` names the unpersisted source that takes at least 30% of the
replay's time, counted by how often each query ran. It is read off the replay,
before anything is built, and it is where to look first.

## 5. Improve, and accept

The edit is a `#@ persist` or `#@ preaggregate` on the source a group reads, made
with the `malloy-materialization` skill. `skill:eval-improve` proposes it and
**the user approves each one before it is made**: a persisted table costs
storage and rebuild time, and that trade is theirs.

Acceptance for a speed edit (`acceptance-check.md`):

1. Compile, save, `eval.py publish` (or `eval.py experiment start <name>`), and
   confirm the build it reports made the source and did not refuse it.
2. Query replay, `--from` the stored replay, over **every** stored query that
   reads the changed source, not only the slow ones. Every row stays the same.
3. The targeted queries are faster than the band. No other query is slower than
   the band.

A persist or a preaggregate changes no name and no doc, so what the agent can
find is unchanged, and no fresh answer run is needed. An edit that also renames
a field or changes a doc is two edits: make them separately, and the second
takes the ordinary acceptance check.

To read it, or hand it to someone, package the replays, baseline first:
`eval.py package --set <set> --run <runs>/perf-01 --run <runs>/perf-02`. That
builds `speed-perf-02` on the truth server: a report of the timings per replay
and per source, and an app with each query's time before and after, any query
whose rows changed listed first.

### Unattended

`eval.py routine --kind speed --mode improve` does all of section 5 with
nobody watching, for the finding whose source costs most (`--max-edits` of
them). `PERSIST-NOT-BUILT` and `LIVE-FALLBACK` are skipped: they need a build
or a look at the server, not an edit.

1. One agent adds the `#@ persist` or `#@ preaggregate`. It is told nobody
   will answer, so it does not wait for approval.
2. The edit is committed on a new branch, `eval/<label>`, then served and
   built as an experiment beside the main package.
3. The night's own replay is replayed against it twice.
4. It is accepted only if both replays say the same: no stored answer's rows
   changed, none now errors, no query is slower, at least one is faster, and
   the total is faster by more than the band. Two replays, because one
   replay's total moves by itself.
5. An accepted edit stays on the branch, with the time saved and the build it
   costs on each refresh in its commit message. A rejected one is taken off.
   Either way the attempt is a line in `attempts.jsonl` on that branch.

The approval the attended flow asks for up front still happens. It is the
merge: nothing is published, and the commit message puts the two numbers a
person needs side by side.

Checkpoint as for any edit: a commit naming the replay's run id. Publishing it
as a new package version puts the speedup in `eval.py versions`.

## What this does not measure

- How long the agent's own queries took inside an answer run. The MCP
  `execute_query` reply carries no timing. The server's access record does
  (`durationMs`, `executionTimeMs`, `servedFrom`), keyed by the caller header,
  but nothing reads it yet. The replay times the same queries with less noise.
- Warehouse cost in money. `queryCostBytes` is recorded where the backend reports
  it, and null is not zero.
- The agent's turn time. `wall_seconds` on each attempt has it, and it is
  dominated by the model, not the warehouse.
