# Eval-run package

A finished run as a Malloy package you can open: the semantic model in
`eval_run.malloy`, a notebook in `eval_run.malloynb`, an HTML data app in
`public/`, and CSV under `data/` written by `../../scripts/build_run_package.py`.

```bash
python skills/eval-loop/scripts/build_run_package.py \
  --run results/2026-08-30-sonnet \
  --run results/2026-08-30-opus \
  --set evals/ecommerce \
  --out target/eval-run
```

Two `--run` flags build both arms into one package, which is what makes an A/B a
`group_by` rather than a diff of two reports.

## Why a package and not a report

`events.jsonl` is right for appending and wrong for reading. The questions people
actually ask of a run -- which cases moved, which entity is missed most often,
whether the derivable cases are dragging the average -- are group-bys, and writing
each one as a bespoke script is how a previous evaluation effort here ended up
complex, stale and unread.

It is also a dogfood with teeth. This is a semantic model, documented to the
standard we ask of a customer's, read by the same agents. If an agent cannot
answer "which cases regressed and why" against it, that is a finding about the
product, found on data we control and understand.

## Notebook or app?

Both, and the split is not stylistic.

**`eval_run.malloynb`** holds the analytical tables: pass rate, cost, effort,
where the failures are, retrieval, the backlog. Publisher renders it natively, so
these are Malloy reading the model directly with no JavaScript in between and
nothing to drift.

**`public/index.html`** is the case list -- one expandable row per case, with a
verdict pill and a row of dots per arm (the entities the golden depends on: a
ranked entity, a sibling-source alias, in a returned source's docs, or missing).
Opening a row shows the golden as a table, then per arm: the effort line (turns,
calls, errors, seconds, dollars), the judge's reasoning, the re-executed rows,
and the attempt as a **timeline** -- every `get_context` and `execute_query` with
its input and result, and the prose between them. That is the view a verdict
cannot give: whether a wrong number came out of a wrong query or a right query
read wrongly. A notebook cannot do that, and drilling into one case is most of
what reading an eval consists of. Data comes from `steps.csv`, `required.csv`
and the `prediction` column on attempts, all built from the run's transcripts.

Both read the same model, which is what stops the two from disagreeing.

## The tables

| Table | Grain |
|---|---|
| `runs` | one per arm |
| `cases` | one per question in the set |
| `attempts` | one per (run, case, sample) |
| `scores` | the judge's verdict, one-to-one with attempts |
| `retrieval` | recall, precision and where-to-fix, one-to-one with attempts |
| `calls` | **many** per attempt: one per MCP call, with the search terms sent |
| `entities` | **many** per attempt: one per (attempt, entity, role) |
| `clusters` / `cluster_members` | failures grouped by root cause |
| `issues` | what diagnosis recorded |

The four attempt-grain tables share an `attempt_key` (`run|qid|sample|phase`) and
declare it as a primary key. Joining on the parts instead works right up until a
case is sampled twice, at which point it fans out and inflates every count
without erroring.

`entities` and `calls` are long rather than wide because the interesting questions
are set questions -- which entity is missed most, across which cases -- and those
are a `group_by` in this shape and a parse in any other. They are also the tables
that will fan out a total if you join and sum through them.

CSV rather than parquet: `duckdb.table()` reads it natively, the volumes are
trivial, and the builder stays dependency-free.

## Two recall numbers, never one

`retrieval_as_used` measures recall using the search terms **the answerer chose**.
It moves when the answerer changes: a stronger model searches better and scores
higher retrieval recall with retrieval untouched. Use it to attribute a failure
*within* one arm. Do not put it in an A/B.

Comparing retrieval itself across engine versions is not this package's job; a customer run reports per-question entity recall only.

## Every failure is placed

`where_to_fix` is one of *query construction*, *retrieval ranking*, *model
coverage* or *refusal behaviour*, and every scored failure has exactly one. The
counts in `failures_by_where_to_fix` therefore sum to the failure count in
`run_summary`. If they ever do not, attribution has a hole -- that exact bug is why
the tables are cross-checked rather than trusted.

`needs_human` is neither a pass nor a failure and is attributed to nothing.

## Serving it

```bash
publisher --server_root <parent-of-package> --mcp_port 4049
# app      http://localhost:4000/environments/evals/packages/eval-run/index.html
# notebook http://localhost:4000/evals/eval-run/eval_run.malloynb
```

The two URLs are different on purpose and neither is guessable. `public/` is
web-served under `/environments/<env>/packages/<pkg>/<file>`, where `<file>` is
relative to `public/` and does not include it. Notebooks and models are **not**
served there at all; they open in the web UI at `/<env>/<pkg>/<file>`.

`/<env>/<pkg>/public/index.html` also works, but only as a 302 to the canonical
`/environments/...` form -- worth knowing when a client that does not follow
redirects reports a 302 as a failure.

To check a package is really being served, rather than trusting a 200 on a static
file:

```bash
curl -s .../api/v0/environments/<env>/packages/<pkg>/models      # compiles the model
curl -s .../api/v0/environments/<env>/packages/<pkg>/notebooks   # lists .malloynb
```

`models` returns a 424 with the full compiler error when the Malloy is broken,
which is the fastest way to see a compile failure. Notebooks are listed by
`notebooks` and are **not** in `models`; asking for one under `models` returns
`404 "<file> is a notebook"`. A notebook fetched from `notebooks/<file>` reports
its cells' compiled schemas under `modelInfo`, but only the cells it treats as
anonymous queries -- do not read a low count there as cells failing to compile. To
verify every cell, run each named query through `malloy_executeQuery`.

Pass `--mcp_port` explicitly. It defaults to 4040 and any other local Publisher
already holds it; the server logs `Port 4040 in use` and carries on without an MCP
endpoint, so the failure is quiet and only shows up later as a tool that isn't
there. `PORT` / `--port` was ignored in the version tested here -- it bound 4000
regardless -- so check the `PUBLISHER_READY` line for the port it actually took.

Two more Publisher behaviours worth knowing, because both look like your edit did
nothing.

It serves a **snapshot copy** under `publisher_data/<env>/<pkg>/`. The `location`
in `publisher.config.json` is a one-time import source, not a live mount, and
`malloy_reloadPackage` does **not** re-fetch from it -- it answers
`mode: in-place`, re-reading the snapshot. So the working recipe is sync then
reload:

```bash
rsync -a --delete <location>/ "$SERVER_ROOT/publisher_data/<env>/<pkg>/"
# then malloy_reloadPackage {environmentName, packageName}
```

Sync the whole package. A new model over stale CSVs fails with a column-not-found
(`` `attempt_key` not found ``) that reads like a model bug rather than a stale
copy. Watch `sourceContentSha` across the reload to confirm it saw the change.
Restarting works too, and is the only option if you would rather not reason about
which half is stale.

And a package must be declared in `publisher.config.json` with an `environments`
block -- a bare `{"frozenConfig": false}` starts cleanly and serves nothing, logging
`packages=0`.

Start it as a persistent process. Backgrounded with `&` inside a shell that then
exits, it dies with the parent and the port simply stops answering.

## Editing the model

The compiler is stricter than it looks, in four places that cost time here:

- Top-level named queries are `query:`, not `view:` (`view:` belongs inside a
  source).
- `!= null` must be `is not null`.
- A passthrough column cannot be redefined as `dimension: x is x`. Document it
  with `#(doc)` above `public: x` in the `include {}` block instead.
- A source must be **defined before** any source that joins it. `retrieval` sits
  above `scores` for that reason, not by accident.

## Querying it from JavaScript

`Publisher.query(modelPath, malloyQuery, opts)` takes **positional** arguments and
returns plain row objects, nesting included -- it sets `compactJson`, so none of
Malloy's cell-typed envelope reaches the app. Passing an options object as the
first argument puts `[object Object]` in the URL's model segment and 404s.
