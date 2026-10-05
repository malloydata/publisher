---
name: malloy-gx
description: Bridges Malloy (a semantic-layer query language, typically served by Malloy Publisher) and Great Expectations (a Python data-quality/validation framework, aka "GX"). Use this whenever the user is working with Malloy models or queries AND mentions data quality, validation, testing data, "expectations", or Great Expectations/GX by name -- e.g. "add data quality checks to this Malloy query", "can we validate this against GX", "generate expectations for this source", "make sure this metric doesn't silently break". Also use it for general-purpose help writing or maintaining a Great Expectations expectation suite even when Malloy isn't involved, since the same scripts and reference material apply. Covers three things: (1) profiling a Malloy query result and generating a starter GX expectation suite from it, (2) running a Malloy query and validating the result against a saved GX suite as a pass/fail gate, and (3) authoring/editing GX expectations by hand with the right `gxe.*` classes.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy + Great Expectations

Malloy models describe *what the data means* (sources, joins, measures);
Great Expectations checks *whether the data is still what you expect*. This
skill connects the two: run a Malloy query, profile or validate its result
with GX. It does not require any specific Malloy Publisher clone -- it talks
to whatever Publisher server (or MCP tools) the user already has running,
and the GX side is a plain Python `great_expectations` install.

## Prerequisites

- `pip install great_expectations pandas` in whatever environment runs the
  scripts below (check with `python3 -c "import great_expectations"` first;
  don't assume and don't reinstall if it's already there).
- A Malloy Publisher server reachable at some host:port (default
  `http://localhost:4000`), OR the Malloy MCP tools (`execute_query`,
  `get_context`, etc.) already connected in this session.
- To validate against a *saved* suite, that suite must already exist (from
  capability 1, or hand-written). Suites live in a GX file-context directory
  (`--gx-dir`, default `./gx_project`) -- pass the same `--gx-dir` every time
  so the suite created by capability 1 is the one capability 2 loads.

**Getting a Malloy query result.** Two ways, pick based on what's available
in the session:

- **MCP tools connected** (check `/mcp` or just try `execute_query`):
  prefer these directly. They give grounded discovery (`get_context`) for
  free, which matters when you don't already know the exact source/view
  names. Once you have the JSON rows, load them into a DataFrame yourself
  (`pd.DataFrame(rows)`) to hand to the GX scripts below.
- **No MCP tools** (headless run, CI, or a session without the Malloy MCP
  server connected): use `scripts/malloy_client.py`, which POSTs to
  Publisher's REST API (`.../models/{path}/query`) and returns rows the same
  way. `generate_expectations.py` and `validate_query.py` both call into it
  directly when you pass `--env/--package/--model/--query`, so most of the
  time you won't invoke it standalone -- just pass those same flags through.

If neither is available, say so and ask whether to start the Publisher
server (`docs/ai-agents.md` / `AGENTS.md` in a Publisher clone covers
startup) rather than guessing at a host/port.

## Capability 1: generate a starter suite from a Malloy query

Use when the user wants data-quality checks for a Malloy source or query and
doesn't already have a suite. This profiles the *actual result* of running
the query (types, null rates, observed ranges, low-cardinality value sets)
and writes a GX expectation suite from it -- see
`reference/malloy_type_mapping.md` for exactly which heuristic produces
which expectation, and why each one is intentionally loose rather than
exact-match.

```bash
python3 scripts/generate_expectations.py \
  --env examples --package storefront --model storefront.malloy \
  --query "run: order_items -> by_category" \
  --suite-name order_items_by_category \
  --gx-dir ./gx_project
```

(or `--csv path.csv` instead of the `--env/--package/--model/--query`
group, if the rows are already saved.)

**Always show the user what got generated** (the script prints a summary)
and walk through `reference/malloy_type_mapping.md`'s override table with
them -- a freshly profiled suite is a draft, not a finished contract. In
particular: point out any `ExpectColumnValuesToBeBetween` on a measure that
looks like it grows monotonically (running totals, lifetime counts), and
any categorical `ExpectColumnValuesToBeInSet` whose value set is likely to
grow (new product categories, new region codes). Those are the two most
common "true today, false in three weeks" traps. Loosen or remove them
based on what the user knows about the field, don't leave it to guesswork.

## Capability 2: validate a Malloy query against a saved suite

Use as a data-quality gate: run a query, check the result against a suite
that already exists (from capability 1 or hand-authored), fail loudly if
something's off.

```bash
python3 scripts/validate_query.py \
  --env examples --package storefront --model storefront.malloy \
  --query "run: order_items -> by_category" \
  --suite-name order_items_by_category \
  --gx-dir ./gx_project
```

Exit codes: `0` all expectations passed, `1` at least one failed (data
problem), `2` setup problem (suite/server not found -- distinguish this
from an actual failure when reporting back to the user, they need different
responses: `1` means look at the data, `2` means fix the command). This
makes it a natural fit for a CI step or a pre-deploy check; if the user
wants it wired into a pipeline, that's just calling this script as a step
and checking the exit code -- no extra plumbing needed.

## Capability 3: general GX authoring help

For anything that isn't "generate from a Malloy query" or "validate a Malloy
query" -- hand-writing or editing expectations, explaining what an
expectation does, choosing which one fits a check the user is describing in
plain English -- work directly with the `great_expectations` Python API
rather than reaching for the scripts above (they're specifically about the
Malloy round-trip).

The modern (v1) fluent API shape:

```python
import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", project_root_dir="./gx_project")

batch = context.data_sources.pandas_default.read_dataframe(df)
# or: .read_csv(path_or_url)

suite = gx.ExpectationSuite(name="my_suite")
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6)
)
context.suites.add(suite)  # persist for later reuse

results = batch.validate(suite)
print(results.describe())
```

Common expectation classes worth knowing by name when translating a plain-
English request (`gxe.<Name>`, all under `great_expectations.expectations`):

| The user says... | Reach for |
|---|---|
| "this column shouldn't have nulls" | `ExpectColumnValuesToNotBeNull` |
| "this column should always have a value between X and Y" | `ExpectColumnValuesToBeBetween` |
| "this should only ever be one of these values" | `ExpectColumnValuesToBeInSet` |
| "this should be unique / no duplicates" | `ExpectColumnValuesToBeUnique` |
| "this column needs to exist" | `ExpectColumnToExist` |
| "the table should have roughly this many rows" | `ExpectTableRowCountToBeBetween` |
| "this should match a pattern" (email, phone, SKU format) | `ExpectColumnValuesToMatchRegex` |
| "this should be a specific type" | `ExpectColumnValuesToBeOfType` |
| "this string column's length should be in a range" | `ExpectColumnValueLengthsToBeBetween` |
| "compare against yesterday's/last run's result" | not a single-batch expectation -- needs two batches or a stored baseline; say so rather than forcing a single-batch expectation to do it |

When in doubt about an expectation's exact parameters, check the docstring
in the installed package (`python3 -c "import great_expectations.expectations as gxe; help(gxe.ExpectColumnValuesToBeBetween)"`)
rather than guessing -- the fluent API has changed shape across GX versions
and guessing risks a parameter name that silently does nothing.

## Files in this skill

- `scripts/malloy_client.py` -- REST client for running a Malloy query
  against Publisher and getting a DataFrame back. Used internally by the
  other two scripts; call it directly only when you specifically need rows
  without profiling or validating them.
- `scripts/generate_expectations.py` -- capability 1.
- `scripts/validate_query.py` -- capability 2.
- `reference/malloy_type_mapping.md` -- the profiling heuristics in
  detail, the override table, and Malloy-type-to-pandas-dtype notes. Read
  this before explaining a generated suite to the user, not after.
