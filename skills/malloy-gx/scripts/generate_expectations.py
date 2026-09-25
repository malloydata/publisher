#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

"""Profile a Malloy query result (or any CSV) and write a starter Great
Expectations suite from what the data actually looks like.

This does NOT try to read Malloy's `.malloy` source syntax and translate
`dimension:`/`measure:` declarations into expectations -- Malloy doesn't
carry column types in the source text (a dimension's type comes from its
expression and the underlying table), so the reliable source of truth is
the query result itself. Run the query, look at the real values, and build
expectations from those. See references/malloy_type_mapping.md for the
heuristics used below and why each one is deliberately conservative.

The resulting suite is a starting point, not a finished contract. Read
what it produced with `--out`, delete or loosen any expectation that
encodes a coincidence of today's sample rather than a real invariant
(e.g. a `ExpectColumnValuesToBeBetween` on a metric that legitimately
grows over time), and tighten anything that matters more than the
heuristic knew to check (e.g. a foreign key that should never be null).

Examples:
    # From a live Malloy query (reads via malloy_client.py)
    python generate_expectations.py \\
        --env examples --package storefront --model storefront.malloy \\
        --query "run: order_items -> by_category" \\
        --suite-name order_items_by_category \\
        --gx-dir ./gx_project

    # From a CSV you already have (e.g. saved by malloy_client.py --out)
    python generate_expectations.py --csv /tmp/by_category.csv \\
        --suite-name order_items_by_category --gx-dir ./gx_project
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from malloy_client import run_query  # noqa: E402

# Columns with <= this many distinct values (and low relative to row count)
# are treated as categorical and get an ExpectColumnValuesToBeInSet.
CATEGORICAL_MAX_DISTINCT = 20
CATEGORICAL_MAX_RATIO = 0.05

# Numeric ranges get padded by this fraction on each side so the suite
# doesn't fail the moment next week's data is a bit higher or lower than
# today's sample. See references/malloy_type_mapping.md for why a fixed
# fraction beats a fixed absolute margin across wildly different scales.
NUMERIC_RANGE_PAD = 0.10

# Row count gets a wider pad since day-to-day volume swings more than a
# metric's plausible range.
ROW_COUNT_PAD = 0.25

PK_NAME_HINTS = ("id", "_id", "key", "_key")


def looks_like_primary_key(column: str, series: pd.Series) -> bool:
    if series.isna().any():
        return False
    if series.nunique() != len(series):
        return False
    name = column.lower()
    return name == "id" or any(name.endswith(hint) for hint in PK_NAME_HINTS)


def build_suite(df: pd.DataFrame, suite_name: str):
    # `gx.get_context(...)` must already have been called before this runs --
    # ExpectationSuite.add_expectation() reaches for the active context as a
    # global singleton, and raises DataContextRequiredError if none exists yet.
    import great_expectations as gx
    import great_expectations.expectations as gxe

    suite = gx.ExpectationSuite(name=suite_name)
    row_count = len(df)

    if row_count > 0:
        low = int(row_count * (1 - ROW_COUNT_PAD))
        high = int(row_count * (1 + ROW_COUNT_PAD)) + 1
        suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=low, max_value=high))

    for column in df.columns:
        series = df[column]
        suite.add_expectation(gxe.ExpectColumnToExist(column=column))

        non_null_ratio = series.notna().mean() if row_count else 1.0
        if non_null_ratio == 1.0:
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=column))
        elif non_null_ratio > 0:
            # Some nulls are present and apparently normal for this column;
            # require no more than what we already observed, not perfection.
            mostly = round(max(non_null_ratio - 0.01, 0.0), 2)
            suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=column, mostly=mostly))

        if looks_like_primary_key(column, series):
            suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=column))

        if pd.api.types.is_numeric_dtype(series) and series.notna().any():
            observed_min = float(series.min())
            observed_max = float(series.max())
            span = observed_max - observed_min
            pad = span * NUMERIC_RANGE_PAD if span > 0 else max(abs(observed_max) * NUMERIC_RANGE_PAD, 1.0)
            suite.add_expectation(
                gxe.ExpectColumnValuesToBeBetween(
                    column=column,
                    min_value=observed_min - pad,
                    max_value=observed_max + pad,
                )
            )
        elif series.dtype == object or isinstance(series.dtype, pd.CategoricalDtype):
            distinct = series.dropna().unique()
            if row_count and 0 < len(distinct) <= CATEGORICAL_MAX_DISTINCT and len(distinct) / row_count <= CATEGORICAL_MAX_RATIO:
                suite.add_expectation(
                    gxe.ExpectColumnValuesToBeInSet(column=column, value_set=sorted(distinct.tolist(), key=str))
                )

    return suite


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", help="Path to a CSV to profile instead of running a live query")
    parser.add_argument("--host", default="http://localhost:4000", help="Publisher REST base URL")
    parser.add_argument("--env", dest="environment", help="Environment name (required unless --csv)")
    parser.add_argument("--package", help="Package name (required unless --csv)")
    parser.add_argument("--model", dest="model_path", help="Model path within the package (required unless --csv)")
    parser.add_argument("--query", help="Ad-hoc Malloy query text")
    parser.add_argument("--query-name", help="Name of a model-level query or a source's named view")
    parser.add_argument("--source-name", help="Source the --query-name view belongs to")
    parser.add_argument("--suite-name", required=True, help="Name to save the expectation suite under")
    parser.add_argument(
        "--gx-dir",
        default="./gx_project",
        help="Directory for the Great Expectations file context (created if missing). "
        "Pass the same path to validate_query.py to reuse this suite.",
    )
    args = parser.parse_args()

    if args.csv:
        df = pd.read_csv(args.csv)
    else:
        missing = [n for n in ("environment", "package", "model_path") if not getattr(args, n)]
        if missing or not (args.query or args.query_name):
            parser.error("either --csv, or --env/--package/--model plus --query or --query-name")
        df = run_query(
            args.host,
            args.environment,
            args.package,
            args.model_path,
            query=args.query,
            query_name=args.query_name,
            source_name=args.source_name,
        )

    if df.empty:
        print("warning: query/CSV returned zero rows; suite will still be created but every "
              "numeric-range and categorical expectation is skipped since there's nothing to "
              "profile", file=sys.stderr)

    import great_expectations as gx

    context = gx.get_context(mode="file", project_root_dir=args.gx_dir)
    suite = build_suite(df, args.suite_name)
    context.suites.add(suite)

    print(f"Profiled {len(df)} rows x {len(df.columns)} columns.")
    print(f"Saved suite '{args.suite_name}' with {len(suite.expectations)} expectations to {args.gx_dir}")
    for exp in suite.expectations:
        print(f"  - {exp.expectation_type}: {exp.column if hasattr(exp, 'column') else ''}".rstrip())


if __name__ == "__main__":
    main()
