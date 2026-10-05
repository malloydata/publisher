#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

"""Run a Malloy query and validate the result against a saved Great
Expectations suite -- a data-quality gate for a Malloy query.

Exits 0 if every expectation passes, 1 if any fail (so this drops straight
into a CI step or a pre-deploy check), and 2 on a setup error (bad suite
name, unreachable server, etc.) so failures-of-the-check are distinguishable
from failures-of-the-data.

Example:
    python validate_query.py \\
        --env examples --package storefront --model storefront.malloy \\
        --query "run: order_items -> by_category" \\
        --suite-name order_items_by_category --gx-dir ./gx_project
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from malloy_client import run_query  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", help="Validate this CSV instead of running a live query")
    parser.add_argument("--host", default="http://localhost:4000", help="Publisher REST base URL")
    parser.add_argument("--env", dest="environment", help="Environment name (required unless --csv)")
    parser.add_argument("--package", help="Package name (required unless --csv)")
    parser.add_argument("--model", dest="model_path", help="Model path within the package (required unless --csv)")
    parser.add_argument("--query", help="Ad-hoc Malloy query text")
    parser.add_argument("--query-name", help="Name of a model-level query or a source's named view")
    parser.add_argument("--source-name", help="Source the --query-name view belongs to")
    parser.add_argument("--suite-name", required=True, help="Name of a suite previously saved with generate_expectations.py")
    parser.add_argument("--gx-dir", default="./gx_project", help="Directory holding the Great Expectations file context")
    args = parser.parse_args()

    if args.csv:
        df = pd.read_csv(args.csv)
    else:
        missing = [n for n in ("environment", "package", "model_path") if not getattr(args, n)]
        if missing or not (args.query or args.query_name):
            parser.error("either --csv, or --env/--package/--model plus --query or --query-name")
        try:
            df = run_query(
                args.host,
                args.environment,
                args.package,
                args.model_path,
                query=args.query,
                query_name=args.query_name,
                source_name=args.source_name,
            )
        except (ValueError, RuntimeError) as e:
            print(f"error: {e}", file=sys.stderr)
            sys.exit(2)

    import great_expectations as gx

    context = gx.get_context(mode="file", project_root_dir=args.gx_dir)
    try:
        suite = context.suites.get(args.suite_name)
    except Exception as e:  # noqa: BLE001 - surfacing whatever GX raises for "not found"
        print(f"error: could not load suite '{args.suite_name}' from {args.gx_dir}: {e}", file=sys.stderr)
        sys.exit(2)

    batch = context.data_sources.pandas_default.read_dataframe(df)
    results = batch.validate(suite)

    print(results.describe())

    if not results.success:
        failed = [r for r in results.results if not r.success]
        print(f"\n{len(failed)} of {len(results.results)} expectations FAILED:", file=sys.stderr)
        for r in failed:
            config = r.expectation_config
            column = getattr(config, "kwargs", {}).get("column", "")
            print(f"  - {config.type}: {column}".rstrip(), file=sys.stderr)
        sys.exit(1)

    print(f"\nAll {len(results.results)} expectations passed against {len(df)} rows.")


if __name__ == "__main__":
    main()
