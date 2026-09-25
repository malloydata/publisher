#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

"""Run a Malloy query against a running Publisher server (REST API) and
return the rows as a pandas DataFrame.

Use this when the Malloy MCP tools (`execute_query`, `get_context`, ...) are
NOT connected in the current session -- for example when running from a
plain shell, a CI job, or a headless script. When those MCP tools ARE
available, prefer calling `execute_query` directly instead of this script;
it talks to the exact same endpoint but the MCP tool also gets you grounded
discovery (get_context) for free.

Examples:
    # Ad-hoc query
    python malloy_client.py --env examples --package storefront \\
        --model storefront.malloy --query "run: order_items -> by_category" \\
        --out /tmp/by_category.csv

    # Named view on a source
    python malloy_client.py --env examples --package storefront \\
        --model storefront.malloy --query-name by_category \\
        --source-name order_items
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

import pandas as pd


def run_query(
    host: str,
    environment: str,
    package: str,
    model_path: str,
    query: str | None = None,
    query_name: str | None = None,
    source_name: str | None = None,
) -> pd.DataFrame:
    """POST a query to Publisher's REST API and return the rows as a DataFrame.

    Mirrors what `execute_query` does over MCP: same endpoint, same request
    shape. See AGENTS.md section 7 in a Publisher clone for the full contract.
    """
    url = f"{host}/api/v0/environments/{environment}/packages/{package}/models/{model_path}/query"
    body: dict[str, object] = {"compactJson": True}
    if query:
        body["query"] = query
    elif query_name:
        body["queryName"] = query_name
        if source_name:
            body["sourceName"] = source_name
    else:
        raise ValueError("Provide either query or query_name")

    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")
        raise RuntimeError(f"Publisher returned HTTP {e.code} for {url}:\n{detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Could not reach Publisher at {host} ({e.reason}). "
            "Is the server running? Check `curl {host}/api/v0/status`."
        ) from e

    rows = json.loads(payload["result"])
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="http://localhost:4000", help="Publisher REST base URL")
    parser.add_argument("--env", required=True, dest="environment", help="Environment name, e.g. 'examples'")
    parser.add_argument("--package", required=True, help="Package name, e.g. 'storefront'")
    parser.add_argument("--model", required=True, dest="model_path", help="Model path within the package, e.g. 'storefront.malloy'")
    parser.add_argument("--query", help="Ad-hoc Malloy query text, e.g. \"run: order_items -> by_category\"")
    parser.add_argument("--query-name", help="Name of a model-level query, or a source's named view (pair with --source-name)")
    parser.add_argument("--source-name", help="Source the --query-name view belongs to")
    parser.add_argument("--out", help="Write result as CSV to this path (default: print CSV to stdout)")
    args = parser.parse_args()

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
        sys.exit(1)

    if args.out:
        df.to_csv(args.out, index=False)
        print(f"Wrote {len(df)} rows x {len(df.columns)} columns to {args.out}", file=sys.stderr)
    else:
        print(df.to_csv(index=False))


if __name__ == "__main__":
    main()
