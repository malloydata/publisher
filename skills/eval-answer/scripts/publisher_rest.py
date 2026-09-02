#!/usr/bin/env python3
"""The one direct path from a script to a Publisher. Stdlib only.

    from publisher_rest import query, try_query, get_json

An eval reaches a server two ways and there is no third. Agents go through the
Claude CLI's own MCP config, because the whole point of an answerer is that it
holds the tools a real user's agent holds. Scripts come here.

WHY THIS FILE EXISTS

`verify_goldens.query` and `run_baseline.execute` were two implementations of
one operation -- POST a Malloy query, uncell the result. Publisher answers with
Malloy's typed cell envelope, which nests: a `record_cell` holds cells and an
`array_cell` holds cells. verify_goldens walked that recursively; run_baseline
took the first key ending `_value` at the top level and stopped.

Measured, on the three shapes a query actually returns: they agree on flat
scalars and on nulls (`null_cell` has no `_value` key, so the shallow version's
default returned None and was right by accident). They disagree on a `nest:`,
where the shallow version handed back the raw envelope -- `[{"kind":
"record_cell", "record_value": [...]}]` -- as the cell's value. Those rows went
to the judge as the re-executed evidence for a verdict, so a nested result was
judged against a golden it could not match.

The recursive uncelling is therefore the one kept here, and both callers get it.
It does not recover the field names INSIDE a nest -- the envelope only names the
top level -- so nested records come back keyed `c0`, `c1`. That was true of the
recursive version before this move too; it is a limit, not a regression.

TWO ERROR CONVENTIONS, ON PURPOSE

`query` raises: a golden that cannot be re-derived must stop the run, and
verify_goldens is that stop. `try_query` returns `(rows, error)`: re-executing
an answerer's query is evidence-gathering, and a query the ANSWERER wrote
failing is a fact about the attempt, not a harness fault. Same request, same
parse, different contract, so neither caller has to remember which exception to
swallow.
"""
from __future__ import annotations

import json
import pathlib
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# The scalar cell kinds, in the order Publisher may emit them. Checked by key
# presence rather than by `kind`, because older servers omitted `kind` on
# scalars while always naming the value key.
SCALAR_KEYS = ("number_value", "string_value", "boolean_value", "date_value",
               "timestamp_value", "json_value")


def _query_url(base: str, environment: str, package: str, model: str) -> str:
    return (f"{base.rstrip('/')}/api/v0/environments/"
            f"{urllib.parse.quote(environment)}"
            f"/packages/{urllib.parse.quote(package)}"
            f"/models/{urllib.parse.quote(model, safe='')}/query")


def uncell(cell: Any, names: list[str] | None = None) -> Any:
    """Collapse Malloy's typed cell envelope into plain values, recursively."""
    if not isinstance(cell, dict):
        return cell
    kind = cell.get("kind")
    if kind == "record_cell":
        vals = cell.get("record_value", [])
        ns = names or [f"c{i}" for i in range(len(vals))]
        return {n: uncell(v) for n, v in zip(ns, vals)}
    if kind == "array_cell":
        return [uncell(v, names) for v in cell.get("array_value", [])]
    if kind == "null_cell":
        return None
    for key in SCALAR_KEYS:
        if key in cell:
            return cell[key]
    return cell


def rows_from_result(body: Any) -> list[dict[str, Any]]:
    """Plain row dicts out of a query response body.

    The field names live in `schema.fields` and the values in `data`,
    positionally -- the envelope does not repeat the names per row.
    """
    result = body.get("result", body) if isinstance(body, dict) else body
    if isinstance(result, str):
        result = json.loads(result)
    if isinstance(result, dict) and "data" in result:
        names = [f.get("name") for f in
                 (result.get("schema") or {}).get("fields") or []]
        data = result["data"]
        records = (data.get("array_value", []) if isinstance(data, dict)
                   else data)
        return [uncell(rc, names) for rc in records]
    return result if isinstance(result, list) else []


def query(base: str, environment: str, package: str, model: str, malloy: str,
          timeout: int = 180) -> list[dict[str, Any]]:
    """Run a Malloy query. Raises on transport, HTTP or parse failure."""
    req = urllib.request.Request(
        _query_url(base, environment, package, model),
        data=json.dumps({"query": malloy}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return rows_from_result(json.loads(r.read().decode()))


def try_query(base: str, environment: str, package: str, model: str,
              malloy: str, timeout: int = 120
              ) -> tuple[list[dict[str, Any]], str | None]:
    """Run a Malloy query. Returns (rows, error); never raises.

    The error string is for a human and a judge to read, so it carries the
    server's own words rather than a class name.
    """
    try:
        return query(base, environment, package, model, malloy,
                     timeout=timeout), None
    except urllib.error.HTTPError as exc:
        return [], f"HTTP {exc.code}: {exc.read().decode()[:300]}"
    except Exception as exc:                        # noqa: BLE001
        return [], f"{exc}"[:300] or exc.__class__.__name__


def get_json(base: str, path: str, timeout: int = 30) -> Any:
    with urllib.request.urlopen(f"{base.rstrip('/')}/{path.lstrip('/')}",
                                timeout=timeout) as resp:
        return json.loads(resp.read())


def served_model_path(base: str, environment: str, package: str,
                      model_path: str) -> pathlib.Path | None:
    """Where the running Publisher reads this model from, or None.

    A snapshot host serves a copy, so the working tree is not the answer and a
    judge shown the working tree may be reading a model the run never used.
    """
    try:
        for proj in get_json(base, "api/v0/projects"):
            if proj.get("name") == environment and proj.get("location"):
                p = pathlib.Path(proj["location"]) / package / model_path
                return p if p.exists() else None
    except Exception:            # noqa: BLE001 - absence is reported, not fatal
        return None
    return None
