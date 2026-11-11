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

The request also asks for `compactJson`, and that is what recovers the field
names INSIDE a nest. The typed envelope names only the top level, so a nested
record came back keyed `c0`, `c1` -- and a golden written the way its author
ran the query, with the real column names, then read as DRIFT, which blocks an
arm. That was called a limit here; it was not. Publisher has answered plain
rows all along, and the request simply never asked. `rows_from_result` handles
both shapes (compact answers with `result` as a JSON string), so the envelope
walk stays as the fallback for a server that ignores the flag.

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
          timeout: int = 180, givens: dict[str, Any] | None = None
          ) -> list[dict[str, Any]]:
    """Run a Malloy query. Raises on transport, HTTP or parse failure.

    `givens` are the model's runtime parameters, sent as the request's own
    `givens` field. A query written against a given and run without it returns
    unfiltered rows, so a caller re-running someone else's query passes theirs.
    """
    body: dict[str, Any] = {"query": malloy, "compactJson": True}
    if givens:
        body["givens"] = givens
    req = urllib.request.Request(
        _query_url(base, environment, package, model),
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return rows_from_result(json.loads(r.read().decode()))


def try_query(base: str, environment: str, package: str, model: str,
              malloy: str, timeout: int = 120,
              givens: dict[str, Any] | None = None
              ) -> tuple[list[dict[str, Any]], str | None]:
    """Run a Malloy query. Returns (rows, error); never raises.

    The error string is for a human and a judge to read, so it carries the
    server's own words rather than a class name.
    """
    try:
        return query(base, environment, package, model, malloy,
                     timeout=timeout, givens=givens), None
    except urllib.error.HTTPError as exc:
        return [], f"HTTP {exc.code}: {exc.read().decode()[:300]}"
    except Exception as exc:                        # noqa: BLE001
        return [], f"{exc}"[:300] or exc.__class__.__name__


def get_json(base: str, path: str, timeout: int = 30) -> Any:
    with urllib.request.urlopen(f"{base.rstrip('/')}/{path.lstrip('/')}",
                                timeout=timeout) as resp:
        return json.loads(resp.read())


def served_package_dir(base: str, environment: str,
                       package: str) -> pathlib.Path | None:
    """The directory the running Publisher serves this package FROM, or None.

    A host commonly serves a copy rather than your working tree, so this is the
    only path whose bytes are the ones answering queries.
    """
    try:
        for proj in get_json(base, "api/v0/projects"):
            if proj.get("name") == environment and proj.get("location"):
                p = pathlib.Path(proj["location"]) / package
                return p if p.is_dir() else None
    except (OSError, ValueError):   # unreachable or non-JSON: absence is reported
        return None
    return None


def package_skill_names(base: str, environment: str, package: str) -> list[str]:
    """Skills the server says this package ships, or [] if it ships none.

    Read from the server rather than from the working tree, because those are
    two different questions: a host serves a COPY of the package. What the
    answerer can actually reach is what this returns.

    An unreachable or older server returns [] rather than raising: the caller
    decides whether "the server serves none" contradicts what it asked for.
    """
    try:
        rows = get_json(
            base,
            f"api/v0/environments/{urllib.parse.quote(environment)}"
            f"/packages/{urllib.parse.quote(package)}/skills")
    except (OSError, ValueError):                       # unreachable or non-JSON
        return []
    return sorted(r["name"] for r in rows
                  if isinstance(r, dict) and r.get("origin") == "package"
                  and isinstance(r.get("name"), str))


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


def package_version(base: str, environment: str, package: str) -> dict[str, Any]:
    """The published version answering a request that names none, i.e. `latest`.

    `{"id", "latest", "contentHash", "archived"}`, read from `GET …/packages/{pkg}`
    (`versionId`, `latestVersion`) and `GET …/versions/{id}` (`contentHash`).
    `{"id": None}` for a package with no published versions, which is every
    package unless the server runs with `PUBLISHER_PACKAGE_VERSIONING=on` and
    the package was published. `{}` when the server cannot be read.

    `contentHash` covers every file in the version except `.git`, so it is the
    version's identity; `id` is the name a person uses for it.
    """
    pkg_path = (f"api/v0/environments/{urllib.parse.quote(environment)}"
                f"/packages/{urllib.parse.quote(package)}")
    try:
        d = get_json(base, pkg_path)
    except Exception:                                   # noqa: BLE001
        return {}
    vid = d.get("versionId") if isinstance(d, dict) else None
    if not isinstance(vid, str) or not vid:
        return {"id": None}
    out: dict[str, Any] = {"id": vid, "latest": vid == d.get("latestVersion"),
                           "contentHash": None, "archived": None}
    try:
        v = get_json(base, f"{pkg_path}/versions/{urllib.parse.quote(vid)}")
        out["contentHash"] = v.get("contentHash")
        out["latest"] = bool(v.get("latest", out["latest"]))
        out["archived"] = v.get("archiveStatus") == "archive"
    except Exception:                                   # noqa: BLE001
        pass
    return out


def package_versions(base: str, environment: str, package: str) -> list[dict[str, Any]]:
    """Every version the package holds, highest first, archived ones included
    (`GET …/packages/{pkg}/versions`). [] when unversioned or unreachable."""
    try:
        rows = get_json(base, f"api/v0/environments/{urllib.parse.quote(environment)}"
                              f"/packages/{urllib.parse.quote(package)}/versions")
    except Exception:                                   # noqa: BLE001
        return []
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def package_identity(base: str, environment: str, package: str) -> dict[str, str]:
    """What the server says it is serving: `sourceContentSha`, `servedRevision`,
    read from the package's serving state (`GET …/packages/{pkg}/status`).

    Take these rather than computing your own. `sourceContentSha` is a content
    hash over EVERY model path in the package, so it moves when an imported file
    moves -- which a sha of the one `--model-path` does not. `servedRevision` is
    minted per load, so it identifies a load rather than content: two loads of
    identical bytes have different revisions, and it is a poor pin on its own.

    Measured on a live server: bumping `version` in publisher.json does not move
    `sourceContentSha`, and neither does adding a non-model file to the package
    directory. On an unversioned package `publisher.json`'s version pins
    nothing; on a published one, `package_version` reads the version and its
    `contentHash`, which covers every file.
    """
    keys = ("sourceContentSha", "servedRevision")
    pkg_path = (f"api/v0/environments/{urllib.parse.quote(environment)}"
                f"/packages/{urllib.parse.quote(package)}")
    # The package's serving state; for a versioned package, its `latest`'s.
    try:
        d = get_json(base, f"{pkg_path}/status")
        if any(isinstance(d.get(k), str) for k in keys):
            return {k: d[k] for k in keys if isinstance(d.get(k), str)}
    except Exception:                                   # noqa: BLE001
        pass
    # A server from before that route: the package's `/status` entry carries
    # them, one entry per version held, and the one serving `latest` answers.
    try:
        status = get_json(base, "api/v0/status")
        for env in status.get("environments") or []:
            if env.get("name") != environment:
                continue
            entries = [p for p in env.get("packages") or []
                       if p.get("name") == package]
            entry = next((p for p in entries
                          if p.get("versionId") in (None, p.get("latestVersion"))),
                         entries[0] if entries else None)
            if entry and any(isinstance(entry.get(k), str) for k in keys):
                return {k: entry[k] for k in keys if isinstance(entry.get(k), str)}
    except Exception:                                   # noqa: BLE001
        pass
    # Older still: on the package resource.
    try:
        d = get_json(base, pkg_path)
    except Exception:                                   # noqa: BLE001
        return {}
    return {k: d[k] for k in keys if isinstance(d.get(k), str)}
