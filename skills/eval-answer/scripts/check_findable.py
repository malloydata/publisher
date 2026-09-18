#!/usr/bin/env python3
"""Can a right-typed search actually retrieve each `required` entity? Stdlib only.

  python3 check_findable.py --set evals/faa-v1 --mcp-url http://localhost:4045/mcp \
      --environment samples --package faa

WHY THIS EXISTS, AND WHAT IT CATCHES THAT NOTHING ELSE DOES

`expectedEntities.required` is the answer key's claim about which entities an
answer depends on, and two separate measurements are computed against it:

  did the agent ASK for it      -- was a target of a type that can return this
                                   entity kind ever issued
  was it RETRIEVED              -- did it come back

Both are fiction if the list names an entity retrieval cannot deliver. The case
then scores a retrieval miss on every run forever, and the miss reads as a
defect in the model or the agent rather than in the key. Five such ids, copied
from a sibling package, cost one set two days.

`verify_goldens.py` check 5 already reads the model TEXT and reports an id that
names nothing. That is necessary and not sufficient: an entity can be present in
the `.malloy` file and still be unreachable, because retrieval answers from an
index rather than from the source. An entity excluded from discovery, or
arriving before its index has built, passes check 5 and fails here.

WHAT IT DOES NOT PROVE

It searches for each entity by its OWN NAME, with the target type its kind
requires. That is the easiest possible query, so a pass means "reachable at
all", never "reachable by the words a question uses". A entity that answers only
to its own identifier, and not to how people ask, is a documentation finding
this check will happily pass -- that one shows up in a run as NOT-RETRIEVED.

So: a failure here is a hard defect in the key or the index. A pass is a floor,
not a verdict on the docs.

It is also how an author learns what a question's entities are searchable AS,
which is the thing they have to know in order to write `required` at all.

EXIT CODES

  0  every required entity is retrievable
  1  at least one is not
  2  usage error
  3  the check could not run (no server, no scope); says nothing either way
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import urllib.error
import urllib.request
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from mcp_payload import entity_hits  # noqa: E402

# An entity kind, and the target type that can return it. The inverse of the
# server's KINDS_BY_TARGET; `score_retrieval.KINDS_BY_TARGET` is the forward map
# and is pinned against the server, so this stays a one-line derivation of it
# rather than a third copy.
TARGET_FOR_KIND = {"measure": "measure", "dimension": "dimension",
                   "view": "view", "query": "view", "source": "source",
                   "join": "join"}


def phrase_for(name: str) -> str:
    """The entity's own name as a search phrase.

    `average_plane_size` -> "average plane size", and a join path
    `aircraft.aircraft_models.seats` -> "aircraft aircraft models seats". The
    easiest query that could find it, which is what makes a failure decisive.
    """
    return name.replace("_", " ").replace(".", " ").strip()


def get_context(mcp_url: str, targets: list[dict[str, str]],
                environment: str, package: str, timeout: int = 60) -> dict[str, Any]:
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "get_context",
                       "arguments": {"search_targets": targets,
                                     "scopes": [{"environment": environment,
                                                 "package": package}]}}}
    req = urllib.request.Request(
        mcp_url, data=json.dumps(body).encode(),
        headers={"content-type": "application/json",
                 "accept": "application/json, text/event-stream"})
    raw = urllib.request.urlopen(req, timeout=timeout).read().decode()
    # The endpoint answers as SSE when the client accepts it; take the data line.
    for line in raw.splitlines():
        if line.startswith("data: "):
            raw = line[len("data: "):]
            break
    d = json.loads(raw)
    if "result" not in d:
        raise RuntimeError(str(d.get("error"))[:200])
    content = d["result"]["content"]
    text = content[0].get("text") or content[0].get("resource", {}).get("text")
    return json.loads(text)


def required_ids(cases: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Every required id, and the qids that require it. Deduped: the same
    entity named by six cases is one search, not six."""
    out: dict[str, list[str]] = {}
    for c in cases:
        exp = c.get("expectedEntities") or {}
        ids = list(exp.get("required") or [])
        for group in exp.get("requiredAnyOf") or []:
            ids += list(group)
        for eid in ids:
            out.setdefault(eid, []).append(c.get("qid", "?"))
    return out


def check(cases: list[dict[str, Any]], mcp_url: str, environment: str,
          package: str) -> tuple[list[str], list[dict[str, Any]]]:
    findings: list[str] = []
    rows: list[dict[str, Any]] = []
    for eid, qids in sorted(required_ids(cases).items()):
        parts = eid.split(":", 2)
        if len(parts) < 3:
            findings.append(f"{eid}: malformed id, needs kind:source:name "
                            f"(required by {', '.join(qids)})")
            continue
        kind, _, name = parts
        target = TARGET_FOR_KIND.get(kind)
        if target is None:
            findings.append(f"{eid}: unknown entity kind {kind!r} "
                            f"(required by {', '.join(qids)})")
            continue
        phrase = phrase_for(name)
        try:
            payload = get_context(
                mcp_url, [{"target_type": target, "search_text": phrase}],
                environment, package)
        except (urllib.error.URLError, RuntimeError, OSError) as e:
            raise SystemExit(f"get_context failed for {eid}: {e}\n"
                             f"The check did not run; this says nothing about "
                             f"the key.") from e
        hit = next((h for h in entity_hits(payload)
                    if h["entity_id"] == eid), None)
        rows.append({"entity_id": eid, "target": target, "search_text": phrase,
                     "found": bool(hit),
                     "relevance": (hit or {}).get("relevance"),
                     "requiredBy": qids})
        if not hit:
            findings.append(
                f"{eid}: a `{target}` search for {phrase!r} does not return it, "
                f"so every case requiring it scores a retrieval miss forever "
                f"(required by {', '.join(qids)})")
    return findings, rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--mcp-url", required=True,
                    help="the MCP endpoint of the model UNDER TEST, not the "
                         "truth server")
    ap.add_argument("--environment", required=True)
    ap.add_argument("--package", required=True)
    ap.add_argument("--out", default=None,
                    help="write the per-entity rows as JSON")
    a = ap.parse_args(argv)

    f = a.set_dir / "cases.jsonl"
    if not f.exists():
        print(f"no cases.jsonl in {a.set_dir}", file=sys.stderr)
        return 3
    cases = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
    findings, rows = check(cases, a.mcp_url, a.environment, a.package)

    if a.out:
        pathlib.Path(a.out).write_text(json.dumps(
            {"set": str(a.set_dir), "environment": a.environment,
             "package": a.package, "entities": rows}, indent=2))

    checked = len(rows)
    print(f"{checked - len(findings)} of {checked} required entities are "
          f"retrievable by a search of their own kind")
    if not findings:
        print("A pass is a floor, not a verdict on the docs: each was searched "
              "by its own name, the easiest query that could find it.")
        return 0
    print(f"\n{len(findings)} NOT retrievable:")
    for x in findings:
        print(f"  {x}")
    print("\nFix the key or the model before running an arm. Until then those "
          "cases measure nothing: they report a retrieval miss on every run "
          "and it reads as the model's fault.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
