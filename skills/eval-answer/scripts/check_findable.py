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
import urllib.parse
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


def compiled_entities(rest_url: str, environment: str,
                      package: str) -> dict[str, set[str]] | None:
    """`{source: {"kind:name", ...}}` from the COMPILED model, or None.

    The authority on whether an entity exists and what kind it is. A grep over
    the `.malloy` text is neither, and gets it wrong in both directions:

    - it passes `dimension:flights:flight_count`, because `flight_count` is in
      the file. The model declares it a MEASURE, `target_type` is a hard
      filter, and no dimension request can ever return it.
    - it fails `dimension:airports:own_type`, which appears zero times in the
      file because the source exposes it implicitly from the parquet, and which
      retrieves at relevance 1.0. Acting on that finding deletes a good entity.

    The compiled model has both: `sourceInfos[].schema.fields[]` carries every
    field the source actually exposes, each with its `kind`. One request per
    model path settles every id at once.

    None when the server cannot be reached or answers nothing usable, which is
    "not checked" and must not read as "nothing exists".
    """
    try:
        base = rest_url.rstrip("/")
        req = urllib.request.Request(
            f"{base}/api/v0/environments/{environment}/packages/{package}/models")
        models = json.load(urllib.request.urlopen(req, timeout=30))
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None
    out: dict[str, set[str]] = {}
    for m in models if isinstance(models, list) else []:
        path = m.get("path") if isinstance(m, dict) else None
        if not path:
            continue
        try:
            req = urllib.request.Request(
                f"{base}/api/v0/environments/{environment}/packages/{package}"
                f"/models/{urllib.parse.quote(path)}")
            doc = json.load(urllib.request.urlopen(req, timeout=30))
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            continue
        for si in doc.get("sourceInfos") or []:
            info = json.loads(si) if isinstance(si, str) else si
            if not isinstance(info, dict):
                continue
            src = info.get("name")
            fields = (info.get("schema") or {}).get("fields") or []
            if not src:
                continue
            bucket = out.setdefault(src, set())
            bucket.add(f"source:{src}")
            for f in fields:
                if isinstance(f, dict) and f.get("name") and f.get("kind"):
                    bucket.add(f"{f['kind']}:{f['name']}")
    return out or None


def declared_findings(cases: list[dict[str, Any]],
                      declared: dict[str, set[str]]) -> list[str]:
    """Ids the compiled model does not declare, or declares as another kind."""
    out = []
    for eid, qids in sorted(required_ids(cases).items()):
        parts = eid.split(":", 2)
        if len(parts) < 3:
            continue          # malformed; `check` reports it on its own
        kind, src, name = parts
        if src not in declared:
            out.append(f"{eid}: the compiled model has no source {src!r} "
                       f"(required by {', '.join(qids)})")
            continue
        if f"{kind}:{name}" in declared[src]:
            continue
        other = sorted(k.split(":", 1)[0] for k in declared[src]
                       if k.split(":", 1)[1] == name)
        if other:
            out.append(f"{eid}: {src}.{name} is declared "
                       f"{'/'.join(other)}, not {kind}. `target_type` is a hard "
                       f"filter, so no {kind} search can return it "
                       f"(required by {', '.join(qids)})")
        else:
            out.append(f"{eid}: the compiled {src} source declares no field "
                       f"{name!r} (required by {', '.join(qids)})")
    return out


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


def finding_id(message: str) -> str:
    """The entity id a finding is about.

    Every finding here and in `declared_findings` is formatted `f"{eid}: ..."`,
    and an entity id never contains a space, so the id is everything before the
    first ": ". Splitting on the bare colon instead returns the KIND -- and
    deduplicating on that silently dropped a genuine finding for
    `measure:flights:distinct_planes` because an unrelated
    `measure:orders:total_sales` had already been reported.
    """
    return message.split(": ", 1)[0]


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
    ap.add_argument("--publisher", default=None,
                    help="REST URL of the same server, e.g. "
                         "http://localhost:4811. With it, each id is also "
                         "checked against the COMPILED model, which knows both "
                         "that a field exists and what KIND it is. That is the "
                         "authority; a grep over the .malloy text is wrong in "
                         "both directions.")
    ap.add_argument("--out", default=None,
                    help="write the per-entity rows as JSON")
    a = ap.parse_args(argv)

    f = a.set_dir / "cases.jsonl"
    if not f.exists():
        print(f"no cases.jsonl in {a.set_dir}", file=sys.stderr)
        return 3
    cases = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]

    declared_out: list[str] = []
    if a.publisher:
        declared = compiled_entities(a.publisher, a.environment, a.package)
        if declared is None:
            print("! the compiled model could not be read; existence and kind "
                  "were NOT checked", file=sys.stderr)
        else:
            declared_out = declared_findings(cases, declared)
            print(f"compiled model: {sum(len(v) for v in declared.values())} "
                  f"entities across {len(declared)} sources")

    findings, rows = check(cases, a.mcp_url, a.environment, a.package)
    # An id the compiled model already rejected is not also reported as
    # unretrievable: it is the same defect, and saying it twice reads as two.
    # The compiled message is the useful one, because it names the real kind.
    already = {finding_id(f) for f in declared_out}
    findings = declared_out + [f for f in findings
                               if finding_id(f) not in already]

    if a.out:
        pathlib.Path(a.out).write_text(json.dumps(
            {"set": str(a.set_dir), "environment": a.environment,
             "package": a.package, "entities": rows}, indent=2))

    # Every distinct required id, not `len(rows)`. `rows` holds only the ids
    # that reached a search, while `findings` also carries the malformed and
    # unknown-kind ids that never did, plus the compiled-model ones -- so
    # subtracting one from the other counted ids that were never in the total
    # and printed "-1 of 2 required entities are retrievable". Findings are
    # deduplicated by entity id above, so this can no longer go negative.
    total = len(required_ids(cases))
    print(f"{total - len(findings)} of {total} required entities are "
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
