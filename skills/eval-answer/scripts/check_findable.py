#!/usr/bin/env python3
"""Can a right-typed search actually retrieve each `required` entity? Stdlib only.

  python3 check_findable.py --set <set-dir>   # server and names from its eval.toml
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
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import config  # noqa: E402
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


def add_fields(bucket: set[str], fields: list[Any], prefix: str = "") -> None:
    """Every field as `kind:name`, and every field reached through a join as
    `kind:join.name`, the dotted path get_context names it by.

    A join field carries the joined source's schema, so each hop of a path is
    checked against the join the source actually declares. Looking the last
    hop up as a source name instead passed a bogus first hop, and failed a
    join whose name is not its source's (`join_one: buyer is customers`).
    """
    for f in fields:
        if not (isinstance(f, dict) and f.get("name") and f.get("kind")):
            continue
        bucket.add(f"{f['kind']}:{prefix}{f['name']}")
        if f["kind"] == "join":
            add_fields(bucket, (f.get("schema") or {}).get("fields") or [],
                       f"{prefix}{f['name']}.")


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

    A model-level named query is NOT a field, and never appears under
    `sourceInfos`: `FieldInfoType` is dimension/measure/join/view/calculate, so
    `query` cannot be a field kind even in principle. It arrives in the
    response's own `queries` array instead, and is read from there. Without
    that, every `query:` id read as a field the source does not declare -- the
    same false positive this function exists to end, reintroduced for one kind.

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
            add_fields(bucket, fields)
        for q in doc.get("queries") or []:
            if not isinstance(q, dict):
                continue
            name, src = q.get("name"), q.get("sourceName")
            # A query over an inline source has no source name, and the server
            # excludes it from the index for exactly that reason
            # (get_context_tool.ts). Leaving it out here keeps a `query:` id
            # naming one reported as unreachable, which it genuinely is.
            if name and src:
                out.setdefault(src, set()).add(f"query:{name}")
    return out or None


def embedding_index(rest_url: str, environment: str, package: str,
                    timeout: int = 15) -> dict[str, Any] | None:
    """The package's `embeddingIndex` object, or None if it cannot be read.

    `status` is `lexical` (no embedding provider: a mode, not a failure),
    `indexing`, `ready` or `error`. An `error` carries `reason` (`cooldown`,
    `too-many-entities`, `provider-error`, `unavailable`) and `lastError`
    (`message`, `retryAt`). With a provider configured, get_context answers
    semantically only at `ready`; a checker that ignores the status reports a
    server-side state as a broken answer key. None also covers a server too old
    to send the field.
    """
    url = (f"{rest_url.rstrip('/')}/api/v0/environments/"
           f"{urllib.parse.quote(environment)}/packages/"
           f"{urllib.parse.quote(package)}")
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body = json.loads(r.read())
    except (urllib.error.URLError, OSError, ValueError):
        return None
    index = body.get("embeddingIndex")
    if not isinstance(index, dict) or not isinstance(index.get("status"), str):
        return None
    return index


# `indexing` is the only state that can still change on its own. `lexical`
# (no provider) and `error` are settled; `error` clears only by a re-run.
TERMINAL = ("ready", "lexical", "error")


def index_message(index: dict[str, Any] | None, wait: int) -> str | None:
    """What to tell the user about the index, or None when it is `ready`."""
    if index is None:
        return ("! the embedding index status could not be read (the server "
                "may be too old to report it); a miss below may be the "
                "lexical matcher rather than the key")
    status = index.get("status")
    if status == "ready":
        return None
    if status == "lexical":
        return ("! no embedding provider is configured, so this run measures "
                "the lexical matcher. The misses below are real for this "
                "server and say nothing about semantic retrieval")
    if status == "indexing":
        return (f"! the embedding index was still indexing after {wait}s, so "
                f"get_context is not answering semantically yet. Re-run "
                f"rather than acting on a miss below")
    if status == "error":
        reason = index.get("reason")
        last = index.get("lastError")
        detail = last.get("message") if isinstance(last, dict) else None
        if reason == "cooldown":
            return ("! the embedding provider failed recently and is in "
                    "cooldown, so get_context returns an error rather than "
                    "an answer. Re-run once it has cleared"
                    + (f" ({detail})" if detail else ""))
        if reason == "too-many-entities":
            return ("! this package is over the embedding entity cap, so it "
                    "will never be indexed. Raise retrieval.indexing."
                    "maxEntities, or split the package")
        return (f"! the embedding index is in error (reason: "
                f"{reason or 'unknown'}): {detail or 'no message given'}")
    return (f"! the embedding index reported an unknown status {status!r}; "
            f"a miss below may be the index, not the key")


def wait_for_index(rest_url: str, environment: str, package: str, wait: int,
                   read=None, sleep=time.sleep,
                   clock=time.monotonic) -> dict[str, Any] | None:
    """Poll until the index status is terminal, unreadable, or `wait` runs out."""
    read = read or embedding_index
    deadline = clock() + wait
    index = read(rest_url, environment, package)
    while index is not None and index["status"] not in TERMINAL \
            and clock() < deadline:
        sleep(2)
        index = read(rest_url, environment, package)
    return index


def stale_packages(rest_url: str, timeout: int = 30) -> set[tuple[str, str]] | None:
    """`{(environment, package)}` serving a model older than their files.

    A stale package answers every request, and answers from the compile BEFORE
    its last save. `/api/v0/status`'s `loadErrors` is the only place that is
    reported: the package's entry under `environments`, and its own package
    resource, both read as serving. So the models endpoint this module treats
    as "the authority" reads entirely normal while describing a model that is
    no longer on disk -- which can fail a good id or pass a deleted one, with
    nothing to say which happened.

    None when the status could not be read at all, which is "not checked".
    """
    try:
        req = urllib.request.Request(f"{rest_url.rstrip('/')}/api/v0/status")
        doc = json.load(urllib.request.urlopen(req, timeout=timeout))
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None
    return {(e.get("environment") or "", e.get("package") or "")
            for e in doc.get("loadErrors") or []
            if isinstance(e, dict) and e.get("stale")}


def current_entities(rest_url: str, environment: str,
                     package: str) -> tuple[dict[str, set[str]] | None, str | None]:
    """`compiled_entities`, unless the package is stale, and a warning if any.

    Staleness first. A stale package answers the models endpoint normally
    while describing the compile BEFORE the last save, so an entity deleted
    since then reads as declared. Refusing to check says so; checking anyway
    would be one more number nobody had earned. Every caller that treats the
    compiled model as the authority goes through here.
    """
    stale = stale_packages(rest_url)
    if stale and (environment, package) in stale:
        return None, (f"{environment}/{package} is serving a STALE model (its "
                      f"last reload failed to compile), so the compiled model "
                      f"is not the authority on what exists; existence and "
                      f"kind were NOT checked. Fix the model and reload, then "
                      f"re-run.")
    declared = compiled_entities(rest_url, environment, package)
    if declared is None:
        return None, ("the compiled model could not be read; existence and "
                      "kind were NOT checked")
    if stale is None:
        return declared, ("the server's status could not be read, so "
                          "staleness is unknown; treating the compiled model "
                          "as current")
    return declared, None


def declared_findings(cases: list[dict[str, Any]],
                      declared: dict[str, set[str]]) -> list[str]:
    """Ids the compiled model does not declare, or declares as another kind."""
    out = []
    for eid, qids in sorted(required_ids(cases).items()):
        parts = eid.split(":", 2)
        if len(parts) < 3 or not all(parts):
            # Reported here as well as by `check`: the in-run lint calls only
            # this, and an id it skips is one no retrieval can ever match.
            out.append(f"{eid}: not a kind:source:name id "
                       f"(required by {', '.join(qids)})")
            continue
        kind, src, name = parts
        if src not in declared:
            out.append(f"{eid}: the compiled model has no source {src!r} "
                       f"(required by {', '.join(qids)})")
            continue
        if f"{kind}:{name}" in declared[src]:
            continue
        # A DOTTED name is a join path. `compiled_entities` records every
        # field reached through a join under its path, so a miss above means a
        # hop or the leaf is wrong. Name the first hop that does not exist.
        if "." in name:
            *path, leaf = name.split(".")
            reached = ""
            for hop in path:
                if f"join:{reached}{hop}" not in declared[src]:
                    out.append(f"{eid}: the compiled {src} source has no join "
                               f"{reached + hop!r} (required by "
                               f"{', '.join(qids)})")
                    break
                reached += hop + "."
            else:
                out.append(f"{eid}: the join {'.'.join(path)!r} from {src} "
                           f"reaches no {kind} {leaf!r} (required by "
                           f"{', '.join(qids)})")
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
    ap.add_argument("--mcp-url", default=None,
                    help="the MCP endpoint of the model UNDER TEST, not the "
                         "truth server. Default: from [model] in the set's "
                         "eval.toml")
    ap.add_argument("--environment", default=None,
                    help="Default: [model] environment in the set's eval.toml")
    ap.add_argument("--package", default=None,
                    help="Default: [model] package in the set's eval.toml, then "
                         "set.json's targetPackage")
    ap.add_argument("--publisher", default=None,
                    help="REST URL of the same server, e.g. "
                         "http://localhost:4811. With it, each id is also "
                         "checked against the COMPILED model, which knows both "
                         "that a field exists and what KIND it is. That is the "
                         "authority; a grep over the .malloy text is wrong in "
                         "both directions.")
    ap.add_argument("--out", default=None,
                    help="write the per-entity rows as JSON")
    ap.add_argument("--index-wait", type=int, default=300,
                    help="seconds to wait for the embedding index to be ready "
                         "before searching (needs --publisher). 0 to skip the "
                         "wait, which risks reading the lexical matcher as a "
                         "missing entity")
    a = ap.parse_args(argv)
    cfg = config.load(a.set_dir)
    a.environment = cfg.need(a.environment, "model", "environment", "--environment")
    a.package = cfg.need(a.package, "model", "package", "--package")
    a.mcp_url = a.mcp_url or cfg.model_mcp_url()

    f = a.set_dir / "cases.jsonl"
    if not f.exists():
        print(f"no cases.jsonl in {a.set_dir}", file=sys.stderr)
        return 3
    cases = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]

    # With an embedding provider configured, get_context does not fall back to
    # the lexical matcher while the index builds: it answers "indexing" or an
    # error, and this check would read that empty answer as "the key names
    # entities that cannot be retrieved". So wait for a terminal status first,
    # and say so when the wait ends on anything but `ready`.
    if a.publisher and a.index_wait:
        # Current servers start indexing when the package loads, so polling
        # alone is enough. Older servers only started a sync on the first
        # ranking call, and polling never triggers one, so send one ranking
        # call first (as serve.py --warm-retrieval does). It is harmless on a
        # current server.
        try:
            get_context(a.mcp_url, [{"target_type": "source"}],
                        a.environment, a.package)
        except (urllib.error.URLError, RuntimeError, OSError):
            pass                      # the search below reports it properly
        index = wait_for_index(a.publisher, a.environment, a.package,
                               a.index_wait)
        note = index_message(index, a.index_wait)
        if note:
            print(note, file=sys.stderr)

    declared_out: list[str] = []
    if a.publisher:
        declared, warning = current_entities(a.publisher, a.environment, a.package)
        if warning:
            print(f"! {warning}", file=sys.stderr)
        if declared is not None:
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
