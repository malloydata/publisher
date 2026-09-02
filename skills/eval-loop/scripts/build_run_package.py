#!/usr/bin/env python3
"""Turn one or more runs into a Malloy package you can serve. Stdlib only.

  python build_run_package.py --run results/sonnet --run results/opus \
      --set evals/ecommerce --out /tmp/evalpkg

Writes CSVs, a Malloy model over them, a notebook for the analytical tables and
an in-package HTML app for the case matrix.

WHY BOTH A NOTEBOOK AND AN APP

They are not redundant, and the split is not stylistic. Publisher renders a
`.malloynb` natively, so the aggregate tables -- pass rate, effort, most-missed
entities, the backlog -- are best expressed as Malloy and left alone: no
JavaScript to drift out of step with the model. What a notebook cannot do is
open a drawer, and reading an eval is mostly drilling into one case. So the
matrix and its drawer are an HTML app, and both read the same model, which is
what keeps the two from disagreeing.

Scoring is imported from eval-answer rather than reimplemented. It was
reimplemented once, and the copy here kept a bug the original had already been
fixed for -- a report whose two tables disagreed about how many failures there
were.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import pathlib
import shutil
import sys
from typing import Any

TEMPLATE = pathlib.Path(__file__).resolve().parent.parent / "templates" / "eval-run-package"
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
from score_retrieval import delivery, groups, score_case  # noqa: E402
from flip_table import outcome  # noqa: E402  (same directory)


def read_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def read_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text()) if path.exists() else {}


def flatten(v: Any) -> Any:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (list, tuple)):
        return " | ".join(str(flatten(x)) for x in v)
    if isinstance(v, dict):
        return json.dumps(v, separators=(",", ":"))
    return v


def write_csv(path: pathlib.Path, rows: list[dict[str, Any]],
              columns: list[str]) -> None:
    """One CSV. An EMPTY table is written as a header only, which DuckDB types
    as all-VARCHAR -- so every numeric column the model aggregates is cast in
    eval_run.malloy rather than trusted to inference. A clean run has no
    clusters, and a viewer that breaks on a clean run is the wrong way round."""
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: flatten(r.get(c)) for c in columns})


def key(e: dict[str, Any]) -> tuple:
    return (e.get("qid"), e.get("sample"), e.get("phase"))


def attempt_key(run_id: str, e: dict[str, Any]) -> str:
    """The grain of the whole package, as one column.

    attempts, scores and retrieval are one-to-one at (run, case, sample), but
    joining on the parts breaks the moment a case is sampled twice, and breaks
    quietly -- as a fan-out that inflates every count. One key makes the grain
    declarable as a primary key instead.
    """
    return f"{run_id}|{e.get('qid')}|{e.get('sample')}|{e.get('phase')}"


def split_entity(eid: str) -> tuple[str, str, str]:
    parts = (eid or "").split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], ":".join(parts[2:])
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return "", "", eid or ""


def golden_display(g: dict[str, Any]) -> str:
    """One short human-readable line for the golden, whatever its shape."""
    if not g:
        return ""
    if g.get("kind") == "unanswerable":
        return "(unanswerable -- the model cannot answer this)"
    v = g.get("value")
    if v is None:
        return "(unanswerable -- the model cannot answer this)"
    if isinstance(v, dict):
        nums = {k: x for k, x in v.items()
                if isinstance(x, (int, float)) and k not in ("round",)}
        if len(nums) == 1:
            k, x = next(iter(nums.items()))
            unit = v.get("currency", "")
            return f"{k} = {x:,.2f}{' ' + unit.upper() if unit else ''}"
        return json.dumps(v, separators=(", ", ": "))[:300]
    if isinstance(v, list):
        return f"{len(v)} rows: " + json.dumps(v[:2], separators=(",", ":"))[:250]
    return str(v)[:300]


_IDENT = re.compile(r"(?<![A-Za-z0-9_])[a-z][a-z0-9]*(?:_[a-z0-9]+)+(?![A-Za-z0-9_])")


def _result_text(block: dict[str, Any]) -> str:
    c = block.get("content")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return "\n".join(x.get("text", "") for x in c if isinstance(x, dict))
    return ""


def _payload(text: str) -> Any:
    """The JSON inside a tool result, whichever wrapper the surface used."""
    m = re.search(r"\[Resource from publisher at [^\]]+\]\s*(\{.*)", text, re.S)
    for cand in ((m.group(1) if m else None), text.strip()):
        if cand and cand.startswith("{"):
            try:
                return json.loads(cand)
            except json.JSONDecodeError:
                continue
    return None


def walk_transcript(path: pathlib.Path) -> tuple[list[dict[str, Any]], str]:
    """Every step of one attempt, in order, plus the text of every source doc
    get_context returned.

    The viewer's first job is to let a reader tell a wrong query from a right
    query summarised wrongly, and the ledger cannot answer that: it keeps
    call counts and the final query, not the sequence. The transcript has
    the sequence -- each tool call's input beside its result -- so the
    package carries it as `steps`. Assistant prose between calls is kept
    too, because the reasoning that chose the next query is part of the
    evidence.
    """
    steps: list[dict[str, Any]] = []
    docs: list[str] = []
    if not path.exists():
        return steps, ""
    pending: dict[str, dict[str, Any]] = {}
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        if e.get("type") == "assistant":
            for c in e["message"].get("content") or []:
                if c.get("type") == "text" and c["text"].strip():
                    steps.append({"kind": "text", "label": c["text"].strip(),
                                  "detail": "", "n_results": None,
                                  "is_error": False})
                elif c.get("type") == "tool_use":
                    name, inp = c["name"], c.get("input") or {}
                    short = name.split("__")[-1]
                    if short == "Skill":
                        st = {"kind": "skill", "label": inp.get("skill", ""),
                              "detail": ""}
                    elif short in ("get_context", "malloy_getContext"):
                        tg = inp.get("search_targets") or []
                        st = {"kind": "get_context",
                              "label": " · ".join(
                                  f"{t.get('target_type', '?')}: "
                                  f"{t.get('search_text', '(listing)')}"
                                  for t in tg) if tg else "(listing)",
                              "detail": ""}
                    elif short in ("execute_query", "malloy_executeQuery"):
                        st = {"kind": "execute_query",
                              "label": inp.get("query") or inp.get("query_name")
                              or "", "detail": inp.get("model_path")
                              or inp.get("modelPath") or ""}
                    else:
                        st = {"kind": short, "label": json.dumps(inp)[:300],
                              "detail": ""}
                    st.update({"n_results": None, "is_error": False})
                    steps.append(st)
                    pending[c["id"]] = st
        elif e.get("type") == "user":
            for c in e["message"].get("content") or []:
                if not (isinstance(c, dict) and c.get("type") == "tool_result"):
                    continue
                st = pending.pop(c.get("tool_use_id"), None)
                if st is None:
                    continue
                text = _result_text(c)
                if c.get("is_error"):
                    st["is_error"] = True
                    st["detail"] = text[:400]
                    continue
                pl = _payload(text)
                if st["kind"] == "get_context" and isinstance(pl, dict):
                    n = 0
                    for src in pl.get("sources") or []:
                        n += 1 + len(src.get("entities") or [])
                        info = src.get("source_info") or {}
                        # Credible's shape carries the source text under
                        # `summary` (LLM-written) plus `docs`/`one_line_summary`;
                        # Publisher's under `docs`. Take every string field.
                        docs.append(" ".join(str(v) for v in info.values()
                                             if isinstance(v, str)))
                        if isinstance(src.get("docs"), str):
                            docs.append(src["docs"])
                    if not pl.get("sources") and isinstance(pl.get("results"), list):
                        n = len(pl["results"])
                    st["n_results"] = n
                elif st["kind"] == "execute_query" and isinstance(pl, dict):
                    if pl.get("error"):
                        st["is_error"] = True
                        st["detail"] = str(pl["error"])[:400]
                    else:
                        rows = pl.get("rows") if isinstance(pl.get("rows"), list) \
                            else pl.get("data") if isinstance(pl.get("data"), list) \
                            else None
                        if rows is not None:
                            st["n_results"] = len(rows)
                            st["detail"] = json.dumps(rows[:3],
                                                      separators=(",", ":"))[:600]
                elif pl is None and text:
                    st["detail"] = text[:300]
    return steps, "\n".join(docs)


def build(run_dirs: list[pathlib.Path], set_dir: pathlib.Path,
          out: pathlib.Path) -> dict[str, int]:
    data = out / "data"
    data.mkdir(parents=True, exist_ok=True)

    cases = read_jsonl(set_dir / "cases.jsonl")
    by_qid = {c["qid"]: c for c in cases}

    case_rows = []
    for c in cases:
        g = c.get("golden") or {}
        exp = c.get("expectedEntities") or {}
        case_rows.append({
            "qid": c["qid"], "question": c.get("question"),
            "split": c.get("split"), "coverage": c.get("coverage"),
            "coverage_note": c.get("coverageNote"),
            "golden_kind": g.get("kind"), "golden_status": g.get("status"),
            "golden_revision": c.get("goldenRevision"),
            "golden_display": golden_display(g),
            "golden_value": json.dumps(g.get("value"))[:40000]
            if g.get("value") is not None else "",
            "rubric": (g.get("rubric") or "")[:2000],
            "must_state": g.get("mustState"),
            "n_required": len(exp.get("required") or []),
            "required_entities": sorted(exp.get("required") or []),
            "tags": c.get("tags"),
            "has_structural_alternate": any(
                not a.get("accept") for a in (g.get("alternates") or [])),
        })
    write_csv(data / "cases.csv", case_rows, [
        "qid", "question", "split", "coverage", "coverage_note", "golden_kind",
        "golden_status", "golden_revision", "golden_display", "golden_value",
        "rubric", "must_state", "n_required", "required_entities", "tags",
        "has_structural_alternate"])

    runs, attempts, scores, retr, ents, issues, calls = [], [], [], [], [], [], []
    steps_rows, required_rows = [], []

    for rd in run_dirs:
        cfg = read_json(rd / "run.json")
        run_id = cfg.get("runId") or rd.name
        runs.append({
            "run_id": run_id, "label": cfg.get("label") or rd.name,
            "target": cfg.get("target"), "model": cfg.get("answererModel"),
            "effort": cfg.get("effort"), "started": cfg.get("started"),
            "judge_version": cfg.get("judgeVersion"),
            "set_version": cfg.get("datasetVersion"),
        })
        events = read_jsonl(rd / "events.jsonl")
        verdicts = {key(e): e for e in events if e.get("kind") == "score"}
        tool_calls: dict[tuple, list[dict[str, Any]]] = {}
        for e in events:
            if e.get("kind") == "tool_call":
                tool_calls.setdefault(key(e), []).append(e)

        for e in events:
            if e.get("kind") == "issue":
                issues.append({"run_id": run_id, **e})

        for e in events:
            if e.get("kind") != "attempt":
                continue
            kk = key(e)
            qid = e.get("qid")
            ak = attempt_key(run_id, e)
            s = verdicts.get(kk, {})
            case = by_qid.get(qid) or {}

            tpath = rd / (e.get("transcriptPath") or f"artifacts/{qid}/answerer.jsonl")
            steps, docs = walk_transcript(tpath)
            for i, st in enumerate(steps, 1):
                steps_rows.append({"attempt_key": ak, "run_id": run_id, "qid": qid,
                                   "step_index": i, **st})
            # prediction.json is {"query", "rendered"}: the re-executed rows as
            # JSON or as a pipe-separated text table, or a "(not re-executed:
            # ...)" note. Passed through verbatim; the viewer renders each.
            pred_path = rd / "artifacts" / str(qid) / "prediction.json"
            pred = read_json(pred_path) if pred_path.exists() else {}
            rendered = pred.get("rendered") if isinstance(pred, dict) else None
            if not isinstance(rendered, str):
                rendered = json.dumps(rendered) if rendered is not None else ""
            attempts.append({"attempt_key": ak, "run_id": run_id, **e,
                             "n_steps": len(steps),
                             "prediction": rendered[:40000]})
            scores.append({"attempt_key": ak, "run_id": run_id, "qid": qid,
                           "sample": e.get("sample"), "phase": e.get("phase"),
                           # Classified here, by the same function the gate
                           # uses, so the package cannot disagree with the
                           # flip table about what a flip is.
                           "outcome": outcome(s.get("verdict")),
                           **s})

            mine = tool_calls.get(kk, [])
            for i, t in enumerate(mine, 1):
                rs = t.get("rankedSummary") or {}
                eids = rs.get("entityIds") or []
                calls.append({
                    "attempt_key": ak,
                    "run_id": run_id, "qid": qid, "sample": e.get("sample"),
                    "call_index": i, "tool": t.get("tool"),
                    "targets": t.get("targets"),
                    "n_returned": len(eids),
                    "entity_ids": eids[:40],
                    "error": t.get("error"),
                })

            # The one scoring implementation, shared with eval-answer.
            r = score_case(case or {"qid": qid, "coverage": "unknown",
                                    "expectedEntities": {}},
                           mine, kk, s.get("verdict"))
            retr.append({"attempt_key": ak, "run_id": run_id, **r})

            exp = case.get("expectedEntities") or {}
            required = {e for g in groups(exp) for e in g}
            acceptable = set(exp.get("acceptable") or []) | required
            got: dict[str, None] = {}
            mine_tokens: list[str] = []
            for t in mine:
                if t.get("tool") == "get_context":
                    rs = t.get("rankedSummary") or {}
                    for eid in rs.get("entityIds") or []:
                        got.setdefault(eid, None)
                    mine_tokens += rs.get("docTokens") or []

            # One implementation of "did this entity reach the answerer":
            # score_retrieval.delivery, fed the ranked ids plus the identifiers
            # named in returned source docs (from the ledger when the run
            # recorded them, else from the transcript walked above).
            tokens = {t for t in mine_tokens} | set(_IDENT.findall(docs))
            for eid in sorted(required):
                required_rows.append({
                    "attempt_key": ak, "run_id": run_id, "qid": qid,
                    "entity_id": eid,
                    "status": delivery(eid, set(got), tokens)})

            roles: list[tuple[str, str]] = []
            roles += [(eid, "required") for eid in sorted(required)]
            roles += [(eid, "returned") for eid in sorted(got)]
            if r["recall"] is not None:
                roles += [(eid, "missing") for eid in sorted(required - got.keys())]
                roles += [(eid, "noise") for eid in sorted(got.keys() - acceptable)]
            for eid, role in roles:
                kind, src, name = split_entity(eid)
                ents.append({"run_id": run_id, "qid": qid,
                             "sample": e.get("sample"), "entity_id": eid,
                             "entity_kind": kind, "entity_source": src,
                             "entity_name": name, "role": role})

    # Every arm's clusters, tagged by run. Reading only run_dirs[0] left the
    # second arm of an A/B with no clusters at all.
    cluster_rows, member_rows = [], []
    for rd in run_dirs:
        rid = read_json(rd / "run.json").get("runId") or rd.name
        # The diagnose agent's clusters when the run was diagnosed; otherwise
        # the mechanical candidates, labelled as such so nobody reads a
        # by-retrieval-outcome grouping as a diagnosis.
        src = rd / "clusters.jsonl"
        if not src.exists() and (rd / "clusters-mechanical.jsonl").exists():
            src = rd / "clusters-mechanical.jsonl"
            print(f"  ! {rid}: no diagnosis; clusters are the mechanical "
                  f"candidates (run diagnose.py for clusters of record)")
        for c in read_jsonl(src):
            c = {**c, "clusterId": f"{rid}|{c.get('clusterId')}", "run_id": rid}
            cluster_rows.append({
                "run_id": rid, "cluster_source": c.get("source") or "mechanical",
                "cluster_id": c.get("clusterId"), "label": c.get("label"),
            "where_to_fix": c.get("whereToFix"), "component": c.get("component"),
            "owner": c.get("owner"), "lever": c.get("lever"),
            "n_cases": len(c.get("qids") or []),
            "proposed_edit": c.get("proposedEdit"),
                "evidence": c.get("evidence"), "confidence": c.get("confidence"),
            })
            for qid in c.get("qids") or []:
                member_rows.append({"cluster_id": c.get("clusterId"), "qid": qid})
    if not cluster_rows:
        print("  ! no clusters.jsonl in any run: the data app's cluster views "
              "will be empty (run diagnose first)")

    write_csv(data / "runs.csv", runs, [
        "run_id", "label", "target", "model", "effort", "started",
        "judge_version", "set_version"])
    write_csv(data / "attempts.csv", attempts, [
        "attempt_key", "run_id", "qid", "sample", "phase", "submitted", "final_query",
        "answer_text", "n_get_context", "n_execute", "n_execute_errors",
        "host_tool_uses", "reported_calls", "contaminated", "servedRevision",
        "input_tokens", "output_tokens", "cache_read_tokens", "cost_usd",
        "num_turns", "wall_seconds", "run_error", "transcriptPath",
        "n_steps", "prediction"])
    write_csv(data / "steps.csv", steps_rows, [
        "attempt_key", "run_id", "qid", "step_index", "kind", "label", "detail",
        "n_results", "is_error"])
    write_csv(data / "required.csv", required_rows, [
        "attempt_key", "run_id", "qid", "entity_id", "status"])
    write_csv(data / "scores.csv", scores, [
        "attempt_key", "run_id", "qid", "sample", "phase", "verdict", "outcome",
        "reason", "confidence",
        "judge_version", "rubric_sha", "golden_revision", "gold_status",
        "contaminated", "artifactPath"])
    write_csv(data / "retrieval.csv", retr, [
        "attempt_key", "run_id", "qid", "sample", "phase", "coverage", "verdict", "failed",
        "recall", "precision", "n_required", "n_returned", "n_get_context",
        "missing", "noise", "component", "owner", "where_to_fix", "why"])
    write_csv(data / "calls.csv", calls, [
        "attempt_key", "run_id", "qid", "sample", "call_index", "tool", "targets",
        "n_returned", "entity_ids", "error"])
    write_csv(data / "entities.csv", ents, [
        "run_id", "qid", "sample", "entity_id", "entity_kind", "entity_source",
        "entity_name", "role"])
    write_csv(data / "issues.csv", issues, [
        "run_id", "qid", "component", "owner", "summary", "evidence", "status",
        "lever", "severity"])
    write_csv(data / "clusters.csv", cluster_rows, [
        "run_id", "cluster_source", "cluster_id", "label", "where_to_fix", "component", "owner", "lever",
        "n_cases", "proposed_edit", "evidence", "confidence"])
    write_csv(data / "cluster_members.csv", member_rows, ["cluster_id", "qid"])

    for name in ("publisher.json", "eval_run.malloy", "eval_run.malloynb"):
        if (TEMPLATE / name).exists():
            shutil.copy(TEMPLATE / name, out / name)
    if (TEMPLATE / "public").exists():
        shutil.copytree(TEMPLATE / "public", out / "public", dirs_exist_ok=True)

    return {"runs": len(runs), "cases": len(case_rows), "attempts": len(attempts),
            "scores": len(scores), "calls": len(calls), "entities": len(ents),
            "clusters": len(cluster_rows), "issues": len(issues),
            "steps": len(steps_rows)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="append", required=True, type=pathlib.Path,
                    help="run directory with events.jsonl; repeat for an A/B")
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    a = ap.parse_args(argv)

    counts = build(a.run, a.set_dir, a.out)
    print(f"{a.out}")
    print("  " + ", ".join(f"{v} {k}" for k, v in counts.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
