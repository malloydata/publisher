#!/usr/bin/env python3
"""Speed findings from a query replay, one issue per source. Stdlib only.

  python3 diagnose.py --run <query-replay> --set <set-dir>   # calls this; no agent runs

A query replay (eval-loop's query_replay.py) has no verdicts to explain, only
timings, and every finding about them is a fact read off the replay. So nothing
here spawns an agent. Each source whose queries need attention gets one `issue`
event, component `performance`, owner `model` (a materialization lives in the
model's annotations), with the codes skill:eval-diagnose defines:

  PERSIST-REFUSED    a query reads a source whose `#@ persist` the build plan refuses
  PERSIST-NOT-BUILT  a query reads a persisted source with no bound table
  SLOW-QUERY         a query's median is over `budget_ms`, and neither of the above
  LIVE-FALLBACK      `servedFrom: live_fallback`: the stored table failed and the query ran live
  HOT-SOURCE         the source the replay spent most of its time on, when it is
                     not persisted and none of the above names it. Over no
                     budget, so nothing is wrong: it is where a persist would
                     pay most, if one pays at all

The issues are appended to the replay's events.jsonl, replacing any this script
wrote before, so it can be re-run.
"""
from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import ledger  # noqa: E402

BY = "perf_findings.py"


def findings(run: pathlib.Path) -> list[dict[str, Any]]:
    meta = json.loads((run / "run.json").read_text())
    mat = meta.get("materialization") or {}
    budget = meta.get("budgetMs")
    rows = [r for r in ledger.read_jsonl(run / "results.jsonl") if not r.get("error")]
    persisted, refused = set(mat.get("persistSources") or []), set(mat.get("refusedSources") or [])
    bound = mat.get("bound")
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        src = r.get("source") or "(other)"
        # A persist the build refused or never ran is a finding whatever the
        # timing: under budget today is not built, and the replay already
        # warned. Every query on the source is in the group, not only slow ones.
        if r.get("slow") or src in refused or (src in persisted and not bound):
            groups.setdefault(src, []).append(r)
    out = []
    for src, rs in sorted(groups.items(), key=lambda kv: -sum(r["median_ms"] for r in kv[1])):
        slow = sum(1 for r in rs if r.get("slow"))
        if src in refused:
            code, why = "PERSIST-REFUSED", (f"`{src}` declares `#@ persist`, and the build plan "
                                            "refuses it, so it computes live on every query. The "
                                            "refusal's message says what to change.")
        elif src in persisted and not bound:
            code, why = "PERSIST-NOT-BUILT", (f"`{src}` is persisted, but no table was bound when "
                                              "the replay ran. Build, then replay again before "
                                              "changing the model.")
        else:
            code, why = "SLOW-QUERY", (f"queries on `{src}` take over {budget} ms. Candidate: "
                                       "persist or preaggregate it (the malloy-materialization skill), "
                                       "after reading its build cost.")
        worst = max(r["median_ms"] for r in rs)
        out.append(ledger.event(
            "issue", issue_id=f"perf-{src}", qids=sorted(r["qid"] for r in rs),
            primary_code=code, component="performance", owner="model",
            diagnosis=(f"{len(rs)} quer{'y' if len(rs) == 1 else 'ies'} on `{src}`, "
                       f"{slow} over the budget; {why}"),
            severity="high" if budget and worst > 4 * budget else "medium",
            confidence="high", diagnosedBy=BY,
            evidence={"source": src, "budgetMs": budget,
                      "medianMs": {r["qid"]: r["median_ms"] for r in rs},
                      "packageVersion": (meta.get("packageVersion") or {}).get("id")}))
    fallback: dict[str, list[str]] = {}
    for r in rows:
        if "live_fallback" in (r.get("served_from") or []):
            fallback.setdefault(r.get("source") or "(other)", []).append(r["qid"])
    for src, qids in sorted(fallback.items()):
        out.append(ledger.event(
            "issue", issue_id=f"perf-fallback-{src}", qids=sorted(qids),
            primary_code="LIVE-FALLBACK", component="performance", owner="model",
            diagnosis=(f"the stored table for `{src}` failed at query time and the query ran "
                       "live. Read the server log's storage lines before the model."),
            severity="medium", confidence="high", diagnosedBy=BY,
            evidence={"source": src}))
    hot = hot_source(rows, persisted | refused, {(i.get("evidence") or {}).get("source")
                                                 for i in out})
    if hot:
        src, ms, share, qids = hot
        out.append(ledger.event(
            "issue", issue_id=f"perf-hot-{src}", qids=qids,
            primary_code="HOT-SOURCE", component="performance", owner="model",
            diagnosis=(f"`{src}` takes {share:.0%} of the replay's time ({ms:g} ms across "
                       f"{len(qids)} quer{'y' if len(qids) == 1 else 'ies'}, each counted as "
                       f"often as it ran) and is not persisted. No query is over a budget. "
                       f"Candidate: persist or preaggregate it, if the time saved is worth a "
                       f"build on every refresh."),
            severity="low", confidence="medium", diagnosedBy=BY,
            evidence={"source": src, "weightedMs": ms, "share": round(share, 3)}))
    return out


HOT_SHARE = 0.3


def hot_source(rows: list[dict[str, Any]], persisted: set[str], named: set[Any]
               ) -> tuple[str, float, float, list[str]] | None:
    """The unpersisted source with the largest share of the replay's time, when
    that share is at least HOT_SHARE and no other finding names it.

    Time is each query's median by how often it ran (`count`, 1 unless the
    replay came from a workload file). A query whose source could not be read
    off its text is left out: there is nothing to annotate.
    """
    cost: dict[str, float] = {}
    qids: dict[str, list[str]] = {}
    for r in rows:
        src = r.get("source")
        ms = (r.get("median_ms") or 0) * (r.get("count") or 1)
        if src:
            cost[src] = cost.get(src, 0.0) + ms
            qids.setdefault(src, []).append(r["qid"])
    total = sum((r.get("median_ms") or 0) * (r.get("count") or 1) for r in rows)
    if not cost or total <= 0:
        return None
    src = max(cost, key=lambda s: (cost[s], s))
    share = cost[src] / total
    if src in persisted or src in named or share < HOT_SHARE:
        return None
    return src, round(cost[src], 1), share, sorted(qids[src])


def write(run: pathlib.Path) -> list[dict[str, Any]]:
    issues = findings(run)
    ledger.replace_events(run / "events.jsonl",
                          lambda e: not (e.get("kind") == "issue" and e.get("diagnosedBy") == BY),
                          issues)
    return issues


def report(run: pathlib.Path, issues: list[dict[str, Any]]) -> None:
    meta = json.loads((run / "run.json").read_text())
    if not meta.get("budgetMs"):
        print("no budget_ms in [performance] or --budget-ms on the replay, so no query is slow; "
              "only live fallbacks are reported.")
    for i in issues:
        print(f"  {i['primary_code']:18s} {i['issue_id']:32s} {', '.join(i['qids'])[:60]}")
    print(f"{len(issues)} speed issue(s) written to {run / 'events.jsonl'}")
