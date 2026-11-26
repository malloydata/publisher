#!/usr/bin/env python3
"""Build eval cases and a query workload from production history. Stdlib only.

  python3 eval.py build-set --set <set-dir> --label prod-20261126 [--questions 20]

Step O1 of the optimization loop (reference/optimization-loop.md): the loop
improves the model on what people actually asked and ran, read from the
history model's tables (`eval.py ingest`, `eval.py judge-sample`).

Two outputs:

- **Cases**, appended to the set's `cases.jsonl`. One case per distinct
  question (case and spacing ignored), chosen in this order:
    1. asked in a turn production judged wrong,
    2. asked in a turn judged but left undecided,
    3. asked most often,
  until `--questions` are chosen. A turn whose `kind` is `build` (an edit
  request, not a data question) is never chosen. A question the set already
  holds is skipped. Each case goes to the `dev` split, tagged `production`
  and with why it was chosen, and carries no golden: `skill:eval-import`
  derives one, drops a question that only makes sense after an earlier turn,
  then `import_cases.py --stamp` seals the set. Until then the case is
  counted as having no key and never scored.
- **A workload**, `<set>/workloads/<label>.jsonl`: every production query of
  the served package that succeeded, one line per distinct query with how
  often it ran, most frequent first, in the shape `query-replay --queries`
  and `simulate` read (workload_from_logs.py, the one place that shape is
  built).

The question text is kept exactly as asked. Nothing is invented and nothing
is rewritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from optimization_package import install_for, read  # noqa: E402
from workload_from_logs import workload  # noqa: E402

WHY = ("judged wrong", "judged, undecided", "asked often")


def normal(q: str) -> str:
    return " ".join((q or "").lower().split())


def qid_for(q: str) -> str:
    return "prod-" + hashlib.sha256(normal(q).encode()).hexdigest()[:10]


def choose(turns: list[dict[str, str]], judgments: list[dict[str, str]],
           held: set[str], n: int) -> list[dict[str, Any]]:
    """Distinct questions in the order the docstring gives, at most n."""
    verdict: dict[str, str] = {}
    for j in judgments:
        # A wrong verdict is never hidden by a later undecided one.
        if j.get("right") == "False" or j["turn_id"] not in verdict:
            verdict[j["turn_id"]] = j.get("right") or ""
    groups: dict[str, dict[str, Any]] = {}
    for t in turns:
        key = normal(t.get("question", ""))
        if not key or (t.get("kind") or "") == "build" or key in held:
            continue
        g = groups.setdefault(key, {"question": t["question"].strip(), "turns": [], "wrong": 0,
                                    "undecided": 0, "last": ""})
        g["turns"].append(t["turn_id"])
        g["last"] = max(g["last"], t.get("at") or "")
        v = verdict.get(t["turn_id"])
        g["wrong"] += v == "False"
        g["undecided"] += v == ""
    for g in groups.values():
        g["why"] = WHY[0] if g["wrong"] else WHY[1] if g["undecided"] else WHY[2]
    order = sorted(groups.values(), key=lambda g: (WHY.index(g["why"]), -g["wrong"],
                                                   -len(g["turns"]), g["last"]), reverse=False)
    return order[:n]


def case(g: dict[str, Any], label: str) -> dict[str, Any]:
    return {"qid": qid_for(g["question"]), "question": g["question"], "split": "dev",
            "tags": ["production", g["why"].replace(", ", "-").replace(" ", "-")],
            "source": f"production:{label}",
            "sourceFields": {"asked": len(g["turns"]), "judgedWrong": g["wrong"],
                             "lastAsked": g["last"], "turns": g["turns"][:5]}}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--label", required=True, help="names this pull in each case's source and the workload file")
    ap.add_argument("--questions", type=int, default=20, help="distinct questions to add at most")
    a = ap.parse_args(argv)
    set_dir = pathlib.Path(a.set_dir)
    cfg = config.load(set_dir)
    package = cfg.get("model", "package")
    data = install_for(cfg)
    cases_path = set_dir / "cases.jsonl"
    existing = [json.loads(l) for l in cases_path.read_text().splitlines() if l.strip()] \
        if cases_path.is_file() else []
    held = {normal(c.get("question", "")) for c in existing}
    turns = read(data / "prod_turns.csv")
    if not turns:
        raise SystemExit(f"No production turns in {data}. Fix: eval.py ingest --set {set_dir} --turns turns.jsonl")
    chosen = choose(turns, read(data / "prod_judgments.csv"), held, a.questions)
    new = [case(g, a.label) for g in chosen]
    with cases_path.open("a", encoding="utf-8") as f:
        for c in new:
            f.write(json.dumps(c) + "\n")
    builds = sum(1 for t in turns if (t.get("kind") or "") == "build")
    print(f"cases: added {len(new)} to {cases_path}"
          + (f" ({', '.join(f'{sum(c['tags'][1] == w.replace(', ', '-').replace(' ', '-') for c in new)} {w}' for w in WHY)})" if new else "")
          + f"; skipped {builds} build turns and {len(held)} questions the set holds")

    rows = [{"query": q["query_text"], "modelPath": q["model_path"], "package": q["package"]}
            for q in read(data / "prod_queries.csv")
            if q.get("package") == package and q.get("status") in ("ok", "200", "")]
    items = ({"query": r["query"], "modelPath": r["modelPath"], "count": 1} for r in rows)
    lines, skipped = workload(items)
    out = set_dir / "workloads" / f"{a.label}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(w) + "\n" for w in lines))
    print(f"workload: {len(lines)} distinct queries from {len(rows)} that succeeded -> {out}"
          + (f"; left out {skipped}" if skipped else ""))
    if new:
        print("next: derive each new case's golden (skill:eval-import), then "
              f"python3 skills/eval-import/scripts/import_cases.py --set {set_dir} --stamp")
    return 0


if __name__ == "__main__":
    sys.exit(main())
