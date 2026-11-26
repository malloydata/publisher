#!/usr/bin/env python3
"""Judge a sample of production turns, and keep the verdicts. Stdlib only.

  python3 eval.py judge-sample pick    --set <set-dir> --label prod-20261125
  python3 eval.py judge-sample collect --set <set-dir> --label prod-20261125

Step H2 of the optimization loop (reference/optimization-loop.md): the
production quality number. Each night, N turns not yet judged are drawn from
`prod_turns` (written by `eval.py ingest`), judged, and the verdicts appended
to `prod_judgments`. The history model's quality rate is computed from those
rows, so it measures production, not the eval set.

A production turn comes with no answer key. This script does not make one and
does not judge; both are existing steps an agent runs between the two verbs:

1. `pick` draws the sample and writes it as an eval set of its own at
   `<workdir>/judging/<label>/`: one case per turn, its `source` the turn id,
   no golden, and an `eval.json` copied from this set's config with its own
   work directory (`judging/<label>/work`) and no store. So the judging run
   is never one of this set's runs, is never shared as one, and never sits
   inside the optimization package, where a run's model snapshot would stop
   the package loading. Where `prod_transcripts` holds the turn's transcript
   (as answerer events, the shape `log_transcript.py` writes), it is written
   to the judging run's `artifacts/<qid>/answerer.jsonl`; otherwise
   `fetch_transcripts.py` fetches it. Build a production transcript at tier
   T2 (the answer was logged, the agent's isolation was not): the production
   agent was never isolated from an answer key, and T3 voids every turn for
   want of the start-up event a local answerer writes.
2. The agent derives each case's golden from the data (skill:eval-import),
   then runs `eval.py run --set <judging set> --label <label> --rejudge` to
   judge the logged answers against them, and `eval.py diagnose` for the
   failures. `--rejudge`, not `--rebuild`: a rebuild reuses saved verdicts,
   and a production turn has none, so every turn would come back unjudged.
3. `collect` reads that run and appends one row per turn to `prod_judgments`:
   the verdict, and for a wrong answer the diagnosed reason and owner.

A turn whose golden could not be established, or whose verdict is neither a
match nor a miss, is kept with `right` empty: it is left out of the rate and
counted, never guessed. Rows are only appended. A new judge or rubric gives a
new `metric_version`, and the history model never mixes two.

The sample is uniform over the turns not yet judged, seeded by the label, so
the same label picks the same turns.
"""
from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import random
import re
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from optimization_package import APPENDED, append, install_for, metric_version, read  # noqa: E402
from ledger import read_jsonl  # noqa: E402
from build_set import normal  # noqa: E402

JUDGMENT_COLUMNS = APPENDED["prod_judgments"][2]
SAFE = re.compile(r"[^A-Za-z0-9_-]+")


def qid_for(turn_id: str) -> str:
    return "p-" + SAFE.sub("-", turn_id).strip("-")[:60]


def pick(turns: list[dict[str, str]], judged: set[str], n: int, seed: str) -> list[dict[str, str]]:
    """N turns with a question and no judgment yet, drawn uniformly, oldest first."""
    open_ = [t for t in turns if t.get("question") and t["turn_id"] not in judged]
    chosen = random.Random(seed).sample(open_, min(n, len(open_)))
    return sorted(chosen, key=lambda t: (t.get("at") or "", t["turn_id"]))


def judging_config(cfg: Any, dir_: pathlib.Path) -> dict[str, Any]:
    """This set's config for the judging set: the same servers, its own work
    directory, no store and no optimization section."""
    out: dict[str, Any] = {}
    for section, values in cfg.data.items():
        if section in ("store", "optimization"):
            continue
        out[section] = {k: str(v) if isinstance(v, pathlib.Path) else v for k, v in values.items()}
    out.setdefault("paths", {})["workdir"] = str(dir_ / "work")
    return out


# What a judging set takes from the set it samples for: where its keys
# re-derive, which model they describe, and the conventions the judge reads.
INHERITED = ("truthPackage", "truthModel", "targetPackage", "targetModelPath", "conventions")


def held_keys(parent: pathlib.Path) -> dict[str, dict[str, Any]]:
    """The parent set's verified goldens, by question (case and spacing
    ignored). A production turn asking a question the set already keys is
    judged against that key, so an agent derives keys only for new ones."""
    path = parent / "cases.jsonl"
    if not path.is_file():
        return {}
    out = {}
    for c in read_jsonl(path):
        g = c.get("golden") or {}
        if g.get("status") == "verified":
            out[normal(c.get("question", ""))] = g
    return out


def write_set(dir_: pathlib.Path, label: str, chosen: list[dict[str, str]],
              cfg_data: dict[str, Any], parent: pathlib.Path | None = None) -> int:
    """Write the judging set. Returns how many cases took a key from `parent`."""
    dir_.mkdir(parents=True, exist_ok=True)
    (dir_ / "eval.json").write_text(json.dumps(cfg_data, indent=1) + "\n")
    meta = {"name": f"production-{label}", "datasetVersion": 1,
            "description": "Production turns drawn for judging by judge_sample.py. "
                           "Each case's source is the turn id. A question the sampled set "
                           "already keys takes that verified key; the rest are derived."}
    keys: dict[str, dict[str, Any]] = {}
    if parent is not None:
        if (parent / "set.json").is_file():
            p = json.loads((parent / "set.json").read_text())
            meta.update({k: p[k] for k in INHERITED if k in p})
        keys = held_keys(parent)
    n = 0
    with (dir_ / "cases.jsonl").open("w", encoding="utf-8") as f:
        for t in chosen:
            # `dev`: diagnose never reads the `test` split, which a set holds
            # back so its reported number stays honest. A judging set is never
            # used to accept an edit, and diagnosing its failures is the point.
            case = {"qid": qid_for(t["turn_id"]), "question": t["question"],
                    "source": t["turn_id"], "split": "dev", "tags": []}
            g = keys.get(normal(t["question"]))
            if g:
                case["golden"], n = g, n + 1
            f.write(json.dumps(case) + "\n")
    (dir_ / "set.json").write_text(json.dumps(meta, indent=1) + "\n")
    return n


def write_transcripts(run: pathlib.Path, chosen: list[dict[str, str]],
                      transcripts: dict[str, str]) -> int:
    n = 0
    for t in chosen:
        text = transcripts.get(t["turn_id"])
        try:
            events = json.loads(text) if text else None
        except ValueError:
            events = None
        if not isinstance(events, list):
            continue
        d = run / "artifacts" / qid_for(t["turn_id"])
        d.mkdir(parents=True, exist_ok=True)
        (d / "answerer.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
        n += 1
    return n


def basis(attempt: dict[str, Any]) -> str | None:
    """What a verdict judged. `answer`: the prose the person saw. `final_query`:
    the log held no prose (an MCP or API client logs only its calls), so the
    judge read the last query that succeeded and its re-run rows. The two are
    different numbers and the history model never averages them."""
    if (attempt.get("answer_text") or "").strip():
        return "answer"
    return "final_query" if attempt.get("final_query") else None


def judgments(run: pathlib.Path, cases: list[dict[str, Any]], now: str) -> list[dict[str, Any]]:
    """One row per case of the judging run, from its score and issue events."""
    meta = json.loads((run / "run.json").read_text())
    events = read_jsonl(run / "events.jsonl") if (run / "events.jsonl").is_file() else []
    judge, metric = meta.get("judgeVersion"), metric_version(meta)
    scores = {e.get("qid"): e for e in events if e.get("kind") == "score"}
    attempts = {e.get("qid"): e for e in events if e.get("kind") == "attempt"}
    issues: dict[str, dict[str, Any]] = {}
    for e in events:
        if e.get("kind") == "issue":
            for q in e.get("qids") or []:
                issues.setdefault(q, e)
    out = []
    for c in cases:
        s, i = scores.get(c["qid"]) or {}, issues.get(c["qid"]) or {}
        a = attempts.get(c["qid"]) or {}
        v = s.get("verdict")
        right = True if v == "match" else False if v == "no_match" else None
        out.append({"turn_id": c["source"], "judged_at": now, "run_id": run.name,
                    "judge_version": judge, "metric_version": metric, "sampled_because": "random",
                    "verdict": v or "not judged", "right": right,
                    "failure_code": i.get("primary_code") if right is False else None,
                    "component": i.get("component") if right is False else None,
                    "owner": i.get("owner") if right is False else None,
                    "note": None if v else "no score: the golden was not established, or the "
                                           "case did not run",
                    "basis": basis(a)})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("verb", choices=("pick", "collect"))
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--label", required=True, help="names the sample, its set and its run")
    ap.add_argument("--n", type=int, default=None, help="turns to draw ([optimization] judge_sample)")
    ap.add_argument("--without-diagnosis", action="store_true",
                    help="collect: keep wrong answers the run has not diagnosed, with no reason")
    a = ap.parse_args(argv)
    cfg = config.load(pathlib.Path(a.set_dir))
    data = install_for(cfg)
    set_dir = cfg.workdir() / "judging" / a.label
    run = set_dir / "work" / "runs" / a.label
    if a.verb == "pick":
        turns = read(data / "prod_turns.csv")
        if not turns:
            raise SystemExit(f"No production turns in {data}. Fix: eval.py ingest --turns <file> first.")
        judged = {r["turn_id"] for r in read(data / "prod_judgments.csv")}
        n = a.n or cfg.get("optimization", "judge_sample")
        chosen = pick(turns, judged, n, a.label)
        if not chosen:
            print("every ingested turn is already judged")
            return 0
        keyed = write_set(set_dir, a.label, chosen, judging_config(cfg, set_dir),
                          pathlib.Path(a.set_dir))
        transcripts = {r["turn_id"]: r["transcript"] for r in read(data / "prod_transcripts.csv")}
        have = write_transcripts(run, chosen, transcripts)
        print(f"{len(chosen)} turns drawn ({len(turns) - len(judged)} not yet judged); "
              f"{have} transcripts written to {run / 'artifacts'}")
        if have < len(chosen):
            print(f"  {len(chosen) - have} need fetch_transcripts.py --set {set_dir} --out {run}")
        print(f"{keyed} of {len(chosen)} ask a question the set already keys and took its "
              f"verified key")
        print(f"next: derive a golden for each of the other {len(chosen) - keyed} "
              f"(skill:eval-import), then\n"
              f"  python3 eval.py run --set {set_dir} --label {a.label} --rejudge\n"
              f"  python3 eval.py diagnose --set {set_dir} --label {a.label}\n"
              f"  python3 eval.py judge-sample collect --set {a.set_dir} --label {a.label}")
        print(set_dir)
        return 0
    if not (run / "run.json").is_file():
        raise SystemExit(f"No judging run at {run}. Fix: eval.py run --set {set_dir} "
                         f"--label {a.label} --rejudge")
    cases = read_jsonl(set_dir / "cases.jsonl")
    done = {(r["turn_id"], r["metric_version"]) for r in read(data / "prod_judgments.csv")}
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    rows = [r for r in judgments(run, cases, now)
            if (r["turn_id"], r["metric_version"] or "") not in done]
    undiagnosed = [r["turn_id"] for r in rows if r["right"] is False and not r["failure_code"]]
    if undiagnosed and not a.without_diagnosis:
        # Rows are only appended, so a wrong answer collected before its
        # diagnosis keeps an empty reason for good.
        raise SystemExit(f"{len(undiagnosed)} wrong answer(s) have no diagnosis yet, and a "
                         f"collected row is never rewritten. Fix: eval.py diagnose --set "
                         f"{set_dir} --label {a.label}, then collect again (or pass "
                         f"--without-diagnosis to keep them with no reason)")
    append(data / "prod_judgments.csv", JUDGMENT_COLUMNS, rows)
    decided = [r for r in rows if r["right"] is not None]
    right = sum(1 for r in decided if r["right"])
    print(f"prod_judgments: added {len(rows)}; {right}/{len(decided)} right, "
          f"{len(rows) - len(decided)} undecided")
    return 0


if __name__ == "__main__":
    sys.exit(main())
