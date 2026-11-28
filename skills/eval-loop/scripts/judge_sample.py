#!/usr/bin/env python3
"""Judge a sample of production turns, and keep the verdicts. Stdlib only.

  python3 eval.py judge-sample pick    --set <set-dir> --label prod-20261125
  python3 eval.py judge-sample share-keys --set <set-dir> --label prod-20261125
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

WHAT IS DRAWN

- **Groups, not turns.** Turns asking the same question are one group
  (case and spacing ignored; a follow-up groups only with turns that asked it
  after the same earlier question). The key is derived once per group, and up
  to `--per-group` of its turns (default 3) are judged against it, because
  the same question can be answered right on one day and wrong on another.
  The group's other turns are shared out among the judged ones in turn and
  carry their verdict (`sampled_because: carried`, `judged_turn` naming the
  one judged). A demo prompt asked 45 times costs one key and three judge
  calls, and weighs 45 in the quality rate.
- **Signals first.** A group where the person's next message corrected the
  answer, or asked the same question again, is drawn before the rest. The
  signal is `user_signal` on the row and `context.nextMessage` on the case,
  for the judge to weigh. It is never the verdict.
- **Earlier messages.** Each case carries up to three earlier user messages
  of its session (`context.earlierMessages`, with a short excerpt of each
  answer). The judge says which the question needs (`depends_on`), and
  `build-set` turns that into a follow-up case.
- Up to N groups, uniform among the rest, seeded by the label, so the same
  label draws the same groups. Only a turn whose `kind` is `question` (or
  empty) is drawn: `build` edited the package, and `context` ran no query (a
  reply to the agent's clarifying question, a remark), so it has no answer to
  judge. Both still appear as earlier messages.
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
from score_retrieval import score_case  # noqa: E402
from build_set import normal  # noqa: E402
import hashlib  # noqa: E402

JUDGMENT_COLUMNS = APPENDED["prod_judgments"][2]
SAFE = re.compile(r"[^A-Za-z0-9_-]+")


def qid_for(turn_id: str) -> str:
    return "p-" + SAFE.sub("-", turn_id).strip("-")[:60]


MAX_EARLIER = 3
EXCERPT = 300
# What a person writes when the answer was wrong. Only a reason to judge the
# turn first; the judge decides.
CORRECTION = re.compile(
    r"\b(wrong|incorrect|not (right|correct|what i)|doesn'?t (look right|match|add up|seem right)"
    r"|that'?s not|is not right|isn'?t right|mistake|you missed|should (be|have)|are you sure)\b"
    r"|^(no|nope|actually)\b", re.I)


def sessions(turns: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    """Each session's question turns, oldest first. Build turns are kept: a
    follow-up of an edit request is not a data question."""
    out: dict[str, list[dict[str, str]]] = {}
    for t in turns:
        if t.get("question"):
            out.setdefault(t.get("session_id") or t["turn_id"], []).append(t)
    for ts in out.values():
        ts.sort(key=lambda t: (t.get("at") or "", t["turn_id"]))
    return out


def signal(turn: dict[str, str], nxt: dict[str, str] | None) -> str:
    """What the person's next message says about this answer: `asked_again`,
    `correction`, or ''."""
    if not nxt:
        return ""
    if normal(nxt["question"]) == normal(turn["question"]):
        return "asked_again"
    return "correction" if CORRECTION.search(nxt["question"].strip()) else ""


def answer_excerpt(transcript: str | None) -> str | None:
    """The last prose the agent wrote in a turn, cut short."""
    try:
        events = json.loads(transcript) if transcript else []
    except ValueError:
        return None
    text = ""
    for e in events if isinstance(events, list) else []:
        if e.get("type") == "assistant":
            parts = [c.get("text", "") for c in (e.get("message") or {}).get("content") or []
                     if isinstance(c, dict) and c.get("type") == "text"]
            text = "".join(parts).strip() or text
    text = " ".join(text.split())
    return (text[:EXCERPT] + "...") if len(text) > EXCERPT else (text or None)


def annotate(turns: list[dict[str, str]],
             transcripts: dict[str, str] | None = None) -> dict[str, dict[str, Any]]:
    """Per turn: its group, the signal in the next message, the next message,
    and the earlier messages of its session."""
    transcripts = transcripts or {}
    out: dict[str, dict[str, Any]] = {}
    for ts in sessions(turns).values():
        for i, t in enumerate(ts):
            prev = ts[i - 1]["question"] if i else ""
            key = normal(t["question"]) if not i else normal(prev) + "\n" + normal(t["question"])
            nxt = ts[i + 1] if i + 1 < len(ts) else None
            earlier = [{"question": e["question"], "kind": e.get("kind") or "question",
                        "answer": answer_excerpt(transcripts.get(e["turn_id"]))}
                       for e in ts[max(0, i - MAX_EARLIER):i]]
            out[t["turn_id"]] = {"group": hashlib.sha256(key.encode()).hexdigest()[:12],
                                 "signal": signal(t, nxt),
                                 "next": nxt["question"] if nxt else None,
                                 "earlier": earlier}
    return out


def pick(turns: list[dict[str, str]], judged: set[str], n: int, seed: str,
         notes: dict[str, dict[str, Any]] | None = None,
         per_group: int = 1) -> list[dict[str, Any]]:
    """Up to n groups of open turns (a question, not a build request, not yet
    judged): groups with a signal first, then uniformly. One entry per turn to
    judge, up to `per_group` of a group (its signalled turns first):
    {"turn", "members": the group's turns this one's verdict covers (itself
    first), "signal", "group", "first": the group's first entry}. Oldest
    first."""
    notes = notes or annotate(turns)
    groups: dict[str, list[dict[str, str]]] = {}
    for t in turns:
        if (t.get("question") and t["turn_id"] not in judged
                and (t.get("kind") or "question") == "question" and t["turn_id"] in notes):
            groups.setdefault(notes[t["turn_id"]]["group"], []).append(t)
    rng = random.Random(seed)
    flagged, plain = [], []
    for key in sorted(groups):
        members = sorted(groups[key], key=lambda t: (t.get("at") or "", t["turn_id"]))
        sig = [t for t in members if notes[t["turn_id"]]["signal"]]
        rest = [t for t in members if t not in sig]
        rng.shuffle(rest)
        reps = (sig + rest)[:max(1, per_group)]
        others = [t for t in members if t not in reps]
        entries = []
        for i, r in enumerate(reps):
            mine = [r] + others[i::len(reps)]
            entries.append({"turn": r, "members": mine, "group": key, "first": i == 0,
                            "signal": notes[r["turn_id"]]["signal"]})
        (flagged if sig else plain).append(entries)
    rng.shuffle(flagged)
    rng.shuffle(plain)
    chosen = [e for g in (flagged + plain)[:n] for e in g]
    return sorted(chosen, key=lambda g: (g["turn"].get("at") or "", g["turn"]["turn_id"]))


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


def write_set(dir_: pathlib.Path, label: str, chosen: list[dict[str, Any]],
              cfg_data: dict[str, Any], parent: pathlib.Path | None = None,
              notes: dict[str, dict[str, Any]] | None = None) -> int:
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
    n, notes = 0, notes or {}
    with (dir_ / "cases.jsonl").open("w", encoding="utf-8") as f:
        for grp in chosen:
            t = grp["turn"]
            # `dev`: diagnose never reads the `test` split, which a set holds
            # back so its reported number stays honest. A judging set is never
            # used to accept an edit, and diagnosing its failures is the point.
            case = {"qid": qid_for(t["turn_id"]), "question": t["question"],
                    "source": t["turn_id"], "split": "dev", "tags": []}
            note = notes.get(t["turn_id"]) or {}
            ctx = {k: v for k, v in (("earlierMessages", note.get("earlier")),
                                     ("nextMessage", note.get("next"))) if v}
            if ctx:
                case["context"] = ctx
            case["sourceFields"] = {"group": note.get("group"), "signal": grp.get("signal") or None,
                                    "members": [m["turn_id"] for m in grp["members"]],
                                    "keyHere": grp.get("first", True)}
            # A held key fits only a question that stands alone.
            g = None if note.get("earlier") else keys.get(normal(t["question"]))
            if g:
                case["golden"], n = g, n + 1
            f.write(json.dumps(case) + "\n")
    (dir_ / "set.json").write_text(json.dumps(meta, indent=1) + "\n")
    return n


def share_keys(set_dir: pathlib.Path) -> tuple[int, int]:
    """Give every case of a group the verified key derived on its first case
    (`sourceFields.keyHere`), with its expected entities and derivation
    record. Returns (cases given a key, groups whose first case has none)."""
    cases = read_jsonl(set_dir / "cases.jsonl")
    keyed = {(c.get("sourceFields") or {}).get("group"): c for c in cases
             if (c.get("sourceFields") or {}).get("keyHere")
             and (c.get("golden") or {}).get("status") == "verified"}
    given, missing = 0, set()
    for c in cases:
        sf = c.get("sourceFields") or {}
        if sf.get("keyHere") or c.get("golden"):
            continue
        src = keyed.get(sf.get("group"))
        if not src:
            missing.add(sf.get("group"))
            continue
        c["golden"] = src["golden"]
        for k in ("expectedEntities", "expectedSearches"):
            if k in src:
                c[k] = src[k]
        gold = set_dir / "gold" / f"{src['qid']}.json"
        if gold.is_file():
            rec = json.loads(gold.read_text())
            rec["qid"] = c["qid"]
            (set_dir / "gold" / f"{c['qid']}.json").write_text(json.dumps(rec, indent=1) + "\n")
        given += 1
    (set_dir / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
    return given, len(missing)


def write_transcripts(run: pathlib.Path, chosen: list[dict[str, Any]],
                      transcripts: dict[str, str]) -> int:
    n = 0
    for grp in chosen:
        t = grp["turn"]
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
    # Did get_context deliver what the key's query used: recall over the
    # case's expectedEntities, scored from the transcript's tool calls.
    recall: dict[str, float] = {}
    for c in cases:
        if not (c.get("expectedEntities") or {}).get("required") and not (
                c.get("expectedEntities") or {}).get("requiredAnyOf"):
            continue
        phase = (attempts.get(c["qid"]) or {}).get("phase") or "baseline"
        try:
            r = score_case(c, events, (c["qid"], None, phase), None)
        except Exception:  # noqa: BLE001 - a scoring failure leaves the cell empty
            continue
        if r.get("recall") is not None:
            recall[c["qid"]] = r["recall"]
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
        sf = c.get("sourceFields") or {}
        earlier = (c.get("context") or {}).get("earlierMessages") or []
        deps = [earlier[d - 1]["question"] for d in s.get("depends_on") or []
                if 0 < d <= len(earlier)]
        r = recall.get(c["qid"])
        row = {"turn_id": c["source"], "judged_at": now, "run_id": run.name,
               "judge_version": judge, "metric_version": metric,
               "sampled_because": "user_signal" if sf.get("signal") else "random",
               "verdict": v or "not judged", "right": right,
               "failure_code": i.get("primary_code") if right is False else None,
               "component": i.get("component") if right is False else None,
               "owner": i.get("owner") if right is False else None,
               "note": None if v else "no score: the golden was not established, or the "
                                      "case did not run",
               "basis": basis(a), "question_group": sf.get("group"),
               "judged_turn": c["source"], "user_signal": sf.get("signal"),
               "depends_on": json.dumps(deps) if deps else None,
               "key_kind": (c.get("golden") or {}).get("kind"),
               "retrieval_recall": round(r, 4) if r is not None else None}
        out.append(row)
        # The rest of the group: same question, same key, same verdict.
        for m in sf.get("members") or []:
            if m != c["source"]:
                out.append({**row, "turn_id": m, "sampled_because": "carried",
                            "user_signal": None})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("verb", choices=("pick", "share-keys", "collect"))
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--label", required=True, help="names the sample, its set and its run")
    ap.add_argument("--n", type=int, default=None,
                    help="question groups to draw ([optimization] judge_sample)")
    ap.add_argument("--per-group", type=int, default=3,
                    help="turns of one group judged against its one key")
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
        transcripts = {r["turn_id"]: r["transcript"] for r in read(data / "prod_transcripts.csv")}
        notes = annotate(turns, transcripts)
        chosen = pick(turns, judged, n, a.label, notes, a.per_group)
        if not chosen:
            print("every ingested turn is already judged")
            return 0
        keyed = write_set(set_dir, a.label, chosen, judging_config(cfg, set_dir),
                          pathlib.Path(a.set_dir), notes)
        have = write_transcripts(run, chosen, transcripts)
        weight = sum(len(g["members"]) for g in chosen)
        flagged = sum(1 for g in chosen if g["signal"])
        groups_n = sum(1 for g in chosen if g.get("first", True))
        print(f"{groups_n} question groups drawn ({len(chosen)} turns to judge), covering {weight} turns "
              f"({len(turns) - len(judged)} not yet judged); {flagged} drawn for a user signal; "
              f"{have} transcripts written to {run / 'artifacts'}")
        if have < len(chosen):
            print(f"  {len(chosen) - have} need fetch_transcripts.py --set {set_dir} --out {run}")
        print(f"{keyed} of {len(chosen)} ask a question the set already keys and took its "
              f"verified key")
        print(f"next: derive a golden for each case with sourceFields.keyHere and no golden "
              f"(skill:eval-import), then\n"
              f"  python3 eval.py judge-sample share-keys --set {a.set_dir} --label {a.label}\n"
              f"  python3 eval.py run --set {set_dir} --label {a.label} --rejudge\n"
              f"  python3 eval.py diagnose --set {set_dir} --label {a.label}\n"
              f"  python3 eval.py judge-sample collect --set {a.set_dir} --label {a.label}")
        print(set_dir)
        return 0
    if a.verb == "share-keys":
        given, missing = share_keys(set_dir)
        print(f"{given} case(s) took their group's key"
              + (f"; {missing} group(s) have no verified key on their first case yet" if missing else ""))
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
