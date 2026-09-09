#!/usr/bin/env python3
"""Re-judge frozen predictions and check the judge still agrees with a human.

Everything else in this loop measures the model. This measures the *judge*, which
nothing else does. An A/A band shows only that the judge is repeatable, and a
judge that answered `no_match` every time would post a perfect band. Repeatable
and correct are different properties and only this asserts the second.

The unit is a PREDICTION, not a question. One qid legitimately earns different
verdicts for different answers -- in the seed set `ecom_levis_customers` is a
match at 27016 and a no_match at 27041, because one filtered `= 'Levi\\'s'` and
the other `~ r'Levi'`. A fixture keyed on qid alone would assert something false.

It judges through `run_baseline.run_judge`, the same function a real run uses,
so a prompt or input change that the run would see is a change this sees too. A
reimplementation here could pass while the thing it stands for was broken.

What a failure means, likeliest first:

1. You changed judge.md, a rubric, or the judge's inputs and moved a verdict you
   did not mean to move.
2. The fixture is wrong. It is a human judgement and humans are wrong. Re-settle
   it and say why -- do not delete it. The case earned its place by being
   contested once and will be again.
3. Judge nondeterminism on identical input. `--repeat 3` separates this from the
   first two; do not touch anything until you know which you have.

Run after any edit to judge.md, to a rubric, or to what the judge is given:

    python3 check_judge.py --set <set-dir>
    python3 check_judge.py --set <set-dir> --repeat 3
"""
from __future__ import annotations

import argparse
import collections
import json
import pathlib
import shutil
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import run_baseline as rb  # noqa: E402


def read_jsonl(p: pathlib.Path) -> list[dict]:
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--fixtures", type=pathlib.Path, default=None,
                    help="default: <set>/judge-regressions.jsonl")
    ap.add_argument("--publisher", default="http://localhost:4811")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--package", default=None, help="default: from set.json")
    ap.add_argument("--model-path", default=None, help="default: from set.json")
    ap.add_argument("--judge-model", default="sonnet")
    ap.add_argument("--repeat", type=int, default=1,
                    help="judge each fixture N times, to tell a real regression "
                         "from judge nondeterminism")
    ap.add_argument("--only", default=None, help="comma-separated qids")
    ap.add_argument("--out", type=pathlib.Path, default=None)
    a = ap.parse_args(argv)
    # run_baseline.run_judge reads two attributes off this namespace that the
    # CLI never defined, so the checker died on its first fixture with an
    # AttributeError -- which is how the only thing that checks the judge came
    # to be unrunnable. `rebuild`/`rejudge` are set below; these are the rest.
    # The SAME skills a run gives its judge. Empty here would hand the judge
    # nothing at all now that the doctrine is skill:eval-judge rather than a
    # pasted file -- which is exactly what happened, and every fixture came
    # back with no parseable verdict.
    a.judge_skills = list(rb.JUDGE_SKILLS)
    a.roots = rb.skills_roots(None)

    fx_path = a.fixtures or (a.set_dir / "judge-regressions.jsonl")
    if not fx_path.exists():
        print(f"no fixture file at {fx_path}, so nothing asserts this judge is "
              f"correct rather than merely repeatable. Seed one from the cases "
              f"an A/A pair disagreed on.")
        return 0

    cfg = json.loads((a.set_dir / "set.json").read_text())
    a.package = a.package or cfg.get("package") or "ecommerce"
    # `targetModelPath` is the set.json key (ledger-schema.md:67) and what
    # run_baseline.py reads. `modelPath` is a real key ELSEWHERE -- on a case,
    # and on a run event -- so the wrong name here read as correct while always
    # missing, silently checking the judge against a different model than the
    # set configures. Default matches run_baseline's for the same reason.
    a.model_path = (a.model_path or cfg.get("targetModelPath")
                    or "model.malloy")
    # run_judge reads these off the namespace; a fixture check never rebuilds
    # from a cache and always re-judges, which is the entire point of it.
    a.rebuild, a.rejudge = False, True

    fixtures = read_jsonl(fx_path)
    if a.only:
        want = {x.strip() for x in a.only.split(",")}
        fixtures = [f for f in fixtures if f["qid"] in want]
    cases = {c["qid"]: c for c in read_jsonl(a.set_dir / "cases.jsonl")}

    judge_md = HERE.parent.parent / "eval-judge" / "SKILL.md"
    jv, rsha = rb.judge_pins(judge_md)
    JUDGE_MD = ""   # the judge LOADS the skill; this arg only pins it
    served = rb.served_model_path(a.publisher, a.environment, a.package, a.model_path)
    model_src = served.read_text() if served else ""
    if not model_src:
        print("warning: Publisher is not serving this model, so predictions "
              "cannot be re-executed and the judge sees prose only -- the same "
              "degraded input a real run would flag. Verdicts may differ for "
              "that reason alone.")

    print(f"{len(fixtures)} fixtures | judge v{jv} rubric {(rsha or '')[:8]} | "
          f"model {rb.sha256(model_src.encode())[:8] if model_src else 'unserved'}"
          + (f" | {a.repeat}x" if a.repeat > 1 else ""))

    rows, unresolved = [], []
    for f in fixtures:
        c = cases.get(f["qid"])
        if not c:
            unresolved.append((f, "qid is no longer in cases.jsonl"))
            continue
        att = {"answer_text": f["prediction"].get("answer_text") or "",
               "final_query": f["prediction"].get("final_query") or "",
               "queries": f["prediction"].get("queries") or []}
        got = []
        for _ in range(a.repeat):
            # Fresh artifact root per judgement: run_judge caches the
            # re-execution and the verdict per qid, and a cache hit would have
            # this assert nothing at all.
            art = pathlib.Path(tempfile.mkdtemp(prefix="judgefix-"))
            (art / f["qid"]).mkdir(parents=True, exist_ok=True)
            try:
                # Same prompt a run builds. This argument used to be the CASE
                # rubric, which the prompt also supplies as `rubric_note`, so
                # the judge saw it twice and the judge doctrine not at all --
                # the failure this file's docstring warns about. Now the judge
                # doctrine is skill:eval-judge, installed for both paths, and
                # this argument carries nothing.
                v = rb.run_judge(c, att, a, art, JUDGE_MD, model_src,
                                 bool(model_src))
                got.append(v.get("verdict"))
            finally:
                shutil.rmtree(art, ignore_errors=True)

        agree = all(x == f["verdict"] for x in got)
        stable = len(set(got)) == 1
        rows.append({"fixtureId": f["fixtureId"], "qid": f["qid"],
                     "want": f["verdict"], "got": got, "agree": agree,
                     "stable": stable, "protects": f.get("protects")})
        mark = "ok  " if agree else ("FAIL " if stable else "FLAKY")
        print(f"  {mark} {f['qid']:34s} want {f['verdict']:11s} "
              f"got {'/'.join(str(x) for x in got)}")

    fails = [r for r in rows if not r["agree"]]
    flaky = [r for r in rows if not r["stable"]]
    print(f"\n{len(rows) - len(fails)}/{len(rows)} fixtures reproduce"
          + (f"; {len(flaky)} unstable across {a.repeat} judgements"
             if a.repeat > 1 else ""))
    for f, why in unresolved:
        print(f"  unresolved: {f['fixtureId']} -- {why}")
    if fails:
        print("\nwhat regressed, by what it protects:")
        for k, n in collections.Counter(r["protects"] for r in fails).most_common():
            print(f"  {n:2d}  {k}")
        print("\nEach fixture is a settled human judgement. If the judge is now "
              "right and the fixture wrong, re-settle it and record why in `why`; "
              "do not delete it.")
    if a.out:
        a.out.write_text(json.dumps(
            {"judgeVersion": jv, "rubricSha": rsha, "repeat": a.repeat,
             "rows": rows, "unresolved": [f["fixtureId"] for f, _ in unresolved]},
            indent=2))
    return 1 if fails or unresolved else 0


if __name__ == "__main__":
    sys.exit(main())
