#!/usr/bin/env python3
"""Compare two runs case by case and report the flips.

Point it at two runs of the *same* configuration to measure the noise floor, or
at two arms to measure a change. The output is the same either way, which is the
whole idea: an A/B result only means something read against an A/A from the same
harness.

    python3 flip_table.py --a results/sonnet-baseline --b results/sonnet-aa

Exits 1 if the runs disagree about which cases they contain, because a flip table
over two different case sets is not a flip table.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

# A flip is movement between PASS and FAIL. Everything else is neither.
#
# `near_match` is deliberately in NEITHER set. It means "defensibly different" --
# a tie broken the other way, a caveat buried, a reading the rubric allows but
# did not prefer -- and a verdict whose whole content is "this is arguable" has
# no business deciding an acceptance check. As a pass it was a large share of the measured
# noise -- the judge stepping across the match/near_match line on an unchanged
# answer, which is noise wearing a verdict's clothes. It is still counted and
# reported, just not as a pass or a fail. Per-set numbers live in that set's
# calibration record, not here, where they would go stale unnoticed.
#
# Answer verdicts only. Retrieval scoring folds near_match into recall and
# precision on purpose (`score_retrieval.py`), because there "the concept
# overlaps" is a genuine retrieval success.
PASS = {"match"}
FAIL = {"no_match", "wrong", "refused_wrongly"}
NEITHER = {"near_match", "needs_human"}


def outcome(verdict: str | None) -> str:
    """"pass" / "fail" / "neither" -- the ONE classification of a verdict.

    Exported because the run package used to re-derive this in Malloy and got a
    different answer: it read anything that was not `match` as a non-pass, so a
    judge that hedged in one arm and not the other manufactured a flip. The
    package now reads this column instead of re-deciding, on the same grounds
    build_run_package already imports its scoring rather than reimplementing it.

    Anything unrecognised is "neither", never "fail". A verdict vocabulary this
    file has not been taught is not evidence that a case failed.
    """
    if verdict in PASS:
        return "pass"
    if verdict in FAIL:
        return "fail"
    return "neither"


def verdicts(run: Path) -> dict[str, dict[str, Any]]:
    """qid -> the scored outcome, for cases this run actually scored."""
    out: dict[str, dict[str, Any]] = {}
    for line in (run / "events.jsonl").read_text().splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        if e.get("kind") != "score":
            continue
        v = e.get("verdict")
        out[e["qid"]] = {
            "verdict": v,
            "confidence": e.get("confidence"),
            "reason": (e.get("reason") or "")[:200],
            # near_match and needs_human are neither: counting either as a fail
            # would manufacture a flip every time the judge hedged in one run
            # and not the other.
            "outcome": outcome(v),
            "passed": {"pass": True, "fail": False}.get(outcome(v)),
        }
    return out


def cost(run: Path) -> dict[str, float]:
    tot: dict[str, float] = {"usd": 0.0, "turns": 0.0, "seconds": 0.0}
    for line in (run / "events.jsonl").read_text().splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        if e.get("kind") != "attempt":
            continue
        tot["usd"] += e.get("cost_usd") or 0.0
        tot["turns"] += e.get("num_turns") or 0
        tot["seconds"] += e.get("wall_seconds") or 0.0
    return tot


def config(run: Path) -> dict[str, Any]:
    f = run / "run.json"
    return json.loads(f.read_text()) if f.exists() else {}


# The pins two runs must share to be one measurement. A difference in any of
# them is a difference in what was measured, so a flip count across it is not a
# noise band and not an A/B -- it is two numbers about two different things.
COMPARABLE = ("datasetVersion", "datasetSha", "judgeVersion", "rubricSha",
              "answererModel", "judgeModel", "answererManifest",
              "retrievalMode")


def retrieval_gate(ca: dict, cb: dict, la: str, lb: str,
                   allow: bool) -> int:
    """Refuse a pair whose runs used different retrievers.

    Local retrieval falls back to lexical SILENTLY when no embedding key is
    set, and partway through a run when the provider fails. Either way the two
    arms searched differently, and the flips that produces read as a model
    change. eval-mvp's standing gate: no A/B is scored under an unavailable
    semantic path. A run written before the harness recorded this carries
    nothing, and an unrecorded mode is not evidence that it matched -- so that
    is reported and allowed, because refusing every historical run would make
    the gate unusable rather than safe.
    """
    ma, mb = ca.get("retrievalMode"), cb.get("retrievalMode")
    if ma is None or mb is None:
        print(f"\n  ! retrieval mode not recorded ({la}: {ma or 'absent'}, "
              f"{lb}: {mb or 'absent'}), so which retriever answered cannot be "
              f"checked. Re-run with a harness that records it before quoting "
              f"this pair.")
        return 0
    if ma == mb and ma != "mixed":
        if ma != "semantic":
            print(f"\n  ! both arms retrieved {ma}, not semantic. The pair is "
                  f"internally consistent, so a band measured here holds for "
                  f"{ma} retrieval and for nothing else.")
        return 0
    print(f"\n  ! retrieval differs: {la} {ma}, {lb} {mb}. The arms did not "
          f"search the same way, so these flips are not a measurement of the "
          f"change.")
    if allow:
        print("    --allow-retrieval-mismatch given; reporting anyway.")
        return 0
    print("    Fix the embedding provider and re-run, or pass "
          "--allow-retrieval-mismatch to report anyway.")
    return 2


def calibration_block(ca: dict, cb: dict, la: str, lb: str, pa: int, pb: int,
                      scored: int, flips: int, stable_near: list[str]) -> str:
    """The set's CALIBRATION.md entry for this pair, ready to append.

    A band is only quotable against the configuration it was measured on, and
    observed bands have moved by a factor of three across a fortnight of
    ordinary work. So the block records every pin that has to match, and a
    reader compares them rather than trusting the number.
    """
    rows = [f"| {k} | {ca.get(k) if ca.get(k) == cb.get(k) else f'{ca.get(k)} / {cb.get(k)}'} |"
            for k in COMPARABLE]
    return "\n".join([
        f"## {ca.get('datasetVersion')} / judge v{ca.get('judgeVersion')} "
        f"/ {ca.get('answererModel')} answerer",
        "",
        f"Measured {time.strftime('%Y-%m-%d')} from `{la}` and `{lb}`.",
        "",
        "| pin | value |",
        "| --- | --- |",
        *rows,
        "",
        f"- **Flips: {flips}** over {scored} cases scored in both arms.",
        f"- Passed {pa} and {pb}, a difference of {pb - pa:+d} with no change "
        f"between the arms.",
        f"- Stable near_match: {len(stable_near)}"
        + (f" ({', '.join(stable_near)})" if stable_near else ""),
        "",
        "Quote this band only for a run whose pins above all match. One that "
        "differs in any of them is a different measurement.",
        "",
    ])


def targeted_report(args: argparse.Namespace, A: dict, B: dict,
                    la: str, lb: str, C: dict | None = None) -> None:
    """Judge a targeted fix on the cases it aimed at, not on the set total.

    The set-total flip count is the right instrument for a broad change and the
    wrong one for a narrow fix: a change that repairs three cases on a 49-case
    set moves the total by three, which is inside the noise band a same-config
    A/A already produces. Read that way, a mechanically-verified fix reports as
    no effect. Splitting targeted from untargeted recovers the signal, at the
    cost of requiring the targets to be named in advance -- which is the point,
    since choosing them afterwards is choosing the answer.
    """
    raw = args.targets
    path = Path(raw)
    names = (path.read_text().split() if path.exists()
             else [t.strip() for t in raw.split(",")])
    targets = [t for t in names if t]

    unknown = [t for t in targets if t not in A]
    if unknown:
        print(f"\nnot in the run: {', '.join(unknown)}")
    targets = [t for t in targets if t in A]

    print(f"\ntargeted cases ({len(targets)})\n{'-' * 22}")
    fixed = still = broke = 0
    for q in targets:
        was, now = A[q]["passed"], B[q]["passed"]
        if was is False and now:
            fixed += 1
            mark = "FIXED"
        elif was is False and now is False:
            still += 1
            mark = "still failing"
        elif was and now is False:
            broke += 1
            mark = "BROKE"
        else:
            mark = "was already passing"
        print(f"  {mark:<20} {q}  ({A[q]['verdict']} -> {B[q]['verdict']})")
    print(f"\n  {fixed} fixed, {still} still failing, {broke} broken")

    other = [q for q in A if q not in targets]
    o_better = [q for q in other if A[q]["passed"] is False and B[q]["passed"]]
    o_worse = [q for q in other if A[q]["passed"] and B[q]["passed"] is False]
    o_flips = len(o_better) + len(o_worse)

    print(f"\nuntargeted cases ({len(other)})\n{'-' * 22}")
    print(f"  {o_flips} flipped  ({len(o_better)} better, {len(o_worse)} worse)")
    for q in o_worse:
        print(f"    worse:  {q}")
    for q in o_better:
        print(f"    better: {q}")

    band = args.noise_band
    if band is None:
        print("\n  no --noise-band given, so whether that is noise is unknown."
              "\n  run an A/A and pass its flip count.")
    elif o_flips <= band:
        print(f"\n  within the measured noise band of {band}, so consistent with"
              f"\n  the fix having no untargeted effect.")
    elif not o_worse:
        # Saying "investigate before accepting" when every flip is an improvement
        # is a false alarm, and an acceptance check that cries wolf is one people learn to
        # click through. The count still exceeds the band, which is worth saying;
        # the direction is what changes what to do about it.
        print(f"\n  above the measured noise band of {band}, but every untargeted"
              f"\n  flip is an improvement. Nothing to revert; confirm below that"
              f"\n  they reproduce, then claim them.")
    else:
        print(f"\n  ABOVE the measured noise band of {band}, and {len(o_worse)} of the"
              f"\n  flips are regressions. Investigate those before accepting.")

    # The band counts flips. It does not ask WHICH cases flipped, and that is
    # the hole: noise scatters, so an untargeted case that breaks in two
    # independent post-edit arms is not noise no matter how small the count is.
    #
    # This is not hypothetical. A change that added a `units_sold` measure fixed
    # all three of its targets and broke `ecom_return_rate` in both post arms --
    # agents reached for the new measure as the return-rate denominator, which
    # excludes cancelled lines. Five untargeted flips against a band of seven,
    # so the band accepted it. Two arms make the same regression obvious.
    if C is not None:
        c_worse = {q for q in other
                   if A[q]["passed"] and q in C and C[q]["passed"] is False}
        c_better = {q for q in other
                    if A[q]["passed"] is False and q in C and C[q]["passed"]}
        repro_worse = sorted(set(o_worse) & c_worse)
        repro_better = sorted(set(o_better) & c_better)
        print(f"\nreproducibility ({len(other)} untargeted, second post arm)"
              f"\n{'-' * 22}")
        if repro_worse:
            print(f"  {len(repro_worse)} REGRESSED IN BOTH post arms -- not noise:")
            for q in repro_worse:
                print(f"    {q}")
        else:
            print("  no untargeted case broke in both post arms")
        if repro_better:
            print(f"  {len(repro_better)} improved in both arms (an unclaimed win):")
            for q in repro_better:
                print(f"    {q}")
        one_armed = sorted((set(o_worse) | set(o_better))
                           - set(repro_worse) - set(repro_better))
        if one_armed:
            print(f"  {len(one_armed)} flipped in one arm only, so noise: "
                  f"{', '.join(one_armed)}")
        if repro_worse:
            print("\n  DO NOT ACCEPT on the band alone. The targets are fixed,"
                  "\n  but the change also causes a reproducible regression."
                  "\n  Either narrow the edit or take the trade knowingly.")
    else:
        print("\n  only one post-edit arm, so a reproducible regression cannot be"
              "\n  told from a noise flip. Pass --b2 with a second arm.")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--a", type=Path, required=True)
    p.add_argument("--b", type=Path, required=True)
    p.add_argument("--label-a", default=None)
    p.add_argument("--label-b", default=None)
    p.add_argument("--targets", default=None,
                   help="comma-separated qids the change was meant to fix, or a "
                        "file with one per line. Declare them BEFORE the run.")
    p.add_argument("--b2", type=Path, default=None,
                   help="a SECOND post-edit arm. With it, an untargeted "
                        "case that breaks in both is reported as a real "
                        "regression rather than counted against the band.")
    p.add_argument("--noise-band", type=int, default=None,
                   help="flips your A/A measured. Untargeted flips at or below "
                        "this are consistent with noise.")
    p.add_argument("--allow-retrieval-mismatch", action="store_true",
                   help="report a pair whose arms used different retrievers. "
                        "The flips are then not a measurement of the change; "
                        "say so wherever the number is quoted.")
    p.add_argument("--calibration", action="store_true",
                   help="print the set's CALIBRATION.md entry for this pair, "
                        "ready to append. Use it on an A/A.")
    a_args = p.parse_args()

    la = a_args.label_a or a_args.a.name
    lb = a_args.label_b or a_args.b.name
    A, B = verdicts(a_args.a), verdicts(a_args.b)

    only_a, only_b = sorted(set(A) - set(B)), sorted(set(B) - set(A))
    if only_a or only_b:
        print(f"the runs do not cover the same cases: "
              f"{len(only_a)} only in {la}, {len(only_b)} only in {lb}")
        for q in (only_a + only_b)[:10]:
            print(f"  {q}")
        return 1

    shared = sorted(A)
    both_pass = [q for q in shared if A[q]["passed"] and B[q]["passed"]]
    both_fail = [q for q in shared
                 if A[q]["passed"] is False and B[q]["passed"] is False]
    a_only = [q for q in shared
              if A[q]["passed"] and B[q]["passed"] is False]
    b_only = [q for q in shared
              if A[q]["passed"] is False and B[q]["passed"]]
    unscored = [q for q in shared
                if A[q]["passed"] is None or B[q]["passed"] is None]

    flips = len(a_only) + len(b_only)
    scored = len(shared) - len(unscored)

    print(f"\n{la}  vs  {lb}\n{'=' * (len(la) + len(lb) + 6)}")
    print(f"{len(shared)} cases, {scored} scored in both, {len(unscored)} not\n")

    # Over the cases scored in BOTH arms, so the numerator and the printed
    # denominator describe the same set (this once printed 19 / 17).
    both = [q for q in shared if q not in unscored]
    pa = len([q for q in both if A[q]['passed']])
    pb = len([q for q in both if B[q]['passed']])
    print(f"  {la:<24} {pa:>3} / {scored} passed")
    print(f"  {lb:<24} {pb:>3} / {scored} passed")
    print(f"  {'difference':<24} {pb - pa:>+3}\n")

    print(f"  stable pass            {len(both_pass):>3}")
    print(f"  stable fail            {len(both_fail):>3}")
    print(f"  flipped                {flips:>3}"
          f"  ({flips / scored * 100:.0f}% of scored)" if scored else "")
    print(f"    {la} only            {len(a_only):>3}")
    print(f"    {lb} only            {len(b_only):>3}")
    if unscored:
        # Broken out rather than lumped as "unscored", because the two mean
        # different things: near_match is the judge saying the answer is
        # arguable, needs_human is it declining to say anything. Excluding both
        # from the band is right; hiding how many there were is not, since a
        # rising near_match count is a rubric going vague.
        def why(q: str) -> str:
            for v in (A[q]["verdict"], B[q]["verdict"]):
                if v in NEITHER:
                    return v
            return "not scored"
        tally: dict[str, int] = {}
        for q in unscored:
            tally[why(q)] = tally.get(why(q), 0) + 1
        print(f"  neither pass nor fail  {len(unscored):>3}"
              f"  ({', '.join(f'{v} {k}' for k, v in sorted(tally.items()))})")

    if a_only or b_only:
        print(f"\nthe flips\n---------")
        for q in a_only:
            print(f"  {q}\n     {la}: {A[q]['verdict']}  ->  "
                  f"{lb}: {B[q]['verdict']}\n     {B[q]['reason'][:150]}")
        for q in b_only:
            print(f"  {q}\n     {la}: {A[q]['verdict']}  ->  "
                  f"{lb}: {B[q]['verdict']}\n     {B[q]['reason'][:150]}")

    if a_args.targets:
        targeted_report(a_args, A, B, la, lb,
                        verdicts(a_args.b2) if a_args.b2 else None)

    # Stable on both sides: the judge is not hedging at random, it is saying
    # the model cannot distinguish two readings the question does. That is a
    # coverage finding for eval-diagnose, which selects no_match by default and
    # so never sees these.
    stable_near = sorted(q for q in shared
                         if A[q]["verdict"] == "near_match"
                         and B[q]["verdict"] == "near_match")
    if stable_near:
        print(f"\nstable near_match ({len(stable_near)})\n"
              f"{'-' * 20}")
        print("  near_match in BOTH arms, so not judge noise. Each is a "
              "coverage gap, not a rubric to soften:")
        for q in stable_near:
            print(f"    {q}")
        print(f"  diagnose.py --only {','.join(stable_near)} "
              f"--verdicts near_match")

    cfg_a, cfg_b = config(a_args.a), config(a_args.b)
    differing = [k for k in COMPARABLE
                 if cfg_a.get(k) != cfg_b.get(k)]
    if differing:
        print(f"\nthe arms differ in {len(differing)} pin(s): "
              f"{', '.join(differing)}")
        for k in differing:
            print(f"    {k:<20} {cfg_a.get(k)}  ->  {cfg_b.get(k)}")
        print("  More than one pin moving makes the flips unattributable.")

    gate = retrieval_gate(cfg_a, cfg_b, la, lb,
                          a_args.allow_retrieval_mismatch)

    ca, cb = cost(a_args.a), cost(a_args.b)
    print(f"\ncost\n----")
    print(f"  {la:<24} ${ca['usd']:.2f}  {ca['turns']:.0f} turns  "
          f"{ca['seconds'] / 60:.0f} min")
    print(f"  {lb:<24} ${cb['usd']:.2f}  {cb['turns']:.0f} turns  "
          f"{cb['seconds'] / 60:.0f} min")

    if a_args.calibration:
        print("\n--- CALIBRATION.md entry, append to the set "
              "---------------------\n")
        print(calibration_block(cfg_a, cfg_b, la, lb, pa, pb, scored, flips,
                                stable_near))

    return gate


if __name__ == "__main__":
    sys.exit(main())
