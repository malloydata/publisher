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
from pathlib import Path
from typing import Any

# A flip is movement between PASS and FAIL. Everything else is neither.
#
# `near_match` is deliberately in NEITHER set. It means "defensibly different" --
# a tie broken the other way, a caveat buried, a reading the rubric allows but
# did not prefer -- and a verdict whose whole content is "this is arguable" has
# no business deciding a gate. As a pass it was a large share of the measured
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
            "passed": True if v in PASS else (False if v in FAIL else None),
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
        # is a false alarm, and a gate that cries wolf is a gate people learn to
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

    ca, cb = cost(a_args.a), cost(a_args.b)
    print(f"\ncost\n----")
    print(f"  {la:<24} ${ca['usd']:.2f}  {ca['turns']:.0f} turns  "
          f"{ca['seconds'] / 60:.0f} min")
    print(f"  {lb:<24} ${cb['usd']:.2f}  {cb['turns']:.0f} turns  "
          f"{cb['seconds'] / 60:.0f} min")

    return 0


if __name__ == "__main__":
    sys.exit(main())
