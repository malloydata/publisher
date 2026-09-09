#!/usr/bin/env python3
"""Validate a freshly imported eval set, and seal each question. Stdlib only.

  python3 import_cases.py --set evals/<set>            # check
  python3 import_cases.py --set evals/<set> --stamp    # check, and seal questions

WHY

Conversion is judgment: only a reader can tell a customer's number from a
customer's criterion, or find the question boundaries in an email thread. So
this does not convert anything. It checks the result of a conversion for the
mistakes that are mechanical, and it writes the one field that must not be
written by hand.

THE SEAL

`questionSha` is SHA-256 of the question text, stamped once at conversion. It
is not derived from the arriving file, which is why it works for a thread as
well as a CSV: it records the decision made at conversion about what the
question is. After that, a mismatch means somebody edited a question, which is
the one edit an eval set must never absorb silently -- narrowing a question to
match what an answerer keeps doing deletes the case's whole purpose and reads
as a pass. `--stamp` refuses to overwrite an existing stamp for the same
reason. A question that genuinely has to change gets a new `qid`.

WHAT IT REFUSES

A golden holding a VALUE that claims `verified` without saying what verified
it. `verified` means two differently shaped derivations agreed through the
truth package, and an import has performed neither, so an imported value is
`provisional` however confident its author was. A `criteria` golden holds no
value and is exempt: the author's clauses are the key, and there is nothing to
re-derive.

EXIT CODES

  0  clean
  1  at least one finding
  2  usage error (argparse), or the set is unreadable
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
from typing import Any

STATUSES = ("verified", "provisional", "invalid", "ambiguous")
SPLITS = ("dev", "holdout")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def holds_value(golden: dict[str, Any]) -> bool:
    """A golden asserting a value, as opposed to prose criteria or a refusal.

    `value` of 0 and an empty row artifact are both real keys, so this tests
    for the KEY's presence and not its truthiness.
    """
    if golden.get("kind") in ("criteria", "unanswerable"):
        return False
    return "value" in golden or "path" in golden


def read_cases(path: pathlib.Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Parsed cases, plus one finding per line that did not parse.

    Counted rather than aborted: an import that silently dropped 3 of 50 lines
    is a measurement on 47 cases claiming to be one on 50, and the count is
    the only thing that says so.
    """
    cases: list[dict[str, Any]] = []
    findings: list[str] = []
    for n, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as e:
            findings.append(f"{path.name}:{n}: does not parse as JSON ({e.msg})")
            continue
        if not isinstance(parsed, dict):
            findings.append(f"{path.name}:{n}: not a JSON object")
            continue
        cases.append(parsed)
    return cases, findings


def check_case(case: dict[str, Any], where: str) -> tuple[list[str], list[str]]:
    """(findings, review items) for one case."""
    findings: list[str] = []
    review: list[str] = []
    qid = case.get("qid") or "<no qid>"

    if not case.get("qid"):
        findings.append(f"{where}: no `qid`")
    question = case.get("question")
    if not isinstance(question, str) or not question.strip():
        findings.append(f"{where} {qid}: no `question`. "
                        "Fix: a case needs the text a human asked; do not "
                        "reconstruct one from a criterion")
        question = None
    if case.get("split") not in SPLITS:
        findings.append(f"{where} {qid}: `split` is {case.get('split')!r}, "
                        f"expected one of {SPLITS}. Fix: freeze it at import, "
                        "because a split chosen after the first failures is "
                        "not a holdout")

    stamp = case.get("questionSha")
    if stamp and question is not None and stamp != sha256_text(question):
        findings.append(
            f"{where} {qid}: the question does not match its `questionSha`. "
            "Either the question was edited after import, which is never "
            "allowed, or the stamp is wrong. Fix: restore the question, or "
            "give the new wording a new qid")

    golden = case.get("golden")
    if golden is None:
        review.append(f"{qid}: no golden. It measures coverage, not accuracy")
        return findings, review
    if not isinstance(golden, dict):
        findings.append(f"{where} {qid}: `golden` is not an object")
        return findings, review

    status = golden.get("status")
    if status not in STATUSES:
        findings.append(f"{where} {qid}: `golden.status` is {status!r}, "
                        f"expected one of {STATUSES}")
    kind = golden.get("kind")

    if kind == "criteria":
        if "value" in golden or "path" in golden:
            findings.append(
                f"{where} {qid}: `kind: criteria` also holds a value. "
                "Fix: split it. The number is a provisional value; the shape "
                "clauses are the criteria")
        if not golden.get("rubric"):
            findings.append(f"{where} {qid}: `kind: criteria` with no "
                            "`golden.rubric`. The clauses ARE the key, so "
                            "there is nothing to judge against")
    elif status == "verified" and holds_value(golden):
        if not golden.get("verifiedBy"):
            findings.append(
                f"{where} {qid}: a golden holding a value claims `verified` "
                "with no `verifiedBy`. Nothing an import can do makes a value "
                "verified. Fix: `provisional`, and re-derive it through the "
                "truth package")

    if golden.get("verifiedBy") == "authored_query" and not golden.get("canonicalQuery"):
        findings.append(f"{where} {qid}: `verifiedBy: authored_query` with no "
                        "`canonicalQuery`. Fix: store the query they sent, or "
                        "drop the claim")

    if (case.get("expectedEntities") or {}).get("required"):
        review.append(f"{qid}: carries `expectedEntities.required` at import. "
                      "An id this model lacks scores as a retrieval miss on "
                      "every run and reads as a model failure")
    return findings, review


def stamp_cases(path: pathlib.Path, cases: list[dict[str, Any]]) -> int:
    """Write `questionSha` where absent. Never overwrites. Returns how many."""
    stamped = 0
    for case in cases:
        question = case.get("question")
        if case.get("questionSha") or not isinstance(question, str) or not question.strip():
            continue
        case["questionSha"] = sha256_text(question)
        stamped += 1
    if stamped:
        path.write_text("".join(json.dumps(c) + "\n" for c in cases))
    return stamped


def summarize(cases: list[dict[str, Any]], lines: int) -> list[str]:
    """The tally to report, scorable count first.

    "47 cases" reads like a 47-case measurement. The number a first run can
    actually score is usually much smaller, so it goes on the first line.
    """
    scorable = provisional_q = provisional_bare = no_golden = verified_values = 0
    for case in cases:
        golden = case.get("golden")
        if not isinstance(golden, dict):
            no_golden += 1
            continue
        status = golden.get("status")
        if status == "verified":
            scorable += 1
            if holds_value(golden):
                verified_values += 1
        elif status == "provisional":
            if golden.get("canonicalQuery"):
                provisional_q += 1
            else:
                provisional_bare += 1
    out = [f"{len(cases)} cases from {lines} lines",
           f"  {scorable} scorable now",
           f"  {provisional_q + provisional_bare} provisional "
           f"({provisional_q} with their query, {provisional_bare} numbers only)",
           f"  {no_golden} no golden (question only)"]
    if verified_values:
        out.append(f"  {verified_values} verified VALUES, which an import "
                   "cannot produce. Check them")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", required=True, type=pathlib.Path)
    ap.add_argument("--cases", default="cases.jsonl",
                    help="cases file within the set directory")
    ap.add_argument("--stamp", action="store_true",
                    help="write questionSha where absent; never overwrites an "
                         "existing stamp. Rewrites the cases file as one "
                         "compact JSON object per line")
    a = ap.parse_args()

    cases_path = a.set_dir / a.cases
    if not cases_path.exists():
        print(f"{cases_path}: no such file", file=sys.stderr)
        return 2

    cases, findings = read_cases(cases_path)
    lines = len([ln for ln in cases_path.read_text().splitlines() if ln.strip()])

    set_json = a.set_dir / "set.json"
    if not set_json.exists():
        findings.append("set.json: missing. It names the set and carries "
                        "`datasetVersion`, which every run records")
    else:
        try:
            meta = json.loads(set_json.read_text())
        except json.JSONDecodeError as e:
            meta = {}
            findings.append(f"set.json: does not parse ({e.msg})")
        for field in ("name", "datasetVersion"):
            if field not in meta:
                findings.append(f"set.json: no `{field}`")

    if a.stamp:
        stamped = stamp_cases(cases_path, cases)
        print(f"stamped {stamped} question(s)"
              + (" (existing stamps left alone)" if stamped < len(cases) else ""))

    seen: dict[str, int] = {}
    review: list[str] = []
    for n, case in enumerate(cases, start=1):
        where = f"{a.cases}:{n}"
        f, r = check_case(case, where)
        findings += f
        review += r
        qid = case.get("qid")
        if isinstance(qid, str) and qid:
            if qid in seen:
                findings.append(f"{where}: duplicate qid {qid!r}, first seen "
                                f"on line {seen[qid]}. Scores are keyed on it")
            else:
                seen[qid] = n

    for line in summarize(cases, lines):
        print(line)
    if review:
        print("\nREVIEW (not failures):")
        for item in review[:20]:
            print(f"  {item}")
        if len(review) > 20:
            print(f"  ... and {len(review) - 20} more")
    if findings:
        print("\nFINDINGS:", file=sys.stderr)
        for item in findings:
            print(f"  {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
