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

# `verified_wrong` is a key a person has established is wrong. Three files
# accepted it (ledger-schema.md, golden-side-door.md, run_baseline.py reads it
# off the case to return `golden_verified_wrong`) and this one rejected it, so
# a mature set carrying an adjudicated key failed validation.
STATUSES = ("verified", "provisional", "invalid", "ambiguous", "verified_wrong")
SPLITS = ("dev", "holdout")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


# The shortest seal worth trusting. `--stamp` writes 16 hex characters and the
# ecommerce set's author script writes `sha256(question)[:16]`, so nothing in
# practice is shorter; a prefix comparison against a TRUNCATED stamp weakens the
# seal in proportion to what is left -- roughly one in sixteen at a single
# character -- and reported clean the whole way down.
STAMP_MIN = 16


def stamp_problem(stamp: Any) -> str | None:
    """Why a stamp cannot be compared, or None if it can.

    Malformed is its own finding with its own message, the rule this branch
    already applied to entity ids in `verify_goldens.py`. Without it a stamp
    that arrived as a number raised `AttributeError` on `.startswith`, which
    `verify_goldens.py`'s top-level handler turns into exit 3, "could not run"
    -- a malformed seal reported as a harness failure rather than as a finding
    about the set.
    """
    if not isinstance(stamp, str):
        return (f"`questionSha` is {type(stamp).__name__}, expected a hex "
                f"string. Fix: re-stamp with `import_cases.py --stamp`")
    if len(stamp) < STAMP_MIN:
        return (f"`questionSha` is {len(stamp)} characters, expected at least "
                f"{STAMP_MIN}. A shorter prefix is a weaker seal and still "
                f"reports clean. Fix: re-stamp with `import_cases.py --stamp`")
    return None


def stamp_matches(stamp: str, question: str) -> bool:
    """Whether a stored seal still matches the question, by PREFIX.

    A stamp is not always the whole digest. The ecommerce set's author script
    writes `sha256(question)[:16]`, and comparing the full 64 read all 49 of
    its cases as edited questions. 64 bits is ample to catch an edit, and a
    false finding is worse than a missed one here: `verify_goldens.py` runs
    this before every arm.
    """
    return sha256_text(question).startswith(stamp)


def holds_value(golden: dict[str, Any]) -> bool:
    """A golden asserting a value, as opposed to prose criteria or a refusal.

    `value` of 0 and an empty row artifact are both real keys, so truthiness is
    the wrong test. An explicit `"value": null` is NOT a key, though: that is
    how the ecommerce set's four refusal cases say there is no number, and
    reading the key's mere presence counted them as holding one.
    """
    if golden.get("kind") in ("criteria", "unanswerable"):
        return False
    return golden.get("value") is not None or "path" in golden


# The entity kinds each get_context `target_type` can return. One definition,
# pinned against the server by score_retrieval_test; its keys are the types
# the server accepts.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]
                       / "eval-answer" / "scripts"))
from score_retrieval import KINDS_BY_TARGET  # noqa: E402

TARGET_TYPES = tuple(KINDS_BY_TARGET)


def search_target_findings(case: dict[str, Any], where: str,
                           qid: Any) -> list[str]:
    """The shape of an optional `searchTargets` list. Never its ids' existence.

    Whether an id names a real entity is a question about the model, which
    this script does not read; `check_findable.py` answers it. This catches
    what would otherwise score silently wrong: a target type the server
    rejects, a key that is not a `kind:source:name` id, a group written as a
    bare string, which `score_targets.py` would read one letter at a time, and
    a key the target's own type can never return. `target_type` is a hard
    filter on the server, so a `dimension` target keyed to a measure scores a
    miss on every run however the agent words it.
    """
    targets = case.get("searchTargets")
    if targets is None:
        return []
    if not isinstance(targets, list):
        return [f"{where} {qid}: `searchTargets` is not a list"]
    out = []
    for i, t in enumerate(targets, 1):
        at = f"{where} {qid}: searchTargets[{i}]"
        if not isinstance(t, dict):
            out.append(f"{at} is not an object")
            continue
        if t.get("target_type") not in TARGET_TYPES:
            out.append(f"{at}: `target_type` is {t.get('target_type')!r}, "
                       f"expected one of {TARGET_TYPES}")
        text = t.get("search_text")
        if text is not None and not (isinstance(text, str) and text.strip()):
            out.append(f"{at}: `search_text` must be a non-empty string, or "
                       f"absent for a target that enumerates its type")
        values = t.get("example_values")
        if values is not None and not isinstance(values, list):
            out.append(f"{at}: `example_values` must be a list")
        exp = t.get("expectedEntities")
        if exp is None:
            continue
        if not isinstance(exp, dict):
            out.append(f"{at}: `expectedEntities` is not an object")
            continue
        req = [[r] for r in exp.get("required") or []]
        for g in exp.get("requiredAnyOf") or []:
            if not isinstance(g, list):
                out.append(f"{at}: a `requiredAnyOf` group is "
                           f"{type(g).__name__}, expected a list of ids. Fix: "
                           f'[["measure:s:a", "measure:s:b"]]')
                continue
            req.append(g)
        bad = False
        for eid in [e for g in req for e in g] + list(exp.get("acceptable") or []):
            parts = eid.split(":", 2) if isinstance(eid, str) else []
            if len(parts) < 3 or not all(parts):
                bad = True
                out.append(f"{at}: {eid!r} is not a kind:source:name id. Fix: "
                           f'"measure:order_items:total_sales"')
        kinds = KINDS_BY_TARGET.get(t.get("target_type"))
        if bad or kinds is None:
            continue
        for g in req:
            if not any(e.split(":", 1)[0] in kinds for e in g):
                out.append(f"{at}: a `{t['target_type']}` target can never "
                           f"return {' | '.join(g)}, because `target_type` is "
                           f"a hard filter. Fix: move it to a target of its "
                           f"own kind")
    return out


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
    problem = stamp_problem(stamp) if stamp is not None else None
    if problem:
        findings.append(f"{where} {qid}: {problem}")
    elif stamp and question is not None and not stamp_matches(stamp, question):
        findings.append(
            f"{where} {qid}: the question does not match its `questionSha`. "
            "Either the question was edited after import, which is never "
            "allowed, or the stamp is wrong. Fix: restore the question, or "
            "give the new wording a new qid")

    findings += search_target_findings(case, where, qid)

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
        by = golden.get("verifiedBy")
        # This skill's rule is flat: "No golden holding a VALUE imports as
        # `verified`." Gating the finding on a MISSING `verifiedBy` let through
        # the one shape step 3 names explicitly -- their query, their number,
        # agreeing -- which step 3 then says to keep `provisional`, because the
        # query ran against the model under test and a model bug certifies its
        # own golden. So the validator accepted exactly what the skill forbids,
        # and that was the only route out of the provisional deadlock.
        #
        # `authored_query` and an absent value are both import-time claims. A
        # re-derivation through the truth package is not, which is why
        # verify_goldens' own markers pass: a mature set whose keys were
        # promoted must keep validating.
        if by in (None, "", "authored_query", "authored_number"):
            claim = (f"`verifiedBy: {by}`" if by else "no `verifiedBy`")
            findings.append(
                f"{where} {qid}: a golden holding a value claims `verified` "
                f"with {claim}. Nothing an import can do makes a value "
                "verified -- their own query agreeing proves the number came "
                "from that query, not that the query is right. Fix: "
                "`provisional`, then re-derive through the truth package and "
                "promote (verify_goldens.py --promote)")

    value = golden.get("value")
    if kind == "scalar" and value is not None and not isinstance(value, dict):
        findings.append(
            f"{where} {qid}: `kind: scalar` holds a bare {type(value).__name__}. "
            "It must name the query's column, or the value check cannot pair "
            f'it with a result. Fix: `"value": {{"answer": {json.dumps(value)}}}`, '
            "with the column name the canonical query returns")

    if golden.get("verifiedBy") == "authored_query" and not golden.get("canonicalQuery"):
        findings.append(f"{where} {qid}: `verifiedBy: authored_query` with no "
                        "`canonicalQuery`. Fix: store the query they sent, or "
                        "drop the claim")

    # No review item for `expectedEntities`. Whether a required id exists is a
    # question about the MODEL, which this script never reads, so a warning
    # here could only be unfalsifiable -- and it fired on 45 of 49 cases of a
    # mature set whose ids were derived from executing queries. The real audit
    # is check 5 of `verify_goldens.py`, which takes `--model` and can answer.
    return findings, review


def stamp_cases(path: pathlib.Path, cases: list[dict[str, Any]]) -> int:
    """Write `questionSha` where absent. Never overwrites. Returns how many.

    The file is rewritten from its own lines, not from the parsed list. A line
    that does not parse is written back verbatim, so a stamp can never delete
    the case it could not read. It did once: `read_cases` drops an unparseable
    line with a finding, and rewriting the file from the parsed list turned
    that finding into a deletion on the one file an eval set cannot
    regenerate. `cases` is stamped in place as well, so the caller's later
    checks see the same stamp the file does.
    """
    stamped = 0
    out: list[str] = []
    parsed = iter(cases)
    for line in path.read_text().splitlines():
        if not line.strip():
            out.append(line)
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            out.append(line)
            continue
        if not isinstance(obj, dict):
            out.append(line)
            continue
        case = next(parsed, None)
        if case is None:
            # More object lines on disk than cases handed in: keep the line as
            # it stands rather than guess which case it was.
            out.append(line)
            continue
        question = case.get("question")
        if (not case.get("questionSha") and isinstance(question, str)
                and question.strip()):
            case["questionSha"] = sha256_text(question)
            stamped += 1
        out.append(json.dumps(case))
    if stamped:
        path.write_text("".join(l + "\n" for l in out))
    return stamped


def summarize(cases: list[dict[str, Any]], lines: int) -> list[str]:
    """The tally to report, scorable count first.

    "47 cases" reads like a 47-case measurement. The number a first run can
    actually score is usually much smaller, so it goes on the first line.
    """
    scorable = with_query = value_only = to_derive = no_golden = 0
    verified_values = invalid = ambiguous = wrong = unrecognised = 0
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
        elif status == "invalid":
            invalid += 1
        elif status == "ambiguous":
            ambiguous += 1
        elif status == "verified_wrong":
            wrong += 1
        elif status != "provisional":
            # check_case reports the status as a finding; the tally still has
            # to account for the case, or the lines below stop summing to the
            # headline and a reader cannot tell a miscount from a bad status.
            unrecognised += 1
        else:
            # Three different amounts of work, and the first summary called
            # them all "numbers only" -- including cases that arrived with no
            # number at all, where the criteria describe a key nobody has
            # derived. That reads as "we have a number we distrust" when the
            # truth is "we have nothing yet".
            if golden.get("canonicalQuery"):
                with_query += 1
            elif holds_value(golden):
                value_only += 1
            else:
                to_derive += 1
    provisional = with_query + value_only + to_derive
    settled = invalid + ambiguous + wrong
    # Every case lands on exactly one of the lines below, so they sum to the
    # headline and a reader can check them against it. A golden marked
    # `invalid`, `ambiguous` or `verified_wrong` used to land on none of them,
    # so the breakdown quietly fell short of the case count.
    out = [f"{len(cases)} cases from {lines} lines",
           f"  {scorable} scorable now",
           f"  {provisional} provisional ({with_query} with their query, "
           f"{value_only} a number alone, {to_derive} nothing to compare yet)",
           f"  {no_golden} no golden (question only)"]
    if settled:
        out.append(f"  {settled} settled unscorable ({invalid} invalid, "
                   f"{ambiguous} ambiguous, {wrong} verified wrong)")
    if unrecognised:
        out.append(f"  {unrecognised} with a status this script does not know "
                   f"(see FINDINGS)")
    if verified_values:
        # Not phrased as an accusation. This script is also the set validator,
        # so it runs on established sets whose values were verified long after
        # import -- the ecommerce set has 45 -- and telling those they "cannot"
        # be verified reads as a defect where there is none.
        out.append(f"  {verified_values} of those hold a verified VALUE. An "
                   "import cannot produce one, so on a fresh conversion this "
                   "is a finding; on an established set it is the record of a "
                   "re-derivation")
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
