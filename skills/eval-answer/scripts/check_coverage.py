#!/usr/bin/env python3
"""Can this package version express an answer at all? Stdlib only.

A per-version score over an eval set: of its questions, what fraction could a
correct answer even be written for, given the model as it stands. Decided by
READING the model, so there is no answerer, no judge, no goldens and no
warehouse. That is what makes it cheap enough to run across every published
version and read as a trend, which is the point of it.

  python check_coverage.py --set <set-dir> --model <package-dir-or-file>
  python check_coverage.py --set <set-dir> --publisher http://localhost:4811 \
      --environment samples --package ecommerce --version 0.0.58

It answers a different question from retrieval recall, and the difference is the
reason it exists. Recall asks whether `get_context` surfaced the entities a case
names as required. Coverage asks whether the model holds the concepts at all. A
version can score full recall on a case and still fail it because the model
never had the field the question needs, and then recall is measuring search
against an expectation that was never satisfiable.

It also answers a different question from the set's own `coverage` field, which
is a standing authored judgement about the question. This is a measurement
against ONE version of ONE model, and the two disagree exactly where a version
regressed. Nothing here writes that field, or a golden, or a rubric, or a
verdict, or any run directory: this reads a set.

The verdicts are `eval-diagnose`'s cause codes, verbatim and validated against
its own table at startup, so a renamed code fails loudly here instead of
quietly meaning nothing:

  ok            a correct answer is expressible
  COVERAGE      no representing entity anywhere
  AMBIGUOUS     several near-identical candidates
  NO-DISAMBIG   two plausible candidates, never resolved
  CONVENTION    right data, wrong statistical or business convention: the
                underlying numbers are present, no named measure expresses the
                convention the question needs

Exit 0 when the measurement ran, whatever it found. A low score is the result,
not a failure. Exit 1 if it could not run.
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import pathlib
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
SKILLS_ROOT = HERE.parent.parent
sys.path.insert(0, str(HERE))

from verify_goldens import model_text as local_model_text  # noqa: E402

# The `claude -p` invocation lives once, in the harness, rather than being
# rewritten here. It costs this script a reach into a sibling skill, which the
# other direction of the dependency does not have; the five eval skills ship as
# one group, so the file is always there, and a second copy of the retry and
# stream-json parsing would drift from the one the runs use.
sys.path.insert(0, str(SKILLS_ROOT / "eval-loop" / "scripts"))
try:
    from agent_harness import no_text, run_cli  # noqa: E402
    from run_baseline import BLOCKED_TOOLS        # noqa: E402
except ImportError as exc:                        # pragma: no cover
    raise SystemExit(
        f"cannot import the agent harness ({exc}). It lives in "
        f"skills/eval-loop/scripts/agent_harness.py and the eval skills run in "
        f"place from a checkout holding all of them.")

# This judge reads the model text it was handed and nothing else, so it holds no
# tools at all: not the file tools, not Skill, not Task. `--restricted` does not
# do this on its own. It was found the hard way -- the agent reached for
# `ReportFindings` to deliver its verdict, spent its only turn on the call, and
# came back as `error_max_turns` with no text, which read as "no reply from the
# model" and was indistinguishable from a rate limit.
JUDGE_BLOCKED = BLOCKED_TOOLS + ("Read", "Glob", "Grep", "Skill")

# Parsed from the table rather than typed here, the same way diagnose.py does
# it, so this cannot drift from the vocabulary it claims to reuse.
CODE_IN_TABLE = re.compile(r"^\|\s*`([A-Z][A-Z-]+)`\s*\|", re.M)
FAIL_VERDICTS = ("COVERAGE", "AMBIGUOUS", "NO-DISAMBIG", "CONVENTION")
OK = "ok"

# A prompt cap, not a limit anyone should hit. The model text goes in argv, and
# an over-long argv fails as an opaque OSError from the exec rather than as
# anything a reader can act on.
MAX_PROMPT = 400_000


def diagnose_codes(skills_root: pathlib.Path | None = None) -> set[str]:
    """The cause codes `eval-diagnose` defines."""
    f = (skills_root or SKILLS_ROOT) / "eval-diagnose" / "SKILL.md"
    codes = set(CODE_IN_TABLE.findall(f.read_text())) if f.exists() else set()
    if not codes:
        raise SystemExit(f"parsed zero cause codes from {f}; the table format "
                         f"changed and this checker would accept anything")
    missing = [c for c in FAIL_VERDICTS if c not in codes]
    if missing:
        raise SystemExit(
            f"eval-diagnose no longer defines {', '.join(missing)}. This "
            f"checker reuses that vocabulary verbatim rather than inventing "
            f"one, so a rename has to be made here too, not worked around.")
    return codes


def verdicts() -> tuple[str, ...]:
    return (OK,) + FAIL_VERDICTS


# --- the model under measurement ---------------------------------------------

def rest_model_text(base: str, environment: str, package: str,
                    model_path: str | None = None,
                    timeout: int = 60) -> str:
    """Every model's source in a served package, or one model's.

    The REST model resource carries `sourceText`, which is the whole point of
    reading coverage over HTTP: a published version can be measured without a
    checkout of it.
    """
    def get(path: str) -> Any:
        url = base.rstrip("/") + "/" + path.lstrip("/")
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode())

    env_q, pkg_q = (urllib.parse.quote(environment), urllib.parse.quote(package))
    stem = f"api/v0/environments/{env_q}/packages/{pkg_q}"
    if model_path:
        paths = [model_path]
    else:
        listed = get(f"{stem}/models")
        rows = listed if isinstance(listed, list) else listed.get("models") or []
        paths = [r.get("path") for r in rows if r.get("path")]
    out = []
    for p in paths:
        body = get(f"{stem}/models/{urllib.parse.quote(p, safe='')}")
        text = body.get("sourceText") or ""
        if text:
            out.append(f"-- {p}\n{text}")
    return "\n\n".join(out)


# --- the judgement -----------------------------------------------------------

PROMPT = """You are measuring whether a semantic model can EXPRESS an answer to
a question. You are not answering the question, and you have no data access:
there are no rows here and no way to run anything. Judge the model text only.

THE MODEL (every source, dimension, measure and doc comment it defines):
{model}

THE QUESTION: {question}

CONCEPTS THE QUESTION NEEDS: {concepts}

Decide ONE verdict:

- `ok` -- a correct answer is expressible. Name the entities that express it.
- `COVERAGE` -- no entity represents a needed concept anywhere in the model.
- `AMBIGUOUS` -- several near-identical candidates, so which one is meant is a
  coin toss.
- `NO-DISAMBIG` -- two plausible candidates and nothing in the docs resolves
  which the question means. The docs should answer that, not the reader.
- `CONVENTION` -- the underlying data is present, but no named measure
  expresses the statistical or business convention the question needs, so
  anyone answering has to pick one and the model does not say which.

Work in this order. The enumeration is the job; skipping it is how this
judgement goes wrong.

**Step 1. Break the question into the quantities it needs.** A share, a rate, a
percentage, a per-something or an index needs at least TWO: a numerator and a
denominator. Name them as quantities, not as fields yet.

**Step 2. For EACH quantity, list every candidate in the model.** Do this even
when one looks obviously right, and especially then. A candidate does not have
to be a named measure. Before you close the list, walk the measures once more
and ask of each: does its doc say it BECOMES this quantity under some filtering
or grouping? A measure documented as equalling a total when nothing is filtered
is a candidate for that total, and it competes with any measure that carries the
total in its name.

**Step 3. Rule out the losers by QUOTING the model.** For each quantity, either
quote the doc sentence that says which candidate this question means, or record
null. A label that resembles the question's words is not a quote, and neither is
your own reasoning about which is more natural. If you cannot quote a sentence
that eliminates the other candidates, the quantity is unresolved, however
obvious the answer feels.

Then the verdict follows mechanically:

- Every quantity has exactly one candidate, or the model states which: `ok`.
- Some quantity has no candidate at all: `COVERAGE`.
- A quantity has several near-identical candidates: `AMBIGUOUS`.
- A quantity has two plausible candidates and nothing resolves which:
  `NO-DISAMBIG`.
- The parts exist but the model names no measure for the combination the
  question asks for, so whoever answers must assemble it and choose a
  convention: `CONVENTION`.

Three rules about that, because each is a way to reach `ok` wrongly:

1. **A question's words resembling a field's label is not the model resolving
   anything.** A question saying "total universe" and a measure labelled
   "Universe Estimate" is a candidate, not an answer. If another candidate is
   also defensible and would give a materially different number, the model has
   not resolved it and you must not resolve it yourself.
2. **Finding the numerator is not coverage.** If the numerator is named and the
   denominator must be assembled or chosen, that is `CONVENTION` or
   `NO-DISAMBIG`, never `ok`. Matching field names against the question is the
   failure mode this measurement exists to remove.
3. **A caveat is not a resolution.** A doc warning that a measure is easy to
   misuse still leaves the question open unless it says which reading this
   question's phrasing means.

Return ONLY a JSON object, no prose around it, keys in this order:

{{"quantities": {{"<quantity the question needs>": ["<every candidate>"]}},
  "ruled_out": {{"<quantity>": "<the doc sentence eliminating the others, or null>"}},
  "resolved_by": "what in the model says which candidate, or null",
  "why": "one or two sentences naming the specific entity or the specific gap",
  "verdict": "ok|COVERAGE|AMBIGUOUS|NO-DISAMBIG|CONVENTION",
  "entities": ["entity names that express it, or [] when it is not expressible"]}}

The enumeration comes first because the verdict follows from it. Before you emit
`verdict`, re-read `quantities` and `ruled_out`: if any quantity holds more than
one candidate and its `ruled_out` entry is null, the verdict is not `ok`.
"""


def json_objects(text: str) -> list[dict[str, Any]]:
    """Every JSON object in `text`, in the order they appear.

    Decoded from each `{` rather than matched with `\\{.*\\}`, which is greedy
    and spans the FIRST brace to the LAST. The prompt hands the agent the model
    text, so one `extend { ... }` quoted back in the narration would swallow the
    verdict and a good reply would read as unparseable.
    """
    dec, out, i = json.JSONDecoder(), [], 0
    while (i := text.find("{", i)) >= 0:
        try:
            v, end = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            i += 1
            continue
        if isinstance(v, dict):
            out.append(v)
        i = max(end, i + 1)
    return out


def parse_reply(text: str, allowed: tuple[str, ...]) -> dict[str, Any]:
    """The agent's JSON, or a null verdict saying why not.

    A verdict outside the vocabulary is dropped rather than recorded. An
    invented sixth value would otherwise land in the report and be counted as
    though it meant something.
    """
    found = json_objects(text or "")
    # The object that carries a verdict, and the last of them, so quoted model
    # text and a rehearsed answer above the real one both lose to it.
    carrying = [v for v in found if "verdict" in v]
    if not (carrying or found):
        return {"verdict": None, "why": "no JSON object in the reply",
                "entities": [], "quantities": {}, "resolved_by": None}
    v = (carrying or found)[-1]
    got = v.get("verdict")
    if got not in allowed:
        return {"verdict": None,
                "why": f"verdict {got!r} is not one of {', '.join(allowed)}",
                "entities": v.get("entities") or [], "quantities": {},
                "resolved_by": None}
    why = (v.get("why") or "").strip()
    ents = v.get("entities")
    ents = [str(x) for x in ents] if isinstance(ents, list) else []
    quantities = v.get("quantities")
    quantities = quantities if isinstance(quantities, dict) else {}
    resolved_by = v.get("resolved_by") or None
    # The enumeration is what the verdict is supposed to follow from, so an
    # `ok` that contradicts its own candidate list is not taken. This is the
    # rule the fixture exists to hold: several candidates and nothing naming
    # one is the definition of unresolved.
    if got == OK and not resolved_by:
        unresolved = [q for q, c in quantities.items()
                      if isinstance(c, list) and len(c) > 1]
        if unresolved:
            return {"verdict": "NO-DISAMBIG",
                    "why": f"[several candidates for {', '.join(unresolved)} "
                           f"and nothing in the model resolves them] {why}",
                    "entities": ents, "quantities": quantities,
                    "resolved_by": None}
    # `ok` with no entity named is not a measurement, it is an assertion. The
    # whole value of this metric is that a pass can be checked against the model.
    if got == OK and not ents:
        return {"verdict": None,
                "why": "ok without naming an entity that expresses the answer",
                "entities": [], "quantities": quantities,
                "resolved_by": resolved_by}
    return {"verdict": got, "why": why, "entities": ents,
            "quantities": quantities, "resolved_by": resolved_by}


def majority(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """The verdict most samples agreed on, with the spread kept beside it.

    A case near the line between `ok` and a gap flips between samples, and that
    is a property of the case rather than noise to average away: the fixture
    below reads CONVENTION twice and `ok` once. One sample cannot tell a stable
    judgement from a marginal one, so a repeated run records both the verdict
    and what the samples actually were.
    """
    counts: dict[str, int] = {}
    for r in rows:
        k = r["verdict"] or "undecided"
        counts[k] = counts.get(k, 0) + 1
    # Ties go to the least favourable answer: a case that reads `ok` half the
    # time is not covered, it is arguable, and scoring it as covered is the
    # optimistic direction this measurement must not drift in.
    top = max(counts.values())
    tied = sorted(k for k, n in counts.items() if n == top)
    gaps = [k for k in tied if k not in (OK, "undecided")]
    if len(gaps) > 1:
        # Two different gaps tied. WHICH gap it is has not been decided, and
        # taking the alphabetically-first would dress a coin flip as a finding,
        # so the case is undecided and drops out of the denominator instead.
        return {**rows[0], "verdict": None,
                "why": "samples disagreed on which gap: "
                       + ", ".join(f"{k} x{counts[k]}" for k in tied),
                "samples": [r["verdict"] for r in rows], "stable": False}
    best = gaps[0] if gaps else next((k for k in tied if k != OK), tied[0])
    winner = next((r for r in rows if (r["verdict"] or "undecided") == best),
                  rows[0])
    return {**winner,
            "verdict": None if best == "undecided" else best,
            "samples": [r["verdict"] for r in rows],
            "stable": len(counts) == 1}


def judge_case(case: dict[str, Any], model: str, a: argparse.Namespace,
               allowed: tuple[str, ...]) -> dict[str, Any]:
    concepts = case.get("requiresConcepts") or []
    prompt = PROMPT.format(model=model, question=case.get("question", ""),
                           concepts=", ".join(concepts) or "(none named)")
    if len(prompt) > MAX_PROMPT:
        # The way out depends on the mode: `--model-path` narrows a `--publisher`
        # read to one file and does nothing to a local one, where the fix is to
        # point `--model` at a single file instead of a tree.
        narrow = ("pass --model-path to measure one model file" if a.publisher
                  else "point --model at a single .malloy file, not a directory")
        return {"qid": case["qid"], "verdict": None,
                "why": f"model text too large for one prompt "
                       f"({len(prompt)} chars > {MAX_PROMPT}); {narrow}",
                "entities": []}
    cmd = ["claude", "-p", prompt, "--model", a.agent_model,
           "--output-format", "stream-json", "--verbose",
           # Two turns, not one: with no tools granted a single turn is enough,
           # and the second exists so that one stray tool attempt costs a turn
           # rather than the whole verdict.
           "--max-turns", "2", "--restricted",
           # This judge names no MCP server, and without the flag that is read
           # as "do not restrict MCP" rather than "grant none": measured
           # 2026-09-08, it was handed 72 of the operator's account connectors,
           # among them a live `execute_query`. The prompt above promises the
           # agent has no data access, so the flag is what makes that true.
           "--strict-mcp-config",
           "--disallowedTools", *JUDGE_BLOCKED]
    # `no_text`, not `no_events`: this judge is instrumentation, not the subject
    # of the measurement, so a call that emitted events but no usable text has
    # nothing to salvage and is worth re-running. `no_events` is the answerer's
    # predicate -- see the docstrings in agent_harness.py -- and it would leave
    # the `error_max_turns`-with-no-text case above unretried.
    _, text, stderr, _, _ = run_cli(cmd, cwd=str(a.set_dir), timeout=a.timeout,
                                    retry_when=no_text, retries=a.retries)
    out = parse_reply(text, allowed)
    if out["verdict"] is None and not text.strip():
        out["why"] = f"no reply from the model ({(stderr or '').strip()[:120]})"
    return {"qid": case["qid"], **out}


def judge_case_repeated(case: dict[str, Any], model: str,
                        a: argparse.Namespace, allowed: tuple[str, ...],
                        repeat: int) -> dict[str, Any]:
    rows = [judge_case(case, model, a, allowed) for _ in range(max(1, repeat))]
    return rows[0] if len(rows) == 1 else majority(rows)


# --- reporting ---------------------------------------------------------------

def summarise(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Coverage over the DECIDED cases, and a count per verdict.

    A case the checker could not decide is not evidence either way, so it is
    excluded from the percentage and reported on its own line. Folding it into
    the denominator would make a flaky judgement look like a model gap.
    """
    by: dict[str, int] = {}
    for r in rows:
        by[r["verdict"] or "undecided"] = by.get(r["verdict"] or "undecided", 0) + 1
    decided = sum(n for k, n in by.items() if k != "undecided")
    ok = by.get(OK, 0)
    return {"cases": len(rows), "decided": decided, "ok": ok,
            "coverage": (ok / decided) if decided else None,
            "by_verdict": by}


# The authored `coverage` field says an entity expresses the answer; the
# verdicts that say the same thing. Everything else is a gap of some kind.
LABEL_EXPECTS_OK = ("covered",)


def compare_labels(rows: list[dict[str, Any]],
                   cases: list[dict[str, Any]]) -> dict[str, Any]:
    """Join the verdicts against the set's own authored `coverage` field.

    NOT a score of this checker. A disagreement means the label and the model
    no longer agree, and EITHER can be the wrong one: the label is a standing
    judgement written at some point against some version of the model, and it
    goes stale when the model gains a measure. Measured on the ecommerce set,
    the label was the wrong side more often than the verdict was, so this
    reports the disagreements for a human to triage and takes no position on
    which way each one resolves.

    That is the whole value of the join. Without it the two numbers are read
    side by side, the headlines agree to within a couple of points because the
    two error directions cancel, and nobody looks at the cases.
    """
    by_qid = {c["qid"]: c for c in cases}
    matrix: dict[tuple[str, str], int] = {}
    disagree: list[dict[str, Any]] = []
    decided = 0
    for r in rows:
        c = by_qid.get(r["qid"]) or {}
        label = c.get("coverage") or "unlabelled"
        verdict = r["verdict"] or "undecided"
        matrix[(label, verdict)] = matrix.get((label, verdict), 0) + 1
        if verdict == "undecided" or label == "unlabelled":
            continue
        decided += 1
        if (label in LABEL_EXPECTS_OK) != (verdict == OK):
            disagree.append({
                "qid": r["qid"], "label": label, "verdict": verdict,
                "coverageNote": c.get("coverageNote"),
                "why": r.get("why"),
                # Which way it reads BEFORE anyone looks: a label claiming a
                # gap the model can express is the stale-label shape, and it is
                # the one worth checking first.
                "shape": ("label says gap, model says expressible"
                          if verdict == OK else
                          "label says covered, model says gap"),
            })
    return {"compared": decided, "agree": decided - len(disagree),
            "disagree": disagree, "matrix": matrix,
            "labels": sorted({(c.get("coverage") or "unlabelled")
                              for c in cases})}


def label_report(cmp: dict[str, Any]) -> str:
    n, agree = cmp["compared"], cmp["agree"]
    out = ["", f"against the set's authored `coverage` field: {agree}/{n} agree"
                + (f" ({100 * agree / n:.0f}%)" if n else "")]
    if not n:
        out.append("  no case carries a `coverage` field; nothing to compare")
        return "\n".join(out)
    verds = [OK, *FAIL_VERDICTS, "undecided"]
    out.append(f"  {'label':>12} " + " ".join(f"{v:>12}" for v in verds))
    for lab in cmp["labels"]:
        out.append(f"  {lab:>12} "
                   + " ".join(f"{cmp['matrix'].get((lab, v), 0):>12}"
                              for v in verds))
    if cmp["disagree"]:
        out.append("")
        out.append("  disagreements, for triage. EITHER side can be the wrong "
                   "one; check the label first")
        for d in cmp["disagree"]:
            out.append(f"    {d['qid']:36s} label={d['label']:10s} "
                       f"verdict={d['verdict']}")
            out.append(f"      {d['shape']}")
            if d.get("coverageNote"):
                out.append(f"      note: {str(d['coverageNote'])[:88]}")
    return "\n".join(out)


def report(rows: list[dict[str, Any]], s: dict[str, Any],
           version: str | None) -> str:
    out = [f"{'qid':38s} {'verdict':12s} why"]
    for r in sorted(rows, key=lambda x: (x["verdict"] or "undecided", x["qid"])):
        out.append(f"{r['qid']:38s} {str(r['verdict'] or 'undecided'):12s} "
                   f"{(r['why'] or '')[:90]}")
    pct = "n/a" if s["coverage"] is None else f"{100 * s['coverage']:.0f}%"
    out.append("")
    out.append(f"coverage {pct} ({s['ok']}/{s['decided']} decided"
               + (f", {s['cases']} cases" if s["cases"] != s["decided"] else "")
               + ")" + (f" at version {version}" if version else ""))
    out.append("  " + "  ".join(f"{k} {n}" for k, n in sorted(s["by_verdict"].items())))
    return "\n".join(out)


# --- the fixture -------------------------------------------------------------
#
# One case whose right answer is known, kept because a coverage measurement that
# string-matches the question against field names scores it `ok` and is then
# worthless on exactly the cases worth measuring.
#
# SYNTHETIC, deliberately. The shape is what the fixture tests, not the domain:
# the numerator the question needs is named, the denominator its phrasing
# implies is not, three defensible denominators give materially different
# numbers, and one measure's LABEL resembles the question's words while its doc
# says it measures something else. A real customer model would test exactly the
# same thing and could not ship in a public repo.

FIXTURE_MODEL = """-- support_desk.malloy (excerpt: the fields in play)
source: tickets is duckdb.table('tickets.parquet') extend {
  #(doc) Every ticket row the desk received, spam and merged duplicates
  #(doc) included. Filter with `where: is_actionable` to exclude those.
  measure: ticket_count is count()

  #(doc) Tickets an agent closed without handing off, counted at the ticket
  #(doc) grain. This is the numerator of first-contact resolution.
  measure: first_contact_resolutions is count() { where: handoff_count = 0 }

  #(doc) Tickets that reached a human at all -- lower than `ticket_count`,
  #(doc) because unactioned spam is never assigned to anyone.
  measure: answered_ticket_count is
    count() { where: first_responded_at is not null }

  #(doc) Whether the ticket is a real customer request: false for spam, and
  #(doc) false for a duplicate that was merged into another ticket.
  dimension: is_actionable is not is_spam and merged_into_id is null

  #(doc) Share of CLOSED tickets that ended resolved rather than withdrawn.
  #(doc) NOTE: this is NOT first-contact resolution. It says nothing about how
  #(doc) many touches a ticket took, and its denominator is closed tickets
  #(doc) rather than received ones.
  measure: resolution_rate is count() { where: closed_reason = 'resolved' }
    / nullif(count() { where: closed_at is not null }, 0)
}
"""

FIXTURE_CASE = {
    "qid": "fixture_first_contact_resolution_rate",
    "question": "What was our first contact resolution rate last quarter?",
    "requiresConcepts": ["first contact resolution", "ticket population"],
}
FIXTURE_EXPECTED = ("CONVENTION", "NO-DISAMBIG")



def self_check(a: argparse.Namespace, allowed: tuple[str, ...]) -> int:
    repeat = a.repeat if a.repeat is not None else 3
    r = judge_case_repeated(FIXTURE_CASE, FIXTURE_MODEL, a, allowed, repeat)
    print(f"{r['qid']}: {r['verdict']}"
          + (f"  (samples: {', '.join(str(x) for x in r['samples'])})"
             if r.get("samples") else "")
          + f"\n  {r['why']}")
    if r["verdict"] in FIXTURE_EXPECTED:
        print(f"\nok: the fixture reads as {r['verdict']}, so the measurement "
              f"is judging expressibility rather than matching field names.")
        return 0
    if r["verdict"] == OK:
        print(f"\nFAIL: the fixture came back `ok`. The numerator "
              f"(first_contact_resolutions) is in the model and the denominator "
              f"the question implies is not, so this is field-name matching "
              f"and the metric will be wrong on every case that matters. "
              f"Expected one of {', '.join(FIXTURE_EXPECTED)}.")
        return 1
    print(f"\nFAIL: expected one of {', '.join(FIXTURE_EXPECTED)}, got "
          f"{r['verdict']!r}.")
    return 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="set_dir", type=pathlib.Path, default=None,
                    help="the eval set; its cases supply question and "
                         "requiresConcepts, and nothing in it is written")
    ap.add_argument("--model", dest="model_path", type=pathlib.Path, default=None,
                    help="the package under measurement, as a local file or "
                         "directory")
    ap.add_argument("--publisher", default=None,
                    help="read the model over REST instead, from a running "
                         "Publisher serving the version to measure")
    ap.add_argument("--environment", default="samples")
    ap.add_argument("--package", default=None)
    ap.add_argument("--model-path", dest="one_model", default=None,
                    help="with --publisher: measure one model file rather than "
                         "every model in the package")
    ap.add_argument("--version", default=None,
                    help="the version label to stamp the report with, for a "
                         "trend across published versions")
    # `dest="agent_model"`, not `dest="model"`: `--model` is the package under
    # measurement, so a bare `a.model` would read as that and be the other one.
    ap.add_argument("--agent-model", dest="agent_model", default="sonnet",
                    help="the model that reads the semantic model")
    ap.add_argument("--parallel", type=int, default=4)
    ap.add_argument("--repeat", type=int, default=None,
                    help="sample each case N times and take the majority, so a "
                         "case sitting on the line reads as arguable rather "
                         "than as whichever way it fell. Default 1 for a set "
                         "(the score is a trend, not a verdict on one case) "
                         "and 3 for --self-check. A tie goes to the gap")
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--only", default=None, help="comma-separated qids")
    ap.add_argument("--out", type=pathlib.Path, default=None,
                    help="write the report as JSON here")
    ap.add_argument("--compare-labels", dest="compare_labels",
                    action="store_true",
                    help="join the verdicts against the set's authored "
                         "`coverage` field and print the disagreements. This "
                         "does NOT score the checker: a label is a standing "
                         "judgement that goes stale when the model gains a "
                         "measure, so either side can be the wrong one. Use it "
                         "to find the cases worth a human look, and check the "
                         "label first")
    ap.add_argument("--self-check", action="store_true",
                    help="measure the built-in fixture instead of a set, and "
                         "fail if it reads as `ok`. One model call")
    a = ap.parse_args(argv)
    # Refused at the CLI, not just clamped downstream by `max(1, repeat)`. Zero
    # samples is not a cheaper measurement, it is no measurement, and the same
    # flag on check_judge.py used to report every fixture green without calling
    # the judge once.
    if a.repeat is not None and a.repeat < 1:
        ap.error(f"--repeat must be at least 1, got {a.repeat}. It is how many "
                 f"times each case is sampled; 0 would measure nothing. "
                 f"Fix: --repeat 3")

    diagnose_codes()
    allowed = verdicts()

    if a.self_check:
        a.set_dir = a.set_dir or pathlib.Path.cwd()
        return self_check(a, allowed)

    if not a.set_dir:
        raise SystemExit("--set is required (or --self-check)")
    if a.model_path:
        model = local_model_text(a.model_path)
        if not model:
            raise SystemExit(f"no .malloy text under {a.model_path}")
    elif a.publisher and a.package:
        try:
            model = rest_model_text(a.publisher, a.environment, a.package,
                                    a.one_model)
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"could not read the model from {a.publisher}: {exc}")
        if not model:
            raise SystemExit(f"{a.publisher} served no model text for "
                             f"{a.environment}/{a.package}")
    else:
        raise SystemExit("pass --model <path>, or --publisher with --package")

    cases = [json.loads(l) for l in (a.set_dir / "cases.jsonl").read_text()
             .splitlines() if l.strip()]
    if a.only:
        want = {q.strip() for q in a.only.split(",")}
        cases = [c for c in cases if c["qid"] in want]
    if not cases:
        raise SystemExit("no cases to measure")

    print(f"{len(cases)} cases, {len(model)} chars of model, "
          f"{a.parallel} at a time")
    rows: list[dict[str, Any]] = []
    with futures.ThreadPoolExecutor(max_workers=max(1, a.parallel)) as ex:
        fs = {ex.submit(judge_case_repeated, c, model, a, allowed,
                        a.repeat or 1): c for c in cases}
        for i, f in enumerate(futures.as_completed(fs), 1):
            r = f.result()
            rows.append(r)
            print(f"  [{i}/{len(cases)}] {r['qid']} {r['verdict']}", flush=True)

    s = summarise(rows)
    print("\n" + report(rows, s, a.version))
    cmp = compare_labels(rows, cases) if a.compare_labels else None
    if cmp:
        print(label_report(cmp))
    if a.out:
        a.out.write_text(json.dumps(
            {"version": a.version, "set": str(a.set_dir),
             "agentModel": a.agent_model, **s, "cases_detail": rows,
             **({"labelComparison": cmp} if cmp else {})},
            indent=2))
        print(f"\n{a.out}")
    # Nothing decided is not a 0% coverage, it is a measurement that did not
    # happen, and the docstring already promises exit 1 when it could not run.
    # The whole-set version of this is the failure mode worth catching: an
    # over-long prompt fails identically on every case, so the run reports
    # `n/a` on every line and used to exit 0 like a success.
    if s["decided"] == 0:
        print("\nno case was decided, so there is no measurement here. The "
              "per-case `why` says which; a prompt over the size cap, or over "
              "the OS argv limit, fails this way on every case at once.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
