#!/usr/bin/env python3
"""The eval ledger in code: the one writer and validator for run records.

`reference/ledger-schema.md` is the human-readable rendering of this module;
this module is the contract. Every script that writes `run.json` or
`events.jsonl` builds its lines through `event()` / `run_config()`, so a field
rename that reaches only one writer fails at write time instead of producing a
ledger the readers silently misread. Stdlib only, like everything else here.

Validate a run directory (or several) from a shell:

  python ledger.py validate <runDir> [<runDir> ...]

Exit 1 on any error. Old runs written before a field existed validate clean:
required fields are the run's identity, everything since is optional, and the
pins an A/B needs are WARNED about when absent rather than failed, because a
run that cannot take part in a comparison is still a run.

Events are FLAT: `kind` plus the fields, one JSON object per line. An earlier
draft of the schema nested fields under `payload` with a `caseId`; no writer
ever did that, and 23 run directories exist in the flat shape, so the flat
shape is the contract and the draft was the bug.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import subprocess
import sys
import time
from typing import Any, Callable, Iterable

# --- field specs -------------------------------------------------------------
#
# required: the key must be PRESENT (None is a legal value where the schema
# says "null when unknown" -- absence and null are different claims).
# optional: may be present. Anything else is a write error.

_CASE = {"qid", "sample", "phase"}

EVENTS: dict[str, dict[str, set[str]]] = {
    "attempt": {
        "required": _CASE | {"submitted", "final_query", "answer_text",
                             "transcriptPath"},
        "optional": {"question_sha", "servedRevision", "n_get_context",
                     "n_execute", "n_execute_errors", "host_tool_uses",
                     "reported_calls", "contaminated", "contamination_reasons",
                     "input_tokens", "output_tokens", "cache_read_tokens",
                     "cost_usd", "num_turns", "wall_seconds", "run_error", "at"},
    },
    "tool_call": {
        "required": _CASE | {"tool"},
        "optional": {"targets", "rankedSummary", "error", "traceId", "at"},
    },
    "score": {
        "required": _CASE | {"verdict", "reason"},
        "optional": {"judge_version", "rubric_sha", "golden_revision",
                     "artifactPath", "confidence", "column_pairing",
                     "contaminated", "gold_status", "gold_note", "at"},
    },
    "retrieval_score": {
        "required": {"intentId", "term"},
        "optional": {"entityType", "in_scope", "coverage", "returned",
                     "resultCount", "judgments", "error", "judge_version",
                     "rubric_sha", "traceId", "at", "servedVersion"},
    },
    # A get_context contract test: one request, one or more assertions over the
    # response, no LLM anywhere. `bugRef` ties it to the register entry it came
    # from, which is what lets a run say "these bugs still reproduce".
    "probe": {
        "required": {"probeId", "bugRef", "passed", "assertions"},
        "optional": {"request", "status", "failures", "responseSummary",
                     "error", "wall_seconds", "at", "inconclusive"},
    },
    "issue": {
        "required": {"issue_id", "qids", "primary_code", "component",
                     "owner", "diagnosis"},
        "optional": {"contributing_codes", "severity", "confidence",
                     "sufficiency", "traceIds", "evidence", "diagnosedBy",
                     "clusteredBy", "at"},
    },
    "issue_status": {
        "required": {"issue_id", "status"},
        "optional": {"at"},
    },
    "candidate": {
        "required": {"issue_ids", "files", "diffSummary", "probes", "edit"},
        "optional": {"qids", "editTier", "disagreement", "compiled",
                     "patchPath", "meaningChanged", "goldenSuspect",
                     "goldenAudit", "improvedBy", "at"},
    },
    "acceptance_check": {
        "required": {"decision", "baselineRunId", "finalRunIds",
                     "regressions", "reason"},
        "optional": {"issue_ids", "class", "holdoutDelta", "at"},
    },
    "checkpoint": {
        "required": {"action", "modelGitSha"},
        "optional": {"label", "issueIds", "at"},
    },
}

ISSUE_STATUSES = {"open", "batched", "fixed", "rejected", "deferred"}
VERDICTS = {"match", "near_match", "no_match", "needs_human", None}

# run.json. Required is the run's identity -- the oldest runs on disk carry
# these. Recommended are the pins two runs must share (or differ in exactly
# one of) to be comparable; their absence is a warning, not an error.
RUN_REQUIRED = {"runId", "target", "answererModel", "phase", "started"}
# `answererManifest` is recommended because a run that cannot say which skills
# the answerer held cannot be compared with one that held different skills --
# and, before 2026-09-01, every run held none while `skillsVersion` (the eval
# harness's own sha) made it look otherwise.
RUN_RECOMMENDED = {"judgeModel", "judgeVersion", "datasetVersion", "modelSha",
                   "skillsVersion", "answererManifest"}
RUN_OPTIONAL = {"label", "effort", "environment", "package", "modelPath",
                "modelGitSha", "mcpUrl", "publisher", "predictionsReExecuted",
                "serverVersion", "diagnoserModel", "improverModel",
                "rubricSha", "setName", "targetVersion", "scope", "mode",
                "traceMode",
                "callBudget", "status", "answererSkills",
                "answererCostUsd", "judgeCostUsd", "goldenCheck",
                "skillsRoot", "harnessVersion",
                "judgeSkills", "diagnoserManifest",
                "improverManifest", "doubtedGoldens",
                "packageSha", "servedRevision", "datasetSha"} | RUN_RECOMMENDED


def dataset_sha(set_dir: pathlib.Path) -> str | None:
    """Content hash of the set: `cases.jsonl` plus `set.json`.

    Separate from the model's sha ON PURPOSE. A golden repair is not a model
    change, and one pin covering both would make every answer-key fix read as an
    edit to the model -- which is exactly the distinction an A/B rests on.

    Automatic, so nobody has to remember it. `datasetVersion` stays beside it as
    the human-readable sequence, and `goldenRevision` stays per case, because a
    score event has to be able to say WHICH key it compared against in a form a
    person can talk about.
    """
    h = hashlib.sha256()
    for name in ("set.json", "cases.jsonl"):
        f = set_dir / name
        h.update(name.encode())
        h.update(b"\0")
        h.update(f.read_bytes() if f.exists() else b"")
        h.update(b"\0")
    return h.hexdigest()


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# `acceptance_check` was called `gate` until 2026-09-03. Run directories written
# before the rename still validate: the old kind is read as the new one, and
# nothing rewrites them. Only the name changed -- "gate" meant a CI gate, a
# feature gate and this, and the word carried none of what the step does.
LEGACY_KINDS = {"gate": "acceptance_check"}


def canonical_kind(kind: str) -> str:
    return LEGACY_KINDS.get(kind, kind)


def _check(kind: str, fields: dict[str, Any]) -> list[str]:
    kind = canonical_kind(kind)
    spec = EVENTS.get(kind)
    if spec is None:
        return [f"unknown event kind {kind!r}"]
    problems = [f"{kind}: missing required field {f!r}"
                for f in sorted(spec["required"] - fields.keys())]
    problems += [f"{kind}: unknown field {f!r}"
                 for f in sorted(fields.keys() - spec["required"]
                                 - spec["optional"] - {"kind"})]
    if kind == "issue_status" and fields.get("status") not in ISSUE_STATUSES:
        problems.append(f"issue_status: bad status {fields.get('status')!r}")
    if kind == "score" and fields.get("verdict") not in VERDICTS:
        problems.append(f"score: bad verdict {fields.get('verdict')!r}")
    return problems


def event(kind: str, **fields: Any) -> dict[str, Any]:
    """Build one ledger event, or raise on a field the schema does not know."""
    problems = _check(kind, fields)
    if problems:
        raise ValueError("; ".join(problems))
    return {"kind": kind, **fields}


def run_config(**fields: Any) -> dict[str, Any]:
    """Build run.json content; raises on missing identity or unknown fields."""
    missing = sorted(RUN_REQUIRED - fields.keys())
    unknown = sorted(fields.keys() - RUN_REQUIRED - RUN_OPTIONAL)
    if missing or unknown:
        raise ValueError(
            "; ".join([f"run.json missing {f!r}" for f in missing]
                      + [f"run.json unknown field {f!r}" for f in unknown]))
    return dict(fields)


# --- io -----------------------------------------------------------------------

def read_jsonl(p: pathlib.Path) -> list[dict[str, Any]]:
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def write_events(path: pathlib.Path, events: Iterable[dict[str, Any]]) -> None:
    with path.open("w") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")


def append_events(path: pathlib.Path, events: Iterable[dict[str, Any]]) -> None:
    with path.open("a") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")


def replace_events(path: pathlib.Path,
                   keep: Callable[[dict[str, Any]], bool],
                   new: Iterable[dict[str, Any]]) -> None:
    """Keyed replace for re-runnable stages (diagnose, improve): keep every
    line `keep` approves, then append `new`. The append-only rule is about
    stages not rewriting EACH OTHER's fields; a stage replacing its own prior
    output after a crash is the sanctioned exception, and it goes through here
    so the pattern exists once."""
    fresh = [e for e in read_jsonl(path) if keep(e)] if path.exists() else []
    write_events(path, list(fresh) + list(new))


def update_run(run_dir: pathlib.Path, **pins: Any) -> dict[str, Any]:
    """Merge pins into run.json -- the one mutable file. Later stages record
    their own model here (diagnoserModel, improverModel) so the run names
    every LLM that touched it."""
    unknown = sorted(pins.keys() - RUN_REQUIRED - RUN_OPTIONAL)
    if unknown:
        raise ValueError(f"run.json unknown fields {unknown}")
    p = run_dir / "run.json"
    cfg = json.loads(p.read_text()) if p.exists() else {}
    cfg.update(pins)
    p.write_text(json.dumps(cfg, indent=2))
    return cfg


def skills_git_sha(root: pathlib.Path | None = None) -> str | None:
    """HEAD of the repo holding the skills the agents loaded, dirty-marked --
    the `skillsVersion` pin. Defaults to this checkout; pass the external
    --skills-root when the doctrine came from elsewhere (a Publisher checkout).
    The skills ARE the doctrine the agents load, so a run that cannot name
    their version cannot take part in a comparison."""
    d = pathlib.Path(root) if root else pathlib.Path(__file__).resolve().parent
    try:
        head = subprocess.run(["git", "-C", str(d), "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=10)
        if head.returncode != 0:
            return None
        dirty = subprocess.run(["git", "-C", str(d), "status", "--porcelain"],
                               capture_output=True, text=True, timeout=10)
        return head.stdout.strip() + ("-dirty" if dirty.stdout.strip() else "")
    except Exception:  # noqa: BLE001
        return None


# --- validation ---------------------------------------------------------------

def validate_run(run_dir: pathlib.Path) -> tuple[list[str], list[str]]:
    """(errors, warnings) for one run directory."""
    errors: list[str] = []
    warnings: list[str] = []

    rj = run_dir / "run.json"
    if not rj.exists():
        errors.append("run.json missing")
    else:
        try:
            cfg = json.loads(rj.read_text())
        except json.JSONDecodeError as e:
            errors.append(f"run.json unparseable: {e}")
            cfg = {}
        for f in sorted(RUN_REQUIRED - cfg.keys()):
            errors.append(f"run.json: missing {f!r}")
        # A probe run has no judge, no answerer and no re-executed
        # predictions, so the judge and model pins can never be filled for it.
        # Warning about them anyway is noise that teaches readers to skip
        # warnings, which is worse than not checking.
        recommended = RUN_RECOMMENDED
        if cfg.get("phase") == "probe":
            recommended = recommended - {"judgeModel", "judgeVersion",
                                         "modelSha", "answererManifest"}
        for f in sorted(recommended - cfg.keys()):
            warnings.append(f"run.json: no {f!r} -- this run cannot anchor "
                            f"a comparison on that pin")
        for f in sorted(cfg.keys() - RUN_REQUIRED - RUN_OPTIONAL):
            warnings.append(f"run.json: unknown field {f!r}")
        sha = cfg.get("modelGitSha") or ""
        if isinstance(sha, str) and sha.endswith("-dirty"):
            warnings.append("run.json: modelGitSha is -dirty; fine for a "
                            "band measurement, not for an A/B pin")

    ev = run_dir / "events.jsonl"
    if not ev.exists():
        errors.append("events.jsonl missing")
        return errors, warnings

    seen_scores: set[tuple[Any, Any, Any]] = set()
    for n, line in enumerate(ev.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            errors.append(f"events.jsonl:{n}: unparseable line")
            continue
        kind = e.get("kind")
        for p in _check(kind, {k: v for k, v in e.items() if k != "kind"}):
            # Unknown fields on EXISTING files are warnings: old runs carry
            # fields that predate the spec, and grandfathering them beats
            # rewriting an append-only ledger. Missing required stays an error.
            (warnings if "unknown field" in p else errors).append(
                f"events.jsonl:{n}: {p}")
        if kind == "score":
            key = (e.get("qid"), e.get("sample"), e.get("phase"))
            if key in seen_scores:
                errors.append(f"events.jsonl:{n}: second score for {key} -- "
                              f"every attempt gets exactly one")
            seen_scores.add(key)
    return errors, warnings


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[0] != "validate":
        print(__doc__)
        return 2
    bad = False
    for arg in argv[1:]:
        run_dir = pathlib.Path(arg)
        errors, warnings = validate_run(run_dir)
        status = "FAIL" if errors else "ok"
        print(f"{status}  {run_dir}  ({len(errors)} errors, "
              f"{len(warnings)} warnings)")
        for e in errors:
            print(f"  error: {e}")
        for w in warnings:
            print(f"  warn:  {w}")
        bad = bad or bool(errors)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
