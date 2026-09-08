#!/usr/bin/env python3
"""The run package's Malloy source and its CSVs have to agree.

A `public:` naming a column write_csv does not write is a compile error that
takes the whole package down, so that direction is checked exhaustively. The
reverse is NOT an error: `include { public: ... }` is an allowlist, and leaving a
column undeclared is how the package hides one on purpose.

What is checked instead is the specific drift that happened: `retrievalMode`,
`targetVersion`, `modelGitSha` and `reExecution` were written to run.json,
summarised by flip_table's console, and then dropped here -- so two arms could
sit side by side in the data app with nothing saying they had measured different
models. Recorded at one end of the pipeline, invisible at the other.

Checked against the REAL headers, by building a package from a synthetic run
rather than by re-parsing build_run_package's column lists.
"""
import csv
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
SCRIPT = HERE / "build_run_package.py"
MALLOY = (HERE.parent / "templates" / "eval-run-package" / "eval_run.malloy")

# `source: <name> is duckdb.table('data/<file>.csv')` then an `include { ... }`
# block of `public: <column>` lines, up to the closing brace.
SOURCE = re.compile(
    r"source:\s+(\w+)\s+is\s+duckdb\.table\('data/([\w.]+)\.csv'\)\s*"
    r"include\s*\{(.*?)\n\}", re.S)
PUBLIC = re.compile(r"^\s*public:\s+(\w+)\s*$", re.M)

RUN_JSON = {
    "runId": "r1", "label": "aa-1", "target": "platform",
    "answererModel": "sonnet", "effort": "medium",
    "started": "2026-09-07T00:00:00Z", "judgeVersion": "3",
    "datasetVersion": "1.0", "retrievalMode": "semantic",
    "targetVersion": "0.0.58", "modelGitSha": "abc123", "modelRepo": "/repo",
    "reExecution": {"attempted": 4, "ok": 3, "failed": 1, "noQuery": 0},
}
EVENTS = [
    {"kind": "attempt", "qid": "q1", "sample": 1, "phase": "baseline",
     "submitted": True, "final_query": "run: x -> y", "answer_text": "42",
     "contaminated": False},
    {"kind": "score", "qid": "q1", "sample": 1, "phase": "baseline",
     "verdict": "match", "outcome": "pass"},
]


class SourcesMatchTheCsvs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = pathlib.Path(tempfile.mkdtemp())
        run, sset, out = cls.tmp / "run1", cls.tmp / "set", cls.tmp / "pkg"
        run.mkdir()
        sset.mkdir()
        (run / "run.json").write_text(json.dumps(RUN_JSON))
        (run / "events.jsonl").write_text(
            "\n".join(json.dumps(e) for e in EVENTS) + "\n")
        (sset / "set.json").write_text(
            json.dumps({"datasetVersion": "1.0", "package": "ecommerce"}))
        (sset / "cases.jsonl").write_text(
            json.dumps({"qid": "q1", "question": "how many?"}) + "\n")
        p = subprocess.run(
            [sys.executable, str(SCRIPT), "--run", str(run),
             "--set", str(sset), "--out", str(out)],
            capture_output=True, text=True, timeout=300)
        assert p.returncode == 0, p.stderr
        cls.data = out / "data"

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def headers(self, stem: str) -> list[str]:
        return self.rows(stem)[0]

    def rows(self, stem: str, data: pathlib.Path | None = None) -> list[list]:
        with ((data or self.data) / f"{stem}.csv").open() as fh:
            return list(csv.reader(fh))

    def sources(self):
        found = SOURCE.findall(MALLOY.read_text())
        self.assertTrue(found, "no duckdb.table sources parsed from the model")
        return found

    def test_every_declared_column_is_actually_written(self):
        for name, stem, block in self.sources():
            if not (self.data / f"{stem}.csv").exists():
                continue
            written = set(self.headers(stem))
            for col in PUBLIC.findall(block):
                self.assertIn(col, written,
                              f"{name} declares public: {col}, but "
                              f"{stem}.csv has no such column")

    # There is deliberately no "every written column is declared" test. Malloy's
    # `include { public: ... }` is an ALLOWLIST, so a column written and left
    # undeclared is how the package hides one on purpose -- `clusters.evidence`
    # and the duplicate `scores.contaminated` are both hidden that way. The
    # named-pin test below is the real guard: it pins the fields whose absence
    # was the defect, without asserting an invariant the mechanism contradicts.

    def test_the_run_level_pins_are_declared_and_not_merely_written(self):
        # The defect this closes: these reached run.json and flip_table's
        # console and stopped there, so the data app could show two arms with
        # nothing saying they had measured different models or retrievers.
        block = next(b for n, _, b in self.sources() if n == "runs")
        declared = set(PUBLIC.findall(block))
        for col in ("retrieval_mode", "target_version", "model_git_sha",
                    "model_repo", "reexec_attempted", "reexec_ok",
                    "reexec_failed", "reexec_no_query"):
            self.assertIn(col, declared,
                          f"runs.csv carries {col} and the source does not "
                          f"expose it, so no view can read it")

    def test_the_run_level_pins_carry_real_values(self):
        # Declared and written is not enough: the values have to survive the
        # copy out of run.json.
        head, first = self.rows("runs")[:2]
        row = dict(zip(head, first))
        self.assertEqual(row["retrieval_mode"], "semantic")
        self.assertEqual(row["target_version"], "0.0.58")
        self.assertEqual(row["model_git_sha"], "abc123")
        self.assertEqual(row["reexec_attempted"], "4")
        self.assertEqual(row["reexec_failed"], "1")

    def test_a_run_without_the_new_fields_writes_them_empty_not_missing(self):
        # Older runs predate every one of them. The column must still exist, so
        # the model compiles, and read as null rather than as a value.
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            run, sset, out = tmp / "run1", tmp / "set", tmp / "pkg"
            run.mkdir()
            sset.mkdir()
            (run / "run.json").write_text(json.dumps(
                {"runId": "old", "label": "old", "judgeVersion": "3"}))
            (run / "events.jsonl").write_text(
                "\n".join(json.dumps(e) for e in EVENTS) + "\n")
            (sset / "set.json").write_text(json.dumps({"package": "x"}))
            (sset / "cases.jsonl").write_text(
                json.dumps({"qid": "q1", "question": "q?"}) + "\n")
            p = subprocess.run(
                [sys.executable, str(SCRIPT), "--run", str(run),
                 "--set", str(sset), "--out", str(out)],
                capture_output=True, text=True, timeout=300)
            self.assertEqual(p.returncode, 0, p.stderr)
            head, first = self.rows("runs", out / "data")[:2]
            row = dict(zip(head, first))
            for col in ("retrieval_mode", "target_version", "model_git_sha",
                        "reexec_attempted"):
                self.assertIn(col, row)
                self.assertEqual(row[col], "")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
