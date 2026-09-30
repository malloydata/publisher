#!/usr/bin/env python3
"""Tests for check_set.py: every gap in a set, named before anything starts."""
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_set  # noqa: E402

CASE = {"qid": "q1", "question": "How many?", "split": "dev",
        "golden": {"status": "provisional", "kind": "scalar", "value": {"n": 1},
                   "canonicalQuery": "run: t -> { aggregate: n is count() }"}}


def a_set(toml: str, cases: list | None = None, truth: str | None = None,
          truth_malloy: str = "") -> pathlib.Path:
    root = pathlib.Path(tempfile.mkdtemp(prefix="check-set-")).resolve()
    (root / "pkg").mkdir()
    (root / "pkg" / "publisher.json").write_text("{}")
    d = root / "set"
    d.mkdir()
    (d / "set.json").write_text(json.dumps(
        {"name": "s", **({"truthPackage": truth} if truth else {})}))
    (d / "eval.toml").write_text(toml)
    if cases is not None:
        (d / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
    if truth_malloy:
        (root / "truth").mkdir()
        (root / "truth" / "publisher.json").write_text("{}")
        (root / "truth" / "truth.malloy").write_text(truth_malloy)
    return d


MODEL = '[model]\nenvironment = "e"\npackage = "p"\nrepo = "../pkg"\n'


class Report(unittest.TestCase):
    def setUp(self):
        # CI runs these without a built Publisher; that check has its own line.
        built = mock.patch.object(check_set.config.Config, "publisher_dir",
                                  return_value=pathlib.Path("/built"))
        built.start()
        self.addCleanup(built.stop)

    def report(self, d):
        with mock.patch.object(check_set.serve, "listening", return_value=False):
            return check_set.report(d)

    def test_a_complete_set_has_no_problems(self):
        problems, notes = self.report(a_set(MODEL, [CASE]))
        self.assertEqual(problems, [])
        self.assertIn("1 cases from 1 lines", notes)

    def test_every_gap_is_named_at_once(self):
        # No cases.jsonl, no [model] environment, a truth package named in
        # set.json with no [truth]: three scripts used to find these one by one.
        d = a_set('[model]\npackage = "p"\nrepo = "../pkg"\n', truth="t")
        problems, _ = self.report(d)
        text = "\n".join(problems)
        self.assertIn("no cases.jsonl", text)
        self.assertIn("no [model] environment", text)
        self.assertIn("has no [truth] section", text)

    def test_a_truth_ref_that_leaves_the_package_is_a_problem(self):
        d = a_set(MODEL + '[truth]\npackage_dir = "../truth"\n', [CASE], truth="t",
                  truth_malloy="source: u is duckdb.table('../pkg/data/u.parquet')\n")
        problems, _ = self.report(d)
        self.assertEqual(len(problems), 1)
        self.assertIn("resolves to nothing once served", problems[0])

    def test_what_init_truth_package_writes_passes(self):
        # The scaffolder links the model's data in; check once refused that
        # link, so the two tools disagreed on the package one of them wrote.
        d = a_set(MODEL + '[truth]\npackage_dir = "../truth"\n', [CASE], truth="t")
        pkg = d.parent / "pkg"
        (pkg / "data").mkdir()
        (pkg / "data" / "u.parquet").write_text("")
        (pkg / "m.malloy").write_text("source: u is duckdb.table('data/u.parquet')\n")
        with mock.patch("builtins.print"):
            check_set.init_truth_package.main(
                ["--package", str(pkg), "--out", str(d.parent / "truth"), "--name", "t"])
        problems, _ = self.report(d)
        self.assertEqual(problems, [])

    def test_eval_toml_and_set_json_naming_two_packages_is_a_problem(self):
        d = a_set(MODEL, [CASE])
        meta = json.loads((d / "set.json").read_text())
        (d / "set.json").write_text(json.dumps({**meta, "targetPackage": "q"}))
        problems, _ = self.report(d)
        self.assertEqual(len(problems), 1)
        self.assertIn("[model] package is 'p' but set.json targetPackage is 'q'",
                      problems[0])

    def test_runs_left_in_the_set_directory_are_noted(self):
        d = a_set(MODEL, [CASE])
        (d / "runs" / "baseline-01").mkdir(parents=True)
        problems, notes = self.report(d)
        self.assertEqual(problems, [])
        self.assertTrue(any("from before runs moved to the workdir" in n
                            for n in notes))

    def test_a_case_the_importer_refuses_is_a_problem(self):
        bad = {**CASE, "golden": {**CASE["golden"], "value": 1}}
        problems, _ = self.report(a_set(MODEL, [bad]))
        self.assertIn("holds a bare int", "\n".join(problems))

    def test_a_port_held_by_another_process_names_the_key(self):
        d = a_set(MODEL, [CASE])
        with mock.patch.object(check_set.serve, "listening",
                               side_effect=lambda p: p == 4811):
            problems, _ = check_set.report(d)
        self.assertEqual(problems, [
            f"model port 4811 is in use by another process. Fix: change "
            f"`port` under [model] in {d / 'eval.toml'}"])


if __name__ == "__main__":
    unittest.main()
