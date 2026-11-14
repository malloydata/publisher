#!/usr/bin/env python3
"""Tests for run_index.py: run kinds, the per-version view, and flips across versions."""
import contextlib
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import run_index as ri  # noqa: E402


def write_run(workdir: pathlib.Path, name: str, scores: dict, version=None, started="2026-11-01",
              slice_=None, golden=None, status="complete", **meta) -> pathlib.Path:
    d = workdir / "runs" / name
    d.mkdir(parents=True)
    run = {"runId": name, "phase": "baseline", "started": started, "status": status,
           "datasetVersion": 3, "packageVersion": {"id": version}, **meta}
    if slice_:
        run["slice"] = slice_
    (d / "run.json").write_text(json.dumps(run))
    ev = [{"kind": "attempt", "qid": q, "question_sha": "s-" + q} for q in scores]
    ev += [{"kind": "score", "qid": q, "verdict": v, "golden_revision": (golden or {}).get(q, 1)}
           for q, v in scores.items()]
    (d / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
    return d


class Index(unittest.TestCase):
    def setUp(self):
        self.w = pathlib.Path(tempfile.mkdtemp())
        write_run(self.w, "base-1", {"a": "match", "b": "no_match", "c": "no_match"},
                  version="1.0.0", started="2026-11-01")
        write_run(self.w, "slice-1", {"b": "match"}, version=None, started="2026-11-02",
                  slice_={"name": "joins", "tags": ["join"], "qids": ["b"]})
        write_run(self.w, "base-2", {"a": "match", "b": "match", "c": "no_match"},
                  version="1.1.0", started="2026-11-03", golden={"c": 2})
        rep = self.w / "runs" / "rep-1"
        rep.mkdir(parents=True)
        (rep / "run.json").write_text(json.dumps({"phase": "replay", "mode": "retrieval",
                                                  "started": "2026-11-04", "status": "complete",
                                                  "packageVersion": {"id": None}}))
        (rep / "summary.json").write_text(json.dumps({"recall_exact": 0.75, "cases": 4}))

    def tearDown(self):
        shutil.rmtree(self.w, ignore_errors=True)

    def test_each_run_has_a_kind_and_a_score(self):
        rows = {r["run"]: r for r in ri.runs(self.w, n_cases=3)}
        self.assertEqual({k: (r["kind"], r.get("full")) for k, r in rows.items()},
                         {"base-1": ("run", True), "slice-1": ("slice", False),
                          "base-2": ("run", True), "rep-1": ("replay", None)})
        self.assertEqual((rows["base-1"]["passed"], rows["base-1"]["decided"]), (1, 3))
        self.assertEqual(rows["slice-1"]["slice"], "joins")
        self.assertEqual(rows["rep-1"]["recall_exact"], 0.75)
        out = "\n".join(ri.fmt_runs(list(rows.values())))
        self.assertIn("recall 0.75", out)
        self.assertIn("slice joins", out)

    def test_versions_highest_first_with_flips_over_comparable_cases(self):
        served = [{"id": "1.1.0", "latest": True, "archiveStatus": "unarchive"},
                  {"id": "1.0.0", "latest": False, "archiveStatus": "archive"},
                  {"id": "0.9.0", "latest": False, "archiveStatus": "unarchive"}]
        rows = ri.versions(ri.runs(self.w, n_cases=3), served)
        self.assertEqual([r["version"] for r in rows], ["1.1.0", "1.0.0", "0.9.0"])
        top = rows[0]
        self.assertEqual((top["latest"], top["passed"], top["decided"], top["vs"]),
                         (True, 2, 3, "1.0.0"))
        # c's key moved from revision 1 to 2, so it is set aside, not compared.
        self.assertEqual(top["flips"], {"compared": 2, "fixed": 1, "broke": 0, "set_aside": 1})
        self.assertTrue(rows[1]["archived"])
        self.assertIsNone(rows[2]["run"])
        text = "\n".join(ri.fmt_versions(rows))
        self.assertIn("vs 1.0.0: 1 fixed, 0 broke over 2, 1 set aside", text)
        self.assertIn("(no answer run)", text)

    def test_a_working_copy_run_is_its_own_row_last(self):
        write_run(self.w, "wc-1", {"a": "match"}, version=None, started="2026-11-05")
        rows = ri.versions(ri.runs(self.w, n_cases=3))
        self.assertEqual(rows[-1]["version"], ri.WORKING_COPY)
        self.assertNotIn("flips", rows[-1])

    def test_an_incomplete_run_does_not_speak_for_its_version(self):
        write_run(self.w, "base-3", {"a": "no_match", "b": "no_match", "c": "no_match"},
                  version="1.2.0", status="incomplete", started="2026-11-06")
        rows = ri.versions(ri.runs(self.w, n_cases=3))
        self.assertNotIn("1.2.0", [r["version"] for r in rows])

    def test_semver_order(self):
        vs = ["1.10.0", "1.2.0", "1.2.0-rc1", "0.9.9", "junk"]
        self.assertEqual(sorted(vs, key=ri.version_key, reverse=True),
                         ["1.10.0", "1.2.0", "1.2.0-rc1", "0.9.9", "junk"])


class Cli(unittest.TestCase):
    def test_versions_reads_the_set_workdir_and_the_server(self):
        root = pathlib.Path(tempfile.mkdtemp())
        try:
            s = root / "set"
            s.mkdir()
            (s / "eval.toml").write_text(f'[model]\nenvironment = "e"\npackage = "p"\n'
                                         f'[paths]\nworkdir = "{root / "w"}"\n')
            (s / "set.json").write_text('{"name": "s"}')
            (s / "cases.jsonl").write_text('{"qid": "a"}\n')
            write_run(root / "w", "r1", {"a": "match"}, version="2.0.0")
            out = io.StringIO()
            with unittest.mock.patch.object(ri, "package_versions",
                                            return_value=[{"id": "2.0.0", "latest": True}]) as pv, \
                    contextlib.redirect_stdout(out):
                self.assertEqual(ri.main(["versions", "--set", str(s), "--publisher", "http://h"]), 0)
            pv.assert_called_once_with("http://h", "e", "p")
            self.assertIn("2.0.0                latest    r1", out.getvalue())
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
