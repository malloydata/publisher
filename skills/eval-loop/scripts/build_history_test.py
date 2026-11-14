#!/usr/bin/env python3
"""Tests for build_history.py: the rows it writes from a workdir of runs."""
import csv
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import build_history  # noqa: E402
import config  # noqa: E402


def answer_run(d: pathlib.Path, version: str, verdicts: dict, package: str = "p") -> None:
    d.mkdir(parents=True)
    (d / "run.json").write_text(json.dumps({
        "status": "complete", "started": d.name, "package": package,
        "packageVersion": {"id": None, "baseVersion": version}}))
    ev = []
    for q, v in verdicts.items():
        ev += [{"kind": "attempt", "qid": q}, {"kind": "score", "qid": q, "verdict": v}]
    (d / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))


def read(p: pathlib.Path) -> list[dict]:
    with p.open() as fh:
        return list(csv.DictReader(fh))


class History(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        s = self.root / "set"
        s.mkdir()
        (s / "set.json").write_text('{"name": "s"}')
        (s / "cases.jsonl").write_text(
            '{"qid": "a", "tags": ["join"]}\n{"qid": "b", "tags": ["join", "ratio"]}\n')
        (s / "eval.toml").write_text(f'[model]\npackage = "p"\n[paths]\nworkdir = "{self.root / "w"}"\n')
        self.cfg = config.load(s)
        runs = self.root / "w" / "runs"
        answer_run(runs / "r1", "1.0.0", {"a": "match", "b": "no_match"})
        answer_run(runs / "r2", "1.1.0", {"a": "match", "b": "match"})
        answer_run(runs / "x1", "1.1.0", {"a": "no_match", "b": "match"}, package="p.exp")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_versions_tags_and_experiments(self):
        out = self.root / "pkg"
        build_history.build(self.cfg, out)
        versions = {r["version"]: r for r in read(out / "data" / "versions.csv")}
        self.assertEqual(versions["1.1.0 (unpublished)"]["run"], "r2")   # not the experiment
        self.assertEqual(versions["1.1.0 (unpublished)"]["fixed"], "1")
        tags = [(r["run"], r["tag"], r["passed"], r["decided"])
                for r in read(out / "data" / "tag_scores.csv")]
        self.assertIn(("r1", "join", "1", "2"), tags)
        self.assertIn(("r2", "ratio", "1", "1"), tags)
        lines = {r["run"]: r["experiment"] for r in read(out / "data" / "runs.csv")}
        self.assertEqual(lines["x1"], "exp")
        self.assertIn("##| artifact", (out / "history_report.malloy").read_text())


    def test_each_routine_is_one_row_saying_what_it_did(self):
        rd = self.root / "w" / "routines"
        rd.mkdir(parents=True)
        (rd / "q1.json").write_text(json.dumps({
            "label": "q1", "kind": "quality", "mode": "improve", "started": "2026-11-02T07:00:00",
            "exit": 0, "reason": "9 of 12 passed. 2 of 2 accepted", "run": "q1",
            "number": {"passed": 9, "decided": 12, "costUsd": {"answerer": 1.0, "judge": 0.5,
                                                              "diagnose": None}},
            "improve": {"branch": "eval/q1", "costUsd": 2.0, "edits": [{"outcome": "accepted"},
                                                       {"outcome": "rejected"}]}}))
        (rd / "s1.json").write_text(json.dumps({
            "label": "s1", "kind": "speed", "mode": "run", "started": "2026-11-01T07:00:00",
            "exit": 4, "reason": "1 query errored", "number": {"totalMs": 4180}}))
        (rd / "broken.json").write_text("{")
        out = self.root / "pkg"
        build_history.build(self.cfg, out)
        rows = read(out / "data" / "routines.csv")
        self.assertEqual([r["label"] for r in rows], ["s1", "q1"])          # oldest first
        q = rows[1]
        self.assertEqual((q["passed"], q["decided"], q["cost_usd"], q["edits_tried"],
                          q["edits_accepted"], q["branch"], q["keys_verified"]),
                         ("9", "12", "3.5", "2", "1", "eval/q1", "true"))
        s = rows[0]
        self.assertEqual((s["exit"], s["total_ms"], s["edits_tried"], s["keys_verified"]),
                         ("4", "4180", "", ""))
        self.assertIn('"routines -> latest"', (out / "history_report.malloy").read_text())


if __name__ == "__main__":
    unittest.main()
