#!/usr/bin/env python3
"""Tests for build_speed_package.py: the CSVs a query-replay package holds, the
verdicts it copies from query_replay.compare, and how package dispatches."""
import csv
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import build_run_package as brp  # noqa: E402
import build_speed_package as bsp  # noqa: E402


def replay(d: pathlib.Path, rows: list[dict], base_version: str = "1.0.0") -> pathlib.Path:
    d.mkdir(parents=True)
    (d / "run.json").write_text(json.dumps({
        "phase": "query-replay", "status": "complete", "started": d.name,
        "latencyBand": 0.2, "repeats": 5, "fromRun": "base",
        "packageVersion": {"id": None, "baseVersion": base_version},
        "materialization": {"persistSources": ["trips"], "bound": True}}))
    (d / "results.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    (d / "summary.json").write_text(json.dumps({"p50_ms": 1, "total_ms": 1, "queries": len(rows)}))
    return d


def row(qid, ms, sha="s1"):
    return {"qid": qid, "query": f"run: trips -> {qid}", "givens": None, "source": "trips",
            "median_ms": ms, "min_ms": ms, "max_ms": ms, "rows_sha": sha, "error": None,
            "served_from": ["none"], "slow": False}


def read(path: pathlib.Path) -> list[dict]:
    with path.open() as fh:
        return list(csv.DictReader(fh))


class Build(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.runs = self.root / "runs"

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_the_first_replay_is_the_baseline_and_verdicts_are_compares(self):
        a = replay(self.runs / "a", [row("fast", 100), row("same", 50), row("changed", 40)])
        b = replay(self.runs / "b", [row("fast", 20), row("same", 52), row("changed", 10, "s2")],
                   base_version="1.1.0")
        out = self.root / "pkg"
        counts = bsp.build([a, b], out)
        self.assertEqual(counts, {"replays": 2, "timings": 6, "comparisons": 3})
        verdicts = {r["qid"]: r["verdict"] for r in read(out / "data" / "comparisons.csv")}
        self.assertEqual(verdicts, {"fast": "faster", "same": "within_band",
                                    "changed": "rows_changed"})
        runs = {r["run_id"]: r["version"] for r in read(out / "data" / "runs.csv")}
        self.assertEqual(runs, {"a": "1.0.0 (unpublished)", "b": "1.1.0 (unpublished)"})
        # `source` is a Malloy keyword, so no column may be named it.
        self.assertNotIn("source", read(out / "data" / "timings.csv")[0])
        for f in ("speed_run.malloy", "speed_report.malloy", "public/index.html", "public/app.js"):
            self.assertTrue((out / f).exists(), f)
        self.assertIn("##| artifact", (out / "speed_report.malloy").read_text())

    def test_one_replay_writes_a_header_only_comparison_file(self):
        a = replay(self.runs / "a", [row("q", 10)])
        bsp.build([a], self.root / "pkg")
        text = (self.root / "pkg" / "data" / "comparisons.csv").read_text()
        self.assertEqual(text.strip().splitlines(), [",".join(bsp.COMPARISON_COLUMNS)])

    def test_package_refuses_replays_mixed_with_answer_runs(self):
        a = replay(self.runs / "a", [row("q", 10)])
        ans = self.runs / "ans"
        ans.mkdir(parents=True)
        (ans / "run.json").write_text(json.dumps({"phase": "baseline"}))
        sset = self.root / "set"
        sset.mkdir()
        (sset / "set.json").write_text('{"name": "s"}')
        (sset / "eval.toml").write_text(f'[paths]\nworkdir = "{self.root / "w"}"\n')
        with self.assertRaises(SystemExit) as e:
            brp.main(["--run", str(a), "--run", str(ans), "--set", str(sset), "--no-register"])
        self.assertIn("mix query replays and answer runs", str(e.exception))


if __name__ == "__main__":
    unittest.main()
