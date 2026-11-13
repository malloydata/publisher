#!/usr/bin/env python3
"""Tests for import_run.py: an exported run comes back the same run."""
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
import accept  # noqa: E402
import config  # noqa: E402
import export_run as ex  # noqa: E402
import import_run as im  # noqa: E402
import run_index  # noqa: E402
import run_store  # noqa: E402


class RoundTrip(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.set = self.root / "set"
        self.set.mkdir()
        (self.set / "set.json").write_text('{"name": "s"}')
        (self.set / "cases.jsonl").write_text(
            '{"qid": "q1", "question": "how many?", "split": "dev"}\n'
            '{"qid": "q2", "question": "which?", "split": "holdout"}\n')
        (self.set / "eval.toml").write_text(f'[paths]\nworkdir = "{self.root / "w"}"\n'
                                            '[model]\npackage = "p"\n')
        self.cfg = config.load(self.set)
        self.run = self.answer_run("base-01", {"q1": "match", "q2": "no_match"})

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def answer_run(self, name, verdicts):
        d = self.root / "w" / "runs" / name
        (d / "artifacts" / "q1").mkdir(parents=True)
        # indent=2, non-ASCII text and key order that sorting would change:
        # everything a re-serialization would lose.
        (d / "run.json").write_text(json.dumps(
            {"status": "complete", "package": "p", "started": "2026-11-01", "label": name,
             "answererModel": "sonnet", "note": "café"}, indent=2))
        ev = []
        for q, v in verdicts.items():
            ev += [{"kind": "attempt", "qid": q, "sample": None, "question_sha": "s",
                    "answer_text": 'line one\nhe said "x", then é'},
                   {"kind": "score", "qid": q, "sample": None, "verdict": v}]
        (d / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
        (d / "artifacts" / "q1" / "answerer.jsonl").write_text('{"type": "result"}\n')
        (d / "artifacts" / "q1" / "answer.md").write_text('3, "quoted"\nsecond line\n')
        (d / "artifacts" / "q1" / "judge.md").write_text('{"verdict": "match"}')
        (d / "clusters.jsonl").write_text('{"clusterId": "c1", "qids": ["q2"]}\n')
        (d / "model.malloy").write_text("source: x is duckdb.table('t')\n")
        return d

    def exported(self, run=None, out="out"):
        ex.export(run or self.run, self.set, self.root / out)
        return self.root / out

    def away(self, run=None):
        """Move the run out of the workdir, as on a machine that never had it."""
        run = run or self.run
        gone = self.root / "gone" / run.name
        gone.parent.mkdir(exist_ok=True)
        shutil.move(str(run), str(gone))
        return gone

    def test_the_record_comes_back_byte_for_byte(self):
        out, gone = self.exported(), self.away()
        self.assertIn("imported base-01", im.import_run(self.cfg, out))
        for f in ("run.json", "events.jsonl", "clusters.jsonl", "model.malloy",
                  "artifacts/q1/answerer.jsonl", "artifacts/q1/answer.md", "artifacts/q1/judge.md"):
            self.assertEqual((self.run / f).read_bytes(), (gone / f).read_bytes(), f)
        self.assertEqual(run_store.content_sha(self.run), run_store.content_sha(gone))

    def test_it_reads_rows_a_query_returned_as_json_under_the_loaded_tables_names(self):
        out = self.exported()
        q = self.root / "queried"
        q.mkdir()
        for t in ("eval_runs", "eval_events", "eval_files", "eval_transcripts"):
            with (out / f"{t}.csv").open(newline="") as f:
                got = list(csv.DictReader(f))
            (q / f"{t}__base_01.json").write_text(json.dumps(got))
        gone = self.away()
        im.import_run(self.cfg, q)
        self.assertEqual((self.run / "events.jsonl").read_bytes(), (gone / "events.jsonl").read_bytes())
        self.assertEqual((self.run / "run.json").read_bytes(), (gone / "run.json").read_bytes())

    def test_an_imported_run_is_the_record_and_accept_compares_against_it(self):
        out = self.exported()
        self.away()
        im.import_run(self.cfg, out)
        rec = run_index.record(run_index.set_runs(self.cfg))
        self.assertEqual(rec["run"], "base-01")
        good = {"q1": "match", "q2": "match"}
        a, b = self.answer_run("fix-01", good), self.answer_run("fix-02", good)
        r = accept.check(self.cfg, pathlib.Path(rec["path"]), [a, b], None, None)
        self.assertTrue(r["accepted"], r["rules"])
        self.assertEqual(r["targets"], ["q2"])

    def test_a_replays_results_and_summary_come_back(self):
        d = self.root / "w" / "runs" / "perf-01"
        d.mkdir(parents=True)
        (d / "run.json").write_text(json.dumps({"status": "complete", "phase": "query-replay",
                                                "package": "p", "started": "2026-11-02"}, indent=1) + "\n")
        (d / "results.jsonl").write_text('{"qid": "q1", "median_ms": 5.0, "rows_sha": "h"}\n')
        (d / "summary.json").write_text(json.dumps({"queries": 1, "total_ms": 5.0, "p50_ms": 5.0}))
        out, gone = self.exported(d, "perf-out"), self.away(d)
        im.import_run(self.cfg, out)
        for f in ("run.json", "results.jsonl", "summary.json"):
            self.assertEqual((d / f).read_bytes(), (gone / f).read_bytes(), f)
        rec = run_index.record(run_index.set_runs(self.cfg), "perf")
        self.assertEqual((rec["run"], rec["total_ms"]), ("perf-01", 5.0))

    def test_the_same_run_twice_is_left_alone_and_a_different_one_is_refused(self):
        out = self.exported()
        self.assertIn("already here", im.import_run(self.cfg, out))
        (self.run / "events.jsonl").write_text('{"kind": "attempt", "qid": "q1"}\n')
        with self.assertRaises(SystemExit) as e:
            im.import_run(self.cfg, out)
        self.assertIn("different run", str(e.exception))
        self.assertEqual([p.name for p in (self.root / "w" / "runs").iterdir()], ["base-01"])

    def test_missing_tables_are_named(self):
        out = self.exported()
        (out / "eval_events.csv").unlink()
        self.away()
        with self.assertRaises(SystemExit) as e:
            im.import_run(self.cfg, out)
        self.assertIn("no eval_events table", str(e.exception))

    def test_run_json_that_was_reformatted_is_refused_with_the_fix(self):
        out = self.exported()
        (out / "eval_runs.json").write_text(json.dumps([{"run_id": "base-01", "run_json": None}]))
        (out / "eval_runs.csv").unlink()
        self.away()
        with self.assertRaises(SystemExit) as e:
            im.import_run(self.cfg, out)
        self.assertIn("select run_json as text", str(e.exception))

    def test_a_path_that_leaves_the_run_directory_is_refused(self):
        out = self.exported()
        (out / "eval_files.csv").unlink()
        (out / "eval_files.json").write_text(json.dumps(
            [{"run_id": "base-01", "path": "../escaped", "text": "x"}]))
        self.away()
        with self.assertRaises(SystemExit):
            im.import_run(self.cfg, out)
        self.assertFalse((self.root / "w" / "escaped").exists())
        self.assertEqual(list((self.root / "w" / "runs").iterdir()), [])

    def test_rows_of_two_runs_need_a_run_id(self):
        out = self.exported()
        other = self.answer_run("base-02", {"q1": "match", "q2": "match"})
        out2 = self.exported(other, "out2")
        both = self.root / "both"
        both.mkdir()
        for t in ("eval_runs", "eval_events"):
            got = im.rows(out / f"{t}.csv") + im.rows(out2 / f"{t}.csv")
            (both / f"{t}.json").write_text(json.dumps(got))
        self.away()
        with self.assertRaises(SystemExit) as e:
            im.import_run(self.cfg, both)
        self.assertIn("--run-id", str(e.exception))
        self.assertIn("imported base-01", im.import_run(self.cfg, both, "base-01"))


if __name__ == "__main__":
    unittest.main()
