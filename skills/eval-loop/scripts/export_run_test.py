#!/usr/bin/env python3
"""Tests for export_run.py: the seven tables, their fixed columns, and the refusals."""
import csv
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import export_run as ex  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent / "eval-answer" / "scripts"))
import ledger  # noqa: E402


def read_csv(p):
    with p.open(newline="") as f:
        return list(csv.DictReader(f))


class Export(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.set = self.root / "set"
        self.set.mkdir()
        cases = [{"qid": "q1", "question": "How many?", "tags": ["join"], "split": "dev",
                  "goldenRevision": 2, "golden": {"kind": "scalar", "value": {"n": 3}}},
                 {"qid": "q2", "question": "Which?", "tags": [], "split": "dev"}]
        (self.set / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
        (self.set / "set.json").write_text('{"name": "s", "datasetVersion": 4}')
        self.run = self.root / "work" / "runs" / "Base-01"
        (self.run / "artifacts" / "q1").mkdir(parents=True)
        (self.run / "artifacts" / "q1" / "answerer.jsonl").write_text('{"type": "x"}\n')
        (self.run / "artifacts" / "q1" / "answer.md").write_text("3, \"quoted\"\nsecond line")
        (self.run / "run.json").write_text(json.dumps({
            "runId": "Base-01", "setName": "s", "phase": "baseline", "status": "complete",
            "datasetVersion": 4, "datasetSha": ledger.dataset_sha(self.set),
            "packageVersion": {"id": "1.2.0", "contentHash": "h"},
            "slice": {"name": "joins", "tags": ["join"], "qids": ["q1"]}}))
        ev = [{"kind": "attempt", "qid": "q1", "sample": None, "question_sha": "s1",
               "submitted": True, "final_query": "run: x", "cost_usd": 0.1},
              {"kind": "score", "qid": "q1", "sample": None, "verdict": "match",
               "golden_revision": 2, "confidence": 9}]
        (self.run / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_seven_tables_with_fixed_columns_and_a_manifest(self):
        out = self.root / "out"
        m = ex.export(self.run, self.set, out)
        self.assertEqual(sorted(m["tables"]), sorted(ex.TABLES))
        for t in ex.TABLES:
            rows = read_csv(out / f"{t}.csv")
            with (out / f"{t}.csv").open() as f:
                header = next(csv.reader(f))
            self.assertEqual(tuple(header), ex.COLUMNS[t], t)
            self.assertTrue(all(r["run_id"] == "Base-01" for r in rows), t)
            self.assertEqual(m["tables"][t]["rows"], len(rows), t)
        run = read_csv(out / "eval_runs.csv")[0]
        self.assertEqual((run["version_id"], run["slice_name"], run["slice_qids"], run["kind"]),
                         ("1.2.0", "joins", '["q1"]', "slice"))
        att = read_csv(out / "eval_attempts.csv")
        self.assertEqual([(a["qid"], a["verdict"], a["passed"], a["golden_revision"], a["tags"])
                          for a in att], [("q1", "match", "True", "2", '["join"]')])
        tr = read_csv(out / "eval_transcripts.csv")
        self.assertEqual(tr[0]["answer"], '3, "quoted"\nsecond line')
        cases = read_csv(out / "eval_cases.csv")
        self.assertEqual(json.loads(cases[0]["golden"])["value"], {"n": 3})
        self.assertEqual(m["tables"]["eval_attempts"]["name"], "eval_attempts__base_01")

    def test_the_summary_holds_the_numbers_and_nothing_that_names_a_case_or_an_answer(self):
        out = self.root / "sum"
        m = ex.export_summary(self.run, self.set, out)
        self.assertEqual(list(m["tables"]), ["eval_summary"])
        self.assertEqual(sorted(p.name for p in out.iterdir()), ["eval_summary.csv", "manifest.json"])
        row = read_csv(out / "eval_summary.csv")[0]
        self.assertEqual((row["run_id"], row["version_id"], row["passed"], row["decided"]),
                         ("Base-01", "1.2.0", "1", "1"))
        self.assertEqual((row["dev_passed"], row["dev_decided"]), ("1", "1"))
        self.assertNotIn("run_json", row)
        self.assertNotIn("slice_qids", row)
        text = (out / "eval_summary.csv").read_text() + (out / "manifest.json").read_text()
        for leak in ("q1", "run: x", "quoted", '"n": 3', "how many"):
            self.assertNotIn(leak, text, leak)

    def test_the_summary_of_every_run_is_one_row_per_run_oldest_first(self):
        second = self.run.parent / "Base-02"
        shutil.copytree(self.run, second)
        meta = json.loads((second / "run.json").read_text())
        meta.update(runId="Base-02", started="2026-11-02T00:00:00Z")
        (second / "run.json").write_text(json.dumps(meta))
        first = json.loads((self.run / "run.json").read_text())
        first["started"] = "2026-11-01T00:00:00Z"
        (self.run / "run.json").write_text(json.dumps(first))
        (self.run.parent / "notes").mkdir()  # not a run: no run.json
        out = self.root / "all"
        m = ex.export_summary_all(self.run.parent, self.set, out)
        self.assertEqual(m["runs"], ["Base-01", "Base-02"])
        self.assertEqual(m["tables"]["eval_summary"]["name"], "eval_summary")
        rows = read_csv(out / "eval_summary.csv")
        self.assertEqual([(r["run_id"], r["passed"]) for r in rows], [("Base-01", "1"), ("Base-02", "1")])
        text = (out / "eval_summary.csv").read_text()
        for leak in ("q1", "run: x", "quoted", "How many"):
            self.assertNotIn(leak, text, leak)

    def test_the_summary_of_every_run_leaves_out_experiment_runs(self):
        cand = self.run.parent / "Base-01-c1-1"
        shutil.copytree(self.run, cand)
        meta = json.loads((cand / "run.json").read_text())
        meta.update(runId="Base-01-c1-1", package="p.c1")
        (cand / "run.json").write_text(json.dumps(meta))
        first = json.loads((self.run / "run.json").read_text())
        (self.run / "run.json").write_text(json.dumps({**first, "package": "p"}))
        m = ex.export_summary_all(self.run.parent, self.set, self.root / "all", "p")
        self.assertEqual((m["runs"], m["experimentsLeftOut"]), (["Base-01"], ["Base-01-c1-1"]))

    def test_all_without_summary_is_refused(self):
        with self.assertRaises(SystemExit) as e:
            ex.main(["--all", "--set", str(self.set)])
        self.assertIn("--all exports summaries only", str(e.exception))

    def test_a_set_that_moved_since_the_run_is_refused(self):
        (self.set / "cases.jsonl").write_text('{"qid": "q1", "question": "edited"}\n')
        with self.assertRaises(SystemExit) as e:
            ex.export(self.run, self.set, self.root / "out")
        self.assertIn("is not the one this run scored", str(e.exception))

    def test_a_replay_exports_its_scores_as_attempts(self):
        (self.run / "events.jsonl").write_text("")
        (self.run / "results.jsonl").write_text(json.dumps(
            {"qid": "q1", "recall": 1.0, "recall_exact": 0.5, "precision": 0.25, "error": None}) + "\n")
        ex.export(self.run, self.set, self.root / "out")
        att = read_csv(self.root / "out" / "eval_attempts.csv")
        self.assertEqual((att[0]["phase"], att[0]["recall_exact"], att[0]["verdict"]),
                         ("replay", "0.5", ""))

    def test_table_names_load(self):
        self.assertEqual(ex.table_name("eval_runs", "storefront-tour-baseline-01"),
                         "eval_runs__storefront_tour_baseline_01")
        self.assertLessEqual(len(ex.table_name("eval_transcripts", "x" * 90)), 63)

    def test_an_unknown_column_is_refused(self):
        with self.assertRaises(ValueError):
            ex.write_csv(self.root / "x.csv", [{"run_id": "r", "surprise": 1}], ("run_id",))


if __name__ == "__main__":
    unittest.main()
