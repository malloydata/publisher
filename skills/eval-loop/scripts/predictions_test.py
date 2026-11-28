"""predictions.py: what a change should do in production, and what it did."""
import datetime
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import optimization_package as op  # noqa: E402
import predictions as pr  # noqa: E402

AT = datetime.datetime(2026, 11, 25, tzinfo=datetime.timezone.utc)


class Predict(unittest.TestCase):
    def test_a_quality_run_predicts_its_pass_rate_with_an_interval_and_its_metric_version(self):
        run = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, run, True)
        (run / "run.json").write_text(json.dumps({"judgeVersion": "j3", "rubricSha": "abcdef0123456789"}))
        ev = [{"kind": "score", "qid": f"q{i}", "verdict": "match" if i < 8 else "no_match"} for i in range(10)]
        ev.append({"kind": "score", "qid": "q10", "verdict": "unscorable"})
        (run / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
        got = pr.from_quality_run(run)
        self.assertEqual((got["predicted"], got["metric_version"]), (0.8, "j3:abcdef012345"))
        self.assertLess(got["low"], 0.8)
        self.assertGreater(got["high"], 0.8)
        self.assertIn("10 decided cases", got["method"])

    def test_a_simulation_predicts_the_weighted_mean_and_the_share_over_the_limit(self):
        sim = {"label": "sim-1", "tier": "dry-run",
               "options": [{"option": "baseline", "feasible": True},
                           {"option": "slow", "feasible": False}, {"option": "r1", "feasible": True}],
               "predictions": [{"option": "r1", "qid": "a", "count": 3, "predicted_ms": 10},
                               {"option": "r1", "qid": "b", "count": 1, "predicted_ms": 2000},
                               {"option": "slow", "qid": "a", "count": 3, "predicted_ms": 99}]}
        got = {g["metric"]: g for g in pr.from_simulation(sim, None, 0.2, 1000)}
        self.assertEqual(got["mean_query_ms"]["predicted"], 507.5)
        self.assertEqual(got["over_limit_share"]["predicted"], 0.25)
        self.assertEqual((got["mean_query_ms"]["low"], got["mean_query_ms"]["high"]), (406.0, 609.0))
        self.assertIn("option r1", got["mean_query_ms"]["method"])

    def test_the_same_prediction_has_the_same_id(self):
        a = pr.row("quality", "fix-01", "abc", None, "quality_rate", "j", None, 0.8, 0.7, 0.9, "m", 20, 7, AT)
        b = pr.row("quality", "fix-01", "abc", None, "quality_rate", "j", None, 0.8, 0.7, 0.9, "m", 20, 7, AT)
        self.assertEqual(a["prediction_id"], b["prediction_id"])
        self.assertEqual(a["check_after"][:10], "2026-12-02")


class Outcomes(unittest.TestCase):
    def setUp(self):
        self.data = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.data, True)
        op.append(self.data / "prod_turns.csv", op.APPENDED["prod_turns"][2],
                  [{"turn_id": f"t{i}", "model_version": "1.5.0"} for i in range(4)])
        op.append(self.data / "prod_judgments.csv", op.APPENDED["prod_judgments"][2],
                  [{"turn_id": "t0", "metric_version": "j", "right": True},
                   {"turn_id": "t1", "metric_version": "j", "right": True},
                   {"turn_id": "t2", "metric_version": "j", "right": False},
                   {"turn_id": "t3", "metric_version": "other", "right": False}])
        self.versions = [{"version": "1.5.0", "commit": "abc1234def", "published_at": "x"}]

    def pred(self, **kw):
        p = pr.row("quality", "fix-01", "abc1234", None, "quality_rate", "j", None, 0.8, 0.6, 0.9,
                   "m", 3, 7, AT - datetime.timedelta(days=8))
        return {k: "" if v is None else str(v) for k, v in {**p, **kw}.items()}

    def test_a_due_prediction_is_checked_against_its_versions_verdicts_of_its_own_metric(self):
        o, _ = pr.outcome(self.pred(), self.versions, self.data, None, AT)
        self.assertEqual((o["version"], o["measured"], o["n"]), ("1.5.0", 0.6667, 3))
        self.assertTrue(o["inside_range"])
        self.assertEqual(o["gap"], round(0.6667 - 0.8, 4))

    def test_not_due_or_too_little_production_waits_and_says_why(self):
        o, why = pr.outcome(self.pred(check_after=(AT + datetime.timedelta(days=1)).isoformat()),
                            self.versions, self.data, None, AT)
        self.assertIsNone(o)
        self.assertIn("due", why)
        o, why = pr.outcome(self.pred(min_n="50"), self.versions, self.data, None, AT)
        self.assertEqual((o, why), (None, "version 1.5.0 has 3 of the 50 needed"))

    def test_a_change_edited_before_merging_is_recorded_with_no_number(self):
        o, _ = pr.outcome(self.pred(candidate_commit="fff"), self.versions, self.data, None, AT)
        self.assertIsNone(o["measured"])
        self.assertIn("edited before merging", o["note"])


class Main(unittest.TestCase):
    def test_predict_then_outcomes_end_to_end(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        (root / "set").mkdir()
        (root / "set" / "set.json").write_text('{"name": "s"}')
        (root / "set" / "eval.toml").write_text(
            f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "p"\nenvironment = "e"\n')
        run = root / "w" / "runs" / "fix-01"
        run.mkdir(parents=True)
        (run / "run.json").write_text('{"judgeVersion": "j"}')
        (run / "events.jsonl").write_text('{"kind": "score", "qid": "a", "verdict": "match"}\n')
        argv = ["predict", "--set", str(root / "set"), "--quality-run", "fix-01", "--commit", "abc"]
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            pr.main(argv)
            pr.main(argv)
            pr.main(["outcomes", "--set", str(root / "set")])
        data = root / "w" / "packages" / "optimization-s" / "data"
        self.assertEqual(len(op.read(data / "predictions.csv")), 1)
        self.assertIn("[already recorded]", out.getvalue())
        self.assertIn("not yet: quality_rate from fix-01: due", out.getvalue())


if __name__ == "__main__":
    unittest.main()
