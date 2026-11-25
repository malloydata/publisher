"""simulate.py: options from a replay, a dry-run candidate, and the check against a replay."""
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
import unittest.mock
import urllib.error

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import simulate as sim  # noqa: E402

OBJ = {"objective": "weighted_ms", "max_staleness_hours": 6.0, "max_build_ms_per_day": None,
       "max_usd_per_day": None, "usd_per_compute_hour": 3.0, "usd_per_tb_scanned": None,
       "table_read_ms": 10.0}


def row(qid, grouped, measures, ms, count=1, filtered=(), kind="live", source="order_items"):
    return {"qid": qid, "source": source, "median_ms": ms, "count": count,
            "served_by": {"kind": kind},
            "shape": {"sourceName": source, "groupedBy": list(grouped),
                      "filteredBy": list(filtered), "measures": list(measures)}}


# Shapes as Publisher 0.18.1's path report gave them for the perfshop bench.
ROWS = [row("by_category", ["category"], ["total_sales"], 35, 120),
        row("by_cat_region", ["category", "region"], ["total_sales"], 55, 30),
        row("returned", ["category"], ["order_item_count"], 15, 15, filtered=["status"]),
        row("by_brand", ["brand"], ["total_sales"], 200, 8)]


class Coverage(unittest.TestCase):
    def table(self, grain, measures):
        return {"kind": "rollup", "source": "order_items", "grain": grain, "measures": measures}

    def test_a_grain_answers_a_query_whose_fields_and_measures_it_holds(self):
        t = self.table(["category", "region"], ["total_sales"])
        self.assertEqual(sim.covers(t, ROWS[0]), (True, None))
        self.assertEqual(sim.covers(t, ROWS[1]), (True, None))

    def test_a_filter_field_must_be_in_the_grain_too(self):
        ok, why = sim.covers(self.table(["category"], ["order_item_count"]), ROWS[2])
        self.assertFalse(ok)
        self.assertEqual(why, "GRAIN_TOO_COARSE: the grain lacks status")

    def test_a_measure_the_table_does_not_store_is_named(self):
        ok, why = sim.covers(self.table(["category"], ["total_margin"]), ROWS[0])
        self.assertEqual((ok, why), (False, "MISSING_FIELD: the table does not store total_sales"))

    def test_a_query_with_no_shape_is_never_counted_as_answered(self):
        self.assertFalse(sim.covers(self.table(["category"], ["total_sales"]),
                                    {**ROWS[0], "shape": None})[0])


class Options(unittest.TestCase):
    def test_proposed_grains_are_each_live_querys_and_their_union(self):
        grains = [t["grain"] for t in sim.proposed_tables(ROWS)]
        self.assertIn(["category"], grains)
        self.assertIn(["category", "status"], grains)
        self.assertIn(["brand", "category", "region", "status"], grains)
        # A query a table already answers proposes nothing.
        done = [row("q", ["x"], ["m"], 5, kind="rollup")]
        self.assertEqual(sim.proposed_tables(done), [])

    def test_an_option_predicts_time_saved_build_and_staleness(self):
        t = {"kind": "rollup", "source": "order_items", "grain": ["category", "region"],
             "measures": ["total_sales"], "tier": "estimate"}
        opt, preds = sim.score(ROWS, [t], OBJ, "x")
        # by_category and by_cat_region drop to table_read_ms.
        self.assertEqual(opt["covered"], 2)
        self.assertEqual(opt["saved_ms_per_day"], 120 * 25 + 30 * 45)
        # Built about as slowly as the slowest live query on the source, four
        # times a day to keep data within six hours.
        self.assertEqual((opt["build_ms"], opt["refreshes_per_day"], opt["staleness_hours"]),
                         (200, 4, 6.0))
        self.assertEqual(opt["net_ms_per_day"], 4350 - 800)
        self.assertLess(opt["usd_per_day"], 0)
        self.assertEqual({p["qid"]: p["predicted_ms"] for p in preds}["by_category"], 10.0)

    def test_a_limit_makes_an_option_infeasible_and_says_why(self):
        t = {"kind": "rollup", "source": "order_items", "grain": ["brand"],
             "measures": ["total_sales"], "tier": "estimate"}
        opt, _ = sim.score(ROWS, [t], {**OBJ, "max_build_ms_per_day": 100.0}, "x")
        self.assertFalse(opt["feasible"])
        self.assertIn("over max_build_ms_per_day", opt["why_not"])

    def test_options_rank_feasible_first_then_by_the_objective(self):
        opts, _ = sim.options(ROWS, sim.proposed_tables(ROWS), OBJ, candidate=False)
        self.assertEqual(opts[-1]["option"], "baseline")
        after = [o["objective_after"] for o in opts if o["feasible"]]
        self.assertEqual(after, sorted(after))

    def test_p95_counts_each_query_as_often_as_it_ran(self):
        self.assertEqual(sim.p95([(10, 99), (1000, 1)]), 10)
        self.assertEqual(sim.p95([(10, 90), (1000, 10)]), 1000)


class DryRun(unittest.TestCase):
    def reply(self, body):
        resp = unittest.mock.MagicMock()
        resp.__enter__.return_value.read.return_value = json.dumps(body).encode()
        return resp

    def test_a_candidates_tables_come_from_its_build_plan_and_verdicts(self):
        body = {"buildPlan": {"sources": {"r@f": {
                    "name": "r", "preaggregate": {"baseSourceName": "order_items",
                                                  "grainDimensions": ["category", "region"],
                                                  "measures": ["total_sales"]}}},
                    "refusedSources": {"order_items.total_margin[category,region]@f": {
                        "reason": "join_not_in_grain",
                        "message": "it aggregates through join `products`"}}},
                "serveVerdicts": [{"sourceName": "r", "verdict": "awaiting_build", "message": "m"}],
                "warnings": [{"severity": "error", "message": "bad drill"}]}
        with unittest.mock.patch.object(sim.urllib.request, "urlopen", return_value=self.reply(body)):
            tables, errors = sim.dry_run_tables("http://h", "e", "p-sim", pathlib.Path("/c"))
        self.assertEqual((tables[0]["grain"], tables[0]["verdict"]),
                         (["category", "region"], "awaiting_build"))
        self.assertEqual(tables[1]["verdict"], "refused")
        self.assertIn("(join_not_in_grain): it aggregates through join", tables[1]["verdict_message"])
        self.assertEqual(errors, ["bad drill"])

    def test_a_package_that_will_not_load_is_one_refused_option_with_the_servers_words(self):
        err = urllib.error.HTTPError("u", 400, "Bad", {},
                                     io.BytesIO(b'{"message": "[rollup-not-mergeable]"}'))
        with unittest.mock.patch.object(sim.urllib.request, "urlopen", side_effect=err):
            tables, _ = sim.dry_run_tables("http://h", "e", "p-sim", pathlib.Path("/c"))
        opt, _ = sim.score(ROWS, tables, OBJ, "candidate")
        self.assertFalse(opt["feasible"])
        self.assertIn("rollup-not-mergeable", opt["why_not"])
        self.assertEqual(opt["covered"], 0)


class Check(unittest.TestCase):
    def test_predictions_are_compared_with_what_the_replay_measured(self):
        t = {"kind": "rollup", "source": "order_items", "grain": ["category"],
             "measures": ["total_sales"], "tier": "dry-run"}
        opts, preds = sim.options(ROWS, [t], OBJ, candidate=True)
        measured = [{**ROWS[0], "median_ms": 4, "served_by": {"kind": "rollup"}},
                    {**ROWS[1], "served_by": {"kind": "rollup"}},   # predicted live
                    ROWS[2], ROWS[3]]
        got = sim.check({"options": opts, "predictions": preds, "objective": OBJ}, measured)
        self.assertEqual([c["qid"] for c in got["coverage_wrong"]], ["by_cat_region"])
        self.assertEqual(got["assumed_table_read_ms"], 10.0)
        self.assertIn(got["measured_table_read_ms"], (4, 55))


class Main(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.set = self.root / "set"
        self.set.mkdir()
        (self.set / "set.json").write_text('{"name": "s"}')
        (self.set / "eval.toml").write_text(
            f'[paths]\nworkdir = "{self.root / "w"}"\n[model]\npackage = "p"\nenvironment = "e"\n'
            f'[performance]\nmax_staleness_hours = 6\n')
        run = self.root / "w" / "runs" / "perf-01"
        run.mkdir(parents=True)
        (run / "results.jsonl").write_text("".join(json.dumps(r) + "\n" for r in ROWS))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_a_simulation_writes_its_options_predictions_and_assumptions(self):
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            self.assertEqual(sim.main(["--set", str(self.set), "--from", "perf-01"]), 0)
        d = self.root / "w" / "simulations" / "sim-perf-01"
        got = json.loads((d / "options.json").read_text())
        self.assertEqual(got["tier"], "estimate")
        self.assertTrue(any("table_read_ms" in a for a in got["assumptions"]))
        self.assertTrue((d / "options.csv").read_text().startswith("option,kind,source,grain"))
        self.assertIn("objective: lower weighted_ms, data at most 6 h old", out.getvalue())

    def test_the_options_app_is_a_package_over_the_simulation_with_yes_no_flags(self):
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO):
            sim.main(["--set", str(self.set), "--from", "perf-01", "--package"])
        pkg = self.root / "w" / "simulations" / "sim-perf-01" / "package"
        self.assertTrue((pkg / "speed_options.malloy").is_file())
        self.assertTrue((pkg / "public" / "app.js").is_file())
        options = (pkg / "options.csv").read_text()
        self.assertIn(",yes,", options)
        self.assertNotIn("True", options)
        self.assertTrue((pkg / "assumptions.csv").read_text().startswith("kind,text\nobjective,"))

    def test_a_bad_objective_names_the_fix(self):
        (self.set / "eval.toml").write_text((self.set / "eval.toml").read_text()
                                            + 'objective = "fastest"\n')
        with self.assertRaises(SystemExit) as e:
            sim.main(["--set", str(self.set), "--from", "perf-01"])
        self.assertIn('Fix: objective = "weighted_ms"', str(e.exception))


if __name__ == "__main__":
    unittest.main()
