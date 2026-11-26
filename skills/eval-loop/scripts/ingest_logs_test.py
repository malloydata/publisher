"""ingest_logs.py: production rows into the history tables, once each, never the loop's own."""
import csv
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import ingest_logs as il  # noqa: E402


def turn(tid, at="2026-11-24T10:00:00Z", **kw):
    return {"turn_id": tid, "session_id": "s1", "at": at, "environment": "prod",
            "package": "storefront", "model_version": "1.4.0", "question": "top brands?", **kw}


class Ingest(unittest.TestCase):
    def setUp(self):
        self.data = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.data, ignore_errors=True)

    def rows(self, table):
        with (self.data / f"{table}.csv").open(newline="") as f:
            return list(csv.DictReader(f))

    def test_a_rerun_or_an_overlapping_pull_adds_nothing_twice(self):
        il.ingest(self.data, "prod_turns", [turn("t1"), turn("t2")], "storefront", set())
        got = il.ingest(self.data, "prod_turns", [turn("t2"), turn("t3")], "storefront", set())
        self.assertEqual((got["added"], got["left_out"]), (1, {"already stored": 1}))
        self.assertEqual([r["turn_id"] for r in self.rows("prod_turns")], ["t1", "t2", "t3"])

    def test_the_loops_own_traffic_and_other_packages_are_left_out_and_counted(self):
        got = il.ingest(self.data, "prod_turns", [
            turn("t1"), turn("t2", environment="evals"), turn("t3", origin="replay"),
            turn("t4", package="other"), turn(None)], "storefront", {"evals"})
        self.assertEqual(got["added"], 1)
        self.assertEqual(got["left_out"], {"the loop's own environment": 1, "origin replay": 1,
                                           "another package": 1, "no turn_id": 1})

    def test_a_transcript_goes_to_its_own_table_and_host_names_are_mapped(self):
        il.ingest(self.data, "prod_turns",
                  [{**turn(None), "request_id": "r9", "transcript": [{"role": "user"}]}],
                  "storefront", set())
        self.assertEqual(self.rows("prod_turns")[0]["turn_id"], "r9")
        self.assertEqual(json.loads(self.rows("prod_transcripts")[0]["transcript"]), [{"role": "user"}])
        self.assertNotIn("transcript", self.rows("prod_turns")[0])

    def test_the_watermark_is_the_newest_stored_timestamp_and_never_moves_back(self):
        r1 = il.ingest(self.data, "prod_turns", [turn("t1", at="2026-11-24T10:00:00Z")],
                       "storefront", set())
        il.update_watermarks(self.data, [r1])
        r2 = il.ingest(self.data, "prod_turns", [turn("t0", at="2026-11-20T00:00:00Z")],
                       "storefront", set())
        self.assertEqual(il.update_watermarks(self.data, [r2])["prod_turns"], "2026-11-24T10:00:00Z")

    def test_versions_are_keyed_on_package_and_version(self):
        v = {"package": "storefront", "version": "1.4.0", "commit": "abc",
             "published_at": "2026-11-20T00:00:00Z"}
        il.ingest(self.data, "model_versions", [v], "storefront", set())
        got = il.ingest(self.data, "model_versions", [v, {**v, "version": "1.5.0"}], "storefront", set())
        self.assertEqual(got["added"], 1)


class Main(unittest.TestCase):
    def test_the_tables_land_in_the_optimization_package_and_the_summary_says_what_was_dropped(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        (root / "set").mkdir()
        (root / "set" / "set.json").write_text('{"name": "s"}')
        (root / "set" / "eval.toml").write_text(
            f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "storefront"\nenvironment = "e"\n'
            f'[history]\nexclude_environments = ["evals"]\n')
        (root / "t.jsonl").write_text(json.dumps(turn("t1")) + "\n"
                                      + json.dumps(turn("t2", environment="evals")) + "\n")
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            self.assertEqual(il.main(["--set", str(root / "set"), "--turns", str(root / "t.jsonl")]), 0)
        pkg = root / "w" / "optimization-package"
        self.assertTrue((pkg / "history.malloy").is_file())
        # Every table exists before its first row, so the model compiles.
        self.assertEqual((pkg / "data" / "prod_judgments.csv").read_text().splitlines()[0][:16],
                         "turn_id,judged_a")
        self.assertIn("max_query_ms", (pkg / "data" / "price_card.csv").read_text())
        self.assertIn("prod_turns: added 1; left out 1 the loop's own environment", out.getvalue())


if __name__ == "__main__":
    unittest.main()
