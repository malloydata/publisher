#!/usr/bin/env python3
"""Tests for query_replay.py: which queries replay, the rows hash, timing, the
comparison rule, and a whole replay against a stub server."""
import argparse
import contextlib
import http.server
import io
import json
import pathlib
import shutil
import sys
import tempfile
import threading
import unittest
import unittest.mock

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
import query_replay as qr  # noqa: E402
import publisher_rest  # noqa: E402
import run_index as ri  # noqa: E402
import export_run as ex  # noqa: E402


def answer_run(d: pathlib.Path, outcomes: dict, model_paths: dict | None = None) -> pathlib.Path:
    d.mkdir(parents=True)
    (d / "run.json").write_text(json.dumps({"runId": d.name, "phase": "baseline",
                                            "modelPath": "m.malloy", "status": "complete"}))
    ev = []
    for q, (verdict, query) in outcomes.items():
        ev.append({"kind": "attempt", "qid": q, "final_query": query, "final_givens": None,
                   "final_model_path": (model_paths or {}).get(q)})
        ev.append({"kind": "score", "qid": q, "verdict": verdict})
    (d / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
    return d


def result(qid, median, sha="s1", **kw):
    return {"qid": qid, "query": "run: t -> v", "givens": None, "median_ms": median,
            "rows_sha": sha, "error": None, **kw}


class Queries(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_only_passing_cases_with_a_query_replay_at_their_own_model(self):
        run = answer_run(self.root / "a", {
            "p1": ("match", "run: orders -> by_month"),
            "p2": ("match", None),
            "f1": ("no_match", "run: orders -> x"),
            "n1": ("near_match", "run: orders -> y"),
            "p3": ("match", "run: items -> z")}, {"p3": "other.malloy"})
        got = {q["qid"]: (q["modelPath"], q["source"]) for q in qr.queries_from(run, "m.malloy")}
        self.assertEqual(got, {"p1": ("m.malloy", "orders"), "p3": ("other.malloy", "items")})

    def test_a_replay_of_a_replay_runs_the_same_queries(self):
        d = self.root / "r"
        d.mkdir()
        (d / "run.json").write_text(json.dumps({"phase": "query-replay"}))
        (d / "results.jsonl").write_text(json.dumps(
            {"qid": "q", "query": "run: a -> b", "givens": {"g": 1}, "modelPath": "m.malloy",
             "source": "a", "median_ms": 3}) + "\n")
        self.assertEqual(qr.queries_from(d, None), [
            {"qid": "q", "tags": None, "query": "run: a -> b", "givens": {"g": 1},
             "modelPath": "m.malloy", "count": 1, "source": "a"}])

    def test_source_of(self):
        self.assertEqual(qr.source_of("  run: order_items -> {\n group_by: x }"), "order_items")
        self.assertEqual(qr.source_of("source: oi is order_items extend {\n dimension: d is 1\n}\n"
                                      "run: oi -> { aggregate: c is count() }"), "order_items")
        # A local source over a connection is its own: there is nothing served to name.
        self.assertEqual(qr.source_of("source: t is duckdb.table('x')\nrun: t -> { select: * }"), "t")
        self.assertIsNone(qr.source_of(None))


class Rows(unittest.TestCase):
    def test_order_and_last_digit_float_noise_do_not_change_the_hash(self):
        a = [{"c": "x", "v": 0.1 + 0.2}, {"c": "y", "v": 2}]
        b = [{"v": 2, "c": "y"}, {"c": "x", "v": 0.3}]
        self.assertEqual(qr.rows_sha(a), qr.rows_sha(b))

    def test_a_real_difference_changes_it(self):
        self.assertNotEqual(qr.rows_sha([{"v": 0.3}]), qr.rows_sha([{"v": 0.31}]))
        self.assertNotEqual(qr.rows_sha([{"v": 1}]), qr.rows_sha([{"v": 1}, {"v": 1}]))


class Timing(unittest.TestCase):
    def a(self, repeats=3):
        return unittest.mock.Mock(publisher="http://h", environment="e", package="p",
                                  timeout=5, repeats=repeats)

    def test_the_warm_up_is_dropped_and_the_median_kept(self):
        times = iter([900, 10, 30, 20])
        calls = lambda *a, **k: {"rows": [{"n": 1}], "error": None, "executionTimeMs": next(times),
                                 "servedFrom": None, "queryCostBytes": None, "roundTripMs": 50}
        with unittest.mock.patch.object(qr, "timed_query", side_effect=calls) as tq:
            row = qr.time_one({"qid": "q", "query": "run: a -> b", "modelPath": "m"}, self.a(), "1.0.0")
        self.assertEqual(tq.call_count, 4)
        self.assertEqual(tq.call_args.kwargs["version"], "1.0.0")
        self.assertEqual((row["median_ms"], row["max_ms"], row["timing"]), (20, 30, "server"))
        self.assertEqual(row["served_from"], ["none"])
        self.assertFalse(row["unstable_rows"])

    def test_an_older_server_falls_back_to_the_round_trip_and_says_so(self):
        trips = iter([1, 7, 9, 8])
        calls = lambda *a, **k: {"rows": [], "error": None, "executionTimeMs": None,
                                 "servedFrom": None, "queryCostBytes": None,
                                 "roundTripMs": next(trips)}
        with unittest.mock.patch.object(qr, "timed_query", side_effect=calls):
            row = qr.time_one({"qid": "q", "query": "x", "modelPath": "m"}, self.a(), None)
        self.assertEqual((row["median_ms"], row["timing"]), (8, "roundTrip"))

    def test_rows_that_move_between_calls_mark_the_query_unstable(self):
        rows = iter([[{"n": 1}], [{"n": 2}], [{"n": 1}], [{"n": 1}]])
        calls = lambda *a, **k: {"rows": next(rows), "error": None, "executionTimeMs": 5,
                                 "servedFrom": None, "queryCostBytes": None, "roundTripMs": 6}
        with unittest.mock.patch.object(qr, "timed_query", side_effect=calls):
            row = qr.time_one({"qid": "q", "query": "x", "modelPath": "m"}, self.a(), None)
        self.assertTrue(row["unstable_rows"])

    def test_an_error_on_any_call_is_the_row(self):
        res = iter([None, "HTTP 400: bad", None, None])
        calls = lambda *a, **k: {"rows": [], "error": next(res), "executionTimeMs": 5,
                                 "servedFrom": None, "queryCostBytes": None, "roundTripMs": 6}
        with unittest.mock.patch.object(qr, "timed_query", side_effect=calls):
            row = qr.time_one({"qid": "q", "query": "x", "modelPath": "m"}, self.a(), None)
        self.assertEqual((row["error"], row["median_ms"]), ("HTTP 400: bad", None))


class Compare(unittest.TestCase):
    def test_band_and_floor(self):
        old = [result("fast", 1000), result("slow", 100), result("same", 1000),
               result("tiny", 2)]
        new = [result("fast", 200), result("slow", 200), result("same", 1100),
               result("tiny", 6)]
        c = qr.compare(old, new, band=0.2, min_delta_ms=10)
        self.assertEqual([e["qid"] for e in c["faster"]], ["fast"])
        self.assertEqual([e["qid"] for e in c["slower"]], ["slow"])
        # 10% is inside the band; 2 -> 6 ms is x3 but under the 10 ms floor.
        self.assertEqual(c["within_band"], 2)
        self.assertEqual((c["total_before_ms"], c["total_after_ms"]), (2102, 1506))
        self.assertFalse(c["blocked"])

    def test_changed_rows_and_new_errors_block_and_are_not_timed(self):
        old = [result("a", 100, "s1"), result("b", 100), result("c", 100, "s1", unstable_rows=True),
               result("d", 100)]
        new = [result("a", 10, "s2"), result("b", None, None, error="HTTP 500"),
               result("c", 100, "s9"), {**result("d", 10), "query": "run: t -> other"}]
        c = qr.compare(old, new, band=0.2)
        self.assertEqual((c["rows_changed"], c["broke"], c["unstable"], c["query_changed"]),
                         (["a"], ["b"], ["c"], ["d"]))
        self.assertEqual(c["faster"], [])
        self.assertTrue(c["blocked"])
        self.assertIn("BLOCKED", "\n".join(qr.fmt_compare(c, "x", "y")))


class StubPublisher(http.server.BaseHTTPRequestHandler):
    bodies: list = []
    rows = [{"category": "a", "n": 1}]
    fail = False

    def log_message(self, *a):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        StubPublisher.bodies.append(body)
        if StubPublisher.fail:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"message": "down"}')
            return
        result = {"schema": {"fields": [{"name": k} for k in self.rows[0]]},
                  "data": {"array_value": [{"kind": "record_cell", "record_value": [
                      {"number_value": v} if isinstance(v, (int, float)) else {"string_value": v}
                      for v in r.values()]} for r in self.rows]}}
        out = json.dumps({"result": json.dumps(result), "executionTimeMs": 12,
                          "servedFrom": "storage", "queryCostBytes": None}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(out)


class Rest(unittest.TestCase):
    def test_timed_query_pins_the_version_and_reads_the_timings(self):
        srv = http.server.HTTPServer(("127.0.0.1", 0), StubPublisher)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            StubPublisher.bodies = []
            r = publisher_rest.timed_query(f"http://127.0.0.1:{srv.server_port}", "e", "p",
                                           "m.malloy", "run: t -> v", version="1.0.1",
                                           givens={"region": "west"})
        finally:
            srv.shutdown()
            srv.server_close()
        self.assertEqual(StubPublisher.bodies[0]["versionId"], "1.0.1")
        self.assertEqual(StubPublisher.bodies[0]["givens"], {"region": "west"})
        self.assertEqual((r["error"], r["executionTimeMs"], r["servedFrom"], r["rows"]),
                         (None, 12, "storage", [{"category": "a", "n": 1}]))
        self.assertIsInstance(r["roundTripMs"], float)


class Replay(unittest.TestCase):
    """A whole replay, then a second one compared with it, against a stub server."""

    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.set = self.root / "set"
        self.set.mkdir()
        (self.set / "eval.toml").write_text(
            f'[model]\nenvironment = "e"\npackage = "p"\n[paths]\nworkdir = "{self.root / "w"}"\n'
            '[performance]\nbudget_ms = 15\nrepeats = 2\n')
        (self.set / "set.json").write_text('{"name": "s"}')
        (self.set / "cases.jsonl").write_text('{"qid": "q1", "tags": ["join"]}\n{"qid": "q2"}\n')
        answer_run(self.root / "w" / "runs" / "base", {"q1": ("match", "run: orders -> a"),
                                                         "q2": ("match", "run: items -> b")})
        self.srv = http.server.HTTPServer(("127.0.0.1", 0), StubPublisher)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_port}"

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        shutil.rmtree(self.root, ignore_errors=True)

    def replay(self, label, *extra, mat=None):
        out = io.StringIO()
        with unittest.mock.patch.object(qr, "package_version", return_value={"id": "1.0.0"}), \
                unittest.mock.patch.object(qr, "materialization_state", return_value=mat or {}), \
                contextlib.redirect_stdout(out):
            code = qr.main(["--set", str(self.set), "--from", "base", "--label", label,
                            "--publisher", self.url, *extra])
        return code, out.getvalue(), self.root / "w" / "runs" / label

    def test_a_replay_records_its_pins_rows_and_summary(self):
        code, out, d = self.replay("perf-1", mat={"persistSources": ["orders"], "bound": False})
        self.assertEqual(code, 0)
        self.assertIn("no bound table", out)
        meta = json.loads((d / "run.json").read_text())
        self.assertEqual((meta["phase"], meta["fromRun"], meta["repeats"], meta["budgetMs"]),
                         ("query-replay", "base", 2, 15))
        self.assertEqual(meta["packageVersion"]["id"], "1.0.0")
        self.assertEqual(StubPublisher.bodies[-1]["versionId"], "1.0.0")
        rows = [json.loads(x) for x in (d / "results.jsonl").read_text().splitlines()]
        self.assertEqual([r["qid"] for r in rows], ["q1", "q2"])
        self.assertEqual(rows[0]["tags"], ["join"])
        self.assertNotIn("rows", rows[0])
        self.assertEqual(json.loads((d / "rows" / "q1.json").read_text()), StubPublisher.rows)
        s = json.loads((d / "summary.json").read_text())
        self.assertEqual((s["p50_ms"], s["served_from_storage"], s["slow"]), (12, 2, []))
        # The run list and the export read it back.
        kinds = {r["run"]: r["kind"] for r in ri.runs(self.root / "w")}
        self.assertEqual(kinds["perf-1"], "perf")
        m = ex.export(d, self.set, d / "export")
        self.assertEqual(m["tables"]["eval_query_timings"]["rows"], 2)
        self.assertEqual(m["tables"]["eval_attempts"]["rows"], 0)

    def test_an_unversioned_package_records_its_publisher_json_version(self):
        with unittest.mock.patch.object(qr.rb, "base_version", return_value="1.2.0"), \
                unittest.mock.patch.object(qr.rb, "served_package_dir", return_value=None):
            a = argparse.Namespace(publisher=self.url, environment="e", package="p",
                                   target_version=None)
            with unittest.mock.patch.object(qr, "package_version", return_value={"id": None}):
                pin = qr.pin_version(a)
        self.assertEqual((pin["id"], pin["baseVersion"]), (None, "1.2.0"))

    def test_a_second_replay_with_changed_rows_is_blocked(self):
        self.replay("perf-1")
        old = StubPublisher.rows
        StubPublisher.rows = [{"category": "a", "n": 2}]
        try:
            code, out, _ = self.replay("perf-2", "--from", "perf-1", "--baseline", "perf-1")
        finally:
            StubPublisher.rows = old
        self.assertEqual(code, 1)
        self.assertIn("rows changed  q1", out)

    def test_a_replay_in_which_every_query_errored_is_aborted_and_never_the_record(self):
        StubPublisher.fail = True
        try:
            code, out, d = self.replay("perf-dead")
        finally:
            StubPublisher.fail = False
        self.assertEqual(code, 1)
        self.assertIn("every query errored", out)
        self.assertEqual(json.loads((d / "run.json").read_text())["status"], "aborted")
        self.assertIsNone(ri.record(ri.set_runs(qr.config.load(self.set)), "perf"))

    def test_a_workload_file_is_replayed_and_its_counts_weigh_the_total(self):
        wl = self.root / "day.jsonl"
        wl.write_text('{"query": "run: orders -> a", "count": 3}\n'
                      '\n'
                      '{"query": "run: items -> b", "modelPath": "other.malloy"}\n'
                      '{"query": "run: orders -> a", "count": 2}\n')
        out = io.StringIO()
        with unittest.mock.patch.object(qr, "package_version", return_value={"id": "1.0.0"}), \
                unittest.mock.patch.object(qr, "materialization_state", return_value={}), \
                contextlib.redirect_stdout(out):
            code = qr.main(["--set", str(self.set), "--queries", str(wl), "--label", "day-1",
                            "--publisher", self.url, "--model-path", "m.malloy"])
        self.assertEqual(code, 0)
        d = self.root / "w" / "runs" / "day-1"
        rows = [json.loads(x) for x in (d / "results.jsonl").read_text().splitlines()]
        by_query = {r["query"]: r for r in rows}
        self.assertEqual(len(rows), 2)                                # the repeat merged
        self.assertEqual(by_query["run: orders -> a"]["count"], 5)
        self.assertEqual(by_query["run: items -> b"]["modelPath"], "other.malloy")
        self.assertTrue(all(r["qid"].startswith("wl-") for r in rows))
        s = json.loads((d / "summary.json").read_text())
        self.assertEqual((s["total_ms"], s["weighted_ms"]), (24.0, 72.0))   # 12 ms each: 5 + 1
        self.assertEqual(s["by_source"]["orders"]["weighted_ms"], 60.0)
        self.assertEqual(json.loads((d / "run.json").read_text())["fromRun"], "day.jsonl")
        # the same file tomorrow names its queries the same, so the two compare
        again = qr.workload(wl, "m.malloy")
        self.assertEqual(sorted(q["qid"] for q in again), sorted(r["qid"] for r in rows))

    def test_a_workload_line_that_is_not_a_query_is_named_with_the_fix(self):
        wl = self.root / "bad.jsonl"
        for text, said in (('{"q": "x"}\n', "`query` string"), ("not json\n", "one JSON object per line"),
                           ('{"query": "run: a -> b", "count": 0}\n', "count")):
            wl.write_text(text)
            with self.assertRaises(SystemExit) as e:
                qr.workload(wl, None)
            self.assertIn(said, str(e.exception))

    def test_from_and_queries_are_one_or_the_other(self):
        for extra in ([], ["--from", "base", "--queries", "x"]):
            with self.assertRaises(SystemExit) as e:
                qr.main(["--set", str(self.set), "--publisher", self.url, *extra])
            self.assertIn("exactly one of --from", str(e.exception))

    def test_a_replay_records_what_the_tables_cost_and_shows_the_baselines(self):
        one = {"builds": [{"source": "orders", "rows": 9, "buildMs": 400, "scannedBytes": None}]}
        two = {"builds": [{"source": "orders", "rows": 9, "buildMs": 300, "scannedBytes": None},
                          {"source": "items", "rows": 2, "buildMs": None, "scannedBytes": None}]}
        _, _, d1 = self.replay("perf-1", mat=one)
        code, out, d2 = self.replay("perf-2", "--from", "perf-1", "--baseline", "perf-1", mat=two)
        b = json.loads((d2 / "summary.json").read_text())["build"]
        self.assertEqual((b["tables"], b["build_ms"], b["scanned_bytes"], b["rows"]),
                         (2, 300, None, 11))
        self.assertEqual(b["unreported"], {"build_ms": 1, "scanned_bytes": 2})
        self.assertIn("build cost: 2 table(s), 300 ms to build (1 did not report a build time)", out)
        self.assertIn("(baseline: 1 table(s), 400 ms to build)", out)
        row = next(r for r in ri.set_runs(qr.config.load(self.set)) if r["run"] == "perf-2")
        self.assertEqual(row["build_ms"], 300)

    def test_a_package_with_no_built_table_has_no_build_cost(self):
        _, out, d = self.replay("perf-1")
        self.assertIsNone(json.loads((d / "summary.json").read_text())["build"])
        self.assertNotIn("build cost", out)
        self.assertIsNone(qr.build_summary([]))

    def test_a_slice_limits_the_queries(self):
        code, _, d = self.replay("perf-s", "--tag", "join")
        self.assertEqual(code, 0)
        self.assertEqual([json.loads(x)["qid"] for x in (d / "results.jsonl").read_text().splitlines()],
                         ["q1"])

    def test_versions_shows_each_versions_replay_and_the_speed_between(self):
        w = self.root / "w"
        for name, ver, ms in (("p-a", "1.0.0", 100), ("p-b", "1.1.0", 20)):
            d = w / "runs" / name
            d.mkdir(parents=True)
            (d / "run.json").write_text(json.dumps({"phase": "query-replay", "status": "complete",
                                                    "started": name, "latencyBand": 0.2,
                                                    "packageVersion": {"id": ver}}))
            (d / "results.jsonl").write_text(json.dumps(result("q1", ms)) + "\n")
            (d / "summary.json").write_text(json.dumps({"p50_ms": ms, "total_ms": ms, "queries": 1}))
        # A later replay of 1.0.0 in which every query errored does not replace the good one.
        d = w / "runs" / "p-c"
        d.mkdir(parents=True)
        (d / "run.json").write_text(json.dumps({"phase": "query-replay", "status": "complete",
                                                "started": "p-c", "packageVersion": {"id": "1.0.0"}}))
        (d / "results.jsonl").write_text(json.dumps({**result("q1", None, None), "error": "HTTP 400"}) + "\n")
        (d / "summary.json").write_text(json.dumps({"p50_ms": None, "queries": 1, "errors": 1}))
        rows = ri.versions(ri.runs(w))
        self.assertEqual(next(r for r in rows if r["version"] == "1.0.0")["perf"]["run"], "p-a")
        top = next(r for r in rows if r["version"] == "1.1.0")
        self.assertEqual(top["speed"], {"faster": 1, "slower": 0, "rows_changed": 0,
                                        "compared": 1, "total_ratio": 0.2})
        self.assertIn("vs 1.0.0: 1 faster, 0 slower", "\n".join(ri.fmt_versions(rows)))

    def test_unpublished_versions_get_a_row_each(self):
        w = self.root / "w"
        for name, base in (("u-a", "1.1.0"), ("u-b", "1.2.0")):
            d = w / "runs" / name
            d.mkdir(parents=True)
            (d / "run.json").write_text(json.dumps({
                "phase": "query-replay", "status": "complete", "started": name,
                "packageVersion": {"id": None, "baseVersion": base}}))
            (d / "results.jsonl").write_text(json.dumps(result("q1", 10)) + "\n")
            (d / "summary.json").write_text(json.dumps({"p50_ms": 10, "total_ms": 10, "queries": 1}))
        names = [r["version"] for r in ri.versions(ri.runs(w))]
        self.assertEqual(names[:2], ["1.2.0 (unpublished)", "1.1.0 (unpublished)"])


if __name__ == "__main__":
    unittest.main()
