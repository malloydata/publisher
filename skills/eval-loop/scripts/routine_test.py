#!/usr/bin/env python3
"""Tests for routine.py: each kind's steps, what stops it, the exit code and the result file."""
import contextlib
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import routine  # noqa: E402
import run_store  # noqa: E402


class Kinds(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.set = self.root / "set"
        self.set.mkdir()
        (self.set / "set.json").write_text('{"name": "s"}')
        (self.set / "cases.jsonl").write_text('{"qid": "a"}\n{"qid": "b"}\n')
        self.work = self.root / "w"
        self.store = self.root / "store"
        (self.set / "eval.toml").write_text(
            f'[paths]\nworkdir = "{self.work}"\n[model]\npackage = "p"\n[truth]\n'
            f'[store]\npath = "{self.store}"\n')
        self.run_dir = self.work / "runs" / "n"

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def answer_run(self, d: pathlib.Path, status="complete", **meta):
        d.mkdir(parents=True, exist_ok=True)
        (d / "run.json").write_text(json.dumps({"status": status, "package": "p",
                                                "started": "2026-11-01", **meta}))
        (d / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in (
            {"kind": "attempt", "qid": "a", "question_sha": "s"},
            {"kind": "score", "qid": "a", "verdict": "match"},
            {"kind": "attempt", "qid": "b", "question_sha": "s"},
            {"kind": "score", "qid": "b", "verdict": "no_match"})))

    def replay_run(self, d: pathlib.Path, status="complete", errors=0, compare=None):
        d.mkdir(parents=True, exist_ok=True)
        (d / "run.json").write_text(json.dumps({"status": status, "package": "p",
                                                "phase": "query-replay", "started": "2026-11-02"}))
        (d / "results.jsonl").write_text("")
        (d / "summary.json").write_text(json.dumps({
            "queries": 2, "errors": errors, "p50_ms": 5, "p95_ms": 9, "total_ms": 14,
            "slow": [], **({"compare": compare} if compare else {})}))

    def go(self, kind, codes=None, on=None, *extra):
        """Run a routine with `step` faked: `codes` are exit codes by step name,
        `on` are side effects by step name (what the step would have written)."""
        ran, calls = [], []

        def step(name, *args):
            ran.append(name)
            calls.append(args)
            (on or {}).get(name, lambda: None)()
            return (codes or {}).get(name, 0)
        out = io.StringIO()
        with mock.patch.object(routine, "step", side_effect=step), \
                contextlib.redirect_stdout(out):
            code = routine.main(["--set", str(self.set), "--kind", kind, "--label", "n", *extra])
        result = json.loads((self.work / "routines" / "n.json").read_text())
        return code, ran, result, calls

    # ---------------------------------------------------------------- quality

    def test_quality_runs_its_steps_in_order_and_reports_the_pass_rate(self):
        code, ran, r, _ = self.go("quality", on={"run": lambda: self.answer_run(self.run_dir)})
        self.assertEqual((code, ran), (0, ["check", "verify", "run", "diagnose",
                                           "package", "history"]))
        self.assertEqual((r["kind"], r["exit"], r["number"]["passed"], r["number"]["decided"],
                          r["number"]["passRate"]), ("quality", 0, 1, 2, 0.5))
        self.assertEqual([s["step"] for s in r["steps"]], ran)

    def test_the_number_is_split_and_the_test_split_is_the_one_to_quote(self):
        (self.set / "cases.jsonl").write_text(
            '{"qid": "a", "split": "test"}\n{"qid": "b", "split": "dev"}\n')
        _, _, r, _ = self.go("quality", on={"run": lambda: self.answer_run(self.run_dir)})
        self.assertEqual(r["number"]["bySplit"],
                         {"dev": {"passed": 0, "decided": 1, "passRate": 0.0},
                          "test": {"passed": 1, "decided": 1, "passRate": 1.0}})
        self.assertIn("1 of 1 on the test split", r["reason"])

    def test_quality_never_times_queries(self):
        _, ran, _, _ = self.go("quality", on={"run": lambda: self.answer_run(self.run_dir)})
        self.assertNotIn("query-replay", ran)

    def test_a_failed_step_stops_everything_after_it_and_exits_1(self):
        code, ran, r, _ = self.go("quality", {"verify": 3})
        self.assertEqual((code, ran), (1, ["check", "verify"]))
        self.assertIn("stopped at verify", r["reason"])

    def test_a_run_that_did_not_finish_has_no_number_and_is_not_diagnosed(self):
        code, ran, r, _ = self.go("quality", on={"run": lambda: self.answer_run(
            self.run_dir, "incomplete", truncated=["b"])})
        self.assertEqual((code, ran[-1]), (5, "run"))
        self.assertIn("1 truncated", r["reason"])

    def test_a_case_voided_twice_is_left_out_and_the_routine_exits_4(self):
        code, ran, r, calls = self.go("quality", on={"run": lambda: self.answer_run(
            self.run_dir, "complete", truncated=["b"], recovered=["b"])})
        self.assertEqual((code, ran[-1]), (4, "history"))
        self.assertIn("voided twice and left out: b", r["reason"])
        self.assertIn("--recover", next(c for c in calls if c[0] == "run"))

    def test_a_run_stopped_at_the_cost_cap_has_no_number(self):
        code, ran, r, calls = self.go(
            "quality", {"run": 1},
            {"run": lambda: self.answer_run(self.run_dir, "aborted", abortReason="max_cost")},
            "--max-cost-usd", "5")
        self.assertEqual((code, ran[-1]), (5, "run"))
        self.assertIn("--max-cost-usd 5", r["reason"])
        run = next(c for c in calls if c[0] == "run")
        self.assertEqual(run[run.index("--max-cost-usd") + 1], "5.0")

    def test_a_run_that_reached_the_cap_keeps_its_number_and_is_not_diagnosed(self):
        code, ran, r, _ = self.go(
            "quality", None,
            {"run": lambda: self.answer_run(self.run_dir, answererCostUsd=4.5, judgeCostUsd=0.6)},
            "--max-cost-usd", "5")
        self.assertEqual((code, ran), (4, ["check", "verify", "run", "history"]))
        self.assertIn("was not diagnosed", r["reason"])
        self.assertEqual(r["number"]["passed"], 1)

    def test_under_the_cap_everything_runs(self):
        code, ran, _, _ = self.go(
            "quality", None,
            {"run": lambda: self.answer_run(self.run_dir, answererCostUsd=1.0, judgeCostUsd=0.1)},
            "--max-cost-usd", "5")
        self.assertEqual((code, "diagnose" in ran), (0, True))

    def test_the_run_is_shared_after_the_diagnosis_is_written_into_it(self):
        def diagnose():
            with (self.run_dir / "events.jsonl").open("a") as f:
                f.write(json.dumps({"kind": "issue", "qid": "b"}) + "\n")
        code, _, r, calls = self.go("quality", on={"run": lambda: self.answer_run(self.run_dir),
                                                   "diagnose": diagnose})
        self.assertEqual((code, r["shared"]), (0, "in the store"))
        stored = (self.store / "s" / "runs" / "n" / "events.jsonl").read_text()
        self.assertIn('"issue"', stored)
        self.assertIn("--no-share", next(c for c in calls if c[0] == "run"))

    # ------------------------------------------------------------------ speed

    def test_speed_times_the_last_record_runs_queries_the_first_time(self):
        self.answer_run(self.work / "runs" / "base")
        code, ran, r, calls = self.go("speed", on={"query-replay": lambda: self.replay_run(self.run_dir)})
        self.assertEqual((code, ran), (0, ["check", "query-replay", "diagnose", "package", "history"]))
        replay = next(c for c in calls if c[0] == "query-replay")
        self.assertEqual(replay[replay.index("--from") + 1], str(self.work / "runs" / "base"))
        self.assertNotIn("--baseline", replay)
        self.assertEqual((r["number"]["totalMs"], r["number"]["queries"]), (14, 2))

    def test_speed_replays_and_compares_with_the_record_replay_after_that(self):
        self.replay_run(self.work / "runs" / "old-perf")
        _, _, _, calls = self.go("speed", on={"query-replay": lambda: self.replay_run(self.run_dir)})
        replay = next(c for c in calls if c[0] == "query-replay")
        self.assertEqual(replay[replay.index("--from") + 1], str(self.work / "runs" / "old-perf"))
        self.assertEqual(replay[replay.index("--baseline") + 1], "record")

    def test_speed_times_a_workload_file_when_given_one(self):
        _, _, _, calls = self.go("speed", None,
                                 {"query-replay": lambda: self.replay_run(self.run_dir)},
                                 "--queries", "/logs/day.jsonl")
        replay = next(c for c in calls if c[0] == "query-replay")
        self.assertEqual(replay[replay.index("--queries") + 1], "/logs/day.jsonl")
        self.assertNotIn("--from", replay)

    def test_speed_with_nothing_to_time_cannot_start(self):
        code, ran, r, _ = self.go("speed")
        self.assertEqual((code, ran), (3, ["check"]))
        self.assertIn("--kind quality", r["reason"])

    def test_changed_rows_exit_4_and_the_rest_still_runs(self):
        self.replay_run(self.work / "runs" / "old-perf")
        blocked = {"baseline": "old-perf", "rows_changed": ["a"], "broke": [], "blocked": True}
        code, ran, r, _ = self.go("speed", {"query-replay": 1},
                                  {"query-replay": lambda: self.replay_run(self.run_dir, compare=blocked)})
        self.assertEqual((code, ran[-1]), (4, "history"))
        self.assertIn("1 stored answer(s) changed rows", r["reason"])

    def test_a_replay_in_which_every_query_errored_has_no_number(self):
        self.answer_run(self.work / "runs" / "base")
        code, ran, _, _ = self.go("speed", {"query-replay": 1}, {
            "query-replay": lambda: self.replay_run(self.run_dir, "aborted", errors=2)})
        self.assertEqual((code, ran[-1]), (5, "query-replay"))

    def test_a_replay_that_wrote_nothing_is_a_failed_step(self):
        self.answer_run(self.work / "runs" / "base")
        code, _, _, _ = self.go("speed", {"query-replay": 1})
        self.assertEqual(code, 1)

    # ------------------------------------------------------------------ serve

    def test_serve_starts_what_is_not_up_and_stops_only_what_it_started(self):
        with mock.patch("check_set.ours", side_effect=lambda cfg, role, port: role == "truth"):
            code, ran, _, _ = self.go("quality", None,
                                      {"run": lambda: self.answer_run(self.run_dir)}, "--serve")
        self.assertEqual(code, 0)
        self.assertEqual(ran[0], "serve model")
        self.assertEqual([x for x in ran if x.startswith(("serve", "stop"))],
                         ["serve model", "stop model"])

    def test_a_server_that_will_not_start_stops_the_routine(self):
        with mock.patch("check_set.ours", return_value=False):
            code, ran, _, _ = self.go("quality", {"serve model": 1}, None, "--serve")
        self.assertEqual((code, ran), (1, ["serve model"]))

    def test_improve_mode_with_a_dirty_repository_stops_before_anything_runs(self):
        # Found live: a run and its diagnosis were paid for, and only then did
        # improve mode find it had nowhere to leave an edit.
        (self.set / "eval.toml").write_text(
            (self.set / "eval.toml").read_text().replace('package = "p"', 'package = "p"\nrepo = "."'))
        with mock.patch("improve_mode.repo_not_ready", return_value="the repository at /r "
                        "has uncommitted changes (3 path(s))"):
            code, ran, result, _ = self.go("quality", None, None, "--mode", "improve")
        self.assertEqual((code, ran), (routine.CANNOT_START, []))
        self.assertIn("improve mode cannot start: the repository at /r", result["reason"])

    def test_improve_mode_with_no_repo_named_cannot_start(self):
        code, ran, result, _ = self.go("quality", None, None, "--mode", "improve")
        self.assertEqual((code, ran), (routine.CANNOT_START, []))
        self.assertIn("no [model] repo in", result["reason"])

    def test_run_mode_does_not_ask_about_the_repository(self):
        with mock.patch("improve_mode.repo_not_ready", side_effect=AssertionError("asked")):
            self.answer_run(self.run_dir)
            code, _, _, _ = self.go("quality")
        self.assertEqual(code, routine.DONE)

    def test_with_no_truth_package_the_keys_go_unchecked_and_the_routine_says_so(self):
        (self.set / "eval.toml").write_text(
            (self.set / "eval.toml").read_text().replace("[truth]\n", ""))
        self.answer_run(self.run_dir)
        code, ran, result, _ = self.go("quality", {"verify": 3})
        self.assertEqual(code, routine.DONE)
        self.assertIn("run", ran)
        self.assertIs(result["keysVerified"], False)
        self.assertIn("the answer keys were not re-derived", result["reason"])

    def test_a_truth_server_that_could_not_check_still_stops_the_routine(self):
        code, ran, _, _ = self.go("quality", {"verify": 3})
        self.assertEqual((code, ran[-1]), (routine.STEP_FAILED, "verify"))

    def test_the_improve_sentence_names_the_edits_cost_and_the_cap(self):
        said = routine.improve_said({"branch": "eval/n", "accepted": 1, "measured": 1,
                                     "edits": [{"outcome": "accepted"}], "costUsd": 3.25,
                                     "capped": "stopped before edit 2 of 2: $9.00 spent"})
        self.assertEqual(said, "1 of 1 edit(s) accepted, on the branch eval/n. Nothing is pushed "
                               "or published: open a pull request from it. The edits cost $3.25. "
                               "Stopped before edit 2 of 2: $9.00 spent")
        self.assertEqual(routine.improve_said({"edits": [], "capped": "stopped before edit 1 of 1"}),
                         "No edit was tried: stopped before edit 1 of 1")

    def test_the_kind_is_required(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            routine.main(["--set", str(self.set)])


if __name__ == "__main__":
    unittest.main()
