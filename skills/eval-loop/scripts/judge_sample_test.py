"""judge_sample.py: draw unjudged production turns, and keep the judging run's verdicts."""
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "eval-answer" / "scripts"))
import ingest_logs as il  # noqa: E402
import judge_sample as js  # noqa: E402
import optimization_package as op  # noqa: E402


def turn(tid, q="top brands?", at="2026-11-24T10:00:00Z"):
    return {"turn_id": tid, "at": at, "package": "storefront", "question": q}


class Pick(unittest.TestCase):
    def test_draws_only_unjudged_turns_with_a_question_and_the_label_fixes_the_draw(self):
        turns = [turn(f"t{i}", at=f"2026-11-24T{i:02d}:00:00Z") for i in range(10)] + [turn("x", q="")]
        a = js.pick(turns, {"t0", "t1"}, 3, "prod-01")
        self.assertEqual(len(a), 3)
        self.assertFalse({t["turn_id"] for t in a} & {"t0", "t1", "x"})
        self.assertEqual(a, js.pick(turns, {"t0", "t1"}, 3, "prod-01"))
        self.assertEqual(a, sorted(a, key=lambda t: t["at"]))
        self.assertEqual(len(js.pick(turns, set(), 50, "s")), 10)


class Collect(unittest.TestCase):
    def test_a_verdict_per_turn_with_the_reason_only_for_a_wrong_answer(self):
        run = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, run, True)
        (run / "run.json").write_text(json.dumps({"judgeVersion": "j3", "rubricSha": "abcdef0123456789"}))
        ev = [{"kind": "attempt", "qid": "p-a", "answer_text": "Top is X.", "final_query": "run: q"},
              {"kind": "attempt", "qid": "p-b", "answer_text": "", "final_query": "run: q"},
              {"kind": "score", "qid": "p-a", "verdict": "match"},
              {"kind": "score", "qid": "p-b", "verdict": "no_match"},
              {"kind": "issue", "qids": ["p-b"], "primary_code": "MISSING", "component": "model-definition",
               "owner": "model"}]
        (run / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
        cases = [{"qid": "p-a", "source": "a"}, {"qid": "p-b", "source": "b"}, {"qid": "p-c", "source": "c"}]
        got = {r["turn_id"]: r for r in js.judgments(run, cases, "now")}
        self.assertEqual((got["a"]["right"], got["a"]["owner"]), (True, None))
        self.assertEqual((got["b"]["right"], got["b"]["failure_code"], got["b"]["owner"]),
                         (False, "MISSING", "model"))
        self.assertIsNone(got["c"]["right"])
        self.assertEqual(got["c"]["verdict"], "not judged")
        self.assertEqual(got["a"]["metric_version"], "j3:abcdef012345")
        # An MCP client logs no prose, so its verdict rests on the last query.
        self.assertEqual([got[t]["basis"] for t in "abc"], ["answer", "final_query", None])


class Keys(unittest.TestCase):
    def test_a_turn_asking_a_question_the_set_keys_takes_that_verified_key(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        parent = root / "set"
        parent.mkdir()
        (parent / "set.json").write_text(json.dumps({"name": "s", "truthPackage": "s-truth",
                                                     "conventions": ["summer is Jun-Aug"]}))
        g = {"status": "verified", "kind": "rows", "value": [{"x": 1}]}
        (parent / "cases.jsonl").write_text(
            json.dumps({"qid": "a", "question": "Top  Brands?", "golden": g}) + "\n"
            + json.dumps({"qid": "b", "question": "margin?",
                          "golden": {"status": "provisional", "value": [{"y": 2}]}}) + "\n")
        out = root / "judging"
        n = js.write_set(out, "l", [{"turn_id": "t1", "question": "top brands?"},
                                    {"turn_id": "t2", "question": "margin?"},
                                    {"turn_id": "t3", "question": "new one"}], {}, parent)
        cases = [json.loads(l) for l in (out / "cases.jsonl").read_text().splitlines()]
        self.assertEqual(n, 1)
        self.assertEqual([c.get("golden") for c in cases], [g, None, None])
        meta = json.loads((out / "set.json").read_text())
        self.assertEqual((meta["truthPackage"], meta["conventions"]), ("s-truth", ["summer is Jun-Aug"]))


class Main(unittest.TestCase):
    def test_pick_writes_a_set_and_the_transcripts_the_history_tables_hold(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        (root / "set").mkdir()
        (root / "set" / "set.json").write_text('{"name": "s"}')
        (root / "set" / "eval.toml").write_text(
            f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "storefront"\nenvironment = "e"\n')
        data = root / "w" / "packages" / "optimization-s" / "data"
        il.ingest(data, "prod_turns", [{**turn("t1"), "transcript": [{"type": "user"}]}, turn("t2")],
                  "storefront", set())
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            js.main(["pick", "--set", str(root / "set"), "--label", "prod-01"])
        judging = root / "w" / "judging" / "prod-01"
        cases = [json.loads(l) for l in (judging / "cases.jsonl").read_text().splitlines()]
        self.assertEqual(sorted(c["source"] for c in cases), ["t1", "t2"])
        self.assertNotIn("golden", cases[0])
        # Its own config and work directory: never one of this set's runs.
        import config
        jcfg = config.load(judging)
        self.assertEqual((jcfg.workdir(), jcfg.get("model", "package")), ((judging / "work").resolve(), "storefront"))
        self.assertTrue((judging / "work" / "runs" / "prod-01" / "artifacts" / "p-t1" / "answerer.jsonl").is_file())
        self.assertFalse((root / "w" / "runs").exists())
        self.assertIn("1 need fetch_transcripts.py", out.getvalue())

    def test_collect_refuses_a_wrong_answer_with_no_diagnosis_unless_told(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        (root / "set").mkdir()
        (root / "set" / "set.json").write_text('{"name": "s"}')
        (root / "set" / "eval.toml").write_text(
            f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "storefront"\nenvironment = "e"\n')
        judging = root / "w" / "judging" / "l"
        run = judging / "work" / "runs" / "l"
        run.mkdir(parents=True)
        (judging / "cases.jsonl").write_text(json.dumps({"qid": "p-t1", "source": "t1"}) + "\n")
        (run / "run.json").write_text('{"judgeVersion": "j"}')
        (run / "events.jsonl").write_text(json.dumps({"kind": "score", "qid": "p-t1", "verdict": "no_match"}) + "\n")
        argv = ["collect", "--set", str(root / "set"), "--label", "l"]
        with self.assertRaises(SystemExit) as e:
            js.main(argv)
        self.assertIn("no diagnosis yet", str(e.exception))
        data = root / "w" / "packages" / "optimization-s" / "data"
        self.assertEqual(op.read(data / "prod_judgments.csv"), [])
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO):
            js.main(argv + ["--without-diagnosis"])
        self.assertEqual(op.read(data / "prod_judgments.csv")[0]["right"], "False")


if __name__ == "__main__":
    unittest.main()
