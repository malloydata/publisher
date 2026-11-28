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
    def test_draws_unjudged_question_groups_and_the_label_fixes_the_draw(self):
        turns = [turn(f"t{i}", q=f"q{i}", at=f"2026-11-24T{i:02d}:00:00Z") for i in range(10)]
        turns += [turn("x", q=""), {**turn("b", q="add a measure"), "kind": "build"}]
        a = js.pick(turns, {"t0", "t1"}, 3, "prod-01")
        self.assertEqual(len(a), 3)
        self.assertFalse({g["turn"]["turn_id"] for g in a} & {"t0", "t1", "x", "b"})
        self.assertEqual(a, js.pick(turns, {"t0", "t1"}, 3, "prod-01"))
        self.assertEqual(a, sorted(a, key=lambda g: g["turn"]["at"]))
        self.assertEqual(len(js.pick(turns, set(), 50, "s")), 10)

    def test_the_same_question_in_45_sessions_is_one_group_of_45(self):
        turns = [{**turn(f"t{i}", q="Ray ban summer sales"), "session_id": f"s{i}"} for i in range(45)]
        turns.append({**turn("u", q="ray ban  SUMMER sales"), "session_id": "s99"})
        got = js.pick(turns, set(), 10, "l")
        self.assertEqual([len(g["members"]) for g in got], [46])

    def test_a_follow_up_groups_only_after_the_same_earlier_question(self):
        turns = [{**turn("a1", q="ray ban sales", at="2026-11-24T01:00:00Z"), "session_id": "A"},
                 {**turn("a2", q="all summers", at="2026-11-24T01:01:00Z"), "session_id": "A"},
                 {**turn("b1", q="oakley sales", at="2026-11-24T02:00:00Z"), "session_id": "B"},
                 {**turn("b2", q="all summers", at="2026-11-24T02:01:00Z"), "session_id": "B"}]
        notes = js.annotate(turns)
        self.assertNotEqual(notes["a2"]["group"], notes["b2"]["group"])
        self.assertEqual([e["question"] for e in notes["a2"]["earlier"]], ["ray ban sales"])
        self.assertEqual(notes["a1"]["next"], "all summers")

    def test_a_turn_the_person_corrected_is_drawn_first_and_says_why(self):
        turns = [{**turn(f"t{i}", q=f"q{i}", at=f"2026-11-24T{i:02d}:00:00Z"), "session_id": f"s{i}"}
                 for i in range(20)]
        turns.append({**turn("c", q="that's not right, it should exclude returns",
                             at="2026-11-24T05:30:00Z"), "session_id": "s5"})
        turns.append({**turn("r", q="q7", at="2026-11-24T07:30:00Z"), "session_id": "s7"})
        got = js.pick(turns, set(), 2, "l")
        self.assertEqual(sorted((g["turn"]["turn_id"], g["signal"]) for g in got),
                         [("t5", "correction"), ("t7", "asked_again")])


    def test_up_to_three_turns_of_a_group_are_judged_and_share_out_the_rest(self):
        turns = [{**turn(f"t{i:02d}", q="Ray ban summer sales"), "session_id": f"s{i}"} for i in range(45)]
        got = js.pick(turns, set(), 10, "l", per_group=3)
        self.assertEqual(len(got), 3)
        self.assertEqual(sorted(len(g["members"]) for g in got), [15, 15, 15])
        self.assertEqual(sum(g["first"] for g in got), 1)
        self.assertEqual(len({m["turn_id"] for g in got for m in g["members"]}), 45)

    def test_share_keys_gives_a_groups_key_to_its_other_cases(self):
        d = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, True)
        (d / "gold").mkdir()
        g = {"status": "verified", "kind": "scalar", "value": 3}
        (d / "gold" / "p-a.json").write_text('{"qid": "p-a", "truthQuery": "run: t"}')
        cases = [{"qid": "p-a", "golden": g, "expectedEntities": {"required": ["m:x"]},
                  "sourceFields": {"group": "G", "keyHere": True}},
                 {"qid": "p-b", "sourceFields": {"group": "G", "keyHere": False}},
                 {"qid": "p-c", "sourceFields": {"group": "H", "keyHere": False}}]
        (d / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
        self.assertEqual(js.share_keys(d), (1, 1))
        got = [json.loads(l) for l in (d / "cases.jsonl").read_text().splitlines()]
        self.assertEqual((got[1]["golden"], got[1]["expectedEntities"]), (g, {"required": ["m:x"]}))
        self.assertEqual(json.loads((d / "gold" / "p-b.json").read_text())["qid"], "p-b")
        self.assertNotIn("golden", got[2])

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
        cases = [{"qid": "p-a", "source": "a", "sourceFields": {"group": "g1", "members": ["a", "a2", "a3"]}},
                 {"qid": "p-b", "source": "b", "sourceFields": {"signal": "correction"},
                  "context": {"earlierMessages": [{"question": "ray ban sales"}]}},
                 {"qid": "p-c", "source": "c"}]
        ev[3]["depends_on"] = [1]
        (run / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ev))
        got = {r["turn_id"]: r for r in js.judgments(run, cases, "now")}
        # The rest of a group carries the judged turn's verdict.
        self.assertEqual([(got[t]["right"], got[t]["sampled_because"], got[t]["judged_turn"])
                          for t in ("a", "a2", "a3")],
                         [(True, "random", "a"), (True, "carried", "a"), (True, "carried", "a")])
        self.assertEqual((got["b"]["sampled_because"], got["b"]["user_signal"], got["b"]["depends_on"]),
                         ("user_signal", "correction", '["ray ban sales"]'))
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
        groups = [{"turn": t, "members": [t], "signal": ""} for t in (
            {"turn_id": "t1", "question": "top brands?"}, {"turn_id": "t2", "question": "margin?"},
            {"turn_id": "t3", "question": "new one"})]
        n = js.write_set(out, "l", groups, {}, parent)
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
        # Same question twice: one group, both turns judged, the key derived on one.
        self.assertEqual(len(cases), 2)
        self.assertEqual({c["sourceFields"]["group"] for c in cases}, {cases[0]["sourceFields"]["group"]})
        self.assertEqual(sorted(c["sourceFields"]["keyHere"] for c in cases), [False, True])
        self.assertNotIn("golden", cases[0])
        # Its own config and work directory: never one of this set's runs.
        import config
        jcfg = config.load(judging)
        self.assertEqual((jcfg.workdir(), jcfg.get("model", "package")), ((judging / "work").resolve(), "storefront"))
        self.assertFalse((root / "w" / "runs").exists())
        self.assertIn("1 question groups drawn (2 turns to judge), covering 2 turns", out.getvalue())

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
