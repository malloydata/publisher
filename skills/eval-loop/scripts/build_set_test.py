"""build_set.py: production questions and queries into the set."""
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import build_set as bs  # noqa: E402
import optimization_package as op  # noqa: E402


def t(tid, q, kind="question", at="2026-11-20T00:00:00Z"):
    return {"turn_id": tid, "question": q, "kind": kind, "at": at}


class Choose(unittest.TestCase):
    def test_wrong_first_then_undecided_then_most_asked_and_never_a_build_or_a_held_question(self):
        turns = [t("1", "Top brands?"), t("2", "top  brands?"), t("3", "Top brands?"),
                 t("4", "Revenue by month"), t("5", "Returns by category"),
                 t("6", "Make a dashboard", kind="build"), t("7", "Margin by brand"),
                 t("8", "Already held")]
        judged = [{"turn_id": "4", "right": "False"}, {"turn_id": "5", "right": ""},
                  {"turn_id": "1", "right": "True"}]
        got = bs.choose(turns, judged, {"already held"}, 10)
        self.assertEqual([(g["question"], g["why"]) for g in got],
                         [("Revenue by month", "judged wrong"),
                          ("Returns by category", "judged, undecided"),
                          ("Top brands?", "asked often"), ("Margin by brand", "asked often")])
        self.assertEqual(len(got[2]["turns"]), 3)
        self.assertEqual(len(bs.choose(turns, judged, set(), 2)), 2)

    def test_a_wrong_verdict_is_not_hidden_by_an_undecided_one_for_the_same_turn(self):
        got = bs.choose([t("1", "q")], [{"turn_id": "1", "right": "False"},
                                        {"turn_id": "1", "right": ""}], set(), 5)
        self.assertEqual(got[0]["why"], "judged wrong")


class Main(unittest.TestCase):
    def test_cases_are_appended_once_and_the_workload_counts_each_query(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        s = root / "set"
        s.mkdir()
        (s / "set.json").write_text('{"name": "s"}')
        (s / "eval.toml").write_text(f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "p"\nenvironment = "e"\n')
        (s / "cases.jsonl").write_text(json.dumps({"qid": "a", "question": "Old one"}) + "\n")
        data = op.install_for(__import__("config").load(s))
        op.append(data / "prod_turns.csv", op.APPENDED["prod_turns"][2],
                  [t("1", "Old one"), t("2", "New one"), t("3", "Edit it", kind="build")])
        q = op.APPENDED["prod_queries"][2]
        op.append(data / "prod_queries.csv", q, [
            {"execution_id": "x1", "package": "p", "query_text": "run: a -> v", "model_path": "m.malloy", "status": "ok"},
            {"execution_id": "x2", "package": "p", "query_text": "run: a -> v", "model_path": "m.malloy", "status": "ok"},
            {"execution_id": "x3", "package": "p", "query_text": "run: bad", "model_path": "m.malloy", "status": "compile_error"},
            {"execution_id": "x4", "package": "other", "query_text": "run: a -> v", "model_path": "m.malloy", "status": "ok"}])
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            bs.main(["--set", str(s), "--label", "prod-1"])
            bs.main(["--set", str(s), "--label", "prod-2"])
        cases = [json.loads(l) for l in (s / "cases.jsonl").read_text().splitlines()]
        self.assertEqual([c["question"] for c in cases], ["Old one", "New one"])
        self.assertEqual((cases[1]["split"], cases[1]["source"], cases[1]["tags"]),
                         ("dev", "production:prod-1", ["production", "asked-often"]))
        self.assertNotIn("golden", cases[1])
        wl = [json.loads(l) for l in (s / "workloads" / "prod-1.jsonl").read_text().splitlines()]
        self.assertEqual(wl, [{"query": "run: a -> v", "modelPath": "m.malloy", "count": 2}])
        self.assertIn("skipped 1 build turns", out.getvalue())

    def test_a_case_carries_the_golden_production_judging_verified_for_one_of_its_turns(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        s = root / "set"
        s.mkdir()
        (s / "set.json").write_text('{"name": "s"}')
        (s / "eval.toml").write_text(f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "p"\nenvironment = "e"\n')
        data = op.install_for(__import__("config").load(s))
        op.append(data / "prod_turns.csv", op.APPENDED["prod_turns"][2],
                  [t("1", "Keyed"), t("2", "keyed"), t("3", "Unverified"), t("4", "Never judged")])
        op.append(data / "prod_judgments.csv", op.APPENDED["prod_judgments"][2],
                  [{"turn_id": "2", "right": "False"}])
        judging = root / "w" / "judging" / "prod-j1"
        judging.mkdir(parents=True)
        gold = {"kind": "scalar", "value": 7, "status": "verified"}
        (judging / "gold").mkdir()
        (judging / "gold" / "p-2.json").write_text(json.dumps({"qid": "p-2", "truthQuery": "run: t -> v"}))
        (judging / "cases.jsonl").write_text(
            json.dumps({"qid": "p-2", "question": "keyed", "source": "2", "golden": gold}) + "\n"
            + json.dumps({"qid": "p-3", "question": "Unverified", "source": "3",
                          "golden": {"kind": "scalar", "value": 1, "status": "proposed"}}) + "\n")
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            bs.main(["--set", str(s), "--label", "prod-1"])
        cases = {c["question"].lower(): c for c in
                 (json.loads(l) for l in (s / "cases.jsonl").read_text().splitlines())}
        self.assertEqual(cases["keyed"]["golden"], gold)
        self.assertEqual(cases["keyed"]["sourceFields"]["keyFrom"], "judging:prod-j1:2")
        qid = cases["keyed"]["qid"]
        self.assertEqual(json.loads((s / "gold" / f"{qid}.json").read_text()),
                         {"qid": qid, "truthQuery": "run: t -> v"})
        self.assertNotIn("unverified", cases)  # judging drew it and found nothing to key
        self.assertNotIn("golden", cases["never judged"])
        self.assertIn("keys: 1 of 2 new cases", out.getvalue())
        self.assertIn("for the 1 without one", out.getvalue())
        self.assertIn("and 1 judging found no question to key in", out.getvalue())


if __name__ == "__main__":
    unittest.main()
