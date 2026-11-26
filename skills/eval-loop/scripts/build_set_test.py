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


if __name__ == "__main__":
    unittest.main()
