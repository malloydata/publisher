"""build_set.py: a new set version from judged production history."""
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
import build_set as bs  # noqa: E402
import config  # noqa: E402
import optimization_package as op  # noqa: E402


def t(tid, q, kind="question", at="2026-11-20T00:00:00Z"):
    return {"turn_id": tid, "question": q, "kind": kind, "at": at, "package": "p"}


def j(tid, right, code=None, group=None, judged=None, deps=None):
    return {"turn_id": tid, "right": right, "failure_code": code, "question_group": group,
            "judged_turn": judged or tid, "depends_on": json.dumps(deps) if deps else ""}


GOLD = {"kind": "scalar", "value": 7, "status": "verified"}


def keys(*tids, earlier=None):
    return {x: ("prod-j1", GOLD, pathlib.Path("/nowhere"),
                {"context": {"earlierMessages": earlier or []}}) for x in tids}


class Choose(unittest.TestCase):
    def test_failures_spread_across_codes_then_as_many_controls_spread_across_sources(self):
        turns = [t(f"w{i}", f"wrong {i}") for i in range(6)] + [t(f"r{i}", f"right {i}") for i in range(8)]
        judged = ([j(f"w{i}", "False", "MISSING_DOC") for i in range(4)]
                  + [j("w4", "False", "BAD_JOIN"), j("w5", "False", "AMBIGUOUS")]
                  + [j(f"r{i}", "True") for i in range(8)])
        sources = {f"r{i}": "orders" if i < 6 else "users" for i in range(8)}
        k = keys(*(x["turn_id"] for x in judged))
        got, left = bs.choose(bs.groups(turns, judged, k, sources), k, 8)
        roles = [(g["role"], sorted(g["codes"]) or g.get("source")) for g in got]
        fails = [r for r in roles if r[0] == "failure"]
        self.assertEqual(len(fails), 4)
        # All three codes appear before the common one takes a second slot.
        self.assertEqual({c[0] for _, c in fails[:3]}, {"MISSING_DOC", "BAD_JOIN", "AMBIGUOUS"})
        controls = [r for r in roles if r[0] == "control"]
        self.assertEqual(len(controls), 4)
        self.assertIn("users", {s for _, s in controls[:2]})
        self.assertEqual(left, {})

    def test_at_least_five_controls_and_unkeyed_or_undecided_groups_are_counted_not_used(self):
        turns = [t("w", "wrong")] + [t(f"r{i}", f"right {i}") for i in range(7)] + [t("u", "open"), t("n", "nokey")]
        judged = [j("w", "False", "X")] + [j(f"r{i}", "True") for i in range(7)] + [j("u", ""), j("n", "True")]
        k = keys("w", "u", *(f"r{i}" for i in range(7)))
        got, left = bs.choose(bs.groups(turns, judged, k, {}), k, 100)
        self.assertEqual(sum(g["role"] == "control" for g in got), 5)
        self.assertEqual(left, {"no verified key": 1, "judged, undecided": 1})

    def test_a_group_right_one_day_and_wrong_another_is_a_failure_tagged_mixed(self):
        turns = [t("a", "brand revenue"), t("b", "Brand  revenue")]
        judged = [j("a", "True", group="g"), j("b", "False", "AMBIGUOUS", group="g")]
        k = keys("a")
        got, _ = bs.choose(bs.groups(turns, judged, k, {}), k, 10)
        c = bs.case(got[0], "v1", k)
        self.assertEqual(c["tags"], ["production", "failure", "failure:AMBIGUOUS", "mixed"])
        self.assertEqual(c["sourceFields"]["asked"], 2)

    def test_a_follow_up_carries_its_earlier_questions_and_long_or_build_chains_are_left_out(self):
        turns = [t("f", "all summers"), t("g", "and by week"), t("h", "now chart it")]
        judged = [j("f", "True", deps=["ray ban sales"]), j("g", "True", deps=["a", "b", "c", "d"]),
                  j("h", "True", deps=["add a dashboard"])]
        k = {**keys("f", "g"), **keys("h", earlier=[{"question": "add a dashboard", "kind": "build"}])}
        got, left = bs.choose(bs.groups(turns, judged, k, {}), k, 10)
        self.assertEqual([g["question"] for g in got], ["all summers"])
        c = bs.case(got[0], "v1", k)
        self.assertEqual(c["context"], {"priorQuestions": ["ray ban sales"]})
        self.assertIn("follow-up", c["tags"])
        self.assertNotEqual(c["qid"], bs.qid_for("all summers"))
        self.assertEqual(left, {"a follow-up of more than 3 earlier questions": 1,
                                "a follow-up of a build request": 1})


class Main(unittest.TestCase):
    def test_a_new_version_in_the_optimization_package_with_its_keys_and_workload(self):
        root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        s = root / "set"
        s.mkdir()
        (s / "set.json").write_text('{"name": "s", "truthPackage": "s-truth"}')
        (s / "eval.toml").write_text(f'[paths]\nworkdir = "{root / "w"}"\n[model]\npackage = "p"\nenvironment = "e"\n')
        (s / "attempts.jsonl").write_text('{"cluster": "c1"}\n')
        data = op.install_for(config.load(s))
        op.append(data / "prod_turns.csv", op.APPENDED["prod_turns"][2],
                  [t("1", "Keyed"), t("2", "keyed"), t("3", "Edit it", kind="build")])
        op.append(data / "prod_judgments.csv", op.APPENDED["prod_judgments"][2],
                  [{"turn_id": "2", "right": "False", "failure_code": "X", "judged_turn": "2"}])
        q = op.APPENDED["prod_queries"][2]
        op.append(data / "prod_queries.csv", q, [
            {"execution_id": "x1", "package": "p", "query_text": "run: a -> v", "model_path": "m.malloy", "status": "ok"},
            {"execution_id": "x2", "package": "p", "query_text": "run: a -> v", "model_path": "m.malloy", "status": "ok"},
            {"execution_id": "x3", "package": "p", "query_text": "run: bad", "model_path": "m.malloy", "status": "compile_error"}])
        judging = root / "w" / "judging" / "prod-j1"
        (judging / "gold").mkdir(parents=True)
        (judging / "gold" / "p-2.json").write_text(json.dumps({"qid": "p-2", "truthQuery": "run: t -> v"}))
        (judging / "cases.jsonl").write_text(
            json.dumps({"qid": "p-2", "question": "keyed", "source": "2", "golden": GOLD}) + "\n")
        with unittest.mock.patch("sys.stdout", new_callable=io.StringIO) as out:
            bs.main(["--set", str(s), "--label", "v1"])
        v = root / "w" / "packages" / "optimization-s" / "sets" / "v1"
        cases = [json.loads(l) for l in (v / "cases.jsonl").read_text().splitlines()]
        self.assertEqual([(c["question"], c["golden"], c["sourceFields"]["keyFrom"]) for c in cases],
                         [("keyed", GOLD, "judging:prod-j1:2")])
        qid = cases[0]["qid"]
        self.assertEqual(json.loads((v / "gold" / f"{qid}.json").read_text()),
                         {"qid": qid, "truthQuery": "run: t -> v"})
        wl = [json.loads(l) for l in (v / "workloads" / "v1.jsonl").read_text().splitlines()]
        self.assertEqual(wl, [{"query": "run: a -> v", "modelPath": "m.malloy", "count": 2}])
        # Runs against the version use the same work directory and package,
        # and the version shares one record of what was tried.
        vc = config.load(v)
        self.assertEqual((vc.workdir(), vc.optimization_dir(), vc.set_name),
                         (config.load(s).workdir(), config.load(s).optimization_dir(), "s"))
        meta = json.loads((v / "set.json").read_text())
        self.assertEqual((meta["version"], meta["truthPackage"]), ("v1", "s-truth"))
        import attempts
        self.assertEqual(attempts.read(v), [{"cluster": "c1"}])
        self.assertFalse((s / "cases.jsonl").exists())
        self.assertIn("set version v1: 1 cases (1 failures across 1 failure codes, 0 controls", out.getvalue())
        with self.assertRaises(SystemExit):
            bs.main(["--set", str(s), "--label", "v1"])


if __name__ == "__main__":
    unittest.main()
