#!/usr/bin/env python3
"""Tests for slices.py: resolution, refusals, and that a slice never moves datasetSha."""
import argparse
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import ledger  # noqa: E402
import slices  # noqa: E402

CASES = [{"qid": "a", "tags": ["join"]}, {"qid": "b", "tags": ["rank", "join"]},
         {"qid": "c", "tags": ["time"]}, {"qid": "d"}]


class Resolve(unittest.TestCase):
    def setUp(self):
        self.d = pathlib.Path(tempfile.mkdtemp())
        (self.d / "set.json").write_text('{"name": "s", "datasetVersion": 1}')
        (self.d / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in CASES))
        (self.d / "slices.json").write_text(json.dumps({
            "joins": {"tags": ["join"]}, "smoke": {"qids": ["d", "c"]},
            "mixed": {"tags": ["rank"], "qids": ["c"], "description": "x"}}))

    def test_neither_flag_is_none(self):
        self.assertIsNone(slices.resolve(self.d, CASES))

    def test_tags_match_any_and_qids_are_added_in_file_order(self):
        self.assertEqual(slices.resolve(self.d, CASES, "joins")["qids"], ["a", "b"])
        self.assertEqual(slices.resolve(self.d, CASES, "smoke")["qids"], ["c", "d"])
        self.assertEqual(slices.resolve(self.d, CASES, "mixed")["qids"], ["b", "c"])
        self.assertEqual(slices.resolve(self.d, CASES, None, ["time", "rank"])["qids"], ["b", "c"])

    def test_a_tag_flag_widens_a_slice(self):
        got = slices.resolve(self.d, CASES, "smoke", ["join"])
        self.assertEqual((got["qids"], got["tags"]), (["a", "b", "c", "d"], ["join"]))

    def test_refusals_name_the_fix(self):
        for args, msg in [(("nope",), "Slices: joins, mixed, smoke"),
                          ((None, ["missing"]), "selects no case")]:
            with self.subTest(args=args), self.assertRaises(SystemExit) as e:
                slices.resolve(self.d, CASES, *args)
            self.assertIn(msg, str(e.exception))
        (self.d / "slices.json").write_text('{"bad": {"qids": ["zz"]}}')
        with self.assertRaises(SystemExit) as e:
            slices.resolve(self.d, CASES, "bad")
        self.assertIn("names qid(s) not in cases.jsonl: zz", str(e.exception))

    def test_a_malformed_file_is_refused(self):
        for body, msg in [('{"s": {}}', "expected"), ('{"s": {"tags": "join"}}', "list of strings"),
                          ('{"s": {"tags": ["a"], "split": "dev"}}', "unknown key")]:
            (self.d / "slices.json").write_text(body)
            with self.subTest(body=body), self.assertRaises(SystemExit) as e:
                slices.load(self.d)
            self.assertIn(msg, str(e.exception))

    def test_editing_slices_does_not_move_dataset_sha(self):
        before = ledger.dataset_sha(self.d)
        (self.d / "slices.json").write_text('{"other": {"tags": ["time"]}}')
        self.assertEqual(ledger.dataset_sha(self.d), before)


class Narrow(unittest.TestCase):
    def setUp(self):
        self.d = pathlib.Path(tempfile.mkdtemp())
        (self.d / "slices.json").write_text('{"joins": {"tags": ["join"]}}')

    def test_writes_the_existing_filter(self):
        a = argparse.Namespace(slice="joins", tag=None, only=None)
        slices.narrow(a, self.d, CASES)
        self.assertEqual(a.only, "a,b")
        a = argparse.Namespace(slice=None, tag=["time"], only=None)
        slices.narrow(a, self.d, CASES, joined=False)
        self.assertEqual(a.only, ["c"])

    def test_refuses_two_filters(self):
        a = argparse.Namespace(slice="joins", tag=None, only="a")
        with self.assertRaises(SystemExit) as e:
            slices.narrow(a, self.d, CASES)
        self.assertIn("pass one of them", str(e.exception))

    def test_no_flag_leaves_only_alone(self):
        a = argparse.Namespace(slice=None, tag=None, only="a")
        self.assertIsNone(slices.narrow(a, self.d, CASES))
        self.assertEqual(a.only, "a")


if __name__ == "__main__":
    unittest.main()
