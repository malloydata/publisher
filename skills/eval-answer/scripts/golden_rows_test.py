#!/usr/bin/env python3
"""Tests for golden_rows.py: reading a `rows` golden that lives in a file."""
import pathlib
import shutil
import tempfile
import unittest

import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import golden_rows  # noqa: E402


class LoadRows(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.set_dir = self.tmp / "myset"
        (self.set_dir / "gold").mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def csv(self, name, text):
        (self.set_dir / "gold" / name).write_text(text)
        return {"kind": "rows", "path": f"gold/{name}"}

    def test_rows_come_from_the_csv_with_numbers_as_numbers(self):
        g = self.csv("q1.csv", "region,total,share\nWest,12,0.5\nEast,7,0.25\n")
        self.assertEqual(
            golden_rows.load_rows(g, self.set_dir, "q1"),
            [{"region": "West", "total": 12, "share": 0.5},
             {"region": "East", "total": 7, "share": 0.25}])

    def test_an_identifier_with_leading_zeros_stays_text(self):
        g = self.csv("q1.csv", "code,n\n007,1\n")
        self.assertEqual(golden_rows.load_rows(g, self.set_dir, "q1"),
                         [{"code": "007", "n": 1}])

    def test_an_empty_cell_is_null(self):
        g = self.csv("q1.csv", "a,b\n1,\n")
        self.assertEqual(golden_rows.load_rows(g, self.set_dir, "q1"),
                         [{"a": 1, "b": None}])

    def test_true_and_false_become_booleans(self):
        g = self.csv("q1.csv", "id,active,note\n1,true,False\n2,false,truex\n")
        self.assertEqual(golden_rows.load_rows(g, self.set_dir, "q1"),
                         [{"id": 1, "active": True, "note": "False"},
                          {"id": 2, "active": False, "note": "truex"}])

    def test_value_wins_when_both_exist(self):
        g = self.csv("q1.csv", "a\n1\n")
        g["value"] = [{"a": 99}]
        self.assertEqual(golden_rows.load_rows(g, self.set_dir, "q1"),
                         [{"a": 99}])

    def test_neither_value_nor_path_is_none(self):
        self.assertIsNone(golden_rows.load_rows({"kind": "rows"},
                                                self.set_dir, "q1"))

    def test_a_missing_file_names_the_case_and_the_path(self):
        with self.assertRaises(golden_rows.GoldenRowsError) as cm:
            golden_rows.load_rows({"kind": "rows", "path": "gold/nope.csv"},
                                  self.set_dir, "q7")
        self.assertIn("q7", str(cm.exception))
        self.assertIn("gold/nope.csv", str(cm.exception))

    def test_a_path_outside_the_set_is_refused(self):
        (self.tmp / "secret.csv").write_text("a\n1\n")
        for bad in ("../secret.csv", str(self.tmp / "secret.csv")):
            with self.subTest(bad=bad):
                with self.assertRaises(golden_rows.GoldenRowsError) as cm:
                    golden_rows.load_rows({"kind": "rows", "path": bad},
                                          self.set_dir, "q1")
                self.assertIn("outside", str(cm.exception))
                self.assertIn("q1", str(cm.exception))

    def test_a_symlink_out_of_the_set_is_refused(self):
        (self.tmp / "secret.csv").write_text("a\n1\n")
        (self.set_dir / "gold" / "link.csv").symlink_to(self.tmp / "secret.csv")
        with self.assertRaises(golden_rows.GoldenRowsError):
            golden_rows.load_rows({"kind": "rows", "path": "gold/link.csv"},
                                  self.set_dir, "q1")


class Render(unittest.TestCase):
    def test_a_short_list_renders_whole(self):
        text = golden_rows.render([{"a": 1}], total=1)
        self.assertEqual(text, '[{"a": 1}]')

    def test_a_long_list_is_cut_and_says_so(self):
        rows = [{"a": i} for i in range(golden_rows.ROWS_CAP + 5)]
        text = golden_rows.render(rows, total=len(rows))
        self.assertIn(f"first {golden_rows.ROWS_CAP} of {len(rows)} rows", text)
        self.assertNotIn(f'"a": {golden_rows.ROWS_CAP + 1}}}', text)


if __name__ == "__main__":
    unittest.main()
