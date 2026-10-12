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

    def test_a_row_longer_than_the_header_is_a_golden_rows_error(self):
        g = self.csv("q1.csv", "region,total\nWest,12,EXTRA\n")
        with self.assertRaises(golden_rows.GoldenRowsError) as cm:
            golden_rows.load_rows(g, self.set_dir, "q1")
        self.assertEqual(
            str(cm.exception),
            "q1: golden.path 'gold/q1.csv' line 2 has a different number of "
            "fields than the header (2); fix the row")

    def test_a_row_shorter_than_the_header_is_a_golden_rows_error(self):
        g = self.csv("q1.csv", "region,total\nWest,12\nEast\n")
        with self.assertRaises(golden_rows.GoldenRowsError) as cm:
            golden_rows.load_rows(g, self.set_dir, "q1")
        self.assertIn("line 3", str(cm.exception))

    def test_an_empty_file_is_a_golden_rows_error(self):
        g = self.csv("q1.csv", "")
        with self.assertRaises(golden_rows.GoldenRowsError) as cm:
            golden_rows.load_rows(g, self.set_dir, "q1")
        self.assertEqual(
            str(cm.exception),
            "q1: golden.path 'gold/q1.csv' has no header row; the first line "
            "must name the columns")

    def test_a_header_with_no_rows_is_a_golden_rows_error(self):
        g = self.csv("q1.csv", "region,total\n")
        with self.assertRaises(golden_rows.GoldenRowsError) as cm:
            golden_rows.load_rows(g, self.set_dir, "q1")
        self.assertEqual(
            str(cm.exception),
            "q1: golden.path 'gold/q1.csv' has no data rows; re-export it, or "
            "write `value: []` on the golden if the answer is no rows")

    def test_an_empty_json_array_is_a_golden_rows_error(self):
        g = self.csv("q1.json", "[]")
        with self.assertRaises(golden_rows.GoldenRowsError) as cm:
            golden_rows.load_rows(g, self.set_dir, "q1")
        self.assertIn("has no data rows", str(cm.exception))

    def test_a_blank_line_in_a_one_column_file_is_a_null(self):
        # DuckDB writes a one-column NULL as a blank line.
        g = self.csv("q1.csv", "region\nWest\n\nEast\n")
        self.assertEqual(golden_rows.load_rows(g, self.set_dir, "q1"),
                         [{"region": "West"}, {"region": None},
                          {"region": "East"}])

    def test_a_blank_line_in_a_wider_file_is_skipped(self):
        g = self.csv("q1.csv", "a,b\n1,2\n\n3,4\n")
        self.assertEqual(golden_rows.load_rows(g, self.set_dir, "q1"),
                         [{"a": 1, "b": 2}, {"a": 3, "b": 4}])

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


class KeyValue(unittest.TestCase):
    """What diagnose, improve and the rubric audit show as the key."""

    def setUp(self):
        self.set_dir = pathlib.Path(tempfile.mkdtemp())
        (self.set_dir / "gold").mkdir()
        (self.set_dir / "gold" / "q1.csv").write_text("a\n1\n")

    def tearDown(self):
        shutil.rmtree(self.set_dir, ignore_errors=True)

    def test_a_path_held_golden_gives_its_rows(self):
        self.assertEqual(golden_rows.key_value(
            {"kind": "rows", "path": "gold/q1.csv"}, self.set_dir, "q1"),
            [{"a": 1}])

    def test_a_value_golden_gives_its_value(self):
        self.assertEqual(golden_rows.key_value(
            {"kind": "scalar", "value": {"n": 3}}, self.set_dir, "q1"),
            {"n": 3})

    def test_an_unreadable_path_becomes_a_note(self):
        self.assertEqual(
            golden_rows.key_value_or_note(
                {"kind": "rows", "path": "gold/nope.csv"}, self.set_dir, "q1"),
            "(the golden's rows could not be read: q1: golden.path "
            f"'gold/nope.csv' does not exist under {self.set_dir.resolve()})")


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
