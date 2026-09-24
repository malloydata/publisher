#!/usr/bin/env python3
"""Tests for eval.py: how `--label` names the run a verb reads."""
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import eval as entry  # noqa: E402


def a_set() -> tuple[pathlib.Path, pathlib.Path]:
    d = pathlib.Path(tempfile.mkdtemp(prefix="eval-set-")).resolve()
    work = d / "work"
    (d / "eval.toml").write_text(f'[paths]\nworkdir = "{work}"\n')
    return d, work


class Label(unittest.TestCase):
    def test_a_label_becomes_the_run_under_the_workdir(self):
        d, work = a_set()
        for label in (["--label", "b1"], ["--label=b1"]):
            with self.subTest(label=label):
                got = entry.run_dir_from_label(["--set", str(d), *label])
                self.assertEqual(got, ["--set", str(d), "--run",
                                       str(work / "runs" / "b1")])

    def test_run_wins_and_the_label_is_dropped(self):
        d, _ = a_set()
        got = entry.run_dir_from_label(
            ["--set", str(d), "--label", "b1", "--run", "/r"])
        self.assertEqual(got, ["--set", str(d), "--run", "/r"])

    def test_an_empty_or_missing_label_is_refused(self):
        d, _ = a_set()
        for args in (["--label="], ["--label"]):
            with self.subTest(args=args):
                with self.assertRaises(SystemExit) as e:
                    entry.run_dir_from_label(["--set", str(d), *args])
                self.assertIn("Invalid --label: expected a run name",
                              str(e.exception))

    def test_no_label_passes_through(self):
        self.assertEqual(entry.run_dir_from_label(["--set", "s", "--run", "/r"]),
                         ["--set", "s", "--run", "/r"])


if __name__ == "__main__":
    unittest.main()
