#!/usr/bin/env python3
"""Tests for publish.py and experiment.py, with the server's replies stubbed."""
import json
import os
import pathlib
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import experiment  # noqa: E402
import publish  # noqa: E402


def replies(table):
    """A stub for publish.send: (method, path prefix) -> (status, body)."""
    def send(base, method, path, body=None, timeout=300):
        for (m, prefix), reply in table.items():
            if m == method and path.startswith(prefix):
                return reply
        return 200, {}
    return send


class Publish(unittest.TestCase):
    def setUp(self):
        self.pkg = pathlib.Path(tempfile.mkdtemp())
        (self.pkg / "publisher.json").write_text('{"name": "p", "version": "1.2.0"}')

    def tearDown(self):
        shutil.rmtree(self.pkg, ignore_errors=True)

    def test_a_changed_package_under_a_published_version_names_the_fix(self):
        table = {("POST", "api/v0/environments/e/packages"):
                 (409, {"code": "VERSION_CONFLICT", "message": "x"})}
        with mock.patch.object(publish, "send", side_effect=replies(table)):
            with self.assertRaises(SystemExit) as e:
                publish.publish("http://s", "e", "p", self.pkg, versioned=True)
        self.assertIn("bump `version`", str(e.exception))
        self.assertIn("1.2.0", str(e.exception))

    def test_a_package_serving_an_older_model_is_refused(self):
        table = {("GET", "api/v0/status"): (200, {"loadErrors": [
            {"environment": "e", "package": "p", "stale": True, "message": "line 3: oops"}]})}
        with mock.patch.object(publish, "send", side_effect=replies(table)):
            with self.assertRaises(SystemExit) as e:
                publish.publish("http://s", "e", "p", self.pkg, versioned=False)
        self.assertIn("older model", str(e.exception))

    def test_nothing_to_build_starts_no_build(self):
        calls = []
        def send(base, method, path, body=None, timeout=300):
            calls.append((method, path))
            return 200, {"sources": {}, "refusedSources": {}}
        with mock.patch.object(publish, "send", side_effect=send):
            self.assertIsNone(publish.build("http://s", "e", "p", None))
        self.assertFalse(any(m == "POST" for m, _ in calls))

    def test_an_experiment_package_name_holds_no_double_dash(self):
        # Publisher refuses `--` in a DuckDB file path, and the served directory is
        # named after the package.
        self.assertEqual(publish.experiment_package("nyc-taxi", "tip-fix"), "nyc-taxi.tip-fix")


class Experiment(unittest.TestCase):
    def test_names_that_would_put_a_double_dash_in_a_path_are_refused(self):
        cfg = mock.Mock()
        for bad in ("a--b", "-a", "a b", "a__b"):
            with self.subTest(bad), self.assertRaises(SystemExit):
                experiment.start(cfg, bad, None, None, build=False)

    def test_a_snapshot_copies_small_files_and_links_data(self):
        src, dst = pathlib.Path(tempfile.mkdtemp()), pathlib.Path(tempfile.mkdtemp()) / "x"
        try:
            (src / "m.malloy").write_text("source: s is duckdb.table('data/t.parquet')")
            (src / "data").mkdir()
            (src / "data" / "t.parquet").write_bytes(b"0" * (experiment.LINK_OVER_BYTES + 1))
            experiment.snapshot(src, dst)
            self.assertFalse((dst / "m.malloy").is_symlink())
            self.assertTrue((dst / "data" / "t.parquet").is_symlink())
            self.assertEqual(os.readlink(dst / "data" / "t.parquet"),
                             str((src / "data" / "t.parquet").resolve()))
        finally:
            shutil.rmtree(src, ignore_errors=True)
            shutil.rmtree(dst.parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
