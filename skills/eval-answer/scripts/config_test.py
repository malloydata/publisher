#!/usr/bin/env python3
"""Tests for config.py: where each setting comes from, and what a gap says."""
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import config  # noqa: E402

TOUR = config.CLONE / "examples" / "storefront" / "evals" / "storefront-tour"


def make_set(files: dict[str, str]) -> pathlib.Path:
    d = pathlib.Path(tempfile.mkdtemp(prefix="config-test-")) / "myset"
    d.mkdir()
    for name, text in files.items():
        (d / name).write_text(text)
    return d


class Precedence(unittest.TestCase):
    def test_a_flag_beats_the_file(self):
        cfg = config.load(make_set({"eval.toml": '[model]\nenvironment = "file"\n'}))
        self.assertEqual(cfg.need("flag", "model", "environment", "--environment"),
                         "flag")

    def test_the_file_beats_set_json(self):
        cfg = config.load(make_set({
            "eval.toml": '[model]\npackage = "from-file"\n',
            "set.json": json.dumps({"targetPackage": "from-set"})}))
        self.assertEqual(cfg.get("model", "package"), "from-file")

    def test_set_json_supplies_the_package_when_the_file_does_not(self):
        cfg = config.load(make_set({"set.json": json.dumps({"targetPackage": "p"})}))
        self.assertEqual(cfg.get("model", "package"), "p")

    def test_ports_have_built_ins_and_urls_derive_from_them(self):
        cfg = config.load(make_set({"eval.toml": "[model]\nport = 4000\n"}))
        self.assertEqual(cfg.model_publisher(), "http://localhost:4000")
        self.assertEqual(cfg.model_mcp_url(), "http://localhost:4040/mcp")
        self.assertEqual(cfg.truth_publisher(), "http://localhost:4881")


class NoExampleDefault(unittest.TestCase):
    def test_a_missing_environment_names_the_key_and_the_flag(self):
        d = make_set({"set.json": json.dumps({"name": "tour"})})
        with self.assertRaises(config.ConfigError) as e:
            config.load(d).need(None, "model", "environment", "--environment")
        self.assertEqual(
            str(e.exception),
            "No model environment for set 'tour'. Fix: add `environment = "
            f"\"<value>\"` under [model] in {d.resolve() / 'eval.toml'}, or "
            "pass --environment.")

    def test_no_file_means_no_truth_server(self):
        # Without a file nothing wrote a truth server's config, so there is no
        # truth server to assume; callers keep their own fallbacks.
        cfg = config.load(make_set({}))
        self.assertIsNone(cfg.truth_publisher())
        self.assertIsNone(cfg.get("truth", "environment"))


class Paths(unittest.TestCase):
    def test_relative_paths_resolve_against_the_file(self):
        d = make_set({"eval.toml": '[model]\nrepo = "../pkg"\n'})
        self.assertEqual(config.load(d).get("model", "repo"),
                         (d.parent / "pkg").resolve())

    def test_the_workdir_is_outside_the_repository_by_default(self):
        cfg = config.load(make_set({"set.json": json.dumps({"name": "tour"})}))
        self.assertEqual(cfg.workdir(),
                         pathlib.Path.home() / ".malloy-eval" / "tour")


class Formats(unittest.TestCase):
    def test_eval_json_is_read_the_same_way(self):
        d = make_set({"eval.json": json.dumps({"model": {"environment": "e"}})})
        self.assertEqual(config.load(d).get("model", "environment"), "e")

    def test_toml_without_tomllib_says_how_to_fix_it(self):
        d = make_set({"eval.toml": '[model]\nenvironment = "e"\n'})
        with mock.patch.object(config, "tomllib", None):
            with self.assertRaises(config.ConfigError) as e:
                config.load(d)
        self.assertIn("needs Python 3.11 or newer", str(e.exception))
        self.assertIn("eval.json", str(e.exception))

    def test_both_files_is_refused(self):
        with self.assertRaises(config.ConfigError):
            config.load(make_set({"eval.toml": "", "eval.json": "{}"}))

    def test_an_unknown_key_is_refused(self):
        # A guard bypass is not a key, so it cannot be switched on from a file.
        with self.assertRaises(config.ConfigError) as e:
            config.load(make_set({"eval.toml": "[model]\nskip_golden_check = true\n"}))
        self.assertIn("'skip_golden_check' under [model]", str(e.exception))

    def test_a_wrong_type_is_refused(self):
        with self.assertRaises(config.ConfigError) as e:
            config.load(make_set({"eval.toml": '[model]\nport = "4000"\n'}))
        self.assertIn("expected an integer", str(e.exception))


class TourSet(unittest.TestCase):
    @unittest.skipIf(config.tomllib is None, "reading eval.toml needs 3.11+")
    def test_the_tour_config_points_at_things_that_exist(self):
        cfg = config.load(TOUR)
        self.assertTrue((cfg.get("model", "repo") / "publisher.json").exists())
        self.assertTrue(cfg.truth_package_dir().is_dir())
        self.assertNotEqual(cfg.model_publisher(), cfg.truth_publisher())


if __name__ == "__main__":
    unittest.main()
