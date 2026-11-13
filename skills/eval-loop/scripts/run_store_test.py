#!/usr/bin/env python3
"""Tests for run_store.py: a run is shared whole, once, and comes back the same."""
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import config  # noqa: E402
import run_store  # noqa: E402


class Store(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.set = self.root / "set"
        self.set.mkdir()
        (self.set / "set.json").write_text('{"name": "s"}')
        (self.set / "cases.jsonl").write_text('{"qid": "a"}\n{"qid": "b"}\n')

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def cfg(self, store: str) -> config.Config:
        (self.set / "eval.toml").write_text(f'[paths]\nworkdir = "{self.root / "w"}"\n{store}')
        return config.load(self.set)

    def make_run(self, cfg, name, status="complete", qids=("a", "b"), **meta):
        d = cfg.workdir() / "runs" / name
        d.mkdir(parents=True)
        (d / "run.json").write_text(json.dumps({"status": status, "package": "p", **meta}))
        (d / "events.jsonl").write_text("".join(
            json.dumps({"kind": "attempt", "qid": q}) + "\n" for q in qids))
        return d

    def test_a_folder_store_round_trips_a_run_and_never_overwrites_one(self):
        cfg = self.cfg(f'[store]\npath = "{self.root / "store"}"\n')
        r = self.make_run(cfg, "base")
        self.assertIn("pushed", run_store.push(cfg, r))
        self.assertIn("already in the store", run_store.push(cfg, r))
        (r / "events.jsonl").write_text("changed\n")
        with self.assertRaises(SystemExit) as e:
            run_store.push(cfg, r)
        self.assertIn("never changes", str(e.exception))
        shutil.rmtree(r)
        self.assertEqual(run_store.sync(cfg, quiet=True), ["base"])
        self.assertTrue((cfg.workdir() / "runs" / "base" / "run.json").exists())

    def test_an_unfinished_run_is_not_shared(self):
        cfg = self.cfg(f'[store]\npath = "{self.root / "store"}"\n')
        with self.assertRaises(SystemExit) as e:
            run_store.push(cfg, self.make_run(cfg, "half", status="running"))
        self.assertIn("not complete", str(e.exception))

    def test_command_templates_are_filled_and_run(self):
        remote = self.root / "remote"
        cfg = self.cfg('[store]\n'
                       f'push = "mkdir -p {remote} && cp -R {{run}} {remote}/{{run_id}}"\n'
                       f'pull = "cp -R {remote}/{{run_id}} {{dest}}"\n'
                       f'list = "ls {remote}"\n')
        r = self.make_run(cfg, "base")
        run_store.push(cfg, r)
        self.assertEqual(run_store.listed(cfg), ["base"])
        shutil.rmtree(r)
        run_store.pull(cfg, "base")
        self.assertTrue((cfg.workdir() / "runs" / "base" / "run.json").exists())

    def test_what_shares_itself(self):
        full = {"status": "complete", "package": "p"}
        self.assertTrue(run_store.is_record(full, 2, 2))
        self.assertFalse(run_store.is_record(full, 2, 1))                         # partial
        self.assertFalse(run_store.is_record({**full, "slice": {"name": "x"}}, 2, 2))
        self.assertFalse(run_store.is_record({**full, "package": "p.exp"}, 2, 2, "p"))  # experiment
        self.assertTrue(run_store.is_record({**full, "phase": "query-replay"}, 2, None))

    def test_no_store_means_nothing_to_sync(self):
        self.assertEqual(run_store.sync(self.cfg("")), [])

    def test_a_command_store_with_no_list_says_so_instead_of_holding_nothing(self):
        import contextlib
        import io
        cfg = self.cfg('[store]\npush = "true"\npull = "true"\n')
        self.assertFalse(run_store.can_list(cfg))
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(run_store.sync(cfg), [])
        self.assertIn("no list command", err.getvalue())
        with self.assertRaises(SystemExit) as e:
            run_store.main(["pull", "--set", str(self.set)])
        self.assertIn("no list command", str(e.exception))

    def test_a_single_brace_in_a_command_is_named_with_the_fix(self):
        cfg = self.cfg("[store]\npush = \"true\"\nlist = \"ls | awk '{print $2}'\"\n")
        with self.assertRaises(SystemExit) as e:
            run_store.listed(cfg)
        self.assertIn("double a literal brace", str(e.exception))

    def test_the_documented_list_command_fills_in(self):
        doc = (pathlib.Path(__file__).resolve().parent.parent / "reference"
               / "sharing-runs.md").read_text()
        line = next(x for x in doc.splitlines() if x.startswith("list = "))
        template = line[len('list = "'):-1]
        self.assertIn("awk '{print $2}'", template.format(set="s"))


if __name__ == "__main__":
    unittest.main()
