#!/usr/bin/env python3
"""Tests for the golden audit improve runs after an edit.

The audit is the gate this step is built around, and nothing exercised the call
itself: the verifier was invoked without the `--set` it declares required, so it
exited 2 every time and the acceptance check reported BLOCKED for every cluster
that made an edit. These tests run the real verifier through the real command
builder, so a missing required argument fails here rather than in a run."""
import argparse
import pathlib
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
import improve  # noqa: E402

VERIFIER = HERE.parent.parent / "eval-answer" / "scripts" / "verify_goldens.py"


class Invocation(unittest.TestCase):
    """The command must satisfy the verifier's own argument contract."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.set_dir = self.tmp / "set"
        self.set_dir.mkdir()
        (self.set_dir / "cases.jsonl").write_text("")
        self.art = self.tmp / "art"
        self.art.mkdir()
        self.a = argparse.Namespace(
            set_dir=self.set_dir, server_root=None, model_dir=self.tmp / "model",
            environment="samples", package="ecommerce",
            # An unroutable port: the verifier must get far enough to try, which
            # is what proves argument parsing succeeded.
            truth_publisher="http://127.0.0.1:9")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def capture(self):
        seen = {}

        def fake_run(cmd, **kw):
            seen["cmd"] = cmd
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        with mock.patch.object(improve.subprocess, "run", fake_run):
            improve.verify_goldens(self.a, self.art, "a diff")
        return seen["cmd"]

    def test_definitions_reaches_the_verifier_command(self):
        # The acceptance check can pass on a set with no truth package only if
        # the ledger gets as far as the verifier.
        self.a.definitions = pathlib.Path("/tmp/led.jsonl")
        cmd = self.capture()
        self.assertIn("--definitions", cmd)
        self.assertIn("/tmp/led.jsonl", cmd)

    def test_a_namespace_without_definitions_is_still_accepted(self):
        # Older callers and these fixtures build the Namespace by hand.
        self.assertNotIn("--definitions", self.capture())

    def test_the_verifier_accepts_the_arguments_improve_sends(self):
        r = improve.verify_goldens(self.a, self.art, "a diff")
        # Exit 2 is argparse refusing the command. Whatever else happens, the
        # arguments must be the ones the verifier declares.
        self.assertNotIn("could not run (exit 2)", r.get("why") or "")
        text = (self.art / "verify_goldens.txt").read_text()
        self.assertNotIn("the following arguments are required", text)

    def test_a_verifier_that_cannot_run_is_not_a_golden_failure(self):
        # Distinct signals: an unusable verifier says nothing about the goldens,
        # and reporting it as a golden problem sends someone to settle a key
        # that is fine.
        r = improve.verify_goldens(self.a, self.art, "a diff")
        if r.get("ran") is False:
            self.assertNotIn("clean", r)
            self.assertIn("why", r)

    def test_no_diff_skips_the_audit(self):
        r = improve.verify_goldens(self.a, self.art, "   ")
        self.assertEqual(r["ran"], False)
        self.assertIn("no edit", r["why"])

    def test_a_set_path_that_does_not_exist_returns_rather_than_crashes(self):
        # It used to run the verifier with cwd=set_dir, so a bad path raised
        # FileNotFoundError uncaught, AFTER the model edit, losing the receipts.
        a = argparse.Namespace(**vars(self.a))
        a.set_dir = self.tmp / "nowhere"
        r = improve.verify_goldens(a, self.art, "a diff")
        self.assertIn("ran", r)

    def stub_verifier(self, body: str) -> None:
        """A per-set verifier override, which verify_goldens() prefers."""
        p = self.set_dir / "verify_goldens.py"
        p.write_text("import sys\n" + body)

    def test_a_verifier_exiting_3_is_marked_could_not_run_not_unclean(self):
        # The discriminator the acceptance gate reads. A skip and a failure both
        # carry `ran: False`, and only one of them may proceed. 3 is the
        # verifier's own "did not run"; it must never read as a golden finding.
        self.stub_verifier("print('boom', file=sys.stderr)\nsys.exit(3)\n")
        r = improve.verify_goldens(self.a, self.art, "a diff")
        self.assertTrue(r.get("couldNotRun"))
        self.assertNotIn("clean", r)
        self.assertIn("exit 3", r["why"])

    def test_a_verifier_exiting_1_is_a_golden_finding_not_a_harness_failure(self):
        # The other side of the split: 1 IS evidence about the goldens.
        self.stub_verifier("print('drifted')\nsys.exit(1)\n")
        r = improve.verify_goldens(self.a, self.art, "a diff")
        self.assertTrue(r["ran"])
        self.assertFalse(r["clean"])
        self.assertFalse(r.get("couldNotRun"))

    def test_a_legitimate_skip_is_not_marked_could_not_run(self):
        # No edit means there is genuinely nothing to invalidate, so this one
        # must NOT block -- otherwise every no-op cluster fails the gate.
        r = improve.verify_goldens(self.a, self.art, "   ")
        self.assertFalse(r.get("couldNotRun"))

    def test_a_set_with_no_truth_package_is_unverified_not_clean(self):
        # The false green this closed. The verifier exited 0 with "nothing to
        # re-derive" on a set naming no truthPackage, so an acceptance gate
        # passed on a check that never happened -- and improve could not tell
        # that from a real pass. It exits 3 now: the server-free audits ran, no
        # golden was re-derived, and the reason has to reach the artifact
        # because improve's own `why` is generic.
        r = improve.verify_goldens(self.a, self.art, "a diff")
        self.assertTrue(r.get("couldNotRun"))
        self.assertNotIn("clean", r)
        self.assertIn("nothing to re-derive",
                      (self.art / "verify_goldens.txt").read_text())


class NoTruthServerNamed(unittest.TestCase):
    """`--truth-publisher` has no default, and unset must BLOCK, not pass.

    It defaulted to `http://localhost:4811` -- which `run_baseline.py` uses as
    its default `--publisher`, the server holding the model under test -- while
    the help text one line below said "the model under test cannot verify its
    own goldens, which is the whole point of the second server".
    `verify_goldens.py` removed the same default from its own `--publisher` for
    the same reason, and both callers kept it.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.set_dir = self.tmp / "set"
        self.set_dir.mkdir()
        (self.set_dir / "cases.jsonl").write_text("")
        (self.set_dir / "set.json").write_text('{"truthPackage": "t"}')
        self.art = self.tmp / "art"
        self.art.mkdir()
        self.a = argparse.Namespace(
            set_dir=self.set_dir, server_root=None, model_dir=self.tmp / "model",
            environment="samples", package="ecommerce", truth_publisher=None)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_truth_publisher_blocks_rather_than_passing(self):
        r = improve.verify_goldens(self.a, self.art, "a diff")
        self.assertTrue(r.get("couldNotRun"))
        self.assertNotEqual(r.get("clean"), True)

    def test_it_does_not_crash_after_the_edit(self):
        # `--publisher None` in the argv list is a TypeError inside
        # subprocess.run, and the excepts here catch OSError and a timeout, not
        # that -- so it would have raised AFTER the model edit, losing the
        # receipts this step exists to produce.
        r = improve.verify_goldens(self.a, self.art, "a diff")
        self.assertIsInstance(r, dict)

    def test_the_default_is_not_the_model_under_test(self):
        src = pathlib.Path(improve.__file__).read_text()
        i = src.index('"--truth-publisher"')
        self.assertNotIn("4811", src[i:i + 200])


class VerifierContract(unittest.TestCase):
    def test_the_verifier_still_requires_set(self):
        # If this ever stops being required, the fix above is no longer load
        # bearing and the comment on it should go.
        p = subprocess.run([sys.executable, str(VERIFIER)],
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 2)
        self.assertIn("--set", p.stderr)

    def test_exit_codes_are_distinguishable(self):
        # 2 for a bad invocation, and 0 or 1 for a real answer. improve.py
        # depends on that split to tell a harness failure from a golden one.
        p = subprocess.run([sys.executable, str(VERIFIER), "--help"],
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0)


class TruthEnvironment(unittest.TestCase):
    """The truth server is separate and names its environments independently.

    Passing the MODEL server's environment to the audit 404'd every case, the
    audit exited non-zero, and the acceptance check read that as "this edit
    may have invalidated a golden" -- so it BLOCKED every cluster and no edit
    could ever be accepted. run_baseline.py grew --truth-environment for
    exactly this; this caller had not.
    """

    SRC = (pathlib.Path(__file__).resolve().parent / "improve.py").read_text()

    def test_the_flag_exists(self):
        self.assertIn('ap.add_argument("--truth-environment"', self.SRC)

    def test_the_audit_is_given_the_truth_environment(self):
        self.assertIn('getattr(a, "truth_environment", None) or a.environment',
                      self.SRC)

    def test_it_falls_back_to_the_model_environment(self):
        # One server serving both is the ordinary local case, and it must keep
        # working without the new flag.
        import argparse as _a
        ns = _a.Namespace(environment="samples", truth_environment=None)
        self.assertEqual(
            getattr(ns, "truth_environment", None) or ns.environment, "samples")



class ServedAsVersions(unittest.TestCase):
    """The snapshot sync (`rsync --delete` onto the served folder) is safe only
    over a copy. Over published versions it serves nothing and deletes them,
    so it is refused."""

    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)

    def test_a_folder_of_versions_is_versions(self):
        served = self.root / "pkg"
        (served / "1.0.0").mkdir(parents=True)
        (served / "1.0.0" / "publisher.json").write_text("{}")
        self.assertTrue(improve.served_as_versions(served))

    def test_a_copy_is_not(self):
        served = self.root / "pkg"
        served.mkdir()
        (served / "publisher.json").write_text("{}")
        self.assertFalse(improve.served_as_versions(served))

    def test_a_watch_mount_is_not(self):
        source = self.root / "src"
        source.mkdir()
        served = self.root / "pkg"
        served.symlink_to(source, target_is_directory=True)
        self.assertFalse(improve.served_as_versions(served))

    def test_a_missing_folder_is_not(self):
        self.assertFalse(improve.served_as_versions(self.root / "nothing"))


class ServedVersion(unittest.TestCase):
    """The server says whether it serves the package as a published version."""

    def serve(self, body: bytes):
        import http.server
        import threading

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.shutdown)
        return f"http://127.0.0.1:{srv.server_port}"

    def test_a_version_is_named(self):
        base = self.serve(b'{"name": "ecommerce", "versionId": "1.0.0"}')
        self.assertEqual(improve.served_version(base, "e", "ecommerce"), "1.0.0")

    def test_a_watch_mounted_package_has_none(self):
        base = self.serve(b'{"name": "ecommerce"}')
        self.assertIsNone(improve.served_version(base, "e", "ecommerce"))

    def test_a_server_that_cannot_be_asked_is_none(self):
        self.assertIsNone(improve.served_version("http://127.0.0.1:9", "e", "p"))
        self.assertIsNone(improve.served_version(None, "e", "p"))


class ClusterMembers(unittest.TestCase):
    """The improver sees each case's key. A rows golden kept in
    `golden.path` used to reach it as no key at all."""

    def test_a_path_held_golden_reaches_the_improver_as_its_rows(self):
        set_dir = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, set_dir, True)
        (set_dir / "gold").mkdir()
        (set_dir / "gold" / "q1.csv").write_text("region,n\nWest,3\n")
        cases = {"q1": {"question": "how many?",
                        "golden": {"kind": "rows", "path": "gold/q1.csv",
                                   "rubric": "West has 3."}}}
        self.assertEqual(
            improve.cluster_members({"qids": ["q1"]}, cases, set_dir),
            [{"qid": "q1", "question": "how many?",
              "golden": [{"region": "West", "n": 3}],
              "rubric": "West has 3."}])

class CandidateDiff(unittest.TestCase):
    """A new file is part of the edit, and our own helper is not."""

    def setUp(self):
        self.repo = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.repo, True)
        for cmd in (["init", "-q"], ["config", "user.email", "t@t"],
                    ["config", "user.name", "t"]):
            subprocess.run(["git", *cmd], cwd=self.repo, check=True)
        (self.repo / "m.malloy").write_text("source: a is x\n")
        subprocess.run(["git", "add", "."], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-qm", "init"], cwd=self.repo, check=True)

    def test_a_new_skills_file_is_in_the_patch_and_the_helper_is_not(self):
        (self.repo / "skills" / "s").mkdir(parents=True)
        (self.repo / "skills" / "s" / "SKILL.md").write_text("NEW GUIDE\n")
        (self.repo / improve.SYNC_HELPER).write_text("#!/bin/bash\n")
        (self.repo / "m.malloy").write_text("source: a is y\n")
        diff = improve.candidate_diff(self.repo)
        self.assertIn("NEW GUIDE", diff)
        self.assertIn("source: a is y", diff)
        self.assertNotIn(improve.SYNC_HELPER, diff)
        self.assertIn("SKILL.md", improve.candidate_diff(self.repo, ("--stat",)))

    def test_only_the_helper_is_an_empty_diff(self):
        (self.repo / improve.SYNC_HELPER).write_text("#!/bin/bash\n")
        self.assertEqual(improve.candidate_diff(self.repo).strip(), "")


class Grants(unittest.TestCase):
    def test_the_tools_the_skill_names_are_granted(self):
        for t in ("get_skill", "get_status"):
            self.assertIn(f"mcp__publisher__{t}", improve.IMPROVE_TOOLS)


class ReloadReceipt(unittest.TestCase):
    """The reload helper prints the served sha: from the package's serving
    state, or from its `/status` entry on a server from before that route.
    A reload that fails prints its error instead, never the old sha."""

    PKG = "/api/v0/environments/env/packages/pkg"

    def run_reload(self, routes):
        import http.server
        import json
        import threading

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                path = self.path.split("?")[0]
                body = routes.get(path)
                code, body = body if isinstance(body, tuple) else (
                    200 if body is not None else 404, body)
                self.send_response(code)
                self.end_headers()
                self.wfile.write(json.dumps(body or {}).encode())

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            cmd = improve.RELOAD.format(
                rest_url=f"http://127.0.0.1:{server.server_port}",
                environment="env", package="pkg")
            return subprocess.run(["bash", "-c", cmd], capture_output=True,
                                  text=True, timeout=60).stdout.strip()
        finally:
            server.shutdown()

    def test_reads_the_serving_state(self):
        out = self.run_reload({
            self.PKG: {"name": "pkg"},
            "/api/v0/environments/env/packages/pkg/status":
                {"sourceContentSha": "abc"},
        })
        self.assertEqual(out, '"sourceContentSha":"abc"')

    def test_falls_back_to_the_status_entry_on_an_older_server(self):
        out = self.run_reload({
            self.PKG: {"name": "pkg"},
            "/api/v0/status": {"environments": [{"name": "env", "packages": [
                {"name": "pkg", "sourceContentSha": "old"}]}]},
        })
        self.assertEqual(out, '"sourceContentSha":"old"')

    def test_a_failed_reload_prints_its_error_and_no_sha(self):
        out = self.run_reload({
            self.PKG: (400, {"message": "bad.malloy: refused"}),
            "/api/v0/environments/env/packages/pkg/status":
                {"sourceContentSha": "stale"},
        })
        self.assertIn("reload failed (HTTP 400)", out)
        self.assertIn("bad.malloy: refused", out)
        self.assertNotIn("stale", out)


class WhatTheImproverIsToldAndWhatItsDiffIsCheckedFor(unittest.TestCase):
    """One cluster, with the agent faked as a function that edits the model."""

    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.model = self.root / "model"
        self.model.mkdir()
        (self.model / "m.malloy").write_text("source: orders is t extend {\n  measure: n is count()\n}\n")
        for cmd in (["init", "-q"], ["add", "."],
                    ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base"]):
            subprocess.run(["git", *cmd], cwd=self.model, check=True, capture_output=True)
        self.set = self.root / "set"
        self.set.mkdir()
        self.cases = {"q1": {"qid": "q1", "question": "net sales?",
                             "golden": {"kind": "value", "value": 464861.37}}}
        self.prompt = None

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def improve(self, added: str, prior=(), experiment=None):
        def agent(prompt, **kw):
            self.prompt = prompt
            self.helper = (self.model / improve.SYNC_HELPER).read_text()
            f = self.model / "m.malloy"
            f.write_text(f.read_text().replace("  measure: n", added + "\n  measure: n"))
            return types.SimpleNamespace(text="done", json={"edit": "doc"}, cost_usd=0.1,
                                         wall_seconds=1, attempts=1, error=None)
        a = argparse.Namespace(
            rest_url="http://x", environment="e", package="p", watch_mode=True, server_root=None,
            model_dir=self.model, set_dir=self.set, role_skills=[], roots=[], model="opus",
            mcp_url="http://x/mcp", max_turns=5, timeout=5, retries=0, isolate=True, force=False,
            prior=list(prior), experiment=experiment)
        a.keys, a.golden_queries = improve.golden_keys(self.cases, self.set)
        with mock.patch.object(improve, "spawn_agent", side_effect=agent), \
                mock.patch.object(improve, "verify_goldens", return_value={"ran": True, "clean": True}):
            return improve.improve_cluster({"issue_id": "r:c1", "qids": ["q1"]}, self.cases, a,
                                       self.root / "run" / "artifacts")

    def test_an_edit_that_states_the_answer_is_reported(self):
        r = self.improve("  #(doc) Net sales, about 464,861 last year")
        self.assertEqual([(x["qid"], x["kind"]) for x in r["goldenLeak"]], [("q1", "value")])

    def test_an_honest_edit_is_not(self):
        r = self.improve("  #(doc) Net sales. Excludes returned lines.")
        self.assertEqual((r["goldenLeak"], r["duplicateOf"]), ([], None))
        self.assertTrue(r["patchSha"])

    def test_the_agent_is_shown_what_was_tried_and_a_repeat_of_a_rejected_edit_is_named(self):
        first = self.improve("  #(doc) Net sales. Excludes returned lines.")
        patch = (self.root / "run" / first["patchPath"]).read_text()
        shutil.rmtree(self.root / "run")
        rejected = improve.attempts.row(
            what="doc net sales", outcome="rejected", baseline="b", candidates=["a", "b2"],
            targets=["q1"], failed={"no regression": "keep in a"},
            effect={"targets": 1, "targetsFixed": 1, "regressions": ["keep in a"]}, diff=patch)
        r = self.improve("  #(doc) Net sales. Excludes returned lines.", prior=[rejected])
        self.assertIn("REJECTED", self.prompt)
        self.assertIn("rejected because: no regression: keep in a", self.prompt)
        self.assertEqual(r["duplicateOf"]["what"], "doc net sales")

    def test_with_an_experiment_the_agents_helper_republishes_the_working_tree(self):
        self.improve("  #(doc) Net sales.", experiment="imp1")
        self.assertIn("experiment.py", self.helper)
        self.assertIn(f"sync imp1 --set {self.set.resolve()} --dir {self.model.resolve()}",
                      self.helper)
        self.assertNotIn("reload=true", self.helper)
        # and without one it is the reload, as before
        shutil.rmtree(self.root / "run")
        self.improve("  #(doc) Net sales, again.")
        self.assertIn("reload=true", self.helper)
        self.assertNotIn("experiment.py", self.helper)

    def test_with_nothing_tried_the_prompt_says_so(self):
        self.improve("  #(doc) Net sales.")
        self.assertIn("(none recorded for this set)", self.prompt)


if __name__ == "__main__":
    unittest.main()
