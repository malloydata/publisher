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
import unittest

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

    def test_a_set_with_no_truth_package_reads_as_clean(self):
        # Documented, not endorsed: the verifier exits 0 with "nothing to
        # re-derive" when it cannot find a truth package, so the audit passes
        # without having checked anything. Improve cannot tell that from a real
        # pass. Worth closing in the verifier, where the contract lives.
        r = improve.verify_goldens(self.a, self.art, "a diff")
        self.assertTrue(r.get("clean"))
        self.assertIn("nothing to re-derive",
                      (self.art / "verify_goldens.txt").read_text())


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


if __name__ == "__main__":
    unittest.main()
