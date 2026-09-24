#!/usr/bin/env python3
"""Tests for the set-name lint: ids and vetoes that name nothing in the model
under test. The rest of verify_goldens needs a live Publisher and is exercised
by running it."""
import hashlib
import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import verify_goldens  # noqa: E402
from verify_goldens import (  # noqa: E402
    check_value, close_enough, model_text, promotion_blocker,
    question_drift_findings, truth_isolation_findings,
    unknown_name_findings, verify)

MODEL = """
source: order_items is duckdb.table('data/order_items.parquet') extend {
  measure: total_sales is sale_price.sum()
  dimension: is_returned is status = 'Returned'
}
"""


def case(qid="q1", required=None, any_of=None, acceptable=None, must_not_use=None):
    return {"qid": qid,
            "expectedEntities": {k: v for k, v in
                                 (("required", required), ("requiredAnyOf", any_of),
                                  ("acceptable", acceptable)) if v},
            "golden": {"mustNotUse": must_not_use} if must_not_use else {}}


class RequiredIds(unittest.TestCase):
    def test_an_id_the_model_has_is_no_finding(self):
        f = unknown_name_findings(
            [case(required=["measure:order_items:total_sales"])], MODEL)
        self.assertEqual(f, [])

    def test_an_id_from_another_package_fails(self):
        # The real bug: five ids named after a sibling package scored as
        # retrieval misses on every run for two days.
        f = unknown_name_findings(
            [case(required=["measure:attribution_creative_linear:creative_name"])],
            MODEL)
        self.assertEqual(len(f), 1)
        self.assertFalse(f[0].startswith("review "))
        self.assertIn("creative_name", f[0])

    def test_a_right_field_under_a_wrong_source_still_fails(self):
        # The field name often survives a rename that the source name does not.
        f = unknown_name_findings(
            [case(required=["measure:attribution_creative_linear:total_sales"])],
            MODEL)
        self.assertEqual(len(f), 1)

    def test_a_two_part_id_is_checked_on_its_name(self):
        self.assertEqual(unknown_name_findings([case(required=["measure:total_sales"])],
                                               MODEL), [])
        self.assertEqual(len(unknown_name_findings(
            [case(required=["measure:no_such_measure"])], MODEL)), 1)

    def test_an_id_with_no_kind_prefix_is_a_finding(self):
        # `all(parts[1:])` is True on an empty slice, so a one-part id passed
        # the check written to catch exactly this shape, while the well-formed
        # `measure:x:no_such_field` was caught. Scoring compares whole ids, so a
        # prefix-less one can never match however real the field is.
        for bad in ("ecommerce.no_such_field", "no_such_field", ""):
            with self.subTest(bad=bad):
                f = unknown_name_findings([case(required=[bad])], MODEL)
                self.assertEqual(len(f), 1)
                self.assertFalse(f[0].startswith("review "))
                self.assertIn("kind:", f[0])

    def test_a_bare_name_the_model_does_have_is_still_a_finding(self):
        # `total_sales` IS in the model, and the id is still unusable: it is
        # the prefix that is missing, not the field. Checking the name alone
        # would let this one through and it would score as a miss every run.
        f = unknown_name_findings([case(required=["total_sales"])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertIn("kind:", f[0])


class RequiredAnyOf(unittest.TestCase):
    def test_a_group_passes_when_one_id_resolves(self):
        # This is the repair for a set scored against two package versions, so
        # it must not be flagged.
        f = unknown_name_findings([case(any_of=[[
            "measure:attribution_creative_linear:creative_name",
            "measure:order_items:total_sales"]])], MODEL)
        self.assertEqual(f, [])

    def test_a_group_where_none_resolves_fails(self):
        f = unknown_name_findings([case(any_of=[[
            "measure:gone:one", "measure:gone:two"]])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertFalse(f[0].startswith("review "))

    def test_a_malformed_entry_does_not_rescue_a_dead_group(self):
        # Worse here than in `required`: a group passes when ANY member
        # resolves, so one prefix-less id used to pass a group whose every
        # well-formed id names nothing.
        f = unknown_name_findings([case(any_of=[[
            "measure:gone:one", "no_such_field"]])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertFalse(f[0].startswith("review "))


class ReviewedNotFailed(unittest.TestCase):
    def test_an_unknown_acceptable_id_is_only_reviewed(self):
        f = unknown_name_findings([case(acceptable=["measure:gone:x"])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertTrue(f[0].startswith("review "))

    def test_a_dead_veto_is_only_reviewed(self):
        # It cannot move a number; it just protects nothing. And a model that
        # passes a raw column through without naming it would trip this.
        f = unknown_name_findings([case(must_not_use=["no_such_field"])], MODEL)
        self.assertEqual(len(f), 1)
        self.assertTrue(f[0].startswith("review "))

    def test_a_live_veto_is_no_finding(self):
        self.assertEqual(
            unknown_name_findings([case(must_not_use=["sale_price"])], MODEL), [])

    def test_prose_must_not_use_is_never_checked(self):
        # It names no field, so there is nothing to look for.
        self.assertEqual(unknown_name_findings(
            [case(must_not_use=["weekly_active_users as a cumulative series"])],
            MODEL), [])


class NoModel(unittest.TestCase):
    def test_without_model_text_the_lint_says_nothing(self):
        # A platform target has no local model text. Silence is right; claiming
        # every id is unknown would be worse than not checking.
        self.assertEqual(unknown_name_findings([case(required=["measure:a:b"])], ""), [])


class ModelText(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_a_directory_is_walked_recursively(self):
        (self.tmp / "dashboards").mkdir()
        (self.tmp / "a.malloy").write_text("source: one is x")
        (self.tmp / "dashboards" / "b.malloy").write_text("source: two is y")
        text = model_text(self.tmp)
        self.assertIn("one", text)
        self.assertIn("two", text)

    def test_a_missing_path_is_empty_not_an_error(self):
        self.assertEqual(model_text(self.tmp / "nope"), "")
        self.assertEqual(model_text(None), "")


class ExitCodes(unittest.TestCase):
    """The three-way signal improve.py's acceptance gate reads.

    An uncaught traceback exits 1 by default, and 1 is the code meaning "a
    golden drifted" -- so a missing cases.jsonl used to send someone to settle a
    golden that was fine. Anything unanticipated must land outside {0, 1}.
    """

    SCRIPT = pathlib.Path(__file__).resolve().parent / "verify_goldens.py"

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_it(self, *args):
        return subprocess.run(
            [sys.executable, str(self.SCRIPT), *args],
            capture_output=True, text=True, timeout=120)

    def test_a_crash_exits_3_not_1(self):
        # set.json present, cases.jsonl absent: the read that used to raise
        # FileNotFoundError straight through Python's default exit status.
        (self.tmp / "set.json").write_text('{"truthPackage": "x"}')
        p = self.run_it("--set", str(self.tmp),
                        "--publisher", "http://127.0.0.1:9")
        self.assertEqual(p.returncode, 3, p.stderr[-400:])
        self.assertIn("could not run", p.stderr)
        self.assertIn("says NOTHING about the goldens", p.stderr)

    def test_a_usage_error_still_exits_2(self):
        self.assertEqual(self.run_it().returncode, 2)

    def audit_set(self, required: str) -> pathlib.Path:
        """A set with no truthPackage and one entity id to audit."""
        (self.tmp / "set.json").write_text('{"name": "probe"}')
        (self.tmp / "cases.jsonl").write_text(json.dumps(
            {"qid": "q1", "expectedEntities": {"required": [required]}}) + "\n")
        model = self.tmp / "m.malloy"
        model.write_text(MODEL)
        return model

    def test_a_set_with_no_truth_package_exits_3_not_0(self):
        # 0 claimed "every golden re-derived, no findings" about a run that
        # re-derived nothing, and improve.py recorded it as `clean`. No
        # --publisher here on purpose: nothing is contacted.
        model = self.audit_set("measure:order_items:total_sales")
        p = self.run_it("--set", str(self.tmp), "--model", str(model))
        self.assertEqual(p.returncode, 3, p.stdout[-400:])
        self.assertIn("truthPackage", p.stdout)
        self.assertIn("do not read it as a pass", p.stderr)

    def test_the_audits_still_run_without_a_truth_package(self):
        # The whole point. Four checks need no server, including the set-name
        # lint, and the early return skipped all of them on exactly the set
        # whose names nobody had verified.
        model = self.audit_set("measure:other_package:creative_name")
        p = self.run_it("--set", str(self.tmp), "--model", str(model))
        self.assertIn("creative_name", p.stdout)

    def test_a_finding_without_a_truth_package_exits_1_not_3(self):
        # A finding outranks a skip: 3 tells the caller there is nothing here
        # to read, and a caller obeying that would discard the one fact this
        # run produced.
        model = self.audit_set("measure:other_package:creative_name")
        p = self.run_it("--set", str(self.tmp), "--model", str(model))
        self.assertEqual(p.returncode, 1, p.stdout[-400:])


class SkipShape(unittest.TestCase):
    """run_baseline.py calls verify() in process and never sees an exit code.

    It reads `tally` and `findings` on both paths, so one return shape has to
    carry both -- two shapes is what let the skip branch drop the findings.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_the_skip_shape_carries_a_tally_and_the_audit_findings(self):
        (self.tmp / "set.json").write_text('{"name": "probe"}')
        (self.tmp / "cases.jsonl").write_text(json.dumps(
            {"qid": "q1",
             "expectedEntities": {"required": ["measure:other_package:x"]}}) + "\n")
        model = self.tmp / "m.malloy"
        model.write_text(MODEL)
        # An unroutable publisher: an empty tally is also the proof that no
        # request went out, since a contacted-and-failed one tallies `error`.
        r = verify(self.tmp, "http://127.0.0.1:9", "samples",
                   model=model, quiet=True)
        self.assertTrue(r["skipped"])
        self.assertEqual(r["tally"], {})
        self.assertEqual(r["drifted"], 0)
        self.assertTrue([f for f in r["findings"]
                         if not f.startswith("review ")])

    def test_a_normal_run_reports_skipped_as_none(self):
        (self.tmp / "set.json").write_text('{"truthPackage": "truth"}')
        (self.tmp / "cases.jsonl").write_text("")
        r = verify(self.tmp, "http://127.0.0.1:9", "samples", quiet=True)
        self.assertIsNone(r["skipped"])


class RefreshNeverWritesAnEmptyResult(unittest.TestCase):
    """A zero-row answer is a failed measurement, not a new value.

    `rows is not None` let an empty result through, and a truth server that
    loaded nothing answers every query that way -- which is exactly the state a
    symlinked truth package produces on a clean clone. The refresh then emptied
    every rows-kind golden and raised IndexError on every scalar one, in the
    one command whose job is to repair them, on artifacts no re-run rebuilds.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        (self.tmp / "set.json").write_text(
            '{"name": "s", "truthPackage": "truth"}')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, golden):
        (self.tmp / "cases.jsonl").write_text(json.dumps(
            {"qid": "q1", "question": "x", "split": "dev",
             "golden": golden}) + "\n")

    def refresh(self, golden):
        self.write(golden)
        with unittest.mock.patch.object(
                verify_goldens, "check_value", return_value=("diff", "", [])):
            r = verify(self.tmp, "http://truth", "samples", refresh=True,
                       quiet=True)
        stored = json.loads((self.tmp / "cases.jsonl").read_text())
        return r, stored

    def test_a_rows_golden_is_not_emptied(self):
        r, stored = self.refresh(
            {"status": "verified", "kind": "rows",
             "value": [{"brand": "a", "n": 3}]})
        self.assertEqual(stored["golden"]["value"], [{"brand": "a", "n": 3}])
        self.assertEqual(r["refreshed"], [])
        self.assertTrue(any("zero rows" in f for f in r["findings"]),
                        r["findings"])

    def test_a_scalar_golden_does_not_crash_the_run(self):
        # `rows[0]` on an empty list raised IndexError, which the top-level
        # handler turns into exit 3 -- the audit reported as unable to run.
        r, stored = self.refresh(
            {"status": "verified", "kind": "scalar", "value": {"total": 42}})
        self.assertEqual(stored["golden"]["value"], {"total": 42})
        self.assertTrue(any("zero rows" in f for f in r["findings"]))

    def test_the_refusal_is_a_finding_so_the_exit_code_is_not_clean(self):
        r, _ = self.refresh({"status": "verified", "kind": "scalar",
                             "value": {"total": 42}})
        hard = [f for f in r["findings"] if not f.startswith("review ")]
        self.assertTrue(hard)

    def test_a_real_result_still_refreshes(self):
        self.write({"status": "verified", "kind": "scalar",
                    "value": {"total": 42}})
        with unittest.mock.patch.object(
                verify_goldens, "check_value",
                return_value=("diff", "", [{"total": 99}])):
            r = verify(self.tmp, "http://truth", "samples", refresh=True,
                       quiet=True)
        stored = json.loads((self.tmp / "cases.jsonl").read_text())
        self.assertEqual(stored["golden"]["value"], {"total": 99})
        self.assertEqual(r["refreshed"], ["q1"])


class QuestionDrift(unittest.TestCase):
    def sealed(self, question, asked=None):
        return {"qid": "q1", "question": asked or question,
                "questionSha": hashlib.sha256(question.encode()).hexdigest()}

    def test_an_intact_question_is_silent(self):
        self.assertEqual(
            question_drift_findings([self.sealed("how many orders?")]), [])

    def test_a_narrowed_question_is_a_hard_finding(self):
        got = question_drift_findings([self.sealed(
            "the frequency distribution",
            asked="the reach frequency distribution")])
        self.assertEqual(len(got), 1)
        self.assertIn("questionSha", got[0])
        self.assertIn("new qid", got[0])

    def test_a_non_string_stamp_is_a_finding_not_a_crash(self):
        # `len(stamp)` on a number raised TypeError, and the top-level handler
        # turns that into exit 3 -- a malformed seal reported as an audit that
        # could not run, on a set whose other checks were fine.
        got = question_drift_findings([{"qid": "q1", "question": "x",
                                        "questionSha": 12345}])
        self.assertEqual(len(got), 1)
        self.assertIn("malformed", got[0])

    def test_a_stamp_shorter_than_the_seal_is_a_finding(self):
        # The quiet half: the prefix comparison still succeeds, on fewer bits
        # than the seal is worth, and reports clean all the way down.
        q = "how many orders?"
        got = question_drift_findings([
            {"qid": "q1", "question": q,
             "questionSha": hashlib.sha256(q.encode()).hexdigest()[:4]}])
        self.assertEqual(len(got), 1)
        self.assertIn("malformed", got[0])

    def test_a_16_char_stamp_is_the_real_shape(self):
        # evals/ecommerce/_author.py:602 writes sha256(question)[:16] for all
        # 49 cases. A full-digest comparison called every one of them edited.
        q = "What were our 2022 bookings?"
        c = {"qid": "ecom_2022_sales_bookings", "question": q,
             "questionSha": hashlib.sha256(q.encode()).hexdigest()[:16]}
        self.assertEqual(question_drift_findings([c]), [])

    def test_a_16_char_stamp_still_catches_an_edit(self):
        stamped = hashlib.sha256(b"the frequency distribution").hexdigest()[:16]
        got = question_drift_findings([
            {"qid": "q1", "question": "the reach frequency distribution",
             "questionSha": stamped}])
        self.assertEqual(len(got), 1)

    def test_an_unsealed_case_is_skipped_not_failed(self):
        # Sets predate the seal. Unguarded is not the same as broken, and
        # failing them would block every arm on every existing set.
        self.assertEqual(
            question_drift_findings([{"qid": "q1", "question": "x"}]), [])


class TruthIsolation(unittest.TestCase):
    """A truth server that also serves the model under test must be refused."""

    def fake_listing(self, names):
        import verify_goldens as vg_mod
        return unittest.mock.patch.object(
            vg_mod, "get_json",
            lambda base, path, timeout=30: [{"name": n} for n in names])

    def test_a_server_holding_both_packages_is_a_finding(self):
        # The laptop case: one server, both packages. The values are still
        # read from the truth package; what breaks is isolation, because the
        # answerer can retrieve the raw truth sources beside the model.
        with self.fake_listing(["ecommerce", "ecommerce-truth"]):
            got = truth_isolation_findings("http://x", "samples", "ecommerce")
        self.assertEqual(len(got), 1)
        self.assertIn("not an isolated truth server", got[0])

    def test_a_truth_only_server_is_silent(self):
        with self.fake_listing(["ecommerce-truth"]):
            self.assertEqual(
                truth_isolation_findings("http://x", "samples", "ecommerce"), [])

    def test_a_set_naming_no_target_package_is_silent(self):
        # Silent HERE is correct -- the function cannot guess what is under
        # test. What was wrong is that `targetPackage` was the only way to
        # supply it, and that field is written by nothing, appears in no
        # schema and is on no set, so the guard never fired anywhere. The
        # caller now passes it; see the two below.
        with self.fake_listing(["ecommerce", "ecommerce-truth"]):
            self.assertEqual(
                truth_isolation_findings("http://x", "samples", None), [])

    def test_verify_takes_the_target_package_from_its_caller(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            (tmp / "set.json").write_text('{"truthPackage": "ecommerce-truth"}')
            (tmp / "cases.jsonl").write_text("")
            with self.fake_listing(["ecommerce", "ecommerce-truth"]):
                r = verify(tmp, "http://x", "samples",
                           target_package="ecommerce", quiet=True)
            self.assertTrue([f for f in r["findings"]
                             if "not an isolated truth server" in f])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_set_json_still_supplies_it_when_the_caller_does_not(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        try:
            (tmp / "set.json").write_text(
                '{"truthPackage": "ecommerce-truth", '
                '"targetPackage": "ecommerce"}')
            (tmp / "cases.jsonl").write_text("")
            with self.fake_listing(["ecommerce", "ecommerce-truth"]):
                r = verify(tmp, "http://x", "samples", quiet=True)
            self.assertTrue([f for f in r["findings"]
                             if "not an isolated truth server" in f])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_an_unreachable_server_is_not_a_golden_finding(self):
        # It must not turn a connection problem into evidence about goldens:
        # the value check reports its own error, and exit 3 means "did not
        # happen".
        import verify_goldens as vg_mod

        def boom(*a, **k):
            raise OSError("connection refused")

        with unittest.mock.patch.object(vg_mod, "get_json", boom):
            self.assertEqual(
                truth_isolation_findings("http://x", "samples", "ecommerce"), [])




class AValueFreeSetNeedsNoTruthServer(unittest.TestCase):
    """A criteria-only set had no route to `verified` by any path.

    The whole `if promote:` block sat inside the value-check loop, which
    `skipped` empties -- including the branch for goldens that hold no value
    and by their own reasoning need no truth server. And a set naming no
    truthPackage now exits 3, which `improve.py` blocks on with no opt-out. So
    such a set could not be promoted here and could not pass the gate there,
    and the repair offered elsewhere ("name a truthPackage") is exactly what a
    set with no value to re-derive has nothing to put in.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        (self.tmp / "set.json").write_text('{"name": "s"}')   # no truthPackage

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, *goldens):
        (self.tmp / "cases.jsonl").write_text("".join(
            json.dumps({"qid": f"q{i}", "question": "x", "split": "dev",
                        "golden": g}) + "\n"
            for i, g in enumerate(goldens)))

    def test_a_criteria_set_promotes_with_no_server(self):
        self.write({"status": "provisional", "kind": "criteria",
                    "rubric": "must break out by region"})
        r = verify(self.tmp, None, "samples", promote=True, quiet=True)
        self.assertEqual(r["promoted"], ["q0"])
        stored = json.loads((self.tmp / "cases.jsonl").read_text())
        self.assertEqual(stored["golden"]["status"], "verified")
        self.assertEqual(stored["golden"]["verifiedBy"], "authored_criteria")

    def test_an_unanswerable_set_promotes_with_no_server(self):
        self.write({"status": "provisional", "kind": "unanswerable"})
        r = verify(self.tmp, None, "samples", promote=True, quiet=True)
        self.assertEqual(r["promoted"], ["q0"])

    def test_a_value_free_set_is_not_reported_as_a_skipped_check(self):
        # There is nothing a truth server would have re-derived, so a missing
        # one is not a check that did not happen -- and exit 3 would block
        # `improve.py` over a check that does not apply.
        self.write({"status": "verified", "kind": "criteria", "rubric": "r"})
        self.assertIsNone(verify(self.tmp, None, "samples",
                                 quiet=True)["skipped"])

    def test_one_value_bearing_golden_brings_the_skip_back(self):
        # A set that asks anything of a truth server must still say it did not
        # get one. This is the false green the exit-3 rule closed.
        self.write({"status": "verified", "kind": "criteria", "rubric": "r"},
                   {"status": "provisional", "kind": "scalar", "value": 42})
        self.assertTrue(verify(self.tmp, None, "samples", quiet=True)["skipped"])

    def test_an_empty_set_still_reports_the_skip(self):
        # "No case needs a value check" is vacuously true of no cases, and an
        # empty set is not a set that asks nothing -- it is a set with nothing.
        (self.tmp / "cases.jsonl").write_text("")
        self.assertTrue(verify(self.tmp, None, "samples", quiet=True)["skipped"])


class TheCompositionRuleAtTheGate(unittest.TestCase):
    """A truth server re-derives values; a ledger validates the definitions a
    value rests on. Either is evidence. Neither present is a check that did not
    happen, and the gate says which."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        (self.tmp / "set.json").write_text('{"name": "s"}')   # no truthPackage
        self.led = self.tmp / "led.jsonl"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write_case(self, status="verified", ids=("measure:s:m",),
                   verification=None):
        g = {"status": status, "kind": "scalar", "value": {"v": 1}}
        if verification:
            g["verification"] = verification
        (self.tmp / "cases.jsonl").write_text(json.dumps(
            {"qid": "q1", "question": "x", "split": "dev", "golden": g,
             "expectedEntities": {"required": list(ids)}}) + "\n")

    def write_ledger(self, **verdicts):
        self.led.write_text("".join(json.dumps(
            {"entityId": e, "verdict": v, "exprSha": "s"}) + "\n"
            for e, v in verdicts.items()))

    def test_validated_definitions_lift_the_skip(self):
        self.write_case()
        self.write_ledger(**{"measure:s:m": "agrees"})
        r = verify(self.tmp, None, "samples", quiet=True, definitions=self.led)
        self.assertTrue(r["skipped"], "still no truth server")
        self.assertTrue(r["ledgerValidated"], "but the definitions are checked")
        self.assertEqual(r["unvalidated"], [])

    def test_an_unchecked_definition_names_the_case(self):
        self.write_case()
        self.write_ledger(**{"measure:s:m": "unchecked"})
        r = verify(self.tmp, None, "samples", quiet=True, definitions=self.led)
        self.assertFalse(r["ledgerValidated"])
        self.assertEqual(r["unvalidated"], ["q1 (unchecked)"])

    def test_a_definition_missing_from_the_ledger_does_not_validate(self):
        self.write_case()
        self.write_ledger(**{"measure:s:other": "agrees"})
        r = verify(self.tmp, None, "samples", quiet=True, definitions=self.led)
        self.assertFalse(r["ledgerValidated"])

    def test_an_empty_ledger_validates_nothing(self):
        # Zero rows is not "every definition agrees"; it is no evidence at all.
        self.write_case()
        self.led.write_text("")
        r = verify(self.tmp, None, "samples", quiet=True, definitions=self.led)
        self.assertFalse(r["ledgerValidated"])

    def test_promotion_through_the_ledger_still_needs_the_second_derivation(self):
        # Validated definitions say the pieces are right; two derivations
        # agreeing say the VALUE is. `verified` needs both.
        self.write_case(status="provisional")
        self.write_ledger(**{"measure:s:m": "agrees"})
        r = verify(self.tmp, None, "samples", quiet=True, definitions=self.led,
                   promote=True)
        self.assertEqual(r["promoted"], [])
        self.assertTrue(any("q1" in n for n in r["promotionNotes"]),
                        r["promotionNotes"])

    def test_promotion_through_the_ledger_names_the_ledger(self):
        self.write_case(status="provisional",
                        verification={"primaryAxis": "a", "variesAxis": "b"})
        self.write_ledger(**{"measure:s:m": "agrees"})
        r = verify(self.tmp, None, "samples", quiet=True, definitions=self.led,
                   promote=True)
        self.assertEqual(r["promoted"], ["q1"])
        g = json.loads((self.tmp / "cases.jsonl").read_text())["golden"]
        self.assertEqual(g["status"], "verified")
        self.assertIn("definition ledger", g["verifiedBy"])

    def test_no_ledger_keeps_the_old_behaviour(self):
        self.write_case()
        r = verify(self.tmp, None, "samples", quiet=True)
        self.assertTrue(r["skipped"])
        self.assertFalse(r["ledgerValidated"])


class Promotion(unittest.TestCase):
    """`--promote` is the ONLY thing that writes `golden.status`.

    Without it the loop was closed: `skill:eval-import` stamps `provisional` on
    every golden holding a value and enforces it on write, `skill:eval-answer`
    refuses a verdict on one, and nothing promoted -- so an imported set was
    unscorable forever and no file said why. These pin the standard it enforces
    rather than a softer one.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        (self.tmp / "set.json").write_text(
            '{"name": "s", "truthPackage": "truth"}')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def case(self, **golden):
        g = {"status": "provisional", "kind": "scalar", "value": 42}
        g.update(golden)
        return {"qid": "q1", "question": "x", "golden": g}

    def write(self, case):
        (self.tmp / "cases.jsonl").write_text(json.dumps(case) + "\n")

    def run_promote(self, case, value_status="ok"):
        self.write(case)
        with unittest.mock.patch.object(
                verify_goldens, "check_value",
                return_value=(value_status, "", [{"x": 42}])):
            r = verify(self.tmp, "http://truth", "samples", promote=True,
                       quiet=True)
        stored = json.loads((self.tmp / "cases.jsonl").read_text())
        return r, stored

    SECOND = {"verification": {"primaryAxis": "day", "variesAxis": "region"}}

    def test_a_re_derived_golden_with_a_second_derivation_promotes(self):
        r, stored = self.run_promote(self.case(**self.SECOND))
        self.assertEqual(r["promoted"], ["q1"])
        self.assertEqual(stored["golden"]["status"], "verified")
        self.assertEqual(stored["golden"]["verifiedBy"],
                         "verify_goldens.py --promote")

    def test_promotion_does_not_bump_goldenRevision(self):
        # The VALUE did not move, so scores taken against it stay comparable.
        # Bumping would have read as a golden repair and invalidated them.
        case = self.case(**self.SECOND)
        case["goldenRevision"] = 3
        _, stored = self.run_promote(case)
        self.assertEqual(stored["goldenRevision"], 3)

    def test_one_derivation_agreeing_with_itself_does_not_promote(self):
        r, stored = self.run_promote(self.case())
        self.assertEqual(r["promoted"], [])
        self.assertEqual(stored["golden"]["status"], "provisional")
        self.assertIn("no second derivation", r["promotionNotes"][0])

    def test_a_drifted_value_does_not_promote(self):
        r, stored = self.run_promote(self.case(**self.SECOND),
                                     value_status="diff")
        self.assertEqual(r["promoted"], [])
        self.assertEqual(stored["golden"]["status"], "provisional")

    def test_their_own_query_agreeing_is_not_enough(self):
        # The exact shape eval-import step 3 describes: their query, their
        # number, agreeing. It ran against the model under test, so a model bug
        # would certify its own golden.
        r, _ = self.run_promote(
            self.case(verifiedBy="authored_query", canonicalQuery="run: a"))
        self.assertEqual(r["promoted"], [])

    def test_invalid_and_ambiguous_never_promote(self):
        # Judgements about the key that a re-derivation cannot make. They go
        # through the golden side door and a person settles them.
        for status in ("invalid", "ambiguous"):
            with self.subTest(status):
                r, stored = self.run_promote(
                    self.case(status=status, **self.SECOND))
                self.assertEqual(r["promoted"], [])
                self.assertEqual(stored["golden"]["status"], status)

    def test_nothing_is_written_without_the_flag(self):
        self.write(self.case(**self.SECOND))
        with unittest.mock.patch.object(
                verify_goldens, "check_value",
                return_value=("ok", "", [{"x": 42}])):
            r = verify(self.tmp, "http://truth", "samples", quiet=True)
        self.assertEqual(r["promoted"], [])
        self.assertEqual(
            json.loads((self.tmp / "cases.jsonl").read_text())["golden"]["status"],
            "provisional")

    def test_every_unpromoted_golden_says_why(self):
        # A caller that asked for promotion and got none must learn the reason,
        # or it is back to guessing at an invisible gate.
        r, _ = self.run_promote(self.case())
        self.assertEqual(len(r["promotionNotes"]), 1)
        self.assertIn("q1", r["promotionNotes"][0])

    def test_blocker_reasons_are_distinct_per_cause(self):
        s = pathlib.Path(self.tmp)
        self.assertIn("already verified",
                      promotion_blocker(self.case(status="verified"), s))
        self.assertIn("holds no value",
                      promotion_blocker(
                          {"qid": "q1", "golden": {"status": "provisional",
                                                   "kind": "criteria"}}, s))
        self.assertIsNone(promotion_blocker(self.case(**self.SECOND), s))



class RequiredAndAcceptableOverlap(unittest.TestCase):
    def test_an_id_in_both_lists_is_reviewed_once(self):
        # Review found sets that copied every `required` id into `acceptable`
        # and never used `requiredAnyOf`; nothing detected it. It moves no
        # number, so it is reviewed, not failed.
        eid = "measure:nowhere:dup"
        f = unknown_name_findings([case(required=[eid], acceptable=[eid])], MODEL)
        dup = [x for x in f if "both required and acceptable" in x]
        self.assertEqual(len(dup), 1)
        self.assertTrue(dup[0].startswith("review "))


class VerifiedOnArrival(Promotion):
    """unanswerable and criteria goldens hold no value; the importer marks
    them verified on arrival, and the promoter refused them, so a set that
    imported them provisional could never score its refusal cases."""

    def test_an_unanswerable_golden_promotes(self):
        r, stored = self.run_promote(
            self.case(kind="unanswerable", value=None, rubric="decline"),
            value_status="skipped")
        self.assertEqual(r["promoted"], ["q1"])
        self.assertEqual(stored["golden"]["verifiedBy"], "authored_criteria")

    def test_a_criteria_golden_promotes(self):
        r, stored = self.run_promote(
            self.case(kind="criteria", value=None, rubric="REQUIRED: names X"),
            value_status="skipped")
        self.assertEqual(r["promoted"], ["q1"])
        self.assertEqual(stored["golden"]["status"], "verified")

    def test_criteria_is_skipped_by_the_value_check_not_an_error(self):
        # It has no canonicalQuery by design; "error" counted as drift and
        # failed the audit for the whole set.
        status, _, _ = verify_goldens.check_value(
            self.case(kind="criteria", value=None, rubric="x"),
            argparse.Namespace(publisher="http://x", environment="e",
                               truth_package="t", truth_model="t.malloy",
                               rewrite=False))
        self.assertEqual(status, "skipped")


class Attestation(Promotion):
    """The human path. A person vouches for a re-derived value that has no
    second derivation, and the record says a person did, not a query."""

    def attest(self, case, value_status="ok", text="jane, 2026-09-14, rows checked by hand"):
        self.write(case)
        with unittest.mock.patch.object(
                verify_goldens, "check_value",
                return_value=(value_status, "", [{"x": 42}])):
            r = verify(self.tmp, "http://truth", "samples", promote=True,
                       quiet=True, attest=text)
        return r, json.loads((self.tmp / "cases.jsonl").read_text())

    def test_attestation_promotes_and_records_who(self):
        r, stored = self.attest(self.case())
        self.assertEqual(r["promoted"], ["q1"])
        self.assertEqual(r["attested"], ["q1"])
        g = stored["golden"]
        self.assertEqual(g["status"], "verified")
        self.assertTrue(g["verifiedBy"].startswith("attested: jane"))
        self.assertEqual(g["verification"]["variesAxis"], "human-attestation")
        self.assertIn("attestation", g["verification"])

    def test_attestation_never_covers_a_drifted_value(self):
        r, stored = self.attest(self.case(), value_status="diff")
        self.assertEqual(r["promoted"], [])
        self.assertEqual(stored["golden"]["status"], "provisional")

    def test_a_real_second_derivation_is_not_marked_attested(self):
        r, stored = self.attest(self.case(**self.SECOND))
        self.assertEqual(r["promoted"], ["q1"])
        self.assertEqual(r["attested"], [])
        self.assertEqual(stored["golden"]["verifiedBy"], "verify_goldens.py --promote")

    def test_without_attest_the_note_names_the_flag(self):
        r, _ = self.run_promote(self.case())
        self.assertIn("--attest", r["promotionNotes"][0])


class RubricFigures(unittest.TestCase):
    """Figures a rubric asserts as RIGHT that the golden does not hold. The
    rejecting half quotes numbers that are supposed to be absent, so the check
    must stop at the first rejecting marker, in whatever words the set uses."""

    def case(self, rubric, **value):
        return {"qid": "q", "golden": {"kind": "scalar", "rubric": rubric,
                                        "value": {**value, "round": 2}}}

    def test_a_trap_after_wrong_pick_is_not_asserted_right(self):
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: sale_price minus cost, 6564004.49. Using total_sales "
            "(12566292.88) is WRONG_PICK.", total_gross_margin=6564004.49))
        self.assertEqual(f, [])

    def test_a_diagnose_code_ends_the_accepting_clause(self):
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: status = 'Complete' only, 11865343.56. The model's total_sales "
            "measure has no status filter (12566292.88), using it is FILTER-LITERAL "
            "/ SCOPE.", total_sales=11865343.56))
        self.assertEqual(f, [])

    def test_a_renamed_code_ends_the_accepting_clause_too(self):
        # The regex listed only the retired names, so a rubric written with
        # the new ones had its trap figure read as asserted-right.
        for code in ("RULE_UNWRITTEN", "MISSING", "CONVENTION"):
            f = verify_goldens.rubric_number_findings(self.case(
                f"Right: net of returns, 11865343.56. Gross (12566292.88) is "
                f"{code}.", total_sales=11865343.56))
            self.assertEqual(f, [], code)

    def test_lower_case_prose_is_not_a_code(self):
        # "the scope of the question" rejects nothing; only the upper-case code does.
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: 99999.99, the scope of the question is one year.",
            total=6564004.49))
        self.assertEqual(len(f), 1)

    def test_a_figure_in_the_accepting_clause_is_still_reported(self):
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: 99999.99 exactly.", total=6564004.49))
        self.assertEqual(len(f), 1)
        self.assertIn("99999.99", f[0])

    def test_a_four_digit_stale_figure_is_now_caught(self):
        # The floor was five significant digits, so a four-digit quantity the
        # golden does not hold passed silently. Measured over the local sets,
        # moving it to three digits at or above 100 took the findings from 14
        # to 17 over 54 rubrics.
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: 2672 returned line items.", returned=1830))
        self.assertEqual(len(f), 1)
        self.assertIn("2672", f[0])

    def test_a_bare_three_digit_figure_is_NOT_caught(self):
        # Deliberate, and the limit of this check. Catching it means
        # tokenising every three-digit integer in rubric prose, which was
        # measured over the same sets: 17 findings become 552, because a
        # rubric quoting a list of ids contributes one per id. A check that
        # reports 552 things is not read at all.
        #
        # So the "615 and 502 over goldens holding 747 and 370" case -- a
        # correct answer failed against prose one re-derivation out of date --
        # is NOT closed here. It is closed in the judge prompt, which is told
        # that where the rubric and the golden disagree about a figure, the
        # golden is the key. That rule holds at any precision and needs no
        # regex. This test exists so nobody re-opens the floor without
        # re-measuring.
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: monthly page views of 615 and 502.",
            first=747, second=370))
        self.assertEqual(f, [])

    def test_a_year_is_not_a_figure(self):
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: the 2024 total, 747.", total=747))
        self.assertEqual(f, [])

    def test_a_small_count_is_not_a_figure(self):
        # Below 100 stays out: "the top 12 categories", "5 rows", an ordinal.
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: the top 12 categories, over 5 regions.", total=747))
        self.assertEqual(f, [])

    def test_a_one_decimal_average_is_checked(self):
        # "about 740.5" is how a rubric quotes an average, and the tokeniser
        # required TWO decimal places, so that whole shape was invisible.
        # Admitting one-decimal figures added zero findings across every set
        # available locally, so it is free.
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: an average flight distance of about 688.2.",
            average=740.48))
        self.assertEqual(len(f), 1)
        self.assertIn("688.2", f[0])

    def test_a_small_decimal_is_still_excluded(self):
        # The floor still applies after the tokeniser: a value under 100 with
        # few digits is a rate or a ratio, not a quoted result.
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: a ratio of 3.5.", total=747))
        self.assertEqual(f, [])

    def test_a_figure_the_golden_holds_is_not_reported(self):
        f = verify_goldens.rubric_number_findings(self.case(
            "Right: 747 page views.", views=747))
        self.assertEqual(f, [])



class ScalarValueShape(unittest.TestCase):
    """A bare string where an object belongs is one finding, not a blackout.

    `"value": "Ecommerce"` instead of `{"answer": "Ecommerce"}` is the obvious
    authoring mistake for a benchmark whose answers ARE strings. It raised
    AttributeError out of `verify()` and killed the sweep, so the other
    thirty-nine cases went unaudited and the script's own closing line --
    "this says NOTHING about the goldens" -- was right.
    """

    def args(self):
        return argparse.Namespace(
            publisher="http://x", environment="truth", truth_package="t",
            truth_model="truth.malloy", rewrite=False)

    def check(self, value, rows):
        c = {"qid": "q", "golden": {"kind": "scalar", "value": value,
                                    "canonicalQuery": "run: t -> { ... }"}}
        with unittest.mock.patch("verify_goldens.try_query",
                                 return_value=(rows, None)):
            return check_value(c, self.args())

    def test_a_bare_string_is_an_error_not_a_crash(self):
        status, detail, _ = self.check("Ecommerce", [{"answer": "Ecommerce"}])
        self.assertEqual(status, "error")
        self.assertIn("value is str", detail)

    def test_the_error_shows_the_shape_it_wanted(self):
        _, detail, _ = self.check("Ecommerce", [{"answer": "Ecommerce"}])
        self.assertIn('{"answer": "Ecommerce"}', detail)

    def test_a_list_under_scalar_is_also_caught(self):
        status, detail, _ = self.check([1, 2], [{"answer": 1}])
        self.assertEqual(status, "error")
        self.assertIn("value is list", detail)

    def test_the_proper_shape_still_works(self):
        status, _, _ = self.check({"answer": "Ecommerce"},
                                  [{"answer": "Ecommerce"}])
        self.assertEqual(status, "ok")


class YoungGoldens(unittest.TestCase):
    """`eval-import` produces "a number alone" as a legitimate outcome, and
    the audit called it a hard finding -- so the import skill's own output
    refused the check that runs before every arm."""

    def args(self):
        return argparse.Namespace(
            publisher="http://x", environment="truth", truth_package="t",
            truth_model="truth.malloy", rewrite=False)

    def check(self, status_in):
        c = {"qid": "q", "golden": {"kind": "scalar", "value": {"n": 1},
                                    "status": status_in}}
        return check_value(c, self.args())

    def test_provisional_with_no_query_is_not_a_finding(self):
        status, detail, _ = self.check("provisional")
        self.assertEqual(status, "unrederivable")
        self.assertIn("no canonicalQuery yet", detail)

    def test_an_absent_status_reads_as_provisional(self):
        c = {"qid": "q", "golden": {"kind": "scalar", "value": {"n": 1}}}
        self.assertEqual(check_value(c, self.args())[0], "unrederivable")

    def test_VERIFIED_with_no_query_is_still_an_error(self):
        # `verified` is a claim that it re-derived. That claim needs the query.
        status, detail, _ = self.check("verified")
        self.assertEqual(status, "error")
        self.assertIn("cannot be re-derived", detail)

    def test_unrederivable_does_not_count_as_drift(self):
        # `drifted` sums diff + error, and the arm gate reads `drifted`.
        self.assertNotIn("unrederivable", ("diff", "error"))


class NumericRendering(unittest.TestCase):
    """Malloy renders a number in its shortest form, so a golden stated to two
    decimals comes back as `75.7`. Comparing the rendered strings called that
    drift, which blocks an arm -- and the workaround was padding expressions
    in the truth queries whose only job was to make one string look like the
    other."""

    def test_numeric_strings_compare_as_numbers(self):
        self.assertTrue(close_enough("75.70", "75.7", None))

    def test_a_number_against_its_string_matches(self):
        self.assertTrue(close_enough(75.70, "75.7", None))

    def test_a_thousands_separator_is_read(self):
        self.assertTrue(close_enough("1,234", "1234", None))

    def test_genuine_text_still_compares_exactly(self):
        self.assertTrue(close_enough("Ecommerce", "Ecommerce", None))
        self.assertFalse(close_enough("Ecommerce", "Retail", None))

    def test_a_bool_is_not_the_number_one(self):
        # `True == 1` in Python, and a boolean golden must not match "1".
        self.assertFalse(close_enough(True, "1", None))

    def test_a_real_difference_is_still_drift(self):
        self.assertFalse(close_enough("75.70", "76.1", None))

    def test_a_compound_string_is_left_alone(self):
        # Out of scope on purpose: it parses as no single number, so it stays
        # an exact compare rather than being guessed at.
        self.assertFalse(close_enough("C: 75.70", "C: 75.7", None))

class VerifyQuotedFigures(unittest.TestCase):
    """Query the numbers a prose answer key quotes, and keep the query.

    The case this exists for: a criteria golden's note said "by order count it
    is Amelia Cohen (80 orders)". 80 was three customers who share a name,
    summed by name -- the exact grouping the neighbouring case forbids -- and
    she was not top even so. Nothing checked it, because a criteria golden has
    no value to re-derive and no rows for the figure check to read.
    """

    def case(self, note):
        return {"qid": "q1", "golden": {"kind": "criteria", "rubric": "prose",
                                        "verification": {"note": note}}}

    def args(self):
        return argparse.Namespace(
            publisher="http://truth", environment="truth",
            truth_package="t-truth", truth_model="truth.malloy",
            figure_model="sonnet")

    @staticmethod
    def cli(text, stderr=""):
        """A stub with `run_cli`'s return shape, not a bare string. The bare
        string is how the empty-reply bug on the real path went unseen."""
        return lambda *a, **k: ([{"type": "assistant"}], text, stderr, 1, 0.1)

    def test_a_contradicted_figure_is_reported_with_its_query(self):
        reply = json.dumps({
            "query": "run: t_order_items -> { group_by: customer_id; "
                     "aggregate: n is count(order_id) }",
            "rows": "884 | 43", "computed": "43",
            "verdict": "contradicted",
            "why": "at customer_id grain the top is 43, not 80; 80 sums three "
                   "customers who share a name"})
        out = verify_goldens.verify_figures(
            self.case("By order count it is Amelia Cohen (80 orders)."),
            self.args(), run=self.cli(reply))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["verdict"], "contradicted")
        self.assertEqual(out[0]["figure"], "80")
        # The query is the receipt: the grouping error is visible in it and
        # invisible in the verdict alone.
        self.assertIn("group_by: customer_id", out[0]["query"])

    def test_the_sentence_travels_with_the_figure(self):
        """A bare number cannot be checked; what it MEANS is in its sentence."""
        seen = {}
        def run(cmd, **kw):
            seen["prompt"] = cmd[2]
            return self.cli(json.dumps({"verdict": "confirmed", "query": "q",
                                        "computed": "943"}))()
        verify_goldens.verify_figures(
            self.case("A customer is one we delivered to: 943 of them."),
            self.args(), run=run)
        self.assertIn("943", seen["prompt"])
        self.assertIn("delivered to", seen["prompt"])

    def test_an_unreadable_reply_is_unverifiable_not_confirmed(self):
        out = verify_goldens.verify_figures(
            self.case("The total was 8,817.56 last year."),
            self.args(), run=self.cli("not json at all"))
        self.assertEqual(out[0]["verdict"], "unverifiable")
        self.assertEqual(out[0]["why"], "the checker returned no readable JSON")

    def test_stderr_is_the_error_never_the_reply(self):
        """A timeout's stderr once went to the JSON scanner as the reply."""
        out = verify_goldens.verify_figures(
            self.case("We sold 12,345 units."), self.args(),
            run=self.cli("", stderr='timeout after 300s {"verdict": "confirmed"}'))
        self.assertEqual(out[0]["verdict"], "unverifiable")
        self.assertIn("timeout after 300s", out[0]["error"])

    def test_a_confirmation_without_its_query_is_not_a_receipt(self):
        out = verify_goldens.verify_figures(
            self.case("We sold 12,345 units."), self.args(),
            run=self.cli(json.dumps({"verdict": "confirmed",
                                     "computed": "12345"})))
        self.assertEqual(out[0]["verdict"], "unverifiable")
        self.assertEqual(out[0]["why"],
                         "confirmed without the query that computed it")

    def test_a_verdict_outside_the_vocabulary_is_unverifiable(self):
        out = verify_goldens.verify_figures(
            self.case("We sold 12,345 units."), self.args(),
            run=self.cli(json.dumps({"verdict": "probably", "query": "q"})))
        self.assertEqual(out[0]["verdict"], "unverifiable")
        self.assertIn("'probably'", out[0]["why"])

    def test_a_crash_does_not_take_down_the_audit(self):
        def boom(*a, **k):
            raise RuntimeError("cli exploded")
        out = verify_goldens.verify_figures(self.case("We sold 12,345 units."),
                                            self.args(), run=boom)
        self.assertEqual(out[0]["verdict"], "unverifiable")
        self.assertIn("cli exploded", out[0]["error"])

    def test_a_golden_holding_a_value_is_not_this_check(self):
        """Those re-derive through their own canonicalQuery already."""
        rows = verify_goldens.verify_figures(
            {"qid": "q2", "golden": {"kind": "scalar", "value": {"n": 943},
                                     "rubric": "about 943"}},
            self.args(), run=self.cli("{}"))
        self.assertEqual(rows, [])

    def test_the_real_cli_path_returns_the_agents_verdict(self):
        """Through the real `run_cli`, with a fake `claude` on PATH.

        Every other test injects `run`. This one does not, so it is the one
        that fails if the command stops asking for stream-json (the reply
        then parses as empty and every figure reads `unverifiable`) or stops
        granting the curl the prompt tells the agent to use.
        """
        with tempfile.TemporaryDirectory() as d:
            argv_log = pathlib.Path(d) / "argv.json"
            reply = json.dumps({"query": "run: t -> { aggregate: n is count() }",
                                "rows": "43", "computed": "43",
                                "verdict": "contradicted", "why": "43, not 80"})
            fake = pathlib.Path(d) / "claude"
            fake.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys\n"
                f"open({str(argv_log)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
                "if 'stream-json' not in sys.argv:\n"
                "    print('prose, as claude -p prints without the flag')\n"
                "    sys.exit(0)\n"
                "print(json.dumps({'type': 'assistant', 'message': {'content': "
                f"[{{'type': 'text', 'text': {reply!r}}}]}}}}))\n")
            fake.chmod(0o755)
            with unittest.mock.patch.dict(
                    "os.environ", {"PATH": f"{d}:/usr/bin:/bin"}):
                out = verify_goldens.verify_figures(
                    self.case("By order count it is Amelia Cohen (80 orders)."),
                    self.args())
            argv = json.loads(argv_log.read_text())
        self.assertEqual(out[0]["verdict"], "contradicted")
        self.assertEqual(out[0]["computed"], "43")
        self.assertIn("Bash(curl:*)", argv)
        self.assertIn("--strict-mcp-config", argv)

    def test_the_flag_reaches_the_figure_check(self):
        """`--verify-figures` was parsed and then dropped before `verify()`."""
        with tempfile.TemporaryDirectory() as d:
            sd = pathlib.Path(d)
            (sd / "set.json").write_text(json.dumps({"truthPackage": "t-truth"}))
            (sd / "cases.jsonl").write_text(json.dumps(
                {**self.case("It was 80 orders."), "question": "q?"}) + "\n")
            called = []
            def fake(case, a, run=None):
                called.append(a.figure_model)
                return []
            with unittest.mock.patch.object(verify_goldens, "verify_figures",
                                            side_effect=fake), \
                 unittest.mock.patch.object(sys, "argv", [
                     "verify_goldens.py", "--set", str(sd),
                     "--publisher", "http://127.0.0.1:9",
                     "--verify-figures", "--figure-model", "opus"]), \
                 unittest.mock.patch("builtins.print"):
                try:
                    verify_goldens.main()
                except SystemExit:
                    pass
        self.assertEqual(called, ["opus"])


if __name__ == "__main__":
    unittest.main()


class HeldGoldensAreNotReDerived(unittest.TestCase):
    """An `ambiguous` or `invalid` golden is a person's judgement. Running the
    value check on one reported an error on every audit, `verify()` counted the
    error as drift, and the arm refused to start."""

    def setUp(self):
        self.a = argparse.Namespace(rewrite=False, publisher="http://x",
                                    environment="e", truth_package="t",
                                    truth_model="truth.malloy")

    def case(self, status):
        return {"qid": "s-1", "golden": {"kind": "scalar", "status": status,
                                          "canonicalQuery": "run: model -> { aggregate: n }",
                                          "value": {"n": 1}}}

    def test_a_held_golden_is_skipped_with_the_reason(self):
        for status in ("ambiguous", "invalid"):
            with self.subTest(status=status):
                with unittest.mock.patch.object(verify_goldens, "try_query") as tq:
                    got, detail, rows = check_value(self.case(status), self.a)
                tq.assert_not_called()
                self.assertEqual(got, "skipped")
                self.assertIn(status, detail)
                self.assertIn("side door", detail)

    def test_a_provisional_golden_with_a_query_still_runs(self):
        with unittest.mock.patch.object(verify_goldens, "try_query",
                                        return_value=([{"n": 1}], None)) as tq:
            got, _, _ = check_value(self.case("provisional"), self.a)
        tq.assert_called_once()
        self.assertEqual(got, "ok")


class ANotQueryableKeyNamesItsRootSource(unittest.TestCase):
    """A set authored before its truth package existed replays packaged views;
    the truth server answers 404 for each, and the bare message sent a reader
    to the server instead of the golden."""

    def test_the_root_source_and_the_remedy_are_in_the_detail(self):
        a = argparse.Namespace(rewrite=False, publisher="http://x", environment="e",
                               truth_package="t", truth_model="truth.malloy")
        case = {"qid": "s-2", "golden": {"kind": "scalar", "status": "provisional",
                                          "canonicalQuery": "run: orders -> { aggregate: n }",
                                          "value": {"n": 1}}}
        err = 'HTTP 404: {"code":404,"message":"Query target is not queryable."}'
        with unittest.mock.patch.object(verify_goldens, "try_query",
                                        return_value=(None, err)):
            got, detail, _ = check_value(case, a)
        self.assertEqual(got, "error")
        self.assertIn("`orders`", detail)
        self.assertIn("truth package", detail)

    def test_any_other_error_is_reported_as_it_came(self):
        a = argparse.Namespace(rewrite=False, publisher="http://x", environment="e",
                               truth_package="t", truth_model="truth.malloy")
        case = {"qid": "s-3", "golden": {"kind": "scalar", "status": "provisional",
                                          "canonicalQuery": "run: t_a -> { aggregate: n }",
                                          "value": {"n": 1}}}
        with unittest.mock.patch.object(verify_goldens, "try_query",
                                        return_value=(None, "HTTP 500: boom")):
            got, detail, _ = check_value(case, a)
        self.assertEqual((got, detail), ("error", "HTTP 500: boom"))
