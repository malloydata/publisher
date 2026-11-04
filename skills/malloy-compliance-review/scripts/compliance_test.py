#!/usr/bin/env python3
"""Unit tests for compliance.py. Stdlib only: python3 -m unittest compliance_test

The checks run against a fake server built from a governance export and canned
query answers, so every rule is pinned without a Publisher. Several tests pin
bugs found while building the clinical-records example: a reserved-word alias
that zeroed every pattern, a row-count reading of "scoped", and a stale model
that reported the controls it broke as holding.
"""
from __future__ import annotations

import re
import unittest

import compliance as c


def field(path, classes, kind="dimension", **kw):
    return {"path": path, "kind": kind, "classifications": classes, **kw}


def source(name, fields, published=True, carries=None, authorize=None,
           access_filter=None, denied=False, model="index.malloy"):
    return {
        "name": name, "modelPath": model, "published": published, "deniedToAll": denied,
        "carries": carries, "controls": [],
        "authorize": authorize or [], "accessFilter": access_filter or [],
        "fields": fields,
    }


def gate(expr, *givens, secure=True):
    return {"expression": expr, "givens": [{"name": g, "secure": secure} for g in givens]}


REGISTER = {
    "identifier_classes": {"direct": ["phi.direct_identifier"], "quasi": ["phi.quasi_identifier"]},
    "personas": {
        "clinician": {"givens": {"ROLES": ["clinical"]}},
        "researcher": {"givens": {"ROLES": ["researcher"]}},
    },
    "access": {"chart": {"clinician": "scoped"}, "research": {"researcher": "rows"}},
    "domains": {"research": {"deidentified": True, "must_not_carry": ["phi.direct_identifier"],
                             "caps": [{"field": "age", "max": 90}]}},
}


def governance(**over):
    chart = source("chart", [field("ssn", ["phi.direct_identifier"])],
                   carries=["phi.direct_identifier"],
                   authorize=[gate("'clinical' in $ROLES", "ROLES")],
                   access_filter=[gate("org in $ORGS", "ORGS")])
    research = source("research", [field("year", ["phi.quasi_identifier"]),
                                   field("age", ["phi.quasi_identifier"])],
                      carries=["phi.quasi_identifier", "none"],
                      authorize=[gate("'researcher' in $ROLES", "ROLES")])
    base = source("base", [field("SSN", [])], published=False, denied=True, model="base.malloy")
    sources = over.get("sources", [chart, research, base])
    return {"sources": sources, "secureGivens": ["ORGS", "ROLES"]}


def profile_row(**counts):
    row = {"rows": 1000, "filled": 1000, "distinct_values": 10, "max_length": 4, "avg_length": 4}
    for name in c.PATTERNS:
        row["m_" + name] = 0
    for k, v in counts.items():
        row[("m_" + k) if k in c.PATTERNS else k] = v
    return row


class FakeServer(c.Server):
    """Answers count probes from `access` and profile queries from `profiles`."""

    def __init__(self, gov, access=None, profiles=None, load_errors=None):
        super().__init__("http://fake", "env", "pkg", "index.malloy")
        self._gov = gov
        self._access = access or {}
        self._profiles = profiles or {}
        self.load_errors = load_errors or []
        self.queries = []

    def governance(self):
        return self._gov

    def status(self):
        errs = [{**e, "environment": "env", "package": "pkg"} for e in self.load_errors]
        return {"operationalState": "serving", **({"loadErrors": errs} if errs else {})}

    def query(self, malloy, givens):
        self.queries.append((malloy, givens))
        m = re.match(r"run: (\w+) -> \{ aggregate: n is count\(\) \}", malloy)
        if m:
            role = (givens.get("ROLES") or [""])[0]
            n = self._access.get((m.group(1), role))
            return ([{"n": n}], None, None) if n is not None else (None, 403, "Access denied")
        m = re.match(r"run: (\w+) -> \{ aggregate: max_value", malloy)
        if m:
            path = re.search(r"max\(([\w.]+)\)", malloy).group(1)
            mx = self._profiles.get((m.group(1), path), {}).get("max_value")
            return [{"max_value": mx, "min_value": 0}], None, None
        m = re.match(r"run: (\w+) -> \{ aggregate: rows is count\(\)", malloy)
        if m:
            path = re.search(r"distinct_values is count\(([\w.]+)\)", malloy).group(1)
            p = self._profiles.get((m.group(1), path), {})
            return [profile_row(**{k: v for k, v in p.items() if k != "max_value"})], None, None
        return None, 400, f"unexpected query {malloy}"


def run(server, controls):
    return c.run_check(server, {**REGISTER, "controls": controls})


class ProfileQueryTest(unittest.TestCase):
    def test_aliases_avoid_reserved_words(self):
        q = c.profile_query("research", "year")
        self.assertIn("m_year is count()", q)
        self.assertNotRegex(q, r"(?<!m_)\byear is count")

    def test_average_through_a_join_names_the_join(self):
        q = c.profile_query("chart", "procedures.procedure_code")
        self.assertIn("avg_length is procedures.avg(length(procedures.procedure_code::string))", q)
        self.assertIn("avg_length is avg(length(age::string))", c.profile_query("research", "age"))


class SignalsTest(unittest.TestCase):
    def profile(self, **kw):
        row = profile_row(**kw)
        return {k.removeprefix("m_"): v for k, v in row.items()}

    def test_patterns_need_a_majority_of_filled_values(self):
        self.assertEqual(c.signals_of(self.profile(ssn=1000), True), ["ssn"])
        self.assertEqual(c.signals_of(self.profile(ssn=400), True), [])

    def test_near_unique_only_at_the_domains_own_grain(self):
        p = self.profile(distinct_values=990)
        self.assertIn("near_unique", c.signals_of(p, own_grain=True))
        self.assertNotIn("near_unique", c.signals_of(p, own_grain=False))

    def test_empty_field(self):
        self.assertEqual(c.signals_of(self.profile(filled=0), True), ["empty"])

    def test_signals_come_from_the_renamed_profile(self):
        # The bug: signals were read from the raw row, whose keys are m_*,
        # so every pattern read zero and a column of SSNs passed as clean.
        server = FakeServer(governance(), profiles={("chart", "ssn"): {"ssn": 1000}})
        out = c.profile_domain(server, REGISTER, governance()["sources"][0])
        self.assertEqual(out[0]["signals"], ["ssn"])
        self.assertEqual(out[0]["profile"]["ssn"], 1000)


    def test_unique_per_subject_needs_text_and_enough_subjects(self):
        p = self.profile(distinct_values=70, subjects=70)
        self.assertIn("unique_per_subject", c.signals_of(p, own_grain=False))
        # A unique dollar amount is not an identifier.
        self.assertNotIn("unique_per_subject", c.signals_of(p, own_grain=False, numeric=True))
        # Seventy people sharing a handful of values is a demographic, not a name.
        self.assertNotIn("unique_per_subject", c.signals_of(self.profile(distinct_values=3, subjects=70), False))
        self.assertNotIn("unique_per_subject", c.signals_of(self.profile(distinct_values=20, subjects=20), False))

    def test_only_a_subjects_join_counts_subjects(self):
        self.assertIn("subjects is patient.count()", c.profile_query("billing", "patient.first_name", subject=True))
        self.assertNotIn("subjects is", c.profile_query("billing", "organization.organization_name"))


class ProposeTest(unittest.TestCase):
    def rows(self, *entries):
        return {r["path"]: r for r in c.propose(list(entries), REGISTER)}

    def entry(self, domain, path, classes, signals):
        return {"domain": domain, "path": path, "classifications": classes, "signals": signals}

    def test_a_floor_is_a_lower_bound_not_the_call(self):
        r = self.rows(self.entry("chart", "birth_date", ["phi.direct_identifier"], ["date_day"]),
                      self.entry("chart", "first_name", ["none"], ["unique_per_subject"]),
                      self.entry("chart", "visit_date", [], ["date_day"]),
                      self.entry("research", "birth_year", ["phi.quasi_identifier"], ["year"]),
                      self.entry("chart", "reason", ["phi.clinical"], []))
        self.assertEqual(r["birth_date"]["verdict"], "agrees")  # declared above a quasi floor
        self.assertEqual(r["first_name"]["verdict"], "below_floor")
        self.assertEqual(r["first_name"]["floor"], "phi.direct_identifier")
        self.assertEqual(r["visit_date"]["verdict"], "undeclared")
        self.assertEqual(r["birth_year"]["verdict"], "hint")
        self.assertIsNone(r["birth_year"]["floor"])
        self.assertEqual(r["reason"]["verdict"], "no_value_evidence")

    def test_one_row_per_field_across_domains(self):
        r = self.rows(self.entry("chart", "zip", ["phi.quasi_identifier"], ["zip5"]),
                      self.entry("lds", "zip", ["phi.quasi_identifier"], []))
        self.assertEqual(r["zip"]["domains"], ["chart", "lds"])
        self.assertIn("below what", c.proposals_md([{**r["zip"], "verdict": "below_floor"}]))


class ChecksTest(unittest.TestCase):
    def test_a_clean_package_holds(self):
        server = FakeServer(governance(), access={("chart", "clinical"): 40, ("research", "researcher"): 900})
        r = run(server, [
            {"id": "CL-01", "checks": ["coverage"]},
            {"id": "AC-02", "checks": ["locked_bases"]},
            {"id": "AC-03", "checks": ["secure_gates"]},
            {"id": "AC-04", "checks": ["ceiling", "values"]},
            {"id": "MN-01", "checks": ["access:chart"]},
            {"id": "AU-01", "status": "not_provable_here"},
        ])
        self.assertEqual(r["summary"], {"pass": 5, "fail": 0, "review": 0, "not_provable_here": 1})

    def test_scoped_means_a_row_filter_not_a_smaller_count(self):
        # The only persona admitted to `chart` reads the most rows anyone does;
        # it is still scoped, because a row filter is in force.
        server = FakeServer(governance(), access={("chart", "clinical"): 40, ("research", "researcher"): 900})
        r = run(server, [{"id": "MN-01", "checks": ["access:chart"]}])
        self.assertEqual(r["controls"][0]["status"], "pass")

    def test_a_dropped_row_filter_fails_the_scoped_expectation(self):
        gov = governance()
        gov["sources"][0]["accessFilter"] = []
        server = FakeServer(gov, access={("chart", "clinical"): 1000, ("research", "researcher"): 900})
        r = run(server, [{"id": "MN-01", "checks": ["access:chart"]},
                         {"id": "MN-04", "checks": ["access:research"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "pass"])

    def test_an_identifier_declared_none_fails(self):
        gov = governance()
        gov["sources"][1]["fields"].append(field("visit_ref", ["none"]))
        server = FakeServer(gov, access={("chart", "clinical"): 1, ("research", "researcher"): 1000},
                            profiles={("research", "visit_ref"): {"uuid": 1000, "distinct_values": 1000}})
        r = run(server, [{"id": "CL-02", "checks": ["values"]},
                         {"id": "MN-01", "checks": ["values:chart"]}])
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertIn("research.visit_ref", r["controls"][0]["findings"][0]["text"])
        # A check scoped elsewhere, whose findings all concern another domain, holds.
        self.assertEqual(r["controls"][1]["status"], "pass")

    def test_a_name_declared_none_fails_on_per_subject_uniqueness(self):
        gov = governance()
        gov["sources"][0]["fields"].append(field("patient.last_name", ["none"]))
        reg_server = FakeServer(gov, access={("chart", "clinical"): 40, ("research", "researcher"): 900},
                                profiles={("chart", "patient.last_name"): {"distinct_values": 64, "subjects": 70}})
        r = c.run_check(reg_server, {**REGISTER, "subjects": ["patient"],
                                     "controls": [{"id": "CL-02", "checks": ["values"]}]})
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertIn("64 distinct values for 70 patient records", r["controls"][0]["findings"][0]["evidence"])

    def test_deidentified_domains_refuse_day_dates_zip5_and_uncapped_values(self):
        server = FakeServer(governance(), access={("research", "researcher"): 1000},
                            profiles={("research", "year"): {"date_day": 1000},
                                      ("research", "age"): {"max_value": 104}})
        r = run(server, [{"id": "DI-01", "checks": ["values:research"]}])
        texts = " ".join(f["text"] for f in r["controls"][0]["findings"])
        self.assertIn("month and day", texts)
        self.assertIn("caps it at 90", texts)

    def test_a_forbidden_class_on_a_domains_ceiling_fails(self):
        gov = governance()
        gov["sources"][1]["carries"].append("phi.direct_identifier")
        r = run(FakeServer(gov), [{"id": "DI-03", "checks": ["ceiling:research"]}])
        self.assertEqual(r["controls"][0]["status"], "fail")

    def test_an_insecure_gate_fails(self):
        gov = governance()
        gov["sources"][0]["accessFilter"] = [gate("org in $ORGS", "ORGS", secure=False)]
        r = run(FakeServer(gov), [{"id": "AC-03", "checks": ["secure_gates"]}])
        self.assertEqual(r["controls"][0]["status"], "fail")

    def test_an_unlocked_unpublished_source_fails_but_a_domains_own_definition_does_not(self):
        gov = governance()
        gov["sources"].append(source("chart", [], published=False, model="domains.malloy"))
        gov["sources"].append(source("scratch", [], published=False, model="core.malloy"))
        r = run(FakeServer(gov), [{"id": "AC-02", "checks": ["locked_bases"]}])
        texts = [f["text"] for f in r["controls"][0]["findings"]]
        self.assertEqual(len(texts), 1)
        self.assertIn("scratch", texts[0])

    def test_a_stale_model_fails_the_control_its_refusal_breaks(self):
        server = FakeServer(governance(), load_errors=[{
            "stale": True,
            "message": "source research (index.malloy) carries [none] but exposes person.ssn, classified phi.direct_identifier.",
        }])
        r = run(server, [{"id": "AC-04", "checks": ["loads:ceiling"]},
                         {"id": "AC-05", "checks": ["loads:join"]},
                         {"id": "DI-03", "checks": ["loads:research"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "pass", "fail"])
        self.assertTrue(r["stale"])
        self.assertIn("still serving the model it compiled before", c.report_md(r))

    def test_an_expected_domain_the_package_does_not_publish_fails(self):
        server = FakeServer(governance(), access={("chart", "clinical"): 1, ("research", "researcher"): 1})
        r = c.run_check(server, {**REGISTER, "access": {**REGISTER["access"], "billing": {"clinician": "scoped"}},
                                 "controls": [{"id": "MN-02", "checks": ["access:billing"]}]})
        self.assertEqual(r["controls"][0]["status"], "fail")

    def test_an_undefined_persona_is_refused(self):
        with self.assertRaises(SystemExit):
            c.run_check(FakeServer(governance()), {**REGISTER, "access": {"chart": {"nobody": "rows"}},
                                                   "controls": [{"id": "X", "checks": ["access"]}]})

    def test_a_package_that_is_not_served_fails_by_its_refusal_and_lets_nothing_pass(self):
        class Unserved(FakeServer):
            def governance(self):
                raise c.urllib.error.HTTPError("u", 400, "Bad Request", {}, None)

        server = Unserved(governance(), load_errors=[{
            "message": "source research (index.malloy) carries [none] but exposes person.ssn, classified phi.direct_identifier."}])
        r = run(server, [{"id": "AC-04", "checks": ["loads:ceiling", "ceiling"]},
                         {"id": "AC-05", "checks": ["loads:join"]},
                         {"id": "MN-01", "checks": ["access:chart"]},
                         {"id": "AU-03", "checks": ["refusals"]},
                         {"id": "AU-01", "status": "not_provable_here"}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "review", "review", "pass", "not_provable_here"])
        self.assertTrue(r["unloaded"])
        self.assertIn("not serving this package", c.report_md(r))

    def test_an_unknown_package_fails(self):
        class Unknown(FakeServer):
            def governance(self):
                raise c.urllib.error.HTTPError("u", 404, "Not Found", {}, None)

        r = run(Unknown(governance()), [{"id": "AC-04", "checks": ["loads:ceiling"]}])
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertIn("does not serve", r["controls"][0]["findings"][0]["text"])

    def test_an_unknown_check_is_refused(self):
        with self.assertRaises(SystemExit):
            run(FakeServer(governance()), [{"id": "X-01", "checks": ["telepathy"]}])


class OutputTest(unittest.TestCase):
    def test_report_and_attestation_carry_every_control(self):
        r = run(FakeServer(governance(), access={("chart", "clinical"): 1, ("research", "researcher"): 2}),
                [{"id": "CL-01", "text": "Coverage.", "checks": ["coverage"]},
                 {"id": "AU-01", "text": "Audit.", "status": "not_provable_here", "why": "The log store's."}])
        md = c.report_md(r)
        self.assertIn("| CL-01 | Holds | Coverage. |", md)
        self.assertIn("The log store's.", md)
        att = c.attestation_md(r)
        self.assertIn("| AU-01 | Not provable here |", att)
        self.assertIn("Signature:", att)


if __name__ == "__main__":
    unittest.main()
