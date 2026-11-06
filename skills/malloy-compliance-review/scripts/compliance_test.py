#!/usr/bin/env python3
"""Unit tests for compliance.py. Stdlib only: python3 -m unittest compliance_test

The checks run against a fake server built from an inventory of the published
domains and canned query answers, so every rule is pinned without a Publisher. Several tests pin
bugs found while building the clinical-records example: a reserved-word alias
that zeroed every pattern, a row-count reading of "scoped", and a stale model
that reported the controls it broke as holding.
"""
from __future__ import annotations

import contextlib
import io
import json
import pathlib
import re
import tempfile
import unittest
import urllib.error

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


def fixture(**over):
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

    def __init__(self, gov, access=None, profiles=None, load_errors=None, statuses=None,
                 numeric_errors=()):
        super().__init__("http://fake", "env", "pkg", "index.malloy")
        self._gov = gov
        self._access = access or {}
        self._profiles = profiles or {}
        self._statuses = statuses or {}  # (domain, first role) -> (status, message)
        self._numeric_errors = set(numeric_errors)
        self.load_errors = load_errors or []
        self.queries = []

    def inventory(self):
        """The published domains, with the summary's counts derived from the
        fields as they stand when the check runs."""
        out = []
        for s in self._gov["sources"]:
            if not s.get("published", True) or s.get("deniedToAll"):
                continue
            counts: dict[str, int] = {}
            for f in s["fields"]:
                for k in f["classifications"]:
                    counts[k] = counts.get(k, 0) + 1
            out.append({**s, "classCounts": counts, "fieldCount": len(s["fields"]),
                        "unclassified": sum(1 for f in s["fields"] if not f["classifications"])})
        return {"sources": out, "secureGivens": self._gov.get("secureGivens", []),
                "fieldsError": self._gov.get("fieldsError"),
                "downgrades": self._gov.get("downgrades", []),
                "bypasses": self._gov.get("bypasses", [])}

    def status(self):
        errs = [{**e, "environment": "env", "package": "pkg"} for e in self.load_errors]
        return {"operationalState": "serving", **({"loadErrors": errs} if errs else {})}

    def query(self, malloy, givens, model=None):
        self.queries.append((malloy, givens, model))
        m = re.match(r"run: (\w+) -> \{ aggregate: n is count\(\) \}", malloy)
        if m:
            role = (givens.get("ROLES") or [""])[0]
            if (m.group(1), role) in self._statuses:
                return (None, *self._statuses[(m.group(1), role)])
            n = self._access.get((m.group(1), role))
            return ([{"n": n}], None, None) if n is not None else (None, 403, "Access denied")
        m = re.match(r"run: (\w+) -> \{ aggregate: max_value", malloy)
        if m:
            path = re.search(r"max\(([\w.]+)\)", malloy).group(1)
            if (m.group(1), path) in self._numeric_errors:
                return None, 500, "boom"
            p = self._profiles.get((m.group(1), path), {})
            return [{"max_value": p.get("max_value"), "min_value": p.get("min_value", 0)}], None, None
        m = re.match(r"run: (\w+) -> \{ aggregate: rows is count\(\)", malloy)
        if m:
            path = re.search(r"distinct_values is count\(([\w.]+)\)", malloy).group(1)
            p = self._profiles.get((m.group(1), path), {})
            return [profile_row(**{k: v for k, v in p.items() if k not in ("max_value", "min_value")})], None, None
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
        server = FakeServer(fixture(), profiles={("chart", "ssn"): {"ssn": 1000}})
        out = c.profile_domain(server, REGISTER, server.inventory()["sources"][0])
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
        server = FakeServer(fixture(), access={("chart", "clinical"): 40, ("research", "researcher"): 900},
                            profiles={("research", "age"): {"max_value": 80}})
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        (pathlib.Path(tmp.name) / "base.malloy").write_text("#(authorize) false\nsource: base is duckdb.table('x')\n")
        (pathlib.Path(tmp.name) / "publisher.json").write_text('{"name": "pkg"}')
        server.package_dir = tmp.name
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
        server = FakeServer(fixture(), access={("chart", "clinical"): 40, ("research", "researcher"): 900})
        r = run(server, [{"id": "MN-01", "checks": ["access:chart"]}])
        self.assertEqual(r["controls"][0]["status"], "pass")

    def test_a_dropped_row_filter_fails_the_scoped_expectation(self):
        gov = fixture()
        gov["sources"][0]["accessFilter"] = []
        server = FakeServer(gov, access={("chart", "clinical"): 1000, ("research", "researcher"): 900})
        r = run(server, [{"id": "MN-01", "checks": ["access:chart"]},
                         {"id": "MN-04", "checks": ["access:research"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "pass"])

    def test_an_identifier_declared_none_fails(self):
        gov = fixture()
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
        gov = fixture()
        gov["sources"][0]["fields"].append(field("patient.last_name", ["none"]))
        reg_server = FakeServer(gov, access={("chart", "clinical"): 40, ("research", "researcher"): 900},
                                profiles={("chart", "patient.last_name"): {"distinct_values": 64, "subjects": 70}})
        r = c.run_check(reg_server, {**REGISTER, "subjects": ["patient"],
                                     "controls": [{"id": "CL-02", "checks": ["values"]}]})
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertIn("64 distinct values for 70 patient records", r["controls"][0]["findings"][0]["evidence"])

    def test_deidentified_domains_refuse_day_dates_zip5_and_uncapped_values(self):
        server = FakeServer(fixture(), access={("research", "researcher"): 1000},
                            profiles={("research", "year"): {"date_day": 1000},
                                      ("research", "age"): {"max_value": 104}})
        r = run(server, [{"id": "DI-01", "checks": ["values:research"]}])
        texts = " ".join(f["text"] for f in r["controls"][0]["findings"])
        self.assertIn("month and day", texts)
        self.assertIn("caps it at 90", texts)

    def test_a_forbidden_class_on_a_domains_ceiling_fails(self):
        gov = fixture()
        gov["sources"][1]["carries"].append("phi.direct_identifier")
        r = run(FakeServer(gov), [{"id": "DI-03", "checks": ["ceiling:research"]}])
        self.assertEqual(r["controls"][0]["status"], "fail")

    def test_an_insecure_gate_fails(self):
        gov = fixture()
        gov["sources"][0]["accessFilter"] = [gate("org in $ORGS", "ORGS", secure=False)]
        r = run(FakeServer(gov), [{"id": "AC-03", "checks": ["secure_gates"]}])
        self.assertEqual(r["controls"][0]["status"], "fail")

    def test_an_unpublished_source_that_answers_fails_and_a_published_one_is_not_probed(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            (root / "base.malloy").write_text("#(authorize) false\nsource: base is duckdb.table('x')\n")
            (root / "publisher.json").write_text('{"name": "pkg"}')
            (root / "core.malloy").write_text("source: scratch is base extend {}\n")
            # A domain is defined in one file and re-exported by another: published.
            (root / "domains.malloy").write_text("source: chart is base extend {}\n")
            server = FakeServer(fixture(), access={("scratch", "clinical"): 7})
            server.package_dir = d
            r = run(server, [{"id": "AC-02", "checks": ["locked_bases"]}])
        f = r["controls"][0]["findings"]
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertEqual(len(f), 1)
        self.assertIn("scratch (core.malloy)", f[0]["text"])
        probed = {q[0].split()[1]: q[2] for q in server.queries}
        self.assertEqual(probed, {"base": "base.malloy", "scratch": "core.malloy"})
        # The probe holds every role any persona holds.
        self.assertEqual(server.queries[0][1]["ROLES"], ["clinical", "researcher"])

    def test_locked_bases_needs_the_package_files(self):
        with tempfile.TemporaryDirectory() as d:
            server = FakeServer(fixture())
            server.package_dir = d
            r = run(server, [{"id": "AC-02", "checks": ["locked_bases"]}])
        self.assertEqual(r["controls"][0]["status"], "review")

    def test_a_stale_model_fails_the_control_its_refusal_breaks(self):
        server = FakeServer(fixture(), load_errors=[{
            "stale": True,
            "message": "source research (index.malloy) carries [none] but exposes person.ssn, classified phi.direct_identifier.",
        }])
        r = run(server, [{"id": "AC-04", "checks": ["loads:ceiling"]},
                         {"id": "AC-05", "checks": ["loads:join"]},
                         {"id": "DI-03", "checks": ["loads:research"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "pass", "fail"])
        self.assertTrue(r["stale"])
        self.assertIn("still serving the model it compiled before", c.report_md(r))

    def test_a_join_bypass_warning_on_a_served_package_fails_loads_join_and_nothing_else(self):
        gov = fixture()
        gov["bypasses"] = [{"code": "join-bypass", "subject": "billing",
                            "message": "source billing (index.malloy) joins patient, whose gate reads $ROLES, "
                                       "and the join bypasses the lock on patient."}]
        server = FakeServer(gov)
        r = run(server, [{"id": "AC-04", "checks": ["loads:ceiling"]},
                         {"id": "AC-05", "checks": ["loads:join"]},
                         {"id": "AC-06", "checks": ["loads"]},
                         {"id": "AC-07", "checks": ["loads:research"]},
                         {"id": "AU-03", "checks": ["refusals"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["pass", "fail", "fail", "pass", "pass"])
        f = r["controls"][1]["findings"]
        self.assertEqual(f[0]["status"], "pass")
        self.assertEqual((f[1]["status"], f[1]["kind"], f[1]["domain"]), ("fail", "join", "billing"))
        self.assertIn("bypasses the lock", f[1]["evidence"])
        self.assertFalse(r["stale"])
        self.assertNotIn("was refused at load", r["controls"][4]["findings"][0]["text"])

    def test_a_clean_package_loads(self):
        r = run(FakeServer(fixture()), [{"id": "AC-05", "checks": ["loads:join"]}, {"id": "AC-06", "checks": ["loads"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["pass", "pass"])

    def test_an_expected_domain_the_package_does_not_publish_fails(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 1, ("research", "researcher"): 1})
        r = c.run_check(server, {**REGISTER, "access": {**REGISTER["access"], "billing": {"clinician": "scoped"}},
                                 "controls": [{"id": "MN-02", "checks": ["access:billing"]}]})
        self.assertEqual(r["controls"][0]["status"], "fail")

    def test_an_undefined_persona_is_refused(self):
        with self.assertRaises(SystemExit):
            c.run_check(FakeServer(fixture()), {**REGISTER, "access": {"chart": {"nobody": "rows"}},
                                                   "controls": [{"id": "X", "checks": ["access"]}]})

    def test_a_package_that_is_not_served_fails_by_its_refusal_and_lets_nothing_pass(self):
        class Unserved(FakeServer):
            def inventory(self):
                raise c.urllib.error.HTTPError("u", 400, "Bad Request", {}, None)

        server = Unserved(fixture(), load_errors=[{
            "message": "source research (index.malloy) carries [none] but exposes person.ssn, classified phi.direct_identifier."}])
        r = run(server, [{"id": "AC-04", "checks": ["loads:ceiling", "ceiling"]},
                         {"id": "AC-05", "checks": ["loads:join"]},
                         {"id": "MN-01", "checks": ["access:chart"]},
                         {"id": "AU-03", "checks": ["refusals"]},
                         {"id": "AU-01", "status": "not_provable_here"}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "review", "review", "pass", "not_provable_here"])
        self.assertTrue(r["unloaded"])
        self.assertIn("not serving this package", c.report_md(r))

    def test_an_unknown_package_is_review_because_nothing_was_checked(self):
        class Unknown(FakeServer):
            def inventory(self):
                raise c.urllib.error.HTTPError("u", 404, "Not Found", {}, None)

        r = run(Unknown(fixture()), [{"id": "AC-04", "checks": ["loads:ceiling"]},
                                     {"id": "AC-03", "checks": ["access:chart"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["review", "review"])
        self.assertIn("does not serve", r["controls"][0]["findings"][0]["text"])

    def test_an_unknown_check_is_refused(self):
        with self.assertRaises(SystemExit):
            run(FakeServer(fixture()), [{"id": "X-01", "checks": ["telepathy"]}])


def package_dir(files):
    files = {"publisher.json": '{"name": "pkg"}', **files}
    tmp = tempfile.TemporaryDirectory()
    for name, text in files.items():
        (pathlib.Path(tmp.name) / name).write_text(text)
    return tmp


SURFACE_404 = (404, "Query target is not queryable.")


class UncheckedIsNotPassedTest(unittest.TestCase):
    """What the server answered with neither data nor a refusal is not a pass."""

    def locked(self, statuses, scope="", manifest='{"name": "pkg"}'):
        tmp = package_dir({"base.malloy": "#(authorize) false\nsource: base is duckdb.table('x')\n",
                           "publisher.json": manifest or ""})
        if manifest is None:
            (pathlib.Path(tmp.name) / "publisher.json").unlink()
        self.addCleanup(tmp.cleanup)
        server = FakeServer(fixture(), statuses=statuses)
        server.package_dir = tmp.name
        return run(server, [{"id": "AC-02", "checks": ["locked_bases" + (f":{scope}" if scope else "")]}])["controls"][0]

    def test_a_locked_probe_answered_400_for_a_typo_is_review_not_pass(self):
        ctl = self.locked({("base", "clinical"): (400, "no such field")})
        self.assertEqual(ctl["status"], "review")
        self.assertIn("400", ctl["findings"][0]["evidence"])
        self.assertEqual(ctl["findings"][0]["domain"], "base")

    def test_a_locked_probe_refused_by_the_surface_or_a_gate_passes(self):
        self.assertEqual(self.locked({("base", "clinical"): SURFACE_404})["status"], "pass")
        self.assertEqual(self.locked({("base", "clinical"): (403, "Access denied")})["status"], "pass")

    def test_a_404_that_is_not_the_surface_refusal_is_review(self):
        self.assertEqual(self.locked({("base", "clinical"): (404, "Some other 404")})["status"], "review")

    def test_a_locked_probe_of_a_model_the_surface_hides_passes(self):
        self.assertEqual(
            self.locked({("base", "clinical"): (404, 'No queryable model "base.malloy".')})["status"], "pass")

    def test_a_package_dir_that_is_not_this_package_is_review(self):
        hidden = {("base", "clinical"): (404, 'No queryable model "base.malloy".')}
        ctl = self.locked(hidden, manifest=None)
        self.assertEqual(ctl["status"], "review")
        self.assertIn("--package-dir", ctl["findings"][0]["evidence"])

    def test_a_locked_probe_hidden_by_the_surface_still_passes_beside_a_wrong_model_path(self):
        self.assertEqual(self.locked({("base", "clinical"): (404, 'No queryable source "base".')})["status"], "pass")

    def test_an_unknown_locked_bases_scope_fails_and_a_declared_one_holds(self):
        ctl = self.locked({}, scope="patiens")
        self.assertEqual(ctl["status"], "fail")
        self.assertIn("unknown scope", ctl["findings"][0]["text"])
        self.assertEqual(self.locked({}, scope="base")["status"], "pass")

    def test_an_unknown_scope_on_a_domain_check_fails(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 1, ("research", "researcher"): 1})
        r = run(server, [{"id": "X", "checks": ["values:resarch"]}, {"id": "Y", "checks": ["loads:ceiling"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["fail", "pass"])

    def test_a_persona_probe_that_is_not_a_refusal_is_review(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 40, ("research", "researcher"): 900},
                            statuses={("research", "clinical"): (401, "no token"),
                                      ("chart", "researcher"): SURFACE_404})
        r = run(server, [{"id": "MN-01", "checks": ["access"]}])
        self.assertEqual([f["status"] for f in r["controls"][0]["findings"]], ["review"])
        self.assertIn("401", r["controls"][0]["findings"][0]["evidence"])

    def test_a_missing_model_404_is_review_not_a_refusal(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 40, ("research", "researcher"): 900},
                            statuses={("research", "clinical"): (404, 'No queryable model "index.malloy".')})
        r = run(server, [{"id": "MN-01", "checks": ["access"]}])
        self.assertEqual(r["controls"][0]["status"], "review")
        self.assertIn("404", r["controls"][0]["findings"][0]["evidence"])

    def test_a_scoped_values_check_is_review_when_nothing_could_be_profiled(self):
        gov = fixture()
        gov["fieldsError"] = "get_context failed: connection refused"
        server = FakeServer(gov, access={("chart", "clinical"): 1, ("research", "researcher"): 1})
        r = run(server, [{"id": "DI-01", "checks": ["values:research"]}])
        self.assertEqual(r["controls"][0]["status"], "review")
        self.assertIn("get_context", r["controls"][0]["findings"][0]["evidence"])

    def test_a_scoped_values_check_still_takes_only_its_domains_findings(self):
        gov = fixture()
        gov["sources"][0]["fields"] = [field("ssn", [])]
        server = FakeServer(gov, access={("chart", "clinical"): 1, ("research", "researcher"): 1},
                            profiles={("chart", "ssn"): {"ssn": 1000}, ("research", "age"): {"max_value": 80}})
        r = run(server, [{"id": "DI-01", "checks": ["values:research"]}])
        self.assertEqual(r["controls"][0]["status"], "pass")

    def test_a_gated_domain_answering_with_no_identity_fails(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 40, ("research", "researcher"): 900,
                                               ("chart", ""): 40})
        r = run(server, [{"id": "AC-01", "checks": ["access"]}])
        f = r["controls"][0]["findings"]
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertIn("no identity", f[0]["text"])
        self.assertEqual(f[0]["domain"], "chart")
        self.assertIn({}, [q[1] for q in server.queries])

    def anonymous(self, gates, rows):
        """The access check of one domain, gated as `gates` says, that answers `rows` to a caller with no identity."""
        src = source("ledger", [field("amount", ["none"])], carries=["none"], **gates)
        reg = {**REGISTER, "access": {"ledger": {"clinician": "rows", "researcher": "rows"}},
               "controls": [{"id": "AC-01", "checks": ["access"]}]}
        server = FakeServer(fixture(sources=[src]),
                            access={("ledger", "clinical"): 3, ("ledger", "researcher"): 3, ("ledger", ""): rows})
        return c.run_check(server, reg)["controls"][0]

    def test_a_row_filtered_only_domain_answering_zero_rows_with_no_identity_holds(self):
        ctl = self.anonymous({"access_filter": [gate("org in $ORGS", "ORGS")]}, 0)
        self.assertEqual([f["text"] for f in ctl["findings"] if "no identity" in f["text"]], [])

    def test_a_row_filtered_only_domain_answering_rows_with_no_identity_fails(self):
        ctl = self.anonymous({"access_filter": [gate("org in $ORGS", "ORGS")]}, 3)
        self.assertTrue(any("no identity" in f["text"] and f["status"] == "fail" for f in ctl["findings"]))

    def test_an_authorize_gated_domain_answering_zero_rows_with_no_identity_fails(self):
        ctl = self.anonymous({"authorize": [gate("'clinical' in $ROLES", "ROLES")]}, 0)
        self.assertTrue(any("no identity" in f["text"] and f["status"] == "fail" for f in ctl["findings"]))

    def test_a_domain_gated_by_both_answering_zero_rows_with_no_identity_fails(self):
        ctl = self.anonymous({"authorize": [gate("'clinical' in $ROLES", "ROLES")],
                              "access_filter": [gate("org in $ORGS", "ORGS")]}, 0)
        self.assertTrue(any("no identity" in f["text"] and f["status"] == "fail" for f in ctl["findings"]))

    def test_a_scope_naming_a_register_domain_the_package_does_not_publish_fails(self):
        reg = {**REGISTER, "domains": {**REGISTER["domains"], "archive": {}}}
        server = FakeServer(fixture(), access={("chart", "clinical"): 1, ("research", "researcher"): 1})
        names = [f"{k}:archive" for k in ("values", "ceiling", "coverage", "gated", "secure_gates")]
        r = c.run_check(server, {**reg, "controls": [{"id": n, "checks": [n]} for n in names]})
        self.assertEqual([x["status"] for x in r["controls"]], ["fail"] * len(names))
        self.assertIn("unknown scope", r["controls"][0]["findings"][0]["text"])

    def test_a_scope_naming_a_published_domain_still_holds(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 1, ("research", "researcher"): 1})
        r = run(server, [{"id": "X", "checks": ["ceiling:research", "coverage:research"]}])
        self.assertEqual(r["controls"][0]["status"], "pass")

    def test_a_gated_domain_whose_anonymous_probe_errors_is_review(self):
        server = FakeServer(fixture(), access={("chart", "clinical"): 40, ("research", "researcher"): 900},
                            statuses={("chart", ""): (500, "boom")})
        r = run(server, [{"id": "AC-01", "checks": ["access"]}])
        self.assertEqual(r["controls"][0]["status"], "review")

    def test_a_cap_on_a_field_that_was_not_profiled_is_review(self):
        reg = {**REGISTER, "domains": {"research": {"caps": [{"field": "agee", "max": 90}]}},
               "controls": [{"id": "DI-01", "checks": ["values:research"]}]}
        server = FakeServer(fixture(), access={("research", "researcher"): 900})
        r = c.run_check(server, reg)
        self.assertEqual(r["controls"][0]["status"], "review")
        self.assertIn("agee", r["controls"][0]["findings"][0]["text"])

    def test_a_cap_whose_numeric_query_errored_is_review(self):
        server = FakeServer(fixture(), access={("research", "researcher"): 900}, numeric_errors={("research", "age")})
        r = run(server, [{"id": "DI-01", "checks": ["values:research"]}])
        self.assertEqual(r["controls"][0]["status"], "review")
        self.assertIn("500", r["controls"][0]["findings"][0]["evidence"])

    def test_a_cap_can_declare_a_minimum(self):
        reg = {**REGISTER, "domains": {"research": {"caps": [{"field": "age", "min": 1930}]}},
               "controls": [{"id": "DI-01", "checks": ["values:research"]}]}
        server = FakeServer(fixture(), access={("research", "researcher"): 900},
                            profiles={("research", "age"): {"max_value": 2000, "min_value": 1900}})
        r = c.run_check(server, reg)
        self.assertEqual(r["controls"][0]["status"], "fail")
        self.assertIn("1900", r["controls"][0]["findings"][0]["text"])

    def test_numeric_extremes_are_kept_only_for_capped_fields(self):
        server = FakeServer(fixture(), access={("research", "researcher"): 900},
                            profiles={("research", "age"): {"max_value": 80}, ("research", "year"): {"max_value": 2020}})
        out = c.profile_domain(server, REGISTER, server.inventory()["sources"][1])
        by_path = {e["path"]: e["profile"] for e in out}
        self.assertEqual(by_path["age"]["max_value"], 80)
        self.assertNotIn("max_value", by_path["year"])
        self.assertNotIn("min_value", by_path["year"])

    def test_refusals_on_an_unserved_package_is_not_checked_rather_than_holding(self):
        class Unserved(FakeServer):
            def inventory(self):
                raise c.urllib.error.HTTPError("u", 404, "Not Found", {}, None)

        r = run(Unserved(fixture()), [{"id": "AU-03", "checks": ["refusals"]}])
        self.assertEqual(r["controls"][0]["status"], "review")

    def test_a_downgrade_warning_is_review_and_the_value_profile_fails_the_field(self):
        # `concat(social, '')` declared "none": Publisher warns, the values prove it.
        gov = fixture()
        gov["sources"][1]["fields"].append(field("x", ["none"]))
        gov["downgrades"] = [{"model": "index.malloy", "subject": "research",
                              "message": "Field x of source research declares none but reads fields classified phi.direct_identifier."}]
        server = FakeServer(gov, access={("research", "researcher"): 1},
                            profiles={("research", "x"): {"ssn": 1000, "distinct_values": 900}})
        r = run(server, [{"id": "CL-01", "checks": ["coverage"]}, {"id": "CL-02", "checks": ["values"]}])
        self.assertEqual([x["status"] for x in r["controls"]], ["review", "fail"])
        self.assertEqual(r["controls"][0]["findings"][0]["domain"], "research")
        self.assertIn("research.x", r["controls"][1]["findings"][0]["text"])


class ExitCodeTest(unittest.TestCase):
    def main(self, server, *extra, server_url="http://localhost:4000", controls=None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        reg = pathlib.Path(tmp.name) / "controls.json"
        reg.write_text(json.dumps({**REGISTER, "controls": controls or [{"id": "CL-01", "checks": ["coverage"]}]}))
        argv = ["check", "--server", server_url, "--environment", "env", "--package", "pkg",
                "--register", str(reg), "--out-dir", str(pathlib.Path(tmp.name) / "out"), *extra]
        real, c._server = c._server, (lambda a: server)
        self.addCleanup(setattr, c, "_server", real)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = c.main(argv)
        return code, err.getvalue()

    def unclassified(self):
        gov = fixture()
        gov["sources"][1]["fields"].append(field("x", []))
        return FakeServer(gov)

    def test_pass_exits_0(self):
        self.assertEqual(self.main(FakeServer(fixture()))[0], 0)

    def test_a_failing_control_exits_1(self):
        self.assertEqual(self.main(self.unclassified())[0], 1)

    def test_a_review_with_no_failure_exits_3_and_allow_review_makes_it_0(self):
        gov = fixture()
        gov["downgrades"] = [{"subject": "chart", "message": "Field x declares none but reads phi."}]
        self.assertEqual(self.main(FakeServer(gov))[0], 3)
        self.assertEqual(self.main(FakeServer(gov), "--allow-review")[0], 0)

    def test_allow_review_does_not_hide_a_failure(self):
        self.assertEqual(self.main(self.unclassified(), "--allow-review")[0], 1)

    def test_an_unexpected_exception_exits_4_with_a_traceback(self):
        class Boom(FakeServer):
            def inventory(self):
                raise RuntimeError("kaboom")

        code, err = self.main(Boom(fixture()))
        self.assertEqual(code, 4)
        self.assertIn("Traceback", err)
        self.assertIn("kaboom", err)

    def test_a_remote_server_without_mcp_is_a_usage_error(self):
        with self.assertRaises(SystemExit) as cm:
            self.main(FakeServer(fixture()), server_url="https://publisher.example.com")
        self.assertEqual(cm.exception.code, 2)

    def test_a_remote_server_with_mcp_runs(self):
        code, _ = self.main(FakeServer(fixture()), "--mcp", "https://publisher.example.com/mcp",
                            server_url="https://publisher.example.com")
        self.assertEqual(code, 0)

    def test_a_register_that_cannot_be_run_is_a_usage_error_not_a_failure(self):
        code, err = self.main(FakeServer(fixture()), controls=[{"id": "X", "checks": ["telepathy"]}])
        self.assertEqual(code, 2)
        self.assertIn("telepathy", err)


class RegisterLoadTest(unittest.TestCase):
    """A register that cannot be read is a usage error (2), not an internal one (4)."""

    def code(self, text):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        reg = pathlib.Path(tmp.name) / "controls.json"
        if text is not None:
            reg.write_text(text)
        argv = ["check", "--environment", "env", "--package", "pkg", "--register", str(reg),
                "--out-dir", str(pathlib.Path(tmp.name) / "out")]
        real, c._server = c._server, (lambda a: FakeServer(fixture()))
        self.addCleanup(setattr, c, "_server", real)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return c.main(argv)

    def test_a_missing_file_is_2(self):
        self.assertEqual(self.code(None), 2)

    def test_bad_json_is_2(self):
        self.assertEqual(self.code("{not json"), 2)

    def test_a_register_with_no_controls_is_2(self):
        self.assertEqual(self.code(json.dumps({"personas": {}})), 2)


class OutputTest(unittest.TestCase):
    def test_report_and_attestation_carry_every_control(self):
        r = run(FakeServer(fixture(), access={("chart", "clinical"): 1, ("research", "researcher"): 2}),
                [{"id": "CL-01", "text": "Coverage.", "checks": ["coverage"]},
                 {"id": "AU-01", "text": "Audit.", "status": "not_provable_here", "why": "The log store's."}])
        md = c.report_md(r)
        self.assertIn("| CL-01 | Holds | Coverage. |", md)
        self.assertIn("The log store's.", md)
        att = c.attestation_md(r)
        self.assertIn("| AU-01 | Not provable here |", att)
        self.assertIn("Signature:", att)



class ClassificationSummaryRead(unittest.TestCase):
    """The summary comes from its own route; an older server's package read
    carries it instead."""

    def server(self, routes):
        srv = c.Server("http://fake", "env", "pkg", "index.malloy")

        def get_json(path):
            if path in routes:
                return routes[path]
            raise urllib.error.HTTPError(path, 404, "Not Found", None, None)

        srv.get_json = get_json
        return srv

    def test_the_route_is_read(self):
        summary = {"sources": [{"name": "chart"}]}
        srv = self.server({"/classification": summary})
        self.assertEqual(srv.classification_summary({}), summary)

    def test_an_older_server_is_read_from_the_package(self):
        summary = {"sources": [{"name": "chart"}]}
        srv = self.server({})
        self.assertEqual(srv.classification_summary({"classification": summary}), summary)

    def test_none_anywhere_is_no_domains(self):
        self.assertEqual(self.server({}).classification_summary({}), {"sources": []})

if __name__ == "__main__":
    unittest.main()
