#!/usr/bin/env python3
"""Tests for the findability audit. No server: `get_context` is substituted.

The audit exists because `expectedEntities.required` is what "did the agent ask
for it" and "was it retrieved" are both computed against, so an id retrieval
cannot deliver makes both numbers fiction -- and the case then reports a
retrieval miss forever, which reads as the model's fault rather than the key's.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_findable  # noqa: E402

MODEL = {
    ("measure", "flight count"): "measure:flights:flight_count",
    ("measure", "average plane size"): "measure:flights:average_plane_size",
    ("dimension", "state"): "dimension:airports:state",
    ("source", "flights"): "source:flights:flights",
}


def fake_get_context(_url, targets, _env, _pkg, timeout=60):
    """A model that answers only a right-typed search for a name it has."""
    t = targets[0]
    eid = MODEL.get((t["target_type"], t["search_text"]))
    ents = ([{"entity_id": eid, "relevance": 1.0}]
            if eid and not eid.startswith("source:") else [])
    return {"sources": [{"source_info": {"resource_id": {"source": "flights"}},
                         "entities": ents}]}


def case(qid, required=(), any_of=()):
    return {"qid": qid, "expectedEntities": {"required": list(required),
                                             "requiredAnyOf": [list(g) for g in any_of]}}


class Findable(unittest.TestCase):
    def setUp(self):
        self._real = check_findable.get_context
        check_findable.get_context = fake_get_context

    def tearDown(self):
        check_findable.get_context = self._real

    def run_check(self, cases):
        return check_findable.check(cases, "http://x/mcp", "e", "p")

    def test_a_reachable_entity_is_no_finding(self):
        f, rows = self.run_check([case("q", ["measure:flights:flight_count"])])
        self.assertEqual(f, [])
        self.assertTrue(rows[0]["found"])

    def test_an_entity_the_model_does_not_have_is_caught(self):
        f, _ = self.run_check([case("q", ["measure:flights:revenue_per_mile"])])
        self.assertEqual(len(f), 1)
        self.assertIn("retrieval miss forever", f[0])

    def test_a_real_name_under_the_wrong_kind_is_caught(self):
        # The case a text grep cannot catch: `flight_count` IS in the model, so
        # checking the name against the source passes it. Only a search of the
        # declared kind shows that no `dimension` request can return it.
        f, _ = self.run_check([case("q", ["dimension:flights:flight_count"])])
        self.assertEqual(len(f), 1)
        self.assertIn("`dimension` search", f[0])

    def test_a_malformed_id_is_caught_before_any_search(self):
        f, rows = self.run_check([case("q", ["flight_count"])])
        self.assertEqual(len(f), 1)
        self.assertIn("malformed", f[0])
        self.assertEqual(rows, [])

    def test_an_unknown_kind_is_caught(self):
        f, _ = self.run_check([case("q", ["metric:flights:x"])])
        self.assertIn("unknown entity kind", f[0])

    def test_requiredanyof_members_are_checked_too(self):
        # A group is satisfied by any member, so a dead member is not fatal to
        # the case -- but it is still an id nothing can deliver, and a group
        # whose every member is dead is a case that can never score.
        f, rows = self.run_check([case("q", any_of=[
            ["measure:flights:flight_count", "measure:flights:gone"]])])
        # Both members are searched; only the dead one is a finding.
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(f), 1)
        self.assertIn("gone", f[0])

    def test_one_entity_required_by_six_cases_is_searched_once(self):
        cases = [case(f"q{i}", ["measure:flights:flight_count"]) for i in range(6)]
        _, rows = self.run_check(cases)
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]["requiredBy"]), 6)

    def test_the_finding_names_every_case_that_would_be_affected(self):
        f, _ = self.run_check([case("q1", ["measure:flights:gone"]),
                               case("q2", ["measure:flights:gone"])])
        self.assertIn("q1, q2", f[0])


class Phrase(unittest.TestCase):
    def test_an_identifier_becomes_the_easiest_possible_query(self):
        self.assertEqual(check_findable.phrase_for("average_plane_size"),
                         "average plane size")

    def test_a_join_path_is_flattened(self):
        self.assertEqual(
            check_findable.phrase_for("aircraft.aircraft_models.seats"),
            "aircraft aircraft models seats")


class KindMap(unittest.TestCase):
    def test_it_inverts_the_forward_map_the_server_pins(self):
        # `score_retrieval.KINDS_BY_TARGET` is pinned against the server's own
        # table. This is its inverse and must not drift from it.
        import score_retrieval
        for target, kinds in score_retrieval.KINDS_BY_TARGET.items():
            for kind in kinds:
                self.assertEqual(check_findable.TARGET_FOR_KIND.get(kind),
                                 target,
                                 f"{kind} should be searched as {target}")


class CompiledModel(unittest.TestCase):
    """The compiled model is the authority on existence AND kind.

    A grep over the `.malloy` text is neither, and is wrong in both directions:
    it passes a real name under the wrong kind, and it fails a column the
    source exposes implicitly.
    """

    DECLARED = {
        "flights": {"source:flights", "measure:flight_count",
                    "dimension:distance"},
        "airports": {"source:airports", "measure:airport_count",
                     "dimension:state", "dimension:own_type"},
    }

    def find(self, *ids):
        cases = [{"qid": "q", "expectedEntities": {"required": list(ids)}}]
        return check_findable.declared_findings(cases, self.DECLARED)

    def test_a_real_name_under_the_wrong_kind_names_the_real_kind(self):
        f = self.find("dimension:flights:flight_count")
        self.assertEqual(len(f), 1)
        self.assertIn("declared measure, not dimension", f[0])
        self.assertIn("hard filter", f[0])

    def test_an_implicit_column_is_NOT_a_finding(self):
        # The grep false positive: `own_type` appears zero times in the
        # .malloy and is exposed by the source. Acting on that finding deletes
        # a good entity from the key.
        self.assertEqual(self.find("dimension:airports:own_type"), [])

    def test_a_field_that_does_not_exist_is_a_finding(self):
        f = self.find("measure:flights:revenue_per_mile")
        self.assertIn("declares no field", f[0])

    def test_an_unknown_source_is_a_finding(self):
        f = self.find("measure:gone:x")
        self.assertIn("no source", f[0])

    def test_a_correct_id_is_silent(self):
        self.assertEqual(self.find("measure:flights:flight_count",
                                   "dimension:airports:state"), [])

    def test_a_malformed_id_is_left_to_the_other_check(self):
        # `check` reports it before any search; reporting it twice reads as
        # two defects.
        self.assertEqual(self.find("flight_count"), [])

    def test_an_unreachable_server_is_not_read_as_an_empty_model(self):
        # None means "not checked". Treating it as {} would report every id in
        # the set as missing.
        self.assertIsNone(check_findable.compiled_entities(
            "http://127.0.0.1:9", "e", "p"))


if __name__ == "__main__":
    unittest.main()
