#!/usr/bin/env python3
"""Tests for salvaging a per-case diagnosis out of a cluster-shaped reply.

Both prompts used to end with the same sentence pointing at the same reference
file, and that file defines two objects. 11 of 28 per-case replies in one run
came back as the clustering object: owner, component, codes and a root cause,
all real, in the wrong envelope, and every one of them discarded by a validator
looking for the per-case keys. About $12 of that run's $20.

The prompts now name their section, which is the actual fix. Salvage is the
belt: it lifts what is present and, deliberately, does not invent `probes`.
That last part is the one worth a test, because fabricating the record that
something was checked is how a shape error becomes a false claim."""
import pathlib
import sys
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import diagnose  # noqa: E402

CLUSTER_REPLY = {
    "clusters": [{
        "cluster_id": "index-values-not-whole",
        "qids": ["q1", "q2"],
        "owner": "model",
        "component": "model-definition",
        "codes": ["CONVENTION", "NO-DISAMBIG"],
        "rootCause": "the index measure returns long decimals",
        "evidence": "28 other places use render tags",
        "confidence": "high",
    }],
    "reasoning": "considered merging with the rounding cluster",
}


class SalvageClusterShape(unittest.TestCase):
    def test_lifts_the_fields_that_are_present(self):
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertEqual(out["owner"], "model")
        self.assertEqual(out["component"], "model-definition")
        self.assertEqual(out["primary_code"], "CONVENTION")
        self.assertEqual(out["contributing_codes"], ["NO-DISAMBIG"])
        self.assertEqual(out["diagnosis"], "the index measure returns long decimals")

    def test_never_synthesises_probes(self):
        # The field that must not be fabricated: it is the record that
        # something was checked, and the cluster shape has no probe records.
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertEqual(out["probes"], [])

    def test_marks_sufficiency_unknown_so_it_stays_ineligible(self):
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertEqual(out["sufficiency"], "unknown")

    def test_a_salvaged_object_still_fails_validation_on_probes(self):
        # Salvage must not launder a diagnosis into looking probed. The
        # vocabulary checks pass; "no probes recorded" must still fire.
        codes = diagnose.skill_codes()
        self.assertIn("CONVENTION", codes,
                      "the skill's own code table should define CONVENTION; "
                      "if it was renamed this test needs the new name")
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        bad = diagnose.validate(out, codes)
        self.assertEqual(bad, ["no probes recorded"],
                         "the vocabulary fields should all pass after salvage, "
                         "and the missing probe record should be the only "
                         "finding left")

    def test_says_it_was_salvaged(self):
        out = diagnose.salvage_cluster_shape(CLUSTER_REPLY)
        self.assertIn("clustering shape", out["_salvaged"])

    def test_records_when_more_than_one_cluster_was_offered(self):
        # Several clusters means the agent answered about the whole run from
        # one case's evidence; that must not read as a clean single cause.
        reply = {"clusters": [CLUSTER_REPLY["clusters"][0],
                              {"cluster_id": "other", "owner": "retrieval"}]}
        out = diagnose.salvage_cluster_shape(reply)
        self.assertIn("2 clusters", out["_salvagedFrom"])

    def test_a_per_case_reply_is_left_alone(self):
        self.assertIsNone(diagnose.salvage_cluster_shape(
            {"probes": [{"why": "w", "query": "q", "result": "r"}],
             "primary_code": "CONVENTION", "owner": "model"}))

    def test_an_empty_or_unparseable_reply_is_not_invented_into_a_diagnosis(self):
        for empty in ({}, {"clusters": []}, {"clusters": "not a list"},
                      {"clusters": ["a string, not an object"]}):
            self.assertIsNone(diagnose.salvage_cluster_shape(empty), empty)

    def test_a_cluster_with_no_codes_yields_no_primary_code(self):
        # Better an object the validator rejects for a missing code than one
        # carrying a code nobody chose.
        out = diagnose.salvage_cluster_shape(
            {"clusters": [{"owner": "model", "component": "model-definition"}]})
        self.assertNotIn("primary_code", out)


class ClusterIdNamesTheDefect(unittest.TestCase):
    """The contract now says an id names the defect, never the remedy.

    Pinned as text because the id is produced by an agent, not by this script,
    so the rule can only be carried in the prompt and the contract. If either
    stops saying it, the next run starts prescribing fixes again."""

    def test_the_rule_is_in_the_clustering_prompt(self):
        self.assertIn("names the DEFECT", diagnose.CLUSTER_PROMPT)
        for word in ("missing", "should", "add", "fix"):
            self.assertIn(word, diagnose.CLUSTER_PROMPT)

    def test_both_prompts_name_their_own_contract_section(self):
        # The ambiguous pointer that caused the 11 of 28. Each prompt must name
        # its section AND say the other object belongs to someone else.
        self.assertIn("## Per case", diagnose.DIAGNOSE_PROMPT)
        self.assertIn("## Per run", diagnose.DIAGNOSE_PROMPT)
        self.assertIn("## Per run, clustering", diagnose.CLUSTER_PROMPT)
        self.assertIn("## Per case", diagnose.CLUSTER_PROMPT)


if __name__ == "__main__":
    unittest.main()
