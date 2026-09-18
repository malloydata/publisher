#!/usr/bin/env python3
"""Tests for cluster_failures.py. Stdlib only: python3 cluster_failures_test.py

What these guard is the seam this file has no other protection against: it
CONSUMES `score_retrieval`'s `where_to_fix` vocabulary and used to spell every
value again in its own branches. When the vocabulary was renamed -- "query
construction" to `delivered, wrong`, "retrieval ranking" to `not retrieved` --
every branch silently stopped matching, the two commonest failure kinds fell to
the catch-all `other:` group, and their lever came back null. Nothing failed,
because this file had no tests at all.

So the rule these pin is: every label `score_retrieval` can emit reaches a
branch here, and the levers come from the owner it already assigned.
"""
from __future__ import annotations

import ast
import pathlib
import sys
import unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))

import cluster_failures as cf  # noqa: E402
import score_retrieval as sr  # noqa: E402

# Every attribution `score_retrieval.attribute()` can return. A new one added
# there without a branch here fails `test_every_label_reaches_a_branch`.
ATTRIBUTIONS = (sr.DELIVERED, sr.NOT_RETURNED, sr.NEVER_ASKED,
                sr.MODEL, sr.REFUSAL, sr.UNMEASURED)


def row(attribution, missing=("measure:m:total",), **kw):
    component, owner, where = attribution
    return {"qid": "q", "where_to_fix": where, "owner": owner,
            "component": component, "missing": list(missing), **kw}


class EveryLabelIsHandled(unittest.TestCase):
    def test_every_label_reaches_a_branch(self):
        """No emitted label may fall through to `other:`, except the one that
        genuinely has no specific edit behind it."""
        fell_through = []
        for att in ATTRIBUTIONS:
            key, _ = cf.cluster_key(row(att), {"tags": ["ratio"]})
            if key.startswith("other:"):
                fell_through.append(att[2])
        # `coverage not measured` is not an edit, it is the absence of a
        # measurement, so it has no group of its own by design.
        self.assertEqual(fell_through, [sr.UNMEASURED[2]])

    def test_no_label_is_spelled_as_a_literal_in_the_code(self):
        """The comments may quote the old names to explain the history; the
        CODE may not spell any label, or the next rename breaks it again."""
        tree = ast.parse(pathlib.Path(cf.__file__).read_text())
        literals = {n.value for n in ast.walk(tree)
                    if isinstance(n, ast.Constant)
                    and isinstance(n.value, str)}
        spelled = literals & {a[2] for a in ATTRIBUTIONS}
        self.assertEqual(spelled, set())
        # And they arrive from the one place that decides them.
        imported = {alias.name for n in ast.walk(tree)
                    if isinstance(n, ast.ImportFrom)
                    and n.module == "score_retrieval" for alias in n.names}
        self.assertLessEqual(
            {"DELIVERED", "NOT_RETURNED", "NEVER_ASKED", "MODEL", "REFUSAL"},
            imported)


class TheGroupAFailureLandsIn(unittest.TestCase):
    def key(self, attribution, case=None, **kw):
        return cf.cluster_key(row(attribution, **kw), case or {})[0]

    def test_a_delivered_wrong_failure_groups_by_its_construction_tag(self):
        self.assertEqual(
            self.key(sr.DELIVERED, {"tags": ["ratio"]}), "construction:ratio")

    def test_a_delivered_wrong_failure_without_a_tag_still_groups(self):
        self.assertEqual(self.key(sr.DELIVERED, {}), "construction:other")

    def test_an_unreturned_entity_groups_by_the_entity(self):
        self.assertEqual(self.key(sr.NOT_RETURNED),
                         "retrieval:measure:m:total")

    def test_never_asked_and_unreturned_are_different_groups(self):
        # Same entity, different edit: one is the docs, the other is the search
        # the agent never issued. Grouping them together proposes one fix for
        # two problems.
        self.assertNotEqual(self.key(sr.NEVER_ASKED),
                            self.key(sr.NOT_RETURNED))

    def test_a_model_gap_groups_on_its_coverage_note(self):
        self.assertEqual(
            self.key(sr.MODEL, {"coverageNote": "no measure; shipped_at"}),
            "model:no measure; shipped_at")

    def test_a_refusal_is_its_own_group(self):
        self.assertEqual(self.key(sr.REFUSAL), "refusal")


class TheLeverComesFromTheOwner(unittest.TestCase):
    """One decision about who owns a failure, not two that drift."""

    def lever(self, attribution):
        return cf.LEVER_BY_OWNER.get(attribution[1])

    def test_a_model_gap_is_fixed_in_the_model(self):
        self.assertEqual(self.lever(sr.MODEL), "model")

    def test_a_mechanical_agent_miss_is_fixed_in_the_skill(self):
        self.assertEqual(self.lever(sr.NEVER_ASKED), "skill")
        self.assertEqual(self.lever(sr.REFUSAL), "skill")

    def test_an_undecided_owner_names_no_lever(self):
        # Naming one would put back the default blame the taxonomy removed:
        # eval-diagnose decides these, sufficiency first.
        self.assertIsNone(self.lever(sr.DELIVERED))
        self.assertIsNone(self.lever(sr.NOT_RETURNED))

    def test_an_unknown_owner_names_no_lever(self):
        self.assertIsNone(self.lever(sr.UNMEASURED))


if __name__ == "__main__":
    unittest.main()
