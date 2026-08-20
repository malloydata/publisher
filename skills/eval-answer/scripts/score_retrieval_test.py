#!/usr/bin/env python3
import os
import unittest

from score_retrieval import matches, score


RANKED = {
    "entityIds": [
        "dimension:space_detail:floor_key",
        "join:space_detail:space_floor",
        "dimension:space_floor:floor_name",
    ],
    "ranks": [1, 2, 3],
}


class RetrievalScoreTests(unittest.TestCase):
    def test_mrr_and_recall(self):
        out = score(["space_floor", "floor_name"], RANKED)
        self.assertEqual(out["n_needed"], 2)
        self.assertEqual(out["n_found"], 2)
        self.assertEqual(out["context_recall"], 1.0)
        self.assertAlmostEqual(out["mrr"], (1 / 2 + 1 / 3) / 2)
        self.assertAlmostEqual(out["rr_first"], 0.5)
        self.assertEqual(out["ranks"]["space_floor"], 2)
        self.assertEqual(out["ranks"]["floor_name"], 3)

    def test_missing_entity_is_zero_rr(self):
        out = score(["space_floor", "space_usage"], RANKED)
        self.assertEqual(out["n_found"], 1)
        self.assertEqual(out["context_recall"], 0.5)
        self.assertAlmostEqual(out["mrr"], 0.25)
        self.assertIsNone(out["ranks"]["space_usage"])
        self.assertEqual(out["reciprocal_ranks"]["space_usage"], 0.0)

    def test_best_rank_across_calls(self):
        ranked = [
            {"entityIds": ["join:space_detail:space_floor"], "ranks": [8]},
            {"entityIds": ["join:space_detail:space_floor"], "ranks": [2]},
        ]
        out = score(["space_floor"], ranked)
        self.assertEqual(out["ranks"]["space_floor"], 2)
        self.assertAlmostEqual(out["mrr"], 0.5)

    def test_kind_and_name_match(self):
        self.assertTrue(matches("join:space_floor", "join:space_detail:space_floor"))
        self.assertFalse(matches("source:space_floor", "join:space_detail:space_floor"))

    def test_bare_name_does_not_substring(self):
        self.assertTrue(matches("floor_key", "dimension:space_detail:floor_key"))
        self.assertFalse(matches("floor", "dimension:space_detail:floor_key"))

    def test_max_rank_counts_as_miss(self):
        out = score(["space_floor"], RANKED, max_rank=1)
        self.assertEqual(out["n_found"], 0)
        self.assertEqual(out["mrr"], 0.0)
        self.assertIsNone(out["ranks"]["space_floor"])

    def test_empty_needed_raises(self):
        with self.assertRaises(ValueError):
            score([], RANKED)

    def test_ranks_optional_means_order(self):
        out = score(
            ["floor_name"],
            {"entityIds": ["dimension:space_floor:floor_name"]},
        )
        self.assertEqual(out["ranks"]["floor_name"], 1)
        self.assertEqual(out["mrr"], 1.0)


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
