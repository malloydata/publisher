#!/usr/bin/env python3
"""python mcp_payload_test.py

The fixtures are trimmed copies of real `malloy_getContext` exchanges. The
regression they exist for: an extractor written against the flat `results`
shape returned [] for the nested one, which scored attempts that had received
everything at 0% recall and moved three baseline failures off "query
construction" onto "retrieval ranking" and "model coverage" -- the wrong team,
for a bug in the eval.
"""
import unittest

from mcp_payload import entity_ids, search_terms

FLAT = {"results": [
    {"kind": "measure", "name": "total_sales_2022", "source": "order_items",
     "doc": "Total sales in 2022"},
    {"kind": "view", "name": "sales_by_month_2022", "source": "order_items"},
]}

NESTED = {"ranking": "relevance", "total_available": 1, "sources": [
    {"name": "order_items", "modelPath": "ecommerce.malloy",
     "joins": [{"name": "users", "relationship": "one"},
               {"name": "inventory_items", "relationship": "one"}],
     "resource_id": {"environment": "samples", "package": "ecommerce",
                     "source": "order_items"},
     "entities": {
         "dimensions": [
             {"entityId": "dimension:order_items:created_at",
              "kind": "dimension", "name": "created_at",
              "source": "order_items", "rank": 1},
             {"entityId": "dimension:order_items:shipped_at",
              "kind": "dimension", "name": "shipped_at",
              "source": "order_items", "rank": 4}],
         "measures": [
             {"entityId": "measure:order_items:total_sales",
              "kind": "measure", "name": "total_sales",
              "source": "order_items", "rank": 1}]}}]}

PER_TARGET = {"targets": [
    {"target_type": "source", "search_text": "sales revenue by year", "results": [
        {"entityId": "source:order_items:order_items", "kind": "source",
         "name": "order_items", "source": "order_items", "rank": 1}]},
    {"target_type": "measure", "search_text": "revenue", "results": [
        {"entityId": "measure:order_items:total_sales", "kind": "measure",
         "name": "total_sales", "source": "order_items", "rank": 1}]}]}

# Transcribed from a hosted retrieval API's published response schema,
# GetContextResponse. Note what it does NOT carry: no `entityId`, no `kind`.
# The identity has to be assembled from source_info.resource_id.source plus the
# entity's own name and entity_type. NESTED above is a different, id-bearing
# shape and is not this one -- keeping both is the point.
HOSTED = {
    "ranking": "relevance", "total_available": 1, "returned": 1,
    "sources": [{
        "source_info": {
            "resource_id": {"environment": "samples", "package": "ecommerce",
                            "model_path": "ecommerce.malloy",
                            "source": "order_items"},
            "one_line_summary": "Order line items",
            "docs": "One row per line item."},
        "relevance": 0.91,
        "matched_targets": [{"search_text": "revenue", "relevance": 0.91}],
        "entities": [
            {"name": "total_sales", "entity_type": "measure", "relevance": 0.93,
             "description": "Sum of sale price"},
            {"name": "created_at", "entity_type": "dimension", "relevance": 0.55}]}]}

# The same exchange from a local Publisher, after malloydata/publisher#1028
# converged its get_context on the shape above. Same skeleton; it additionally
# states `entity_id` rather than leaving it to be assembled, and it retrieves
# a `join`, which is outside that host's three-value entity_type enum.
PUBLISHER = {
    "ranking": "relevance", "total_available": 1, "returned": 1,
    "retrieval": "semantic", "below_cutoff_count": 4, "total_entities": 31,
    "sources": [{
        "source_info": {
            "resource_id": {"environment": "samples", "package": "ecommerce",
                            "model_path": "ecommerce.malloy",
                            "source": "order_items"},
            "docs": "One row per line item.",
            "joins": [{"name": "users", "relationship": "one"}]},
        "relevance": 0.93,
        "entities": [
            {"name": "total_sales", "entity_type": "measure", "relevance": 0.93,
             "entity_id": "measure:order_items:total_sales",
             "description": "Sum of sale price"},
            {"name": "created_at", "entity_type": "dimension", "relevance": 0.55,
             "entity_id": "dimension:order_items:created_at"},
            {"name": "users", "entity_type": "join", "relevance": 0.51,
             "entity_id": "join:order_items:users", "relationship": "one"}]}]}


class SearchTerms(unittest.TestCase):
    def test_the_simple_query_string_convention(self):
        self.assertEqual(search_terms({"query": "total sales 2022"}),
                         ["total sales 2022"])

    def test_the_structured_convention_keeps_the_target_type(self):
        # The type is half the request: "measure: revenue" and "dimension:
        # revenue" ask different questions and can be answered differently.
        self.assertEqual(
            search_terms({"search_targets": [
                {"target_type": "measure", "search_text": "revenue sales total"},
                {"target_type": "dimension", "search_text": "order date"}]}),
            ["measure: revenue sales total", "dimension: order date"])

    def test_a_request_with_no_terms_yields_none(self):
        self.assertEqual(search_terms({"scopes": [{"package": "ecommerce"}]}), [])
        self.assertEqual(search_terms({"query": "  "}), [])


class EntityIds(unittest.TestCase):
    def test_the_flat_results_shape(self):
        self.assertEqual(entity_ids(FLAT),
                         ["measure:order_items:total_sales_2022",
                          "view:order_items:sales_by_month_2022"])

    def test_entities_nested_under_sources_by_kind(self):
        got = entity_ids(NESTED)
        self.assertIn("dimension:order_items:created_at", got)
        self.assertIn("dimension:order_items:shipped_at", got)
        self.assertIn("measure:order_items:total_sales", got)

    def test_the_containing_source_counts_as_returned(self):
        # The agent was shown the source, so a case requiring it has had it.
        self.assertIn("source:order_items:order_items", entity_ids(NESTED))

    def test_a_named_join_is_not_a_returned_entity(self):
        # joins name neighbouring sources without returning them. Counting them
        # would credit retrieval for entities the agent never saw.
        got = entity_ids(NESTED)
        self.assertNotIn("source:users:users", got)
        self.assertNotIn("source:inventory_items:inventory_items", got)

    def test_per_target_results(self):
        self.assertEqual(entity_ids(PER_TARGET),
                         ["source:order_items:order_items",
                          "measure:order_items:total_sales"])

    def test_hosted_real_shape_yields_publisher_compatible_ids(self):
        # The regression: SourceEntity carries a bare `name` and no `kind`, so
        # the generic sources-block rule read each one as a source and returned
        # source:total_sales:total_sales. Every id was unmatchable, so recall
        # and precision both scored 0 on a call that delivered the answer.
        self.assertEqual(entity_ids(HOSTED),
                         ["source:order_items:order_items",
                          "measure:order_items:total_sales",
                          "dimension:order_items:created_at"])

    def test_the_two_targets_agree_on_an_entitys_id(self):
        # The property that makes a set's expectedEntities portable between a
        # local Publisher and the platform. If this breaks, an A/B across the
        # two targets stops comparing and nothing says so -- the scores just
        # diverge. Publisher states the ids; the host's are assembled; the
        # entities both returned have to come out identical either way.
        shared = {"source:order_items:order_items",
                  "measure:order_items:total_sales",
                  "dimension:order_items:created_at"}
        self.assertEqual(set(entity_ids(HOSTED)), shared)
        self.assertEqual(set(entity_ids(PUBLISHER)) & shared, shared)

    def test_publisher_keeps_the_join_a_hosted_enum_cannot_express(self):
        # entity_type is a superset, not a copy: that host allows
        # view/measure/dimension, and narrowing to that would drop the join --
        # which retrieval returns precisely so an agent stops concluding the
        # model declares none. A set may expect one, so it has to score.
        self.assertIn("join:order_items:users", entity_ids(PUBLISHER))

    def test_publisher_prefers_the_id_the_server_stated(self):
        # Where the two disagree the server wins, so a future change to how
        # Publisher names an entity does not need a matching edit here.
        stated = {"sources": [{
            "source_info": {"resource_id": {"source": "orders"}},
            "entities": [{"name": "hiring_manager.employee_count",
                          "entity_type": "dimension",
                          "entity_id": "dimension:employees:employee_count"}]}]}
        self.assertIn("dimension:employees:employee_count", entity_ids(stated))

    def test_the_same_entity_under_two_targets_is_returned_once(self):
        both = {"targets": PER_TARGET["targets"] + [
            {"target_type": "measure", "search_text": "money", "results": [
                {"entityId": "measure:order_items:total_sales",
                 "kind": "measure", "name": "total_sales",
                 "source": "order_items", "rank": 7}]}]}
        got = entity_ids(both)
        self.assertEqual(got.count("measure:order_items:total_sales"), 1)

    def test_a_rank_one_hit_outranks_a_low_hit_from_another_target(self):
        payload = {"targets": [
            {"search_text": "a", "results": [
                {"entityId": "measure:s:late", "kind": "measure",
                 "name": "late", "source": "s", "rank": 9}]},
            {"search_text": "b", "results": [
                {"entityId": "measure:s:early", "kind": "measure",
                 "name": "early", "source": "s", "rank": 1}]}]}
        self.assertEqual(entity_ids(payload)[0], "measure:s:early")

    def test_an_id_is_assembled_when_the_payload_omits_it(self):
        self.assertEqual(
            entity_ids({"results": [{"kind": "measure", "name": "gmv",
                                     "source": "orders"}]}),
            ["measure:orders:gmv"])

    def test_an_empty_or_error_payload_is_empty_not_an_exception(self):
        for payload in ({}, {"_error": "boom"}, {"results": []}, [], None):
            self.assertEqual(entity_ids(payload), [])

    def test_an_unknown_nesting_is_still_found(self):
        # The reason this walks rather than encodes a shape.
        self.assertEqual(
            entity_ids({"some": {"future": {"wrapper": [
                {"entityId": "measure:s:m", "kind": "measure", "name": "m",
                 "source": "s"}]}}}),
            ["measure:s:m"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
