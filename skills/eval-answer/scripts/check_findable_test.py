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
from unittest import mock

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

    def test_findings_never_outnumber_the_required_ids(self):
        # `rows` holds only the ids that reached a search; a malformed id is a
        # finding that never becomes a row. Reporting `len(rows) - len(f)`
        # subtracted findings that were never in the total and printed
        # "-1 of 2 required entities are retrievable".
        cases = [case("q", ["flight_count",                  # malformed
                            "measure:flights:gone",          # genuine miss
                            "measure:flights:also_gone"])]   # genuine miss
        f, rows = self.run_check(cases)
        total = len(check_findable.required_ids(cases))
        self.assertEqual(total, 3)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(f), 3)
        self.assertGreaterEqual(total - len(f), 0)

    def test_the_finding_names_every_case_that_would_be_affected(self):
        f, _ = self.run_check([case("q1", ["measure:flights:gone"]),
                               case("q2", ["measure:flights:gone"])])
        self.assertIn("q1, q2", f[0])


class ServerCannotSearch(unittest.TestCase):
    """An `indexing` or `error` answer has no entities, and is not a miss."""

    def setUp(self):
        self._real = check_findable.get_context

    def tearDown(self):
        check_findable.get_context = self._real

    def answer(self, payload):
        check_findable.get_context = lambda *_a, **_k: payload

    def test_an_indexing_answer_is_inconclusive_not_a_finding(self):
        self.answer({"retrieval": "indexing", "sources": []})
        with self.assertRaises(check_findable.Inconclusive) as ctx:
            check_findable.check([case("q", ["measure:flights:flight_count"])],
                                 "http://x/mcp", "e", "p")
        self.assertIn("`indexing`", str(ctx.exception))
        self.assertIn("measure:flights:flight_count", str(ctx.exception))

    def test_an_error_answer_is_inconclusive_too(self):
        self.answer({"retrieval": "error", "error": "provider down"})
        with self.assertRaises(check_findable.Inconclusive):
            check_findable.check([case("q", ["dimension:airports:state"])],
                                 "http://x/mcp", "e", "p")

    def test_a_semantic_answer_that_misses_is_still_a_finding(self):
        self.answer({"retrieval": "semantic", "sources": []})
        f, _ = check_findable.check([case("q", ["dimension:airports:state"])],
                                    "http://x/mcp", "e", "p")
        self.assertEqual(len(f), 1)

    def test_the_wait_ending_on_indexing_or_error_blocks_the_check(self):
        self.assertTrue(check_findable.index_cannot_search({"status": "indexing"}))
        self.assertTrue(check_findable.index_cannot_search({"status": "error"}))

    def test_ready_lexical_and_an_unreadable_status_do_not_block_it(self):
        for index in ({"status": "ready"}, {"status": "lexical"}, None):
            self.assertFalse(check_findable.index_cannot_search(index))

    def test_the_exit_code_is_its_own(self):
        self.assertEqual(check_findable.EXIT_INCONCLUSIVE, 4)


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

    def test_a_malformed_id_is_reported(self):
        # run_baseline's lint calls only this, so skipping it here dropped the
        # id there. main() dedups on the id, so the CLI still says it once.
        out = self.find("flight_count")
        self.assertEqual(len(out), 1)
        self.assertEqual(check_findable.finding_id(out[0]), "flight_count")

    def test_a_finding_is_identified_by_its_whole_entity_id(self):
        # Splitting on the bare colon returns the KIND, and deduplicating the
        # search findings against the compiled-model ones on that basis
        # silently deleted an unrelated entity's genuine finding -- which can
        # flip the exit code from 1 to 0.
        self.assertEqual(
            check_findable.finding_id(
                "measure:flights:distinct_planes: a `measure` search for "
                "'distinct planes' does not return it (required by q2)"),
            "measure:flights:distinct_planes")

    def test_dedup_keeps_a_finding_that_only_shares_a_kind(self):
        declared = ["measure:orders:total_sales: the compiled model has no "
                    "source 'orders' (required by q1)"]
        found = ["measure:flights:distinct_planes: a `measure` search does "
                 "not return it (required by q2)"]
        already = {check_findable.finding_id(f) for f in declared}
        kept = declared + [f for f in found
                           if check_findable.finding_id(f) not in already]
        self.assertEqual(len(kept), 2)

    def test_an_unreachable_server_is_not_read_as_an_empty_model(self):
        # None means "not checked". Treating it as {} would report every id in
        # the set as missing.
        self.assertIsNone(check_findable.compiled_entities(
            "http://127.0.0.1:9", "e", "p"))


def fake_model_doc(queries=(), fields=(("carrier", "dimension"),)):
    """One model path, one source, with whatever fields and queries are asked
    for. Mirrors the REST shape: `queries` is a SIBLING of `sourceInfos`."""
    return {"sourceInfos": [{"name": "flights",
                             "schema": {"fields": [{"name": n, "kind": k}
                                                   for n, k in fields]}}],
            "queries": [dict(q) for q in queries]}


class CompiledQueries(unittest.TestCase):
    """A model-level named query is not a field and never appears under
    `sourceInfos`; `FieldInfoType` has no `query`. Read from the response's own
    `queries` array or every `query:` id reads as a field that does not exist --
    the exact false positive `compiled_entities` exists to end."""

    def declared_from(self, doc):
        """`compiled_entities`' per-doc walk, with the two HTTP calls stubbed."""
        import json as _json
        import urllib.request as _u
        real = _u.urlopen

        class Resp:
            def __init__(self, payload): self.payload = payload
            def read(self): return _json.dumps(self.payload).encode()
            def __enter__(self): return self
            def __exit__(self, *a): return False

        def fake(req, timeout=30):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            return Resp([{"path": "m.malloy"}] if url.endswith("/models")
                        else doc)

        _u.urlopen = fake
        try:
            return check_findable.compiled_entities("http://x", "e", "p")
        finally:
            _u.urlopen = real

    def test_a_named_query_is_declared(self):
        declared = self.declared_from(fake_model_doc(
            queries=[{"name": "top_carriers", "sourceName": "flights"}]))
        self.assertIn("query:top_carriers", declared["flights"])

    def test_a_named_query_raises_no_finding(self):
        declared = self.declared_from(fake_model_doc(
            queries=[{"name": "top_carriers", "sourceName": "flights"}]))
        cases = [case("q1", required=["query:flights:top_carriers"])]
        self.assertEqual(check_findable.declared_findings(cases, declared), [])

    def test_without_the_queries_array_it_would_have_been_a_false_finding(self):
        # The regression this pins: reading only `sourceInfos` reports a real,
        # retrievable query as a field the source does not declare, and that
        # finding survives main()'s dedup and exits 1.
        declared = self.declared_from(fake_model_doc(queries=[]))
        cases = [case("q1", required=["query:flights:top_carriers"])]
        out = check_findable.declared_findings(cases, declared)
        self.assertEqual(len(out), 1)
        self.assertIn("declares no field", out[0])

    def test_a_query_over_an_inline_source_stays_undeclared(self):
        # It has no source name, and the server excludes it from the index for
        # that reason, so an id naming it is genuinely unreachable.
        declared = self.declared_from(fake_model_doc(
            queries=[{"name": "adhoc"}]))
        self.assertNotIn("query:adhoc", declared["flights"])


class Staleness(unittest.TestCase):
    """A stale package answers the models endpoint normally while describing
    the compile BEFORE the last save, so the compiled model is not the
    authority and must not be read as one."""

    def status(self, payload):
        import json as _json
        import urllib.request as _u
        real = _u.urlopen

        class Resp:
            def read(self): return _json.dumps(payload).encode()
        _u.urlopen = lambda req, timeout=30: Resp()
        try:
            return check_findable.stale_packages("http://x")
        finally:
            _u.urlopen = real

    def test_a_stale_package_is_named(self):
        self.assertEqual(
            self.status({"loadErrors": [{"environment": "e", "package": "p",
                                         "stale": True}]}),
            {("e", "p")})

    def test_an_ordinary_load_failure_is_not_staleness(self):
        # No `stale` flag: the package did not load at all, which is a
        # different fact and is visible by the package simply being absent.
        self.assertEqual(
            self.status({"loadErrors": [{"environment": "e", "package": "p",
                                         "message": "boom"}]}),
            set())

    def test_a_healthy_server_omits_the_field(self):
        self.assertEqual(self.status({"operationalState": "serving"}), set())

    def test_an_unreadable_status_is_not_read_as_healthy(self):
        # None means "could not tell", which main() reports rather than
        # silently treating the model as current.
        self.assertIsNone(check_findable.stale_packages("http://127.0.0.1:9"))

    def test_current_entities_refuses_a_stale_package(self):
        # run_baseline read the compiled model with no staleness check, so an
        # entity deleted since the last good compile read as declared.
        with mock.patch.object(check_findable, "stale_packages",
                               return_value={("e", "p")}), \
             mock.patch.object(check_findable, "compiled_entities") as compiled:
            declared, warning = check_findable.current_entities("http://x", "e", "p")
        self.assertIsNone(declared)
        self.assertIn("STALE", warning)
        compiled.assert_not_called()

    def test_current_entities_passes_a_current_package_through(self):
        with mock.patch.object(check_findable, "stale_packages", return_value=set()), \
             mock.patch.object(check_findable, "compiled_entities",
                               return_value={"s": {"source:s"}}):
            self.assertEqual(check_findable.current_entities("http://x", "e", "p"),
                             ({"s": {"source:s"}}, None))


class DottedJoinPaths(unittest.TestCase):
    """A dotted name is a join path, checked hop by hop against the joins the
    source declares.

    Reading only a source's OWN fields made every dotted id a miss: on the
    storefront tour set, five of eight reported misses, each one an id
    get_context returns as its top result. Resolving only the LAST hop, as a
    source name, then passed a bogus first hop and failed an aliased join.
    """

    PRODUCTS = [{"kind": "dimension", "name": "brand"},
                {"kind": "measure", "name": "product_count"}]
    # The shape the server returns: a join field carries the joined schema.
    FIELDS = [{"kind": "measure", "name": "total_sales"},
              {"kind": "join", "name": "products", "relationship": "one",
               "schema": {"fields": PRODUCTS}},
              {"kind": "join", "name": "buyer", "relationship": "one",
               "schema": {"fields": [{"kind": "dimension", "name": "state"}]}}]

    def findings(self, eid):
        declared = {"order_items": {"source:order_items"}}
        check_findable.add_fields(declared["order_items"], self.FIELDS)
        cases = [{"qid": "q1", "expectedEntities": {"required": [eid]}}]
        return check_findable.declared_findings(cases, declared)

    def test_a_reachable_join_path_is_not_a_finding(self):
        self.assertEqual(self.findings("dimension:order_items:products.brand"), [])

    def test_a_join_named_other_than_its_source_is_reachable(self):
        # `join_one: buyer is customers`: no source is called `buyer`.
        self.assertEqual(self.findings("dimension:order_items:buyer.state"), [])

    def test_the_kind_still_has_to_match_on_the_joined_source(self):
        out = self.findings("measure:order_items:products.brand")
        self.assertEqual(len(out), 1)
        self.assertIn("reaches no measure 'brand'", out[0])

    def test_an_unknown_hop_is_named_as_the_hop(self):
        out = self.findings("dimension:order_items:suppliers.name")
        self.assertEqual(len(out), 1)
        self.assertIn("has no join 'suppliers'", out[0])

    def test_a_bogus_first_hop_is_caught_even_when_the_last_hop_exists(self):
        out = self.findings("dimension:order_items:nowhere.products.brand")
        self.assertEqual(len(out), 1)
        self.assertIn("has no join 'nowhere'", out[0])

    def test_a_field_missing_on_the_joined_source_is_a_finding(self):
        out = self.findings("dimension:order_items:products.colour")
        self.assertEqual(len(out), 1)
        self.assertIn("'colour'", out[0])

    def test_a_malformed_id_is_reported_not_dropped(self):
        out = self.findings("dimension:brand")
        self.assertEqual(len(out), 1)
        self.assertIn("not a kind:source:name id", out[0])


class IndexWait(unittest.TestCase):
    """`embeddingIndex.status` is lexical | indexing | ready | error.

    Only `indexing` is worth waiting on; the other three are settled, and each
    says something different about what the misses mean.
    """

    def wait(self, reads, wait=300):
        """Run wait_for_index over scripted reads; returns (index, reads used)."""
        it = iter(reads)
        used = []

        def read(*_a):
            used.append(1)
            return next(it)

        index = check_findable.wait_for_index(
            "http://x", "e", "p", wait, read=read, sleep=lambda _s: None,
            clock=iter(range(0, 10_000)).__next__)
        return index, len(used)

    def test_ready_ends_the_wait_and_says_nothing(self):
        index, n = self.wait([{"status": "ready"}])
        self.assertEqual(n, 1)
        self.assertIsNone(check_findable.index_message(index, 300))

    def test_indexing_is_waited_on_until_it_settles(self):
        index, n = self.wait([{"status": "indexing"}, {"status": "indexing"},
                              {"status": "ready"}])
        self.assertEqual((index["status"], n), ("ready", 3))

    def test_indexing_past_the_deadline_says_so_and_asks_for_a_re_run(self):
        index, n = self.wait([{"status": "indexing"}] * 50, wait=3)
        self.assertEqual(index["status"], "indexing")
        self.assertLess(n, 50)
        msg = check_findable.index_message(index, 3)
        self.assertIn("still indexing after 3s", msg)
        self.assertIn("Re-run", msg)

    def test_lexical_is_terminal_and_says_the_run_measures_the_lexical_matcher(self):
        index, n = self.wait([{"status": "lexical"}])
        self.assertEqual(n, 1)
        msg = check_findable.index_message(index, 300)
        self.assertIn("no embedding provider", msg)
        self.assertIn("measures the lexical matcher", msg)

    def test_error_is_terminal_not_waited_on(self):
        _index, n = self.wait([{"status": "error", "reason": "cooldown"}])
        self.assertEqual(n, 1)

    def test_cooldown_says_to_re_run(self):
        msg = check_findable.index_message(
            {"status": "error", "reason": "cooldown",
             "lastError": {"message": "429 from provider"}}, 300)
        self.assertIn("cooldown", msg)
        self.assertIn("Re-run", msg)
        self.assertIn("429 from provider", msg)

    def test_too_many_entities_names_the_setting_to_raise(self):
        msg = check_findable.index_message(
            {"status": "error", "reason": "too-many-entities"}, 300)
        self.assertIn("retrieval.indexing.maxEntities", msg)
        self.assertNotIn("Re-run", msg)

    def test_any_other_error_shows_the_reason_and_the_server_message(self):
        msg = check_findable.index_message(
            {"status": "error", "reason": "provider-error",
             "lastError": {"message": "401 bad key"}}, 300)
        self.assertIn("provider-error", msg)
        self.assertIn("401 bad key", msg)

    def test_an_error_with_no_reason_or_message_still_says_something(self):
        msg = check_findable.index_message({"status": "error"}, 300)
        self.assertIn("reason: unknown", msg)
        self.assertIn("no message given", msg)

    def test_a_server_with_no_field_ends_the_wait_and_is_not_read_as_ready(self):
        index, n = self.wait([None])
        self.assertIsNone(index)
        self.assertEqual(n, 1)
        self.assertIn("could not be read",
                      check_findable.index_message(index, 300))


class EmbeddingIndexRead(unittest.TestCase):
    def read(self, payload):
        import json as _json

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return _json.dumps(payload).encode()

        with mock.patch.object(check_findable.urllib.request, "urlopen",
                               lambda *a, **k: Resp()):
            return check_findable.embedding_index("http://x", "e", "p")

    def test_the_whole_object_comes_back_so_reason_is_not_lost(self):
        idx = {"status": "error", "reason": "cooldown",
               "lastError": {"message": "m"}}
        self.assertEqual(self.read({"embeddingIndex": idx}), idx)

    def test_an_older_server_with_no_field_reads_as_none(self):
        self.assertIsNone(self.read({"name": "p"}))

    def test_a_field_with_no_status_reads_as_none(self):
        self.assertIsNone(self.read({"embeddingIndex": {}}))


if __name__ == "__main__":
    unittest.main()
