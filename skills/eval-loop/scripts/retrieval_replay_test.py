#!/usr/bin/env python3
"""Tests for retrieval_replay.py: the labels, the request, the scores, and one run end to end."""
import contextlib
import hashlib
import http.server
import io
import json
import pathlib
import sys
import tempfile
import threading
import unittest
import unittest.mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import retrieval_replay as rr  # noqa: E402

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent / "eval-answer" / "scripts"))
import ledger  # noqa: E402


def payload(*ids: str, retrieval: str | None = "semantic") -> dict:
    """A get_context response holding `ids`, grouped under one card per source."""
    by_source: dict[str, list] = {}
    for eid in ids:
        by_source.setdefault(eid.split(":")[1], []).append({"entity_id": eid})
    out: dict = {"sources": [{"source_info": {"resource_id": {"source": s}}, "entities": ents}
                             for s, ents in by_source.items()]}
    if retrieval is not None:
        out["retrieval"] = retrieval
    return out


def a_case(qid="q1", tags=("t",), **exp) -> dict:
    return {"qid": qid, "question": "Which category sells most?", "tags": list(tags),
            "searchTargets": [{"target_type": "measure", "search_text": "revenue",
                               "expectedEntities": exp}]}


def scored(case: dict, body: dict) -> dict:
    args = rr.request_for(case, {"environment": "e", "package": "p"})
    return rr.score(case, rr.call_event(case["qid"], args, body, None))


SALES = "measure:order_items:total_sales"
MARGIN = "measure:order_items:total_margin"
CAT = "dimension:order_items:category"
PCAT = "dimension:products:category"
BRAND = "dimension:order_items:brand"


class Labels(unittest.TestCase):
    def test_the_case_and_every_target_are_merged_and_a_repeat_counts_once(self):
        case = {"qid": "q", "expectedEntities": {"required": [SALES], "requiredAnyOf": [[CAT, PCAT]]},
                "searchTargets": [
                    {"target_type": "measure", "search_text": "x",
                     "expectedEntities": {"required": [SALES], "acceptable": [MARGIN]}},
                    {"target_type": "dimension", "search_text": "y",
                     "expectedEntities": {"requiredAnyOf": [[PCAT, CAT], [SALES]]}}]}
        self.assertEqual(rr.labels_of(case), {"required": [SALES], "requiredAnyOf": [[CAT, PCAT]],
                                              "acceptable": [MARGIN]})

    def test_problems_name_the_case_the_target_and_the_fix(self):
        """The targets go through import_cases' validator, the one check for searchTargets."""
        case = {"qid": "q9", "searchTargets": [
            {"target_type": "metric", "search_text": "x"},
            {"target_type": "measure", "search_text": " "},
            {"target_type": "measure", "search_text": "x", "expectedEntities": {"requiredAnyOf": [SALES]}}]}
        got = rr.case_problems(case)
        self.assertEqual(len(got), 3)
        self.assertIn("q9: searchTargets[1]: `target_type` is 'metric'", got[0])
        self.assertIn("q9: searchTargets[2]: `search_text` must be a non-empty string, or absent",
                      got[1])
        self.assertIn("q9: searchTargets[3]: a `requiredAnyOf` group is str", got[2])
        self.assertEqual(rr.case_problems({"qid": "q", "searchTargets": []})[0],
                         'q: searchTargets must be a non-empty list of {"target_type", "search_text"} objects')

    def test_a_target_with_no_text_is_valid(self):
        case = {"qid": "q", "searchTargets": [{"target_type": "source"}]}
        self.assertEqual(rr.case_problems(case), [])


class Request(unittest.TestCase):
    def test_one_call_per_case_with_only_the_fields_the_tool_takes(self):
        case = a_case()
        case["searchTargets"].append({"target_type": "dimension", "search_text": "category",
                                      "concept": "not sent", "expectedEntities": {"required": [CAT]}})
        got = rr.request_for(case, {"environment": "e", "package": "p", "model_path": "m.malloy"})
        self.assertEqual(got, {
            "search_targets": [{"target_type": "measure", "search_text": "revenue"},
                               {"target_type": "dimension", "search_text": "category"}],
            "scopes": [{"environment": "e", "package": "p", "model_path": "m.malloy"}],
            "user_prompt": "Which category sells most?"})

    def test_a_target_with_no_text_is_sent_with_none(self):
        case = {"qid": "q", "searchTargets": [{"target_type": "source"},
                                              {"target_type": "measure", "search_text": "revenue"}]}
        got = rr.request_for(case, {"environment": "e", "package": "p"})
        self.assertEqual(got["search_targets"], [{"target_type": "source"},
                                                 {"target_type": "measure", "search_text": "revenue"}])

    def test_a_cases_own_scope_replaces_the_sets(self):
        case = a_case()
        case["scope"] = {"environment": "e2", "package": "p2", "source": "orders"}
        got = rr.request_for(case, {"environment": "e", "package": "p"})
        self.assertEqual(got["scopes"], [{"environment": "e2", "package": "p2", "source": "orders"}])
        case["scope"] = {"environment": "e2", "package": 7}
        self.assertIn("q1: scope must be an object of get_context scope fields", rr.case_problems(case)[0])

    def test_a_case_may_search_several_scopes_at_once(self):
        case = a_case()
        case["scopes"] = [{"environment": "e", "package": "a"}, {"environment": "e", "package": "b"}]
        self.assertEqual(rr.request_for(case, {"environment": "x", "package": "y"})["scopes"], case["scopes"])
        self.assertEqual(rr.case_problems(case), [])
        case["scope"] = {"environment": "e", "package": "a"}
        self.assertIn("q1: a case carries `scope` or `scopes`, not both", rr.case_problems(case))
        self.assertIn("q1: scopes must be a non-empty list", rr.case_problems({**a_case(), "scopes": []})[0])

    def test_a_cases_givens_and_filter_params_are_sent_as_written(self):
        case = a_case()
        case["givens"] = {"REPORT_ID": 7}
        case["filter_params"] = {"region": "West"}
        got = rr.request_for(case, {"environment": "e", "package": "p"})
        self.assertEqual((got["givens"], got["filter_params"]), ({"REPORT_ID": 7}, {"region": "West"}))
        self.assertNotIn("givens", rr.request_for(a_case(), {"environment": "e", "package": "p"}))
        case["givens"] = [7]
        self.assertIn("q1: givens must be an object", rr.case_problems(case)[0])

    def test_no_question_sends_no_user_prompt(self):
        case = a_case()
        del case["question"]
        self.assertNotIn("user_prompt", rr.request_for(case, {"environment": "e", "package": "p"}))

    def test_a_header_is_name_colon_value(self):
        self.assertEqual(rr.parse_header("Authorization:  Bearer a:b "), ("Authorization", "Bearer a:b"))
        for bad in ("Authorization", ": x", "X-A:"):
            with self.subTest(bad=bad), self.assertRaises(SystemExit) as e:
                rr.parse_header(bad)
            self.assertIn("expected 'Name: value'", str(e.exception))


class Score(unittest.TestCase):
    def test_required_any_of_and_acceptable(self):
        """Needs: total_sales, and one of the two category fields. Back: a source card,
        total_sales, total_margin (acceptable) and brand (noise)."""
        row = scored(a_case(required=[SALES], requiredAnyOf=[[CAT, PCAT]], acceptable=[MARGIN]),
                     payload(SALES, MARGIN, BRAND))
        self.assertEqual(row["recall_exact"], 0.5)
        self.assertEqual(row["recall"], 0.5)
        self.assertEqual(row["n_returned"], 4)          # the order_items source card counts
        self.assertEqual(row["precision"], 0.5)         # total_sales and total_margin of 4
        self.assertEqual(row["missing"], [f"{CAT} | {PCAT}"])

    def test_any_member_of_a_group_satisfies_it(self):
        row = scored(a_case(requiredAnyOf=[[CAT, PCAT]]), payload(PCAT))
        self.assertEqual((row["recall_exact"], row["recall"]), (1.0, 1.0))
        self.assertEqual(row["precision"], 0.5)          # products card + products.category

    def test_the_same_name_under_another_source_is_credited_but_not_exact(self):
        row = scored(a_case(required=[CAT]), payload("dimension:inventory:category"))
        self.assertEqual(row["recall"], 1.0)
        self.assertEqual(row["recall_exact"], 0.0)
        self.assertEqual(row["delivery"], {CAT: "alias"})

    def test_a_case_with_no_label_has_no_recall_and_still_counts_what_came_back(self):
        row = scored(a_case(), payload(BRAND))
        self.assertEqual((row["recall"], row["recall_exact"], row["precision"]), (None, None, None))
        self.assertEqual(row["n_returned"], 2)

    def test_an_empty_response_is_a_miss_with_no_precision(self):
        row = scored(a_case(required=[SALES]), payload())
        self.assertEqual((row["recall"], row["recall_exact"], row["precision"], row["n_returned"]),
                         (0.0, 0.0, None, 0))

    def test_each_target_is_scored_against_its_own_key(self):
        """Two targets, attributed hits: the measure target returned total_sales, the
        dimension target returned brand but not category."""
        case = {"qid": "q", "tags": [], "searchTargets": [
            {"target_type": "measure", "search_text": "revenue",
             "expectedEntities": {"required": [SALES]}},
            {"target_type": "dimension", "search_text": "category",
             "expectedEntities": {"required": [CAT]}}]}
        body = {"sources": [{"source_info": {"resource_id": {"source": "order_items"}}, "entities": [
            {"entity_id": SALES, "matched_targets": [{"search_text": "revenue"}]},
            {"entity_id": BRAND, "matched_targets": [{"search_text": "category"}]}]}]}
        row = scored(case, body)
        self.assertEqual([(t["search_text"], t["recall"], t["missing"]) for t in row["targets"]],
                         [("revenue", 1.0, []), ("category", 0.0, [CAT])])
        self.assertEqual(rr.summarize([row], skipped=0)["target_recall"], 0.5)

    def test_pooled_and_per_tag_means(self):
        rows = [scored(a_case("a", ("x",), required=[SALES], requiredAnyOf=[[CAT, PCAT]], acceptable=[MARGIN]),
                       payload(SALES, MARGIN, BRAND)),                          # 0.5 / 0.5
                scored(a_case("b", ("x", "y"), required=[SALES]), payload(SALES)),  # 1.0 / 0.5
                scored(a_case("c", ("y",), required=[SALES]), payload()),           # 0.0 / none
                scored(a_case("d", ("y",)), payload(BRAND)),                          # unlabelled
                rr.score(a_case("e", ("x",), required=[SALES]),
                         {"error": "URLError: refused"})]                            # error
        s = rr.summarize(rows, skipped=2)
        self.assertEqual({k: s[k] for k in ("cases", "errors", "unlabelled", "skipped_no_search_targets",
                                            "scored", "complete", "empty_responses")},
                         {"cases": 5, "errors": 1, "unlabelled": 1, "skipped_no_search_targets": 2,
                          "scored": 3, "complete": 1, "empty_responses": 1})
        self.assertEqual(s["recall_exact"], 0.5)        # (0.5 + 1.0 + 0.0) / 3
        self.assertEqual(s["precision"], 0.5)           # (0.5 + 0.5) / 2: the empty one has none
        self.assertEqual(s["by_tag"]["x"]["cases"], 3)
        self.assertEqual(s["by_tag"]["x"]["recall_exact"], 0.75)
        self.assertEqual(s["by_tag"]["y"]["recall_exact"], 0.5)
        self.assertEqual(s["by_tag"]["y"]["scored"], 2)


class StubMcp(http.server.BaseHTTPRequestHandler):
    """Answers each tools/call with the next of `first`, then with `body`, and records each
    request's headers and arguments. With `rest` set it is a REST get_context instead: the
    request body is the arguments and the reply is the payload."""
    body: dict = {}
    first: list = []
    seen: list = []
    rest = False

    def do_POST(self):  # noqa: N802
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        StubMcp.seen.append((dict(self.headers), req if StubMcp.rest else req["params"]["arguments"]))
        text = json.dumps(StubMcp.first.pop(0) if StubMcp.first else StubMcp.body)
        out = text.encode() if StubMcp.rest else json.dumps(
            {"jsonrpc": "2.0", "id": 1,
             "result": {"content": [{"type": "text", "text": text}]}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *a):
        pass


class PerTarget(unittest.TestCase):
    def body(self):
        """Two targets answered on one card, each entity naming the target that matched it."""
        return {"sources": [{"source_info": {"resource_id": {"source": "order_items"}}, "entities": [
            {"entity_id": SALES, "relevance": 0.94,
             "matched_targets": [{"search_text": "revenue", "relevance": 0.94}]},
            {"entity_id": MARGIN, "relevance": 0.9,
             "matched_targets": [{"search_text": "revenue", "relevance": 0.9}]},
            {"entity_id": BRAND, "relevance": 0.6,
             "matched_targets": [{"search_text": "shipping carrier", "relevance": 0.6}]}]}]}

    def case(self):
        case = a_case("q1", required=[SALES])
        case["searchTargets"].append({"target_type": "dimension", "search_text": "shipping carrier",
                                      "tags": ["not-found"], "expectedEntities": {"absent": True}})
        return case

    def test_each_target_is_scored_on_what_it_returned(self):
        row = scored(self.case(), self.body())
        first, second = row["targets"]
        self.assertEqual((first["n_found"], first["n_returned"], first["n_ok"], first["best_pick_ok"]),
                         (1, 2, 1, True))
        # The absent target returned a field nobody labelled as its closest: a wrong pick.
        self.assertEqual((second["not_found_ok"], second["n_returned"], second["n_ok"]), (False, 1, 0))

    def test_the_summary_pools_targets_and_reads_a_target_tag(self):
        rows = [dict(scored(self.case(), self.body()), latency_s=1.0),
                dict(scored(a_case("q2", required=[MARGIN]), self.body()), latency_s=3.0)]
        s = rr.summarize(rows, 0)
        pt = s["per_target"]
        self.assertEqual((pt["targets"], pt["required_answers"], pt["recall"], pt["returned"]), (3, 2, 1.0, 5))
        self.assertAlmostEqual(pt["precision"], 2 / 5)
        self.assertEqual((pt["best_pick"], pt["not_found"], pt["not_found_targets"]), (0.5, 0.0, 1))
        self.assertEqual(s["by_tag"]["not-found"]["per_target"]["targets"], 1)
        self.assertEqual((s["latency_p50_s"], s["latency_p95_s"]), (1.0, 3.0))

    def test_a_case_with_only_absent_targets_has_no_case_recall_and_still_counts_per_target(self):
        case = {"qid": "q", "tags": [], "searchTargets": [
            {"target_type": "dimension", "search_text": "shipping carrier",
             "expectedEntities": {"absent": True}}]}
        row = scored(case, {"sources": []})
        self.assertIsNone(row["recall"])
        self.assertEqual(rr.summarize([row], 0)["per_target"]["not_found"], 1.0)


class Checks(unittest.TestCase):
    def case(self):
        case = a_case("q1", required=[SALES])
        case["searchTargets"].append({"target_type": "dimension", "search_text": "shipping carrier",
                                      "expectedEntities": {"absent": True}})
        return case

    def row(self, warnings, scope=None, ids=(SALES,)):
        case = self.case()
        if scope:
            case["scope"] = scope
        body = {"sources": [{"source_info": {"resource_id": {"source": i.split(":")[1]}}, "entities": [
            {"entity_id": i, "relevance": 0.9, "matched_targets": [{"search_text": "revenue", "relevance": 0.9}]}]}
            for i in ids]}
        if warnings is not None:
            body["warnings"] = warnings
        args = rr.request_for(case, {"environment": "e", "package": "p"})
        return rr.score(case, rr.call_event("q1", args, body, None))

    def test_a_target_with_no_result_must_be_named_in_a_warning(self):
        said = self.row(["No field was found for target 2 (dimension: 'shipping carrier')."])
        self.assertEqual(said["checks"]["not_found_says_so"], [])
        silent = self.row(["Something unrelated."])
        self.assertEqual(silent["checks"]["not_found_says_so"], ["target 2 (dimension: shipping carrier)"])

    def test_a_host_with_no_warnings_field_is_not_checked(self):
        self.assertIsNone(self.row(None)["checks"]["not_found_says_so"])

    def test_a_pinned_source_admits_only_its_own_card(self):
        scope = {"environment": "e", "package": "p", "source": "order_items"}
        self.assertEqual(self.row([], scope)["checks"]["pinned_source_only"], [])
        leak = self.row([], scope, ids=(SALES, PCAT))
        self.assertEqual(leak["checks"]["pinned_source_only"], sorted({PCAT, "source:products:products"}))
        self.assertIsNone(self.row([])["checks"]["pinned_source_only"])

    def test_one_source_on_two_cards_is_reported(self):
        case = a_case("q1", required=[SALES])
        card = lambda path: {"source_info": {"resource_id": {"source": "order_items", "package": "p", "model_path": path}},
                             "entities": [{"entity_id": SALES}]}
        args = rr.request_for(case, {"environment": "e", "package": "p"})
        twice = rr.score(case, rr.call_event("q1", args, {"sources": [card("a.malloy"), card("b.malloy")]}, None))
        self.assertEqual(twice["checks"]["one_card_per_source"], ["order_items on 2 cards"])
        once = rr.score(case, rr.call_event("q1", args, {"sources": [card("a.malloy")]}, None))
        self.assertEqual(once["checks"]["one_card_per_source"], [])

    def test_a_cut_response_must_carry_a_warning(self):
        case = a_case("q1", required=[SALES])
        args = rr.request_for(case, {"environment": "e", "package": "p"})
        card = {"source_info": {"resource_id": {"source": "order_items", "package": "p"}}, "entities": [{"entity_id": SALES}]}
        run = lambda extra: rr.score(case, rr.call_event("q1", args, {"sources": [card], **extra}, None))["checks"]["cut_says_so"]
        self.assertEqual(run({"returned": 1, "total_available": 4}), ["returned 1 of 4 sources with no warning"])
        self.assertEqual(run({"returned": 1, "total_available": 4, "warnings": ["Returned 1 of 4 sources."]}), [])
        self.assertIsNone(run({"returned": 1, "total_available": 1}))
        self.assertIsNone(run({}))

    def test_the_summary_counts_checked_and_broken_responses(self):
        rows = [dict(self.row(["Something unrelated."]), latency_s=1.0),
                dict(self.row(["No field was found for target 2 (dimension: 'shipping carrier')."]), latency_s=1.0)]
        c = rr.summarize(rows, 0)["checks"]["not_found_says_so"]
        self.assertEqual((c["checked"], c["failed"], c["examples"][0]["qid"]), (2, 1, "q1"))


class Compare(unittest.TestCase):
    def rows(self, *ids):
        """One case with a required measure, a two-answer dimension target and an absent target."""
        case = a_case("q1", required=[SALES])
        case["searchTargets"] += [
            {"target_type": "dimension", "search_text": "category",
             "expectedEntities": {"required": [CAT, BRAND]}},
            {"target_type": "dimension", "search_text": "carrier", "expectedEntities": {"absent": True}}]
        text = {SALES: "revenue", MARGIN: "revenue", CAT: "category", BRAND: "category", PCAT: "carrier"}
        body = {"sources": [{"source_info": {"resource_id": {"source": "order_items"}}, "entities": [
            {"entity_id": e, "relevance": 0.9, "matched_targets": [{"search_text": text[e], "relevance": 0.9}]}
            for e in ids]}]}
        return [dict(scored(case, body), latency_s=1.0)]

    def test_gains_and_losses_are_counted_per_required_answer(self):
        c = rr.compare(self.rows(SALES, CAT), self.rows(CAT, BRAND, PCAT))
        self.assertEqual((c["targets_compared"], c["targets_set_aside"]), (3, 0))
        r = c["recall"]
        self.assertEqual((r["units"], r["a"], r["b"]), (3, 2, 2))
        self.assertEqual((r["gained"], r["lost"]), ([["q1", 2, BRAND]], [["q1", 1, SALES]]))
        self.assertEqual(c["not_found"]["lost"], [["q1", 3]])
        self.assertEqual(c["best_pick"]["lost"], [["q1", 1]])
        self.assertIn("required answers   2 -> 2 of 3   gained 1, lost 1", "\n".join(rr.fmt_compare(c, "a", "b")))

    def test_a_target_sent_or_labelled_differently_is_set_aside(self):
        a, b = self.rows(SALES), self.rows()
        b[0]["targets"][0]["search_text"] = "sales"
        c = rr.compare(a, b)
        self.assertEqual((c["targets_compared"], c["targets_set_aside"], c["recall"]["lost"]), (2, 1, []))
        # A changed `acceptable` or `closest` changes what counts as right, so it sets the target aside too.
        b = self.rows()
        b[0]["targets"][2]["label_ids"] = [MARGIN]
        self.assertEqual(rr.compare(self.rows(), b)["targets_set_aside"], 1)

    def test_an_errored_case_is_set_aside_and_qids_limit_the_comparison(self):
        b = self.rows(SALES)
        b[0]["error"] = "boom"
        self.assertEqual(rr.compare(self.rows(SALES), b)["targets_set_aside"], 3)
        self.assertEqual(rr.compare(self.rows(SALES), self.rows(SALES), qids=["other"])["targets_compared"], 0)


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.srv = http.server.HTTPServer(("127.0.0.1", 0), StubMcp)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_port}/mcp"
        StubMcp.seen, StubMcp.first = [], []
        StubMcp.body = payload(SALES, BRAND)
        self.d = pathlib.Path(tempfile.mkdtemp(prefix="replay-set-")).resolve()
        (self.d / "eval.toml").write_text(
            f'[model]\nenvironment = "examples"\npackage = "storefront"\n'
            f'[paths]\nworkdir = "{self.d / "work"}"\n')
        (self.d / "set.json").write_text(json.dumps({"name": "demo", "datasetVersion": 4,
                                                     "targetModelPath": "storefront.malloy"}))
        q2 = a_case("q2", requiredAnyOf=[[CAT, PCAT]])
        q2["searchTargets"][0].update(target_type="dimension", search_text="category")
        cases = [a_case("q1", required=[SALES]), q2,
                 {"qid": "q3", "question": "answer-run only"}]
        (self.d / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
        self.manifest = self.d / "manifest.json"
        self.manifest.write_text('{"files": []}\n')

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()

    def run_main(self, *extra: str) -> int:
        with contextlib.redirect_stdout(io.StringIO()):
            return rr.main(["--set", str(self.d), "--mcp-url", self.url, "--label", "r1",
                            "--ready-pause", "0", *extra])

    def test_a_run_writes_a_valid_ledger_with_its_pins_and_never_a_header_value(self):
        code = self.run_main("--mcp-header", "Authorization: Bearer s3cret",
                             "--set-version", "1.2.0", "--set-manifest", str(self.manifest))
        self.assertEqual(code, 0)
        out = self.d / "work" / "runs" / "r1"
        self.assertEqual(ledger.validate_run(out), ([], []))
        run = json.loads((out / "run.json").read_text())
        self.assertEqual({k: run[k] for k in ("phase", "mode", "setName", "datasetVersion", "setVersion",
                                              "setManifestSha256", "mcpHeaderNames", "modelPath",
                                              "retrievalMode")},
                         {"phase": "replay", "mode": "retrieval", "setName": "demo", "datasetVersion": 4,
                          "setVersion": "1.2.0",
                          "setManifestSha256": hashlib.sha256(self.manifest.read_bytes()).hexdigest(),
                          "mcpHeaderNames": ["Authorization"], "modelPath": "storefront.malloy",
                          "retrievalMode": "semantic"})
        for f in out.iterdir():
            self.assertNotIn("s3cret", f.read_text(), f.name)
        # Two gate probes, then one call per case that has searchTargets.
        self.assertEqual(len(StubMcp.seen), 4)
        self.assertTrue(all(h.get("Authorization") == "Bearer s3cret" for h, _ in StubMcp.seen))
        self.assertEqual(StubMcp.seen[2][1]["scopes"],
                         [{"environment": "examples", "package": "storefront", "model_path": "storefront.malloy"}])
        s = json.loads((out / "summary.json").read_text())
        # Both cases get the order_items card, total_sales and brand back. q1 needs total_sales
        # (recall 1, precision 1/3); q2 needs a category field (recall 0, precision 0).
        self.assertEqual((s["cases"], s["skipped_no_search_targets"], s["recall_exact"], s["precision"]),
                         (2, 1, 0.5, 1 / 6))
        self.assertEqual(len(ledger.read_jsonl(out / "events.jsonl")), 2)

    def test_a_rest_endpoint_takes_the_request_as_its_body_with_no_gate_and_each_cases_scope(self):
        cases = [dict(a_case("q1", required=[SALES]), scope={"environment": "org", "package": "pk", "source": "order_items"})]
        (self.d / "cases.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cases))
        StubMcp.rest = True
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                code = rr.main(["--set", str(self.d), "--get-context-url", self.url, "--case-scopes",
                                "--mcp-header", "X-Engine: new", "--label", "r1"])
        finally:
            StubMcp.rest = False
        self.assertEqual(code, 0)
        # One request: no readiness probes on a REST endpoint.
        self.assertEqual(len(StubMcp.seen), 1)
        headers, body = StubMcp.seen[0]
        self.assertEqual(headers.get("X-Engine"), "new")
        self.assertEqual(body["scopes"], [{"environment": "org", "package": "pk", "source": "order_items"}])
        run = json.loads((self.d / "work" / "runs" / "r1" / "run.json").read_text())
        self.assertEqual((run["getContextUrl"], run["caseScopes"], run["target"]), (self.url, True, "platform"))

    def test_case_scopes_refuses_a_case_without_one(self):
        with self.assertRaises(SystemExit) as e, contextlib.redirect_stdout(io.StringIO()):
            rr.main(["--set", str(self.d), "--get-context-url", self.url, "--case-scopes", "--label", "r1"])
        self.assertIn("--case-scopes: 2 case(s) carry no scope", str(e.exception))

    def test_case_scopes_refuses_one_target_version_for_the_whole_set(self):
        with self.assertRaises(SystemExit) as e, contextlib.redirect_stdout(io.StringIO()):
            rr.main(["--set", str(self.d), "--get-context-url", self.url, "--case-scopes",
                     "--target-version", "1.0.0", "--label", "r1"])
        self.assertIn("put the version in each case's scope", str(e.exception))

    def test_retrieval_that_stops_ranking_mid_run_is_an_error_not_a_miss(self):
        StubMcp.first = [payload(SALES), payload(SALES)]        # the two gate probes
        StubMcp.body = {"retrieval": "indexing", "sources": []}
        self.assertEqual(self.run_main("--only", "q1"), 0)
        out = self.d / "work" / "runs" / "r1"
        row = ledger.read_jsonl(out / "results.jsonl")[0]
        self.assertEqual(row["error"], "retrieval indexing mid-run (no reason given)")
        self.assertIsNone(row["recall"])
        s = json.loads((out / "summary.json").read_text())
        self.assertEqual((s["errors"], s["scored"], s["recall_exact"]), (1, 0, None))
        self.assertEqual(json.loads((out / "run.json").read_text())["retrievalMode"], "unavailable")

    def test_a_slice_runs_only_its_cases_and_run_json_records_what_it_resolved_to(self):
        (self.d / "slices.json").write_text('{"one": {"qids": ["q2"]}}')
        self.assertEqual(self.run_main("--slice", "one"), 0)
        out = self.d / "work" / "runs" / "r1"
        self.assertEqual(ledger.validate_run(out), ([], []))
        run = json.loads((out / "run.json").read_text())
        self.assertEqual(run["slice"], {"name": "one", "tags": [], "qids": ["q2"]})
        self.assertEqual([e["qid"] for e in ledger.read_jsonl(out / "events.jsonl")], ["q2"])

    def test_a_slice_and_only_together_are_refused(self):
        (self.d / "slices.json").write_text('{"one": {"qids": ["q2"]}}')
        with self.assertRaises(SystemExit) as e:
            self.run_main("--slice", "one", "--only", "q1")
        self.assertIn("pass one of them", str(e.exception))

    def test_the_version_that_answered_is_pinned(self):
        pv = {"id": "1.2.0", "latest": True, "contentHash": "h", "archived": False}
        with unittest.mock.patch.object(rr.rb, "package_version", return_value=pv), \
                unittest.mock.patch.object(rr.rb, "served_package_dir", return_value=None):
            self.assertEqual(self.run_main("--target-version", "1.2.0"), 0)
        run = json.loads((self.d / "work" / "runs" / "r1" / "run.json").read_text())
        self.assertEqual(run["packageVersion"], {"id": "1.2.0", "contentHash": "h", "latest": True,
                                                 "served": "publisher"})
        self.assertEqual((run["targetVersion"], run["status"]), ("1.2.0", "complete"))

    def test_latest_moving_mid_replay_leaves_it_incomplete(self):
        first = {"id": "1.2.0", "latest": True, "contentHash": "h"}
        with unittest.mock.patch.object(rr.rb, "package_version",
                                        side_effect=[first, {"id": "1.3.0"}]), \
                unittest.mock.patch.object(rr.rb, "served_package_dir", return_value=None):
            self.assertEqual(self.run_main(), 1)
        run = json.loads((self.d / "work" / "runs" / "r1" / "run.json").read_text())
        self.assertEqual((run["status"], run["packageVersionMovedTo"]), ("incomplete", "1.3.0"))

    def test_an_existing_run_is_not_overwritten(self):
        self.assertEqual(self.run_main(), 0)
        with self.assertRaises(SystemExit) as e:
            self.run_main()
        self.assertIn("already holds a run", str(e.exception))


if __name__ == "__main__":
    unittest.main()
