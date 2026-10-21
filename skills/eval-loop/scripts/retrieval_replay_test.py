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
        case = {"qid": "q9", "searchTargets": [
            {"target_type": "metric", "search_text": "x"},
            {"target_type": "measure", "search_text": " "},
            {"target_type": "measure", "search_text": "x", "expectedEntities": {"requiredAnyOf": [SALES]}}]}
        got = rr.case_problems(case)
        self.assertEqual(len(got), 3)
        self.assertIn("q9: searchTargets[0]: invalid target_type 'metric'", got[0])
        self.assertIn('Fix: "target_type": "measure"', got[0])
        self.assertIn("q9: searchTargets[1]: search_text must be a non-empty string", got[1])
        self.assertIn("q9: searchTargets[2].expectedEntities.requiredAnyOf: expected a list of "
                      "non-empty lists", got[2])
        self.assertEqual(rr.case_problems({"qid": "q", "searchTargets": []})[0],
                         'q: searchTargets must be a non-empty list of {"target_type", "search_text"} objects')

    def test_target_types_are_the_ones_get_context_accepts(self):
        self.assertEqual(set(rr.TARGET_TYPES),
                         {"source", "dimension", "measure", "view", "join", "dimensional_value"})


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
    request's headers and arguments."""
    body: dict = {}
    first: list = []
    seen: list = []

    def do_POST(self):  # noqa: N802
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        StubMcp.seen.append((dict(self.headers), req["params"]["arguments"]))
        text = json.dumps(StubMcp.first.pop(0) if StubMcp.first else StubMcp.body)
        out = json.dumps({"jsonrpc": "2.0", "id": 1,
                          "result": {"content": [{"type": "text", "text": text}]}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *a):
        pass


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
        cases = [a_case("q1", required=[SALES]), a_case("q2", requiredAnyOf=[[CAT, PCAT]]),
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

    def test_an_existing_run_is_not_overwritten(self):
        self.assertEqual(self.run_main(), 0)
        with self.assertRaises(SystemExit) as e:
            self.run_main()
        self.assertIn("already holds a run", str(e.exception))


if __name__ == "__main__":
    unittest.main()
