"""workload_from_logs.py: Publisher's debug log and other query rows become a replay workload."""
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import workload_from_logs as wl  # noqa: E402

# Lines as Publisher 0.18.1 writes them at debug level, colour codes included.
URL = "POST /api/v0/environments/bench/packages/perfshop/models/perfshop.malloy/query"


def line(payload, status=200, pkg="perfshop"):
    rec = {"durationMs": 56.1, "params": {}, "payload": payload, "query": {},
           "statusCode": status, "traceId": "0f92"}
    return f"\x1b[32minfo\x1b[39m: {URL.replace('perfshop/models', pkg + '/models')} {json.dumps(rec)}\n"


class PublisherLog(unittest.TestCase):
    def go(self, lines):
        return wl.workload(wl.from_publisher_log(lines, "perfshop"))

    def test_the_same_query_is_one_line_with_its_count(self):
        q = {"compactJson": True, "query": "run: order_items -> by_category"}
        got, skipped = self.go([line(q), line(q), line({"query": "run: o -> x"})])
        self.assertEqual(got[0], {"query": "run: order_items -> by_category",
                                  "modelPath": "perfshop.malloy", "count": 2})
        self.assertEqual(len(got), 2)
        self.assertEqual(skipped, {})

    def test_givens_are_part_of_what_makes_a_query_the_same(self):
        got, _ = self.go([line({"query": "run: o -> x", "givens": {"REGION": "West"}}),
                          line({"query": "run: o -> x", "givens": {"REGION": "East"}})])
        self.assertEqual(sorted(w["givens"]["REGION"] for w in got), ["East", "West"])

    def test_a_named_query_or_view_call_is_rebuilt_as_run_text(self):
        got, _ = self.go([line({"queryName": "by_category", "sourceName": "order_items"}),
                          line({"queryName": "top_carriers"})])
        self.assertEqual(sorted(w["query"] for w in got),
                         ["run: order_items -> by_category", "run: top_carriers"])

    def test_failed_queries_other_packages_and_other_routes_are_left_out(self):
        other = "\x1b[32minfo\x1b[39m: GET /api/v0/environments/bench/packages/perfshop/status {}\n"
        got, skipped = self.go([line({"query": "run: o -> x"}, status=400),
                                line({"query": "run: o -> x"}, pkg="else"), other])
        self.assertEqual((got, skipped), ([], {"the query failed": 1}))

    def test_an_info_level_log_says_how_to_get_the_query_text(self):
        rec = {"durationMs": 5, "statusCode": 200}
        got, skipped = self.go([f"info: {URL} {json.dumps(rec)}\n"])
        self.assertEqual(skipped, {"no request body (the log is not at debug level)": 1})


class Rows(unittest.TestCase):
    def test_rows_with_given_names_and_no_values_are_left_out_not_guessed(self):
        rows = [json.dumps(r) for r in (
            {"package": "perfshop", "modelPath": "m.malloy", "query": "run: a -> b", "count": 40},
            {"package": "perfshop", "modelPath": "m.malloy", "query": "run: a -> c",
             "givenKeys": ["REGION"]},
            {"package": "other", "modelPath": "m.malloy", "query": "run: a -> b"})]
        got, skipped = wl.workload(wl.from_rows(rows, "perfshop"))
        self.assertEqual(got, [{"query": "run: a -> b", "modelPath": "m.malloy", "count": 40}])
        self.assertEqual(skipped, {"names givens without their values": 1})


if __name__ == "__main__":
    unittest.main()
