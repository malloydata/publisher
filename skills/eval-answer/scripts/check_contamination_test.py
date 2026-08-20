#!/usr/bin/env python3
import os
import unittest

from check_contamination import check


class ContaminationTests(unittest.TestCase):
    def test_clean_mcp_only(self):
        log = {
            "toolUses": [
                {"name": "malloy_getContext", "input": {"query": "orders"}},
                {"name": "malloy_executeQuery", "input": {"query": "run: orders -> { aggregate: n is count() }"}},
            ],
            "reportedCalls": 2,
        }
        out = check(log)
        self.assertFalse(out["contaminated"])
        self.assertEqual(out["reasons"], [])

    def test_read_gold_csv(self):
        log = {
            "toolUses": [
                {"name": "Read", "input": {"path": "results/gold/beaver_full/dw_5261.csv"}},
            ],
            "reportedCalls": 0,
        }
        out = check(log)
        self.assertTrue(out["contaminated"])
        self.assertTrue(any("gold" in r for r in out["reasons"]))

    def test_eval_rest_path(self):
        log = {
            "toolUses": [
                {"name": "Shell", "input": {"command": "curl http://127.0.0.1:4000/api/v0/evals/cases/abc"}},
            ],
        }
        out = check(log)
        self.assertTrue(out["contaminated"])

    def test_model_file_read(self):
        log = {
            "toolUses": [
                {"name": "Read", "input": {"path": "/tmp/pkg/orders.malloy"}},
            ],
        }
        out = check(log, model_path="/tmp/pkg/orders.malloy")
        self.assertTrue(out["contaminated"])

    def test_mcp_model_path_is_not_contamination(self):
        log = {
            "toolUses": [
                {
                    "name": "malloy_executeQuery",
                    "input": {
                        "modelPath": "storefront.malloy",
                        "query": "run: order_items -> { aggregate: order_count }",
                    },
                }
            ],
            "reportedCalls": 1,
        }
        out = check(log, model_path="/tmp/pkg/storefront.malloy")
        self.assertFalse(out["contaminated"])

    def test_underreported_calls(self):
        log = {
            "toolUses": [
                {"name": "malloy_getContext", "input": {}},
            ],
            "reportedCalls": 9,
        }
        out = check(log)
        self.assertTrue(out["contaminated"])
        self.assertTrue(any("reportedCalls" in r for r in out["reasons"]))


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
