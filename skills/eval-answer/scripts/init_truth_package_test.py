#!/usr/bin/env python3
"""Tests for init_truth_package. Stdlib only: python3 init_truth_package_test.py"""
import os
import unittest

from init_truth_package import stem


class StemTests(unittest.TestCase):
    def test_file_path_names_the_file_not_the_extension(self):
        # The regression. Every table in a parquet package scaffolded as
        # t_parquet, t_parquet_2, ... because the split ran before the
        # extension strip, and a golden author cannot write against those.
        self.assertEqual(stem("data/users.parquet"), "t_users")
        self.assertEqual(stem("data/order_items.parquet"), "t_order_items")

    def test_a_whole_package_of_one_file_type_gets_distinct_names(self):
        refs = ["data/users.parquet", "data/products.parquet",
                "data/inventory_items.parquet", "data/order_items.parquet"]
        self.assertEqual(len({stem(r) for r in refs}), 4)

    def test_warehouse_ref_still_takes_the_last_dot_segment(self):
        self.assertEqual(stem("analytics.public.orders"), "t_orders")
        self.assertEqual(stem("orders"), "t_orders")

    def test_bare_filename_without_a_directory(self):
        self.assertEqual(stem("users.parquet"), "t_users")

    def test_prefix_noise_is_trimmed(self):
        self.assertEqual(stem("data/fact_sales.csv"), "t_sales")
        self.assertEqual(stem("/abs/path/dim_date.parquet"), "t_date")
        self.assertEqual(stem("warehouse.dim_user"), "t_user")

    def test_dots_inside_a_filename_survive(self):
        # `.parquet` is the extension; `.b` is part of the name.
        self.assertEqual(stem("data/a.b.parquet"), "t_a_b")

    def test_path_with_no_extension(self):
        self.assertEqual(stem("data/users"), "t_users")

    def test_every_other_extension_we_know(self):
        for ext in ("csv", "tsv", "json", "jsonl", "ndjson", "orc", "avro"):
            self.assertEqual(stem(f"data/events.{ext}"), "t_events", ext)


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
