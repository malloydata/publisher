#!/usr/bin/env python3
"""Tests for init_truth_package. Stdlib only: python3 init_truth_package_test.py"""
import os
import pathlib
import unittest

import init_truth_package as itp
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



class RefsWorkOnceServed(unittest.TestCase):
    """Publisher serves a COPY of the truth package, so refs must survive it."""

    def setUp(self):
        import tempfile
        self.root = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.pkg = self.root / "model"
        (self.pkg / "data").mkdir(parents=True)
        (self.pkg / "publisher.json").write_text("{}")
        (self.root / "shared").mkdir()

    def test_a_data_dir_ref_keeps_its_form_and_the_dir_is_linked(self):
        src = self.pkg / "m.malloy"
        self.assertEqual(itp.place_ref(self.pkg, src, "data/u.parquet"),
                         ("data/u.parquet", "data"))

    def test_a_ref_out_of_the_package_becomes_absolute(self):
        src = self.pkg / "m.malloy"
        self.assertEqual(itp.place_ref(self.pkg, src, "../shared/u.parquet"),
                         (str(self.root / "shared" / "u.parquet"), None))

    def test_a_dir_holding_models_is_not_linked(self):
        (self.pkg / "models").mkdir()
        (self.pkg / "models" / "x.malloy").write_text("")
        src = self.pkg / "models" / "x.malloy"
        self.assertEqual(itp.place_ref(self.pkg, src, "u.parquet"),
                         (str(self.pkg / "models" / "u.parquet"), None))

    def test_a_ref_written_from_the_package_root_is_not_resolved_against_its_own_folder(self):
        # A model in `_shared/` names its data relative to the package root:
        # `_shared/data/u.parquet`. Resolved against `_shared/` itself that is
        # `_shared/_shared/data/u.parquet`, which does not exist.
        (self.pkg / "_shared" / "data").mkdir(parents=True)
        (self.pkg / "_shared" / "data" / "u.parquet").write_text("")
        (self.pkg / "_shared" / "x.malloy").write_text("")
        src = self.pkg / "_shared" / "x.malloy"
        served, _ = itp.place_ref(self.pkg, src, "_shared/data/u.parquet")
        self.assertEqual(pathlib.Path(served),
                         self.pkg / "_shared" / "data" / "u.parquet")

    def test_a_ref_beside_its_model_still_resolves_against_that_folder(self):
        (self.pkg / "models").mkdir()
        (self.pkg / "models" / "x.malloy").write_text("")
        (self.pkg / "models" / "u.parquet").write_text("")
        served, _ = itp.place_ref(self.pkg, self.pkg / "models" / "x.malloy",
                                  "u.parquet")
        self.assertEqual(pathlib.Path(served), self.pkg / "models" / "u.parquet")

    def test_a_warehouse_table_is_left_alone(self):
        self.assertEqual(itp.place_ref(self.pkg, self.pkg / "m.malloy",
                                       "analytics.public.orders"),
                         ("analytics.public.orders", None))

    def test_main_links_the_data_dir(self):
        (self.pkg / "m.malloy").write_text(
            "source: u is duckdb.table('data/u.parquet')\n")
        out = self.root / "truth"
        itp.main(["--package", str(self.pkg), "--out", str(out), "--name", "t"])
        self.assertTrue((out / "data").is_symlink())
        self.assertEqual((out / "data").resolve(), self.pkg / "data")
        self.assertIn("duckdb.table('data/u.parquet')", (out / "truth.malloy").read_text())


class OutMustBeOutsideThePackage(unittest.TestCase):
    """A truth package inside the model package is served by the model server."""

    def test_an_out_inside_the_package_is_refused(self):
        import tempfile
        pkg = pathlib.Path(tempfile.mkdtemp()) / "model"
        pkg.mkdir()
        (pkg / "publisher.json").write_text("{}")
        with self.assertRaises(SystemExit) as e:
            itp.main(["--package", str(pkg), "--out", str(pkg / "evals" / "truth"),
                      "--name", "t"])
        self.assertIn("is inside the model package", str(e.exception))
        self.assertFalse((pkg / "evals").exists())


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
