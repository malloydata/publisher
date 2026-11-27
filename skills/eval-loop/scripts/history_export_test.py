"""history_export.py: history batches for a database that takes no inserts."""
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import history_export as he  # noqa: E402
import optimization_package as op  # noqa: E402


class Export(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.data = op.install(self.root / "optimization-s", {"usd_per_compute_hour": 5})
        self.cols = op.APPENDED["prod_turns"][2]

    def turns(self, *ids):
        op.append(self.data / "prod_turns.csv", self.cols, [{"turn_id": i} for i in ids])

    def test_each_batch_takes_only_the_rows_added_since_and_a_label_again_takes_the_same(self):
        self.turns("a", "b")
        m1 = he.export(self.data, self.root / "x1", "b1")
        self.turns("c")
        m2 = he.export(self.data, self.root / "x2", "b2")
        rows = lambda d: [r["turn_id"] for r in op.read(d / "prod_turns.csv")]
        self.assertEqual((rows(self.root / "x1"), rows(self.root / "x2")), (["a", "b"], ["c"]))
        self.assertIn({"file": "prod_turns.csv", "table": "prod_turns__b1", "rows": 2}, m1["tables"])
        # A table with nothing new has no file; the price card goes every time.
        self.assertNotIn("prod_judgments.csv", [t["file"] for t in m2["tables"]])
        self.assertIn("price_card.csv", [t["file"] for t in m2["tables"]])
        he.export(self.data, self.root / "x1b", "b1")
        self.assertEqual(rows(self.root / "x1b"), ["a", "b"])
        with self.assertRaises(SystemExit):
            he.export(self.data, self.root / "x3", "Bad-Label")

    def test_the_hosted_model_unions_loaded_batches_typed_and_reads_no_batch_as_no_rows(self):
        text = ("source: t is duckdb.sql(\"\"\"\n  select * from read_csv('data/prod_turns.csv', "
                "header = true, columns = {\n    'turn_id': 'VARCHAR', 'n': 'INTEGER'})\n\"\"\")\n"
                "source: p is duckdb.sql(\"\"\"select * from read_csv('data/price_card.csv', "
                "header = true, columns = {'x': 'DOUBLE'})\"\"\")\n"
                "source: j is duckdb.sql(\"\"\"select * from read_csv('data/prod_judgments.csv', "
                "header = true, columns = {'right': 'BOOLEAN'})\"\"\")\n")
        out = he.hosted_model(text, {"prod_turns": ["prod_turns__b1", "prod_turns__b2"],
                                     "price_card": ["price_card__b1", "price_card__b2"]}, "evals")
        self.assertIn("evals.sql(", out)
        self.assertNotIn("duckdb.sql(", out)
        self.assertIn('TRY_CAST("n" AS INTEGER) AS "n"', out)
        self.assertIn("SELECT * FROM prod_turns__b1\n      UNION ALL BY NAME\n      SELECT * FROM prod_turns__b2", out)
        self.assertIn("SELECT * FROM price_card__b2", out)
        self.assertNotIn("price_card__b1", out)
        self.assertIn('(SELECT CAST(NULL AS BOOLEAN) AS "right" WHERE FALSE)', out)

    def test_the_hosted_package_is_a_sibling_never_inside_the_optimization_package(self):
        self.turns("a")
        he.export(self.data, self.root / "x1", "b1")
        pkg = he.write_hosted(self.root / "optimization-s", self.data, "evals")
        self.assertEqual(pkg, self.root / "optimization-s-hosted")
        self.assertIn("prod_turns__b1", (pkg / "history.malloy").read_text())
        self.assertTrue((pkg / "public" / "history" / "app.js").is_file())
        self.assertFalse((self.root / "optimization-s" / "hosted").exists())


if __name__ == "__main__":
    unittest.main()
