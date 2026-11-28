"""optimization_package.py is the one definition of each table's columns.

The two models read every CSV with an explicit column list, so DuckDB never
guesses a type. Those lists are a second copy, and a column added to one copy
and not the other stops the model loading. This test holds them equal.
"""
import pathlib
import re
import shutil
import tempfile
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import optimization_package as op  # noqa: E402

READ = re.compile(r"read_csv\('data/(\w+)\.csv'[^)]*?columns\s*=\s*\{(.*?)\}\)", re.S)


class Columns(unittest.TestCase):
    def test_every_csv_a_model_reads_lists_the_columns_the_table_defines(self):
        tables = {**{t: v[2] for t, v in op.APPENDED.items()}, **op.REBUILT,
                  "price_card": op.PRICE_COLUMNS}
        seen = 0
        for model in sorted(op.TEMPLATE.glob("*.malloy")):
            for table, cols in READ.findall(model.read_text()):
                seen += 1
                with self.subTest(model=model.name, table=table):
                    self.assertIn(table, tables)
                    self.assertEqual(tuple(re.findall(r"'(\w+)'\s*:", cols)), tuple(tables[table]))
        self.assertGreater(seen, 10)



class Append(unittest.TestCase):
    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)

    def test_a_table_that_gained_a_column_keeps_its_rows_and_new_rows_line_up(self):
        path = self.dir / "t.csv"
        op.append(path, ("a", "b"), [{"a": "1", "b": "2"}])
        op.append(path, ("a", "x", "b"), [{"a": "3", "x": "9", "b": "4"}])
        self.assertEqual(path.read_text().splitlines()[0], "a,x,b")
        self.assertEqual(op.read(path), [{"a": "1", "x": "", "b": "2"}, {"a": "3", "x": "9", "b": "4"}])

    def test_a_dropped_column_is_refused_not_lost(self):
        path = self.dir / "t.csv"
        op.append(path, ("a", "b"), [{"a": "1", "b": "2"}])
        with self.assertRaises(SystemExit) as e:
            op.append(path, ("a",), [])
        self.assertIn("column(s) b", str(e.exception))
        self.assertEqual(op.read(path), [{"a": "1", "b": "2"}])


if __name__ == "__main__":
    unittest.main()
