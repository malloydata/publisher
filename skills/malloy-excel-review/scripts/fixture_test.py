"""Structural invariants and classifier routes for the recipe fixture.

Lives in scripts/ so `bun run test:skills-python` (skills/*/scripts/*_test.py) discovers it;
the code under test is ../fixtures/build_fixture.py. Asserts structure, not bytes: an Excel
save of the fixture changes the bytes. The committed binary is checked against the engine
that fixtures/README.md records for it.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import re
import tempfile
import unittest
import zipfile
from unittest import mock
from xml.etree import ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "..", "fixtures")


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


bf = load("build_fixture", os.path.join(FIXTURES, "build_fixture.py"))
cw = load("classify_workbook", os.path.join(HERE, "classify_workbook.py"))

M = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
PR = "{http://schemas.openxmlformats.org/package/2006/relationships}"
SHEETS = ["Data", "Ledger", "Lookup", "Report", "Assumptions", "Forecast", "MonteCarlo", "_Config"]
FORMULA_CELLS = 1101
SECRET = "dummy-Passw0rd-not-real"


def truthy(v):
    return v in ("1", "true")


def quiet_main(args):
    with contextlib.redirect_stdout(io.StringIO()) as out:
        bf.main(args)
    return out.getvalue()


def readme_field(label):
    with open(os.path.join(FIXTURES, "README.md")) as f:
        m = re.search(r"^\| %s \| (.*) \|$" % re.escape(label), f.read(), re.M)
    return m.group(1) if m else None


def readme_engine():
    m = re.search(r"`(\w+)`", readme_field("Engine of the committed binary") or "")
    return m.group(1) if m else None


class Book:
    """A parsed xlsx: sheets resolved through the workbook rels, cells by (sheet, address)."""

    def __init__(self, path):
        self.z = zipfile.ZipFile(path)
        self.wb = ET.fromstring(self.z.read("xl/workbook.xml"))
        rels = {r.get("Id"): r.get("Target") for r in
                ET.fromstring(self.z.read("xl/_rels/workbook.xml.rels")).iter(PR + "Relationship")}
        self.sheets, self.paths = [], {}
        for s in self.wb.iter(M + "sheet"):
            target = rels[s.get(R + "id")]
            part = target.lstrip("/") if target.startswith("/") else "xl/" + target
            self.sheets.append((s.get("name"), s.get("state")))
            self.paths[s.get("name")] = part
        self.names = [n for n, _ in self.sheets]
        self.sst = [("".join(t.text or "" for t in si.iter(M + "t")))
                    for si in ET.fromstring(self.z.read("xl/sharedStrings.xml")).iter(M + "si")]
        self.cells = {}
        for name in self.names:
            for c in self.sheet_root(name).iter(M + "c"):
                self.cells[(name, c.get("r"))] = c

    def sheet_root(self, name):
        return ET.fromstring(self.z.read(self.paths[name]))

    def table(self):
        for n in self.z.namelist():
            if n.startswith("xl/tables/"):
                t = ET.fromstring(self.z.read(n))
                if t.get("name") == "tbl_Sales":
                    return t
        raise AssertionError("tbl_Sales missing")

    def v(self, sheet, a):
        c = self.cells[(sheet, a)]
        v = c.find(M + "v")
        if v is None:
            return None
        if c.get("t") == "s":
            return self.sst[int(v.text)]
        return v.text if c.get("t") in ("str", "e") else float(v.text)

    def formula_cells(self):
        return [c for c in self.cells.values() if c.find(M + "f") is not None]


class Invariants:
    """Structure every build and every engine's save must keep; `self.book` is set by the subclass."""

    pivot_allowed = False
    python_engine = False

    def test_sheet_list_and_states(self):
        self.assertEqual([(n, s) for n, s in self.book.sheets][:8],
                         [(n, "veryHidden" if n == "_Config" else None) for n in SHEETS])

    def test_pivot_only_in_an_excel_build(self):
        has = [n for n in self.book.z.namelist() if "pivot" in n.lower()]
        if self.pivot_allowed:
            self.assertEqual(self.book.names[-1], "Pivot")
        else:
            self.assertEqual(self.book.names, SHEETS)
            self.assertEqual(has, [])

    def test_calc_properties_and_modified_date(self):
        calc = self.book.wb.find(M + "calcPr")
        self.assertTrue(truthy(calc.get("iterate")))
        if self.python_engine:
            self.assertTrue(truthy(calc.get("fullCalcOnLoad")))
            self.assertIn("2024-06-30T12:00:00Z", self.book.z.read("docProps/core.xml").decode())

    def test_defined_names(self):
        names = {d.get("name"): d.text for d in self.book.wb.iter(M + "definedName")}
        self.assertEqual(names["GrowthRate"].replace("$", ""), "Assumptions!B3")
        self.assertIn("SalesData", names)

    def test_table_part(self):
        t = self.book.table()
        self.assertEqual((t.get("ref"), t.get("totalsRowCount")), ("A1:F13", "1"))
        self.assertEqual(t.find(M + "autoFilter").get("ref"), "A1:F12")
        calc = list(t.iter(M + "calculatedColumnFormula"))
        self.assertEqual(len(calc), 1)
        self.assertIn("[#This Row]", calc[0].text)
        funcs = [c.get("totalsRowFunction") for c in t.iter(M + "tableColumn") if c.get("totalsRowFunction")]
        self.assertEqual(funcs, ["sum"])
        self.assertEqual(self.book.v("Data", "A13"), "Total")
        self.assertEqual(self.book.cells[("Data", "F13")].find(M + "f").text, "SUBTOTAL(109,tbl_Sales[Revenue])")

    def test_data_table_cell(self):
        f = self.book.cells[("Assumptions", "E3")].find(M + "f")
        self.assertEqual((f.get("t"), f.get("ref"), f.get("r1")), ("dataTable", "E3:E6", "B3"))

    def test_merges_hidden_row_and_shared_formulas(self):
        ledger = self.book.sheet_root("Ledger")
        self.assertEqual(sorted(m.get("ref") for m in ledger.iter(M + "mergeCell")), ["A1:A2", "B1:B2", "C1:D1"])
        self.assertEqual([r.get("r") for r in ledger.iter(M + "row") if r.get("hidden") == "1"], ["5"])
        for sheet, a, ref in (("MonteCarlo", "B2", "B2:B1001"), ("Forecast", "B4", "B4:F4"), ("Report", "I2", "I2:I9")):
            f = self.book.cells[(sheet, a)].find(M + "f")
            self.assertEqual((f.get("t"), f.get("ref")), ("shared", ref), (sheet, a))

    def test_plug_cell_has_no_formula(self):
        self.assertIsNone(self.book.cells[("Report", "I6")].find(M + "f"))
        self.assertEqual(self.book.v("Report", "I6"), 999)

    def test_formula_count(self):
        self.assertEqual(len(self.book.formula_cells()), FORMULA_CELLS)

    def test_mixed_type_cells_keep_their_types(self):
        b = self.book
        self.assertEqual(b.v("Data", "C5"), "1")
        self.assertEqual(b.cells[("Data", "C5")].get("t"), "s")
        self.assertEqual(b.cells[("Data", "D8")].get("t"), "s")
        self.assertEqual(b.v("Data", "D8"), "2024-07-15")
        self.assertEqual(b.v("Data", "D10"), 60)
        self.assertNotIn(("Data", "A7"), b.cells)

    def test_constant_wide_block_on_assumptions(self):
        b = self.book
        self.assertEqual([b.v("Assumptions", c + "10") for c in "BCDEF"], [2025, 2026, 2027, 2028, 2029])
        self.assertEqual([b.v("Assumptions", c + "11") for c in "BCDEF"], [1000, 1200, 1500, 1800, 2000])

    def test_veryhidden_config_has_labelled_password(self):
        self.assertEqual(self.book.v("_Config", "A2"), "Password")


class FreshBuild(Invariants, unittest.TestCase):
    python_engine = True

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        quiet_main(["--recalc", "python", "--out-dir", cls.tmp.name])
        cls.main_path = os.path.join(cls.tmp.name, "fixture.xlsx")
        cls.y1904_path = os.path.join(cls.tmp.name, "fixture_1904.xlsx")
        cls.book = Book(cls.main_path)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_build_is_reproducible(self):
        with tempfile.TemporaryDirectory() as other:
            quiet_main(["--recalc", "python", "--out-dir", other])
            for name in ("fixture.xlsx", "fixture_1904.xlsx"):
                with open(os.path.join(other, name), "rb") as a, open(os.path.join(self.tmp.name, name), "rb") as b:
                    self.assertEqual(a.read(), b.read())

    def test_shared_string_counts(self):
        sst = ET.fromstring(self.book.z.read("xl/sharedStrings.xml"))
        refs = sum(1 for c in self.book.cells.values() if c.get("t") == "s")
        self.assertEqual((int(sst.get("count")), int(sst.get("uniqueCount"))), (refs, len(self.book.sst)))
        self.assertLess(len(self.book.sst), refs)


class CachedValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        quiet_main(["--recalc", "python", "--out-dir", cls.tmp.name])
        cls.book = Book(os.path.join(cls.tmp.name, "fixture.xlsx"))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_every_formula_cell_has_a_value_in_python_build(self):
        missing = [c.get("r") for c in self.book.formula_cells() if c.find(M + "v") is None]
        self.assertEqual(missing, [])

    def test_excel_quirk_values_hand_computed(self):
        b = self.book
        expected = {
            ("Report", "B3"): 77.75, ("Report", "B4"): 335.75, ("Report", "B5"): 260.75, ("Report", "B6"): 41,
            ("Report", "B8"): 9, ("Report", "B9"): 10, ("Report", "B10"): 3, ("Report", "B11"): 5,
            ("Report", "B12"): 712.5, ("Report", "B14"): 181, ("Report", "B15"): 289, ("Data", "F13"): 356.25,
            ("Lookup", "H2"): 0.1, ("Lookup", "H3"): 0, ("Lookup", "H5"): 0, ("Lookup", "H7"): 3, ("Lookup", "H8"): 2,
            ("Ledger", "C6"): 1250, ("Ledger", "D6"): 100, ("Ledger", "C10"): 0, ("Ledger", "D10"): 1250,
        }
        for (sheet, a), want in expected.items():
            self.assertAlmostEqual(b.v(sheet, a), want, places=9, msg="%s!%s" % (sheet, a))
        self.assertAlmostEqual(b.v("Report", "B7"), 26 / 9, places=12)
        self.assertAlmostEqual(b.v("Report", "B13"), 77.75 * 1.08, places=9)

    def test_numeric_criteria_ignore_the_text_one(self):
        # A text-coercing translation of ">=1" would add the text-"1" row (20.5) and give 356.25
        self.assertEqual(self.book.v("Report", "A1"), 1)
        self.assertNotAlmostEqual(self.book.v("Report", "B4"), 356.25)

    def test_no_match_is_na_error_cell(self):
        for a in ("H4", "H6"):
            c = self.book.cells[("Lookup", a)]
            self.assertEqual((c.get("t"), c.find(M + "v").text), ("e", "#N/A"))

    def test_circularity_converges_to_the_fixed_point(self):
        b = self.book
        for col in "BCDEF":
            opening, closing, interest = (b.v("Forecast", "%s%d" % (col, r)) for r in (2, 5, 4))
            flow = b.v("Forecast", col + "3")
            self.assertAlmostEqual(interest, 0.06 * (opening + closing) / 2, delta=0.01)
            self.assertAlmostEqual(closing, opening + flow + interest, places=9)

    def test_roll_forward_and_depreciation_rows(self):
        b = self.book
        self.assertEqual([b.v("Forecast", c + "3") for c in "BCDEF"], [1000, 1200, 1500, 1800, 2000])
        self.assertAlmostEqual(b.v("Forecast", "F6"), 10000 * 1.05 ** 5, places=6)
        self.assertEqual([b.v("Forecast", "%s8" % c) for c in "BCDEF"], [10000, 8000, 6000, 4000, 2000])
        self.assertEqual([b.v("Forecast", "%s9" % c) for c in "BCDEF"], [1000, 2200, 3700, 5500, 7500])
        self.assertAlmostEqual(b.v("Assumptions", "E3"), 10000 * 1.02 ** 5, places=6)
        self.assertAlmostEqual(b.v("Assumptions", "E6"), 10000 * 1.08 ** 5, places=6)

    def test_monte_carlo_is_seeded_and_plausible(self):
        draws = [self.book.v("MonteCarlo", "B%d" % r) for r in range(2, 1002)]
        mean = sum(draws) / len(draws)
        self.assertAlmostEqual(self.book.v("MonteCarlo", "C2"), mean, places=9)
        self.assertTrue(95 < mean < 105)


class EngineMapping(unittest.TestCase):
    def build(self, engine):
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        out = quiet_main(["--recalc", engine, "--out-dir", d.name])
        return d.name, out

    def test_excel_writes_no_values_in_either_book_and_prints_steps(self):
        d, out = self.build("excel")
        for name in ("fixture.xlsx", "fixture_1904.xlsx"):
            book = Book(os.path.join(d, name))
            self.assertTrue(book.formula_cells())
            self.assertEqual([c.get("r") for c in book.formula_cells() if c.find(M + "v") is not None], [])
        self.assertEqual(len(Book(os.path.join(d, "fixture.xlsx")).formula_cells()), FORMULA_CELLS)
        self.assertIn("Move or Copy", out)
        self.assertIn("Data!D8 must end as a string", out)

    def test_python_writes_values_in_both_books(self):
        d, _ = self.build("python")
        for name in ("fixture.xlsx", "fixture_1904.xlsx"):
            book = Book(os.path.join(d, name))
            self.assertTrue(book.formula_cells())
            self.assertTrue(all(c.find(M + "v") is not None for c in book.formula_cells()))
        self.assertEqual(Book(os.path.join(d, "fixture.xlsx")).table().get("totalsRowCount"), "1")

    def test_libreoffice_without_soffice_exits_and_writes_nothing(self):
        d = tempfile.TemporaryDirectory()
        self.addCleanup(d.cleanup)
        with mock.patch.object(bf.shutil, "which", return_value=None):
            with self.assertRaises(SystemExit) as cm:
                bf.main(["--recalc", "libreoffice", "--out-dir", d.name])
        self.assertIn("brew install --cask libreoffice", str(cm.exception))
        self.assertEqual(os.listdir(d.name), [])


class Date1904(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        quiet_main(["--recalc", "python", "--out-dir", cls.tmp.name])

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_1904_flag_and_offset(self):
        main = Book(os.path.join(self.tmp.name, "fixture.xlsx"))
        b = Book(os.path.join(self.tmp.name, "fixture_1904.xlsx"))
        self.assertTrue(truthy(b.wb.find(M + "workbookPr").get("date1904")))
        self.assertFalse(truthy(main.wb.find(M + "workbookPr").get("date1904")))
        self.assertEqual(main.v("Data", "D2") - b.v("Data", "A2"), 1462)
        self.assertEqual([c.find(M + "v") is not None for c in b.formula_cells()], [True])


class CommittedBinaries(Invariants, unittest.TestCase):
    """The committed fixture.xlsx, held to the invariants and to the engine README records."""

    @classmethod
    def setUpClass(cls):
        cls.engine = readme_engine()
        cls.path = os.path.join(FIXTURES, "fixture.xlsx")
        cls.book = Book(cls.path)
        cls.pivot_allowed = cls.engine == "excel"
        cls.python_engine = cls.engine == "python"

    def test_readme_names_a_known_engine(self):
        self.assertIn(self.engine, ("python", "libreoffice", "excel"))

    def test_excel_pivot_definition(self):
        if self.engine != "excel":
            self.skipTest("engine is %s" % self.engine)
        z = self.book.z
        tables = [n for n in z.namelist() if n.startswith("xl/pivotTables/pivotTable") and n.endswith(".xml")]
        caches = [n for n in z.namelist() if n.startswith("xl/pivotCache/pivotCacheDefinition") and n.endswith(".xml")]
        self.assertTrue(tables and caches, "engine is excel but the file has no pivot")
        pt = ET.fromstring(z.read(tables[0]))
        self.assertIsNotNone(pt.find(M + "pageFields"), "no page field")
        self.assertIn("percentOfTotal", [d.get("showDataAs") for d in pt.iter(M + "dataField")])
        cache = ET.fromstring(z.read(caches[0]))
        self.assertTrue([c for c in cache.iter(M + "cacheField") if c.get("formula")], "no calculated field")
        self.assertIsNotNone(next(cache.iter(M + "fieldGroup"), None), "OrderDate is not grouped")

    @staticmethod
    def round_floats(raw):
        # libm differs by platform in the last digit of a generated float
        return re.sub(rb"-?\d+\.\d{10,}(?:[eE][-+]?\d+)?", lambda m: b"%.9g" % float(m.group()), raw)

    def test_python_engine_binary_matches_a_fresh_build(self):
        if self.engine != "python":
            self.skipTest("engine is %s" % self.engine)
        with tempfile.TemporaryDirectory() as d:
            quiet_main(["--recalc", "python", "--out-dir", d])
            for name in ("fixture.xlsx", "fixture_1904.xlsx"):
                a, b = zipfile.ZipFile(os.path.join(FIXTURES, name)), zipfile.ZipFile(os.path.join(d, name))
                self.assertEqual(sorted(a.namelist()), sorted(b.namelist()))
                for part in a.namelist():
                    self.assertEqual(self.round_floats(a.read(part)), self.round_floats(b.read(part)), "%s:%s differs: rebuild the fixture" % (name, part))

    def test_every_formula_cell_has_a_cached_value(self):
        self.assertEqual([c.get("r") for c in self.book.formula_cells() if c.find(M + "v") is None], [])

    def test_1904_book_flag(self):
        y = Book(os.path.join(FIXTURES, "fixture_1904.xlsx"))
        self.assertTrue(truthy(y.wb.find(M + "workbookPr").get("date1904")))

    def test_readme_records_both_binary_shas_and_the_generator_hash(self):
        for label, name in (("`fixture.xlsx` SHA-256", "fixture.xlsx"), ("`fixture_1904.xlsx` SHA-256", "fixture_1904.xlsx")):
            with open(os.path.join(FIXTURES, name), "rb") as f:
                self.assertEqual(readme_field(label), "`%s`" % hashlib.sha256(f.read()).hexdigest(), name)
        with open(os.path.join(FIXTURES, "build_fixture.py"), "rb") as f:
            self.assertEqual(readme_field("Generator source SHA-256"), "`%s`" % hashlib.sha256(f.read()).hexdigest())


class ClassifierRoutes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        quiet_main(["--recalc", "python", "--out-dir", cls.tmp.name])
        cls.rep = cw.analyze(os.path.join(cls.tmp.name, "fixture.xlsx"))
        cls.region = {(r["sheet"], r["ref"]): r for r in cls.rep["regions"]}

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def flags(self, sheet, ref):
        return {f["id"] for f in self.region[(sheet, ref)]["flags"]}

    def test_status_and_oracle_untrusted_until_recalculated(self):
        self.assertEqual(self.rep["status"], "ok")
        self.assertEqual(self.rep["oracle"]["status"], "untrusted")
        self.assertTrue(any(t["tell"] == "full_calc_on_load" for t in self.rep["oracle"]["tells"]))

    def test_vlookup_true_routes_as_approximate_range_join(self):
        for ref in ("H2", "H3"):
            r = self.region[("Lookup", ref)]
            self.assertEqual(r["route"], "C")
            self.assertIn("approx_match", self.flags("Lookup", ref))
            self.assertIn("range join", " ".join(r["reasons"]))
        self.assertNotIn("approx_match", self.flags("Lookup", "H4"))
        self.assertIn("approx_match", self.flags("Lookup", "H7"))

    def test_sumifs_criteria_routes(self):
        self.assertIn("criteria_comparison", self.flags("Report", "B4"))
        self.assertIn("criteria_cell", self.flags("Report", "B4"))
        self.assertIn("criteria_cell", self.flags("Report", "B15"))
        self.assertIn("criteria_wildcard", self.flags("Report", "B5"))
        self.assertIn("criteria_blank", self.flags("Report", "B6"))
        for ref in ("B3", "B5", "B6", "B11"):
            self.assertIn("ci_match", self.flags("Report", ref))
        # a comparison built from a number or date cell is numeric, so text case-insensitivity does not apply
        for ref in ("B4", "B10", "B15"):
            self.assertNotIn("ci_match", self.flags("Report", ref))

    def test_full_column_double_count(self):
        self.assertIn("full_column_total", self.flags("Report", "B12"))
        self.assertIn("full_column", self.flags("Report", "B10"))

    def test_hardcoded_constant_and_plug(self):
        self.assertIn("hardcoded_constant", self.flags("Report", "B13"))
        self.assertIn("plug", self.flags("Report", "I2:I9"))
        self.assertEqual(self.region[("Report", "I2:I9")]["plugs"], ["I6"])

    def test_rand_routes_x_and_today_is_pinned(self):
        self.assertEqual(self.region[("MonteCarlo", "B2:B1001")]["route"], "X")
        self.assertIn("volatile", self.flags("Report", "B14"))
        self.assertEqual(self.rep["workbook_props"]["modified"], "2024-06-30T12:00:00Z")

    def test_circularity_is_a_real_cycle(self):
        g = self.rep["graph"]
        self.assertTrue(g["cycles"])
        self.assertTrue(g["iterate"])
        self.assertIn("real circularity", g["iterate_note"])
        loop = {self.region[("Forecast", "B4:F4")]["id"], self.region[("Forecast", "B5:F5")]["id"]}
        self.assertTrue(all(set(c["regions"]) == loop for c in g["cycles"]))
        for ref in ("B4:F4", "B5:F5"):
            self.assertEqual(self.region[("Forecast", ref)]["route"], "C")

    def test_data_table_is_excluded_from_lifting(self):
        self.assertEqual(self.region[("Assumptions", "E3:E6")]["kind"], "datatable")
        self.assertIn({"sheet": "Assumptions", "kind": "data_table", "ref": "E3:E6"}, self.rep["excluded_ranges"])
        src = next(s for s in self.rep["sources"] if s["sheet"] == "Assumptions" and s["ref"].startswith("D1"))
        self.assertEqual(src["lifted"], ["Growth sensitivity"])
        self.assertEqual(len(src["not_lifted"]), 1)

    def test_constant_wide_block_unpivots(self):
        src = next(s for s in self.rep["sources"] if s["sheet"] == "Assumptions" and s["ref"] == "A10:F11")
        self.assertEqual((src["layout"], src["period_columns"]), ("wide", 5))
        self.assertIn("UNPIVOT", src["stanza"])
        self.assertIn("range = 'A11:F11'", src["stanza"])

    def test_no_formula_cell_is_lifted_from_the_data_table(self):
        src = next(s for s in self.rep["sources"] if s.get("name") == "tbl_Sales")
        self.assertEqual(src["data_ref"], "A1:F12")
        self.assertEqual(src["lifted"], ["Region", "Product", "Qty", "OrderDate", "Price"])
        self.assertEqual(src["not_lifted_reasons"], {"Revenue": "formula column"})
        self.assertEqual(src["formula_cells_in_lifted"], [])
        self.assertNotIn("Revenue", src["stanza"].split("SELECT")[1].split("FROM")[0])

    def test_total_row_trimmed_and_subtotal_excluded(self):
        led = next(s for s in self.rep["sources"] if s["sheet"] == "Ledger")
        self.assertIn("NOT ILIKE '%subtotal%'", led["stanza"])
        self.assertEqual(led["hidden_rows"], [5])

    def test_tables_and_names(self):
        t = self.rep["tables"][0]
        self.assertEqual((t["name"], t["data_ref"], t["totals_row_count"]), ("tbl_Sales", "A1:F12", 1))
        self.assertEqual({n["name"] for n in self.rep["defined_names"]}, {"GrowthRate", "SalesData"})

    def test_config_sheet_is_masked_and_value_never_emitted(self):
        cfg = next(s for s in self.rep["sheets"] if s["name"] == "_Config")
        self.assertEqual(cfg["state"], "veryHidden")
        src = next(s for s in self.rep["sources"] if s["sheet"] == "_Config")
        self.assertFalse(src.get("stanza"))
        self.assertTrue(any(f["flag"] == "secret_labelled_cell" for f in self.rep["security"]))
        self.assertEqual(Book(os.path.join(self.tmp.name, "fixture.xlsx")).v("_Config", "B2"), SECRET)
        self.assertNotIn(SECRET, json.dumps(self.rep))
        self.assertNotIn(SECRET, cw.render_text(self.rep))


if __name__ == "__main__":
    unittest.main()
