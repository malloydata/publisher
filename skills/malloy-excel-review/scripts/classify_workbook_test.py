#!/usr/bin/env python3
"""Fixture workbooks are built from hand-written XML in a tempdir, so every tell
the script relies on is exercised, including the ones no public file carries.

A fixture proves the parser reads the tell. It does not prove Excel writes the
tell that way; the tells marked unconfirmed in the script are the ones still
waiting on a real file."""
import base64
import io
import json
import os
import pathlib
import re
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import warnings
import zipfile
import zlib
from unittest import mock
from xml.sax.saxutils import escape

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import classify_workbook as cw  # noqa: E402

SCRIPT = pathlib.Path(__file__).resolve().parent / "classify_workbook.py"

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RNS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
RT = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/"
PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
S_MAIN = "http://purl.oclc.org/ooxml/spreadsheetml/main"
S_RNS = "http://purl.oclc.org/ooxml/officeDocument/relationships"
S_RT = "http://purl.oclc.org/ooxml/officeDocument/relationships/"

CALC = '<calcPr calcId="191029"/>'


# --------------------------------------------------------------------------
# Fixture builder
# --------------------------------------------------------------------------

class F:
    """A formula cell. `v=None` writes no <v> at all (a library-written file)."""

    def __init__(self, text, v=0, t=None, fa=None, ca=None):
        self.text, self.v, self.t, self.fa, self.ca = text, v, t, fa or {}, ca or {}


def col_letters(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def cell_xml(ref, val):
    if val is None:
        return ""
    if isinstance(val, F):
        attrs = dict(val.ca)
        if val.t:
            attrs["t"] = val.t
        a = "".join(f' {k}="{v}"' for k, v in attrs.items())
        fa = "".join(f' {k}="{v}"' for k, v in val.fa.items())
        f = f"<f{fa}>{escape(val.text)}</f>" if val.text else f"<f{fa}/>"
        v = "" if val.v is None else f"<v>{escape(str(val.v))}</v>"
        return f'<c r="{ref}"{a}>{f}{v}</c>'
    if isinstance(val, bool):
        return f'<c r="{ref}" t="b"><v>{int(val)}</v></c>'
    if isinstance(val, (int, float)):
        return f'<c r="{ref}"><v>{val}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(val)}</t></is></c>'


def grid(rows, top=1, left=1):
    """rows: lists of values. A row may be (attrs_dict, [values])."""
    out = []
    for i, row in enumerate(rows):
        attrs = {}
        if isinstance(row, tuple):
            attrs, row = row
        r = top + i
        a = "".join(f' {k}="{v}"' for k, v in attrs.items())
        cells = "".join(cell_xml(f"{col_letters(left + j)}{r}", v) for j, v in enumerate(row))
        out.append(f'<row r="{r}"{a}>{cells}</row>')
    return "".join(out)


class Sheet:
    def __init__(self, name, rows=(), state=None, before="", after="", rels=(), top=1, left=1):
        self.name, self.state, self.before, self.after = name, state, before, after
        self.rels, self.data = rels, grid(rows, top, left)


def make_book(tmp, sheets, calc=CALC, names="", wb_pr="", parts=None, strict=False,
              filename="book.xlsx", extra_wb="", wb_rels_extra="", root_rels=None):
    main, rns, rt = (S_MAIN, S_RNS, S_RT) if strict else (MAIN, RNS, RT)
    z = {}
    n = len(sheets)
    sheet_tags, rel_tags = [], []
    for i, sh in enumerate(sheets):
        fname = f"sheet{n - i}.xml"  # reversed, so nothing can map by filename
        state = f' state="{sh.state}"' if sh.state else ""
        sheet_tags.append(f'<sheet name="{escape(sh.name)}" sheetId="{10 + i}"{state} r:id="rId{i + 5}"/>')
        rel_tags.append(f'<Relationship Id="rId{i + 5}" Type="{rt}worksheet" Target="worksheets/{fname}"/>')
        z[f"xl/worksheets/{fname}"] = (
            f'<worksheet xmlns="{main}" xmlns:r="{rns}">{sh.before}<sheetData>{sh.data}</sheetData>{sh.after}</worksheet>')
        if sh.rels:
            body = "".join(f'<Relationship Id="{rid}" Type="{rt}{typ}" Target="{tgt}"/>' for typ, tgt, rid in sh.rels)
            z[f"xl/worksheets/_rels/{fname}.rels"] = f'<Relationships xmlns="{PKG_REL}">{body}</Relationships>'
    z["xl/workbook.xml"] = (
        f'<workbook xmlns="{main}" xmlns:r="{rns}">{wb_pr}<sheets>{"".join(sheet_tags)}</sheets>'
        f'{names}{calc}{extra_wb}</workbook>')
    z["xl/_rels/workbook.xml.rels"] = f'<Relationships xmlns="{PKG_REL}">{"".join(rel_tags)}{wb_rels_extra}</Relationships>'
    z["_rels/.rels"] = root_rels or (
        f'<Relationships xmlns="{PKG_REL}"><Relationship Id="rId1" Type="{rt}officeDocument" Target="xl/workbook.xml"/></Relationships>')
    z.update(parts or {})
    path = os.path.join(tmp, filename)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in z.items():
            zf.writestr(name, data)
    return path


def one_sheet(tmp, rows, name="S", **kw):
    return make_book(tmp, [Sheet(name, rows)], **kw)


def region(rep, sheet, ref):
    hits = [r for r in rep["regions"] if r["sheet"] == sheet and r["ref"] == ref]
    assert hits, f"no region {sheet}!{ref} in {[(r['sheet'], r['ref']) for r in rep['regions']]}"
    return hits[0]


def flag_ids(reg):
    return {f["id"] for f in reg["flags"]}


def tell_ids(rep):
    return {t["tell"] for t in rep["oracle"]["tells"]}


def sec_ids(rep):
    return {s["flag"] for s in rep["security"]}


class Tmp(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.tmp = self._td.name
        self.addCleanup(self._td.cleanup)

    def run_formula(self, formula, extra_rows=(), v=0, ca=None, **kw):
        """A sheet S with numbers in A1:C4 and `formula` at E1."""
        rows = [[1, 10, "x", None, F(formula, v, ca=ca)],
                [2, 20, "y"], [3, 30, "z"], [4, 40, "w"]] + list(extra_rows)
        rep = cw.analyze(one_sheet(self.tmp, rows, **kw))
        return rep, region(rep, "S", "E1")


# --------------------------------------------------------------------------
# Package hygiene
# --------------------------------------------------------------------------

class Hygiene(Tmp):
    def raw(self, data, name="b.xlsx"):
        p = os.path.join(self.tmp, name)
        with open(p, "wb") as fh:
            fh.write(data)
        return p

    def test_cfb_with_EncryptedPackage_is_reported_as_encrypted(self):
        data = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1" + b"\0" * 512 + "EncryptedPackage".encode("utf-16-le") + b"\0" * 64
        rep = cw.analyze(self.raw(data))
        self.assertEqual(rep["status"], "unreadable")
        self.assertEqual(rep["error"]["kind"], "encrypted")
        self.assertIn("unencrypted", rep["error"]["message"])

    def test_cfb_with_Workbook_stream_is_a_legacy_xls(self):
        data = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1" + b"\0" * 512 + "Workbook".encode("utf-16-le") + b"\0" * 64
        rep = cw.analyze(self.raw(data))
        self.assertEqual(rep["error"]["kind"], "legacy_xls")
        self.assertIn(".xlsx", rep["error"]["message"])

    def test_random_bytes_are_not_a_zip(self):
        rep = cw.analyze(self.raw(b"hello, not a zip"))
        self.assertEqual(rep["error"]["kind"], "not_zip")

    def test_xlsb_is_named_and_refused(self):
        p = make_book(self.tmp, [Sheet("S")], parts={"xl/workbook.bin": b"\0"})
        with zipfile.ZipFile(p) as zf:
            names = zf.namelist()
        self.assertIn("xl/workbook.bin", names)
        os.remove(p)
        with zipfile.ZipFile(p, "w") as zf:
            zf.writestr("xl/workbook.bin", b"\0")
        self.assertEqual(cw.analyze(p)["error"]["kind"], "xlsb")

    def test_too_many_entries_is_refused_before_anything_is_read(self):
        p = make_book(self.tmp, [Sheet("S")], parts={f"junk/{i}.txt": "x" for i in range(20)})
        with mock.patch.object(cw, "MAX_ENTRIES", 10):
            rep = cw.analyze(p)
        self.assertEqual(rep["error"]["kind"], "too_many_entries")

    def test_a_part_over_the_cap_is_rejected_and_named(self):
        rows = [[i] for i in range(200)]
        p = one_sheet(self.tmp, rows)
        with mock.patch.object(cw, "MAX_PART_BYTES", 500):
            rep = cw.analyze(p)
        self.assertTrue(any(r["reason"] == "too_large" for r in rep["package"]["rejected"]))
        self.assertIn("part_too_large", sec_ids(rep))

    def test_a_forged_size_header_is_not_trusted_to_accept_a_truncated_part(self):
        big = b"<a>" + b" " * 5000 + b"</a>"
        p = os.path.join(self.tmp, "forged.xlsx")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("xl/workbook.xml", big)
        raw = bytearray(buf.getvalue())
        crc = zlib.crc32(big[:10])  # sizes AND crc agree with the first 10 bytes: zipfile.read() returns them happily
        struct.pack_into("<I", raw, raw.find(b"PK\x01\x02") + 24, 10)
        struct.pack_into("<I", raw, raw.find(b"PK\x03\x04") + 22, 10)
        struct.pack_into("<I", raw, raw.find(b"PK\x01\x02") + 16, crc)
        struct.pack_into("<I", raw, raw.find(b"PK\x03\x04") + 14, crc)
        with open(p, "wb") as fh:
            fh.write(bytes(raw))
        with zipfile.ZipFile(p) as zf:
            self.assertEqual(len(zf.read("xl/workbook.xml")), 10)  # the naive reader accepts the truncation
        rep = cw.analyze(p)
        self.assertEqual(rep["status"], "unreadable")
        self.assertTrue(rep["package"]["rejected"])

    def test_total_cap_never_overshoots(self):
        p = make_book(self.tmp, [Sheet("S", [[i] for i in range(300)])])
        with mock.patch.object(cw, "MAX_TOTAL_BYTES", 2000):
            pkg = cw.Package(p)
            self.assertIsNone(pkg.read("xl/worksheets/sheet1.xml"))
            self.assertLessEqual(pkg.bytes_read, 2000)
            self.assertEqual(pkg.rejected[-1]["reason"], "total_cap")

    def test_total_bytes_cap_stops_reading(self):
        p = one_sheet(self.tmp, [[i] for i in range(200)])
        with mock.patch.object(cw, "MAX_TOTAL_BYTES", 600):
            rep = cw.analyze(p)
        self.assertTrue(any(r["reason"] == "total_cap" for r in rep["package"]["rejected"]))

    def test_doctype_in_a_part_is_rejected_not_parsed(self):
        evil = ('<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;">]>'
                f'<worksheet xmlns="{MAIN}"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>&b;</t></is></c></row></sheetData></worksheet>')
        p = make_book(self.tmp, [Sheet("S")])
        with zipfile.ZipFile(p) as zf:
            parts = {n: zf.read(n) for n in zf.namelist()}
        parts["xl/worksheets/sheet1.xml"] = evil.encode()
        with zipfile.ZipFile(p, "w") as zf:
            for n, d in parts.items():
                zf.writestr(n, d)
        rep = cw.analyze(p)
        self.assertIn("xml_dtd_rejected", sec_ids(rep))
        self.assertTrue(any(r["reason"] == "dtd" for r in rep["package"]["rejected"]))

    def test_doctype_is_found_through_a_utf16_bom_and_after_a_comment(self):
        for enc, bom in (("utf-16-le", b"\xff\xfe"), ("utf-16-be", b"\xfe\xff")):
            xml = '<?xml version="1.0"?><!-- c --><!DOCTYPE x [<!ENTITY a "b">]><worksheet/>'
            data = bom + xml.encode(enc)
            with self.assertRaises(cw.Rejected) as cm:
                cw.parse_xml(data)
            self.assertEqual(cm.exception.reason, "dtd")

    def test_a_dtd_in_a_non_sheet_part_is_rejected_too(self):
        evil = b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]><cp:coreProperties xmlns:cp="urn:x"/>'
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], parts={"docProps/core.xml": evil}))
        self.assertIn("xml_dtd_rejected", sec_ids(rep))
        self.assertEqual(rep["sheets"][0]["constant_cells"], 1)

    def test_entity_only_prolog_is_rejected_too(self):
        with self.assertRaises(cw.Rejected):
            cw.parse_xml(b'<?xml version="1.0"?><!ENTITY a "b"><x/>')

    def test_a_dtd_lookalike_in_element_text_is_not_a_dtd(self):
        el = cw.parse_xml(b'<x>about &lt;!DOCTYPE and &lt;!ENTITY</x>')
        self.assertEqual(cw.ln(el.tag), "x")

    def test_unsafe_member_names_are_ignored_and_counted_never_extracted(self):
        p = make_book(self.tmp, [Sheet("S", [[1]])],
                      parts={"../escape.xml": "<x/>", "/abs.xml": "<x/>", "a/../../b.xml": "<x/>"})
        rep = cw.analyze(p)
        self.assertEqual(rep["package"]["unsafe_names"], 3)
        self.assertIn("unsafe_member_name", sec_ids(rep))
        self.assertFalse(os.path.exists(os.path.join(os.path.dirname(self.tmp), "escape.xml")))

    def test_strict_namespaces_read_the_same_as_transitional(self):
        rows = [[1, 2, F("A1+B1", 3)]]
        rep_t = cw.analyze(one_sheet(self.tmp, rows, filename="t.xlsx"))
        rep_s = cw.analyze(one_sheet(self.tmp, rows, strict=True, filename="s.xlsx"))
        self.assertEqual(region(rep_s, "S", "C1")["r1c1"], region(rep_t, "S", "C1")["r1c1"])
        self.assertEqual(rep_s["sheets"][0]["formula_cells"], 1)

    def test_sheets_map_through_the_relationships_not_the_filename_or_sheetId(self):
        p = make_book(self.tmp, [Sheet("First", [[1, F("A1*2", 2)]]), Sheet("Second", [[5]])])
        rep = cw.analyze(p)
        by = {s["name"]: s for s in rep["sheets"]}
        self.assertEqual(by["First"]["formula_cells"], 1)
        self.assertEqual(by["Second"]["formula_cells"], 0)
        self.assertEqual(by["First"]["path"], "xl/worksheets/sheet2.xml")

    def test_only_the_root_relationship_locates_the_workbook_part(self):
        p = make_book(self.tmp, [Sheet("S", [[1]])])
        with zipfile.ZipFile(p) as zf:
            parts = {n: zf.read(n) for n in zf.namelist()}
        parts["xl/book2.xml"] = parts.pop("xl/workbook.xml")
        parts["xl/_rels/book2.xml.rels"] = parts.pop("xl/_rels/workbook.xml.rels")
        parts["_rels/.rels"] = (f'<Relationships xmlns="{PKG_REL}"><Relationship Id="r" Type="{RT}officeDocument" '
                                'Target="xl/book2.xml"/></Relationships>').encode()
        with zipfile.ZipFile(p, "w") as zf:
            for n, d in parts.items():
                zf.writestr(n, d)
        self.assertEqual(cw.analyze(p)["sheets"][0]["name"], "S")


# --------------------------------------------------------------------------
# workbook.xml
# --------------------------------------------------------------------------

class WorkbookXml(Tmp):
    def test_calcpr_and_date1904(self):
        p = one_sheet(self.tmp, [[1]], calc='<calcPr calcId="191029" iterate="1" iterateCount="50"/>',
                      wb_pr='<workbookPr date1904="1"/>')
        rep = cw.analyze(p)
        self.assertTrue(rep["workbook_props"]["date1904"])
        self.assertTrue(rep["workbook_props"]["calc"]["iterate"])
        self.assertEqual(rep["workbook_props"]["calc"]["calcId"], "191029")

    def test_defined_names_are_classified_and_constants_carry_no_value(self):
        names = ('<definedNames>'
                 '<definedName name="Rate">Assump!$B$3</definedName>'
                 '<definedName name="TaxRate">0.21</definedName>'
                 '<definedName name="Pwd">"hunter2-not-emitted"</definedName>'
                 '<definedName name="Dyn">OFFSET(Assump!$A$1,0,0,5,1)</definedName>'
                 '<definedName name="Sq" localSheetId="0">_xlfn.LAMBDA(_xlpm.x,_xlpm.x*_xlpm.x)</definedName>'
                 '<definedName name="solver_opt" hidden="1">Assump!$B$3</definedName>'
                 '</definedNames>')
        p = make_book(self.tmp, [Sheet("Assump", [[1, 2]])], names=names)
        rep = cw.analyze(p)
        kinds = {n["name"]: n["kind"] for n in rep["defined_names"]}
        self.assertEqual(kinds["Rate"], "ref")
        self.assertEqual(kinds["TaxRate"], "constant")
        self.assertEqual(kinds["Pwd"], "constant")
        self.assertEqual(kinds["Dyn"], "formula")
        self.assertEqual(kinds["Sq"], "lambda")
        self.assertNotIn("hunter2-not-emitted", json.dumps(rep))
        self.assertIn("lambda_name", rep["code_attached"])
        self.assertIn("solver", rep["code_attached"])

    def test_hidden_and_veryhidden_sheets(self):
        p = make_book(self.tmp, [Sheet("A", [[1]]), Sheet("H", [[2]], state="hidden"),
                                 Sheet("V", [[3]], state="veryHidden")])
        rep = cw.analyze(p)
        st = {s["name"]: (s["state"], s["masked"]) for s in rep["sheets"]}
        self.assertEqual(st, {"A": ("visible", False), "H": ("hidden", True), "V": ("veryHidden", True)})
        self.assertIn("veryhidden_sheet", sec_ids(rep))


# --------------------------------------------------------------------------
# Oracle gate
# --------------------------------------------------------------------------

class OracleGate(Tmp):
    def book(self, rows, **kw):
        return cw.analyze(one_sheet(self.tmp, rows, **kw))

    def test_an_excel_saved_file_is_trusted(self):
        rep = self.book([[1, F("A1*2", 2)]])
        self.assertEqual(rep["oracle"]["status"], "trusted")
        self.assertEqual(rep["oracle"]["tells"], [])

    def test_fullCalcOnLoad_is_untrusted(self):
        rep = self.book([[1, F("A1*2", 0)]], calc='<calcPr calcId="1" fullCalcOnLoad="1"/>')
        self.assertEqual(rep["oracle"]["status"], "untrusted")
        self.assertIn("full_calc_on_load", tell_ids(rep))

    def test_a_formula_without_v_is_untrusted_and_counted(self):
        rep = self.book([[1, F("A1*2", None), F("A1*3", None), F("A1*4", 4)]])
        tell = [t for t in rep["oracle"]["tells"] if t["tell"] == "formula_without_v"][0]
        self.assertEqual(tell["count"], 2)
        self.assertEqual(rep["oracle"]["status"], "untrusted")

    def raw_sheet(self, cells):
        """A sheet whose formula cells are written verbatim, so <v></v> and <v/> reach the parser."""
        body = "".join(f'<c r="{r}"{t}><f>{f}</f>{v}</c>' for r, t, f, v in cells)
        return cw.analyze(self.raw_book(body))

    def raw_book(self, body):
        p = make_book(self.tmp, [Sheet("S")])
        with zipfile.ZipFile(p) as zf:
            parts = {n: zf.read(n) for n in zf.namelist()}
        key = [n for n in parts if n.startswith("xl/worksheets/sheet")][0]
        parts[key] = parts[key].replace(b"<sheetData></sheetData>", f"<sheetData><row r=\"1\">{body}</row></sheetData>".encode())
        with zipfile.ZipFile(p, "w") as zf:
            for n, d in parts.items():
                zf.writestr(n, d)
        return p

    def test_a_formula_with_an_empty_or_self_closing_v_has_no_cached_value(self):
        rep = self.raw_sheet([("A1", "", "B1+1", "<v></v>"), ("B1", "", "C1+1", "<v/>"), ("C1", "", "D1+1", "")])
        tell = [t for t in rep["oracle"]["tells"] if t["tell"] == "formula_without_v"][0]
        self.assertEqual(tell["count"], 3)
        self.assertEqual(rep["oracle"]["status"], "untrusted")
        self.assertEqual(rep["oracle"]["cached"], {})
        self.assertIn("saved without calculating", tell["detail"])
        self.assertFalse(rep["oracle"]["excel_saved"])

    def test_empty_v_among_populated_cells_counts_only_the_empty_ones(self):
        rep = self.raw_sheet([("A1", "", "B1+1", "<v></v>"), ("B1", "", "C1+1", "<v>2</v>"), ("C1", "", "D1+1", "<v/>")])
        tell = [t for t in rep["oracle"]["tells"] if t["tell"] == "formula_without_v"][0]
        self.assertEqual(tell["count"], 2)
        self.assertEqual(rep["oracle"]["cached"], {"number": 1})

    def test_a_str_formula_with_an_empty_v_is_a_cached_empty_string(self):
        rep = self.raw_sheet([("A1", ' t="str"', 'IF(1,"","x")', "<v></v>"), ("B1", ' t="str"', 'IF(1,"","x")', "<v/>")])
        self.assertNotIn("formula_without_v", tell_ids(rep))
        self.assertEqual(rep["oracle"]["cached"], {"string": 2})

    def test_missing_calcId_is_untrusted_even_when_every_formula_has_v(self):
        for calc in ("", "<calcPr/>"):
            rep = self.book([[1, F("A1*2", 2)]], calc=calc)
            self.assertIn("missing_calc_id", tell_ids(rep), calc)
            self.assertEqual(rep["oracle"]["status"], "untrusted")

    def test_manual_calc_and_calcOnSave_off_ask_for_a_recalculated_save(self):
        rep = self.book([[1, F("A1*2", 2)]], calc='<calcPr calcId="1" calcMode="manual" calcOnSave="0"/>')
        self.assertTrue({"manual_calc", "calc_on_save_off"} <= tell_ids(rep))
        self.assertEqual(rep["oracle"]["status"], "untrusted")

    def test_volatile_functions_are_a_caveat_not_untrusted(self):
        rep = self.book([[1, F("TODAY()-A1", 5), F("RAND()", 0.3), F("OFFSET(A1,0,0)", 1)]])
        tell = [t for t in rep["oracle"]["tells"] if t["tell"] == "volatile"][0]
        self.assertEqual(tell["severity"], "caveat")
        for fn in ("TODAY", "RAND", "OFFSET"):
            self.assertIn(fn, tell["detail"])
        self.assertEqual(rep["oracle"]["status"], "caveats")

    def test_modified_date_is_surfaced_for_pinning_today(self):
        core = ('<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
                'xmlns:dcterms="http://purl.org/dc/terms/"><dcterms:modified>2024-03-05T10:00:00Z</dcterms:modified>'
                '</cp:coreProperties>')
        rep = self.book([[1, F("TODAY()", 5)]], parts={"docProps/core.xml": core})
        self.assertEqual(rep["workbook_props"]["modified"], "2024-03-05T10:00:00Z")

    def test_cached_value_types_are_counted_separately(self):
        rows = [[F("1", 1), F('"a"', "a", t="str"), F("TRUE", 1, t="b"),
                 F("NA()", "#N/A", t="e"), F("1/0", "#DIV/0!", t="e")]]
        rep = self.book(rows)
        self.assertEqual(rep["oracle"]["cached"], {"number": 1, "string": 1, "boolean": 1, "error": 2})
        self.assertEqual(rep["oracle"]["cached_errors"], {"#N/A": 1, "#DIV/0!": 1})
        self.assertIn("cached_errors", tell_ids(rep))

    def test_cached_error_cells_are_listed_per_sheet_and_type_capped_at_twenty(self):
        rows = [[F("NA()", "#N/A", t="e"), F("1/0", "#DIV/0!", t="e"), F("1", 1)]] + [[F("NA()", "#N/A", t="e")] for _ in range(24)]
        o = self.book(rows)["oracle"]
        self.assertEqual(o["cached_error_cells"]["#DIV/0!"], {"S": ["B1"]})
        self.assertEqual(o["cached_error_cells"]["#N/A"]["S"], [f"A{i}" for i in range(1, 21)])
        self.assertEqual(o["cached_error_cell_totals"], {"#N/A": {"S": 25}, "#DIV/0!": {"S": 1}})

    def test_masked_sheets_give_error_counts_without_refs(self):
        hid = Sheet("H", [[F("NA()", "#N/A", t="e")]], state="hidden")
        vis = Sheet("V", [[F("NA()", "#N/A", t="e")]])
        o = cw.analyze(make_book(self.tmp, [vis, hid]))["oracle"]
        self.assertEqual(o["cached_error_cells"], {"#N/A": {"V": ["A1"]}})
        self.assertEqual(o["cached_error_cell_totals"], {"#N/A": {"V": 1, "H": 1}})

    def test_the_text_report_summarises_error_cells_and_points_at_json(self):
        txt = cw.render_text(self.book([[F("NA()", "#N/A", t="e"), F("1/0", "#DIV/0!", t="e")]]))
        self.assertIn("cached error cells: #N/A 1, #DIV/0! 1: refs per sheet are in --json as `cached_error_cells`", txt)
        self.assertNotIn("A1", txt.split("cached error cells")[1].split("\n")[0])

    def test_autofilter_slicers_and_timelines_are_ui_state_caveats(self):
        sh = Sheet("S", [["h"], [1]], after='<autoFilter ref="A1:A2"/>')
        p = make_book(self.tmp, [sh], parts={"xl/slicers/slicer1.xml": "<slicers/>",
                                             "xl/timelines/timeline1.xml": "<timelines/>"})
        rep = cw.analyze(p)
        self.assertTrue({"autofilter", "slicers", "timelines"} <= tell_ids(rep))

    def test_subtotal_over_an_autofiltered_sheet_is_flagged(self):
        sh = Sheet("S", [["h"], [1], [2], [F("SUBTOTAL(109,A2:A3)", 3)]], after='<autoFilter ref="A1:A3"/>')
        rep = cw.analyze(make_book(self.tmp, [sh]))
        self.assertIn("subtotal_ui_state", tell_ids(rep))
        self.assertEqual(region(rep, "S", "A4")["route"], "C")

    def test_pivot_refreshed_date_is_a_caveat(self):
        rep = cw.analyze(make_book(self.tmp, [Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])],
                                   parts=pivot_parts()))
        self.assertIn("pivot_snapshot", tell_ids(rep))


# --------------------------------------------------------------------------
# Formulas: shared expansion, R1C1, regions, split
# --------------------------------------------------------------------------

class SharedAndR1C1(Tmp):
    def test_expand_shared_shifts_relative_parts_only(self):
        self.assertEqual(cw.expand_shared("B2*$C2+D$1+$E$1+Sheet2!F2", 2, 1),
                         "C4*$C4+E$1+$E$1+Sheet2!G4")
        self.assertEqual(cw.expand_shared("SUM(A1:B2)", 1, 0), "SUM(A2:B3)")
        self.assertEqual(cw.expand_shared('"A1"&A1', 1, 0), '"A1"&A2')

    def test_shared_formula_is_one_region_of_n_cells(self):
        rows = [[1, 2, F("A1+B1", 3, fa={"t": "shared", "ref": "C1:C4", "si": "0"})],
                [2, 3, F("", 5, fa={"t": "shared", "si": "0"})],
                [3, 4, F("", 7, fa={"t": "shared", "si": "0"})],
                [4, 5, F("", 9, fa={"t": "shared", "si": "0"})]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(len(rep["regions"]), 1)
        reg = rep["regions"][0]
        self.assertEqual((reg["ref"], reg["cells"], reg["r1c1"]), ("C1:C4", 4, "RC[-2]+RC[-1]"))
        self.assertEqual(reg["split"], "row_local")
        self.assertEqual(rep["sheets"][0]["formula_cells"], 4)

    def test_a_shared_formula_child_before_its_master_still_resolves(self):
        rows = [[1, F("", 2, fa={"t": "shared", "si": "3"})],
                [2, F("A2*2", 4, fa={"t": "shared", "ref": "B1:B2", "si": "3"})]]
        # master is at B2 here; the child at B1 shifts up
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(len(rep["regions"]), 1)

    def test_copied_down_normal_formulas_are_one_region(self):
        rows = [[2, 3, F("A1*B1", 6)], [3, 4, F("A2*B2", 12)], [4, 5, F("A3*B3", 20)]]
        reg = region(cw.analyze(one_sheet(self.tmp, rows)), "S", "C1:C3")
        self.assertEqual(reg["cells"], 3)
        self.assertEqual(reg["example"], "A1*B1")

    def test_three_way_split(self):
        rows = [[1, 2, F("A1+B1", 3), F("SUM($A$1:$A$3)", 6), F("$A$1*2", 2), F("7", 7), F("A1*$F$9", 0)],
                [2, 3], [3, 4]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(region(rep, "S", "C1")["split"], "single_cell")
        self.assertEqual(region(rep, "S", "D1")["split"], "range_aggregate")
        self.assertEqual(region(rep, "S", "E1")["split"], "absolute")
        self.assertEqual(region(rep, "S", "F1")["split"], "constant")
        mixed = region(rep, "S", "G1")
        self.assertEqual(mixed["split"], "single_cell")
        self.assertEqual(mixed["absolute_refs"], ["S!$F$9"])

    def test_prefixes_are_stripped_from_function_names(self):
        rep, reg = self.run_formula("_xlfn.IFS(A1>1,1,TRUE,2)+_xlfn.STDEV.S(A1:A3)")
        self.assertEqual(set(reg["functions"]), {"IFS", "STDEV.S"})
        self.assertNotIn("unresolved_function", rep["code_attached"])
        rep, reg = self.run_formula("_xlfn._xlws.SORT(A1:A3)")
        self.assertIn("SORT", reg["functions"])

    def test_structured_references(self):
        this_row = "tbl[[#This Row],[Qty]]*tbl[[#This Row],[Price]]"
        table = table_xml("tbl", "A1:C5", ["Qty", "Price", "Amount"], totals=1, calc={"Amount": this_row})
        sh = Sheet("T", [["Qty", "Price", "Amount"], [1, 2, F(this_row, 2)], [2, 3, F(this_row, 6)], [3, 4, F(this_row, 12)],
                         [None, None, F("SUBTOTAL(109,tbl[Amount])", 20)], [None],
                         [None, None, F("SUM(tbl[Amount])", 20)], [None, None, F("tbl[[#Totals],[Amount]]", 20)]],
                   rels=[("table", "../tables/table1.xml", "rId1")], after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table}))
        rows_reg = region(rep, "T", "C2:C4")
        self.assertEqual(rows_reg["split"], "row_local")
        self.assertIn("structured_ref", flag_ids(rows_reg))
        self.assertEqual(region(rep, "T", "C7")["split"], "range_aggregate")
        self.assertEqual(region(rep, "T", "C8")["split"], "absolute")
        self.assertEqual(region(rep, "T", "C8")["absolute_refs"], ["T!$C$5"])
        self.assertEqual(rep["graph"]["incomplete_cells"], 0)

    def test_a_structured_reference_to_a_missing_table_is_an_incomplete_graph(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("SUM(nosuch[Amount])", 0)]]))
        self.assertEqual(rep["graph"]["incomplete_cells"], 1)

    def test_3d_references_resolve_across_the_sheet_span(self):
        p = make_book(self.tmp, [Sheet("Jan", [[1]]), Sheet("Feb", [[2]]), Sheet("Mar", [[3]]),
                                 Sheet("Sum", [[F("SUM(Jan:Mar!A1)", 6)]])])
        rep = cw.analyze(p)
        reg = region(rep, "Sum", "A1")
        self.assertIn("3d_ref", flag_ids(reg))
        edges = {(e["from"], e["to"]) for e in rep["graph"]["sheet_edges"]}
        self.assertTrue({("Sum", "Jan"), ("Sum", "Feb"), ("Sum", "Mar")} <= edges)


class ExcludedRanges(Tmp):
    def test_array_spill_datatable_and_pivot_ranges_are_excluded_from_lifting(self):
        rows = [[1, 2, F("A1:A3*2", 2, fa={"t": "array", "ref": "C1:C3"}), None, F("SORT(A1:A3)", 1, fa={"t": "array", "ref": "E1:E3"}, ca={"cm": "1"})],
                [2, 3, 4, None, 2], [3, 4, 6, None, 3],
                [None] * 6,
                [None, None, F("TABLE(B7,)", 0, fa={"t": "dataTable", "ref": "C8:C10", "dt2D": "0", "dtr": "0", "r1": "B7"})]]
        sh = Sheet("S", rows, rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [sh], parts=pivot_parts()))
        ex = {(e["kind"], e["ref"]) for e in rep["excluded_ranges"]}
        self.assertEqual(ex, {("array", "C1:C3"), ("spill", "E1:E3"), ("data_table", "C8:C10"), ("pivot", "A3:B8")})
        kinds = {r["ref"]: r["kind"] for r in rep["regions"]}
        self.assertEqual(kinds, {"C1:C3": "array", "E1:E3": "spill", "C8:C10": "datatable"})

    def test_excluded_ranges_overlapping_a_table_drop_it_from_the_lift_list(self):
        rows = [["k", "v"], ["a", 1], ["b", 2]]
        sh = Sheet("S", rows + [[None, None], [None, F("SORT(B2:B3)", 1, fa={"t": "array", "ref": "B6:B7"}, ca={"cm": "1"})]],
                   rels=[("table", "../tables/table1.xml", "rId1")], after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        p = make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table_xml("t1", "A1:B7", ["k", "v"])})
        src = [s for s in cw.analyze(p)["sources"] if s["name"] == "t1"][0]
        self.assertTrue(src["excluded_overlaps"])
        self.assertEqual(src["lifted"], ["k"])
        self.assertEqual(src["not_lifted"], ["v"])
        self.assertNotIn('"v"', src["stanza"].split("FROM")[0])


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------

class Routes(Tmp):
    def check(self, formula, route, flags=(), absent=(), **kw):
        rep, reg = self.run_formula(formula, **kw)
        self.assertEqual(reg["route"], route, (formula, reg["route"], reg["reasons"]))
        for f in flags:
            self.assertIn(f, flag_ids(reg), (formula, flag_ids(reg)))
        for f in absent:
            self.assertNotIn(f, flag_ids(reg), (formula, flag_ids(reg)))
        return rep, reg

    def test_vlookup_approximate_is_a_range_join_not_an_equality_join(self):
        self.check("VLOOKUP(A1,A1:B4,2)", "C", ["approx_match"])
        self.check("VLOOKUP(A1,A1:B4,2,TRUE)", "C", ["approx_match"])
        self.check("VLOOKUP(A1,A1:B4,2,1)", "C", ["approx_match"])
        self.check("HLOOKUP(A1,A1:B4,2)", "C", ["approx_match"])
        self.check("LOOKUP(A1,A1:A4,B1:B4)", "C", ["approx_match"])

    def test_vlookup_exact_translates_but_is_still_case_insensitive(self):
        self.check("VLOOKUP(C1,C1:D4,2,FALSE)", "T", ["ci_match"], ["approx_match"])
        self.check("VLOOKUP(C1,C1:D4,2,0)", "T", ["ci_match"], ["approx_match"])

    def test_ci_match_is_flagged_only_when_the_key_can_be_text(self):
        for f in ("VLOOKUP(2,A1:B4,2,FALSE)", "VLOOKUP(A1,A1:B4,2,FALSE)", "HLOOKUP(A1,A1:B4,2,FALSE)", "MATCH(A1,A1:A4,0)",
                  "_xlfn.XLOOKUP(A1,A1:A4,B1:B4)", "SUMIF(A1:A4,2,B1:B4)", "COUNTIFS(A1:A4,A1)", "SUMIFS(B1:B4,A1:A4,\">=\"&D1)"):
            self.check(f, "T", [], ["ci_match"])
        for f in ("VLOOKUP(C1,C1:D4,2,FALSE)", "VLOOKUP(Z9,C1:D4,2,FALSE)", "MATCH(\"x\",C1:C4,0)", "_xlfn.XLOOKUP(Z9,C1:C4,B1:B4)",
                  "SUMIF(C1:C4,\"x\",B1:B4)", "VLOOKUP(Z9,Q1:R4,2,FALSE)"):
            self.check(f, "T", ["ci_match"])

    def test_ci_match_stays_flagged_when_the_key_range_has_a_formula_or_is_unbounded(self):
        self.check("VLOOKUP(Z9,A1:B5,2,FALSE)", "T", ["ci_match"], extra_rows=[[F("A1+1", 2)]])
        self.check("MATCH(Z9,A:A,0)", "T", ["ci_match"])

    def test_vlookup_match_mode_from_a_cell_is_not_assumed_exact(self):
        _, reg = self.check("VLOOKUP(A1,A1:B4,2,B1)", "C", ["approx_match"])
        self.assertTrue(any("match mode" in r for r in reg["reasons"]))

    def test_match_with_the_third_argument_omitted_is_approximate(self):
        self.check("MATCH(A1,A1:A4)", "C", ["approx_match"])
        self.check("MATCH(A1,A1:A4,1)", "C", ["approx_match"])
        self.check("MATCH(A1,A1:A4,-1)", "C", ["approx_match"])
        self.check("MATCH(C1,C1:C4,0)", "T", ["ci_match"], ["approx_match"])

    def test_xlookup_defaults_to_exact(self):
        self.check("_xlfn.XLOOKUP(A1,A1:A4,B1:B4)", "T", [], ["approx_match"])
        self.check("_xlfn.XLOOKUP(A1,A1:A4,B1:B4,0,-1)", "C", ["approx_match"])
        self.check("INDEX(B1:B4,MATCH(A1,A1:A4,0))", "T")

    def test_sumifs_criteria_strings_each_get_their_own_flag(self):
        self.check('SUMIFS(B1:B4,C1:C4,">="&D1)', "T", ["criteria_comparison", "ci_match"])
        self.check('SUMIFS(B1:B4,C1:C4,"*st")', "T", ["criteria_wildcard"])
        self.check('SUMIFS(B1:B4,C1:C4,"")', "T", ["criteria_blank"])
        self.check('SUMIFS(B1:B4,C1:C4,"<>")', "T", ["criteria_nonblank"])
        self.check('COUNTIF(C1:C4,"a?c")', "T", ["criteria_wildcard"])
        self.check('SUMIF(C1:C4,D1,B1:B4)', "T", ["criteria_cell"])

    def test_a_numeric_criterion_also_matches_text_that_looks_numeric(self):
        self.check("COUNTIF(A1:A4,1)", "T", ["criteria_numeric"])

    def test_average_count_and_counta_semantics(self):
        self.check("AVERAGE(A1:A4)", "T", ["avg_skips_blank_text"])
        self.check("COUNT(A1:A4)", "T", ["count_numbers_only"])
        self.check("COUNTA(A1:A4)", "T", ["counta_nonblank"])
        self.check("AVERAGEIFS(B1:B4,A1:A4,1)", "T", ["avg_skips_blank_text"])

    def test_sumproduct_has_two_shapes(self):
        self.check("SUMPRODUCT((A1:A4>1)*(B1:B4))", "T", ["sumproduct_mask"], ["sumproduct_product"])
        self.check("SUMPRODUCT(A1:A4,B1:B4)", "T", ["sumproduct_product"], ["sumproduct_mask"])

    def test_full_column_refs_double_count_a_total_beneath_the_data(self):
        d = Sheet("Data", [["amt"], [1], [2], [3], [F("SUM(A2:A4)", 6)]])
        r = Sheet("Report", [[F("SUM(Data!A:A)", 12)]])
        rep = cw.analyze(make_book(self.tmp, [d, r]))
        reg = region(rep, "Report", "A1")
        self.assertIn("full_column", flag_ids(reg))
        self.assertIn("full_column_total", flag_ids(reg))

    def test_full_column_without_a_total_is_just_full_column(self):
        rep, reg = self.check("SUM(A:A)", "T", ["full_column"], ["full_column_total"])

    def test_hardcoded_constants_inside_formulas_are_flagged(self):
        _, reg = self.check("A1*1.08", "T", ["hardcoded_constant"])
        self.assertEqual([f for f in reg["flags"] if f["id"] == "hardcoded_constant"][0]["detail"], "1.08")
        self.check("A1*2+0", "T", ["hardcoded_constant"])
        self.check("ROUND(A1,2)", "T", [], ["hardcoded_constant"])
        self.check("IF(A1>0,A1,1)", "T", [], ["hardcoded_constant"])

    def test_a_literal_plug_inside_a_copied_down_region_is_flagged(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, 99], [4, F("A4*2", 8)], [5, F("A5*2", 10)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        reg = [r for r in rep["regions"] if r["cells"] == 4][0]
        self.assertIn("plug", flag_ids(reg))
        self.assertEqual(reg["plugs"], ["B3"])

    def test_volatile_now_today_route_to_a_given(self):
        for fn in ("TODAY()", "NOW()"):
            _, reg = self.check(f"{fn}-A1", "C", ["volatile"])
            self.assertTrue(any("dcterms:modified" in r for r in reg["reasons"]))

    def test_rand_stays_in_excel_and_wins_over_a_translatable_wrapper(self):
        self.check("NORM.INV(RAND(),0,1)", "X", ["volatile"])
        self.check("RANDBETWEEN(1,6)", "X", ["volatile"])
        self.check("_xlfn.RANDARRAY(3)", "X", ["volatile"])

    def test_offset_indirect_and_choose_as_ref_ask_the_user(self):
        self.check("SUM(OFFSET(A1,0,0,2,1))", "NR", ["opaque_dependency"])
        self.check('INDIRECT("A"&1)', "NR", ["opaque_dependency"])
        self.check("SUM(CHOOSE(1,A1:A2,B1:B2))", "NR", ["opaque_dependency"])
        self.check("CHOOSE(2,A1,B1)", "T", [], ["opaque_dependency"])

    def test_lambda_map_scan_ask_the_user_and_let_translates(self):
        self.check("_xlfn.MAP(A1:A3,_xlfn.LAMBDA(_xlpm.x,_xlpm.x*2))", "NR")
        self.check("_xlfn.SCAN(0,A1:A3,_xlfn.LAMBDA(_xlpm.a,_xlpm.b,_xlpm.a+_xlpm.b))", "NR")
        rep, reg = self.check("_xlfn.LET(_xlpm.x,A1*2,_xlpm.x+1)", "T")
        self.assertEqual(rep["graph"]["incomplete_cells"], 0)

    def test_dynamic_array_helpers_are_c_even_in_one_cell(self):
        for f in ("_xlfn.UNIQUE(A1:A4)", "_xlfn.SEQUENCE(3)", "_xlfn.SORT(A1:A4)", "_xlfn.FILTER(A1:A4,B1:B4>1)"):
            self.check(f, "C", [], ["dynamic_array_scalar"])

    def test_a_scalar_aggregate_over_a_dynamic_array_function_is_c(self):
        for f, detail in (("COUNTA(_xlfn.UNIQUE(A1:A4))", "UNIQUE inside COUNTA"), ("ROWS(_xlfn.UNIQUE(A1:A4))", "UNIQUE inside ROWS"),
                          ("SUM(_xlfn.UNIQUE(A1:A4))", "UNIQUE inside SUM"), ("COUNT(_xlfn.FILTER(A1:A4,B1:B4>1))", "FILTER inside COUNT"),
                          ("SUM(_xlfn._xlws.SORT(A1:A4))", "SORT inside SUM")):
            with self.subTest(f=f):
                rep, reg = self.check(f, "C", ["dynamic_array_scalar"])
                self.assertEqual([x["detail"] for x in reg["flags"] if x["id"] == "dynamic_array_scalar"], [detail])
                self.assertTrue(any("dynamic-array" in r for r in reg["reasons"]), reg["reasons"])

    def test_a_bare_unique_spill_stays_c_and_is_not_a_scalar(self):
        sh = Sheet("S", [[1, 2]])
        sh.data = ('<row r="1"><c r="A1"><v>1</v></c><c r="B1" cm="1"><f t="array" ref="B1:B3">_xlfn.UNIQUE(A1:A4)</f><v>2</v></c></row>')
        rep = cw.analyze(make_book(self.tmp, [sh]))
        self.assertEqual([(e["kind"], e["ref"]) for e in rep["excluded_ranges"]], [("spill", "B1:B3")])

    def test_a_number_concatenated_into_text_is_flagged_with_its_operand_shape(self):
        for f, detail in (('ROUND(AVERAGE(A1:A4),1)&"%"', "ROUND(...)&text"), ('A1&" units"', "cell&text"),
                          ('"n="&SUM(A1:A4)', "text&SUM(...)"), ('2&"x"', "number&text"), ('A1*100&"%"', "arithmetic&text")):
            with self.subTest(f=f):
                rep, reg = self.check(f, "T", ["number_to_text"])
                self.assertEqual([x["detail"] for x in reg["flags"] if x["id"] == "number_to_text"], [detail])

    def test_text_wrapped_or_untyped_concatenation_is_not_number_to_text(self):
        for f in ('TEXT(A1,"0.0")&"%"', 'FIXED(A1,1)&"%"', 'DOLLAR(A1)&"x"', '"a"&"b"', 'C1&" units"', 'C1&D1', 'A1&B1'):
            with self.subTest(f=f):
                self.check(f, "T", [], ["number_to_text"])

    def test_proper_is_t_with_a_semantics_flag_and_its_siblings_are_not_flagged(self):
        self.check("PROPER(C1)", "T", ["proper_semantics"])
        for f in ("UPPER(C1)", "LOWER(C1)", "TRIM(C1)", "SUBSTITUTE(C1,\"x\",\"y\")"):
            with self.subTest(f=f):
                self.check(f, "T", [], ["proper_semantics"])

    def test_financial_and_distribution_functions_are_at_cost(self):
        self.check("PMT(0.01,12,A1)", "C")
        self.check("NORM.INV(0.5,0,1)", "C")

    def test_unknown_function_asks_which_addin_and_its_cache_is_not_an_oracle(self):
        rep, reg = self.check("FOO(A1)", "NR")
        self.assertTrue(any("FOO" in r and "add-in" in r for r in reg["reasons"]))
        self.assertIn("xll_udf", rep["code_attached"])

    def test_unknown_function_with_a_vba_project_names_the_udf_possibility(self):
        rep, reg = self.check("FOO(A1)", "NR", parts={"xl/vbaProject.bin": b"\xD0\xCF\x11\xE0"})
        self.assertTrue(any("VBA" in r for r in reg["reasons"]))
        self.assertEqual(rep["code_attached"]["vba"]["count"], 1)

    def test_vendor_feeds_stay_in_excel_and_name_the_system(self):
        for fn, system in (("BDP(A1,\"PX_LAST\")", "Bloomberg"), ("FDS(A1,\"P_PRICE\")", "FactSet"),
                           ("CIQ(A1,\"IQ_CLOSEPRICE\")", "Capital IQ"), ("DBRW(A1)", "TM1"),
                           ("HSGETVALUE(1,A1)", "Smart View"), ("SAPGETDATA(A1)", "SAP"),
                           ("EPMRETRIEVEDATA(A1)", "BPC"), ("XFGETCELL(A1)", "OneStream"),
                           ("ESSCELL(A1)", "Essbase"), ("_xlfn.STOCKHISTORY(A1,A2)", "STOCKHISTORY")):
            with self.subTest(fn=fn):
                rep, reg = self.check(fn, "X")
                self.assertTrue(any(system in r for r in reg["reasons"]), reg["reasons"])
                self.assertIn("vendor_feed", rep["code_attached"])

    def test_webservice_and_filterxml_stay_in_excel_and_are_security_flagged(self):
        rep, reg = self.check('_xlfn.WEBSERVICE("http://x")', "X")
        self.assertIn("web_fetch_formula", sec_ids(rep))
        self.check('_xlfn.FILTERXML("<a/>","//a")', "X")

    def test_cube_functions_route_with_the_data_model(self):
        rep, reg = self.check('CUBEVALUE("ThisWorkbookDataModel")', "NR")
        self.assertIn("cube", rep["code_attached"])

    def test_linked_data_types_and_image_are_no_recipe(self):
        self.check("_xlfn.FIELDVALUE(A1,\"Price\")", "NR")
        self.check('_xlfn.IMAGE("http://x/y.png")', "NR")

    def test_addin_tokens(self):
        rep, reg = self.check('_xll.MYFUNC(A1)', "NR")
        self.assertIn("xll_udf", rep["code_attached"])
        self.assertTrue(rep["code_attached"]["xll_udf"]["unconfirmed"])
        rep, reg = self.check('_xludf.OLDFN(A1)', "NR")
        self.assertIn("unresolved_function", rep["code_attached"])
        self.assertTrue(rep["code_attached"]["unresolved_function"]["unconfirmed"])
        rep, reg = self.check('[1]!Helper(A1)', "NR")
        self.assertIn("addin_link", rep["code_attached"])
        self.assertTrue(rep["code_attached"]["addin_link"]["unconfirmed"])

    def test_cached_name_error_counts_as_unresolved(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("FOO(1)", "#NAME?", t="e")]]))
        self.assertEqual(rep["code_attached"]["unresolved_function"]["count"], 1)

    def test_python_in_excel_extracts_the_code_and_is_at_cost(self):
        rep, reg = self.check('_xlfn._xlws.PY(0,1,"df = xl(""A1:B4"")\ndf.sum()")', "C")
        py = rep["code_attached"]["python_in_excel"]
        self.assertEqual(py["count"], 1)
        self.assertTrue(py["unconfirmed"])
        self.assertEqual(py["code"], ['df = xl("A1:B4")\ndf.sum()'])
        self.assertIn("python_in_excel", sec_ids(rep))

    PY_PART = ('<pythonScripts><pythonScript><code>df = xl("A1:B4")\ndf.sum()</code></pythonScript>'
               '<pythonScript><code>total = 2</code></pythonScript><pythonScript><other/></pythonScript></pythonScripts>')

    def test_python_in_excel_code_is_read_from_the_script_part_when_no_cell_holds_it(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1]], parts={"xl/pythonScripts.xml": self.PY_PART}))
        py = rep["code_attached"]["python_in_excel"]
        self.assertEqual(py["code"], ['df = xl("A1:B4")\ndf.sum()', "total = 2"])
        self.assertEqual(py["count"], 2)
        self.assertIn("python_in_excel", sec_ids(rep))

    def test_python_script_part_code_joins_the_formula_code_and_is_capped(self):
        many = "<pythonScripts>" + "".join(f"<pythonScript><code>x{i} = 1</code></pythonScript>" for i in range(cw.MAX_PY_SCRIPTS + 5)) + "</pythonScripts>"
        rep = cw.analyze(one_sheet(self.tmp, [[F('_xlfn._xlws.PY(0,1,"y = 1")', 1)]], parts={"xl/pythonScripts.xml": many}))
        code = rep["code_attached"]["python_in_excel"]["code"]
        self.assertEqual(code[0], "y = 1")
        self.assertEqual(len(code), cw.MAX_PY_SCRIPTS)
        long_part = "<pythonScripts><pythonScript><code>" + "a" * (cw.MAX_PY_CODE_CHARS + 50) + "</code></pythonScript></pythonScripts>"
        rep = cw.analyze(one_sheet(self.tmp, [[1]], parts={"xl/python.xml": long_part}))
        self.assertLess(len(rep["code_attached"]["python_in_excel"]["code"][0]), cw.MAX_PY_CODE_CHARS + 50)

    def test_named_lambda_call_is_at_cost_and_inlined(self):
        names = '<definedNames><definedName name="Sq">_xlfn.LAMBDA(_xlpm.x,_xlpm.x*_xlpm.x)</definedName></definedNames>'
        rep = cw.analyze(one_sheet(self.tmp, [[2, F("Sq(A1)", 4)]], names=names))
        self.assertEqual(region(rep, "S", "B1")["route"], "C")

    def test_route_counts_are_per_route(self):
        rep, _ = self.run_formula("A1+1")
        self.assertEqual(rep["routes"]["T"]["regions"], 1)
        self.assertEqual(set(rep["routes"]), {"T", "C", "X", "NR"})

    def test_function_inventory_counts_cells_and_regions(self):
        rows = [[1, F("SUM($A$1:$A$2)", 3)], [2, F("SUM($A$1:$A$2)", 3)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(rep["functions"]["SUM"], {"regions": 1, "cells": 2, "route": "T", "routes": {"T": 1}})

    def test_array_formulas_are_never_regioned_silently(self):
        rows = [[1, F("A1:A2*2", 2, fa={"t": "array", "ref": "B1:B2"})], [2, 4]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        reg = region(rep, "S", "B1:B2")
        self.assertEqual(reg["kind"], "array")
        self.assertIn(reg["route"], ("C", "X", "NR"))


# --------------------------------------------------------------------------
# Graph
# --------------------------------------------------------------------------

class EmptyArgument(Tmp):
    def detail(self, formula):
        _, reg = self.run_formula(formula)
        return [f["detail"] for f in reg["flags"] if f["id"] == "empty_argument"]

    def test_a_trailing_or_doubled_comma_in_an_aggregate_is_flagged_with_function_and_position(self):
        self.assertEqual(self.detail("AVERAGE(A1:A4,)"), ["AVERAGE arg 2"])
        self.assertEqual(self.detail("SUM(A1,,B1)"), ["SUM arg 2"])
        self.assertEqual(self.detail("COUNTA(A1:A4,B1,)"), ["COUNTA arg 3"])

    def test_if_and_ifs_value_arguments_are_flagged(self):
        self.assertEqual(self.detail("IF(A1,,3)"), ["IF arg 2"])
        self.assertEqual(self.detail("IF(A1,3,)"), ["IF arg 3"])
        self.assertEqual(self.detail("_xlfn.IFS(A1>1,,A1>0,2)"), ["IFS arg 2"])

    def test_complete_calls_and_other_functions_are_not_flagged(self):
        for f in ("AVERAGE(A1:A4)", "SUM()", "IF(A1,2,3)", "IF(A1,2)", "VLOOKUP(A1,A1:B4,2,)", "_xlfn.IFS(A1>1,2,A1>0,3)"):
            self.assertEqual(self.detail(f), [], f)


class CriteriaFifteenSignificantDigits(Tmp):
    def detail(self, formula):
        _, reg = self.run_formula(formula)
        return [f["detail"] for f in reg["flags"] if f["id"] == "criteria_comparison"]

    def test_a_comparison_criterion_built_from_a_cell_or_expression_carries_15sig(self):
        self.assertEqual(self.detail('SUMIFS(B1:B4,C1:C4,">"&D1)'), ["SUMIFS 15sig"])
        self.assertEqual(self.detail('COUNTIF(A1:A4,">="&MAX(B1:B4))'), ["COUNTIF 15sig"])
        self.assertEqual(self.detail('AVERAGEIF(A1:A4,"<"&D1*2,B1:B4)'), ["AVERAGEIF 15sig"])

    def test_a_literal_criterion_has_no_15sig(self):
        self.assertEqual(self.detail('COUNTIF(A1:A4,">5")'), ["COUNTIF"])
        self.assertEqual(self.detail('COUNTIF(A1:A4,">"&5)'), ["COUNTIF"])


class Graph(Tmp):
    def test_a_real_cycle_is_found_and_iterate_setting_is_reported_separately(self):
        rows = [[F("B1+1", 0), F("A1+1", 0)]]
        rep = cw.analyze(one_sheet(self.tmp, rows, calc='<calcPr calcId="1" iterate="1"/>'))
        self.assertEqual(len(rep["graph"]["cycles"]), 1)
        self.assertEqual(rep["graph"]["cycles"][0]["cells"], 2)
        self.assertTrue(rep["graph"]["iterate"])

    def test_iterate_without_a_cycle_is_a_stale_setting(self):
        rows = [[1, F("A1+1", 2)]]
        rep = cw.analyze(one_sheet(self.tmp, rows, calc='<calcPr calcId="1" iterate="1"/>'))
        self.assertEqual(rep["graph"]["cycles"], [])
        self.assertTrue(rep["graph"]["iterate"])
        self.assertIn("no cycle", rep["graph"]["iterate_note"])

    def test_a_total_inside_its_own_range_is_self_inclusive_not_a_cycle(self):
        rows = [[1], [2], [F("SUM(A1:A3)", 3)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(rep["graph"]["cycles"], [])
        self.assertEqual([(x["cells"], len(x["regions"])) for x in rep["graph"]["self_inclusive_range"]], [(1, 1)])
        self.assertIn("self-inclusive range: 1 cell(s)", cw.render_text(rep))
        rows = [[1], [2], [F("COUNTA(A1:A3)", 3)]]
        self.assertEqual(len(cw.analyze(one_sheet(self.tmp, rows))["graph"]["self_inclusive_range"]), 1)

    def test_a_real_self_reference_or_a_loop_through_an_aggregate_is_still_a_cycle(self):
        for rows in ([[F("A1+1", 0)]], [[1], [F("SUM(A1:A2)+A2", 3)]], [[F("B1+1", 0), F("SUM(A1:B1)", 0)]]):
            g = cw.analyze(one_sheet(self.tmp, rows))["graph"]
            self.assertEqual(len(g["cycles"]), 1, rows)
            self.assertEqual(g["self_inclusive_range"], [], rows)

    def test_iterate_with_only_a_self_inclusive_range_is_not_called_stale(self):
        rows = [[1], [F("SUM(A1:A2)", 1)]]
        g = cw.analyze(one_sheet(self.tmp, rows, calc='<calcPr calcId="1" iterate="1"/>'))["graph"]
        self.assertIn("self-inclusive", g["iterate_note"])
        self.assertNotIn("stale", g["iterate_note"])

    def test_a_running_balance_is_not_a_cycle(self):
        rows = [[1, F("A1", 1)], [2, F("B1+A2", 3)], [3, F("B2+A3", 6)]]
        self.assertEqual(cw.analyze(one_sheet(self.tmp, rows))["graph"]["cycles"], [])

    def schedule(self, col_b, col_c=None, rows=30):
        """A 30-row schedule: B2 is typed, B3.. and C3.. come from the row builders."""
        out = [[None, None, None]]
        for r in range(2, rows + 1):
            out.append([None, 1000 if r == 2 else F(col_b(r), 1), (F(col_c(r), 1) if col_c and r > 2 else None)])
        return cw.analyze(one_sheet(self.tmp, out))["graph"]

    def test_a_prior_balance_recurrence_and_an_expanding_range_are_not_cycles(self):
        for label, g in (
            ("previous row", self.schedule(lambda r: f"B{r - 1}*1.1")),
            ("expanding range beside", self.schedule(lambda r: f"B{r - 1}*1.1", lambda r: f"SUM(B$2:B{r - 1})")),
            ("index-anchored range beside", self.schedule(lambda r: f"B{r - 1}*1.1", lambda r: f"SUM(INDEX(B$2:B$30,1,1):B{r - 1})")),
            ("expanding range in its own column", self.schedule(lambda r: f"SUM(B$2:B{r - 1})")),
            ("index-anchored range in its own column", self.schedule(lambda r: f"SUM(INDEX(B$2:B$30,1,1):B{r - 1})")),
        ):
            self.assertEqual(g["cycles"], [], label)
            self.assertEqual(g["self_inclusive_range"], [], label)

    def test_an_index_anchor_with_a_dynamic_position_in_its_own_column_is_still_a_cycle(self):
        g = self.schedule(lambda r: f"SUM(INDEX(B$2:B$30,A{r},1):B{r - 1})")
        self.assertEqual(len(g["cycles"]), 1)

    def test_a_range_that_includes_its_own_cell_is_still_reported(self):
        g = self.schedule(lambda r: f"SUM(B$2:B{r})")
        self.assertEqual(g["cycles"], [])
        self.assertEqual(len(g["self_inclusive_range"]), 1)
        g = cw.analyze(one_sheet(self.tmp, [[F("B1*0.5+1", 0), F("A1*0.5", 0)]]))["graph"]
        self.assertEqual(len(g["cycles"]), 1)

    def test_regions_in_a_reported_cycle_route_c_as_circular_for_any_iterate_setting(self):
        for calc in (CALC, '<calcPr calcId="1" iterate="1"/>'):
            rep = cw.analyze(one_sheet(self.tmp, [[F("B1+1", 0), F("A1*0.5", 0)]], calc=calc))
            for ref in ("A1", "B1"):
                reg = region(rep, "S", ref)
                self.assertEqual(reg["route"], "C", calc)
                self.assertTrue(any(r.startswith("circular") for r in reg["reasons"]), calc)

    def test_a_cycle_region_keeps_its_flags(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("B1+1", 0), F('SUMIF(C1:C3,">"&A1)', 0)], [None, None, 1]]))
        self.assertTrue(region(rep, "S", "B1")["flags"])

    def test_a_self_inclusive_range_iterates_under_iterate_and_routes_c_only_then(self):
        rows = [[1], [F("SUM(A1:A2)", 1)]]
        off = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(off["graph"]["cycles"], [])
        self.assertTrue(off["graph"]["self_inclusive_range"][0]["iterates_when_iterate_on"])
        self.assertEqual(region(off, "S", "A2")["route"], "T")
        on = cw.analyze(one_sheet(self.tmp, rows, calc='<calcPr calcId="1" iterate="1"/>'))
        self.assertEqual(on["graph"]["cycles"], [])
        self.assertIn("treat as SC6 loops", on["graph"]["iterate_note"])
        reg = region(on, "S", "A2")
        self.assertEqual(reg["route"], "C")
        self.assertTrue(any(r.startswith("circular") for r in reg["reasons"]))

    def test_incomplete_graph_counts_the_cells_it_cannot_follow(self):
        rows = [[1, F("OFFSET(A1,0,0)", 1)], [2, F("OFFSET(A2,0,0)", 2)], [3, F("A3*2", 6)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(rep["graph"]["incomplete_cells"], 2)
        self.assertIn("dependency graph incomplete: 2 cells", cw.render_text(rep))

    def test_cross_sheet_edges_and_region_dependencies(self):
        d = Sheet("Data", [[1, F("A1*2", 2)], [2, F("A2*2", 4)]])
        r = Sheet("Report", [[F("SUM(Data!B1:B2)", 6)]])
        rep = cw.analyze(make_book(self.tmp, [d, r]))
        self.assertEqual([(e["from"], e["to"]) for e in rep["graph"]["sheet_edges"]], [("Report", "Data")])
        rr = region(rep, "Report", "A1")
        dd = region(rep, "Data", "B1:B2")
        self.assertEqual(rr["depends_on"], [dd["id"]])
        self.assertIn({"sheet": "Data", "ref": "B1:B2", "via": "SUM", "kind": "formula"}, rr["reads"])

    def test_reads_of_constants_are_marked_data(self):
        d = Sheet("Data", [[1], [2]])
        r = Sheet("Report", [[F("SUM(Data!A1:A2)", 3)]])
        rr = region(cw.analyze(make_book(self.tmp, [d, r])), "Report", "A1")
        self.assertEqual(rr["reads"][0]["kind"], "data")

    def test_defined_names_resolve_to_their_ranges(self):
        names = '<definedNames><definedName name="Rate">S!$A$1</definedName><definedName name="Tax">0.2</definedName></definedNames>'
        rep = cw.analyze(one_sheet(self.tmp, [[0.05, F("A1*Rate*Tax", 0)]], names=names))
        reg = region(rep, "S", "B1")
        self.assertEqual(reg["split"], "single_cell")
        self.assertIn("named_constant", flag_ids(reg))
        self.assertIn("S!$A$1", reg["absolute_refs"])

    def test_edge_budget_is_reported_not_hidden(self):
        rows = [[i, F("A1", 0) if i == 1 else F(f"SUM(B1:B{i - 1})", 0)] for i in range(1, 30)]
        with mock.patch.object(cw, "MAX_EDGES", 10):
            rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertTrue(rep["graph"]["edge_budget_exceeded"])


# --------------------------------------------------------------------------
# Tables, sources, structure
# --------------------------------------------------------------------------

def table_xml(name, ref, cols, totals=0, calc=None, header_rows=None):
    calc = calc or {}
    tc = "".join(
        f'<tableColumn id="{i + 1}" name="{c}">' +
        (f"<calculatedColumnFormula>{escape(calc[c])}</calculatedColumnFormula>" if c in calc else "") +
        "</tableColumn>" for i, c in enumerate(cols))
    t = f' totalsRowCount="{totals}"' if totals else ""
    h = f' headerRowCount="{header_rows}"' if header_rows is not None else ""
    return (f'<table xmlns="{MAIN}" id="1" name="{name}" displayName="{name}" ref="{ref}"{t}{h}>'
            f'<tableColumns count="{len(cols)}">{tc}</tableColumns></table>')


def pivot_parts(cache_source=None, extra_cache_fields="", data_field_attrs="", page=True, refreshed="45000.5",
                pivot_extra="", location="A3:B8"):
    cache_source = cache_source or '<cacheSource type="worksheet"><worksheetSource ref="A1:C4" sheet="Data"/></cacheSource>'
    cache = (f'<pivotCacheDefinition xmlns="{MAIN}" xmlns:r="{RNS}" r:id="rId1" refreshedBy="x" refreshedDate="{refreshed}" recordCount="3">'
             f'{cache_source}<cacheFields count="3">'
             '<cacheField name="Region" numFmtId="0"><sharedItems/></cacheField>'
             '<cacheField name="Amount" numFmtId="0"><sharedItems containsNumber="1"/></cacheField>'
             '<cacheField name="Margin" numFmtId="0" formula="Amount*0.1" databaseField="0"/>'
             f'{extra_cache_fields}</cacheFields></pivotCacheDefinition>')
    page_xml = '<pageFields count="1"><pageField fld="2" hier="-1"/></pageFields>' if page else ""
    pv = (f'<pivotTableDefinition xmlns="{MAIN}" name="PivotTable1" cacheId="1" dataCaption="Values">'
          f'<location ref="{location}" firstHeaderRow="1" firstDataRow="1" firstDataCol="1"/>'
          '<pivotFields count="3"><pivotField axis="axisRow"/><pivotField dataField="1"/><pivotField axis="axisPage"/></pivotFields>'
          '<rowFields count="1"><field x="0"/></rowFields>'
          f'{page_xml}'
          f'<dataFields count="1"><dataField name="Sum of Amount" fld="1" subtotal="sum"{data_field_attrs}/></dataFields>'
          f'{pivot_extra}</pivotTableDefinition>')
    rels = lambda typ, tgt: (f'<Relationships xmlns="{PKG_REL}"><Relationship Id="rId1" Type="{RT}{typ}" Target="{tgt}"/></Relationships>')
    return {
        "xl/pivotTables/pivotTable1.xml": pv,
        "xl/pivotTables/_rels/pivotTable1.xml.rels": rels("pivotCacheDefinition", "../pivotCache/pivotCacheDefinition1.xml"),
        "xl/pivotCache/pivotCacheDefinition1.xml": cache,
        "xl/pivotCache/_rels/pivotCacheDefinition1.xml.rels": rels("pivotCacheRecords", "pivotCacheRecords1.xml"),
        "xl/pivotCache/pivotCacheRecords1.xml": f'<pivotCacheRecords xmlns="{MAIN}" count="3"/>',
    }


class Tables(Tmp):
    def book(self):
        cols = ["Region", "Qty", "Price", "Total"]
        calc = {"Total": "tbl_Sales[[#This Row],[Qty]]*tbl_Sales[[#This Row],[Price]]"}
        rows = [cols, ["East", 1, 2, F(calc["Total"], 2)], ["east", 2, 3, F(calc["Total"], 6)],
                ["West", 3, 4, F(calc["Total"], 12)], ["Total", F("SUBTOTAL(109,tbl_Sales[Qty])", 6), None, None]]
        sh = Sheet("Data", rows, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        return make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table_xml("tbl_Sales", "A1:D5", cols, totals=1, calc=calc)})

    def test_table_ref_loses_its_totals_row_and_calculated_columns_are_not_lifted(self):
        rep = cw.analyze(self.book())
        t = rep["tables"][0]
        self.assertEqual((t["name"], t["sheet"], t["ref"], t["data_ref"], t["totals_row_count"]),
                         ("tbl_Sales", "Data", "A1:D5", "A1:D4", 1))
        self.assertTrue([c for c in t["columns"] if c["name"] == "Total"][0]["calculated"])
        src = [s for s in rep["sources"] if s["name"] == "tbl_Sales"][0]
        self.assertEqual(src["lifted"], ["Region", "Qty", "Price"])
        self.assertEqual(src["not_lifted"], ["Total"])

    def test_stanza_is_an_explicit_read_xlsx_with_sheet_range_and_header(self):
        src = [s for s in cw.analyze(self.book())["sources"] if s["name"] == "tbl_Sales"][0]
        st = src["stanza"]
        for needle in ("duckdb.sql(", "read_xlsx(", "'book.xlsx'", "sheet = 'Data'", "range = 'A1:D4'",
                       "header = true", '"Region", "Qty", "Price"'):
            self.assertIn(needle, st)
        self.assertNotIn('"Total"', st.split("FROM")[0])

    def test_table_region_is_labelled_as_the_table_formula(self):
        rep = cw.analyze(self.book())
        reg = region(rep, "Data", "D2:D4")
        self.assertEqual(reg["split"], "row_local")


class Structure(Tmp):
    def test_mergecells_outline_hidden_autofilter_are_counted_per_sheet(self):
        sh = Sheet("S", [["g", None, "h"], ["a", "b", "c"], ({"hidden": "1"}, [1, 2, 3]),
                         ({"outlineLevel": "1"}, [4, 5, 6])],
                   before='<cols><col min="2" max="2" width="9" hidden="1"/></cols>',
                   after='<autoFilter ref="A2:C4"/><mergeCells count="1"><mergeCell ref="A1:B1"/></mergeCells>')
        rep = cw.analyze(make_book(self.tmp, [sh]))
        s = rep["sheets"][0]
        self.assertEqual((s["hidden_rows"], s["hidden_cols"], s["outline_rows"], s["merge_cells"], s["autofilter"]),
                         (1, 1, 1, 1, "A2:C4"))

    def test_two_row_header_is_detected_from_a_merged_group_row(self):
        rows = [["Q1", None, "Q2", None], ["Rev", "Cost", "Rev", "Cost"], [1, 2, 3, 4], [5, 6, 7, 8]]
        sh = Sheet("S", rows, after='<mergeCells count="2"><mergeCell ref="A1:B1"/><mergeCell ref="C1:D1"/></mergeCells>')
        src = cw.analyze(make_book(self.tmp, [sh]))["sources"][0]
        self.assertEqual(src["header_rows"], 2)
        self.assertIn("range = 'A3:D4'", src["stanza"])
        self.assertIn('"C" AS "column3"', src["stanza"])

    def test_a_one_row_header_with_a_merged_cell_is_not_labelled_a_two_row_header(self):
        rows = [["Name", "Notes", None], ["a", 1, 2], ["b", 3, 4], ["c", 5, 6]]
        sh = Sheet("S", rows, after='<mergeCells count="1"><mergeCell ref="B1:C1"/></mergeCells>')
        src = cw.analyze(make_book(self.tmp, [sh]))["sources"][0]
        self.assertEqual(src["header_rows"], 1)
        self.assertNotIn("-row header", src["stanza"])

    def test_the_header_label_states_the_true_row_count(self):
        rows = [["Q1", None, "Q2", None], ["Rev", "Cost", "Rev", "Cost"], [1, 2, 3, 4], [5, 6, 7, 8]]
        sh = Sheet("S", rows, after='<mergeCells count="2"><mergeCell ref="A1:B1"/><mergeCell ref="C1:D1"/></mergeCells>')
        self.assertIn("2-row header", cw.analyze(make_book(self.tmp, [sh]))["sources"][0]["stanza"])
        plain = cw.analyze(one_sheet(self.tmp, [["k", "v"], ["a", 1], ["b", 2]]))["sources"][0]
        self.assertEqual(plain["header_rows"], 1)
        self.assertNotIn("-row header", plain["stanza"])

    def test_inregion_subtotal_rows_are_found_and_excluded_in_the_stanza(self):
        rows = [["k", "v"], ["a", 1], ["a", 2], ["sub", F("SUM(B2:B3)", 3)], ["b", 5], ["b", 6], ["sub", F("SUBTOTAL(9,B5:B6)", 11)]]
        src = cw.analyze(one_sheet(self.tmp, rows))["sources"][0]
        self.assertEqual(src["subtotal_rows"], [4, 7])
        self.assertIn("WHERE __r NOT IN (4)", src["stanza"])

    def test_formula_columns_of_a_plain_range_are_not_lifted(self):
        rows = [["a", "b", "c"], [1, 2, F("A2+B2", 3)], [3, 4, F("A3+B3", 7)], [5, 6, F("A4+B4", 11)]]
        src = cw.analyze(one_sheet(self.tmp, rows))["sources"][0]
        self.assertEqual(src["formula_cells"], 3)
        self.assertEqual(src["lifted"], ["a", "b"])
        self.assertEqual(src["not_lifted"], ["c"])

    def test_multiple_components_on_one_sheet_are_separate_sources(self):
        rows = [["a", "b"], [1, 2], [3, 4], [None, None], [None, None], ["x", "y"], [5, 6], [7, 8]]
        srcs = cw.analyze(one_sheet(self.tmp, rows))["sources"]
        self.assertEqual([s["ref"] for s in srcs], ["A1:B3", "A6:B8"])

    def test_shared_strings_supply_header_names(self):
        sst = f'<sst xmlns="{MAIN}" count="2" uniqueCount="2"><si><t>Region</t></si><si><r><t>Am</t></r><r><t>ount</t></r></si></sst>'
        data = ('<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>'
                '<row r="2"><c r="A2" t="s"><v>0</v></c><c r="B2"><v>5</v></c></row>')
        sh = Sheet("S")
        sh.data = data
        src = cw.analyze(make_book(self.tmp, [sh], parts={"xl/sharedStrings.xml": sst}))["sources"][0]
        self.assertEqual(src["lifted"], ["Region", "Amount"])

    def test_mixed_text_and_number_columns_are_counted_per_column(self):
        rows = [["h"], [1], ["1"], [3], ["x"], [None]]
        s = cw.analyze(one_sheet(self.tmp, rows))["sheets"][0]
        self.assertEqual(s["mixed_columns"], {"A": {"number": 2, "text": 2}})  # the "h" header is not data

    def test_masked_sheets_emit_no_headers_no_examples_no_stanza(self):
        rows = [["Password", "hunter2-secret"], ["k", F('"sekret-literal"&A2', "x", t="str")], ["k", F('"sekret-literal"&A3', "x", t="str")]]
        p = make_book(self.tmp, [Sheet("Main", [[1]]), Sheet("_Config", rows, state="veryHidden")])
        rep = cw.analyze(p)
        blob = json.dumps(rep) + cw.render_text(rep)
        for secret in ("hunter2-secret", "sekret-literal", "Password"):
            self.assertNotIn(secret, blob)
        reg = [r for r in rep["regions"] if r["sheet"] == "_Config"][0]
        self.assertIsNone(reg["example"])
        self.assertIsNone(reg["r1c1"])
        self.assertTrue(all(s["stanza"] is None for s in rep["sources"] if s["sheet"] == "_Config"))


class SheetClasses(Tmp):
    def test_sheet_classes(self):
        data = Sheet("Data", [["k", "v"]] + [[f"a{i}", i] for i in range(30)])
        lookup = Sheet("Lookup", [[0, "low"], [10, "mid"], [20, "high"]])
        assump = Sheet("Assumptions", [["growth", 0.05], ["tax", 0.2]])
        report = Sheet("Report", [[F("SUMIFS(Data!B2:B31,Data!A2:A31,\"a1\")", 1)], [F("VLOOKUP(A1,Lookup!A1:B3,2)", 0)],
                                  [F("Data!B2*Assumptions!$B$2", 0)], [F("SUM(Data!B2:B31)", 0)]])
        calc = Sheet("Calc", [[1, F("A1*2", 2), F("B1+1", 3)], [2, F("A2*2", 4), F("B2+1", 5)]])
        scratch = Sheet("Scratch", [])
        cfg = Sheet("_Config", [["x", 1]], state="veryHidden")
        rep = cw.analyze(make_book(self.tmp, [data, lookup, assump, report, calc, scratch, cfg]))
        got = {s["name"]: s["class"] for s in rep["sheets"]}
        self.assertEqual(got, {"Data": "data", "Lookup": "lookup", "Assumptions": "input", "Report": "report",
                               "Calc": "calc", "Scratch": "scratch", "_Config": "config"})


# --------------------------------------------------------------------------
# Pivots
# --------------------------------------------------------------------------

class Pivots(Tmp):
    def pivot(self, **kw):
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        names = '<definedNames><definedName name="tbl_Sales">Data!$A$1:$C$4</definedName></definedNames>'
        return cw.analyze(make_book(self.tmp, [Sheet("Data", [["Region", "Amount"], ["East", 1]]), sh], parts=pivot_parts(**kw), names=names))["pivots"][0]

    def test_pivot_source_fields_and_values(self):
        pv = self.pivot(data_field_attrs=' showDataAs="percentOfTotal"')
        self.assertEqual(pv["sheet"], "P")
        self.assertEqual(pv["location"], "A3:B8")
        self.assertEqual(pv["cache_source"], {"type": "worksheet", "sheet": "Data", "ref": "A1:C4"})
        self.assertEqual(pv["row_fields"], ["Region"])
        self.assertEqual(pv["page_fields"], ["Margin"])
        self.assertEqual(pv["data_fields"], [{"name": "Sum of Amount", "field": "Amount", "subtotal": "sum",
                                              "show_data_as": "percentOfTotal"}])
        self.assertEqual(pv["calculated_fields"], [{"name": "Margin", "formula": "Amount*0.1"}])
        self.assertEqual(pv["refreshed_date"], "45000.5")
        self.assertTrue(pv["records"])

    def test_data_model_pivot_takes_the_external_branch(self):
        pv = self.pivot(cache_source='<cacheSource type="external" connectionId="1"/>')
        self.assertEqual(pv["cache_source"]["type"], "external")
        rep = cw.analyze(make_book(self.tmp, [Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])],
                                   parts=dict(pivot_parts(cache_source='<cacheSource type="external" connectionId="1"/>'), **{"xl/model/item.data": b"x"})))
        self.assertEqual(rep["external"]["pivot_external_caches"], 1)

    def test_table_named_source_and_date_grouping_and_top_n(self):
        grouped = ('<cacheField name="Date" numFmtId="14"><sharedItems/>'
                   '<fieldGroup base="0"><rangePr groupBy="months"/></fieldGroup></cacheField>')
        pv = self.pivot(cache_source='<cacheSource type="worksheet"><worksheetSource name="tbl_Sales"/></cacheSource>',
                        extra_cache_fields=grouped,
                        pivot_extra=('<filters count="1"><filter fld="0" type="count" id="1"><autoFilter ref="A1">'
                                     '<filterColumn colId="0"><top10 val="3" filterVal="3"/></filterColumn></autoFilter></filter></filters>'
                                     '<calculatedItems count="1"><calculatedItem formula="East+West"/></calculatedItems>'))
        self.assertEqual(pv["cache_source"], {"type": "worksheet", "name": "tbl_Sales"})
        self.assertEqual(pv["date_grouping"], [{"field": "Date", "group_by": "months", "grouped_on_axis": False}])
        self.assertEqual(pv["top_n_filters"], 1)
        self.assertEqual(pv["calculated_items"], 1)

    def test_cache_found_through_workbook_pivotCaches_when_the_pivot_has_no_rels(self):
        parts = pivot_parts()
        del parts["xl/pivotTables/_rels/pivotTable1.xml.rels"]
        wb_rels = f'<Relationship Id="rId90" Type="{RT}pivotCacheDefinition" Target="pivotCache/pivotCacheDefinition1.xml"/>'
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [sh], parts=parts, wb_rels_extra=wb_rels,
                                   extra_wb='<pivotCaches><pivotCache cacheId="1" r:id="rId90"/></pivotCaches>'))
        self.assertEqual(rep["pivots"][0]["cache_source"]["type"], "worksheet")

    def test_sheet_with_a_pivot_is_a_report(self):
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [sh], parts=pivot_parts()))
        self.assertEqual(rep["sheets"][0]["class"], "report")


class PivotNonAxisFilters(Tmp):
    """A filter on a field that sits on no axis still removes rows from the pivot, whether by hidden items or a slicer."""
    SHARED = '<sharedItems count="3"><s v="N"/><s v="S"/><s v="E"/></sharedItems>'
    OFF_AXIS = '<pivotField><items count="4"><item x="0"/><item h="1" x="1"/><item x="2"/><item t="default"/></items></pivotField>'

    def slicer(self, items, source="Region", pivot="PivotTable1"):
        i = "".join(f'<i x="{x}" s="1"/>' if s else f'<i x="{x}"/>' for x, s in items)
        return (f'<slicerCacheDefinition xmlns="http://schemas.microsoft.com/office/spreadsheetml/2009/9/main" name="Slicer_{source}" '
                f'sourceName="{source}"><pivotTables><pivotTable tabId="1" name="{pivot}"/></pivotTables><data>'
                f'<tabular pivotCacheId="1"><items count="{len(items)}">{i}</items></tabular></data></slicerCacheDefinition>')

    def pivot(self, hidden_field=False, slicer=None, state=None):
        parts = pivot_parts(page=False)
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        pv = pv.replace('<pivotFields count="3"><pivotField axis="axisRow"/>',
                        '<pivotFields count="3">' + (self.OFF_AXIS if hidden_field else "<pivotField/>"))
        pv = pv.replace('<pivotField axis="axisPage"/>', '<pivotField axis="axisRow"/>')
        pv = pv.replace('<rowFields count="1"><field x="0"/></rowFields>', '<rowFields count="1"><field x="2"/></rowFields>')
        parts["xl/pivotTables/pivotTable1.xml"] = pv
        cache = parts["xl/pivotCache/pivotCacheDefinition1.xml"]
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = cache.replace(
            '<cacheField name="Region" numFmtId="0"><sharedItems/></cacheField>',
            f'<cacheField name="Region" numFmtId="0">{self.SHARED}</cacheField>')
        if slicer is not None:
            parts["xl/slicerCaches/slicerCache1.xml"] = slicer
        data = Sheet("Data", [["Region", "Amount"], ["N", 1]], state=state)
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        self.book_path = make_book(self.tmp, [data, sh], parts=parts)
        return cw.analyze(self.book_path)["pivots"][0]

    def test_hidden_items_on_a_field_with_no_axis_are_a_filter(self):
        p = self.pivot(hidden_field=True)
        self.assertEqual(p["hidden_items"], [{"field": "Region", "hidden": ["S"], "count": 1}])
        self.assertEqual(p["filters"], [{"via": "filter", "field": "Region", "kept": ["N", "E"], "kept_count": 2, "hidden": ["S"]}])
        self.assertTrue(p["filter_on_non_axis_field"])

    def test_a_slicer_with_two_of_three_items_selected_is_a_filter(self):
        p = self.pivot(slicer=self.slicer([(0, True), (1, False), (2, True)]))
        self.assertEqual(p["filters"], [{"via": "slicer", "field": "Region", "kept": ["N", "E"], "kept_count": 2, "hidden": ["S"]}])
        self.assertTrue(p["filter_on_non_axis_field"])
        self.assertFalse(p["slicer_filter_unresolved"])

    def test_a_slicer_with_every_item_selected_filters_nothing(self):
        p = self.pivot(slicer=self.slicer([(0, True), (1, True), (2, True)]))
        self.assertEqual(p["filters"], [])
        self.assertFalse(p["filter_on_non_axis_field"])
        self.assertFalse(p["slicer_filter_unresolved"])

    def test_a_slicer_whose_field_cannot_be_resolved_is_reported_not_silent(self):
        p = self.pivot(slicer=self.slicer([(0, True), (1, False)], source="Gone"))
        self.assertEqual(p["filters"], [])
        self.assertTrue(p["slicer_filter_unresolved"])

    def test_a_slicer_for_another_pivot_is_ignored(self):
        p = self.pivot(slicer=self.slicer([(0, True), (1, False)], pivot="Other"))
        self.assertEqual((p["filters"], p["slicer_filter_unresolved"]), ([], False))

    def test_slicer_status_tells_an_unexamined_pivot_from_one_with_a_slicer_that_filters_nothing(self):
        self.assertEqual(self.pivot()["slicer_status"], "none")
        self.assertEqual(self.pivot(slicer=self.slicer([(0, True), (1, False)], pivot="Other"))["slicer_status"], "none")
        self.assertEqual(self.pivot(slicer=self.slicer([(0, True), (1, True), (2, True)]))["slicer_status"], "all_selected")
        self.assertEqual(self.pivot(slicer=self.slicer([(0, True), (1, False), (2, True)]))["slicer_status"], "filtering")
        self.assertEqual(self.pivot(slicer=self.slicer([(0, True), (1, False)], source="Gone"))["slicer_status"], "unresolved")

    def test_the_text_report_prints_the_slicer_status_only_for_a_pivot_with_a_slicer(self):
        self.pivot(slicer=self.slicer([(0, True), (1, True), (2, True)]))
        self.assertIn("slicer status: all_selected", cw.render_text(cw.analyze(self.book_path)))
        self.pivot()
        self.assertNotIn("slicer status", cw.render_text(cw.analyze(self.book_path)))

    def test_a_slicer_on_a_hidden_sheet_pivot_reports_nothing(self):
        p = self.pivot(slicer=self.slicer([(0, True), (1, False)]), state="hidden")
        self.assertEqual(p["slicer_status"], "none")

    def test_filters_leak_nothing_from_a_hidden_sheet(self):
        p = self.pivot(hidden_field=True, slicer=self.slicer([(0, True), (1, False)]), state="hidden")
        self.assertEqual((p["filters"], p["filter_on_non_axis_field"], p["slicer_filter_unresolved"]), ([], False, False))


class PivotSlicerAndTimelineAttribution(PivotNonAxisFilters):
    """Pivots may share a name across sheets, so a slicer or timeline cache binds by (sheet, name)."""

    def slicer_for(self, tab, items, source="Region"):
        x = self.slicer(items, source=source)
        return x.replace('tabId="1"', f'tabId="{tab}"') if tab is not None else x.replace(' tabId="1"', "")

    def timeline(self, tab, start="2020-03-01T00:00:00", end="2020-06-30T00:00:00", name="PivotTable1"):
        t = f' tabId="{tab}"' if tab is not None else ""
        return ('<timelineCacheDefinition xmlns="http://schemas.microsoft.com/office/spreadsheetml/2010/11/main" '
                'name="NativeTimeline_Date" sourceName="Date"><pivotTables>'
                f'<pivotTable{t} name="{name}"/></pivotTables><state filterType="dateRange">'
                f'<selection startDate="{start}" endDate="{end}"/>'
                '<bounds startDate="2020-01-01T00:00:00" endDate="2020-12-31T00:00:00"/></state></timelineCacheDefinition>')

    def build(self, extra_parts, second=True, states=(None, None)):
        parts = pivot_parts(page=False)
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        pv = pv.replace('<pivotFields count="3"><pivotField axis="axisRow"/>', '<pivotFields count="3"><pivotField/>')
        pv = pv.replace('<pivotField axis="axisPage"/>', '<pivotField axis="axisRow"/>')
        pv = pv.replace('<rowFields count="1"><field x="0"/></rowFields>', '<rowFields count="1"><field x="2"/></rowFields>')
        parts["xl/pivotTables/pivotTable1.xml"] = pv
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = parts["xl/pivotCache/pivotCacheDefinition1.xml"].replace(
            '<cacheField name="Region" numFmtId="0"><sharedItems/></cacheField>',
            f'<cacheField name="Region" numFmtId="0">{self.SHARED}</cacheField>')
        parts["xl/pivotTables/pivotTable2.xml"] = pv
        parts["xl/pivotTables/_rels/pivotTable2.xml.rels"] = parts["xl/pivotTables/_rels/pivotTable1.xml.rels"]
        parts.update(extra_parts)
        rel = lambda n: [("pivotTable", f"../pivotTables/pivotTable{n}.xml", "rId1")]
        sheets = [Sheet("Data", [["Region", "Amount"], ["N", 1]]), Sheet("P1", [[1]], rels=rel(1), state=states[0])]
        if second:
            sheets.append(Sheet("P2", [[1]], rels=rel(2), state=states[1]))
        self.book_path = make_book(self.tmp, sheets, parts=parts)
        return {p["sheet"]: p for p in cw.analyze(self.book_path)["pivots"]}

    def test_same_named_pivots_on_two_sheets_each_get_only_their_own_slicer(self):
        got = self.build({"xl/slicerCaches/slicerCache1.xml": self.slicer_for(11, [(0, True), (1, False), (2, True)]),
                          "xl/slicerCaches/slicerCache2.xml": self.slicer_for(12, [(0, True), (1, True), (2, True)])})
        self.assertEqual(got["P1"]["slicer_status"], "filtering")
        self.assertEqual([f["kept"] for f in got["P1"]["filters"]], [["N", "E"]])
        self.assertEqual((got["P2"]["slicer_status"], got["P2"]["filters"]), ("all_selected", []))

    def test_a_slicer_for_the_other_sheets_pivot_does_not_reach_this_one(self):
        got = self.build({"xl/slicerCaches/slicerCache1.xml": self.slicer_for(12, [(0, True), (1, False), (2, True)])})
        self.assertEqual((got["P1"]["slicer_status"], got["P1"]["filters"]), ("none", []))
        self.assertEqual(got["P2"]["slicer_status"], "filtering")

    def test_a_slicer_link_with_no_tab_id_falls_back_to_the_pivot_name(self):
        got = self.build({"xl/slicerCaches/slicerCache1.xml": self.slicer_for(None, [(0, True), (1, False), (2, True)])})
        self.assertEqual((got["P1"]["slicer_status"], got["P2"]["slicer_status"]), ("filtering", "filtering"))

    def test_a_narrowed_timeline_is_reported_and_makes_the_pivot_filtering(self):
        got = self.build({"xl/timelineCaches/timelineCache1.xml": self.timeline(11)})
        self.assertEqual(got["P1"]["timelines"], [{"field": "Date", "start": "2020-03-01T00:00:00", "end": "2020-06-30T00:00:00",
                                                    "bounds_start": "2020-01-01T00:00:00", "bounds_end": "2020-12-31T00:00:00",
                                                    "filtering": True}])
        self.assertEqual(got["P1"]["slicer_status"], "filtering")
        self.assertEqual((got["P2"]["timelines"], got["P2"]["slicer_status"]), ([], "none"))

    def test_a_timeline_spanning_its_bounds_filters_nothing(self):
        got = self.build({"xl/timelineCaches/timelineCache1.xml": self.timeline(11, "2020-01-01T00:00:00", "2020-12-31T00:00:00")})
        self.assertFalse(got["P1"]["timelines"][0]["filtering"])
        self.assertEqual(got["P1"]["slicer_status"], "all_selected")

    def test_an_unparseable_timeline_cache_is_unresolved(self):
        got = self.build({"xl/timelineCaches/timelineCache1.xml": "<timelineCacheDefinition"}, second=False)
        self.assertEqual(got["P1"]["slicer_status"], "unresolved")

    def test_the_text_report_prints_the_timeline_range_for_a_visible_pivot_only(self):
        self.build({"xl/timelineCaches/timelineCache1.xml": self.timeline(11)}, second=False)
        txt = cw.render_text(cw.analyze(self.book_path))
        self.assertIn("timeline on Date: 2020-03-01T00:00:00 to 2020-06-30T00:00:00 (narrower than 2020-01-01T00:00:00 to 2020-12-31T00:00:00)", txt)
        got = self.build({"xl/timelineCaches/timelineCache1.xml": self.timeline(11)}, second=False, states=("hidden", None))
        self.assertEqual((got["P1"]["timelines"], got["P1"]["slicer_status"]), ([], "none"))


# --------------------------------------------------------------------------
# Code attached to the workbook
# --------------------------------------------------------------------------

def dde_link():
    return (f'<externalLink xmlns="{MAIN}"><ddeLink ddeService="svc" ddeTopic="topic"><ddeItems/></ddeLink></externalLink>')


class CodeAttached(Tmp):
    def book(self, parts=None, sheets=None, **kw):
        return cw.analyze(make_book(self.tmp, sheets or [Sheet("S", [[1]])], parts=parts or {}, **kw))

    def test_each_mechanism_is_counted_separately(self):
        rep = self.book(parts={
            "docProps/custom.xml": '<Properties><property name="_AssemblyLocation"><lpwstr>x.dll</lpwstr></property>'
                                   '<property name="_AssemblyName"><lpwstr>X</lpwstr></property></Properties>',
            "xl/vbaProject.bin": b"\xD0\xCF\x11\xE0",
            "xl/macrosheets/sheet1.xml": "<xm/>",
            "xl/activeX/activeX1.xml": "<ax/>",
            "xl/embeddings/oleObject1.bin": b"\0",
            "xl/webextensions/webextension1.xml": "<we/>",
            "customUI/customUI.xml": '<customUI><button onAction="DoIt"/></customUI>',
            "xl/ctrlProps/ctrlProp1.xml": '<formControlPr fmlaLink="$A$1" fmlaRange="$B$1:$B$3"/>',
            "xl/richData/rdrichvalue.xml": "<rv/>",
        })
        ca = rep["code_attached"]
        for k in ("vsto", "vba", "xlm_macro", "activex", "ole_embedding", "web_extension", "custom_ui", "form_control", "rich_data"):
            self.assertEqual(ca[k]["count"], 1, k)
        self.assertEqual(ca["form_control"]["route"], "T")
        self.assertEqual(ca["vba"]["route"], "C")
        self.assertEqual(ca["xlm_macro"]["route"], "NR")
        self.assertTrue({"vba_project", "xlm_macros", "activex", "ole_embedding", "custom_ui", "web_extension"} <= sec_ids(rep))

    def test_vsto_only_needs_one_of_its_properties(self):
        rep = self.book(parts={"docProps/custom.xml": '<Properties><property name="_AssemblyName"/></Properties>'})
        self.assertEqual(rep["code_attached"]["vsto"]["count"], 1)
        self.assertEqual(rep["code_attached"]["vsto"]["route"], "X")

    def test_xlm_via_defined_name_and_xlm_functions(self):
        rep = self.book(names='<definedNames><definedName name="_xlnm.Auto_Open">Macro1!$A$1</definedName></definedNames>')
        self.assertIn("xlm_macro", rep["code_attached"])
        rep = cw.analyze(one_sheet(self.tmp, [[F("EXEC(\"calc.exe\")", 0)]]))
        self.assertIn("xlm_macros", sec_ids(rep))

    def test_dde_and_xla_links_in_external_links(self):
        ext = (f'<externalLink xmlns="{MAIN}" xmlns:r="{RNS}"><externalBook r:id="rId1"/></externalLink>')
        rel = (f'<Relationships xmlns="{PKG_REL}"><Relationship Id="rId1" Type="{RT}externalLinkPath" '
               'Target="file:///C:/addins/tools.xlam" TargetMode="External"/></Relationships>')
        rep = self.book(parts={"xl/externalLinks/externalLink1.xml": ext, "xl/externalLinks/_rels/externalLink1.xml.rels": rel,
                               "xl/externalLinks/externalLink2.xml": dde_link()})
        self.assertEqual(rep["code_attached"]["addin_link"]["count"], 1)
        self.assertEqual(rep["code_attached"]["dde"]["count"], 1)
        self.assertTrue(rep["code_attached"]["dde"]["unconfirmed"])
        self.assertEqual(rep["external"]["external_links"], 2)
        self.assertNotIn("C:/addins", json.dumps(rep))
        self.assertIn("dde", sec_ids(rep))

    def test_customui_and_webextension_are_marked_unconfirmed_and_form_control_is_not(self):
        rep = self.book(parts={"xl/ctrlProps/ctrlProp1.xml": '<formControlPr fmlaLink="$A$1"/>',
                               "customUI/customUI14.xml": '<customUI><button onAction="X"/></customUI>',
                               "xl/webextensions/w.xml": "<w/>"})
        for k in ("custom_ui", "web_extension"):
            self.assertTrue(rep["code_attached"][k]["unconfirmed"], k)
        self.assertFalse(rep["code_attached"]["form_control"]["unconfirmed"])

    def test_scenario_manager_and_data_validation_and_cf_expression(self):
        sh = Sheet("S", [[1]], after=('<scenarios><scenario name="a"/></scenarios>'
                                      '<conditionalFormatting sqref="A1"><cfRule type="expression" priority="1"><formula>A1&gt;1</formula></cfRule></conditionalFormatting>'
                                      '<dataValidations count="1"><dataValidation type="list" sqref="A1"><formula1>"a,b"</formula1></dataValidation></dataValidations>'))
        ca = cw.analyze(make_book(self.tmp, [sh]))["code_attached"]
        self.assertEqual(ca["scenario_manager"]["count"], 1)
        self.assertFalse(ca["scenario_manager"]["unconfirmed"])
        self.assertEqual(ca["cf_expression"]["count"], 1)
        self.assertEqual(ca["data_validation"]["count"], 1)

    def test_richdata_vm_cells_count_linked_data_types(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("1", 1, ca={"vm": "1"})]]))
        self.assertEqual(rep["code_attached"]["rich_data"]["count"], 1)

    def test_sensitivity_label_is_flagged(self):
        rep = self.book(parts={"docProps/custom.xml": '<Properties><property name="MSIP_Label_abc_Enabled"><lpwstr>true</lpwstr></property></Properties>'})
        self.assertIn("sensitivity_label", sec_ids(rep))

    def test_the_unconfirmed_list_names_every_dagger_tell(self):
        self.assertEqual(set(cw.UNCONFIRMED_TELLS), {
            "xll_udf", "unresolved_function", "addin_link", "python_in_excel", "web_extension",
            "dde", "custom_ui"})
        rep = self.book()
        self.assertEqual(set(rep["unconfirmed_tells"]), set(cw.UNCONFIRMED_TELLS))


def dm_records(rep):
    return rep["external"]["connection_list"]


class Connections(Tmp):
    def conn_xml(self, extra=""):
        return (f'<connections xmlns="{MAIN}">'
                '<connection id="1" name="c1" type="1" savePassword="1" refreshedVersion="6">'
                '<dbPr connection="Provider=SQLOLEDB;Data Source=srv;Initial Catalog=db;User ID=u;Password=topsecret-pw" '
                'command="SELECT secret_col FROM t" commandType="2"/>'
                '<parameters count="1"><parameter name="p" cell="Sheet1!$B$2"/></parameters></connection>'
                f'{extra}</connections>')

    def test_connections_and_querytables_are_counted_as_flags_only(self):
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], parts={
            "xl/connections.xml": self.conn_xml(),
            "xl/queryTables/queryTable1.xml": f'<queryTable xmlns="{MAIN}" name="q" connectionId="1"/>'}))
        ext = rep["external"]
        self.assertEqual((ext["connections"], ext["query_tables"], ext["connections_save_password"],
                          ext["connections_embedded_credential"], ext["connection_parameters"]), (1, 1, 1, 1, 1))
        blob = json.dumps(rep) + cw.render_text(rep)
        self.assertNotIn("topsecret-pw", blob)
        self.assertEqual(rep["external"]["connection_list"][0]["command"], "SELECT secret_col FROM t")
        self.assertTrue({"connections_save_password", "connections_embedded_credential"} <= sec_ids(rep))
        self.assertIn("external_data", tell_ids(rep))

    def mashup(self, m_text, bom="utf-16"):
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("Formulas/Section1.m", m_text)
        body = inner.getvalue()
        blob = struct.pack("<I", 0) + struct.pack("<I", len(body)) + body + struct.pack("<I", 0)
        xml = ('<?xml version="1.0" encoding="utf-16"?><DataMashup xmlns="http://schemas.microsoft.com/DataMashup">'
               + base64.b64encode(blob).decode() + "</DataMashup>")
        return xml.encode(bom)  # python's "utf-16" writes a BOM

    def test_datamashup_in_utf16_is_read_through_its_inner_zip(self):
        m = ('section Section1;\n'
             'shared Orders = let Source = Sql.Database("srv", "db", [Query="select password from x"]) in Source;\n'
             'shared Files = let Source = Excel.Workbook(File.Contents("C:\\\\x.xlsx")) in Source;\n'
             'shared PG = PostgreSQL.Database("h", "d");\n')
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], parts={"customXml/item1.xml": self.mashup(m)}))
        dm = rep["external"]["data_mashup"]
        self.assertEqual(dm["queries"], 3)
        self.assertEqual(dm["connectors"], {"Sql.Database": 1, "Excel.Workbook": 1, "PostgreSQL.Database": 1})
        self.assertIn("data_mashup", sec_ids(rep))
        blob = json.dumps(rep) + cw.render_text(rep)
        self.assertEqual({r["query"]: r["command"] for r in dm_records(rep)}["Orders"], "select password from x")
        self.assertNotIn("C:", blob)

    def test_datamashup_inner_zip_obeys_the_caps(self):
        with mock.patch.object(cw, "MAX_PART_BYTES", 3000):
            rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])],
                                       parts={"customXml/item1.xml": self.mashup("section Section1;\n" + "shared A = 1;\n" * 2000)}))
        self.assertTrue(rep["external"]["data_mashup"]["rejected"])

    def test_embedded_password_in_m_is_a_flag_not_a_value(self):
        m = 'section Section1;\nshared A = Sql.Database("s","d",[Password="hunter2-m"]);\n'
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], parts={"customXml/item1.xml": self.mashup(m)}))
        self.assertTrue(rep["external"]["data_mashup"]["embedded_credential"])
        self.assertNotIn("hunter2-m", json.dumps(rep) + cw.render_text(rep))


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

class Output(Tmp):
    def book(self):
        return make_book(self.tmp, [Sheet("Data", [["k", "v"], ["a", 1], ["b", 2]]),
                                    Sheet("R", [[F("SUM(Data!B2:B3)", 3)]]),
                                    Sheet("H", [[1]], state="veryHidden")], parts={"xl/vbaProject.bin": b"\0"})

    def test_security_flags_print_before_the_classification(self):
        text = cw.render_text(cw.analyze(self.book()))
        self.assertIn("## Security flags", text)
        self.assertLess(text.index("## Security flags"), text.index("## Sheets"))
        self.assertLess(text.index("## Security flags"), text.index("## Oracle"))
        first_heading = [l for l in text.splitlines() if l.startswith("## ")][0]
        self.assertEqual(first_heading, "## Security flags")

    def test_text_report_sections_and_what_was_not_read(self):
        text = cw.render_text(cw.analyze(self.book()))
        for h in ("## Sheets", "## Oracle", "## Sources", "## Formula regions", "## Routes", "## Functions",
                  "## Code attached", "## Dependency graph", "## Not read"):
            self.assertIn(h, text)
        self.assertIn("vbaProject.bin", text.split("## Not read")[1])
        self.assertIn("unconfirmed", text)

    def test_cli_default_form_and_json(self):
        book = self.book()
        plain = subprocess.run([sys.executable, str(SCRIPT), book], capture_output=True, text=True)
        self.assertEqual(plain.returncode, 0, plain.stderr)
        self.assertTrue(plain.stdout.startswith("# book.xlsx"))
        explicit = subprocess.run([sys.executable, str(SCRIPT), "classify", book], capture_output=True, text=True)
        self.assertEqual(explicit.stdout, plain.stdout)
        js = subprocess.run([sys.executable, str(SCRIPT), "classify", book, "--json"], capture_output=True, text=True)
        self.assertEqual(json.loads(js.stdout)["workbook"], "book.xlsx")
        js2 = subprocess.run([sys.executable, str(SCRIPT), book, "--json"], capture_output=True, text=True)
        self.assertEqual(js.stdout, js2.stdout)

    def test_unreadable_input_exits_nonzero_with_a_message(self):
        p = os.path.join(self.tmp, "x.xlsx")
        with open(p, "wb") as fh:
            fh.write(b"nope")
        r = subprocess.run([sys.executable, str(SCRIPT), p], capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        self.assertIn("not_zip", r.stdout)

    def test_json_never_carries_hidden_sheet_values_or_connection_text(self):
        rows = [["Password", "hunter2-secret"], ["k", F('"sekret-literal"', "sekret-literal", t="str")]]
        p = make_book(self.tmp, [Sheet("V", rows, state="veryHidden"), Sheet("H", rows, state="hidden"),
                                 Sheet("M", [[1]])],
                      parts={"customXml/item1.xml": "<root>top-secret-xml</root>",
                             "xl/connections.xml": Connections.conn_xml(self)})
        rep = cw.analyze(p)
        blob = json.dumps(rep) + cw.render_text(rep)
        for s in ("hunter2-secret", "sekret-literal", "top-secret-xml", "topsecret-pw"):
            self.assertNotIn(s, blob)

    def test_1900_and_1904_flag_show_in_the_report(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1]], wb_pr='<workbookPr date1904="1"/>'))
        self.assertIn("1904", cw.render_text(rep))


# --------------------------------------------------------------------------
# Safety hardening
# --------------------------------------------------------------------------

def patch_part(path, part, old, new):
    with zipfile.ZipFile(path) as zf:
        parts = {n: zf.read(n) for n in zf.namelist()}
    assert old.encode() in parts[part], old
    parts[part] = parts[part].replace(old.encode(), new.encode())
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for n, d in parts.items():
            zf.writestr(n, d)


DTD = '<!DOCTYPE x [<!ENTITY a "b">]>'


class PrologGuardFailsClosed(unittest.TestCase):
    def rejected(self, data, reason=None):
        """The guard itself must refuse it: expat alone may or may not."""
        with self.assertRaises(cw.Rejected) as cm:
            cw.check_prolog(data)
        if reason:
            self.assertEqual(cm.exception.reason, reason)
        with self.assertRaises(cw.Rejected):
            cw.parse_xml(data)

    def test_utf16_without_a_bom_and_with_leading_whitespace(self):
        for enc in ("utf-16-le", "utf-16-be"):
            for lead in ("", " ", "\n  "):
                with self.subTest(enc=enc, lead=repr(lead)):
                    self.rejected((lead + '<?xml version="1.0"?>' + DTD + "<x/>").encode(enc), "dtd")

    def test_a_prolog_longer_than_the_scan_window_still_finds_the_dtd(self):
        pad = 3 * 1024 * 1024
        self.rejected((" " * pad + DTD + "<x/>").encode())
        self.rejected(("<!--" + "c" * pad + "-->" + DTD + "<x/>").encode())
        self.rejected(("<?pi " + "p" * pad + "?>" + DTD + "<x/>").encode())
        self.rejected((" " * pad + DTD + "<x/>").encode("utf-16"))

    def test_an_unterminated_comment_or_pi_before_the_root_fails_closed(self):
        self.rejected(b"<!-- never closed <x/>")
        self.rejected(b"<?pi never closed <x/>")

    def test_a_document_that_does_not_start_with_a_tag_is_refused(self):
        ebcdic = ('<?xml version="1.0" encoding="cp037"?>' + DTD + "<x/>").encode("cp037")
        self.rejected(ebcdic)
        self.rejected(('<?xml version="1.0" encoding="cp037"?>' + "\x4c\x6f").encode("latin-1"))

    def test_only_expat_native_encodings_may_be_declared(self):
        for enc in ("shift_jis", "utf-7", "nosuch-encoding", "cp037"):
            with self.subTest(enc=enc):
                with self.assertRaises(cw.Rejected) as cm:  # the scan itself, not expat's own complaint
                    cw.check_prolog(f'<?xml version="1.0" encoding="{enc}"?><x/>'.encode())
                self.assertEqual(cm.exception.reason, "malformed")
                self.assertNotIn(enc, str(cm.exception) + cm.exception.detail)
                with self.assertRaises(cw.Rejected):
                    cw.parse_xml(f'<?xml version="1.0" encoding="{enc}"?><x/>'.encode())
        cw.check_prolog(b'<?xml version="1.0" encoding="ISO-8859-1"?><x/>')
        self.assertEqual(cw.ln(cw.parse_xml(b'<?xml version="1.0" encoding="ISO-8859-1"?><x/>').tag), "x")

    def test_a_declared_bad_encoding_in_a_sheet_does_not_abort_the_report(self):
        tmp = tempfile.mkdtemp()
        p = make_book(tmp, [Sheet("S", [[1]])])
        patch_part(p, "xl/worksheets/sheet1.xml", "<worksheet", '<?xml version="1.0" encoding="shift_jis"?><worksheet')
        rep = cw.analyze(p)
        self.assertTrue(any(r["part"] == "xl/worksheets/sheet1.xml" for r in rep["package"]["rejected"]))
        self.assertIn("## Security flags", cw.render_text(rep))


class SafetyHardening(Tmp):
    def mashup_book(self, m_text, dup=False):
        inner = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with zipfile.ZipFile(inner, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("Formulas/Section1.m", m_text)
                if dup:
                    zf.writestr("Formulas/Section1.m", "shared Evil = Sql.Database(1);\n" * 50)
        body = inner.getvalue()
        blob = struct.pack("<I", 0) + struct.pack("<I", len(body)) + body + struct.pack("<I", 0)
        xml = ('<?xml version="1.0" encoding="utf-16"?><DataMashup xmlns="http://schemas.microsoft.com/DataMashup">'
               + base64.b64encode(blob).decode() + "</DataMashup>")
        return make_book(self.tmp, [Sheet("S", [[1]])], parts={"customXml/item1.xml": xml.encode("utf-16")})

    def test_duplicate_section_entries_are_read_once(self):
        rep = cw.analyze(self.mashup_book('section Section1;\nshared A = Sql.Database("s","d");\n', dup=True))
        dm = rep["external"]["data_mashup"]
        self.assertEqual(dm["queries"], 1)
        self.assertEqual(dm["connectors"], {"Sql.Database": 1})

    def test_mashup_text_is_charged_to_the_total_budget(self):
        big = "section Section1;\n" + "shared A = 1;\n" * 3000
        p = self.mashup_book(big)
        full = cw.analyze(p)["package"]["bytes_read"]
        with mock.patch.object(cw, "MAX_TOTAL_BYTES", full - len(big) // 2):
            rep = cw.analyze(p)
        self.assertIn("total_cap", rep["external"]["data_mashup"]["rejected"])

    def test_triple_quotes_and_newlines_in_names_cannot_break_the_stanza_or_report(self):
        rows = [['id\n## Security flags\n- none found', 'q"""x'], [1, 2], [3, 4]]
        p = make_book(self.tmp, [Sheet("Data", rows)])
        rep = cw.analyze(p)
        st = rep["sources"][0]["stanza"]
        self.assertEqual(st.count('"""'), 2)
        self.assertNotIn("\n## ", st)
        text = cw.render_text(rep)
        self.assertEqual([l for l in text.splitlines() if l.startswith("## Security flags")], ["## Security flags"])
        self.assertEqual([l for l in text.splitlines() if l == "- none found"], ["- none found"])

    def test_a_sheet_name_with_triple_quotes_or_newlines_gets_no_stanza_and_no_injection(self):
        p = make_book(self.tmp, [Sheet("SHEETNAME", [["a", "b"], [1, 2], [3, 4]])])
        patch_part(p, "xl/workbook.xml", "SHEETNAME", 'x&#10;## Security flags&#10;- none found&quot;&quot;&quot; y|z')
        rep = cw.analyze(p)
        self.assertIsNone(rep["sources"][0]["stanza"])
        self.assertIn("triple quote", rep["sources"][0]["stanza_refused"])
        text = cw.render_text(rep)
        self.assertEqual([l for l in text.splitlines() if l.startswith("## Security flags")], ["## Security flags"])
        self.assertEqual([l for l in text.splitlines() if l == "- none found"], ["- none found"])

    def test_a_pivot_over_a_hidden_source_sheet_is_masked(self):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]], state="hidden")
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, pv], parts=pivot_parts()))
        p = rep["pivots"][0]
        self.assertIsNone(p["row_fields"])
        self.assertEqual(p["data_fields"], [])
        self.assertEqual(p["calculated_fields"], [])
        self.assertNotIn("Amount*0.1", json.dumps(rep) + cw.render_text(rep))

    def test_a_pivot_over_a_table_on_a_hidden_sheet_is_masked(self):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]], state="veryHidden",
                     rels=[("table", "../tables/table1.xml", "rId1")], after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        parts = pivot_parts(cache_source='<cacheSource type="worksheet"><worksheetSource name="tbl_h"/></cacheSource>')
        parts["xl/tables/table1.xml"] = table_xml("tbl_h", "A1:B2", ["Region", "Amount"])
        rep = cw.analyze(make_book(self.tmp, [data, pv], parts=parts))
        self.assertIsNone(rep["pivots"][0]["row_fields"])

    def test_an_overflowing_subtotal_code_does_not_abort(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("SUBTOTAL(1e999,A1:A2)", 0)]]))
        self.assertEqual(rep["status"], "ok")

    def test_a_part_that_raises_unexpectedly_degrades_into_the_report(self):
        p = one_sheet(self.tmp, [[1, F("A1", 1)]])
        with mock.patch.object(cw, "take_cell", side_effect=RuntimeError("boom with /secret/text")):
            rep = cw.analyze(p)
        blob = json.dumps(rep) + cw.render_text(rep)
        self.assertNotIn("/secret/text", blob)
        self.assertIn("## Security flags", cw.render_text(rep))
        self.assertTrue(any(r["reason"] == "parse_error" for r in rep["package"]["rejected"]))

    def test_duplicate_sheet_names_are_refused(self):
        p = make_book(self.tmp, [Sheet("Dup", [[1]]), Sheet("dup", [[2]], state="veryHidden")])
        rep = cw.analyze(p)
        self.assertEqual(rep["status"], "unreadable")
        self.assertEqual(rep["error"]["kind"], "duplicate_sheet_names")

    def test_code_parts_are_found_by_content_type_not_only_by_path(self):
        ct = ('<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
              '<Override PartName="/xl/hidden/blob.bin" ContentType="application/vnd.ms-office.vbaProject"/>'
              '<Override PartName="/xl/weird/m1.xml" ContentType="application/vnd.ms-excel.macrosheet+xml"/></Types>')
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]])], parts={
            "[Content_Types].xml": ct, "xl/hidden/blob.bin": b"\0", "xl/weird/m1.xml": "<m/>"}))
        self.assertEqual(rep["code_attached"]["vba"]["count"], 1)
        self.assertEqual(rep["code_attached"]["xlm_macro"]["count"], 1)

    def test_a_dde_formula_in_a_cell_is_flagged(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("cmd|'/c calc'!A0", 0)]]))
        self.assertIn("dde", sec_ids(rep))
        self.assertEqual(rep["code_attached"]["dde"]["count"], 1)

    def test_refinitiv_tr_is_a_vendor_feed(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F('TR("IBM.N","TR.PriceClose")', 0)]]))
        self.assertEqual(region(rep, "S", "A1")["route"], "X")


# --------------------------------------------------------------------------
# Lifting and masking
# --------------------------------------------------------------------------

class LiftMask(Tmp):
    def src(self, sheets, parts=None, **kw):
        return cw.analyze(make_book(self.tmp, sheets, parts=parts, **kw))["sources"]

    def test_array_output_inside_a_block_is_never_lifted(self):
        rows = [["a", "b", "c"], [1, 2, F("A2:A4*2", 2, fa={"t": "array", "ref": "C2:C4"})], [3, 4, 6], [5, 6, 10], [7, 8, 14]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertEqual(s["lifted"], ["a", "b"])
        self.assertEqual(s["not_lifted"], ["c"])
        self.assertNotIn('"c"', s["stanza"].split("FROM")[0])

    def test_spill_datatable_and_pivot_output_inside_a_block_are_never_lifted(self):
        base = [["a", "b", "c"], [1, 2, 3], [4, 5, 6], [7, 8, 9], [1, 1, 1]]
        spill = [r[:] for r in base]
        spill[1][2] = F("SORT(A2:A4)", 3, fa={"t": "array", "ref": "C2:C4"}, ca={"cm": "1"})
        dtab = [r[:] for r in base]
        dtab[1][1] = F("TABLE(,A9)", 2, fa={"t": "dataTable", "ref": "B2:B4", "dt2D": "0", "dtr": "0", "r1": "A9"})
        for rows, gone in ((spill, "c"), (dtab, "b")):
            with self.subTest(gone=gone):
                s = self.src([Sheet("S", rows)])[0]
                self.assertNotIn(gone, s["lifted"])
                self.assertIn(gone, s["not_lifted"])
                self.assertNotIn(f'"{gone}"', s["stanza"].split("FROM")[0])
        sh = Sheet("S", base, rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        s = self.src([sh], parts=pivot_parts(location="B3:B4"))[0]
        self.assertEqual(s["lifted"], ["a", "c"])

    def test_headerless_blocks_get_an_explicit_column_list_without_the_formula_column(self):
        rows = [[1, 2, F("A1+B1", 3)], [2, 3, F("A2+B2", 5)], [3, 4, F("A3+B3", 7)]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertFalse(s["header"])
        self.assertEqual(s["lifted"], ["A", "B"])
        self.assertNotIn("SELECT *", s["stanza"])
        self.assertIn('SELECT "A", "B"', s["stanza"])

    def test_a_minority_formula_in_a_lifted_column_is_flagged_never_silent(self):
        rows = [["h1", "h2"], [1, 2], [3, F("A3*2", 6)], [5, 6], [7, 8]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertEqual(s["lifted"], ["h1", "h2"])
        self.assertEqual(s["formula_cells_in_lifted"], ["B3"])
        self.assertIn("WARNING", s["stanza"])
        self.assertIn("B3", s["stanza"])

    def test_trailing_total_rows_are_trimmed_out_of_the_range(self):
        rows = [["k", "v"], ["a", 1], ["b", 2], ["Total", F("SUM(B2:B3)", 3)]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertEqual(s["data_ref"], "A1:B3")
        self.assertEqual(s["trimmed_rows"], [4])
        self.assertIn("range = 'A1:B3'", s["stanza"])

    def test_an_inrange_subtotal_placeholder_fails_loudly_if_run_as_is(self):
        rows = [["k", "v"], ["a", 1], ["a", 2], ["sub", F("SUM(B2:B3)", 3)], ["b", 5], ["b", 6]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertEqual(s["subtotal_rows"], [4])
        self.assertNotIn("WHERE TRUE", s["stanza"])
        self.assertIn("WHERE __r NOT IN (4)", s["stanza"])
        self.assertIn("row_number() OVER () + 1 AS __r", s["stanza"])

    def side_by_side(self, side_formulas, extra_bottom=None):
        rows = []
        for i in range(30):
            r = i + 1
            left = ["k", "v", "w", "x"] if r == 1 else [f"n{r}", r, r * 2, r * 3]
            mid = [None, None, None]
            right = ["g", "t"] if r == 1 else [f"s{r}", r]
            if r in side_formulas:
                right = [f"s{r}", side_formulas[r]]
            rows.append(left + mid + right)
        if extra_bottom:
            rows.append(extra_bottom + [None] * 3)
        return rows

    def test_aggregates_in_another_block_do_not_exclude_rows_of_this_one(self):
        rows = self.side_by_side({24: F("SUM(I2:I23)", 0), 28: F("SUM(I25:I27)", 0)})
        srcs = [s for s in self.src([Sheet("S", rows)]) if s["ref"].startswith("A1")]
        self.assertEqual(len(srcs), 1)
        s = srcs[0]
        self.assertEqual(s["ref"], "A1:D30")
        self.assertEqual(s["subtotal_rows"], [])
        self.assertEqual(s["trimmed_rows"], [])
        self.assertEqual(s["excluded_rows"], [])
        self.assertNotIn("NOT IN", s["stanza"])
        self.assertNotIn("excluded rows", s["stanza"])

    def test_aggregates_reading_this_blocks_columns_from_another_block_do_not_exclude_rows(self):
        rows = self.side_by_side({24: F("SUMIF(A2:A30,\"n5\",B2:B30)", 0), 28: F("SUM(B2:B27)", 0)})
        s = [s for s in self.src([Sheet("S", rows)]) if s["ref"].startswith("A1")][0]
        self.assertEqual(s["excluded_rows"], [])
        self.assertEqual(s["subtotal_rows"], [])

    def test_a_total_row_in_the_blocks_own_columns_is_excluded_and_reported(self):
        rows = self.side_by_side({}, extra_bottom=["Total", F("SUM(B2:B30)", 0), F("SUM(C2:C30)", 0), None])
        s = [s for s in self.src([Sheet("S", rows)]) if s["ref"].startswith("A1")][0]
        self.assertEqual(s["trimmed_rows"], [31])
        self.assertEqual(s["excluded_rows"], [{"row": 31, "via": "B31"}])
        self.assertIn("-- excluded rows: 31 (via B31)", s["stanza"])
        self.assertIn("range = 'A1:D30'", s["stanza"])

    def test_a_mid_block_subtotal_in_the_blocks_own_columns_is_excluded_and_reported(self):
        rows = [["k", "v"], ["a", 1], ["a", 2], ["sub", F("SUM(B2:B3)", 3)], ["b", 5], ["b", 6]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertEqual(s["excluded_rows"], [{"row": 4, "via": "B4"}])
        self.assertIn("-- excluded rows: 4 (via B4)", s["stanza"])

    def test_a_totals_row_under_a_blank_row_is_not_part_of_the_range(self):
        rows = self.side_by_side({}) + [[None] * 7, ["Total", F("SUM(B2:B30)", 0), None, None]]
        s = [s for s in self.src([Sheet("S", rows)]) if s["ref"].startswith("A1")][0]
        self.assertEqual(s["data_ref"], "A1:D30")
        self.assertEqual(s["excluded_rows"], [])

    def test_wide_layout_is_unpivoted_and_the_header_row_is_never_lifted_as_data(self):
        rows = [["Account", 2021, 2022, 2023], ["Rev", 1, 2, 3], ["Cost", 4, 5, 6]]
        s = self.src([Sheet("S", rows)])[0]
        self.assertEqual(s["layout"], "wide")
        self.assertTrue(s["header"])
        self.assertEqual(s["period_columns"], 3)
        self.assertIn("header = false", s["stanza"])
        self.assertIn("range = 'A2:D3'", s["stanza"])
        self.assertIn("UNPIVOT", s["stanza"])
        self.assertIn('EXCLUDE ("Account")', s["stanza"])

    def test_an_ordinary_lookup_is_not_mistaken_for_a_wide_layout(self):
        s = self.src([Sheet("S", [[0, "low"], [10, "mid"], [20, "high"]])])[0]
        self.assertEqual(s.get("layout"), "long")


# --------------------------------------------------------------------------
# Routes and flags
# --------------------------------------------------------------------------

class RoutesNamesAndRanges(Tmp):
    check = Routes.check

    def test_a_defined_name_holding_a_formula_folds_into_the_caller(self):
        names = ('<definedNames><definedName name="Dyn">OFFSET(S!$A$1,0,0,2,1)</definedName>'
                 '<definedName name="Noise">RAND()</definedName></definedNames>')
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("SUM(Dyn)", 0), F("A1*Noise", 0)]], names=names))
        self.assertEqual(region(rep, "S", "B1")["route"], "NR")
        self.assertIn("OFFSET", region(rep, "S", "B1")["functions"])
        self.assertEqual(region(rep, "S", "C1")["route"], "X")

    def test_a_range_end_built_by_a_function_asks_the_user(self):
        self.check("SUM(A1:INDEX(A:A,3))", "NR", ["opaque_dependency"])
        self.check("SUM(INDEX(A:A,B1):A3)", "NR", ["opaque_dependency"])

    def test_concatenated_criteria_are_not_blank_or_nonblank(self):
        self.check('SUMIFS(B1:B4,A1:A4,"<>"&D1)', "T", ["criteria_comparison"], ["criteria_nonblank", "criteria_blank"])
        self.check('SUMIFS(B1:B4,A1:A4,"="&D1)', "T", ["criteria_comparison"], ["criteria_blank"])
        self.check('SUMIFS(B1:B4,A1:A4,""&D1)', "T", ["criteria_cell"], ["criteria_blank"])
        self.check('SUMIFS(B1:B4,C1:C4,D1&"*")', "T", ["criteria_wildcard", "criteria_cell"])
        self.check('SUMIFS(B1:B4,C1:C4,"=")', "T", ["criteria_blank"])

    def test_an_empty_but_present_argument_is_exact(self):
        self.check("VLOOKUP(C1,C1:D4,2,)", "T", ["ci_match"], ["approx_match"])
        self.check("MATCH(C1,C1:C4,)", "T", ["ci_match"], ["approx_match"])
        self.check("_xlfn.XLOOKUP(A1,A1:A4,B1:B4,,)", "T", [], ["approx_match"])
        self.check("VLOOKUP(A1,A1:B4,2)", "C", ["approx_match"])

    def test_spill_and_implicit_intersection_syntax_are_not_addins(self):
        rep, reg = self.check("SUM(_xlfn.ANCHORARRAY(A1))", "NR", ["spill_ref", "opaque_dependency"])
        self.assertNotIn("xll_udf", rep["code_attached"])
        self.assertNotIn("code_snapshot", tell_ids(rep))
        rep, reg = self.check("_xlfn.SINGLE(A1:A3)", "T", ["implicit_intersection"])
        self.assertNotIn("xll_udf", rep["code_attached"])

    def test_text_equality_is_case_insensitive_everywhere(self):
        self.check('IF(C1="x",1,0)', "T", ["ci_match"])
        self.check('SUMPRODUCT((C1:C4="a")*B1:B4)', "T", ["ci_match", "sumproduct_mask"])
        self.check('IF(C1<>"x",1,0)', "T", ["ci_match"])
        self.check("IF(A1=1,1,0)", "T", [], ["ci_match"])

    def test_equality_between_cells_is_case_insensitive_unless_both_sides_are_numeric(self):
        for f in ("IF(C1=C2,1,0)", "IF(C1<>C2,1,0)", "IF(Z9=A1,1,0)", "IF(A1=Z9,1,0)", "SUMPRODUCT((C1:C4=C1)*B1:B4)",
                  'IFS(C1="x",1,TRUE,0)', "IF(SUM(A1:A2)=B1,1,0)"):
            with self.subTest(f=f):
                self.check(f, "T", ["ci_match"])
        for f in ("IF(A1=B1,1,0)", "IF(A1<>B1,1,0)", "IF(Z9=1,1,0)", "IF(1=Z9,1,0)", "SUMPRODUCT((A1:A4=2)*B1:B4)", "IF(A1:A2=B1:B2,1,0)"):
            with self.subTest(f=f):
                self.check(f, "T", [], ["ci_match"])

    def test_lookup_two_over_one_divided_by_a_condition_is_the_last_match_idiom(self):
        for f in ('LOOKUP(2,1/(C1:C4<>""),B1:B4)', "LOOKUP(2,1/(A1:A4<>0),B1:B4)", 'LOOKUP(2,1/(C1:C4="x"),B1:B4)',
                  "LOOKUP(9.99E+307,1/(A1:A4>1),B1:B4)"):
            with self.subTest(f=f):
                _, reg = self.check(f, "C", ["last_match_idiom"], ["approx_match"])
                self.assertTrue(any("last" in r for r in reg["reasons"]), reg["reasons"])

    def test_lookup_without_a_division_by_a_condition_stays_an_approximate_search(self):
        self.check("LOOKUP(2,A1:A4,B1:B4)", "C", ["approx_match"], ["last_match_idiom"])
        self.check("LOOKUP(2,1/A1:A4,B1:B4)", "C", ["approx_match"], ["last_match_idiom"])
        self.check("LOOKUP(A1,1/(A1:A4>1),B1:B4)", "C", ["approx_match"], ["last_match_idiom"])

    def test_index_with_a_row_or_column_that_can_reach_zero_is_flagged(self):
        for f in ("INDEX(B1:B4,ROW()-1)", "INDEX(B1:B4,MATCH(A1,A1:A4,0)-1)", 'INDEX(B1:B4,COUNTIF(A1:A4,">1"))',
                  "INDEX(A1:B4,1,COLUMN()-1)", "INDEX(B1:B4,(ROW()-1))"):
            with self.subTest(f=f):
                _, reg = self.check(f, "T", ["index_row_zero"])
        _, reg = self.check("INDEX(B1:B4,ROW()-1)", "T", ["index_row_zero"])
        self.assertEqual([x["detail"] for x in reg["flags"] if x["id"] == "index_row_zero"], ["row"])
        _, reg = self.check("INDEX(A1:B4,1,COLUMN()-1)", "T", ["index_row_zero"])
        self.assertEqual([x["detail"] for x in reg["flags"] if x["id"] == "index_row_zero"], ["column"])

    def test_a_running_total_anchored_with_a_literal_index_routes_like_the_plain_expanding_range(self):
        def column(form):
            rows = [["h", "v"]] + [[i, F(form(i), 0)] for i in range(2, 32)]
            rep = cw.analyze(one_sheet(self.tmp, rows))
            return [(r["ref"], r["route"], r["reasons"], sorted(flag_ids(r))) for r in rep["regions"]]
        plain = column(lambda i: f"SUM(A$2:A{i})")
        self.assertEqual(column(lambda i: f"SUM(INDEX(A$2:A$40,1,1):A{i})"), plain)
        self.assertEqual(column(lambda i: f"SUM(INDEX(A$2:A$40,1):A{i})"), plain)

    def test_an_index_range_end_with_a_dynamic_position_still_routes_nr(self):
        self.check("SUM(INDEX(A1:A4,MATCH(A1,A1:A4,0)):A4)", "NR", ["opaque_dependency"])
        self.check("SUM(INDEX(A1:A4,B1,1):A4)", "NR", ["opaque_dependency"])

    def test_index_row_zero_is_not_flagged_when_the_row_is_guarded(self):
        for f in ('IF(ROW()-1>0,INDEX(B1:B4,ROW()-1),"")', 'IF(ROW()-1<>0,INDEX(B1:B4,ROW()-1),0)',
                  'IF(ISNUMBER(ROW()-1),INDEX(B1:B4,ROW()-1),0)', 'IFERROR(INDEX(B1:B4,ROW()-1),0)',
                  'IFNA(INDEX(B1:B4,MATCH(A1,A1:A4,0)-1),0)', 'IFS(ROW()-1>0,INDEX(B1:B4,ROW()-1),TRUE,0)',
                  'SUM(IF(ROW()-1>0,INDEX(B1:B4,ROW()-1),0),1)', 'IFS(A1<0,0,ROW()-1>0,INDEX(B1:B4,ROW()-1))'):
            with self.subTest(f=f):
                self.check(f, "T", [], ["index_row_zero"])

    def test_index_row_zero_is_flagged_when_the_guard_tests_something_else(self):
        for f in ('IF(A1>0,INDEX(B1:B4,ROW()-1),"")', 'IF(ROW()-1>0,"",INDEX(B1:B4,ROW()-1))',
                  'IF(INDEX(B1:B4,ROW()-1)>0,1,0)', 'IFERROR(1,INDEX(B1:B4,ROW()-1))'):
            with self.subTest(f=f):
                self.check(f, "T", ["index_row_zero"])

    def test_index_with_a_row_that_cannot_reach_zero_is_not_flagged(self):
        for f in ("INDEX(B1:B4,2)", "INDEX(B1:B4,0)", "INDEX(B1:B4,MATCH(A1,A1:A4,0))",
                  "INDEX(B1:B4,MAX(1,ROW()-1))", "INDEX(B1:B4,ROW())", "INDEX(A1:B4,1,2)"):
            with self.subTest(f=f):
                self.check(f, "T", [], ["index_row_zero"])
        self.check("INDEX(B1:B4,MATCH(A1,A1:A4,-1))", "C", ["approx_match"], ["index_row_zero"])

    def test_aggregate_options_decide_whether_it_reads_ui_state(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1], [2], [F("AGGREGATE(9,5,A1:A2)", 3)], [F("AGGREGATE(9,6,A1:A2)", 3)]]))
        self.assertIn("subtotal_ui_state", flag_ids(region(rep, "S", "A3")))
        self.assertNotIn("subtotal_ui_state", flag_ids(region(rep, "S", "A4")))

    def test_subtotal_checks_the_autofilter_of_the_sheet_it_reads(self):
        d = Sheet("Data", [["h"], [1], [2]], after='<autoFilter ref="A1:A3"/>')
        r = Sheet("Report", [[F("SUBTOTAL(9,Data!A2:A3)", 3)]])
        rep = cw.analyze(make_book(self.tmp, [d, r]))
        self.assertIn("subtotal_ui_state", flag_ids(region(rep, "Report", "A1")))

    def test_calcOnSave_false_is_untrusted(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1", 1)]], calc='<calcPr calcId="1" calcOnSave="false"/>'))
        self.assertIn("calc_on_save_off", tell_ids(rep))

    def test_the_same_formula_in_two_separate_blocks_is_two_regions(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, F("A3*2", 6)]] + [[None]] * 6 + [[4, F("A10*2", 8)], [5, F("A11*2", 10)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual([r["ref"] for r in rep["regions"]], ["B1:B3", "B10:B11"])

    def test_more_builtins_are_not_addins(self):
        for f in ("CONFIDENCE.NORM(0.05,1,10)", "ERF(1)", "GAMMA(2)", "T.TEST(A1:A4,B1:B4,2,1)", "STANDARDIZE(A1,2,1)",
                  "PHI(1)"):
            with self.subTest(f=f):
                rep, reg = self.check(f, "C")
                self.assertNotIn("xll_udf", rep["code_attached"])

    def test_forecast_ets_family_is_not_reproducible_and_routes_nr(self):
        for f in ("_xlfn.FORECAST.ETS(A1,B1:B4,A1:A4)", "_xlfn.FORECAST.ETS.CONFINT(A1,B1:B4,A1:A4)",
                  "_xlfn.FORECAST.ETS.SEASONALITY(B1:B4,A1:A4)", "_xlfn.FORECAST.ETS.STAT(B1:B4,A1:A4,1)"):
            with self.subTest(f=f):
                rep, reg = self.check(f, "NR")
                self.assertTrue(any("AAA" in r and "cached" in r for r in reg["reasons"]), reg["reasons"])
                self.assertNotIn("xll_udf", rep["code_attached"])

    def test_linear_forecast_and_regression_functions_keep_their_routes(self):
        self.check("FORECAST(5,B1:B4,A1:A4)", "T")
        self.check("_xlfn.FORECAST.LINEAR(5,B1:B4,A1:A4)", "T")
        self.check("SLOPE(B1:B4,A1:A4)", "T")
        self.check("INTERCEPT(B1:B4,A1:A4)", "T")
        self.check("TREND(B1:B4,A1:A4)", "C")
        self.check("LINEST(B1:B4,A1:A4)", "C")

    def test_a_shifted_reference_off_the_sheet_becomes_ref_error(self):
        self.assertEqual(cw.expand_shared("A1+B2", -1, 0), "#REF!+B1")

    def test_a_plug_at_the_end_of_a_region_is_a_candidate(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, F("A3*2", 6)], [4, 99]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        reg = [r for r in rep["regions"] if r["cells"] == 3][0]
        self.assertIn("plug_at_end", flag_ids(reg))
        self.assertEqual(reg["plug_candidates"], ["B4"])

    def forecast_row(self, consts, fixed=None):
        n, cols = len(consts), "ABCDEFGH"
        fs = [F(fixed or f"{cols[n + i - 1]}1*1.1", i + 1) for i in range(3)]
        rep = cw.analyze(one_sheet(self.tmp, [consts + fs]))
        return [r for r in rep["regions"] if r["cells"] == 3][0]

    def test_actuals_then_a_forecast_run_is_not_a_plug(self):
        reg = self.forecast_row([100, 110, 120])
        self.assertNotIn("plug_at_end", flag_ids(reg))
        self.assertEqual(reg["plug_candidates"], [])

    def test_actuals_down_a_column_then_a_forecast_run_is_not_a_plug(self):
        rows = [[100], [110], [120], [F("A3*1.1", 1)], [F("A4*1.1", 2)], [F("A5*1.1", 3)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        reg = [r for r in rep["regions"] if r["cells"] == 3][0]
        self.assertNotIn("plug_at_end", flag_ids(reg))

    def test_a_lone_constant_before_a_run_or_constants_the_run_ignores_still_flag(self):
        self.assertIn("plug_at_end", flag_ids(self.forecast_row([120])))
        self.assertIn("plug_at_end", flag_ids(self.forecast_row([100, 110, 120], fixed="$A$9*2")))

    def run_then_constants(self, tail, vertical=True):
        if vertical:
            rows = [[i, F(f"A{i}*2", 2 * i)] for i in (1, 2, 3)] + [[None, t] for t in tail]
        else:
            rows = [[F("A2*2", 2), F("B2*2", 4), F("C2*2", 6)] + list(tail), [1, 2, 3]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        return [r for r in rep["regions"] if r["cells"] == 3][0]

    def test_a_typed_block_of_two_after_a_run_is_inputs_not_a_plug(self):
        for vertical in (True, False):
            reg = self.run_then_constants([99, 98], vertical)
            self.assertNotIn("plug_at_end", flag_ids(reg), vertical)
            self.assertEqual(reg["plug_candidates"], [], vertical)

    def test_a_lone_constant_after_a_run_still_flags(self):
        for vertical in (True, False):
            self.assertIn("plug_at_end", flag_ids(self.run_then_constants([99], vertical)), vertical)

    def test_a_block_the_run_reads_is_not_suppressed(self):
        rows = [[F("$A$4*2", 1)], [F("$A$4*2", 1)], [F("$A$4*2", 1)], [99], [98]]
        reg = [r for r in cw.analyze(one_sheet(self.tmp, rows))["regions"] if r["cells"] == 3][0]
        self.assertIn("plug_at_end", flag_ids(reg))

    def test_a_shared_child_with_no_master_is_flagged_and_asks(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("", 2, fa={"t": "shared", "si": "9"})]]))
        reg = region(rep, "S", "B1")
        self.assertIn("shared_master_missing", flag_ids(reg))
        self.assertEqual(reg["route"], "NR")

    def test_a_same_row_aggregate_copied_down_notes_the_wide_layout(self):
        rows = [[1, 2, 3, F("SUM(A1:C1)", 6)], [4, 5, 6, F("SUM(A2:C2)", 15)]]
        reg = region(cw.analyze(one_sheet(self.tmp, rows)), "S", "D1:D2")
        self.assertEqual(reg["split"], "range_aggregate")
        self.assertIn("row_range_aggregate", flag_ids(reg))


# --------------------------------------------------------------------------
# Stanza quoting, pivot sources, name cost and wide layouts
# --------------------------------------------------------------------------

BAD_HEADERS = ['a\nb', 'x"', '"x', 'a""b', 'Pipe 5"', '" ), source: evil is duckdb.sql(""select 1 --', 'q"""x']


class StanzaInjection(Tmp):
    def stanza(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))["sources"][0]

    def assertSafe(self, s, bad):
        st = s["stanza"]
        self.assertIsNotNone(st, s["stanza_refused"])
        self.assertEqual(st.count('"""'), 2, st)
        self.assertNotIn(bad, st)
        self.assertNotIn("evil", st)

    def test_a_lifted_header_with_quotes_is_aliased_never_emitted(self):
        for bad in BAD_HEADERS:
            with self.subTest(bad=bad):
                s = self.stanza([[bad, "b"], [1, 2], [3, 4]])
                self.assertSafe(s, bad)
                self.assertIn("header = false", s["stanza"])
                self.assertIn("range = 'A2:B3'", s["stanza"])
                self.assertIn('"A" AS "column1", "B" AS "b"', s["stanza"])

    def test_a_not_lifted_formula_column_with_a_quoted_header_is_safe(self):
        for bad in BAD_HEADERS:
            with self.subTest(bad=bad):
                s = self.stanza([["a", bad], [1, F("A2*2", 2)], [2, F("A3*2", 4)]])
                self.assertSafe(s, bad)

    def test_a_mixed_column_with_a_quoted_header_is_safe(self):
        for bad in BAD_HEADERS:
            with self.subTest(bad=bad):
                s = self.stanza([["a", bad], [1, 1], [2, "x"], [3, 3]])
                self.assertSafe(s, bad)

    def test_a_wide_layout_with_a_quoted_label_header_is_refused_or_safe(self):
        for bad in BAD_HEADERS:
            with self.subTest(bad=bad):
                s = self.stanza([[bad, 2021, 2022], ["Rev", 1, 2]])
                self.assertEqual(s["layout"], "wide")
                if s["stanza"] is not None:
                    self.assertSafe(s, bad)
                else:
                    self.assertTrue(s["stanza_refused"])

    def test_blank_header_cells_alias_the_column_instead_of_a_binder_error(self):
        s = self.stanza([["a", None, "c"], [1, 2, 3], [4, 5, 6]])
        self.assertIn('"B" AS "column2"', s["stanza"])
        self.assertIn("header = false", s["stanza"])

    def test_a_whitespace_only_header_is_aliased(self):
        s = self.stanza([["a", "   ", "c"], [1, 2, 3], [4, 5, 6]])
        self.assertIn('"B" AS "column2"', s["stanza"])
        self.assertNotIn('"   "', s["stanza"])

    def test_a_hostile_row_zero_with_a_quoted_header_does_not_crash(self):
        sh = Sheet("S")
        sh.data = ('<row r="0"><c r="A0" t="inlineStr"><is><t>x"</t></is></c><c r="B0" t="inlineStr"><is><t>b</t></is></c></row>'
                   '<row r="1"><c r="A1"><v>1</v></c><c r="B1"><v>2</v></c></row>'
                   '<row r="2"><c r="A2"><v>3</v></c><c r="B2"><v>4</v></c></row>')
        rep = cw.analyze(make_book(self.tmp, [sh]))
        for src in rep["sources"]:
            if src["stanza"]:
                self.assertEqual(src["stanza"].count('"""'), 2)

    def test_duplicate_headers_are_aliased_too(self):
        s = self.stanza([["a", "a"], [1, 2], [3, 4]])
        self.assertIn('"A" AS "a"', s["stanza"])
        self.assertIn('"B" AS "column2"', s["stanza"])

    def test_clean_headers_still_read_with_header_true(self):
        s = self.stanza([["k", "v"], [1, 2], [3, 4]])
        self.assertIn("header = true", s["stanza"])
        self.assertNotIn("unverified", s["stanza"])

    def test_a_stanza_that_still_contains_a_triple_quote_is_refused(self):
        stanza, why = cw.render_stanza("b.xlsx", "S", {
            "rng": "A1:A2", "header": True, "layout": "long", "lifted": ['x"""y'], "dropped": [], "not_lifted": [],
            "subtotal_rows": [], "trimmed": [], "hidden": [], "mixed": [], "header_rows": 1, "flagged": [], "alias": None})
        self.assertIsNone(stanza)
        self.assertIn("triple quote", why)


class PivotSourceMasking(Tmp):
    def masked(self, cache_source, names="", extra=()):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]], state="hidden")
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, pv] + list(extra), parts=pivot_parts(cache_source=cache_source), names=names))
        p = rep["pivots"][0]
        self.assertIsNone(p["row_fields"])
        self.assertEqual(p["calculated_fields"], [])
        self.assertNotIn("Amount*0.1", json.dumps(rep) + cw.render_text(rep))

    def test_a_sheet_scoped_defined_name_over_a_hidden_sheet(self):
        names = '<definedNames><definedName name="Src" localSheetId="1">Data!$A$1:$B$2</definedName></definedNames>'
        self.masked('<cacheSource type="worksheet"><worksheetSource name="Src"/></cacheSource>', names)

    def test_a_sheet_scoped_name_over_a_visible_sheet_stays_unmasked(self):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        names = '<definedNames><definedName name="Src" localSheetId="1">Data!$A$1:$B$2</definedName></definedNames>'
        parts = pivot_parts(cache_source='<cacheSource type="worksheet"><worksheetSource name="Src"/></cacheSource>')
        rep = cw.analyze(make_book(self.tmp, [data, pv], parts=parts, names=names))
        self.assertEqual(rep["pivots"][0]["row_fields"], ["Region"])

    def test_an_unresolvable_name_with_a_hidden_sheet_attribute(self):
        self.masked('<cacheSource type="worksheet"><worksheetSource name="Nope" sheet="Data"/></cacheSource>')

    def test_a_consolidation_source(self):
        self.masked('<cacheSource type="consolidation"><consolidation><pages count="0"/><rangeSets count="1">'
                    '<rangeSet sheet="Data" ref="A1:B2"/></rangeSets></consolidation></cacheSource>')

    def test_a_source_that_resolves_to_a_visible_sheet_is_not_masked(self):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [data, pv], parts=pivot_parts()))
        self.assertEqual(rep["pivots"][0]["row_fields"], ["Region"])


class DefinedNameCost(Tmp):
    NAMES = ('<definedNames><definedName name="NameA">NameB+1</definedName><definedName name="NameB">NameC+1</definedName>'
             '<definedName name="NameC">NameD+1</definedName><definedName name="NameD">S!$A$1*2</definedName></definedNames>')

    def test_a_name_is_analysed_once_per_scope_not_once_per_occurrence(self):
        formula = "+".join(["NameA"] * 40)
        p = one_sheet(self.tmp, [[1, F(formula, 0)]], names=self.NAMES)
        with mock.patch.object(cw, "analyze_formula", wraps=cw.analyze_formula) as m:
            cw.analyze(p)
        self.assertLess(m.call_count, 12)

    def test_the_depth_cap_asks_instead_of_staying_translatable(self):
        names = ('<definedNames><definedName name="NameA">NameB</definedName><definedName name="NameB">NameC</definedName>'
                 '<definedName name="NameC">NameD</definedName><definedName name="NameD">NameE</definedName>'
                 '<definedName name="NameE">OFFSET(S!$A$1,0,0,2,1)</definedName></definedNames>')
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("SUM(NameA)", 0)]], names=names))
        reg = region(rep, "S", "B1")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("named_formula_depth", flag_ids(reg))

    def test_a_leaf_name_with_many_refs_is_not_multiplied_through_the_chain(self):
        n = 60
        for kind, leaf in (("absolute", "+".join(f"S!$A${i}" for i in range(1, n + 1))),
                           ("relative", "+".join(f"S!A{i}" for i in range(1, n + 1)))):
            with self.subTest(kind=kind):
                names = ('<definedNames><definedName name="Top">' + "+".join(["Mid"] * n) + '</definedName>'
                         '<definedName name="Mid">' + "+".join(["Leaf"] * n) + '</definedName>'
                         f'<definedName name="Leaf">{leaf}</definedName></definedNames>')
                p = one_sheet(self.tmp, [[1, F("Top", 0)]], names=names)
                merged = []
                real = cw.merge_analysis
                def spy(a, sub):
                    merged.append(len(sub.refs))
                    return real(a, sub)
                with mock.patch.object(cw, "merge_analysis", spy):
                    cw.analyze(p)
                self.assertLess(sum(merged), 20000)

    def test_a_wide_name_chain_finishes_in_bounded_time(self):
        import time
        n = 120
        names = ('<definedNames><definedName name="Top">' + "+".join(["Mid"] * n) + '</definedName>'
                 '<definedName name="Mid">' + "+".join(["Leaf"] * n) + '</definedName>'
                 '<definedName name="Leaf">' + "+".join(f"S!$A${i}" for i in range(1, n + 1)) + '</definedName></definedNames>')
        p = one_sheet(self.tmp, [[1, F("Top", 0)]], names=names)
        t0 = time.time()
        cw.analyze(p)
        self.assertLess(time.time() - t0, 5.0)

    def test_a_relative_name_is_reanalysed_at_each_cell_position(self):
        names = '<definedNames><definedName name="Rel">S!A1*2</definedName></definedNames>'
        p = one_sheet(self.tmp, [[1, F("Rel+1", 0)], [2, F("Rel+2", 0)]], names=names)
        texts = []
        real = cw.analyze_formula
        def spy(text, *a, **k):
            texts.append(text)
            return real(text, *a, **k)
        with mock.patch.object(cw, "analyze_formula", spy):
            cw.analyze(p)
        self.assertEqual(texts.count("S!A1*2"), 2)

    def test_a_range_end_that_is_a_name_is_opaque(self):
        names = '<definedNames><definedName name="MyEnd">S!$A$3</definedName></definedNames>'
        rep = cw.analyze(one_sheet(self.tmp, [[1], [2], [3], [F("SUM(A1:MyEnd)", 0)]], names=names))
        reg = region(rep, "S", "A4")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("opaque_dependency", flag_ids(reg))


class WideLayoutNeedsPeriodHeaders(Tmp):
    def src(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))["sources"][0]

    def test_a_label_and_two_input_columns_is_data_not_a_wide_header(self):
        s = self.src([["East", 100, 200], ["West", 150, 250], ["North", 1, 2]])
        self.assertEqual(s["layout"], "long")
        self.assertFalse(s["header"])
        self.assertTrue(s["wide_ambiguous"])
        self.assertIn("range = 'A1:C3'", s["stanza"])
        self.assertIn("ambiguous", s["stanza"])
        self.assertNotIn("UNPIVOT", s["stanza"])

    def test_year_serial_and_sequential_headers_are_periods(self):
        for hdr in ([2021, 2022, 2023], [44927, 44958, 44986], [1, 2, 3, 4]):
            with self.subTest(hdr=hdr):
                s = self.src([["Account"] + hdr, ["Rev"] + [1] * len(hdr), ["Cost"] + [2] * len(hdr)])
                self.assertEqual(s["layout"], "wide")
                self.assertEqual(s["period_columns"], len(hdr))

    def test_unordered_integers_are_not_periods(self):
        s = self.src([["Account", 7, 3, 9], ["Rev", 1, 1, 1], ["Cost", 2, 2, 2]])
        self.assertEqual(s["layout"], "long")

    def test_period_headers_built_by_formula_are_not_lifted_as_data(self):
        rows = [["Account", 2021, F("B1+1", 2022), F("C1+1", 2023)], ["Rev", 1, 2, 3], ["Cost", 4, 5, 6]]
        s = self.src(rows)
        self.assertEqual(s["layout"], "wide")
        self.assertTrue(s["header"])
        self.assertEqual(s["period_columns"], 3)
        self.assertIn("UNPIVOT", s["stanza"])

    def test_a_header_row_of_only_formulas_after_a_label_is_not_wide(self):
        for rows in ([["Total", F("SUM(A2:A3)", 3), F("SUM(B2:B3)", 3)], ["x", 1, 2], ["y", 2, 1]],
                     [["Account", F("YEAR(TODAY())", 2024), F("B1+1", 2025)], ["Rev", 1, 2]]):
            s = self.src(rows)
            self.assertEqual(s["layout"], "long")
            self.assertTrue(s["wide_ambiguous"])

    def test_two_values_in_a_run_are_not_enough_for_the_consecutive_integer_rule(self):
        s = self.src([["East", 1, 2], ["West", 3, 4]])
        self.assertEqual(s["layout"], "long")
        self.assertTrue(s["wide_ambiguous"])

    def test_descending_years_are_periods(self):
        s = self.src([["Account", 2023, 2022, 2021], ["Rev", 1, 2, 3]])
        self.assertEqual(s["layout"], "wide")

    def test_non_finite_and_huge_header_values_are_not_periods_and_do_not_raise(self):
        for raw in ("1e999", "-1e999", "nan", "inf", "-inf", "9" * 400):
            with self.subTest(raw=raw):
                sh = Sheet("S")
                sh.data = ('<row r="1"><c r="A1" t="inlineStr"><is><t>Account</t></is></c><c r="B1"><v>%s</v></c><c r="C1"><v>2022</v></c></row>'
                           '<row r="2"><c r="A2" t="inlineStr"><is><t>Rev</t></is></c><c r="B2"><v>1</v></c><c r="C2"><v>2</v></c></row>' % raw)
                src = cw.analyze(make_book(self.tmp, [sh]))["sources"][0]
                self.assertEqual(src["layout"], "long")
        for v in (float("inf"), float("-inf"), float("nan"), 1e300):
            self.assertFalse(cw.period_like([v, v]))
            self.assertFalse(cw.period_like([v]))

    def test_a_missing_numeric_header_value_is_ambiguous_never_wide(self):
        with mock.patch.object(cw, "MAX_NUMVALS", 1):
            s = self.src([["Account", 2021, 2022, 2023], ["Rev", 1, 2, 3]])
        self.assertEqual(s["layout"], "long")
        self.assertTrue(s["wide_ambiguous"])

    def test_period_like_rules(self):
        pl = cw.period_like
        self.assertFalse(pl([]))
        self.assertTrue(pl([2021]))
        self.assertTrue(pl([2023, 2022, 2021]))
        self.assertTrue(pl([44927, 44958]))
        self.assertTrue(pl([44986, 44958, 44927]))
        self.assertTrue(pl([1, 2, 3]))
        self.assertTrue(pl([3, 2, 1]))
        self.assertFalse(pl([1, 2]))
        self.assertFalse(pl([7, 3, 9]))
        self.assertFalse(pl([2021, 2021]))
        self.assertFalse(pl([1.5, 2.5, 3.5]))
        self.assertFalse(pl([100, 200, 300]))

    def test_cells_without_r_attributes_are_read_by_position(self):
        sh = Sheet("S")
        sh.data = ('<row r="1"><c t="inlineStr"><is><t>East</t></is></c><c><v>100</v></c><c><v>200</v></c></row>'
                   '<row r="2"><c t="inlineStr"><is><t>West</t></is></c><c><v>150</v></c><c><v>250</v></c></row>'
                   '<row r="3"><c t="inlineStr"><is><t>North</t></is></c><c><v>1</v></c><c><v>2</v></c></row>')
        s = cw.analyze(make_book(self.tmp, [sh]))["sources"][0]
        self.assertEqual(s["layout"], "long")
        self.assertFalse(s["header"])
        self.assertIn("range = 'A1:C3'", s["stanza"])
        self.assertTrue(s["wide_ambiguous"])
        sh.data = ('<row r="1"><c t="inlineStr"><is><t>Account</t></is></c><c><v>2021</v></c><c><v>2022</v></c></row>'
                   '<row r="2"><c t="inlineStr"><is><t>Rev</t></is></c><c><v>1</v></c><c><v>2</v></c></row>')
        s = cw.analyze(make_book(self.tmp, [sh]))["sources"][0]
        self.assertEqual(s["layout"], "wide")

    def test_the_unpivot_and_letter_naming_carry_no_unverified_caveat(self):
        s = self.src([["Account", 2021, 2022], ["Rev", 1, 2]])
        self.assertNotIn("unverified", s["stanza"])
        s = self.src([[1, 2], [3, 4]])
        self.assertNotIn("verify", s["stanza"])


class MashupRegexIsLinear(Tmp):
    def test_a_long_run_of_blank_lines_does_not_stall_the_shared_scan(self):
        import time
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("Formulas/Section1.m", "section Section1;\n" + " \n" * 40000 + "x\n")
        body = inner.getvalue()
        blob = struct.pack("<I", 0) + struct.pack("<I", len(body)) + body + struct.pack("<I", 0)
        xml = ('<?xml version="1.0" encoding="utf-16"?><DataMashup xmlns="http://schemas.microsoft.com/DataMashup">'
               + base64.b64encode(blob).decode() + "</DataMashup>")
        p = make_book(self.tmp, [Sheet("S", [[1]])], parts={"customXml/item1.xml": xml.encode("utf-16")})
        t0 = time.time()
        cw.analyze(p)
        self.assertLess(time.time() - t0, 2.0)


# --------------------------------------------------------------------------
# Plug versus separate blocks, lookup keys, sheet classes, cycles, serial 60 and wide formula layouts
# --------------------------------------------------------------------------

FIXTURE = pathlib.Path(__file__).resolve().parent.parent / "fixtures" / "fixture.xlsx"
STYLES_DATE = (f'<styleSheet xmlns="{MAIN}"><numFmts count="1"><numFmt numFmtId="164" formatCode="yyyy\\-mm\\-dd"/></numFmts>'
               '<cellXfs count="3"><xf numFmtId="0"/><xf numFmtId="164" applyNumberFormat="1"/><xf numFmtId="14" applyNumberFormat="1"/></cellXfs></styleSheet>')


class FixtureDefects(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rep = cw.analyze(str(FIXTURE))
        cls.text = cw.render_text(cls.rep)

    def regions(self, sheet):
        return [r for r in self.rep["regions"] if r["sheet"] == sheet]

    def test_ledger_subtotal_rows_are_separate_regions_and_not_plugs(self):
        regs = self.regions("Ledger")
        self.assertEqual(sorted(r["ref"] for r in regs), ["C10:D10", "C6:D6"])
        for r in regs:
            self.assertNotIn("plug", flag_ids(r))
            self.assertEqual(r["plugs"], [])

    def test_a_real_plug_in_the_fixture_is_still_flagged(self):
        self.assertIn("plug", flag_ids(region(self.rep, "Report", "I2:I9")))

    def test_literal_lookup_keys_are_hardcoded_constants(self):
        keyed = [r for r in self.regions("Lookup") if r["example"] and "(750" in r["example"].replace(" ", "")]
        self.assertTrue(keyed)
        for r in keyed:
            self.assertIn("hardcoded_constant", flag_ids(r), r["example"])

    def test_sheet_classes_over_the_fixture(self):
        got = {s["name"]: s["class"] for s in self.rep["sheets"]}
        got.pop("Pivot", None)  # an Excel save of the fixture adds a Pivot sheet
        self.assertEqual(got, {"Data": "data", "Ledger": "data", "Lookup": "lookup", "Report": "report", "Assumptions": "input",
                               "Forecast": "calc", "MonteCarlo": "calc", "_Config": "config"})

    def test_one_circularity_is_one_cycle_with_its_loop_count(self):
        cycles = self.rep["graph"]["cycles"]
        self.assertEqual(len(cycles), 1)
        ids = {r["ref"]: r["id"] for r in self.regions("Forecast")}
        self.assertEqual(cycles[0]["regions"], sorted([ids["B4:F4"], ids["B5:F5"]], key=lambda s: int(s[1:])))
        self.assertEqual((cycles[0]["cells"], cycles[0]["loops"]), (10, 5))
        self.assertEqual(sum(1 for line in self.text.splitlines() if line.startswith("- cycle:")), 1)

    def test_a_function_route_is_the_worst_over_its_uses(self):
        v = self.rep["functions"]["VLOOKUP"]
        self.assertEqual(v["route"], "C")
        self.assertEqual(v["routes"], {"C": 3, "T": 3})
        self.assertEqual(self.rep["functions"]["SUMIFS"]["route"], "T")
        row = [x for x in self.text.splitlines() if x.startswith("| VLOOKUP |")][0]
        self.assertIn("C", row)
        self.assertIn("T 3", row)

    def test_serial_60_is_flagged_by_value_and_the_leap_year_bug_is_noted(self):
        d = self.rep["date_system"]
        self.assertEqual(d["serial_60"], ["Data!D10"])
        self.assertTrue(d["pre_61_dates"])
        self.assertIn("date_serial_60", {t["tell"] for t in self.rep["oracle"]["tells"]})
        self.assertIn("Data!D10", self.text.split("## Sources")[0])
        self.assertIn("1900", self.text.split("## Sources")[0])

    def test_a_formula_derived_wide_block_is_reported_but_has_no_stanza(self):
        lay = [x for x in self.rep["layouts"] if x["sheet"] == "Forecast"]
        self.assertEqual(len(lay), 1)
        self.assertEqual((lay[0]["layout"], lay[0]["period_columns"], lay[0]["label_column"], lay[0]["header_ref"]),
                         ("wide", 5, "A", "B1:F1"))
        self.assertEqual([(s["data_ref"], s["formula_cells"]) for s in self.rep["sources"] if s["sheet"] == "Forecast"],
                         [("A1:F1", 0), ("A2:A9", 0)])
        sect = self.text.split("### Forecast")[1].split("##")[0]
        self.assertIn("wide layout", sect)
        self.assertIn("formula-derived", sect)
        self.assertIn("formula output is never lifted", sect)


class PlugVersusSeparateBlocks(Tmp):
    def test_two_copies_of_one_formula_around_a_constant_block_are_two_regions(self):
        rows = [[1], [2], [F("SUM(A1:A2)", 3)], [3], [4], [F("SUM(A4:A5)", 7)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        regs = [r for r in rep["regions"] if r["sheet"] == "S"]
        self.assertEqual(sorted(r["ref"] for r in regs), ["A3", "A6"])
        for r in regs:
            self.assertNotIn("plug", flag_ids(r))

    def test_a_constant_inside_a_run_of_one_formula_is_still_a_plug(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, 99], [4, F("A4*2", 8)], [5, F("A5*2", 10)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        reg = region(rep, "S", "B1:B5")
        self.assertEqual(reg["plugs"], ["B3"])
        self.assertIn("plug", flag_ids(reg))


class LiteralLookupKeys(Tmp):
    check = Routes.check

    def test_any_numeric_literal_key_is_a_hardcoded_constant(self):
        for f, route in (("VLOOKUP(750,A1:B4,2,FALSE)", "T"), ("VLOOKUP(-5,A1:B4,2,FALSE)", "T"), ("MATCH(750,A1:A4,0)", "T"),
                         ("HLOOKUP(2,A1:B4,2,FALSE)", "T"), ("_xlfn.XLOOKUP(5,A1:A4,B1:B4)", "T"), ("VLOOKUP(1,A1:B4,2,FALSE)", "T")):
            with self.subTest(f=f):
                _, reg = self.check(f, route, ["hardcoded_constant"])
                detail = [x for x in reg["flags"] if x["id"] == "hardcoded_constant"][0]["detail"]
                self.assertEqual(detail.count(";"), 0, detail)

    def test_a_cell_or_text_key_is_not(self):
        self.check("VLOOKUP(A1,A1:B4,2,FALSE)", "T", [], ["hardcoded_constant"])
        self.check('VLOOKUP("x",A1:B4,2,FALSE)', "T", [], ["hardcoded_constant"])

    def test_a_literal_in_a_later_argument_is_not_a_key(self):
        self.check("INDEX(A1:A4,3)", "T", [], ["hardcoded_constant"])
        self.check("VLOOKUP(A1,A1:B4,2,FALSE)", "T", [], ["hardcoded_constant"])


class SheetClassSignals(Tmp):
    def classes(self, sheets):
        rep = cw.analyze(make_book(self.tmp, sheets))
        return {s["name"]: s["class"] for s in rep["sheets"]}

    def test_a_small_table_read_by_vlookup_from_another_sheet_is_a_lookup(self):
        keys = Sheet("K", [[1, "a"], [2, "b"], [3, "c"], [4, "d"]])
        use = Sheet("U", [[F("VLOOKUP(2,K!A1:B4,2,FALSE)", "b")], [F("VLOOKUP(3,K!A1:B4,2,FALSE)", "c")]])
        self.assertEqual(self.classes([keys, use])["K"], "lookup")

    def test_a_sheet_of_formulas_that_aggregate_other_sheets_is_a_report(self):
        data = Sheet("D", [["k", "v"]] + [[f"a{i}", i] for i in range(20)])
        rows = [["Total", F("SUM(D!B2:B21)", 190)], ["Count", F("COUNT(D!B2:B21)", 20)], ["Max", F("MAX(D!B2:B21)", 19)],
                ["Min", F("MIN(D!B2:B21)", 0)], ["Note", "see data"], ["Owner", "me"], ["Date", "today"]]
        self.assertEqual(self.classes([data, Sheet("R", rows)])["R"], "report")

    def test_a_few_constants_read_absolutely_with_a_stray_formula_are_inputs(self):
        inp = Sheet("In", [["rate", 0.05], ["term", 5], ["fee", 10], ["total", F("B1*B2", 0.25)]])
        use = Sheet("U", [[F("In!$B$1*2", 0.1)], [F("In!$B$2*2", 10)], [F("In!$B$3+1", 11)]])
        self.assertEqual(self.classes([inp, use])["In"], "input")

    def test_one_relative_cross_sheet_input_row_plus_a_roll_forward_is_a_calc(self):
        inp = Sheet("In", [[0.1, 0.2, 0.3, 0.4]])
        rows = [[100, F("A3", 0), F("B3", 0), F("C3", 0)],
                [F("In!A1", 0.1), F("In!B1", 0.2), F("In!C1", 0.3), F("In!D1", 0.4)],
                [F("A1*(1+A2)", 110), F("B1*(1+B2)", 0), F("C1*(1+C2)", 0), F("D1*(1+D2)", 0)]]
        self.assertEqual(self.classes([inp, Sheet("F", rows)])["F"], "calc")

    def test_a_self_chained_sheet_just_under_half_formulas_is_a_calc(self):
        rows = [[i, F(f"A{i}*2", i * 2), F(f"B{i}+1", i * 2 + 1)] for i in range(1, 5)] + [["x", "y", "z"], ["p", "q", "r"]]
        self.assertEqual(self.classes([Sheet("C", rows)])["C"], "calc")


class CycleDedupe(Tmp):
    def test_one_circularity_across_many_rows_is_one_entry(self):
        rows = [[F(f"B{i}+1", 0), F(f"A{i}+1", 0)] for i in range(1, 4)]
        g = cw.analyze(one_sheet(self.tmp, rows))["graph"]
        self.assertEqual(len(g["cycles"]), 1)
        self.assertEqual((g["cycles"][0]["cells"], g["cycles"][0]["loops"]), (6, 3))

    def test_two_different_circularities_stay_two_entries(self):
        rows = [[F("B1+1", 0), F("A1+1", 0), None, F("E1+1", 0), F("D1+1", 0)]]
        g = cw.analyze(one_sheet(self.tmp, rows))["graph"]
        self.assertEqual(len(g["cycles"]), 2)


class FunctionRouteIsWorst(Tmp):
    def test_the_route_of_a_function_is_the_most_restrictive_over_its_regions(self):
        rows = [[1, "x", F("VLOOKUP(A1,A1:B2,2,FALSE)", "x"), F("VLOOKUP(A1,A1:B2,2)", "x")], [2, "y"]]
        f = cw.analyze(one_sheet(self.tmp, rows))["functions"]["VLOOKUP"]
        self.assertEqual(f["route"], "C")
        self.assertEqual(f["routes"], {"T": 1, "C": 1})

    def test_a_function_used_only_one_way_reports_just_that(self):
        f = cw.analyze(one_sheet(self.tmp, [[1, F("SUM(A1:A1)", 1)]]))["functions"]["SUM"]
        self.assertEqual((f["route"], f["routes"]), ("T", {"T": 1}))


class SerialSixtyByValue(Tmp):
    def book(self, cells, date1904=False):
        sh = Sheet("S")
        sh.data = "".join(f'<row r="{i}">{c}</row>' for i, c in enumerate(cells, 1))
        wb_pr = '<workbookPr date1904="1"/>' if date1904 else ""
        return make_book(self.tmp, [sh], parts={"xl/styles.xml": STYLES_DATE}, wb_pr=wb_pr)

    def test_a_date_styled_serial_60_is_flagged_and_noted(self):
        rep = cw.analyze(self.book(['<c r="A1" s="1"><v>60</v></c>', '<c r="A2" s="2"><v>45000</v></c>']))
        self.assertEqual(rep["date_system"]["serial_60"], ["S!A1"])
        self.assertEqual(rep["date_system"]["pre_61_dates"], 1)
        self.assertIn("date_serial_60", {t["tell"] for t in rep["oracle"]["tells"]})

    def test_a_plain_number_60_is_not_a_date(self):
        rep = cw.analyze(self.book(['<c r="A1"><v>60</v></c>', '<c r="A2" s="1"><v>45000</v></c>']))
        self.assertEqual(rep["date_system"]["serial_60"], [])
        self.assertEqual(rep["date_system"]["pre_61_dates"], 0)
        self.assertNotIn("date_serial_60", {t["tell"] for t in rep["oracle"]["tells"]})

    def test_a_small_date_serial_notes_the_leap_year_bug_without_being_60(self):
        rep = cw.analyze(self.book(['<c r="A1" s="2"><v>30</v></c>']))
        self.assertEqual(rep["date_system"]["serial_60"], [])
        self.assertEqual(rep["date_system"]["pre_61_dates"], 1)

    def test_nothing_is_flagged_in_a_1904_workbook(self):
        rep = cw.analyze(self.book(['<c r="A1" s="1"><v>60</v></c>'], date1904=True))
        self.assertEqual(rep["date_system"]["serial_60"], [])


class FormulaDerivedWideLayout(Tmp):
    def test_a_wide_block_of_formulas_is_reported_not_lifted(self):
        rows = [["Account", 2021, 2022, 2023], ["Rev", F("Z1*1", 1), F("Z2*1", 2), F("Z3*1", 3)],
                ["Cost", F("Z1*2", 1), F("Z2*2", 2), F("Z3*2", 3)], ["Net", F("Z1*3", 1), F("Z2*3", 2), F("Z3*3", 3)]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual([(s["data_ref"], s["formula_cells"]) for s in rep["sources"]], [("A1:D1", 0), ("A2:A4", 0)])
        self.assertEqual([(x["sheet"], x["layout"], x["period_columns"], x["label_column"], x["header_ref"]) for x in rep["layouts"]],
                         [("S", "wide", 3, "A", "B1:D1")])

    def test_an_ordinary_formula_block_without_period_headers_is_not_a_layout(self):
        rows = [["k", "v", "w"], ["a", F("Z1*1", 1), F("Z2*1", 2)], ["b", F("Z1*2", 1), F("Z2*2", 2)]]
        self.assertEqual(cw.analyze(one_sheet(self.tmp, rows))["layouts"], [])


# --------------------------------------------------------------------------
# Lifted-stanza names, text cells, merged headers, subtotal predicates and single-cell splits
# --------------------------------------------------------------------------

class StanzaExecutionDefects(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rep = cw.analyze(str(FIXTURE))

    def src(self, sheet, ref=None):
        return [s for s in self.rep["sources"] if s["sheet"] == sheet and (ref is None or s["ref"] == ref)][0]

    def test_unpivot_names_do_not_collide_with_the_period_label_column(self):
        st = self.src("Assumptions", "A10:F11")["stanza"]
        self.assertNotIn("FOR period IN", st)
        self.assertIn("FOR period_ IN", st)

    def test_the_data_table_names_the_text_cells(self):
        s = self.src("Data")
        self.assertEqual([(t["cell"], t["column"], t["in"]) for t in s["text_cells"]],
                         [("C5", "Qty", "numeric"), ("D8", "OrderDate", "date")])
        self.assertEqual(s["text_cell_count"], 2)
        self.assertIn("C5", s["stanza"])
        self.assertIn("D8", s["stanza"])
        self.assertIn("all_varchar = true", s["stanza"])
        self.assertIn("TRY_CAST", s["stanza"])
        self.assertNotIn("ignore_errors", s["stanza"])

    def test_the_ledger_stanza_is_accurate(self):
        s = self.src("Ledger")
        self.assertEqual(s["data_ref"], "A3:D9")
        st = s["stanza"]
        self.assertIn("range = 'A3:D9', header = false", st)
        self.assertIn('"A" AS "Group", "B" AS "Entry", "C" AS "Debit", "D" AS "Credit"', st)
        self.assertNotIn("starts at the column-name row", st)
        self.assertIn("first data row", st)
        self.assertIn("""WHERE COALESCE("A", '') NOT ILIKE '%subtotal%'""", st)
        self.assertNotIn("WHERE <", st)

    def test_a_one_cell_formula_is_a_single_cell_not_row_local(self):
        self.assertEqual(region(self.rep, "Report", "B13")["split"], "single_cell")
        self.assertEqual(region(self.rep, "Report", "I2:I9")["split"], "row_local")


class WideNamesAndTextCells(Tmp):
    def src(self, rows, **kw):
        return cw.analyze(one_sheet(self.tmp, rows, **kw))["sources"][0]

    def test_a_value_column_label_is_not_reused_case_insensitively(self):
        s = self.src([["Amount", 2021, 2022], ["Rev", 1, 2]])
        self.assertIn("UNPIVOT (amount_ FOR period IN", s["stanza"])

    def test_an_unambiguous_wide_block_keeps_the_plain_names(self):
        s = self.src([["Account", 2021, 2022], ["Rev", 1, 2]])
        self.assertIn("UNPIVOT (amount FOR period IN", s["stanza"])

    def test_text_cells_are_capped_at_ten_with_a_count(self):
        rows = [["k", "v"]] + [["a", "x"] if i % 2 else ["a", i] for i in range(1, 31)]
        s = self.src(rows)
        self.assertEqual(len(s["text_cells"]), 10)
        self.assertEqual(s["text_cell_count"], 15)
        self.assertIn("+5 more", s["stanza"])

    def test_a_clean_column_has_no_text_cells(self):
        s = self.src([["k", "v"], ["a", 1], ["b", 2], ["c", 3]])
        self.assertEqual((s["text_cells"], s["text_cell_count"]), ([], 0))
        self.assertNotIn("ignore_errors", s["stanza"])

    def test_text_in_a_date_styled_column_is_reported_as_a_date_problem(self):
        sh = Sheet("S")
        sh.data = ('<row r="1"><c r="A1" t="inlineStr"><is><t>d</t></is></c></row>'
                   '<row r="2"><c r="A2" s="1"><v>45000</v></c></row>'
                   '<row r="3"><c r="A3" s="1"><v>45001</v></c></row>'
                   '<row r="4"><c r="A4" t="inlineStr"><is><t>2024-07-15</t></is></c></row>')
        s = cw.analyze(make_book(self.tmp, [sh], parts={"xl/styles.xml": STYLES_DATE}))["sources"][0]
        self.assertEqual([(t["cell"], t["in"]) for t in s["text_cells"]], [("A4", "date")])


class SubtotalPredicate(Tmp):
    def src(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))["sources"][0]

    def test_a_label_the_subtotal_rows_share_becomes_the_predicate(self):
        s = self.src([["k", "v"], ["a", 1], ["b", 2], ["Subtotal", F("SUM(B2:B3)", 3)], ["c", 3], ["d", 4]])
        self.assertIn("""WHERE COALESCE("k", '') NOT ILIKE '%subtotal%'""", s["stanza"])

    def test_a_data_row_that_also_matches_falls_back_to_row_position(self):
        s = self.src([["k", "v"], ["a", 1], ["Subtotals r us", 2], ["Subtotal", F("SUM(B2:B3)", 3)], ["c", 3], ["d", 4]])
        self.assertIn("WHERE __r NOT IN (4)", s["stanza"])

    def test_a_subtotal_row_with_no_label_is_excluded_by_row_position(self):
        s = self.src([["k", "v"], ["a", 1], ["b", 2], [None, F("SUM(B2:B3)", 3)], ["c", 3], ["d", 4]])
        self.assertIn("WHERE __r NOT IN (4)", s["stanza"])


class SingleCellSplit(Tmp):
    def test_one_cell_is_single_cell_and_a_copy_down_is_row_local(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*1.08", 1.08), F("A1*2", 2)], [2, None, F("A2*2", 4)]]))
        self.assertEqual(region(rep, "S", "B1")["split"], "single_cell")
        self.assertEqual(region(rep, "S", "C1:C2")["split"], "row_local")


class PivotDateGroups(Tmp):
    """Hidden items on a date-grouped field index the cache's group items, not its raw shared items."""
    GROUP = ('<cacheField name="Date" numFmtId="14"><sharedItems containsSemiMixedTypes="0" containsDate="1" containsString="0"/>'
             '<fieldGroup base="3"><rangePr groupBy="months" startDate="2020-01-01T00:00:00" endDate="2020-12-31T00:00:00"/>'
             '<groupItems count="{n}">{items}</groupItems></fieldGroup></cacheField>')
    LABELS = ["&lt;1/1/2020", "Jan", "Feb", "&gt;12/31/2020"]

    def pivot(self, hide=(1,), labels=LABELS, slicer=None, items=4, blank_region=False):
        group = self.GROUP.format(n=len(labels), items="".join(f'<s v="{x}"/>' for x in labels))
        parts = pivot_parts(page=False, extra_cache_fields=group)
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        date_items = "".join(f'<item h="1" x="{i}"/>' if i in hide else f'<item x="{i}"/>' for i in range(items))
        pv = pv.replace('<pivotField axis="axisPage"/>', f'<pivotField axis="axisPage"/><pivotField axis="axisRow"><items count="{items}">{date_items}</items></pivotField>')
        parts["xl/pivotTables/pivotTable1.xml"] = pv.replace('<pivotFields count="3">', '<pivotFields count="4">')
        if blank_region:
            cache = parts["xl/pivotCache/pivotCacheDefinition1.xml"]
            parts["xl/pivotCache/pivotCacheDefinition1.xml"] = cache.replace(
                '<cacheField name="Region" numFmtId="0"><sharedItems/></cacheField>',
                '<cacheField name="Region" numFmtId="0"><sharedItems count="2"><s v="N"/><m/></sharedItems></cacheField>')
            parts["xl/pivotTables/pivotTable1.xml"] = parts["xl/pivotTables/pivotTable1.xml"].replace(
                '<pivotField axis="axisRow"/>', '<pivotField axis="axisRow"><items count="3"><item x="0"/><item h="1" x="1"/><item t="default"/></items></pivotField>', 1)
        if slicer is not None:
            parts["xl/slicerCaches/slicerCache1.xml"] = slicer
        data = Sheet("Data", [["Region", "Amount"], ["N", 1]])
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        self.book_path = make_book(self.tmp, [data, sh], parts=parts)
        return cw.analyze(self.book_path)["pivots"][0]

    def slicer(self, n, source="Date"):
        return PivotNonAxisFilters.slicer(self, [(i, True) for i in range(n)], source=source)

    def test_a_hidden_group_item_prints_its_group_label(self):
        p = self.pivot()
        self.assertEqual([h for h in p["hidden_items"] if h["field"] == "Date"], [{"field": "Date", "hidden": ["Jan"], "count": 1}])

    def test_a_group_item_with_no_label_stays_opaque_and_the_slicer_status_is_unresolved(self):
        p = self.pivot(hide=(2,), labels=["a", "b"], slicer=self.slicer(4))
        self.assertEqual([h["hidden"] for h in p["hidden_items"] if h["field"] == "Date"], [["item 2"]])
        self.assertEqual(p["slicer_status"], "unresolved")

    def test_an_all_selected_slicer_never_hides_a_hidden_group_item(self):
        p = self.pivot(slicer=self.slicer(4))
        self.assertEqual(p["slicer_status"], "unresolved")
        self.assertTrue(p["slicer_filter_unresolved"])

    def test_a_group_slicer_with_nothing_hidden_is_still_all_selected(self):
        self.assertEqual(self.pivot(hide=(), slicer=self.slicer(4))["slicer_status"], "all_selected")

    def test_a_hidden_blank_item_prints_as_blank(self):
        p = self.pivot(hide=(), blank_region=True)
        self.assertEqual([h["hidden"] for h in p["hidden_items"] if h["field"] == "Region"], [["(blank)"]])


class PivotNumericGroups(Tmp):
    """A numeric field grouped into ranges carries its start, end and interval from the cache's rangePr."""
    GROUP = ('<cacheField name="Age" numFmtId="0"><sharedItems containsSemiMixedTypes="0" containsString="0" containsNumber="1" minValue="3" maxValue="47"/>'
             '<fieldGroup base="3"><rangePr startNum="0" endNum="50" groupInterval="10"/>'
             '<groupItems count="7"><s v="&lt;0"/><s v="0-9"/><s v="10-19"/><s v="20-29"/><s v="30-39"/><s v="40-49"/><s v="&gt;50"/></groupItems></fieldGroup></cacheField>')

    def pivot(self, state=None, on_axis=False):
        parts = pivot_parts(page=False, extra_cache_fields=self.GROUP)
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        if on_axis:
            pv = pv.replace('<pivotField axis="axisPage"/>', '<pivotField axis="axisPage"/><pivotField axis="axisCol"/>')
            pv = pv.replace('<pivotFields count="3">', '<pivotFields count="4">')
        parts["xl/pivotTables/pivotTable1.xml"] = pv
        sh = Sheet("P", [[1]], state=state, rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [Sheet("Data", [["Region", "Amount"], ["N", 1]]), sh], parts=parts))

    def test_a_numeric_group_reports_start_end_and_interval_and_prints_them(self):
        rep = self.pivot(on_axis=True)
        self.assertEqual(rep["pivots"][0]["numeric_grouping"],
                         [{"field": "Age", "group": {"kind": "numeric", "start": 0.0, "end": 50.0, "interval": 10.0}, "grouped_on_axis": True}])
        self.assertEqual(rep["pivots"][0]["date_grouping"], [])
        self.assertIn("numeric grouping on Age: 0 to 50 in steps of 10, placed on an axis", cw.render_text(rep))

    def test_a_masked_pivot_reports_no_numeric_group(self):
        self.assertEqual(self.pivot(state="hidden")["pivots"][0]["numeric_grouping"], [])


class PivotValueFilters(Tmp):
    """A Top-N or label filter on an axis field changes which items the pivot lists, so it must reach the translator."""

    def pivot(self, filters, state=None):
        parts = pivot_parts(page=False, pivot_extra=f'<filters count="1">{filters}</filters>')
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        parts["xl/pivotTables/pivotTable1.xml"] = pv.replace('<pivotField axis="axisRow"/>', '<pivotField axis="axisRow" measureFilter="1"/>')
        data = Sheet("Data", [["Region", "Amount"], ["N", 1]], state=state)
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        self.book_path = make_book(self.tmp, [data, sh], parts=parts)
        return cw.analyze(self.book_path)["pivots"][0]

    @staticmethod
    def top10(attrs, typ="count", fld=0, measure=0):
        return (f'<filter fld="{fld}" type="{typ}" evalOrder="-1" id="1" iMeasureFld="{measure}"><autoFilter ref="A1">'
                f'<filterColumn colId="0"><top10 {attrs}/></filterColumn></autoFilter></filter>')

    def test_a_top_n_count_filter_is_reported_with_its_measure(self):
        p = self.pivot(self.top10('val="5" filterVal="5"'))
        self.assertEqual(p["value_filters"], [{"field": "Region", "kind": "top_n", "n": 5, "measure": "Sum of Amount", "direction": "top"}])

    def test_top_zero_is_the_bottom_and_percent_and_sum_have_their_own_kinds(self):
        self.assertEqual(self.pivot(self.top10('top="0" val="3" filterVal="3"'))["value_filters"][0]["kind"], "bottom_n")
        f = self.pivot(self.top10('percent="1" val="10" filterVal="10"', typ="percent"))["value_filters"][0]
        self.assertEqual((f["kind"], f["n"], f["direction"]), ("top_percent", 10, "top"))
        f = self.pivot(self.top10('val="1000" filterVal="1000"', typ="sum"))["value_filters"][0]
        self.assertEqual((f["kind"], f["n"]), ("top_sum", 1000))

    def test_a_label_or_date_filter_is_reported_as_unsupported_not_dropped(self):
        label = ('<filter fld="0" type="captionEqual" evalOrder="-1" id="1"><autoFilter ref="A1"><filterColumn colId="0">'
                 '<customFilters><customFilter val="N"/></customFilters></filterColumn></autoFilter></filter>')
        self.assertEqual(self.pivot(label)["value_filters"], [{"field": "Region", "kind": "unsupported", "type": "captionEqual"}])

    def test_the_text_report_states_the_grand_total_rule_and_the_malloy_shape(self):
        self.pivot(self.top10('val="5" filterVal="5"'))
        text = cw.render_text(cw.analyze(self.book_path))
        self.assertIn("Grand Total covers only the visible items", text)
        self.assertIn("order_by: Sum of Amount desc", text)
        self.assertIn("limit: 5", text)

    def test_a_pivot_without_value_filters_prints_no_such_line(self):
        self.assertEqual(self.pivot("")["value_filters"], [])
        self.assertNotIn("Grand Total covers only", cw.render_text(cw.analyze(self.book_path)))

    def test_a_hidden_sheet_pivot_reports_no_value_filters(self):
        self.assertEqual(self.pivot(self.top10('val="5" filterVal="5"'), state="hidden")["value_filters"], [])


# --------------------------------------------------------------------------
# GETPIVOTDATA, calcPr defaults, pivot selections, scenarios and XLOOKUP match modes
# --------------------------------------------------------------------------

class GetPivotData(Tmp):
    def book(self, formula='GETPIVOTDATA("Sum of Amount",P!$A$3)'):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep_sheet = Sheet("R", [[F(formula, 1)]])
        return make_book(self.tmp, [data, pv, rep_sheet], parts=pivot_parts())

    def test_it_routes_nr_flags_and_names_the_pivot_it_reads(self):
        rep = cw.analyze(self.book())
        reg = region(rep, "R", "A1")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("getpivotdata", flag_ids(reg))
        self.assertIn("cookbook-pivot.md", " ".join(reg["reasons"]))
        self.assertEqual([(p["name"], p["location"]) for p in reg["pivots"]], [("PivotTable1", "A3:B8")])
        self.assertEqual(reg["pivots"][0]["source"]["sheet"], "Data")

    def test_the_nr_reason_says_a_recipe_exists_and_carries_a_stable_reason_id(self):
        reg = region(cw.analyze(self.book()), "R", "A1")
        self.assertEqual(reg["reason_id"], "pivot_recipe")
        self.assertIn("a recipe exists (cookbook-pivot.md P8)", " ".join(reg["reasons"]))

    def test_a_region_with_another_nr_reason_or_none_has_no_pivot_recipe_id(self):
        rep = cw.analyze(self.book('GETPIVOTDATA("x",P!$A$3)+CUBEVALUE("conn","[M].[x]")'))
        self.assertEqual((region(rep, "R", "A1")["route"], region(rep, "R", "A1")["reason_id"]), ("NR", None))
        self.assertIsNone(region(cw.analyze(self.book("1+2")), "R", "A1")["reason_id"])

    def test_the_route_table_prints_recipe_backed_nr_separately(self):
        rep = cw.analyze(self.book())
        self.assertEqual(rep["routes_nr_pivot_recipe"], {"regions": 1, "cells": 1})
        txt = cw.render_text(rep)
        self.assertIn("| NR (pivot recipe) | 1 | 1 |", txt)
        self.assertIn("| NR | 0 | 0 |", txt)

    def test_a_reference_outside_every_pivot_still_flags_with_no_dependency(self):
        reg = region(cw.analyze(self.book('GETPIVOTDATA("x",R!$B$9)')), "R", "A1")
        self.assertEqual(reg["route"], "NR")
        self.assertEqual(reg["pivots"], [])

    def test_other_regions_have_an_empty_pivot_list(self):
        reg = region(cw.analyze(self.book("1+2")), "R", "A1")
        self.assertEqual(reg["pivots"], [])

    def test_regions_with_one_reason_text_print_as_one_line_with_a_count(self):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rows = []
        for i in range(31):
            rows += [[F(f'GETPIVOTDATA("Sum of Amount",P!$A$3,"Region","k{i}")', 1)], [None]]
        path = make_book(self.tmp, [data, pv, Sheet("R", rows)], parts=pivot_parts())
        rep = cw.analyze(path)
        regs = [r for r in rep["regions"] if "getpivotdata" in [f["id"] for f in r["flags"]]]
        self.assertEqual(len(regs), 31)
        self.assertTrue(all(r["reasons"] for r in regs))
        lines = [l for l in cw.render_text(rep).splitlines() if "GETPIVOTDATA reads a pivot" in l]
        self.assertEqual(len(lines), 1)
        self.assertIn("31 regions", lines[0])
        self.assertIn("R!A1", lines[0])
        self.assertIn("R!A9", lines[0])
        self.assertNotIn("R!A11", lines[0])


class CalcProperties(Tmp):
    def props(self, calc):
        return cw.analyze(one_sheet(self.tmp, [[1]], calc=calc))["workbook_props"]["calc"]

    def test_every_calcpr_attribute_and_the_excel_defaults_are_reported(self):
        c = self.props('<calcPr calcId="191029" calcMode="manual" calcOnSave="0" fullCalcOnLoad="1" iterate="1" '
                       'iterateCount="50" iterateDelta="0.0001"/>')
        self.assertEqual((c["calcMode"], c["calcOnSave"], c["fullCalcOnLoad"], c["iterate"]), ("manual", "0", True, True))
        self.assertEqual((c["iterateCount"], c["iterateDelta"]), ("50", "0.0001"))
        self.assertEqual(c["effective"], {"calcMode": "manual", "calcOnSave": False, "fullCalcOnLoad": True, "iterate": True,
                                          "iterateCount": 50, "iterateDelta": 0.0001})

    def test_defaults_fill_what_the_file_leaves_out(self):
        c = self.props('<calcPr calcId="191029" iterate="1"/>')
        self.assertIsNone(c["iterateDelta"])
        self.assertEqual(c["effective"], {"calcMode": "auto", "calcOnSave": True, "fullCalcOnLoad": False, "iterate": True,
                                          "iterateCount": 100, "iterateDelta": 0.001})


class PivotSelections(Tmp):
    def parts(self, item="1", cache_source=None):
        parts = pivot_parts(cache_source=cache_source, pivot_extra='<calculatedItems count="1"><calculatedItem field="0" formula="East+West"/></calculatedItems>')
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        pv = pv.replace('<pivotField axis="axisRow"/>', '<pivotField axis="axisPage"><items count="3"><item x="0"/><item x="1"/><item t="default"/></items></pivotField>')
        pv = pv.replace('<pageField fld="2" hier="-1"/>', f'<pageField fld="0" item="{item}" hier="-1"/>' if item is not None else '<pageField fld="0" hier="-1"/>')
        parts["xl/pivotTables/pivotTable1.xml"] = pv
        cache = parts["xl/pivotCache/pivotCacheDefinition1.xml"]
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = cache.replace(
            '<cacheField name="Region" numFmtId="0"><sharedItems/></cacheField>',
            '<cacheField name="Region" numFmtId="0"><sharedItems count="2"><s v="East"/><s v="West"/></sharedItems></cacheField>')
        return parts

    def pivot(self, state=None, **kw):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]], state=state)
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [data, pv], parts=self.parts(**kw)))["pivots"][0]

    def test_the_selected_page_item_is_resolved_against_the_shared_items(self):
        self.assertEqual(self.pivot()["page_items"], [{"field": "Region", "item": "West"}])
        self.assertEqual(self.pivot(item="0")["page_items"], [{"field": "Region", "item": "East"}])

    def test_a_page_field_with_no_item_is_all(self):
        self.assertEqual(self.pivot(item=None)["page_items"], [{"field": "Region", "item": "(All)"}])

    def test_calculated_item_formulas_are_reported(self):
        p = self.pivot()
        self.assertEqual(p["calculated_item_formulas"], [{"field": "Region", "formula": "East+West"}])
        self.assertEqual(p["calculated_items"], 1)

    def test_a_pivot_over_a_hidden_sheet_leaks_neither(self):
        p = self.pivot(state="hidden")
        self.assertEqual((p["page_items"], p["calculated_item_formulas"]), ([], []))
        self.assertNotIn("West", json.dumps(p))

    def test_the_text_report_prints_them(self):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        text = cw.render_text(cw.analyze(make_book(self.tmp, [data, pv], parts=self.parts())))
        self.assertIn("Region = West", text)
        self.assertIn("East+West", text)


class ScenarioValues(Tmp):
    XML = ('<scenarios current="0"><scenario name="Base" count="1" user="u" comment="c"><inputCells r="B2" val="10"/></scenario>'
           '<scenario name="High" count="2"><inputCells r="B2" val="20"/><inputCells r="B3" val="0.5"/></scenario></scenarios>')

    def test_names_input_cells_and_values_are_reported(self):
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1, 2]], after=self.XML)]))
        self.assertEqual(rep["scenarios"], [
            {"sheet": "S", "name": "Base", "cells": [{"cell": "B2", "value": "10"}]},
            {"sheet": "S", "name": "High", "cells": [{"cell": "B2", "value": "20"}, {"cell": "B3", "value": "0.5"}]}])
        self.assertNotIn("comment", json.dumps(rep["scenarios"]))
        self.assertIn("Base", cw.render_text(rep))

    def test_a_hidden_sheet_contributes_no_scenario_values(self):
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", [[1]], state="hidden", after=self.XML.replace("Base", "Zorbax"))]))
        self.assertEqual(rep["scenarios"], [])
        self.assertNotIn("Zorbax", json.dumps(rep) + cw.render_text(rep))


class XlookupMatchModeText(Tmp):
    check = Routes.check

    def reasons(self, f):
        return " ".join(self.check(f, "C", ["approx_match"])[1]["reasons"])

    def test_match_mode_plus_minus_one_is_a_linear_search_correct_on_unsorted_data(self):
        for mode, word in (("-1", "smaller"), ("1", "larger")):
            r = self.reasons(f"_xlfn.XLOOKUP(A1,A1:A4,B1:B4,0,{mode})")
            self.assertIn(word, r)
            self.assertIn("linear", r)
            self.assertIn("no sort assumption", r)
            self.assertNotIn("range join", r)

    def test_only_binary_search_modes_assume_sorted_data(self):
        r = " ".join(self.check("_xlfn.XLOOKUP(A1,A1:A4,B1:B4,0,1,2)", "C", ["approx_match"])[1]["reasons"])
        self.assertIn("binary search mode assumes sorted data", r)

    def test_match_mode_two_is_a_wildcard(self):
        self.assertIn("wildcard", " ".join(self.check("_xlfn.XLOOKUP(A1,A1:A4,B1:B4,0,2)", "C", ["criteria_wildcard"])[1]["reasons"]))


# --------------------------------------------------------------------------
# Pivot shapes, self-anchored OFFSET, intersections, external workbooks and user-defined function kinds
# --------------------------------------------------------------------------

class PivotShapes(Tmp):
    """Pivot XML shapes seen in real files: x14 show-as, hidden items, multi-select pages, calculated items in the cache."""
    X14 = "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main"

    def parts(self, field0, page='<pageFields count="1"><pageField fld="0" hier="-1"/></pageFields>', datafield=None,
              cache_extra="", shared='<sharedItems count="3"><s v="East"/><s v="West"/><s v="North"/></sharedItems>'):
        parts = pivot_parts(page=False, pivot_extra="")
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        pv = pv.replace('<pivotField axis="axisRow"/>', field0)
        pv = pv.replace('<rowFields count="1"><field x="0"/></rowFields>', '<rowFields count="1"><field x="0"/></rowFields>' + page)
        if datafield:
            pv = pv.replace('<dataField name="Sum of Amount" fld="1" subtotal="sum"/>', datafield)
        parts["xl/pivotTables/pivotTable1.xml"] = pv
        cache = parts["xl/pivotCache/pivotCacheDefinition1.xml"]
        cache = cache.replace('<cacheField name="Region" numFmtId="0"><sharedItems/></cacheField>',
                              f'<cacheField name="Region" numFmtId="0">{shared}</cacheField>')
        cache = cache.replace("</cacheFields>", "</cacheFields>" + cache_extra)
        parts["xl/pivotCache/pivotCacheDefinition1.xml"] = cache
        return parts

    def pivot(self, state=None, **kw):
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]], state=state)
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [data, pv], parts=self.parts(**kw)))["pivots"][0]

    ROW = '<pivotField axis="axisRow"><items count="4"><item x="0"/><item h="1" x="1"/><item h="1" x="2"/><item t="default"/></items></pivotField>'
    PAGE = ('<pivotField axis="axisPage" multipleItemSelectionAllowed="1"><items count="4"><item x="0"/><item h="1" x="1"/><item x="2"/>'
            '<item t="default"/></items></pivotField>')

    def test_x14_show_as_is_read_and_wins_over_the_base_attribute(self):
        df = (f'<dataField name="Sum of Amount" fld="1" subtotal="sum" showDataAs="percentOfRow"><extLst><ext uri="{{E15A36E0}}" '
              f'xmlns:x14="{self.X14}"><x14:dataField pivotShowAs="percentOfParentRow"/></ext></extLst></dataField>')
        p = self.pivot(field0='<pivotField axis="axisRow"/>', datafield=df)
        self.assertEqual(p["data_fields"][0]["show_data_as"], "percentOfParentRow")

    def test_the_base_attribute_alone_and_the_default_still_work(self):
        p = self.pivot(field0='<pivotField axis="axisRow"/>',
                       datafield='<dataField name="Sum of Amount" fld="1" subtotal="sum" showDataAs="percentOfTotal"/>')
        self.assertEqual(p["data_fields"][0]["show_data_as"], "percentOfTotal")
        self.assertEqual(self.pivot(field0='<pivotField axis="axisRow"/>')["data_fields"][0]["show_data_as"], "normal")

    def test_hidden_items_are_named_per_field(self):
        p = self.pivot(field0=self.ROW)
        self.assertEqual(p["hidden_items"], [{"field": "Region", "hidden": ["West", "North"], "count": 2}])

    def test_a_multi_select_page_filter_is_not_all(self):
        p = self.pivot(field0=self.PAGE)
        self.assertEqual(p["page_items"], [{"field": "Region", "item": "East, North", "multi": True, "hidden": ["West"]}])

    def test_a_multi_select_page_with_nothing_hidden_is_all(self):
        page = self.PAGE.replace('<item h="1" x="1"/>', '<item x="1"/>')
        self.assertEqual(self.pivot(field0=page)["page_items"], [{"field": "Region", "item": "(All)"}])

    def test_hidden_items_leak_nothing_from_a_hidden_sheet(self):
        p = self.pivot(field0=self.ROW, state="hidden")
        self.assertEqual(p["hidden_items"], [])
        self.assertNotIn("West", json.dumps(p))

    def test_calculated_items_stored_in_the_cache_definition_are_read(self):
        extra = ('<calculatedItems count="1"><calculatedItem formula="East+West"><pivotArea cacheIndex="1" outline="0" fieldPosition="0">'
                 '<references count="1"><reference field="0" count="1"><x v="2"/></reference></references></pivotArea></calculatedItem>'
                 '</calculatedItems>')
        p = self.pivot(field0='<pivotField axis="axisRow"/>', cache_extra=extra)
        self.assertEqual(p["calculated_item_formulas"], [{"field": "Region", "formula": "East+West"}])
        self.assertEqual(p["calculated_items"], 1)


class SelfAnchoredOffset(Tmp):
    def test_an_offset_anchored_on_its_own_cell_is_not_a_cycle(self):
        rows = [[F(f"OFFSET(A{i},0,1)", 0)] for i in range(1, 4)]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(rep["graph"]["cycles"], [])
        self.assertGreater(rep["graph"]["incomplete_cells"], 0)
        self.assertEqual(rep["regions"][0]["route"], "NR")

    def test_a_real_cycle_beside_it_is_still_found(self):
        rows = [[F("OFFSET(A1,0,1)", 0), F("C1+1", 0), F("B1+1", 0)]]
        self.assertEqual(len(cw.analyze(one_sheet(self.tmp, rows))["graph"]["cycles"]), 1)


class SpaceIntersection(Tmp):
    NAMES = ('<definedNames><definedName name="RowN">S!$2:$2</definedName><definedName name="ColN">S!$B:$B</definedName></definedNames>')

    def test_an_intersection_of_a_row_and_a_column_is_one_cell_not_a_cycle(self):
        rows = [[None, 5, None], [1, 7, F("(RowN ColN)*2", 14)], [2, 9]]
        rep = cw.analyze(one_sheet(self.tmp, rows, names=self.NAMES))
        self.assertEqual(rep["graph"]["cycles"], [])
        reg = region(rep, "S", "C2")
        self.assertEqual([(r["sheet"], r["ref"]) for r in reg["reads"]], [("S", "B2")])
        self.assertIn("intersection", flag_ids(reg))

    def test_a_cell_reference_intersected_with_a_range(self):
        rows = [[1, 2, 3], [4, 5, 6], [F("(A1:C2 B1:B3)", 0)]]
        reg = region(cw.analyze(one_sheet(self.tmp, rows)), "S", "A3")
        self.assertEqual([r["ref"] for r in reg["reads"]], ["B1:B2"])

    def test_an_empty_intersection_reads_nothing(self):
        rows = [[1, 2], [3, 4], [F("(A1:A2 B1:B2)", 0)]]
        reg = region(cw.analyze(one_sheet(self.tmp, rows)), "S", "A3")
        self.assertEqual(reg["reads"], [])
        self.assertIn("intersection", flag_ids(reg))

    def test_a_space_between_arguments_of_a_function_is_not_an_intersection(self):
        rows = [[1, 2], [F("SUM(A1, B1)", 3)]]
        reg = region(cw.analyze(one_sheet(self.tmp, rows)), "S", "A2")
        self.assertNotIn("intersection", flag_ids(reg))


class ExternalWorkbookReferences(Tmp):
    def test_a_named_external_workbook_reference_routes_nr_with_a_reason(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("SUM('[link-external-workbook-a.xlsx]Sheet0'!A1:B1)", 0)]]))
        reg = region(rep, "S", "A1")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("external_ref", flag_ids(reg))
        self.assertNotIn("ref_error", flag_ids(reg))
        self.assertIn("external workbook", " ".join(reg["reasons"]))

    def test_a_numbered_external_link_routes_nr_too(self):
        reg = region(cw.analyze(one_sheet(self.tmp, [[F("[1]Sheet1!A1+1", 0)]])), "S", "A1")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("external_ref", flag_ids(reg))

    def test_a_defined_name_into_another_workbook_is_external_and_taints_readers(self):
        names = ('<definedNames><definedName name="Rate">[1]Sheet1!$A$1</definedName>'
                 '<definedName name="Local">S!$A$1</definedName><definedName name="Alias">[1]!Foo</definedName>'
                 '<definedName name="Gone">\'[1]Book\'!#REF!</definedName></definedNames>')
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("Rate*2", 0), F("Local*2", 2)]], names=names))
        by = {n["name"]: n for n in rep["defined_names"]}
        self.assertTrue(by["Rate"]["external"] and by["Alias"]["external"] and by["Gone"]["external"])
        self.assertFalse(by["Local"]["external"])
        reg = region(rep, "S", "B1")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("external_ref", flag_ids(reg))
        self.assertEqual(region(rep, "S", "C1")["route"], "T")
        self.assertIn("external_link_snapshot", {t["tell"] for t in rep["oracle"]["tells"]})


class DynamicArrayScalars(Tmp):
    def book(self, ref):
        sh = Sheet("S", [[1, 2]])
        sh.data = ('<row r="1"><c r="A1"><v>1</v></c><c r="B1" cm="1"><f t="array" ref="%s">A1*2</f><v>2</v></c></row>' % ref)
        return make_book(self.tmp, [sh])

    def test_a_one_cell_dynamic_array_is_a_flagged_scalar_not_a_spill(self):
        rep = cw.analyze(self.book("B1"))
        reg = region(rep, "S", "B1")
        self.assertEqual(reg["kind"], "formula")
        self.assertIn("dynamic_array_scalar", flag_ids(reg))
        self.assertEqual(rep["excluded_ranges"], [])

    def test_a_real_spill_is_still_excluded(self):
        rep = cw.analyze(self.book("B1:B3"))
        self.assertEqual([(e["kind"], e["ref"]) for e in rep["excluded_ranges"]], [("spill", "B1:B3")])


class UserDefinedFunctionKinds(Tmp):
    def test_let_bound_names_called_as_functions_are_not_add_in_functions(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("_xlfn.LET(_xlpm.f,_xlfn.LAMBDA(_xlpm.x,_xlpm.x*2),_xlpm.f(3))", 6),
                                              F("_xlfn.LET(g,_xlfn.LAMBDA(x,x+1),g(3))", 4)]]))
        self.assertNotIn("xll_udf", rep["code_attached"])
        self.assertNotIn("xll_udf", " ".join(" ".join(r["reasons"]) for r in rep["regions"]))

    def test_an_unknown_function_without_vba_stays_xll_udf(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("MYADDIN(1)", 1)]]))
        self.assertIn("xll_udf", rep["code_attached"])
        self.assertNotIn("vba_udf", rep["code_attached"])

    def test_an_unknown_function_in_a_workbook_with_vba_is_a_vba_udf(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("MYUDF(1)", 1)]], parts={"xl/vbaProject.bin": b"\x00" * 16}))
        self.assertIn("vba_udf", rep["code_attached"])
        self.assertNotIn("xll_udf", rep["code_attached"])
        reg = region(rep, "S", "A1")
        self.assertEqual(reg["route"], "NR")
        self.assertIn("VBA", " ".join(reg["reasons"]))


# --------------------------------------------------------------------------
# Reference parsing is bounded
# --------------------------------------------------------------------------

class HugeReferences(Tmp):
    LETTERS = "A" * 1000000

    def test_out_of_bound_addresses_are_rejected_without_scanning(self):
        for bad in ("A" * 100 + "1", "A1" + "0" * 30, "AAAA1", "A" + "9" * 8, "A0"):
            with self.subTest(bad=bad[:20]):
                self.assertIsNone(cw.parse_cell_ref(bad))
                self.assertIsNone(cw.parse_cell(bad))
        self.assertEqual(cw.parse_cell_ref("XFD1048576"), (1048576, 16384))
        self.assertEqual(cw.parse_cell_ref("B7"), (7, 2))

    def test_a_million_letter_cell_reference_finishes_quickly_and_is_ignored(self):
        import time
        sh = Sheet("S")
        sh.data = (f'<row r="1"><c r="{self.LETTERS}1"><v>1</v></c><c r="B1"><v>2</v></c></row>')
        sh.after = f'<mergeCells count="1"><mergeCell ref="{self.LETTERS}1:B2"/></mergeCells>'
        sh.before = f'<dimension ref="{self.LETTERS}1:B2"/>'
        names = f'<definedNames><definedName name="Big">S!{self.LETTERS}1</definedName></definedNames>'
        t0 = time.time()
        rep = cw.analyze(make_book(self.tmp, [sh], names=names))
        cw.render_text(rep)
        self.assertLess(time.time() - t0, 10.0)
        self.assertEqual(rep["status"], "ok")

    def test_a_million_letter_name_inside_a_formula_finishes_quickly(self):
        import time
        t0 = time.time()
        rep = cw.analyze(one_sheet(self.tmp, [[1, F(f"{self.LETTERS}1+A1:{self.LETTERS}5", 0)]]))
        cw.render_text(rep)
        self.assertLess(time.time() - t0, 10.0)


# --------------------------------------------------------------------------
# Real-file findings: headerless tables, header inputs, runnable stanzas, pivots, data tables
# --------------------------------------------------------------------------

CT_ONE_SHEET = ('<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/></Types>')


def duck_connection():
    try:
        import duckdb
        con = duckdb.connect()
        con.execute("LOAD excel")
        return con
    except Exception:
        return None


DUCK = duck_connection()


class RunnableStanzas(Tmp):
    """Every stanza the script prints is executed here, so a wrong range or a typed read of text fails the suite."""

    def build(self, rows, **kw):
        path = make_book(self.tmp, [Sheet("S", rows, **kw)], parts={"[Content_Types].xml": CT_ONE_SHEET})
        return path, cw.analyze(path)["sources"][0]

    def run_stanza(self, path, src):
        import re
        sql = re.search(r'"""\n(.*)\n"""', src["stanza"], re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
        return DUCK.execute(sql).fetchall()

    def test_the_printed_sql_is_what_these_tests_run(self):
        path, s = self.build([["k", "v"], ["a", 1], ["b", 2]])
        self.assertIn("read_xlsx('book.xlsx'", s["stanza"])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_text_among_numbers_and_a_blank_led_text_column_read_as_varchar_with_try_cast(self):
        rows = [["k", "v", "w"], ["a", 1, None], ["b", "oops", None], ["c", 3, "note"]]
        path, s = self.build(rows)
        self.assertIn("all_varchar = true", s["stanza"])
        self.assertIn('TRY_CAST("v" AS DOUBLE)', s["stanza"])
        self.assertEqual(self.run_stanza(path, s), [("a", 1.0, None), ("b", None, None), ("c", 3.0, "note")])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_a_headerless_range_counts_its_first_row_as_data(self):
        rows = [[None, 1], [None, 2], ["Years", 3]]
        path, s = self.build(rows)
        self.assertFalse(s["header"])
        self.assertIn("all_varchar = true", s["stanza"])
        self.assertEqual(len(self.run_stanza(path, s)), 3)

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_formula_cells_in_a_lifted_column_come_back_null_by_sheet_row(self):
        rows = [["k", "v"], ["a", 1], ["b", 2], ["c", F("B2+B3", 3)], ["d", 4]]
        path, s = self.build(rows)
        self.assertEqual(s["formula_cells_in_lifted"], ["B4"])
        self.assertIn("CASE WHEN __r IN (4) THEN NULL ELSE", s["stanza"])
        self.assertEqual(self.run_stanza(path, s), [("a", 1.0), ("b", 2.0), ("c", None), ("d", 4.0)])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_subtotal_rows_are_excluded_by_sheet_row_position(self):
        rows = [["k", "v"], ["a", 1], ["b", 2], [None, F("SUM(B2:B3)", 3)], ["c", 5], ["d", 6]]
        path, s = self.build(rows)
        self.assertEqual(self.run_stanza(path, s), [("a", 1.0), ("b", 2.0), ("c", 5.0), ("d", 6.0)])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_a_header_with_a_leading_space_is_read_positionally(self):
        rows = [[" ABC COMPANY LTD"], ["x"]]
        path, s = self.build(rows)
        self.assertIn("header = false", s["stanza"])
        self.assertIn('"A" AS "column1"', s["stanza"])
        self.assertIn("range = 'A2:A2'", s["stanza"])
        self.assertEqual(self.run_stanza(path, s), [("x",)])

    def test_a_formula_error_in_a_column_the_lift_skips_still_switches_to_varchar(self):
        rows = [["k", "calc"], ["a", F("1/0", "#DIV/0!", t="e")], ["b", F("1/0", "#DIV/0!", t="e")], ["c", 3]]
        path, s = self.build(rows)
        self.assertIn("all_varchar = true", s["stanza"])
        if DUCK:
            self.assertEqual(self.run_stanza(path, s), [("a",), ("b",), ("c",)])

    def test_more_formula_cells_than_the_cap_refuse_the_column(self):
        rows = [["k", "v"]] + [["a", i] for i in range(30)] + [["b", F(f"B{i + 2}*2", 0)] for i in range(5)]
        with mock.patch.object(cw, "MAX_NULLED_ROWS", 3):
            s = cw.analyze(one_sheet(self.tmp, rows))["sources"][0]
        self.assertEqual(s["lifted"], ["k"])
        self.assertIn("too many to exclude by row", s["not_lifted_reasons"]["v"])


class HeaderlessAndHeaderInputs(Tmp):
    def src(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))["sources"][0]

    def product_rows(self):
        rows = [["SK001", "Washing Machine", 2000, F("C1*1.2", 2400)]]
        for i, name in enumerate(["Fridge", "Oven", "Kettle", "Toaster"]):
            rows.append([f"SK{i + 2:03d}", name, 2000 + 10 * i, F(f"C{i + 2}*1.2", 1)])
        return rows

    def test_a_first_row_of_product_data_is_not_a_period_header(self):
        s = self.src(self.product_rows())
        self.assertEqual((s["layout"], s["header"], s["wide_ambiguous"]), ("long", False, False))
        self.assertIn('SELECT "A", "B", "C"', s["stanza"])
        self.assertIn("header = false", s["stanza"])
        self.assertNotIn("UNPIVOT", s["stanza"])
        self.assertEqual(s["not_lifted"], ["D"])

    def test_a_real_period_header_beside_a_text_label_stays_wide(self):
        s = self.src([["Account", 2021, 2022, 2023], ["Rev", 1, 2, 3]])
        self.assertEqual(s["layout"], "wide")

    def test_a_period_column_holding_text_below_the_header_is_not_wide(self):
        s = self.src([["Account", 2021, 2022], ["Rev", 1, 2], ["Note", "n/a", 3]])
        self.assertEqual(s["layout"], "long")

    def header_input_book(self):
        rows = [["Code", "Name", "Cost", 0.1, 0.2]]
        for r in range(2, 6):
            rows.append([f"C{r}", f"Item {r}", 100 + r, F(f"$C{r}*(1+D$1)", 1), F(f"$C{r}*(1+E$1)", 1)])
        return rows

    def test_numeric_header_cells_that_formulas_read_are_proposed_as_inputs(self):
        s = self.src(self.header_input_book())
        self.assertTrue(s["header"])
        self.assertEqual([(i["cell"], i["value"], i["read_by"]) for i in s["inputs_in_header"]], [("D1", 0.1, 4), ("E1", 0.2, 4)])
        self.assertEqual(s["inputs_in_header"][0]["suggested_given"], "given: input_d1 :: number is 0.1")
        self.assertIn("range = 'A2:E5'", s["stanza"])
        self.assertIn('"A" AS "Code", "B" AS "Name", "C" AS "Cost"', s["stanza"])
        self.assertIn("inputs in the header row: D1 = 0.1", s["stanza"])

    def test_a_numeric_header_cell_nothing_reads_is_not_an_input(self):
        rows = [["Code", "Name", "Cost", 0.1], ["a", "b", 1, F("C2*2", 2)], ["c", "d", 2, F("C3*2", 4)]]
        self.assertEqual(self.src(rows)["inputs_in_header"], [])


class PivotRefreshAndGrouping(Tmp):
    def report(self, refreshed="45000.5", modified="2023-04-01T09:30:00Z", on_axis=False):
        grouped = ('<cacheField name="Date" numFmtId="14"><sharedItems/>'
                   '<fieldGroup base="0"><rangePr groupBy="months"/></fieldGroup></cacheField>')
        parts = pivot_parts(extra_cache_fields=grouped, refreshed=refreshed)
        if on_axis:
            key = "xl/pivotTables/pivotTable1.xml"
            parts[key] = parts[key].replace("</pivotFields>", '<pivotField axis="axisCol"/></pivotFields>')
        if modified:
            parts["docProps/core.xml"] = ('<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
                                          'xmlns:dcterms="http://purl.org/dc/terms/"><dcterms:modified>' + modified + '</dcterms:modified></cp:coreProperties>')
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [data, pv], parts=parts))

    def test_grouping_reports_whether_the_group_is_on_an_axis(self):
        self.assertFalse(self.report()["pivots"][0]["date_grouping"][0]["grouped_on_axis"])
        self.assertTrue(self.report(on_axis=True)["pivots"][0]["date_grouping"][0]["grouped_on_axis"])

    def test_the_text_report_prints_grouping_and_the_refresh_date_against_the_save(self):
        text = cw.render_text(self.report())
        self.assertIn("refreshed 2023-03-15", text)
        self.assertIn("pivot predates last save by 17 day(s)", text)
        self.assertIn("date grouping on Date: by months, not placed on an axis", text)
        self.assertIn("placed on an axis", cw.render_text(self.report(on_axis=True)))

    def test_a_pivot_refreshed_on_the_save_day_says_so_and_a_missing_date_prints_raw(self):
        self.assertIn("refreshed on the day of the last save", cw.render_text(self.report(modified="2023-03-15T23:00:00Z")))
        rep = self.report(refreshed="bogus", modified=None)
        self.assertIsNone(rep["pivots"][0]["refresh"]["refreshed"])
        self.assertNotIn("predates", cw.render_text(rep))

    def test_serial_dates_follow_the_workbook_date_system(self):
        self.assertEqual(cw.serial_date("45000"), "2023-03-15")
        self.assertEqual(cw.serial_date("1000", True), "1906-09-27")
        self.assertIsNone(cw.serial_date("0"))
        self.assertIsNone(cw.serial_date("nan"))


class OracleAndSheetSummary(Tmp):
    def test_an_excel_shaped_cache_is_said_affirmatively_with_the_calc_id(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*2", 2)]]))
        self.assertTrue(rep["oracle"]["excel_saved"])
        text = cw.render_text(rep)
        self.assertIn("calcId: 191029", text)
        self.assertIn("cache looks Excel-saved: calcId 191029, 1 formula cells all cached", text)

    def test_a_library_written_file_is_not_called_excel_saved(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*2", None)]]))
        self.assertFalse(rep["oracle"]["excel_saved"])
        self.assertNotIn("looks Excel-saved", cw.render_text(rep))
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*2", 2)]], calc=""))
        self.assertIn("calcId: none", cw.render_text(rep))

    def test_the_volatile_caveat_names_the_functions_actually_found(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("RAND()", 0.5)]]))
        tell = [t for t in rep["oracle"]["tells"] if t["tell"] == "volatile"][0]
        self.assertIn("RAND", tell["detail"])
        self.assertNotIn("TODAY", tell["detail"])
        rep = cw.analyze(one_sheet(self.tmp, [[F("TODAY()", 45000)]]))
        tell = [t for t in rep["oracle"]["tells"] if t["tell"] == "volatile"][0]
        self.assertIn("TODAY pin to docProps/core.xml", tell["detail"])

    def test_mixed_columns_count_only_the_lifted_data_rows(self):
        rows = [[None, "A title in the data column"], [None, None], ["k", "v"], ["a", 1], ["b", 2]]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertEqual(rep["sheets"][0]["mixed_columns"], {})
        self.assertNotIn("mixed text/number columns", cw.render_text(rep))


class DataTableInputs(Tmp):
    def book(self, dt_attrs, ref):
        rows = [["scen", None], [5, F("A2*2", 10)], [10, F("", 20, fa=dict(t="dataTable", ref=ref, **dt_attrs))], [20, 40]]
        return cw.analyze(one_sheet(self.tmp, rows))

    def test_a_one_way_column_table_reports_its_input_axis_and_formula_cells(self):
        rep = self.book({"dt2D": "0", "dtr": "0", "r1": "A2"}, "B3:B4")
        reg = [r for r in rep["regions"] if r["kind"] == "datatable"][0]
        d = reg["data_table"]
        self.assertEqual((d["two_d"], d["dtr"], d["r1"], d["input_cells"], d["column_input_cell"]), (False, False, "A2", ["A2"], "A2"))
        self.assertEqual((d["column_axis"], d["formula_cells"]), ("A3:A4", "B2"))
        text = cw.render_text(rep)
        self.assertIn("one-way down: column input A2 (values A3:A4)", text)

    def test_a_two_way_table_reports_both_inputs_and_both_axes(self):
        rep = self.book({"dt2D": "1", "dtr": "1", "r1": "A2", "r2": "B1"}, "B3:C4")
        d = [r for r in rep["regions"] if r["kind"] == "datatable"][0]["data_table"]
        self.assertTrue(d["two_d"])
        self.assertEqual((d["row_input_cell"], d["column_input_cell"], d["row_axis"], d["column_axis"], d["formula_cell"]),
                         ("A2", "B1", "B2:C2", "A3:A4", "A2"))
        self.assertIn("two-way: row input A2", cw.render_text(rep))

    def test_an_input_cell_inside_a_lifted_range_is_flagged_on_the_source(self):
        rep = self.book({"dt2D": "0", "dtr": "0", "r1": "A2"}, "B3:B4")
        src = [s for s in rep["sources"] if s["stanza"]][0]
        self.assertEqual(src["data_table_overlaps"][0]["input_cells_in_range"], ["A2"])
        self.assertEqual(src["data_table_overlaps"][0]["axis_cells_in_range"], ["A3:A4"])
        self.assertIn("what-if levers, not data", src["stanza"])
        self.assertTrue(src["stanza"].rstrip().endswith('""")'))

    def test_a_masked_sheet_data_table_reports_nothing(self):
        rows = [["Password", "hunter2"], [5, F("", 10, fa=dict(t="dataTable", ref="B2:B3", dt2D="0", dtr="0", r1="A2"))], [10, 20]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows, state="veryHidden")]))
        for r in rep["regions"]:
            if r["kind"] == "datatable":
                self.assertIsNone(r["data_table"])


# --------------------------------------------------------------------------
# Review round: injection, masked inputs, the 1904 system, coercion, regex cost
# --------------------------------------------------------------------------

STYLES_FMT = ('<styleSheet xmlns="%s"><cellXfs count="3"><xf numFmtId="0"/><xf numFmtId="14" applyNumberFormat="1"/>'
              '<xf numFmtId="22" applyNumberFormat="1"/></cellXfs></styleSheet>' % MAIN)
STYLES_REL = '<Relationship Id="rIdS" Type="%sstyles" Target="styles.xml"/>' % RT


def raw_cell(ref, v, t=None, s=None):
    a = (f' t="{t}"' if t else "") + (f' s="{s}"' if s else "")
    if t == "inlineStr":
        return f'<c r="{ref}"{a}><is><t>{escape(str(v))}</t></is></c>'
    return f'<c r="{ref}"{a}><v>{escape(str(v))}</v></c>'


def raw_rows(rows):
    return "".join(f'<row r="{i}">' + "".join(raw_cell(f"{col_letters(j + 1)}{i}", *c) for j, c in enumerate(r)) + "</row>"
                   for i, r in enumerate(rows, 1))


class StanzaInjection(Tmp):
    EVIL = "B3:&#10;UNION ALL SELECT 'pwned', 999 --:B3"

    def book(self, fa):
        rows = [["Assumption", "Value", None, None, None], ["Opening", 100, None, None, F("B2*(1+B3)", 105)],
                ["Growth", 0.05, None, 0.02, F("", 102, fa=fa)], ["Other", 7, None, 0.04, 104], [None, None, None, 0.06, 106]]
        return cw.analyze(make_book(self.tmp, [Sheet("S", rows)], parts={"[Content_Types].xml": CT_ONE_SHEET})), self.tmp

    def test_a_data_table_address_is_rebuilt_from_its_numbers_never_echoed(self):
        rep, _ = self.book(dict(t="dataTable", ref="E3:E6", dt2D="0", dtr="0", r1=self.EVIL))
        blob = json.dumps(rep) + cw.render_text(rep)
        self.assertNotIn("UNION", blob)
        self.assertNotIn("pwned", blob)
        for s in rep["sources"]:
            for line in (s["stanza"] or "").splitlines():
                self.assertFalse(line.startswith("UNION"), line)
        dt = [r for r in rep["regions"] if r["kind"] == "datatable"][0]["data_table"]
        self.assertIsNone(dt["r1"])
        self.assertEqual(dt["input_cells"], [])

    def test_a_hostile_ref_is_dropped_everywhere(self):
        rep, _ = self.book(dict(t="dataTable", ref=self.EVIL, dt2D="0", dtr="0", r1="B3"))
        self.assertNotIn("UNION", json.dumps(rep) + cw.render_text(rep))

    def test_workbook_text_never_starts_a_new_sql_line_in_a_stanza(self):
        for payload in ("evil\nUNION ALL SELECT 1 --", "a */ b -- c", "x'; DROP TABLE t; --", "q\r\nSELECT 2"):
            with self.subTest(payload=payload):
                rows = [[payload, "v"], ["a", 1], ["b", F("B2+1", 2)]]
                sh = Sheet(payload, rows)
                src = cw.analyze(make_book(self.tmp, [sh]))["sources"][0]
                for line in (src["stanza"] or "").splitlines():
                    self.assertTrue(line.startswith(("duckdb.sql", "  ", '""")')), line)
                self.assertNotIn("\nUNION", src["stanza"] or "")
                self.assertNotIn("\nSELECT 2", src["stanza"] or "")

    def test_a_note_with_a_triple_quote_is_refused_by_the_appender(self):
        st = 'duckdb.sql("""\n  SELECT 1\n""")'
        self.assertEqual(cw.add_stanza_note(st, 'a """ b'), st)
        self.assertEqual(cw.add_stanza_note(st, "one\ntwo"), 'duckdb.sql("""\n  SELECT 1\n  -- one two\n""")')

    def test_refs_with_more_than_one_colon_do_not_parse(self):
        self.assertIsNone(cw.parse_loc("A1:B2:C3"))
        self.assertEqual(cw.norm_ref("$B$3:$C$4"), "B3:C4")
        self.assertIsNone(cw.norm_ref(self.EVIL))


class HeaderInputsStayMasked(Tmp):
    def rows(self, value=0.0731, label=None):
        head = ["Item", "Base", value] if label is None else ["Item", "Base", label, value]
        col = "C" if label is None else "D"
        return [head] + [[f"i{r}", 100 * r] + ([] if label is None else ["x"]) + [F(f"$B{r}*(1+{col}$1)", 1)] for r in range(2, 6)]

    def test_inputs_on_hidden_sheets_are_not_reported(self):
        for state in ("hidden", "veryHidden"):
            with self.subTest(state=state):
                rep = cw.analyze(make_book(self.tmp, [Sheet("Vis", [["a", 1], ["b", 2]]), Sheet("Hid", self.rows(), state=state)]))
                self.assertNotIn("0.0731", json.dumps(rep) + cw.render_text(rep))

    def test_a_number_beside_a_password_label_is_not_an_input(self):
        rep = cw.analyze(one_sheet(self.tmp, self.rows(739154, "Password")))
        self.assertNotIn("739154", json.dumps(rep) + cw.render_text(rep))

    def test_a_secret_cell_value_never_reaches_the_inputs(self):
        path = one_sheet(self.tmp, self.rows(739154, "PIN"))
        rep = cw.analyze(path, secret_cells=("S!D1=PIN",))
        self.assertNotIn("739154", json.dumps(cw.scrub(rep)) + json.dumps(rep) + cw.render_text(rep))

    def test_an_ordinary_visible_input_is_still_reported(self):
        rep = cw.analyze(one_sheet(self.tmp, self.rows()))
        self.assertEqual([i["cell"] for i in rep["sources"][0]["inputs_in_header"]], ["C1"])


class Workbook1904AndDates(Tmp):
    def book(self, rows, wb_pr="", filename="d.xlsx"):
        sh = Sheet("S", [])
        sh.data = raw_rows(rows)
        return make_book(self.tmp, [sh], parts={"[Content_Types].xml": CT_ONE_SHEET, "xl/styles.xml": STYLES_FMT}, wb_rels_extra=STYLES_REL,
                         wb_pr=wb_pr, filename=filename)

    def stanza_rows(self, path):
        import re
        src = cw.analyze(path)["sources"][0]
        sql = re.search(r'"""\n(.*)\n"""', src["stanza"], re.S).group(1).replace("'" + os.path.basename(path) + "'", "'" + path + "'")
        return src, DUCK.execute(sql).fetchall()

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_the_bundled_1904_fixture_comes_back_in_2024(self):
        path = str(pathlib.Path(__file__).resolve().parent.parent / "fixtures" / "fixture_1904.xlsx")
        src, rows = self.stanza_rows(path)
        import datetime
        self.assertEqual(rows[0][0], datetime.date(2024, 1, 1))
        self.assertIn("DATE '1904-01-01'", src["stanza"])
        self.assertIn("1904 date system", src["stanza"])
        self.assertIn("four years and a day off", cw.render_text(cw.analyze(path)))

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_a_1904_date_column_with_no_text_is_still_converted(self):
        hdr = [("Item", "inlineStr"), ("When", "inlineStr")]
        rows = [hdr, [("a", "inlineStr"), (45292, None, 1)], [("b", "inlineStr"), (45293, None, 1)]]
        path = self.book(rows, wb_pr='<workbookPr date1904="1"/>')
        src, got = self.stanza_rows(path)
        import datetime
        self.assertEqual(got, [("a", datetime.date(2028, 1, 2)), ("b", datetime.date(2028, 1, 3))])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_time_of_day_and_the_fraction_survive_the_conversion(self):
        import datetime
        hdr = [("Item", "inlineStr"), ("When", "inlineStr")]
        rows = [hdr, [("a", "inlineStr"), (45292.75, None, 2)], [("b", "inlineStr"), (45293.5, None, 2)], [("c", "inlineStr"), ("n/a", "inlineStr")]]
        src, got = self.stanza_rows(self.book(rows))
        self.assertEqual(got, [("a", datetime.datetime(2024, 1, 1, 18, 0)), ("b", datetime.datetime(2024, 1, 2, 12, 0)), ("c", None)])
        rows = [hdr, [("a", "inlineStr"), (45292.75, None, 1)], [("b", "inlineStr"), ("n/a", "inlineStr")]]
        src, got = self.stanza_rows(self.book(rows, filename="e.xlsx"))
        self.assertEqual(got[0][1], datetime.date(2024, 1, 1))

    def test_serials_below_61_are_flagged_in_the_stanza(self):
        hdr = [("Item", "inlineStr"), ("When", "inlineStr")]
        rows = [hdr, [("a", "inlineStr"), (45292, None, 1)], [("b", "inlineStr"), (1, None, 1)]]
        src = cw.analyze(self.book(rows))["sources"][0]
        self.assertIn("serial below 61", src["stanza"])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_serials_below_61_are_corrected_and_the_phantom_day_is_null(self):
        import datetime
        hdr = [("Item", "inlineStr"), ("When", "inlineStr")]
        rows = [hdr] + [[(k, "inlineStr"), (n, None, 1)] for k, n in (("a", 45292), ("b", 1), ("c", 59), ("d", 60), ("e", 61))]
        src, got = self.stanza_rows(self.book(rows))
        self.assertEqual(got, [("a", datetime.date(2024, 1, 1)), ("b", datetime.date(1900, 1, 1)), ("c", datetime.date(1900, 2, 28)),
                               ("d", None), ("e", datetime.date(1900, 3, 1))])
        self.assertIn("serial below 61", src["stanza"])

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_a_datetime_column_with_low_serials_gets_the_same_correction(self):
        import datetime
        hdr = [("Item", "inlineStr"), ("When", "inlineStr")]
        rows = [hdr] + [[(k, "inlineStr"), (n, None, 2)] for k, n in (("a", 45292.75), ("b", 1.5), ("c", 60.25), ("d", 61.5))]
        src, got = self.stanza_rows(self.book(rows))
        self.assertEqual(got, [("a", datetime.datetime(2024, 1, 1, 18, 0)), ("b", datetime.datetime(1900, 1, 1, 12, 0)), ("c", None),
                               ("d", datetime.datetime(1900, 3, 1, 12, 0))])

    def test_a_column_with_no_low_serial_keeps_the_plain_conversion(self):
        hdr = [("Item", "inlineStr"), ("When", "inlineStr")]
        rows = [hdr, [("a", "inlineStr"), (45292, None, 1)], [("b", "inlineStr"), ("n/a", "inlineStr")]]
        src = cw.analyze(self.book(rows))["sources"][0]
        self.assertIn("DATE '1899-12-30' + TRY_CAST(floor(", src["stanza"])
        self.assertNotIn("1899-12-31", src["stanza"])


class CoercionIsExplicit(Tmp):
    def book(self, rows):
        sh = Sheet("S", [])
        sh.data = raw_rows(rows)
        path = make_book(self.tmp, [sh], parts={"[Content_Types].xml": CT_ONE_SHEET})
        return path, cw.analyze(path)["sources"][0]

    def run_it(self, path, src):
        import re
        sql = re.search(r'"""\n(.*)\n"""', src["stanza"], re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
        return DUCK.execute(sql).fetchall()

    def amt_rows(self):
        txt = lambda v: (v, "inlineStr")
        return [[txt("Item"), txt("Amt")], [txt("a"), (5, None)], [txt("b"), txt("1")], [txt("c"), (7, None)], [txt("d"), (1, "b")],
                [txt("e"), (9, None)], [txt("f"), txt(" 7 ")], [txt("g"), ("2024-01-05T00:00:00", "d")], [txt("h"), (3, None)]]

    def test_numeric_looking_text_and_booleans_are_listed_and_nulled_by_row(self):
        path, src = self.book(self.amt_rows())
        cells = [t["cell"] for t in src["text_cells"]]
        self.assertEqual(cells, ["B3", "B5", "B7", "B8"])
        self.assertIn("CASE WHEN __r IN (3, 5, 7, 8) THEN NULL", src["stanza"])
        self.assertIn("'1', ' 7 ', '1e3' or 'nan'", src["stanza"])
        if DUCK:
            self.assertEqual([r[1] for r in self.run_it(path, src)], [5.0, None, 7.0, None, 9.0, None, None, 3.0])

    def test_a_label_column_with_one_number_is_not_turned_into_nulls(self):
        txt = lambda v: (v, "inlineStr")
        rows = [[(1, None), (10, None)]] + [[txt(f"label {i}"), (i, None)] for i in range(13)]
        path, src = self.book(rows)
        self.assertIn('"A_number"', src["stanza"])
        self.assertIn('"A",', src["stanza"])
        if DUCK:
            got = self.run_it(path, src)
            self.assertEqual([r[0] for r in got][:3], ["1", "label 0", "label 1"])
            self.assertEqual([r[1] for r in got][:2], [1.0, None])

    def test_a_mostly_numeric_column_keeps_one_typed_column(self):
        txt = lambda v: (v, "inlineStr")
        rows = [[txt("k"), txt("v")]] + [[txt(f"k{i}"), (i, None)] for i in range(6)] + [[txt("z"), txt("n/a")]]
        path, src = self.book(rows)
        self.assertNotIn('"v_number"', src["stanza"])
        self.assertIn('TRY_CAST("v" AS DOUBLE)', src["stanza"])


class OracleAndPivotSkew(Tmp):
    def test_a_manual_calc_workbook_is_not_called_excel_saved(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*2", 2)]], calc='<calcPr calcId="191029" calcMode="manual"/>'))
        self.assertFalse(rep["oracle"]["excel_saved"])
        self.assertNotIn("looks Excel-saved", cw.render_text(rep))
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*2", 2)]], calc='<calcPr calcId="191029" calcOnSave="0"/>'))
        self.assertNotIn("looks Excel-saved", cw.render_text(rep))

    def test_a_pivot_refreshed_after_the_save_is_not_called_stale(self):
        grouped = pivot_parts(refreshed="45010")
        grouped["docProps/core.xml"] = ('<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
                                        'xmlns:dcterms="http://purl.org/dc/terms/"><dcterms:modified>2023-03-15T09:00:00Z</dcterms:modified></cp:coreProperties>')
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]])
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        text = cw.render_text(cw.analyze(make_book(self.tmp, [data, pv], parts=grouped)))
        self.assertNotIn("predates", text)
        self.assertIn("saved 10 day(s) before the pivot refreshed", text)


class LinearTimeOnHostileText(Tmp):
    N = 200000

    def timed(self, fn):
        import time
        t0 = time.time()
        fn()
        self.assertLess(time.time() - t0, 5.0)

    def test_a_secret_label_followed_by_a_long_bracket_run_is_linear(self):
        self.assertEqual(cw.mask_text("token=abc" + ")" * 10 + "x").count("[masked]"), 1)
        self.timed(lambda: cw.mask_text("token=abc" + ")" * self.N + "x"))
        self.timed(lambda: cw.mask_text("password:" + ")" * self.N))
        self.timed(lambda: cw.mask_text("pwd" + " " * self.N + "x"))
        self.timed(lambda: cw.mask_text("secret=" + "a;" * self.N))

    def test_the_label_mask_keeps_closing_brackets_and_masks_the_value(self):
        self.assertEqual(cw.mask_text("Sales (password: Kiwi-9876x)"), "Sales (password: [masked])")
        self.assertEqual(cw.mask_text("a;token=abcd1234;b"), "a;token=[masked];b")

    def test_a_run_of_empty_comments_before_the_mashup_root_is_linear(self):
        head = "<!---->" * 500 + "<x/>"
        self.timed(lambda: cw._MASHUP_ROOT.match(head))
        self.assertIsNotNone(cw._MASHUP_ROOT.match("<!-- a --> <!-- b --><DataMashup xmlns='x'>"))
        self.assertIsNone(cw._MASHUP_ROOT.match(head))

    def test_quoted_string_scanners_are_linear(self):
        for s in ('"' * self.N, '"a' * (self.N // 2), '"a""' * (self.N // 4)):
            self.timed(lambda: cw._M_STR.findall(s))
            self.timed(lambda: cw._M_OPT.findall("[" + s))


class WideLayoutNeverReadsAFormula(Tmp):
    def build(self, rows, left=1):
        path = make_book(self.tmp, [Sheet("S", rows, left=left)], parts={"[Content_Types].xml": CT_ONE_SHEET})
        return path, cw.analyze(path)["sources"][0]

    def run_it(self, path, src):
        import re
        sql = re.search(r'"""\n(.*)\n"""', src["stanza"], re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
        return DUCK.execute(sql).fetchall()

    def forecast(self):
        return [["Account", 2024, 2025, 2026]] + [[a, 100 * i, 110 * i, F(f"C{i + 1}*1.1", 121 * i)] for i, a in enumerate(["Rev", "Cost"], 1)]

    def test_a_forecast_formula_column_is_not_read_by_the_unpivot(self):
        path, s = self.build(self.forecast())
        self.assertEqual(s["layout"], "wide")
        self.assertNotIn('"2026"', s["stanza"])
        self.assertIn("-- not lifted", s["stanza"])
        self.assertNotIn("TODO", s["stanza"])
        if DUCK:
            got = self.run_it(path, s)
            self.assertEqual(sorted({r[1] for r in got}), ["2024", "2025"])
            self.assertEqual(sorted(got), [("Cost", "2024", 200.0), ("Cost", "2025", 220.0), ("Rev", "2024", 100.0), ("Rev", "2025", 110.0)])

    def test_a_formula_cell_among_constants_is_nulled_by_row_and_period(self):
        rows = [["Account", 2024, 2025], ["Rev", 100, F("B2*1.1", 110)], ["Cost", 200, 220], ["Opex", 300, 330]]
        path, s = self.build(rows)
        self.assertIn("(__r IN (2) AND period = '2025')", s["stanza"])
        self.assertIn("NULL here, by sheet row and period", s["stanza"])
        if DUCK:
            self.assertEqual(sorted(self.run_it(path, s)), [("Cost", "2024", 200.0), ("Cost", "2025", 220.0), ("Opex", "2024", 300.0), ("Opex", "2025", 330.0), ("Rev", "2024", 100.0)])

    def test_an_unnameable_period_header_beside_a_formula_column_refuses_the_stanza(self):
        rows = [["Account", 2021, F("B1+1", 2022), F("C1+1", 2023), F("A9", 1)], ["Rev", 1, 2, 3, F("B2*2", 2)], ["Cost", 4, 5, 6, F("B3*2", 8)]]
        s = self.build(rows)[1]
        if s["layout"] == "wide":
            self.assertIsNone(s["stanza"])
            self.assertIn("cannot be named safely", s["stanza_refused"])

    def test_a_first_row_formula_that_repeats_the_formulas_below_is_data(self):
        rows = [["Widget", 2024, 2025, F("AD1+AE1", 4049)]] + [[f"P{i}", i, i + 1, F(f"AD{i}+AE{i}", 2 * i + 1)] for i in range(2, 8)]
        path, s = self.build(rows, left=29)
        self.assertEqual((s["layout"], s["header"]), ("long", False))
        self.assertNotIn("UNPIVOT", s["stanza"])
        self.assertIn('"AF"', s["stanza"].split("not lifted")[1])
        if DUCK:
            got = self.run_it(path, s)
            self.assertEqual(got[0], ("Widget", 2024.0, 2025.0))
            self.assertEqual(len(got[0]), 3)

    def test_the_wide_header_row_is_not_consumed_as_data(self):
        path, s = self.build([["Account", 2024, 2025], ["Rev", 1, 2]])
        self.assertIn("header = false", s["stanza"])
        self.assertIn("range = 'A2:C2'", s["stanza"])
        if DUCK:
            self.assertEqual(sorted(self.run_it(path, s)), [("Rev", "2024", 1.0), ("Rev", "2025", 2.0)])


class FormulaDominatedKeys(Tmp):
    """A formula-dominated block still lifts its constant label column and period-header row; the formula cells stay unlifted."""

    ROWS = [["Item", "P1", "P2", "P3", "P4"],
            ["Item A", F("1+0", 1), F("1+1", 2), F("1+2", 3), F("1+3", 4)],
            ["Item B", F("2+0", 2), F("2+1", 3), F("2+2", 4), F("2+3", 5)],
            ["Item C", F("3+0", 3), F("3+1", 4), F("3+2", 5), F("3+3", 6)]]

    def build(self, rows, **kw):
        path = make_book(self.tmp, [Sheet("S", rows, **kw)], parts={"[Content_Types].xml": CT_ONE_SHEET})
        return path, cw.analyze(path)

    def test_constant_label_column_and_header_row_are_lifted_never_the_formula_body(self):
        _, rep = self.build(self.ROWS)
        refs = sorted(s["data_ref"] for s in rep["sources"])
        self.assertEqual(refs, ["A1:E1", "A2:A4"])
        for s in rep["sources"]:
            self.assertIsNotNone(s["stanza"], s)
            self.assertEqual(s["formula_cells_in_lifted"], [])
            self.assertNotIn("range = 'B2", s["stanza"])

    def test_no_stanza_range_covers_a_formula_cell(self):
        _, rep = self.build(self.ROWS)
        fcells = {(r, c) for r in range(2, 5) for c in range(2, 6)}
        for s in rep["sources"]:
            r1, c1, r2, c2 = cw.parse_loc(s["data_ref"])
            self.assertFalse([1 for r in range(r1, r2 + 1) for c in range(c1, c2 + 1) if (r, c) in fcells])

    def test_a_formula_dominated_block_with_no_constants_says_why_it_has_no_stanza(self):
        rows = [[F("1+0", 1), F("1+1", 2)], [F("2+0", 2), F("2+1", 3)], [F("3+0", 3), F("3+1", 4)]]
        _, rep = self.build(rows)
        self.assertEqual(len(rep["sources"]), 1)
        s = rep["sources"][0]
        self.assertIsNone(s["stanza"])
        self.assertIn("formula", s["stanza_refused"])
        self.assertIn(s["stanza_refused"], cw.render_text(rep))

    def test_the_report_says_masked_only_for_a_masked_sheet(self):
        _, rep = self.build(self.ROWS, state="hidden")
        self.assertIn("masked sheet", cw.render_text(rep))
        _, rep = self.build(self.ROWS)
        for s in rep["sources"]:
            s["stanza"], s["stanza_refused"] = None, None
        text = cw.render_text(rep)
        self.assertNotIn("masked sheet", text)
        self.assertIn("- no stanza", text)

    def test_a_hidden_sheet_stays_masked(self):
        _, rep = self.build(self.ROWS, state="hidden")
        for s in rep["sources"]:
            self.assertIsNone(s["stanza"])
            self.assertEqual(s["lifted"], [])
        self.assertNotIn("Item A", json.dumps(rep))

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_the_lifted_key_stanzas_execute(self):
        import re
        path, rep = self.build(self.ROWS)
        got = {}
        for s in rep["sources"]:
            sql = re.search(r'"""\n(.*)\n"""', s["stanza"], re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
            got[s["data_ref"]] = DUCK.execute(sql).fetchall()
        self.assertEqual(got["A2:A4"], [("Item A",), ("Item B",), ("Item C",)])
        self.assertEqual(got["A1:E1"], [("Item", "P1", "P2", "P3", "P4")])


class HiddenDependencies(Tmp):
    def book(self):
        hidden = Sheet("H", [[1, F("A1+1", 2)], [2, F("A2+1", 3)]], state="hidden")
        return make_book(self.tmp, [Sheet("S", [[1, F("A1*2", 2)]]), hidden])

    def test_regions_on_a_hidden_sheet_are_flagged_hidden_dep_and_keep_their_route(self):
        rep = cw.analyze(self.book())
        hid = [r for r in rep["regions"] if r["sheet"] == "H"]
        self.assertTrue(hid and all(r["hidden_dep"] for r in hid))
        self.assertFalse(region(rep, "S", "B1")["hidden_dep"])
        self.assertEqual({r["route"] for r in hid}, {"T"})
        self.assertIsNone(hid[0]["example"])

    def test_the_text_report_counts_them_without_naming_cells(self):
        txt = cw.render_text(cw.analyze(self.book()))
        self.assertIn("1 region(s) on hidden sheets: hidden_dep, not compared", txt)
        self.assertNotIn("A2+1", txt)


class VolatileDependents(Tmp):
    def rep(self, calc=CALC):
        draw = F("NORM.INV(RAND(),100,15)", 100)
        rows = [[draw, F("AVERAGE(A1:A3)", 100), F("B1*2", 200), F("E1+1", 6), 5], [draw], [draw]]
        return cw.analyze(one_sheet(self.tmp, rows, calc=calc))

    def test_a_region_reading_random_draws_is_x_with_random_dep(self):
        reg = region(self.rep(), "S", "B1")
        self.assertTrue(reg["volatile_dep"])
        self.assertEqual(reg["volatile_dep_via"], "direct")
        self.assertIn("random_dep", flag_ids(reg))
        self.assertEqual(reg["route"], "X")
        self.assertTrue(any("reads random draws" in r for r in reg["reasons"]))

    def test_a_region_two_hops_from_the_draws_is_transitive(self):
        reg = region(self.rep(), "S", "C1")
        self.assertEqual(reg["volatile_dep_via"], "transitive")
        self.assertEqual(reg["route"], "X")
        self.assertIn("random_dep", flag_ids(reg))

    def test_an_unrelated_region_is_clean(self):
        reg = region(self.rep(), "S", "D1")
        self.assertFalse(reg["volatile_dep"])
        self.assertIsNone(reg["volatile_dep_via"])
        self.assertEqual(reg["route"], "T")

    def test_today_dependents_are_flagged_but_keep_their_route(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("TODAY()", 45000), F("A1+1", 45001)]]))
        reg = region(rep, "S", "B1")
        self.assertTrue(reg["volatile_dep"])
        self.assertNotIn("random_dep", flag_ids(reg))
        self.assertEqual(reg["route"], "T")

    def test_a_cycle_through_a_random_region_terminates(self):
        rep = cw.analyze(one_sheet(self.tmp, [[F("B1+RAND()", 0), F("A1+1", 0)]]))
        self.assertTrue(all(r["volatile_dep"] for r in rep["regions"]))

    def test_the_text_report_counts_dependents(self):
        self.assertIn("2 region(s) depend on volatile cells", cw.render_text(self.rep()))


class TransitiveHiddenDependencies(Tmp):
    def rep(self):
        hidden = Sheet("H", [["Zorbax", 5]], state="veryHidden")
        s = Sheet("S", [[1, F("H!B1*2", 10), None, F("B1+1", 11)],
                        [2, None, None, None],
                        [3, F("A3+1", 4), None, F("C2+B3", 4)]])
        t = Sheet("T", [[F("S!D1+1", 12)], [None], [F("A1+S!B3", 5)]])
        return cw.analyze(make_book(self.tmp, [s, t, hidden]))

    def test_a_visible_region_reading_a_hidden_sheet_is_hidden_dep_direct(self):
        rep = self.rep()
        reg = region(rep, "S", "B1")
        self.assertTrue(reg["hidden_dep"])
        self.assertEqual(reg["hidden_dep_via"], "direct")

    def test_a_region_reading_a_hidden_dep_region_is_transitive_and_an_unrelated_one_is_clean(self):
        rep = self.rep()
        self.assertEqual(region(rep, "S", "D1")["hidden_dep_via"], "transitive")
        self.assertEqual(region(rep, "T", "A1")["hidden_dep_via"], "transitive")
        self.assertFalse(region(rep, "S", "B3")["hidden_dep"])
        self.assertIsNone(region(rep, "S", "B3")["hidden_dep_via"])

    def test_a_cycle_through_a_hidden_dep_region_terminates(self):
        hidden = Sheet("H", [[7]], state="hidden")
        s = Sheet("S", [[F("A2+H!A1", 0), None], [F("A1+1", 0), None]])
        rep = cw.analyze(make_book(self.tmp, [s, hidden]))
        self.assertTrue(all(r["hidden_dep"] for r in rep["regions"] if r["sheet"] == "S"))

    def test_the_text_report_counts_dependents_and_prints_nothing_from_the_hidden_sheet(self):
        rep = self.rep()
        txt = cw.render_text(rep)
        self.assertIn("4 region(s) depend on hidden sheets: hidden_dep, not compared", txt)
        self.assertNotIn("Zorbax", txt + json.dumps(rep))


class MaskedPivotCounts(Tmp):
    def rep(self, state):
        parts = pivot_parts(pivot_extra=('<filters count="1"><filter fld="0" type="count" id="1"><autoFilter ref="A1">'
                                         '<filterColumn colId="0"><top10 val="3" filterVal="3"/></filterColumn></autoFilter></filter></filters>'
                                         '<calculatedItems count="1"><calculatedItem formula="East+West"/></calculatedItems>'))
        data = Sheet("Data", [["Region", "Amount"], ["East", 1]], state=state)
        pv = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [data, pv], parts=parts))

    def test_a_masked_pivot_reports_no_top_n_count(self):
        self.assertEqual(self.rep("hidden")["pivots"][0]["top_n_filters"], 0)
        self.assertEqual(self.rep(None)["pivots"][0]["top_n_filters"], 1)

    def test_a_masked_pivot_reports_no_calculated_item_count(self):
        self.assertEqual(self.rep("hidden")["pivots"][0]["calculated_items"], 0)
        self.assertEqual(self.rep(None)["pivots"][0]["calculated_items"], 1)

    def test_the_text_report_says_once_that_masked_pivot_structure_is_withheld(self):
        txt = cw.render_text(self.rep("hidden"))
        self.assertEqual(txt.count("structure masked; ask the user to unhide or share"), 1)
        self.assertNotIn("structure masked", cw.render_text(self.rep(None)))


class OracleStanzas(Tmp):
    """Each formula region and pivot output carries a read of exactly its own cached cells."""

    def build(self, extra_sheets=()):
        rows = [["k", "v", "w"], ["a", 1, F("B2*2", 2)], ["b", 2, F("B3*2", 4)], ["c", 3, F("B4*2", 6)]]
        return make_book(self.tmp, [Sheet("S", rows)] + list(extra_sheets), parts={"[Content_Types].xml": CT_ONE_SHEET})

    def test_a_formula_region_gets_a_read_of_exactly_its_own_range(self):
        rep = cw.analyze(self.build())
        reg = region(rep, "S", "C2:C4")
        st = reg["oracle_stanza"]
        self.assertTrue(st.startswith('duckdb.sql("""') and st.endswith('""")'))
        self.assertIn("ORACLE read", st)
        self.assertIn("read_xlsx('book.xlsx', sheet = 'S', range = 'C2:C4', header = false, all_varchar = true)", st)

    def test_masked_sheets_and_non_formula_regions_get_no_oracle_stanza(self):
        hidden = Sheet("H", [[1, F("A1+1", 2)]], state="hidden")
        rep = cw.analyze(self.build([hidden]))
        self.assertIsNone(region(rep, "H", "B1")["oracle_stanza"])
        self.assertNotIn("ORACLE read", json.dumps([r for r in rep["regions"] if r["sheet"] == "H"]))

    def kind_book(self, hidden=False):
        rows = [[1, F("A1:A3*2", 2, fa={"t": "array", "ref": "B1:B3"}), None, F("SORT(A1:A3)", 1, fa={"t": "array", "ref": "D1:D3"}, ca={"cm": "1"})],
                [2, 4, None, 2], [3, 6, None, 3], [None] * 4,
                ["x", None, None, None],
                [5, F("A6*2", 10)],
                [10, F("", 20, fa=dict(t="dataTable", ref="B7:B8", dt2D="0", dtr="0", r1="A6"))], [20, 40]]
        return make_book(self.tmp, [Sheet("S", rows, state="hidden" if hidden else None)], parts={"[Content_Types].xml": CT_ONE_SHEET})

    def test_array_spill_and_data_table_regions_get_a_read_of_exactly_their_own_range(self):
        rep = cw.analyze(self.kind_book())
        for ref in ("B1:B3", "D1:D3", "B7:B8"):
            with self.subTest(ref=ref):
                self.assertIn(f"range = '{ref}', header = false, all_varchar = true)", region(rep, "S", ref)["oracle_stanza"])
        dt = region(rep, "S", "B7:B8")["data_table"]
        self.assertEqual(dt["oracle_stanza"], region(rep, "S", "B7:B8")["oracle_stanza"])
        self.assertEqual(rep["oracle_stanzas"]["stanzas"], sum(1 for r in rep["regions"] if r["oracle_stanza"]))

    def test_a_masked_array_spill_or_data_table_gets_no_oracle_stanza(self):
        rep = cw.analyze(self.kind_book(hidden=True))
        self.assertTrue(rep["regions"])
        self.assertTrue(all(r["oracle_stanza"] is None for r in rep["regions"]))
        self.assertNotIn("ORACLE read", json.dumps(rep["regions"]))

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_a_data_table_oracle_stanza_returns_its_cached_results(self):
        import re
        path = self.kind_book()
        st = region(cw.analyze(path), "S", "B7:B8")["data_table"]["oracle_stanza"]
        sql = re.search(r'"""\n(.*)\n"""', st, re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
        self.assertEqual(DUCK.execute(sql).fetchall(), [('20',), ('40',)])

    def test_a_triple_quote_in_the_sheet_name_refuses_the_stanza(self):
        self.assertIsNone(cw.render_oracle_stanza("book.xlsx", 'a"""b', "B1:B2"))
        self.assertIsNone(cw.render_oracle_stanza('b"""k.xlsx', "S", "B1:B2"))
        self.assertIn("range = 'B1:B1'", cw.render_oracle_stanza("book.xlsx", "S", "B1"))

    def test_a_pivot_with_a_known_location_gets_one(self):
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [Sheet("Data", [["Region", "Amount"], ["East", 1]]), sh], parts=pivot_parts()))
        self.assertIn("sheet = 'P', range = 'A3:B8'", rep["pivots"][0]["oracle_stanza"])

    def test_the_text_report_counts_them_and_points_at_json(self):
        txt = cw.render_text(cw.analyze(self.build()))
        self.assertIn("Oracle reads", txt)
        self.assertIn("--json", txt)
        self.assertNotIn("ORACLE read:", txt)

    def test_the_cap_is_reported(self):
        with mock.patch.object(cw, "MAX_ORACLE_STANZAS", 0):
            rep = cw.analyze(self.build())
        self.assertIsNone(region(rep, "S", "C2:C4")["oracle_stanza"])
        self.assertEqual(rep["oracle_stanzas"]["omitted_by_cap"], 1)
        self.assertIn("left out: the cap is 0", cw.render_text(rep))

    @unittest.skipUnless(DUCK, "duckdb with the excel extension is not available")
    def test_the_oracle_stanza_executes_and_returns_the_cached_values(self):
        import re
        path = self.build()
        st = region(cw.analyze(path), "S", "C2:C4")["oracle_stanza"]
        sql = re.search(r'"""\n(.*)\n"""', st, re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
        self.assertEqual(DUCK.execute(sql).fetchall(), [('2',), ('4',), ('6',)])


class DateAsText(Tmp):
    def rep(self, dates, extra=None, **kw):
        rows = [["when", "n"]] + [[d, i] for i, d in enumerate(dates, 1)]
        if extra:
            rows[1].extend([None, F(extra, 0)])
        return cw.analyze(one_sheet(self.tmp, rows, **kw))

    def test_a_column_of_iso_text_dates_is_reported_as_text_not_serials(self):
        rep = self.rep(["2024-01-05", "2024-02-17", "2024-03-02"])
        d = rep["sources"][0]["date_as_text"]
        self.assertEqual([(x["column"], x["format"], x["order"]) for x in d], [("when", "yyyy-mm-dd", None)])
        self.assertIn("date_as_text", rep["sources"][0]["stanza"])

    def test_slash_dates_name_the_order_only_when_a_cell_proves_it(self):
        self.assertEqual(self.rep(["13/01/2024", "02/03/2024"])["sources"][0]["date_as_text"][0]["order"], "day-first")
        self.assertEqual(self.rep(["01/13/2024", "02/03/2024"])["sources"][0]["date_as_text"][0]["order"], "month-first")
        self.assertEqual(self.rep(["01/02/2024", "02/03/2024"])["sources"][0]["date_as_text"][0]["order"], "ambiguous")
        self.assertEqual(self.rep(["13/01/2024", "01/13/2024"])["sources"][0]["date_as_text"][0]["order"], "conflicting")

    def test_month_year_text_is_a_text_date(self):
        d = self.rep(["Jan 2024", "Feb 2024"])["sources"][0]["date_as_text"]
        self.assertEqual(d[0]["format"], "mmm yyyy")

    def test_a_column_with_a_non_date_string_or_a_number_is_not_flagged(self):
        self.assertEqual(self.rep(["2024-01-05", "n/a"])["sources"][0]["date_as_text"], [])
        self.assertEqual(self.rep(["2024-01-05", 45000])["sources"][0]["date_as_text"], [])
        self.assertEqual(self.rep(["2024-13-45", "2024-01-05"])["sources"][0]["date_as_text"], [])

    def test_a_formula_reading_the_text_column_through_a_date_function_is_flagged(self):
        for f in ("YEAR(A2)", 'TEXT(A2,"mmm")', "DATEVALUE(A2)", "MONTH(A2)"):
            with self.subTest(f=f):
                rep = self.rep(["2024-01-05", "2024-02-17"], extra=f)
                self.assertIn("date_as_text", flag_ids(region(rep, "S", "D2")))
        rep = self.rep(["2024-01-05", "2024-02-17"], extra="LEN(A2)")
        self.assertNotIn("date_as_text", flag_ids(region(rep, "S", "D2")))

    def test_a_hidden_sheet_gets_no_source_flag(self):
        rows = [["when"], ["2024-01-05"], ["2024-02-17"]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows, state="hidden")]))
        self.assertFalse(any(x.get("date_as_text") for x in rep["sources"]))


class TypedOverwrite(Tmp):
    def regions_of(self, rows, **kw):
        rep = cw.analyze(one_sheet(self.tmp, rows, **kw))
        return rep, [r for r in rep["regions"] if r["kind"] == "formula"]

    def detail(self, reg):
        return [f["detail"] for f in reg["flags"] if f["id"] == "typed_overwrite"]

    def test_a_text_constant_between_two_runs_of_one_formula_is_flagged_on_both(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, F("A3*2", 6)], [4, "n/a"], [5, F("A5*2", 10)], [6, F("A6*2", 12)]]
        _, regs = self.regions_of(rows)
        self.assertEqual([r["ref"] for r in regs], ["B1:B3", "B5:B6"])
        self.assertEqual([self.detail(r) for r in regs], [["B4"], ["B4"]])

    def test_a_text_constant_inside_a_row_of_one_formula_is_flagged(self):
        rows = [[1, 2, 3, "n/a", 5, 6], [F("A1*2", 2), F("B1*2", 4), F("C1*2", 6), "n/a", F("E1*2", 10), F("F1*2", 12)]]
        _, regs = self.regions_of(rows)
        self.assertEqual([self.detail(r) for r in regs], [["D2"], ["D2"]])

    def test_a_numeric_overwrite_stays_a_plug_and_is_not_reported_twice(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, F("A3*2", 6)], [4, 99], [5, F("A5*2", 10)]]
        _, regs = self.regions_of(rows)
        self.assertEqual(len(regs), 1)
        self.assertIn("plug", flag_ids(regs[0]))
        self.assertNotIn("typed_overwrite", flag_ids(regs[0]))

    def test_a_constant_beside_a_different_formula_or_at_a_run_end_is_not_reported(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, "n/a"], [4, F("A4*3", 12)], [5, F("A5*3", 15)]]
        _, regs = self.regions_of(rows)
        self.assertTrue(all("typed_overwrite" not in flag_ids(r) for r in regs))
        rows = [[1, "n/a"], [2, F("A2*2", 4)], [3, F("A3*2", 6)], [4, F("A4*2", 8)]]
        _, regs = self.regions_of(rows)
        self.assertTrue(all("typed_overwrite" not in flag_ids(r) for r in regs))

    def test_actuals_at_the_start_of_a_run_are_still_not_a_plug(self):
        rows = [[100], [110], [F("A2*1.1", 121)], [F("A3*1.1", 133)], [F("A4*1.1", 146)]]
        _, regs = self.regions_of(rows)
        self.assertTrue(all({"plug", "plug_at_end", "typed_overwrite"}.isdisjoint(flag_ids(r)) for r in regs))

    def test_a_hidden_sheet_gets_no_flag(self):
        rows = [[1, F("A1*2", 2)], [2, F("A2*2", 4)], [3, F("A3*2", 6)], [4, "n/a"], [5, F("A5*2", 10)]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows, state="hidden")]))
        self.assertTrue(rep["regions"])
        self.assertTrue(all("typed_overwrite" not in flag_ids(r) for r in rep["regions"]))


class ControlsBlockInputs(Tmp):
    """A label column plus an input column read by absolute refs: every input keeps a readable value and is listed as a given candidate."""
    STYLES = ('<styleSheet xmlns="%s"><cellXfs count="3"><xf numFmtId="0"/><xf numFmtId="14" applyNumberFormat="1"/>'
              '<xf numFmtId="9" applyNumberFormat="1"/></cellXfs></styleSheet>' % MAIN)

    def book(self, outputs_in_value_column, state=None, text="Base"):
        txt = lambda v: (v, "inlineStr")
        top = [[txt("Assumption"), txt("Value")], [txt("Rate"), (0.05, None, 2)], [txt("Mode"), txt(text)], [txt("Start"), (45292, None, 1)],
               [txt("Margin"), (0.2, None, 2)], [txt("Flag"), (1, "b")]]
        sh = Sheet("S", [])
        sh.state = state
        out_col = 1 if outputs_in_value_column else 3
        fs = ["$B$2*100", 'IF($B$3="Base",1,2)', "$B$4+1", "$B$5*2", "IF($B$6,1,0)"]
        body = [[None] * out_col + [F(f, 1)] for f in fs]
        sh.data = raw_rows(top) + grid(body, top=7 if outputs_in_value_column else 8, left=1)
        return make_book(self.tmp, [sh], parts={"[Content_Types].xml": CT_ONE_SHEET, "xl/styles.xml": self.STYLES}, wb_rels_extra=STYLES_REL)

    def stanza(self, rep):
        return [s for s in rep["sources"] if s["stanza"] and s["ref"].startswith("A1")][0]["stanza"]

    def run_it(self, path, st):
        import re
        sql = re.search(r'"""\n(.*)\n"""', st, re.S).group(1).replace("'book.xlsx'", "'" + path + "'")
        return DUCK.execute(sql).fetchall()

    def test_a_text_selector_is_not_nulled_by_the_numeric_read(self):
        for in_col in (False, True):
            with self.subTest(outputs_in_value_column=in_col):
                path = self.book(in_col)
                st = self.stanza(cw.analyze(path))
                self.assertNotRegex(st, r'__r IN \([^)]*\b3\b[^)]*\) THEN NULL ELSE "Value" END AS "Value"')
                self.assertIn('"Value_number"', st)
                if DUCK:
                    rows = self.run_it(path, st)
                    self.assertIn("Base", [r[1] for r in rows])
                    self.assertEqual([r[2] for r in rows if r[0] == "Rate"], [0.05])

    def test_every_input_is_a_given_candidate_with_ref_kind_and_value(self):
        got = {g["ref"]: (g["kind"], g["value"]) for g in cw.analyze(self.book(False))["given_candidates"] if g["sheet"] == "S"}
        self.assertEqual(got, {"B2": ("percent", 0.05), "B3": ("text", "Base"), "B4": ("date", "2024-01-01"),
                               "B5": ("percent", 0.2), "B6": ("bool", True)})
        self.assertIn("Given candidates", cw.render_text(cw.analyze(self.book(False))))

    def test_a_masked_sheet_lists_no_candidates_and_leaks_no_value(self):
        rep = cw.analyze(self.book(False, state="hidden", text="Zebra"))
        self.assertEqual(rep["given_candidates"], [])
        self.assertNotIn("Zebra", json.dumps(rep))



class GivenCandidateSelectors(Tmp):
    def cands(self, rows, **kw):
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows)], **kw))
        return {g["ref"]: g for g in rep["given_candidates"] if g["sheet"] == "S"}

    @staticmethod
    def sparse(entries):
        rows = [[] for _ in range(max(r for r, _ in entries))]
        for r, row in entries:
            rows[r - 1] = row
        return rows

    def test_a_selector_constant_read_by_two_formulas_by_relative_ref_is_a_candidate(self):
        got = self.cands(self.sparse([(10, [None, 3, None, F("IF(B10=1,1,2)", 2)]), (12, [None, None, None, F("CHOOSE(B10,1,2,3)", 3)])]))
        self.assertEqual(got["B10"]["read_by"], 2)

    def test_ifs_switch_and_index_selector_positions_count(self):
        rows = self.sparse([(50, [None, 9, None, F("_xlfn.IFS(B50>1,1,TRUE,2)", 1)]), (52, [None, None, None, F("_xlfn.SWITCH(B50,1,1,2)", 2)]),
                            (54, [None, None, None, F("INDEX(A1:A4,B50)", 0)])])
        self.assertEqual(self.cands(rows)["B50"]["read_by"], 3)

    def test_a_selector_constant_read_once_with_no_labelled_block_is_not_a_candidate(self):
        self.assertNotIn("B20", self.cands(self.sparse([(20, [None, 4, None, F("CHOOSE(B20,1,2)", 1)])])))

    def test_a_selector_constant_in_a_labelled_controls_block_is_a_candidate_read_once(self):
        got = self.cands(self.sparse([(30, ["Scenario", 2, None, F("CHOOSE(B30,1,2)", 2)]), (31, ["Horizon", 5])]))
        self.assertIn("B30", got)
        self.assertNotIn("B31", got)

    def test_a_value_position_read_is_not_a_selector(self):
        rows = self.sparse([(40, [None, 7, None, F("IF(1,B40,0)", 7)]), (42, [None, None, None, F("IF(1,B40,0)", 7)])])
        self.assertNotIn("B40", self.cands(rows))

    def test_pivot_cells_and_getpivotdata_arguments_are_never_candidates(self):
        rows = [["", ""], [None, None], ["Sum of Amount", None], [None, 5], [None, None, None, F("$B$4*2", 10)],
                [None, None, None, F('GETPIVOTDATA("Sum of Amount",$A$3)', 5)], [None, 1, None, F('GETPIVOTDATA("Sum of Amount",$A$3,"k",$B$7)', 5)]]
        sh = Sheet("S", rows, rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        rep = cw.analyze(make_book(self.tmp, [sh], parts=pivot_parts(location="A3:B6")))
        self.assertEqual(rep["given_candidates"], [])


class ControlsAndSolver(Tmp):
    CTRL = "xl/ctrlProps/ctrlProp%d.xml"

    def ctrl(self, **attrs):
        return "<formControlPr " + " ".join(f'{k}="{v}"' for k, v in attrs.items()) + "/>"

    def book(self, parts, names="", hidden_rows=None):
        vis = Sheet("S", [[1, 10, None, 5], [2, 20], [None, None], [None, 7], [None, 8]],
                    rels=[("ctrlProp", "../ctrlProps/ctrlProp1.xml", "rId1"), ("ctrlProp", "../ctrlProps/ctrlProp2.xml", "rId2"),
                          ("ctrlProp", "../ctrlProps/ctrlProp3.xml", "rId3")])
        hid = Sheet("H", [[3], [4]], state="hidden", rels=[("ctrlProp", "../ctrlProps/ctrlProp4.xml", "rId1")])
        return make_book(self.tmp, [vis, hid], parts=parts, names=names)

    def test_form_controls_are_inventoried_by_reference_and_their_linked_cells_become_givens(self):
        parts = {self.CTRL % 1: self.ctrl(objectType="Drop", fmlaLink="S!$B$3", fmlaRange="S!$A$1:$A$2", sel="2"),
                 self.CTRL % 2: self.ctrl(objectType="Spin", fmlaLink="$B$4", min="0", max="50", inc="5", val="7"),
                 self.CTRL % 3: self.ctrl(objectType="GBox"),
                 self.CTRL % 4: self.ctrl(objectType="CheckBox", fmlaLink="H!$A$1", checked="Checked"),
                 "xl/drawings/vmlDrawing1.vml": "<xml><x:Caption>Pick a scenario</x:Caption></xml>"}
        rep = cw.analyze(self.book(parts))
        fc = rep["form_controls"]
        self.assertEqual((fc["count"], fc["not_listed"]), (4, 1))
        by_type = {c["type"]: c for c in fc["controls"]}
        self.assertEqual(sorted(by_type), ["Drop", "GBox", "Spin"])
        self.assertEqual(by_type["Drop"], {"type": "Drop", "sheet": "S", "linked_cell": "B3", "list_range": "A1:A2", "min": None, "max": None,
                                           "step": None, "selected": 2})
        self.assertEqual((by_type["Spin"]["linked_cell"], by_type["Spin"]["min"], by_type["Spin"]["max"], by_type["Spin"]["step"],
                          by_type["Spin"]["selected"]), ("B4", 0, 50, 5, 7))
        got = {g["ref"]: g for g in rep["given_candidates"]}
        self.assertEqual((got["B3"]["label"], got["B3"]["kind"], got["B3"]["value"]), ("form control: Drop", "choice", 2))
        self.assertEqual((got["B4"]["label"], got["B4"]["kind"], got["B4"]["value"]), ("form control: Spin", "number", 7))
        self.assertEqual({g["ref"] for g in rep["given_candidates"] if g["sheet"] == "H"}, set())
        self.assertNotIn("H!", json.dumps(rep["form_controls"]))
        self.assertNotIn("Pick a scenario", json.dumps(rep))
        self.assertEqual(rep["code_attached"]["form_control"]["count"], 3)
        text = cw.render_text(rep)
        self.assertIn("Form controls", text)
        self.assertIn("linked B3", text)

    def test_solver_names_are_inventoried_by_reference_only(self):
        names = ('<definedNames>'
                 '<definedName name="solver_opt" hidden="1">S!$D$1</definedName>'
                 '<definedName name="solver_adj" hidden="1">S!$B$1:$B$2,S!$B$4</definedName>'
                 '<definedName name="solver_lhs1" hidden="1">S!$B$5</definedName>'
                 '<definedName name="solver_rel1" hidden="1">1</definedName>'
                 '<definedName name="solver_rhs1" hidden="1">S!$D$1</definedName>'
                 '<definedName name="solver_lhs2" hidden="1">S!$B$1:$B$2</definedName>'
                 '<definedName name="solver_rel2" hidden="1">3</definedName>'
                 '<definedName name="solver_rhs2" hidden="1">0</definedName>'
                 '<definedName name="solver_lhs3" hidden="1">H!$A$1</definedName>'
                 '<definedName name="solver_rel3" hidden="1">2</definedName>'
                 '<definedName name="solver_rhs3" hidden="1">1</definedName>'
                 '<definedName name="solver_typ" hidden="1">1</definedName>'
                 '</definedNames>')
        rep = cw.analyze(self.book({}, names=names))
        m = rep["solver"]
        self.assertEqual(m["count"], 12)
        (model,) = m["models"]
        self.assertEqual(model["objective"], "S!D1")
        self.assertEqual(model["changing_cells"], ["S!B1:B2", "S!B4"])
        self.assertEqual(model["constraints"], [{"lhs": "S!B5", "relation": "<=", "rhs": "S!D1"},
                                                {"lhs": "S!B1:B2", "relation": ">=", "rhs": 0}])
        self.assertEqual(model["hidden_targets"], 1)
        got = {g["ref"]: g for g in rep["given_candidates"] if g["sheet"] == "S"}
        self.assertEqual({r for r, g in got.items() if g["label"] == "solver changing cell"}, {"B1", "B2", "B4"})
        text = cw.render_text(rep)
        self.assertIn("solver", text.lower())
        self.assertIn("S!B5 <= S!D1", text)
        self.assertNotIn("H!A1", json.dumps(rep["solver"]))

    def test_a_solver_objective_on_a_hidden_sheet_is_counted_not_listed(self):
        names = ('<definedNames><definedName name="solver_opt" hidden="1">H!$A$1</definedName>'
                 '<definedName name="solver_adj" hidden="1">H!$A$2</definedName></definedNames>')
        rep = cw.analyze(self.book({}, names=names))
        (model,) = rep["solver"]["models"]
        self.assertEqual((model["objective"], model["changing_cells"], model["hidden_targets"]), (None, [], 2))
        self.assertEqual([g for g in rep["given_candidates"] if g["sheet"] == "H"], [])


class TypedInputsInFormulaColumns(Tmp):
    def rep(self, rows, secret_cells=(), **kw):
        return cw.analyze(make_book(self.tmp, [Sheet("S", rows)], **kw), secret_cells=secret_cells)

    def cands(self, rows, **kw):
        return {g["ref"]: g for g in self.rep(rows, **kw)["given_candidates"] if g["sheet"] == "S"}

    def test_an_opening_balance_above_a_copied_down_formula_column_is_a_candidate(self):
        rows = [["Opening balance", 1000]] + [[None, F(f"B{r - 1}*1.05", 1)] for r in range(2, 12)]
        rep = self.rep(rows)
        g = {c["ref"]: c for c in rep["given_candidates"]}["B1"]
        self.assertEqual((g["kind"], g["value"], g["label"], g["where"]), ("number", 1000.0, "Opening balance", "formula_column"))
        self.assertEqual(g["read_by"], 1)
        self.assertEqual(g["read_by_regions"], [region(rep, "S", "B2:B11")["id"]])
        self.assertEqual([x for x in rep["sources"] if x["kind"] == "inputs"], [])

    def test_a_weights_row_read_by_a_sumproduct_is_listed_and_gets_one_inputs_stanza(self):
        rows = [[None, 0.1, 0.2, 0.3, 0.4], [], [None] + [F(f"{col}$1*100", 1) for col in "BCDE"],
                [None] * 6 + [F("SUMPRODUCT($B$1:$E$1,B3:E3)", 1)]]
        rep = self.rep(rows)
        got = {c["ref"]: c for c in rep["given_candidates"]}
        self.assertEqual(sorted(got), ["B1", "C1", "D1", "E1"])
        self.assertEqual([got[r]["value"] for r in ("B1", "C1", "D1", "E1")], [0.1, 0.2, 0.3, 0.4])
        self.assertTrue(all(got[r]["read_by"] >= 1 and got[r]["read_by_regions"] for r in got))
        inputs = [x for x in rep["sources"] if x["kind"] == "inputs"]
        self.assertEqual([(x["sheet"], x["ref"]) for x in inputs], [("S", "B1:E1")])
        self.assertIn("range = 'B1:E1'", inputs[0]["stanza"])
        self.assertIn("all_varchar = true", inputs[0]["stanza"])
        self.assertIn("header = false", inputs[0]["stanza"])

    def test_an_inputs_source_next_to_a_text_criteria_region_does_not_abort_the_report(self):
        rows = [[None, 0.1, 0.2, 0.3, 0.4], [], [None] + [F(f"{col}$1*100", 1) for col in "BCDE"],
                [None] * 6 + [F("SUMPRODUCT($B$1:$E$1,B3:E3)", 1)],
                ["East", 1], ["east", 2], ["West", 3], [None, None, F('SUMIF(A5:A7,"east",B5:B7)', 3)]]
        rep = self.rep(rows)
        self.assertEqual(rep["status"], "ok")
        self.assertTrue([x for x in rep["sources"] if x["kind"] == "inputs"])
        self.assertTrue(all("case_variant_keys" in x for x in rep["sources"]))
        self.assertIn("ci_match", flag_ids(region(rep, "S", "C8")))

    def test_a_typed_driver_in_the_middle_of_a_formula_column_is_listed_without_a_stanza(self):
        rows = [[None, 100]] + [[None, F(f"B{r - 1}*1.1", 1)] for r in range(2, 6)] + [[None, 250]] + \
               [[None, F(f"B{r - 1}*1.1", 1)] for r in range(7, 12)]
        rep = self.rep(rows)
        got = {c["ref"]: c for c in rep["given_candidates"]}
        self.assertEqual(got["B6"]["where"], "formula_column")
        self.assertEqual(got["B6"]["value"], 250.0)
        self.assertEqual([x for x in rep["sources"] if x["kind"] == "inputs"], [])
        self.assertTrue(any("plug" in flag_ids(r) for r in rep["regions"]))

    def test_a_typed_text_driver_in_a_formula_column_is_listed_and_flagged_as_an_overwrite(self):
        rows = [[None, 100]] + [[None, F(f"B{r - 1}*1.1", 1)] for r in range(2, 6)] + [[None, "Base"]] + \
               [[None, F(f"B{r - 1}*1.1", 1)] for r in range(7, 12)]
        rep = self.rep(rows)
        got = {c["ref"]: c for c in rep["given_candidates"]}
        self.assertEqual((got["B6"]["kind"], got["B6"]["value"], got["B6"]["where"]), ("text", "Base", "formula_column"))
        self.assertTrue(any("typed_overwrite" in flag_ids(r) for r in rep["regions"]))

    def test_constants_on_a_hidden_sheet_or_shaped_like_a_secret_are_not_listed(self):
        rows = [["Opening balance", 1000]] + [[None, F(f"B{r - 1}*1.05", 1)] for r in range(2, 8)]
        rows += [[], [None, 0.1, 0.2, 0.3, 0.4], [], [None] + [F(f"{col}$9*100", 1) for col in "BCDE"], [None] * 6 + [F("SUMPRODUCT($B$9:$E$9,B11:E11)", 1)]]
        hidden = cw.analyze(make_book(self.tmp, [Sheet("S", rows, state="hidden")]))
        self.assertEqual(hidden["given_candidates"], [])
        self.assertEqual([x for x in hidden["sources"] if x["kind"] == "inputs"], [])
        secret = "AKIAIOSFODNN7EXAMPLE"
        rows = [["Key", secret]] + [[None, F(f"B{r - 1}&\"x\"", "x")] for r in range(2, 8)]
        rep = self.rep(rows)
        self.assertNotIn("B1", {c["ref"] for c in rep["given_candidates"]})
        self.assertNotIn(secret, json.dumps(rep))
        rep = self.rep([["Opening balance", 1000]] + [[None, F(f"B{r - 1}*1.05", 1)] for r in range(2, 8)], secret_cells=["S!B1=OPENING"])
        self.assertNotIn("B1", {c["ref"] for c in rep["given_candidates"]})

    def test_a_constant_nobody_reads_is_not_listed(self):
        rows = [["Opening balance", 1000, "note"]] + [[None, F(f"B{r - 1}*1.05", 1), 7] for r in range(2, 8)]
        got = self.cands(rows)
        self.assertIn("B1", got)
        self.assertEqual({r for r in got}, {"B1"})

    def test_candidates_stay_capped_and_count_what_was_left_out(self):
        rows = [[None] + [float(i) for i in range(1, 251)], [], [None] + [F(f"{col_letters(c)}1*2", 2) for c in range(2, 252)],
                [None] * 253 + [F("SUMPRODUCT(B1:IQ1,B3:IQ3)", 1)]]
        rep = self.rep(rows)
        self.assertEqual((len(rep["given_candidates"]), rep["given_candidates_omitted"]), (200, 50))

    def test_a_formula_column_label_read_through_a_range_is_not_a_text_candidate(self):
        rows = [["Revenue", 10, F('SUMIF(A1:A3,"Revenue",B1:B3)', 10)], ["Cost", F("B1*2", 20), None], ["Margin", F("B1-B2", -10), None]]
        self.assertEqual([r for r in self.cands(rows) if r.startswith("A")], [])


class TableAutofilter(Tmp):
    """An Excel Table's own autoFilter filterColumn is reported like the sheet-level one: column, filter type, values on visible sheets only."""
    FILTER = ('<autoFilter ref="A1:B4"><filterColumn colId="1"><filters><filter val="3"/><filter val="5"/></filters></filterColumn>'
              '<filterColumn colId="0"><top10 val="2"/></filterColumn></autoFilter>')

    def book(self, state=None, with_filter=True):
        rows = [["k", "v"], ["a", 3], ["b", 4], ["c", 5]]
        sh = Sheet("S", [(({"hidden": "1"}, r) if i == 2 else r) for i, r in enumerate(rows)], state=state, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        tx = table_xml("t1", "A1:B4", ["k", "v"])
        if with_filter:
            tx = tx.replace('<tableColumns', self.FILTER + '<tableColumns', 1)
        return make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": tx})

    def test_a_table_autofilter_is_a_tell_and_names_its_filtered_columns(self):
        rep = cw.analyze(self.book())
        self.assertIn("autofilter", tell_ids(rep))
        af = rep["tables"][0]["autofilter"]
        self.assertEqual(af["ref"], "A1:B4")
        self.assertEqual([(f["column"], f["type"], f["values"]) for f in af["filters"]], [("v", "values", ["3", "5"]), ("k", "top10", [])])
        self.assertEqual(rep["sheets"][0]["table_autofilters"][0]["table"], "t1")
        txt = cw.render_text(rep)
        self.assertIn("t1", txt.split("Autofilters", 1)[1])
        self.assertEqual(rep["sheets"][0]["hidden_rows"], 1)

    def test_a_table_without_a_filter_is_not_a_tell(self):
        rep = cw.analyze(self.book(with_filter=False))
        self.assertNotIn("autofilter", tell_ids(rep))
        self.assertIsNone(rep["tables"][0]["autofilter"])

    def test_a_masked_sheet_reports_no_columns_or_values(self):
        rep = cw.analyze(self.book(state="hidden"))
        self.assertEqual(rep["tables"][0]["autofilter"]["filters"], [])
        self.assertNotIn('"values": ["3"', json.dumps(rep))

    def test_the_sheet_level_filter_names_its_column_and_type_too(self):
        sh = Sheet("S", [["k", "v"], ["a", 3]], after='<autoFilter ref="A1:B2"><filterColumn colId="1"><customFilters><customFilter val="1"/></customFilters></filterColumn></autoFilter>')
        rep = cw.analyze(make_book(self.tmp, [sh]))
        self.assertEqual([(f["column"], f["type"]) for f in rep["sheets"][0]["autofilter_filters"]], [("B", "custom")])


class StanzaCommentWording(Tmp):
    """Naive scans for the banned-keywords list must not trip on a generated comment (headers here avoid the words on purpose)."""
    BANNED = ("read_text", "read_blob", "glob", "http", "s3:", "attach", "install", "load", "copy", "export")

    def stanzas(self):
        hdr = Sheet("Hdr", [["k", "v"], ["a", 1], ["b", 2]])
        wide = Sheet("Wide", [["Account", 2021, 2022, 2023], ["Rev", 1, 2, 3], ["Cost", 4, 5, 6]])
        mixed = Sheet("Mixed", [["k", "v", "w"], ["a", 1, None], ["b", "oops", None], ["c", F("B2+B2", 2), "note"], ["d", 4, None],
                                [None, F("SUM(B2:B5)", 7), None]], after='<mergeCells count="0"/>')
        mixed.data = mixed.data.replace('<row r="3">', '<row r="3" hidden="1">')
        inputs = Sheet("Inputs", [["Code", "Name", "Cost", 0.1], ["a", "b", 1, F("$C2*(1+D$1)", 1)], ["c", "d", 2, F("$C3*(1+D$1)", 2)]])
        consts = Sheet("Consts", [["Item", "P1", "P2"], ["A", F("1+0", 1), F("1+1", 2)], ["B", F("2+0", 2), F("2+1", 3)]])
        dated = Sheet("Dated", [["when", "n"], ["2024-01-05", 1], ["2024-02-17", 2]])
        sib = Sheet("Sib", [["k", "v"], ["a", "x"], ["b", "y"], ["c", "z"], ["d", 5]])
        tab = Sheet("Tab", [["scen", None], [5, F("A2*2", 10)], [10, F("", 20, fa=dict(t="dataTable", ref="B3:B4", dt2D="0", dtr="0", r1="A2"))], [20, 40]])
        rep = cw.analyze(make_book(self.tmp, [hdr, wide, mixed, inputs, consts, dated, sib, tab], parts={"[Content_Types].xml": CT_ONE_SHEET}))
        out = [x["stanza"] for x in rep["sources"] if x["stanza"]] + [x["oracle_stanza"] for x in rep["regions"] if x["oracle_stanza"]]
        out.append(cw.render_oracle_stanza("book.xlsx", "S", "A1:B2"))
        return out, rep

    def test_no_generated_stanza_contains_a_banned_word(self):
        out, rep = self.stanzas()
        self.assertGreater(len(out), 8)
        self.assertTrue(any("ORACLE read" in x for x in out) and any("UNPIVOT" in x for x in out) and any("inputs in the header row" in x for x in out))
        self.assertTrue(any("all_varchar" in x for x in out) and any("_number" in x for x in out))
        for st in out:
            lower = st.replace("read_xlsx(", "").lower()
            for word in self.BANNED:
                self.assertNotIn(word, lower, f"{word!r} in {st}")

class ReservedColumnNames(Tmp):
    def src(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))["sources"][0]

    def test_malloy_reserved_and_date_part_headers_are_kept_and_flagged_not_renamed(self):
        s = self.src([["month", "Year", "years", "region"], ["a", 1, 2, "x"], ["b", 3, 4, "y"]])
        self.assertEqual(s["reserved_columns"], ["month", "Year", "years"])
        self.assertIn('"month", "Year", "years"', s["stanza"])
        self.assertIn("-- Malloy-reserved column name(s): month, Year, years: reference them with backticks (`month`) "
                      "or rename with SELECT ... AS", s["stanza"])

    def test_the_text_report_names_the_reserved_columns(self):
        rep = cw.analyze(one_sheet(self.tmp, [["month", "qty"], ["a", 1], ["b", 2]]))
        self.assertIn("reserved column name(s): month", cw.render_text(rep))

    def test_ordinary_headers_get_no_reserved_comment(self):
        s = self.src([["name", "qty"], ["a", 1], ["b", 2]])
        self.assertEqual(s["reserved_columns"], [])
        self.assertNotIn("Malloy-reserved", s["stanza"])
        self.assertNotIn("reserved column", cw.render_text(cw.analyze(one_sheet(self.tmp, [["name", "qty"], ["a", 1], ["b", 2]]))))

    def test_the_check_is_case_insensitive_and_covers_aliased_reads(self):
        s = self.src([["Code", "DAY", " pad "], ["a", 1, 2], ["b", 3, 4]])
        self.assertIn("AS \"DAY\"", s["stanza"])
        self.assertEqual(s["reserved_columns"], ["DAY"])

    def test_helper_aliases_never_equal_a_sheet_column_letter(self):
        rows = [["SK%03d" % i, "n%d" % i, 2000 + i, F("C%d*2" % (i + 1), 1)] for i in range(5)]
        s = self.src(rows)
        self.assertIn("header = false", s["stanza"])
        letters = {c.lower() for c in "abcdefghijklmnopqrstuvwxyz"}
        for alias in re.findall(r"\bAS ([A-Za-z_]\w*)", s["stanza"]):
            self.assertNotIn(alias.lower(), letters)


class HeaderRowLiftedAsData(Tmp):
    def src(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))["sources"][0]

    def data(self, n=5):
        return [["a%d" % i, "n%d" % i, i, i * 2] for i in range(n)]

    def test_a_text_header_with_a_computed_text_cell_over_numeric_columns_is_a_header(self):
        s = self.src([["id", F('"na"&"me"', "name", t="str"), "qty", "price"]] + self.data())
        self.assertTrue(s["header"])
        self.assertEqual(s["first_data_row"], 2)
        self.assertIn("range = 'A2:D6'", s["stanza"])
        self.assertFalse(s["first_row_text"])

    def test_an_all_text_first_row_over_a_text_block_is_flagged_and_still_read_as_data(self):
        rows = [[F('"i"&"d"', "id", t="str"), "name"]] + [["a%d" % i, "n%d" % i] for i in range(4)]
        s = self.src(rows)
        self.assertFalse(s["header"])
        self.assertTrue(s["first_row_text"])
        self.assertIn("-- first row is all text and may be a header: verify; filter it out in the wrapper", s["stanza"])
        self.assertIn("first row is all text", cw.render_text(cw.analyze(one_sheet(self.tmp, rows))))

    def test_a_mostly_text_first_row_with_a_numeric_cell_is_flagged(self):
        rows = [["id", "name", 2024, "price"]] + self.data()
        s = self.src(rows)
        self.assertFalse(s["header"])
        self.assertTrue(s["first_row_text"])
        self.assertIn("first row is mostly text and may be a header", s["stanza"])

    def test_a_real_header_and_a_data_first_row_are_not_flagged(self):
        plain = self.src([["id", "name", "qty", "price"]] + self.data())
        self.assertTrue(plain["header"])
        self.assertFalse(plain["first_row_text"])
        products = self.src([["SK001", "Washing Machine", 2000, F("C1*1.2", 2400)],
                             ["SK002", "Fridge", 2010, F("C2*1.2", 1)], ["SK003", "Oven", 2020, F("C3*1.2", 1)]])
        self.assertFalse(products["first_row_text"])
        self.assertNotIn("may be a header", products["stanza"])


class ValidatedAndKeyInputs(Tmp):
    DV = ('<dataValidations count="1"><dataValidation type="list" sqref="C4"><formula1>{f}</formula1></dataValidation></dataValidations>')

    def rep(self, formula, c4="Base", f='"Base,Upside"', state=None, dv=True):
        rows = [[], [], [], [None, None, c4, None, F(formula, 1)]]
        sh = Sheet("S", rows, state=state, after=self.DV.format(f=f) if dv else "")
        return cw.analyze(make_book(self.tmp, [sh, Sheet("T", [["a", 1], ["b", 2]])]))

    def cands(self, rep):
        return {g["ref"]: g for g in rep["given_candidates"] if g["sheet"] == "S"}

    def test_a_list_validated_constant_that_a_formula_reads_is_a_choice_candidate(self):
        g = self.cands(self.rep("IF(1,C4,0)"))["C4"]
        self.assertEqual((g["kind"], g["value"], g["choices"]), ("choice", "Base", ["Base", "Upside"]))

    def test_the_same_constant_read_in_a_value_position_without_validation_is_not_a_candidate(self):
        self.assertNotIn("C4", self.cands(self.rep("IF(1,C4,0)", dv=False)))

    def test_a_list_validated_constant_nothing_reads_is_not_a_candidate(self):
        self.assertNotIn("C4", self.cands(self.rep("1+1")))

    def test_a_lookup_value_a_criteria_argument_and_an_if_operand_are_candidates_read_once(self):
        for formula, c4, kind in (("VLOOKUP(C4,T!A1:B2,2,FALSE)", "b", "text"), ("SUMIF(T!B1:B2,C4)", 2, "number"),
                                  ("IF(C4>1,1,2)", 5, "number"), ("COUNTIFS(T!A1:A2,C4)", "a", "text")):
            with self.subTest(formula=formula):
                g = self.cands(self.rep(formula, c4=c4, dv=False))["C4"]
                self.assertEqual((g["kind"], g["read_by"]), (kind, 1))

    def test_a_value_position_argument_of_a_lookup_is_not_a_key(self):
        self.assertNotIn("C4", self.cands(self.rep("VLOOKUP(1,T!A1:B2,C4,FALSE)", c4=2, dv=False)))

    def test_the_validation_inventory_lists_sheet_range_type_and_source_on_visible_sheets(self):
        entries = self.rep("1+1")["code_attached"]["data_validation"]["entries"]
        self.assertEqual(entries, [{"sheet": "S", "ref": "C4", "type": "list", "items": ["Base", "Upside"]}])
        entries = self.rep("1+1", f="T!$A$1:$A$2")["code_attached"]["data_validation"]["entries"]
        self.assertEqual(entries[0]["source"], "T!$A$1:$A$2")
        long = ",".join("v%d" % i for i in range(30))
        self.assertEqual(len(self.rep("1+1", f='"%s"' % long)["code_attached"]["data_validation"]["entries"][0]["items"]), 20)

    def test_a_masked_sheet_gets_counts_only_and_no_candidate(self):
        rep = self.rep("IF(1,C4,0)", c4="Zebra", state="hidden")
        self.assertEqual(rep["given_candidates"], [])
        dv = rep["code_attached"]["data_validation"]
        self.assertEqual(dv["count"], 1)
        self.assertNotIn("entries", dv)
        self.assertNotIn("Zebra", json.dumps(rep))
        self.assertNotIn("Upside", json.dumps(rep))


class StructuredRefNames(Tmp):
    COLS = ["Регион", "Сумма", "Unit Price", "Qty#"]

    def rep(self, formulas, totals=0):
        rows = [self.COLS, ["a", 1, 2, 3], ["b", 4, 5, 6]]
        if totals:
            rows.append(["", F("SUBTOTAL(109,Продажи[Сумма])", 5), None, None])
        rows = [r + [None, F(formulas[i], 0) if i < len(formulas) else None] for i, r in enumerate(rows)]
        for i in range(3, len(formulas)):
            rows.append([None] * 5 + [F(formulas[i], 0)])
        sh = Sheet("Data", rows, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        ref = "A1:D4" if totals else "A1:D3"
        return cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table_xml("Продажи", ref, self.COLS, totals=totals)}))

    def unresolved(self, rep):
        return [r["ref"] for r in rep["regions"] if "unresolved_structured_ref" in flag_ids(r)]

    def test_a_cyrillic_table_and_column_resolve(self):
        self.assertEqual(self.unresolved(self.rep(["SUM(Продажи[Сумма])"])), [])

    def test_table_and_column_names_match_case_insensitively(self):
        self.assertEqual(self.unresolved(self.rep(["SUM(ПРОДАЖИ[сумма])", "SUM(продажи[СУММА])"])), [])

    def test_escape_forms_resolve(self):
        forms = ["Продажи[[#Headers],[Сумма]]", "Продажи[[#Data],[Сумма]]", "Продажи[[#Headers],[#Data],[Сумма]]", "Продажи[#Data]",
                 "Продажи[[#Totals],[Сумма]]", "Продажи[Unit Price]", "Продажи[[Unit Price]]", "Продажи[Qty'#]", "Продажи[[Регион]:[Unit Price]]"]
        rep = self.rep(["SUM(%s)" % f for f in forms], totals=1)
        self.assertEqual(self.unresolved(rep), [])

    def test_this_row_forms_resolve_inside_the_table(self):
        rows = [["Регион", "Сумма", "Дубль"], ["a", 1, F("Продажи[[#This Row],[Сумма]]*2", 2)], ["b", 2, F("[@Сумма]*2", 4)]]
        sh = Sheet("Data", rows, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table_xml("Продажи", "A1:C3", ["Регион", "Сумма", "Дубль"])}))
        self.assertEqual(self.unresolved(rep), [])

    def test_a_missing_table_or_column_is_still_unresolved(self):
        rep = self.rep(["SUM(Нет[Сумма])", "SUM(Продажи[Нет])"])
        self.assertEqual(len(self.unresolved(rep)), 2)

    def test_the_headers_and_data_special_items_cover_the_right_rows(self):
        rep = self.rep(["SUM(Продажи[[#Headers],[#Data],[Сумма]])", "SUM(Продажи[[#Headers],[Сумма]])"])
        reads = {r["ref"]: [x["ref"] for x in r["reads"]] for r in rep["regions"]}
        self.assertEqual((reads["F1"], reads["F2"]), (["B1:B3"], ["B1"]))


class CiMatchOnNumericCriteria(Tmp):
    def flags(self, formula, **kw):
        _, reg = self.run_formula(formula, **kw)
        return flag_ids(reg), [f["detail"] for f in reg["flags"] if f["id"] == "criteria_comparison"]

    def test_criteria_built_from_dates_numbers_or_arithmetic_are_numeric(self):
        for f in ('SUMIFS(B1:B4,C1:C4,">="&DATE(2024,1,1))', 'SUMIFS(B1:B4,C1:C4,"<="&EOMONTH(TODAY(),0))', 'COUNTIFS(C1:C4,">"&A1)',
                  'SUMIF(C1:C4,">5",B1:B4)', 'SUMIFS(B1:B4,C1:C4,">="&A1+1)', 'COUNTIF(C1:C4,"<>"&EDATE(A1,1))', "COUNTIF(C1:C4,TODAY())"):
            with self.subTest(formula=f):
                self.assertNotIn("ci_match", self.flags(f)[0])

    def test_a_formula_cell_holding_a_date_function_or_a_cached_number_is_numeric(self):
        rows = [[None] * 5, [None] * 5, [None] * 5, [None] * 5, [None] * 5 + [F("DATE(2024,1,1)", 45292)]]
        self.assertNotIn("ci_match", flag_ids(self.run_formula('SUMIFS(B1:B4,C1:C4,">="&F5)', extra_rows=rows[4:])[1]))

    def test_the_fifteen_digit_detail_stays_on_a_built_comparison(self):
        self.assertEqual(self.flags('SUMIFS(B1:B4,C1:C4,">="&DATE(2024,1,1))')[1], ["SUMIFS 15sig"])

    def test_unknown_or_text_operands_still_flag(self):
        for f in ('SUMIFS(B1:B4,C1:C4,">="&D1)', 'SUMIFS(B1:B4,C1:C4,">="&C1)', 'SUMIFS(B1:B4,C1:C4,"x"&A1)', 'SUMIFS(B1:B4,C1:C4,">="&TEXT(A1,"0"))'):
            with self.subTest(formula=f):
                self.assertIn("ci_match", self.flags(f)[0])


class CaseVariantKeys(Tmp):
    def rep(self, rows, formula='VLOOKUP("x",A2:B4,2,FALSE)', state=None):
        rows = [r + [None, F(formula, 1) if i == 0 else None] for i, r in enumerate(rows)]
        return cw.analyze(make_book(self.tmp, [Sheet("S", rows, state=state)]))

    def src(self, rep):
        return [s for s in rep["sources"] if s["ref"].startswith("A1")][0]

    def test_a_text_column_counts_the_lowercase_values_with_more_than_one_spelling(self):
        rep = self.rep([["k", "v"], ["East", 1], ["east", 2], ["EAST", 3], ["West", 4], ["west", 5], ["North", 6]])
        self.assertEqual(self.src(rep)["case_variant_keys"], {"k": 2})
        self.assertFalse(rep["ci_match_cannot_bite"])

    def test_no_variants_means_ci_match_cannot_bite_and_the_report_says_so(self):
        rep = self.rep([["k", "v"], ["East", 1], ["West", 2], ["North", 3]])
        self.assertEqual(self.src(rep)["case_variant_keys"], {"k": 0})
        self.assertTrue(rep["ci_match_cannot_bite"])
        self.assertIn("ci_match cannot bite: no case variants", cw.render_text(rep))

    def test_a_masked_sheet_reports_no_counts(self):
        rep = self.rep([["k", "v"], ["East", 1], ["east", 2]], state="hidden")
        self.assertEqual(self.src(rep)["case_variant_keys"], {})
        self.assertFalse(rep["ci_match_cannot_bite"])

    def test_the_scan_is_capped(self):
        rows = [["k", "v"], ["East", 1], ["east", 2], ["West", 3]]
        with mock.patch.object(cw, "MAX_CASE_SCAN_ROWS", 2):
            self.assertEqual(self.src(self.rep(rows)).get("case_variant_keys"), {})


class DataTableAxesAndArrayCounts(Tmp):
    def dt(self, attrs, ref, hidden=False):
        rows = [["scen", None, None], [5, F("A2*2", 10)], [10, F("", 20, fa=dict(t="dataTable", ref=ref, **attrs))], [20, 40]]
        rep = cw.analyze(make_book(self.tmp, [Sheet("S", rows, state="hidden" if hidden else None)], parts={"[Content_Types].xml": CT_ONE_SHEET}))
        return rep, [r for r in rep["regions"] if r["kind"] == "datatable"][0]

    def test_a_two_way_table_reads_both_axis_ranges_exactly(self):
        _, reg = self.dt({"dt2D": "1", "dtr": "1", "r1": "A2", "r2": "B1"}, "B3:C4")
        axes = reg["data_table"]["axis_oracle_stanzas"]
        self.assertEqual([(a["axis"], a["ref"]) for a in axes], [("row", "B2:C2"), ("column", "A3:A4")])
        self.assertIn("range = 'B2:C2', header = false, all_varchar = true)", axes[0]["stanza"])
        self.assertIn("range = 'A3:A4', header = false, all_varchar = true)", axes[1]["stanza"])
        self.assertIn("ORACLE read", axes[0]["stanza"])

    def test_a_one_way_table_reads_its_single_axis(self):
        _, reg = self.dt({"dt2D": "0", "dtr": "0", "r1": "A2"}, "B3:B4")
        self.assertEqual([(a["axis"], a["ref"]) for a in reg["data_table"]["axis_oracle_stanzas"]], [("column", "A3:A4")])

    def test_a_masked_data_table_has_no_axis_stanzas(self):
        rep, reg = self.dt({"dt2D": "0", "dtr": "0", "r1": "A2"}, "B3:B4", hidden=True)
        self.assertIsNone(reg["data_table"])
        self.assertNotIn("ORACLE read", json.dumps(rep["regions"]))

    def array_book(self, rows):
        return cw.analyze(one_sheet(self.tmp, rows))

    def test_a_fixed_cse_array_counts_every_cell_of_its_ref_and_is_not_called_dynamic(self):
        rep = self.array_book([[1, F("A1:A4*2", 2, fa={"t": "array", "ref": "B1:B4"})], [2, 4], [3, 6], [4, 8]])
        reg = region(rep, "S", "B1:B4")
        self.assertEqual(reg["cells"], 4)
        self.assertEqual(rep["routes"]["C"]["cells"], 4)
        self.assertTrue(any("fixed-size array formula" in r for r in reg["reasons"]), reg["reasons"])
        self.assertFalse(any("size is dynamic" in r for r in reg["reasons"]), reg["reasons"])

    def test_a_spill_and_a_cse_array_of_a_dynamic_function_stay_dynamic(self):
        rows = [[1, F("SORT(A1:A3)", 1, fa={"t": "array", "ref": "B1:B3"}, ca={"cm": "1"}), F("SORT(A1:A3)", 1, fa={"t": "array", "ref": "C1:C3"})],
                [2, 2, 2], [3, 3, 3]]
        rep = self.array_book(rows)
        for ref, kind in (("B1:B3", "spill"), ("C1:C3", "array")):
            with self.subTest(ref=ref):
                reg = region(rep, "S", ref)
                self.assertEqual((reg["kind"], reg["cells"]), (kind, 3))
                self.assertTrue(any("size is dynamic" in r for r in reg["reasons"]), reg["reasons"])

    def test_function_inventory_counts_the_cells_of_the_array_ref(self):
        rep = self.array_book([[1, F("SUM(A1:A2)*A1:A2", 2, fa={"t": "array", "ref": "B1:B2"})], [2, 4]])
        self.assertEqual(rep["functions"]["SUM"]["cells"], 2)



def shared_f(text, ref, si="0", v=1):
    return F(text, v, fa={"t": "shared", "ref": ref, "si": si})


def shared_child(si="0", v=1):
    return F("", v, fa={"t": "shared", "si": si})


class CiMatchComparedSides(Tmp):
    def rep(self, sheets):
        return cw.analyze(make_book(self.tmp, sheets))

    def keys(self, name, vals):
        return Sheet(name, [["k", "v"]] + [[k, i + 1] for i, k in enumerate(vals)])

    def test_a_lookup_value_spelled_differently_from_the_key_range_can_bite(self):
        look = Sheet("Sheet1", [["k", "v", "w"]] + [[k, i + 1, F(f"VLOOKUP(A{i + 2},Sheet2!$A$2:$B$4,2,FALSE)", 1)] for i, k in enumerate(["abc", "def", "ghi"])])
        rep = self.rep([look, self.keys("Sheet2", ["ABC", "DEF", "GHI"])])
        self.assertIn("ci_match", flag_ids(region(rep, "Sheet1", "C2:C4")))
        self.assertFalse(rep["ci_match_cannot_bite"])

    def test_a_lookup_value_and_key_range_spelled_alike_cannot_bite(self):
        look = Sheet("Sheet1", [["k", "v", "w"]] + [[k, i + 1, F(f"VLOOKUP(A{i + 2},Sheet2!$A$2:$B$4,2,FALSE)", 1)] for i, k in enumerate(["abc", "def", "ghi"])])
        rep = self.rep([look, self.keys("Sheet2", ["abc", "def", "ghi"])])
        self.assertTrue(rep["ci_match_cannot_bite"])

    def test_a_criteria_literal_spelled_differently_from_the_keys_can_bite(self):
        sh = Sheet("S", [["k", "v", None, F('SUMIF(A2:A4,"east",B2:B4)', 1)], ["East", 1], ["West", 2], ["North", 3]])
        rep = self.rep([sh])
        self.assertIn("ci_match", flag_ids(region(rep, "S", "D1")))
        self.assertFalse(rep["ci_match_cannot_bite"])

    def test_a_criteria_literal_spelled_like_the_keys_cannot_bite(self):
        sh = Sheet("S", [["k", "v", None, F('SUMIF(A2:A4,"East",B2:B4)', 1)], ["East", 1], ["West", 2], ["North", 3]])
        self.assertTrue(self.rep([sh])["ci_match_cannot_bite"])

    def test_a_lookup_value_outside_any_source_is_not_known(self):
        sh = Sheet("S", [["k", "v", None, F("VLOOKUP(Z9,A2:B4,2,FALSE)", 1)], ["East", 1], ["West", 2], ["North", 3]])
        self.assertFalse(self.rep([sh])["ci_match_cannot_bite"])


class RelativeFlagsAcrossTheRegion(Tmp):
    ROWS = [[1, 1], ["east", "East"]]

    def book(self, formula_cells):
        rows = [self.ROWS[0] + [formula_cells[0]], self.ROWS[1] + [formula_cells[1]]]
        return cw.analyze(one_sheet(self.tmp, rows))

    def test_a_numeric_first_row_does_not_clear_ci_match_for_the_text_rows_below(self):
        rep = self.book([F("A1=B1", 1), F("A2=B2", 1)])
        self.assertIn("ci_match", flag_ids(region(rep, "S", "C1:C2")))

    def test_the_same_holds_for_a_shared_formula(self):
        rep = self.book([shared_f("A1=B1", "C1:C2"), shared_child()])
        self.assertIn("ci_match", flag_ids(region(rep, "S", "C1:C2")))

    def test_a_numeric_lookup_value_in_the_first_row_does_not_clear_the_text_rows(self):
        rows = [[1, "k", F("VLOOKUP(A1,$E$1:$F$2,2,FALSE)", 1)], ["east", "k", F("VLOOKUP(A2,$E$1:$F$2,2,FALSE)", 1)]]
        rows[0] += [None, 1, "x"]
        rows[1] += [None, "East", "y"]
        rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertIn("ci_match", flag_ids(region(rep, "S", "C1:C2")))

    def test_an_all_numeric_region_still_clears_ci_match(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, 1, F("A1=B1", 1)], [2, 2, F("A2=B2", 1)]]))
        self.assertNotIn("ci_match", flag_ids(region(rep, "S", "C1:C2")))


class RelativeDependenciesAcrossTheRegion(Tmp):
    def test_a_later_row_reading_a_random_cell_makes_the_region_volatile_and_x(self):
        rep = cw.analyze(one_sheet(self.tmp, [[1, F("A1*2", 2)], [F("RAND()", 0.5), F("A2*2", 1)]]))
        reg = region(rep, "S", "B1:B2")
        self.assertTrue(reg["volatile_dep"])
        self.assertEqual(reg["route"], "X")

    def test_a_later_row_reading_a_hidden_sheet_through_a_chain_stays_dependent(self):
        sheets = [Sheet("S", [[1, F("A1*2", 2)], [F("Hid!A1+0", 1), F("A2*2", 2)]]), Sheet("Hid", [[5]], state="hidden")]
        rep = cw.analyze(make_book(self.tmp, sheets))
        reg = region(rep, "S", "B1:B2")
        self.assertTrue(reg["hidden_dep"])
        self.assertEqual(reg["hidden_dep_via"], "transitive")


class MatchModeBooleans(Tmp):
    check = Routes.check

    def test_false_is_exact_and_true_is_approximate_for_match_and_xmatch_and_xlookup(self):
        self.check("MATCH(C1,C1:C4,FALSE)", "T", ["ci_match"], ["approx_match"])
        self.check("MATCH(C1,C1:C4,TRUE)", "C", ["approx_match"])
        self.check("_xlfn.XMATCH(C1,C1:C4,FALSE)", "T", [], ["approx_match"])
        self.check("_xlfn.XLOOKUP(C1,C1:C4,B1:B4,0,FALSE)", "T", [], ["approx_match"])
        self.check("_xlfn.XLOOKUP(C1,C1:C4,B1:B4,0,TRUE)", "C", ["approx_match"])


class QualifiedNamesResolveInTheirSheet(Tmp):
    def test_a_sheet_qualified_name_uses_that_sheets_scope(self):
        names = ('<definedNames><definedName name="Rate" localSheetId="0">Main!$B$1</definedName>'
                 '<definedName name="Rate" localSheetId="1">Other!$B$1</definedName></definedNames>')
        sheets = [Sheet("Main", [[None, 1], [F("Other!Rate*2", 2)]]), Sheet("Other", [[None, 9]], state="hidden")]
        rep = cw.analyze(make_book(self.tmp, sheets, names=names))
        reg = region(rep, "Main", "A2")
        self.assertEqual([r["sheet"] for r in reg["reads"]], ["Other"])
        self.assertEqual(reg["hidden_dep_via"], "direct")


class HeaderInputsWithoutARecordedValue(Tmp):
    def test_a_header_input_whose_value_was_not_recorded_is_dropped_not_formatted(self):
        rows = [["Code", "Name", "Cost", 0.1, 0.2]] + [[f"C{r}", f"Item {r}", 100 + r, F(f"$C{r}*(1+D$1)", 1), F(f"$C{r}*(1+E$1)", 1)] for r in range(2, 6)]
        with mock.patch.object(cw, "MAX_NUMVALS", 0):
            rep = cw.analyze(one_sheet(self.tmp, rows))
        self.assertNotEqual(rep["status"], "unreadable", rep.get("error"))
        src = rep["sources"][0]
        self.assertEqual(src["inputs_in_header"], [])
        self.assertNotIn("None", src["stanza"] or "")
        self.assertNotIn("number is 0", json.dumps(rep))


def stanza_strings(rep):
    """Every string in the report under a key that names a stanza."""
    out = []

    def walk(v, key=""):
        if isinstance(v, dict):
            for k, x in v.items():
                walk(x, k)
        elif isinstance(v, list):
            for x in v:
                walk(x, key)
        elif isinstance(v, str) and "stanza" in key and "refused" not in key:
            out.append(v)
    walk(rep)
    return out


class MalloyInterpolationInWorkbookText(Tmp):
    ROWS = [["Region", "Sales %{ x }%", "Calc %{ z }%"]] + [[f"r{i}", i, F(f"B{i + 1}*2", 1)] for i in range(1, 5)]

    def test_a_header_with_an_interpolation_marker_is_read_positionally_under_a_plain_alias(self):
        rep = cw.analyze(one_sheet(self.tmp, self.ROWS))
        stanzas = stanza_strings(rep)
        self.assertTrue(stanzas)
        for st in stanzas:
            self.assertNotIn("%{", st)
        self.assertIn('AS "column2"', rep["sources"][0]["stanza"])

    def test_a_table_header_with_an_interpolation_marker_falls_back_to_a_column_alias(self):
        sh = Sheet("Data", self.ROWS, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table_xml("T", "A1:C5", ["Region", "Sales %{ x }%", "Calc %{ z }%"])}))
        for st in stanza_strings(rep):
            self.assertNotIn("%{", st)

    def test_a_sheet_name_with_an_interpolation_marker_refuses_every_stanza(self):
        rep = cw.analyze(one_sheet(self.tmp, self.ROWS, name="Q1 %{y}%"))
        self.assertIsNone(rep["sources"][0]["stanza"])
        self.assertIn("%{", rep["sources"][0]["stanza_refused"])
        for st in stanza_strings(rep):
            self.assertNotIn("%{", st)

    def test_a_book_name_with_an_interpolation_marker_refuses_every_stanza(self):
        rep = cw.analyze(one_sheet(self.tmp, self.ROWS, filename="b%{y}%.xlsx"))
        self.assertIsNone(rep["sources"][0]["stanza"])
        for st in stanza_strings(rep):
            self.assertNotIn("%{", st)

    def test_a_note_or_oracle_read_with_an_interpolation_marker_is_refused(self):
        st = 'duckdb.sql("""\n  SELECT 1\n""")'
        self.assertEqual(cw.add_stanza_note(st, "a %{ b"), st)
        self.assertIsNone(cw.render_oracle_stanza("b.xlsx", "Q1 %{y}%", "A1:B2"))
        self.assertIsNone(cw.render_oracle_stanza("b%{y}.xlsx", "S", "A1:B2"))
        self.assertIsNone(cw.safe_ident("Sales %{ x }%", None))


class SecretCellsAreNulledInTheLift(Tmp):
    ROWS = [["Setting", "Value"], ["Host", "db1.example"], ["Password", "hunter2-fake"], ["Port", "5432"], ["User", "svc"], ["Region", "us"]]

    def rep(self, rows=None, **kw):
        return cw.analyze(one_sheet(self.tmp, rows or self.ROWS, name="Hosts"), **kw)

    def test_a_value_beside_a_password_label_is_nulled_by_row(self):
        rep = self.rep()
        st = rep["sources"][0]["stanza"]
        self.assertIn('CASE WHEN __r IN (3) THEN NULL ELSE "Value" END', st)
        self.assertIn("cells masked as secrets are set to NULL", st)
        self.assertNotIn("hunter2-fake", json.dumps(rep) + cw.render_text(rep))

    def test_a_secret_cell_named_on_the_command_line_is_nulled_by_row(self):
        rep = self.rep(secret_cells=("Hosts!B6=API_TOKEN",))
        self.assertRegex(rep["sources"][0]["stanza"], r'CASE WHEN __r IN \([^)]*\b6\b[^)]*\) THEN NULL ELSE "Value" END')

    def test_a_block_with_no_secret_cell_has_no_null_mechanism(self):
        rep = self.rep([["Setting", "Value"], ["Host", "db1"], ["Port", "5432"], ["Region", "us"]])
        self.assertNotIn("__r", rep["sources"][0]["stanza"])


class PivotPageFilterOnAGroupedField(Tmp):
    def page_items(self, labels, item="1"):
        group = PivotDateGroups.GROUP.format(n=len(labels), items="".join(f'<s v="{x}"/>' for x in labels)) if labels else \
            '<cacheField name="Date" numFmtId="14"><sharedItems/></cacheField>'
        parts = pivot_parts(page=False, extra_cache_fields=group)
        pv = parts["xl/pivotTables/pivotTable1.xml"]
        items = "".join(f'<item x="{i}"/>' for i in range(4))
        pv = pv.replace('<pivotField axis="axisPage"/>', f'<pivotField axis="axisPage"/><pivotField axis="axisPage"><items count="4">{items}</items></pivotField>')
        pv = pv.replace('<pivotFields count="3">', '<pivotFields count="4">')
        pv = pv.replace("<dataFields", f'<pageFields count="1"><pageField fld="3" item="{item}" hier="-1"/></pageFields><dataFields')
        parts["xl/pivotTables/pivotTable1.xml"] = pv
        sh = Sheet("P", [[1]], rels=[("pivotTable", "../pivotTables/pivotTable1.xml", "rId1")])
        return cw.analyze(make_book(self.tmp, [Sheet("Data", [["Region", "Amount"], ["N", 1]]), sh], parts=parts))["pivots"][0]["page_items"]

    def test_a_selected_group_item_prints_its_group_label(self):
        self.assertEqual(self.page_items(PivotDateGroups.LABELS), [{"field": "Date", "item": "Jan"}])

    def test_a_selected_item_that_cannot_be_named_is_never_called_all(self):
        items = self.page_items(None)
        self.assertEqual(items, [{"field": "Date", "item": "item 1"}])


class FallbackReportIsRebuilt(Tmp):
    def test_a_failure_after_the_report_is_assembled_returns_only_the_safe_fields(self):
        path = one_sheet(self.tmp, [["Password", "hunter2-fake"], ["a", 1], [F("B2*2", 2)]])
        with mock.patch.object(cw, "not_read", side_effect=TypeError("boom")):
            rep = cw.analyze(path)
        self.assertEqual(rep["status"], "unreadable")
        self.assertEqual(rep["error"]["kind"], "internal")
        self.assertEqual(set(rep), {"workbook", "status", "error", "package", "security", "masking_note"})
        self.assertNotIn("hunter2-fake", json.dumps(rep))


class MalformedPartsDoNotAbortTheReport(Tmp):
    def test_a_table_with_fewer_columns_than_its_ref_is_still_reported(self):
        rows = [["a", "b", "c"], [1, 2, 3], [4, 5, 6]]
        sh = Sheet("Data", rows, rels=[("table", "../tables/table1.xml", "rId1")],
                   after='<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
        rep = cw.analyze(make_book(self.tmp, [sh], parts={"xl/tables/table1.xml": table_xml("T", "A1:C3", ["a", "b"])}))
        self.assertEqual(rep["status"], "ok", rep.get("error"))

    def lzma_book(self, member):
        src = one_sheet(self.tmp, [["a", 1], ["b", 2]] * 20)
        out = os.path.join(self.tmp, "lz.xlsx")
        with zipfile.ZipFile(src) as zin, zipfile.ZipFile(out, "w") as zout:
            for info in zin.infolist():
                kind = zipfile.ZIP_LZMA if info.filename == member else zipfile.ZIP_DEFLATED
                zout.writestr(info.filename, zin.read(info.filename), compress_type=kind)
        raw = bytearray(pathlib.Path(out).read_bytes())
        with zipfile.ZipFile(out) as z:
            info = z.getinfo(member)
        start = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra) + 12
        raw[start:start + 40] = b"\xff" * 40
        pathlib.Path(out).write_bytes(bytes(raw))
        return out

    def test_a_corrupt_lzma_member_is_rejected_like_a_corrupt_deflate_one(self):
        rep = cw.analyze(self.lzma_book("xl/workbook.xml"))
        self.assertEqual(rep["error"]["kind"], "no_workbook")
        self.assertIn({"part": "xl/workbook.xml", "reason": "bad_member"}, rep["package"]["rejected"])

    def test_deeply_nested_parentheses_route_the_region_nr_and_the_report_continues(self):
        deep = "(" * 400 + "A1" + ")" * 400
        rep = cw.analyze(one_sheet(self.tmp, [[1, 2, F(f"INDEX(A1:A4,{deep})", 1), F("A1*2", 2)]]))
        self.assertEqual(rep["status"], "ok", rep.get("error"))
        reg = region(rep, "S", "C1")
        self.assertEqual(reg["route"], "NR")
        self.assertTrue(any("nest" in r for r in reg["reasons"]), reg["reasons"])
        self.assertEqual(region(rep, "S", "D1")["route"], "T")


class ManyExcludedRectanglesStayLinear(Tmp):
    def test_thousands_of_one_cell_array_formulas_do_not_make_the_sheet_scan_quadratic(self):
        n = 5000
        rows = [["k", "v", "w", "x"]] + [[f"k{i}", i, F(f"B{i + 2}*2", 1, fa={"t": "array", "ref": f"C{i + 2}"}), i] for i in range(n)]
        path = one_sheet(self.tmp, rows)
        t0 = time.monotonic()
        rep = cw.analyze(path)
        elapsed = time.monotonic() - t0
        self.assertEqual(rep["status"], "ok")
        self.assertLess(elapsed, 3.0)

    def test_the_rectangle_test_agrees_with_a_plain_scan(self):
        rects = [(2, 2, 4, 3), (7, 1, 7, 1), (10, 5, 12, 5)]
        for index_cap in (cw.MAX_RECT_INDEX_ROWS, 0):
            with mock.patch.object(cw, "MAX_RECT_INDEX_ROWS", index_cap):
                hit = cw.rect_hit(rects)
                got = {(r, c) for r in range(1, 14) for c in range(1, 7) if hit(r, c)}
            want = {(r, c) for r in range(1, 14) for c in range(1, 7) if any(a <= r <= x and b <= c <= y for a, b, x, y in rects)}
            self.assertEqual(got, want)


class ConfigSheetNames(Tmp):
    def masked(self, names):
        rep = cw.analyze(make_book(self.tmp, [Sheet(n, [["a", 1], ["b", 2]]) for n in names]))
        return {sh["name"]: sh["masked"] for sh in rep["sheets"]}

    def test_a_config_word_followed_by_a_separator_digit_or_capital_masks_the_sheet(self):
        names = ["Config", "_Config", "Settings", "Config_Prod", "Credentials_DB", "Secrets2024", "SettingsV2", "Passwords", "API Keys", "Keys", "Config Prod",
                 "Configuration"]
        self.assertEqual(self.masked(names), {n: True for n in names})

    def test_an_ordinary_word_that_merely_starts_with_one_is_not_masked(self):
        names = ["Reconfig", "Keystone", "Configurator", "Secretary", "Passwordless", "Connectionsmith"]
        self.assertEqual(self.masked(names), {n: False for n in names})


class RejectedPartFlags(Tmp):
    def test_a_part_that_fails_to_parse_is_malformed_not_a_forged_size_header(self):
        with mock.patch.object(cw, "parse_sheet", side_effect=ZeroDivisionError):
            rep = cw.analyze(one_sheet(self.tmp, [[1, 2]]))
        self.assertIn("malformed_xml", sec_ids(rep))
        self.assertNotIn("bad_member", sec_ids(rep))

    def test_line_and_paragraph_separators_are_flattened_and_spelled_as_escapes(self):
        self.assertEqual(cw.flat("a\u2028b\u2029c"), "a b c")
        self.assertIn("\\u2028", cw._CTRL.pattern)


class ConfigSheetNames(unittest.TestCase):
    def test_ordinary_key_sheets_are_not_config(self):
        for nm in ("Key Metrics", "Key Assumptions", "Key Inputs", "Key_Data", "Key", "Keystone", "Reconfig"):
            with self.subTest(name=nm):
                self.assertFalse(cw.is_config_sheet(nm))

    def test_config_words_and_api_keys_still_are(self):
        for nm in ("Config_Prod", "Credentials_DB", "Secrets2024", "SettingsV2", "Passwords", "API Keys", "api_key"):
            with self.subTest(name=nm):
                self.assertTrue(cw.is_config_sheet(nm))


class CiMatchOperandsAreSingleTokens(Tmp):
    def rep(self, rows):
        return cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))

    def test_a_concatenated_right_side_is_not_read_as_its_first_cell(self):
        rows = [["k", "j", "x", "f"]] + [["abx", "ab", "X", F(f"A{r}=B{r}&C{r}", 1)] for r in range(2, 6)]
        rep = self.rep(rows)
        self.assertIn("ci_match", flag_ids(region(rep, "S", "D2:D5")))
        self.assertFalse(rep["ci_match_cannot_bite"])

    def test_a_concatenated_left_side_is_not_read_as_its_last_cell(self):
        rows = [["k", "f"]] + [["ABX", F(f'A{r}&""="abx"', 1)] for r in range(2, 6)]
        rep = self.rep(rows)
        self.assertIn("ci_match", flag_ids(region(rep, "S", "B2:B5")))
        self.assertFalse(rep["ci_match_cannot_bite"])


class PositionFallbackFlag(Tmp):
    def shared(self, formula):
        sh = lambda: F("", 1, fa=dict(t="shared", si="0"))
        return [["a", 1, F(formula, "a1", fa=dict(t="shared", ref="C1:C3", si="0"))], ["b", 2, sh()], ["c", 3, sh()]]

    def test_an_exhausted_position_budget_does_not_flag_a_pure_concat(self):
        with mock.patch.object(cw, "MAX_POSITION_ANALYSES", 0):
            rep = cw.analyze(make_book(self.tmp, [Sheet("S", self.shared("A1&B1"))]))
        self.assertNotIn("ci_match", flag_ids(region(rep, "S", "C1:C3")))

    def test_an_exhausted_position_budget_still_flags_a_comparison(self):
        with mock.patch.object(cw, "MAX_POSITION_ANALYSES", 0):
            rep = cw.analyze(make_book(self.tmp, [Sheet("S", self.shared("A1=B1"))]))
        self.assertIn("ci_match", flag_ids(region(rep, "S", "C1:C3")))


class SharedFormulaScanIsMemoized(Tmp):
    def test_an_absolute_key_range_is_scanned_once_not_once_per_copy(self):
        n = 400
        sh = lambda: F("", 1, fa=dict(t="shared", si="0"))
        f = "IF(A1=B1,VLOOKUP(A1,$A$1:$B$40,2,FALSE),MATCH(B1,$B$1:$B$40,0))"
        rows = [[i, i, F(f, 1, fa=dict(t="shared", ref=f"C1:C{n}", si="0")) if i == 1 else sh()] for i in range(1, n + 1)]
        scans = []
        real = cw._scan_numeric_const

        def counting(*a):
            if a[1:] != (a[1], a[2], a[1], a[2]):
                scans.append(a)
            return real(*a)

        with mock.patch.object(cw, "_scan_numeric_const", counting):
            cw.analyze(make_book(self.tmp, [Sheet("S", rows)]))
        self.assertLessEqual(len(scans), 20)


class ManyCiSourceRegions(Tmp):
    def test_a_key_column_is_checked_for_variants_once_across_many_regions(self):
        n, m = 300, 100
        data = Sheet("Data", [["k", "v"]] + [[f"key{i}", i] for i in range(n)])
        summ = Sheet("Sum", [[F(f'SUMIF(Data!$A$2:$A${n + 1},"key{j}",Data!$B$2:$B${n + 1})', 1)] for j in range(m)])
        calls = []
        real = cw._has_variant

        def counting(d):
            calls.append(len(d))
            return real(d)

        with mock.patch.object(cw, "_has_variant", counting):
            rep = cw.analyze(make_book(self.tmp, [data, summ]))
        self.assertGreaterEqual(rep["ci_match"]["flagged_regions"], m)
        self.assertTrue(rep["ci_match_cannot_bite"])
        self.assertLessEqual(len([c for c in calls if c > 1]), 3)

    def test_a_criteria_spelled_differently_from_the_key_still_bites(self):
        data = Sheet("Data", [["k", "v"], ["Key1", 1], ["Key2", 2]])
        summ = Sheet("Sum", [[F('SUMIF(Data!$A$2:$A$3,"key1",Data!$B$2:$B$3)', 1)]])
        self.assertFalse(cw.analyze(make_book(self.tmp, [data, summ]))["ci_match_cannot_bite"])


if __name__ == "__main__":
    unittest.main()
