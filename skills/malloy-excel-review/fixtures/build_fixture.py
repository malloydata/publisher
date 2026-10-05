#!/usr/bin/env python3
"""Build the malloy-excel-review recipe fixture (fixture.xlsx, fixture_1904.xlsx), CC0.

Stdlib only. Formula cells are always written without a cached <v>, except in the
`python` engine, which computes the values itself with Excel's semantics for the
specific formulas below. See README.md for what each engine can be labelled.

    build_fixture.py [--recalc excel|libreoffice|python] [--out-dir DIR]
"""
import argparse
import datetime
import math
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import zipfile
from xml.sax.saxutils import escape

HERE = os.path.dirname(os.path.abspath(__file__))
MODIFIED = datetime.datetime(2024, 6, 30, 12, 0, 0)
EPOCH_1900 = datetime.date(1899, 12, 30)
EPOCH_1904 = datetime.date(1904, 1, 1)
MC_SEED = 20240630
MC_ROWS = 1000
FLOWS = [1000, 1200, 1500, 1800, 2000]

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
XML_DECL = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'

STYLE_BOLD, STYLE_DATE, STYLE_PCT, STYLE_NUM2 = 1, 2, 3, 4


class XlError(str):
    """An Excel error value such as #N/A."""


NA = XlError("#N/A")


def col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def col_index(s):
    n = 0
    for ch in s:
        n = n * 26 + ord(ch) - 64
    return n


def parse_addr(a):
    m = re.fullmatch(r"([A-Z]+)(\d+)", a)
    return int(m.group(2)), col_index(m.group(1))


def addr(r, c):
    return "%s%d" % (col_letter(c), r)


def is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def fmt_num(x):
    x = float(x)
    if x.is_integer() and abs(x) < 1e15:
        return str(int(x))
    return repr(x)


def serial_1900(d):
    return (d - EPOCH_1900).days


def serial_1904(d):
    return (d - EPOCH_1904).days


class Cell:
    def __init__(self, value=None, formula=None, calc=None, style=0, shared=None, dtable=None):
        self.value, self.formula, self.calc = value, formula, calc
        self.style, self.shared, self.dtable = style, shared, dtable
        self.done = False


class Sheet:
    def __init__(self, wb, name, state=None):
        self.wb, self.name, self.state = wb, name, state
        self.cells = {}
        self.merges = []
        self.hidden_rows = set()
        self.table = None

    def put(self, a, value, style=0):
        self.cells[parse_addr(a)] = Cell(value=value, style=style)

    def f(self, a, text, calc, style=0, shared=None):
        self.cells[parse_addr(a)] = Cell(formula=text, calc=calc, style=style, shared=shared)

    def shared_run(self, cells, text, calc_fn, ref=None):
        """One shared formula: `text` is the master's (first cell's) formula; `ref` defaults to the cells' span."""
        si = self.wb.next_si()
        ref = ref or ("%s:%s" % (cells[0], cells[-1]) if len(cells) > 1 else cells[0])
        for i, a in enumerate(cells):
            shared = ("master", si, ref) if i == 0 else ("child", si, None)
            self.f(a, text if i == 0 else None, calc_fn(a), 0, shared)

    def data_table_cell(self, a, calc, dtable=None):
        self.cells[parse_addr(a)] = Cell(calc=calc, dtable=dtable, shared=None if dtable else ("dtchild", 0, None))

    def get(self, a):
        cell = self.cells.get(parse_addr(a))
        if cell is None:
            return None
        if cell.calc is not None and not cell.done:
            cell.value = cell.calc()
            cell.done = True
        return cell.value

    def rng(self, ref):
        a, b = ref.split(":")
        (r0, c0), (r1, c1) = parse_addr(a), parse_addr(b)
        return [self.get(addr(r, c)) for r in range(r0, r1 + 1) for c in range(c0, c1 + 1)]

    def column(self, letter):
        c = col_index(letter)
        last = max(r for (r, cc) in self.cells if cc == c)
        return [self.get(addr(r, c)) for r in range(1, last + 1)]


class Workbook:
    def __init__(self, date1904=False):
        self.sheets = []
        self.names = []
        self.date1904 = date1904
        self._si = -1

    def next_si(self):
        self._si += 1
        return self._si

    def sheet(self, name, state=None):
        s = Sheet(self, name, state)
        self.sheets.append(s)
        return s

    def __getitem__(self, name):
        return next(s for s in self.sheets if s.name == name)


# ---- Excel semantics for the specific formulas in the fixture --------------------------


def _wild(pattern):
    out = ""
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "~" and i + 1 < len(pattern):
            out += re.escape(pattern[i + 1])
            i += 1
        elif ch == "*":
            out += ".*"
        elif ch == "?":
            out += "."
        else:
            out += re.escape(ch)
        i += 1
    return re.compile(out + r"\Z", re.S | re.I)


def _try_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def crit_matcher(c):
    """COUNTIF/SUMIFS criteria: case-insensitive text, wildcards, text numbers match numeric equality."""
    if is_num(c):
        op, operand = "=", c
    else:
        m = re.match(r"(>=|<=|<>|>|<|=)?(.*)\Z", c, re.S)
        op, operand = m.group(1) or "=", m.group(2)
    num = float(operand) if is_num(operand) else _try_float(operand)
    if num is not None:
        def match(x):
            if op in ("=", "<>"):
                xn = float(x) if is_num(x) else (_try_float(x) if isinstance(x, str) else None)
                eq = xn is not None and xn == num
                return eq if op == "=" else not eq
            if not is_num(x):
                return False
            return {">": x > num, "<": x < num, ">=": x >= num, "<=": x <= num}[op]
        return match
    if operand == "":
        def match(x):
            blank = x is None or x == ""
            if op == "<>":
                return not blank
            return blank if c != "=" else x is None
        return match
    rx = _wild(operand)

    def match(x):
        hit = isinstance(x, str) and bool(rx.match(x))
        if op == "=":
            return hit
        return not hit if op == "<>" else False
    return match


def sumifs(sum_rng, *pairs):
    ms = [(r, crit_matcher(c)) for r, c in pairs]
    total = 0.0
    for i, v in enumerate(sum_rng):
        if all(m(r[i]) for r, m in ms) and is_num(v):
            total += v
    return total


def countif(rng, c):
    m = crit_matcher(c)
    return sum(1 for x in rng if m(x))


def countifs(*pairs):
    ms = [(r, crit_matcher(c)) for r, c in pairs]
    return sum(1 for i in range(len(ms[0][0])) if all(m(r[i]) for r, m in ms))


def averageifs(avg_rng, *pairs):
    ms = [(r, crit_matcher(c)) for r, c in pairs]
    return average([v for i, v in enumerate(avg_rng) if all(m(r[i]) for r, m in ms)])


def average(vals):
    nums = [v for v in vals if is_num(v)]
    return math.fsum(nums) / len(nums)  # fsum: sum() changed its rounding in Python 3.12


def approx_index(keys, key):
    """Binary search for the last position whose key <= `key`; None below the first. Models Excel, unverified on unsorted data."""
    lo, hi = 0, len(keys) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if keys[mid] <= key:
            lo = mid + 1
        else:
            hi = mid - 1
    return hi if hi >= 0 else None


def vlookup(key, keys, results, approx):
    if approx:
        i = approx_index(keys, key)
        return NA if i is None else results[i]
    for k, r in zip(keys, results):
        if k == key:
            return r
    return NA


def match_approx(keys, key):
    i = approx_index(keys, key)
    return NA if i is None else i + 1


def iterate_circular(opening, flow, rate):
    """Interest on average balance, iterated like Excel: up to 100 passes from 0, stopping under 0.001 change."""
    interest = closing = 0.0
    for _ in range(100):
        new_int = rate * (opening + closing) / 2
        new_close = opening + flow + new_int
        change = max(abs(new_int - interest), abs(new_close - closing))
        interest, closing = new_int, new_close
        if change < 0.001:
            break
    return interest, closing


# ---- the workbook ------------------------------------------------------------------------

# Region, Product, Qty, OrderDate, Price. Qty None = blank, "1" = text; OrderDate "2024-07-15" = text, 60 = 1900-02-29.
DATA_ROWS = [
    ("East", "Widget", 3, datetime.date(2024, 1, 1), 10.0),
    ("east", "Widget", 2, datetime.date(2024, 2, 1), 10.0),
    ("West", "Gadget", 5, datetime.date(2024, 3, 1), 20.5),
    ("West", "Gadget", "1", datetime.date(2024, 4, 1), 20.5),
    ("North", "Widget", 4, datetime.date(2024, 5, 1), 10.0),
    (None, "Gadget", 2, datetime.date(2024, 6, 1), 20.5),
    ("East", "Gizmo", 1, "2024-07-15", 7.25),
    ("East", "Widget", None, datetime.date(2024, 8, 1), 10.0),
    ("West", "Widget", 6, 60, 10.0),
    ("North", "Gizmo", 2, datetime.date(2024, 10, 1), 7.25),
    ("EAST", "Gadget", 1, datetime.date(2024, 11, 1), 20.5),
]
DATA_FIRST, DATA_LAST = 2, 1 + len(DATA_ROWS)
DATA_TOTAL = DATA_LAST + 1
TODAY_SERIAL = serial_1900(MODIFIED.date())


def date_cell(sheet, a, d, base):
    if isinstance(d, str):
        sheet.put(a, d)
    elif isinstance(d, int):
        sheet.put(a, d, STYLE_DATE)
    else:
        sheet.put(a, base(d), STYLE_DATE)


def build_data(wb):
    s = wb.sheet("Data")
    for i, h in enumerate(["Region", "Product", "Qty", "OrderDate", "Price", "Revenue"], 1):
        s.put(addr(1, i), h, STYLE_BOLD)
    for n, (region, product, qty, d, price) in enumerate(DATA_ROWS):
        r = DATA_FIRST + n
        if region is not None:
            s.put("A%d" % r, region)
        s.put("B%d" % r, product)
        if qty is not None:
            s.put("C%d" % r, qty)
        date_cell(s, "D%d" % r, d, serial_1900)
        s.put("E%d" % r, price, STYLE_NUM2)

        def revenue(r=r):
            q = s.get("C%d" % r)
            return (0.0 if q is None else float(q)) * s.get("E%d" % r)
        s.f("F%d" % r, "tbl_Sales[[#This Row],[Qty]]*tbl_Sales[[#This Row],[Price]]", revenue, STYLE_NUM2)
    s.put("A%d" % DATA_TOTAL, "Total")
    s.f("F%d" % DATA_TOTAL, "SUBTOTAL(109,tbl_Sales[Revenue])",
        lambda: sum(v for v in s.rng("F%d:F%d" % (DATA_FIRST, DATA_LAST)) if is_num(v)), STYLE_NUM2)
    s.table = {
        "name": "tbl_Sales", "ref": "A1:F%d" % DATA_TOTAL, "filter": "A1:F%d" % DATA_LAST,
        "columns": ["Region", "Product", "Qty", "OrderDate", "Price", "Revenue"],
    }
    return s


def build_ledger(wb):
    s = wb.sheet("Ledger")
    for a, v in (("A1", "Group"), ("B1", "Entry"), ("C1", "Amounts"), ("C2", "Debit"), ("D2", "Credit")):
        s.put(a, v, STYLE_BOLD)
    s.merges = ["A1:A2", "B1:B2", "C1:D1"]
    rows = [
        (3, "Ops", "Rent", 1000, 0), (4, "Ops", "Power", 250, 0), (5, "Ops", "Refund", 0, 100),
        (7, "Sales", "Alpha", 0, 700), (8, "Sales", "Beta", 0, 400), (9, "Sales", "Gamma", 0, 150),
    ]
    for r, g, e, d, c in rows:
        s.put("A%d" % r, g)
        s.put("B%d" % r, e)
        s.put("C%d" % r, d, STYLE_NUM2)
        s.put("D%d" % r, c, STYLE_NUM2)
    s.hidden_rows.add(5)
    for r, label, lo, hi in ((6, "Ops Subtotal", 3, 5), (10, "Sales Subtotal", 7, 9)):
        s.put("A%d" % r, label, STYLE_BOLD)
        for col in "CD":
            s.f("%s%d" % (col, r), "SUBTOTAL(9,%s%d:%s%d)" % (col, lo, col, hi),
                lambda col=col, lo=lo, hi=hi: sum(v for v in s.rng("%s%d:%s%d" % (col, lo, col, hi)) if is_num(v)),
                STYLE_NUM2)
    return s


LOOKUP_SORTED = [(0, 0.0), (100, 0.05), (500, 0.10), (1000, 0.15), (5000, 0.20)]
TEXT_KEYS = [("East", 1), ("West", 2), ("North", 3)]
LOOKUP_UNSORTED = [(500, 0.10), (0, 0.0), (5000, 0.20), (100, 0.05), (1000, 0.15)]


def build_lookup(wb):
    s = wb.sheet("Lookup")
    for a, v in (("A1", "Threshold"), ("B1", "Rate"), ("D1", "Threshold"), ("E1", "Rate"), ("G1", "Test"), ("H1", "Result")):
        s.put(a, v, STYLE_BOLD)
    for i, (k, v) in enumerate(LOOKUP_SORTED):
        s.put("A%d" % (2 + i), k)
        s.put("B%d" % (2 + i), v, STYLE_PCT)
    for i, (k, v) in enumerate(LOOKUP_UNSORTED):
        s.put("D%d" % (2 + i), k)
        s.put("E%d" % (2 + i), v, STYLE_PCT)
    sk, sr = [k for k, _ in LOOKUP_SORTED], [v for _, v in LOOKUP_SORTED]
    uk, ur = [k for k, _ in LOOKUP_UNSORTED], [v for _, v in LOOKUP_UNSORTED]
    no_match = lambda: vlookup(750, sk, sr, False)
    tests = [
        ("VLOOKUP TRUE, sorted bracket", "VLOOKUP(750,A2:B6,2,TRUE)", lambda: vlookup(750, sk, sr, True)),
        ("VLOOKUP TRUE, unsorted copy (garbage)", "VLOOKUP(750,D2:E6,2,TRUE)", lambda: vlookup(750, uk, ur, True)),
        ("VLOOKUP exact, no match", "VLOOKUP(750,A2:B6,2,FALSE)", no_match),
        ("IFERROR around the no match", "IFERROR(VLOOKUP(750,A2:B6,2,FALSE),0)",
         lambda: 0 if isinstance(no_match(), XlError) else no_match()),
        ("VLOOKUP TRUE below the first key", "VLOOKUP(-5,A2:B6,2,TRUE)", lambda: vlookup(-5, sk, sr, True)),
        ("MATCH, 3rd arg omitted, sorted", "MATCH(750,A2:A6)", lambda: match_approx(sk, 750)),
        ("MATCH, 3rd arg omitted, unsorted", "MATCH(750,D2:D6)", lambda: match_approx(uk, 750)),
    ]
    for i, (label, text, calc) in enumerate(tests):
        r = 2 + i
        s.put("G%d" % r, label)
        s.f("H%d" % r, text, calc, STYLE_PCT if i < 5 else 0)
    # Text-key table (mixed-case keys) and two lookups whose keys differ from the stored case
    for a, v in (("J1", "Key"), ("K1", "Code")):
        s.put(a, v, STYLE_BOLD)
    for i, (k, v) in enumerate(TEXT_KEYS):
        s.put("J%d" % (2 + i), k)
        s.put("K%d" % (2 + i), v)
    tk, tv = [k for k, _ in TEXT_KEYS], [v for _, v in TEXT_KEYS]
    ci_index = lambda key: next((i for i, k in enumerate(tk) if k.lower() == key.lower()), None)
    s.put("G9", "VLOOKUP exact, text key in another case")
    s.f("H9", 'VLOOKUP("east",J2:K4,2,FALSE)', lambda: tv[ci_index("east")] if ci_index("east") is not None else NA)
    s.put("G10", "MATCH exact, text key in another case")
    s.f("H10", 'MATCH("EAST",J2:J4,0)', lambda: ci_index("EAST") + 1)
    return s


def build_report(wb):
    s, d = wb.sheet("Report"), wb["Data"]
    rng = lambda col: d.rng("%s%d:%s%d" % (col, DATA_FIRST, col, DATA_LAST))
    s.put("A1", 1)
    s.put("C1", "min Qty (A1)")
    s.put("B2", serial_1900(datetime.date(2024, 1, 1)), STYLE_DATE)
    s.put("C2", "report start")
    rows = [
        (3, "SUMIFS region = east (case-insensitive)", 'SUMIFS(Data!$F$2:$F$12,Data!$A$2:$A$12,"east")',
         lambda: sumifs(rng("F"), (rng("A"), "east")), STYLE_NUM2),
        (4, "SUMIFS Qty >= A1", 'SUMIFS(Data!$F$2:$F$12,Data!$C$2:$C$12,">="&A1)',
         lambda: sumifs(rng("F"), (rng("C"), ">=" + fmt_num(s.get("A1")))), STYLE_NUM2),
        (5, "SUMIFS region wildcard *st", 'SUMIFS(Data!$F$2:$F$12,Data!$A$2:$A$12,"*st")',
         lambda: sumifs(rng("F"), (rng("A"), "*st")), STYLE_NUM2),
        (6, "SUMIFS blank region", 'SUMIFS(Data!$F$2:$F$12,Data!$A$2:$A$12,"")',
         lambda: sumifs(rng("F"), (rng("A"), "")), STYLE_NUM2),
        (7, "AVERAGE Qty (skips text and blanks)", "AVERAGE(Data!C2:C12)", lambda: average(rng("C")), 0),
        (8, "COUNT Qty", "COUNT(Data!C2:C12)", lambda: sum(1 for x in rng("C") if is_num(x)), 0),
        (9, "COUNTA Qty", "COUNTA(Data!C2:C12)", lambda: sum(1 for x in rng("C") if x is not None), 0),
        (10, "COUNTIF Qty = 1 (matches text 1)", "COUNTIF(Data!C:C,1)", lambda: countif(d.column("C"), 1), 0),
        (11, "COUNTIF region = east (case-insensitive)", 'COUNTIF(Data!A:A,"east")',
         lambda: countif(d.column("A"), "east"), 0),
        (12, "SUM full column incl. total row (double count)", "SUM(Data!F:F)",
         lambda: sum(v for v in d.column("F") if is_num(v)), STYLE_NUM2),
        (13, "East plus tax (hardcoded 1.08)", "B3*1.08", lambda: s.get("B3") * 1.08, STYLE_NUM2),
        (14, "Days since report start (TODAY)", "TODAY()-B2", lambda: TODAY_SERIAL - s.get("B2"), 0),
        (15, "SUMIFS OrderDate >= B2 (drops the text date and serial 60)", 'SUMIFS(Data!$F$2:$F$12,Data!$D$2:$D$12,">="&B2)',
         lambda: sumifs(rng("F"), (rng("D"), ">=" + fmt_num(s.get("B2")))), STYLE_NUM2),
    ]
    for r, label, text, calc, style in rows:
        s.put("A%d" % r, label)
        s.f("B%d" % r, text, calc, style)
    # Second context for each measure, in D3:D15 (inputs D1 and D2): another case, another operator, blanks again
    s.put("D1", 3)
    s.put("E1", "min Qty, context 2 (D1)")
    s.put("D2", serial_1900(datetime.date(2024, 6, 1)), STYLE_DATE)
    s.put("E2", "report start, context 2 (D2)")
    A, C, D, F = rng("A"), rng("C"), rng("D"), rng("F")
    ctx2 = [
        (3, "WEST, upper case", 'SUMIFS(Data!$F$2:$F$12,Data!$A$2:$A$12,"WEST")',
         lambda: sumifs(F, (A, "WEST")), STYLE_NUM2),
        (4, ">= D1", 'SUMIFS(Data!$F$2:$F$12,Data!$C$2:$C$12,">="&D1)',
         lambda: sumifs(F, (C, ">=" + fmt_num(s.get("D1")))), STYLE_NUM2),
        (5, "N*", 'SUMIFS(Data!$F$2:$F$12,Data!$A$2:$A$12,"N*")', lambda: sumifs(F, (A, "N*")), STYLE_NUM2),
        (6, "non-blank region", 'SUMIFS(Data!$F$2:$F$12,Data!$A$2:$A$12,"<>")',
         lambda: sumifs(F, (A, "<>")), STYLE_NUM2),
        (7, "AVERAGEIFS west", 'AVERAGEIFS(Data!$C$2:$C$12,Data!$A$2:$A$12,"west")',
         lambda: averageifs(C, (A, "west")), 0),
        (8, "COUNTIFS west, numeric Qty", 'COUNTIFS(Data!$A$2:$A$12,"WEST",Data!$C$2:$C$12,">=0")',
         lambda: countifs((A, "WEST"), (C, ">=0")), 0),
        (9, "COUNTIFS west, non-empty Qty", 'COUNTIFS(Data!$A$2:$A$12,"west",Data!$C$2:$C$12,"<>")',
         lambda: countifs((A, "west"), (C, "<>")), 0),
        (10, 'COUNTIF Qty = "2"', 'COUNTIF(Data!C:C,"2")', lambda: countif(d.column("C"), "2"), 0),
        (11, 'COUNTIF region = "NORTH"', 'COUNTIF(Data!A:A,"NORTH")', lambda: countif(d.column("A"), "NORTH"), 0),
        (12, "SUMIFS full column, west", 'SUMIFS(Data!F:F,Data!A:A,"west")',
         lambda: sumifs(d.column("F"), (d.column("A"), "west")), STYLE_NUM2),
        (13, "D3 plus tax (hardcoded 1.08)", "D3*1.08", lambda: s.get("D3") * 1.08, STYLE_NUM2),
        (14, "Days since D2 (TODAY)", "TODAY()-D2", lambda: TODAY_SERIAL - s.get("D2"), 0),
        (15, "SUMIFS OrderDate >= D2", 'SUMIFS(Data!$F$2:$F$12,Data!$D$2:$D$12,">="&D2)',
         lambda: sumifs(F, (D, ">=" + fmt_num(s.get("D2")))), STYLE_NUM2),
    ]
    for r, note, text, calc, style in ctx2:
        s.put("E%d" % r, "context 2: " + note)
        s.f("D%d" % r, text, calc, style)
    # Copied-down region I2:I9 with a plug at I6 (the shared group still spans it, as after an overwrite in Excel)
    for col, h in (("G", "Month"), ("H", "Units"), ("I", "Doubled")):
        s.put(col + "1", h, STYLE_BOLD)
    for i in range(8):
        s.put("G%d" % (2 + i), i + 1)
        s.put("H%d" % (2 + i), 10 * (i + 1))
    s.shared_run(["I%d" % r for r in (2, 3, 4, 5, 7, 8, 9)], "H2*2",
                 lambda x: (lambda x=x: s.get("H" + x[1:]) * 2), ref="I2:I9")
    s.put("I6", 999)
    return s


def build_assumptions_forecast(wb):
    a, f = wb.sheet("Assumptions"), wb.sheet("Forecast")
    a.put("A1", "Assumption", STYLE_BOLD)
    a.put("B1", "Value", STYLE_BOLD)
    inputs = [("Opening balance", 10000, 0), ("Growth rate", 0.05, STYLE_PCT), ("Asset cost", 12000, 0),
              ("Salvage value", 2000, 0), ("Life (periods)", 5, 0), ("Interest rate", 0.06, STYLE_PCT)]
    for i, (label, v, st) in enumerate(inputs):
        a.put("A%d" % (2 + i), label)
        a.put("B%d" % (2 + i), v, st)
    # Constant periods-across block: the wide layout the classifier can unpivot
    a.put("A10", "Period", STYLE_BOLD)
    a.put("A11", "Flow")
    for c, year, flow in zip("BCDEF", range(2025, 2030), FLOWS):
        a.put(c + "10", year, STYLE_BOLD)
        a.put(c + "11", flow)
    wb.names.append(("GrowthRate", "Assumptions!$B$3"))
    wb.names.append(("SalesData", "Data!$A$1:$F$%d" % DATA_LAST))

    f.put("A1", "Line", STYLE_BOLD)
    periods = list("BCDEF")
    for i, c in enumerate(periods):
        f.put("%s1" % c, 2025 + i, STYLE_BOLD)
    labels = ["Opening", "Flow", "Interest (avg balance)", "Closing", "Compounded",
              "Depreciation", "Net book value", "Cumulative flow"]
    for r, label in enumerate(labels, 2):
        f.put("A%d" % r, label)
    av = a.get
    circ = {}

    def solve(i):
        if i not in circ:
            opening = av("B2") if i == 0 else f.get("%s5" % periods[i - 1])
            circ[i] = iterate_circular(opening, av(periods[i] + "11"), av("B7"))
        return circ[i]

    pidx = lambda x: col_index(x[0]) - 2
    prev = lambda x: col_letter(col_index(x[0]) - 1)
    row = lambda n, cols=periods: ["%s%d" % (c, n) for c in cols]
    f.f("B2", "Assumptions!$B$2", lambda: av("B2"))
    f.shared_run(row(2, periods[1:]), "B5", lambda x: (lambda x=x: f.get("%s5" % prev(x))))
    f.shared_run(row(3), "Assumptions!B11", lambda x: (lambda x=x: av(x[0] + "11")))
    f.shared_run(row(4), "Assumptions!$B$7*AVERAGE(B2,B5)", lambda x: (lambda x=x: solve(pidx(x))[0]))
    f.shared_run(row(5), "B2+B3+B4", lambda x: (lambda x=x: solve(pidx(x))[1]))
    f.f("B6", "Assumptions!$B$2*(1+GrowthRate)", lambda: av("B2") * (1 + av("B3")))
    f.shared_run(row(6, periods[1:]), "B6*(1+GrowthRate)",
                 lambda x: (lambda x=x: f.get("%s6" % prev(x)) * (1 + av("B3"))))
    f.shared_run(row(7), "(Assumptions!$B$4-Assumptions!$B$5)/Assumptions!$B$6",
                 lambda x: (lambda: (av("B4") - av("B5")) / av("B6")))
    f.f("B8", "Assumptions!$B$4-B7", lambda: av("B4") - f.get("B7"))
    f.shared_run(row(8, periods[1:]), "B8-C7",
                 lambda x: (lambda x=x: f.get("%s8" % prev(x)) - f.get("%s7" % x[0])))
    f.shared_run(row(9), "SUM($B$3:B3)", lambda x: (lambda x=x: sum(f.rng("B3:%s3" % x[0]))))

    # One-variable What-If table on Assumptions: Excel requires the input cell (B3) on the table's own sheet
    a.put("D1", "Growth sensitivity", STYLE_BOLD)
    a.f("E2", "Forecast!F6", lambda: f.get("F6"))
    for i, g in enumerate([0.02, 0.04, 0.06, 0.08]):
        a.put("D%d" % (3 + i), g, STYLE_PCT)
        a.data_table_cell("E%d" % (3 + i), lambda g=g: av("B2") * (1 + g) ** 5,
                          {"ref": "E3:E6", "dt2D": "0", "dtr": "0", "r1": "B3"} if i == 0 else None)
    return a, f


def build_montecarlo(wb):
    s = wb.sheet("MonteCarlo")
    for col, h in (("A", "Draw"), ("B", "Value"), ("C", "Mean")):
        s.put(col + "1", h, STYLE_BOLD)
    rnd = random.Random(MC_SEED)
    dist = statistics.NormalDist(100, 15)
    draws = [dist.inv_cdf(rnd.random() or 1e-12) for _ in range(MC_ROWS)]
    for i in range(MC_ROWS):
        s.put("A%d" % (2 + i), i + 1)
    s.shared_run(["B%d" % (2 + i) for i in range(MC_ROWS)], "_xlfn.NORM.INV(RAND(),100,15)",
                 lambda x: (lambda x=x: draws[parse_addr(x)[0] - 2]))
    s.f("C2", "AVERAGE(B2:B%d)" % (MC_ROWS + 1), lambda: average(draws))
    return s


def build_config(wb):
    s = wb.sheet("_Config", state="veryHidden")
    s.put("A1", "Setting", STYLE_BOLD)
    s.put("B1", "Value", STYLE_BOLD)
    s.put("A2", "Password")
    s.put("B2", "dummy-Passw0rd-not-real")
    s.put("A3", "Environment")
    s.put("B3", "fixture")


def build_main_workbook():
    wb = Workbook()
    build_data(wb)
    build_ledger(wb)
    build_lookup(wb)
    build_report(wb)
    build_assumptions_forecast(wb)
    build_montecarlo(wb)
    build_config(wb)
    return wb


def build_1904_workbook():
    """The main Data sheet's dates in the 1904 system; serial 60 (1900-02-29) has no 1904 equivalent."""
    wb = Workbook(date1904=True)
    s = wb.sheet("Data")
    s.put("A1", "OrderDate", STYLE_BOLD)
    s.put("B1", "Amount", STYLE_BOLD)
    rows = [d for (_, _, _, d, _) in DATA_ROWS if not isinstance(d, int)]
    for i, d in enumerate(rows):
        date_cell(s, "A%d" % (2 + i), d, serial_1904)
        s.put("B%d" % (2 + i), 10 * (i + 1))
    numeric = [d for d in rows if isinstance(d, datetime.date)]
    s.put("C1", "Latest", STYLE_BOLD)
    s.f("C2", "MAX(A2:A%d)" % (1 + len(rows)), lambda: serial_1904(max(numeric)), STYLE_DATE)
    return wb


# ---- xlsx writer -------------------------------------------------------------------------


def sheet_xml(sheet, sst, with_values, first):
    rows = {}
    for (r, c), cell in sheet.cells.items():
        rows.setdefault(r, {})[c] = cell
    maxc = max([c for (_, c) in sheet.cells] or [1])
    out = [XML_DECL, '<worksheet xmlns="%s" xmlns:r="%s">' % (NS_MAIN, NS_REL)]
    out.append('<dimension ref="A1:%s"/>' % addr(max(rows), maxc))
    out.append('<sheetViews><sheetView workbookViewId="0"%s/></sheetViews>' % (' tabSelected="1"' if first else ""))
    out.append('<sheetFormatPr defaultRowHeight="15"/><sheetData>')
    for r in sorted(set(rows) | sheet.hidden_rows):
        out.append('<row r="%d"%s>' % (r, ' hidden="1"' if r in sheet.hidden_rows else ""))
        for c in sorted(rows.get(r, {})):
            out.append(cell_xml(r, c, rows[r][c], sst, with_values))
        out.append("</row>")
    out.append("</sheetData>")
    if sheet.merges:
        out.append('<mergeCells count="%d">%s</mergeCells>' % (
            len(sheet.merges), "".join('<mergeCell ref="%s"/>' % m for m in sheet.merges)))
    out.append('<pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/>')
    if sheet.table:
        out.append('<tableParts count="1"><tablePart r:id="rId1"/></tableParts>')
    out.append("</worksheet>")
    return "".join(out)


def value_xml(v):
    """(t attribute or None, <v> text) for a cached value."""
    if isinstance(v, XlError):
        return "e", escape(str(v))
    if isinstance(v, bool):
        return "b", "1" if v else "0"
    if isinstance(v, str):
        return "str", escape(v)
    return None, fmt_num(v)


def cell_xml(r, c, cell, sst, with_values):
    a = addr(r, c)
    s = ' s="%d"' % cell.style if cell.style else ""
    if cell.dtable is not None:
        d = cell.dtable
        f = '<f t="dataTable" ref="%s" dt2D="%s" dtr="%s" r1="%s"/>' % (d["ref"], d["dt2D"], d["dtr"], d["r1"])
    elif cell.shared and cell.shared[0] == "dtchild":
        return '<c r="%s"%s><v>%s</v></c>' % (a, s, fmt_num(cell.calc())) if with_values else ""
    elif cell.shared and cell.shared[0] == "master":
        f = '<f t="shared" ref="%s" si="%d">%s</f>' % (cell.shared[2], cell.shared[1], escape(cell.formula))
    elif cell.shared:
        f = '<f t="shared" si="%d"/>' % cell.shared[1]
    elif cell.formula is not None:
        f = "<f>%s</f>" % escape(cell.formula)
    else:
        v = cell.value
        if v is None:
            return ""
        if isinstance(v, str):
            return '<c r="%s"%s t="s"><v>%d</v></c>' % (a, s, sst.index(v))
        return '<c r="%s"%s><v>%s</v></c>' % (a, s, fmt_num(v))
    if not with_values:
        return '<c r="%s"%s>%s</c>' % (a, s, f)
    t, v = value_xml(cell.calc())
    return '<c r="%s"%s%s>%s<v>%s</v></c>' % (a, s, ' t="%s"' % t if t else "", f, v)


class SharedStrings:
    def __init__(self):
        self.items, self.idx, self.refs = [], {}, 0

    def add(self, s):
        if s not in self.idx:
            self.idx[s] = len(self.items)
            self.items.append(s)

    def index(self, s):
        self.refs += 1
        return self.idx[s]


def styles_xml():
    return XML_DECL + (
        '<styleSheet xmlns="%s">'
        '<numFmts count="1"><numFmt numFmtId="164" formatCode="yyyy\\-mm\\-dd"/></numFmts>'
        '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font>'
        '<font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="5">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
        '<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
        '<xf numFmtId="10" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
        '<xf numFmtId="2" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>" % NS_MAIN)


def table_xml(t):
    cols = []
    for i, name in enumerate(t["columns"], 1):
        attrs = ' id="%d" name="%s"' % (i, escape(name))
        if i == 1:
            attrs += ' totalsRowLabel="Total"'
        if name == "Revenue":
            cols.append("<tableColumn%s totalsRowFunction=\"sum\"><calculatedColumnFormula>"
                        "tbl_Sales[[#This Row],[Qty]]*tbl_Sales[[#This Row],[Price]]"
                        "</calculatedColumnFormula></tableColumn>" % attrs)
        else:
            cols.append("<tableColumn%s/>" % attrs)
    return XML_DECL + (
        '<table xmlns="%s" id="1" name="%s" displayName="%s" ref="%s" totalsRowCount="1">'
        '<autoFilter ref="%s"/><tableColumns count="%d">%s</tableColumns>'
        '<tableStyleInfo name="TableStyleMedium2" showFirstColumn="0" showLastColumn="0" '
        'showRowStripes="1" showColumnStripes="0"/></table>' % (
            NS_MAIN, t["name"], t["name"], t["ref"], t["filter"], len(cols), "".join(cols)))


CT = "application/vnd.openxmlformats-officedocument.spreadsheetml."


def workbook_parts(wb, with_values):
    sst = SharedStrings()
    for s in wb.sheets:
        for cell in s.cells.values():
            if isinstance(cell.value, str) and cell.formula is None and not cell.shared and cell.dtable is None:
                sst.add(cell.value)
    parts = {}
    sheets_xml, rels, overrides = [], [], []
    for i, s in enumerate(wb.sheets, 1):
        parts["xl/worksheets/sheet%d.xml" % i] = sheet_xml(s, sst, with_values, i == 1)
        state = ' state="%s"' % s.state if s.state else ""
        sheets_xml.append('<sheet name="%s" sheetId="%d"%s r:id="rId%d"/>' % (escape(s.name), i, state, i))
        rels.append('<Relationship Id="rId%d" Type="%s/worksheet" Target="worksheets/sheet%d.xml"/>' % (i, NS_REL, i))
        overrides.append('<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="%sworksheet+xml"/>' % (i, CT))
        if s.table:
            parts["xl/worksheets/_rels/sheet%d.xml.rels" % i] = XML_DECL + (
                '<Relationships xmlns="%s"><Relationship Id="rId1" Type="%s/table" Target="../tables/table1.xml"/>'
                "</Relationships>" % (NS_PKG_REL, NS_REL))
            parts["xl/tables/table1.xml"] = table_xml(s.table)
            overrides.append('<Override PartName="/xl/tables/table1.xml" ContentType="%stable+xml"/>' % CT)
    n = len(wb.sheets)
    rels += ['<Relationship Id="rId%d" Type="%s/styles" Target="styles.xml"/>' % (n + 1, NS_REL),
             '<Relationship Id="rId%d" Type="%s/sharedStrings" Target="sharedStrings.xml"/>' % (n + 2, NS_REL)]
    names = "".join('<definedName name="%s">%s</definedName>' % (k, escape(v)) for k, v in wb.names)
    parts["xl/workbook.xml"] = XML_DECL + (
        '<workbook xmlns="%s" xmlns:r="%s"><workbookPr%s/><bookViews><workbookView activeTab="0"/></bookViews>'
        "<sheets>%s</sheets>%s"
        '<calcPr calcId="191029" fullCalcOnLoad="1" iterate="1" iterateCount="100" iterateDelta="0.001"/></workbook>' % (
            NS_MAIN, NS_REL, ' date1904="1"' if wb.date1904 else "", "".join(sheets_xml),
            "<definedNames>%s</definedNames>" % names if names else ""))
    parts["xl/_rels/workbook.xml.rels"] = XML_DECL + '<Relationships xmlns="%s">%s</Relationships>' % (
        NS_PKG_REL, "".join(rels))
    parts["xl/styles.xml"] = styles_xml()
    parts["xl/sharedStrings.xml"] = XML_DECL + '<sst xmlns="%s" count="%d" uniqueCount="%d">%s</sst>' % (
        NS_MAIN, sst.refs, len(sst.items), "".join("<si><t>%s</t></si>" % escape(t) for t in sst.items))
    parts["docProps/core.xml"] = XML_DECL + (
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:creator>build_fixture.py</dc:creator>'
        '<dcterms:created xsi:type="dcterms:W3CDTF">2024-06-01T09:00:00Z</dcterms:created>'
        '<dcterms:modified xsi:type="dcterms:W3CDTF">%s</dcterms:modified></cp:coreProperties>' %
        MODIFIED.strftime("%Y-%m-%dT%H:%M:%SZ"))
    parts["docProps/app.xml"] = XML_DECL + (
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">'
        "<Application>Microsoft Excel</Application></Properties>")
    parts["_rels/.rels"] = XML_DECL + (
        '<Relationships xmlns="%s"><Relationship Id="rId1" Type="%s/officeDocument" Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/'
        'core-properties" Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" Type="%s/extended-properties" Target="docProps/app.xml"/></Relationships>' % (
            NS_PKG_REL, NS_REL, NS_REL))
    parts["[Content_Types].xml"] = XML_DECL + (
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="%ssheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="%sstyles+xml"/>'
        '<Override PartName="/xl/sharedStrings.xml" ContentType="%ssharedStrings+xml"/>'
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.'
        'core-properties+xml"/>'
        '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.'
        'extended-properties+xml"/>%s</Types>' % (CT, CT, CT, "".join(overrides)))
    return parts


def write_xlsx(path, wb, with_values):
    parts = workbook_parts(wb, with_values)
    first = ["[Content_Types].xml", "_rels/.rels"]
    # A fixed timestamp keeps the zip bytes (and the README's SHA-256) reproducible
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name in first + sorted(k for k in parts if k not in first):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, parts[name].encode("utf-8"))


EXCEL_STEPS = """\
Excel build: the files hold formulas with no cached values (fullCalcOnLoad is set).

  1. Quit Excel, then open %(main)s in a FRESH instance with no other workbook open
     (the iterative-calculation setting is taken from the first workbook opened). Confirm
     iterative calculation is on (Windows: File > Options > Formulas; macOS: Excel > Settings >
     Calculation) and that Forecast shows no circular-reference warning.
  2. Select tbl_Sales and Insert > PivotTable > New Worksheet. Rename the sheet Pivot, then
     right-click its tab > Move or Copy > (move to end) so the Pivot sheet is last.
  3. Leave "Add this data to the Data Model" UNCHECKED (an OLAP pivot offers neither calculated
     fields nor this grouping). Build the pivot: Region in Filters (a page field); OrderDate in
     Rows (grouping needs it); a calculated field (PivotTable Analyze > Fields, Items & Sets >
     Calculated Field) named Rev108 = Revenue * 1.08; Revenue as a value shown as "%% of Grand Total".
  4. Excel will not group a field that holds text. Retype Data!D8 as a real date (2024-07-15),
     refresh the pivot once, group OrderDate by Months, then retype Data!D8 back to the text
     "2024-07-15" (format the cell as Text first). Data!D8 must end as a string.
  5. Never refresh the pivot again: its cache keeps D8 as a date, which is deliberate.
  6. Save as .xlsx over fixtures/fixture.xlsx. Open %(y1904)s once and save it too.
  7. This procedure is reasoned from Excel's pivot behaviour and has not been run in Excel.
  8. Record the engine, the Excel version and both SHA-256 values in fixtures/README.md.
"""


def run_libreoffice(src_dir, out_dir):
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        sys.exit("soffice not found. Install it with: brew install --cask libreoffice")
    work = tempfile.mkdtemp()
    try:
        # A private profile keeps a running soffice from swallowing the headless conversion
        profile = "-env:UserInstallation=file://" + os.path.join(work, "profile")
        for name in ("fixture.xlsx", "fixture_1904.xlsx"):
            conv = os.path.join(work, "out")
            subprocess.run([soffice, profile, "--headless", "--convert-to", "xlsx", "--outdir", conv,
                            os.path.join(src_dir, name)], check=True)
            if not os.path.exists(os.path.join(conv, name)):
                sys.exit("soffice produced no %s" % name)
            shutil.move(os.path.join(conv, name), os.path.join(out_dir, name))
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--recalc", choices=["excel", "libreoffice", "python"], default="python")
    ap.add_argument("--out-dir", default=HERE)
    args = ap.parse_args(argv)
    if args.recalc == "libreoffice" and not (shutil.which("soffice") or shutil.which("libreoffice")):
        sys.exit("soffice not found. Install it with: brew install --cask libreoffice")
    os.makedirs(args.out_dir, exist_ok=True)
    staging = tempfile.mkdtemp() if args.recalc == "libreoffice" else args.out_dir
    try:
        main_path = os.path.join(staging, "fixture.xlsx")
        y1904_path = os.path.join(staging, "fixture_1904.xlsx")
        write_xlsx(main_path, build_main_workbook(), args.recalc == "python")
        write_xlsx(y1904_path, build_1904_workbook(), args.recalc == "python")
        if args.recalc == "excel":
            print(EXCEL_STEPS % {"main": main_path, "y1904": y1904_path})
        elif args.recalc == "libreoffice":
            run_libreoffice(staging, args.out_dir)
    finally:
        if staging != args.out_dir:
            shutil.rmtree(staging, ignore_errors=True)
    print("wrote fixture.xlsx and fixture_1904.xlsx to %s (%s engine)" % (args.out_dir, args.recalc))


if __name__ == "__main__":
    main()
