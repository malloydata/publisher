#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Recover the model inside an Excel workbook, and gate the cached values before they are trusted.

A workbook hands you cells, not a model. This reads the .xlsx zip directly
(stdlib only) and answers what an agent reading sheets one at a time cannot:

  * which formula cells are really ONE formula copied down (R1C1 regions, with
    shared formulas expanded), and whether each is row-local, a range aggregate
    or an absolute reference: a dimension, a measure, or a given;
  * which cells are data a source may lift (no formula, outside every array,
    spill, data-table and pivot range) and the read_xlsx stanza that lifts them;
  * whether the cached values can serve as a parity oracle at all;
  * which functions and attached code cannot be translated, and who owns them;
  * the cross-sheet dependency graph, its cycles, and how much of it is unknown.

It emits a routing table, not a verdict. Routes: T translate, C translate at a
stated cost, X stays in Excel, NR ask the user. Tells marked unconfirmed in the
output are spec-derived and have not been seen in a real file yet.

Usage:
    classify_workbook.py book.xlsx              # same as `classify book.xlsx`
    classify_workbook.py classify book.xlsx --json [--secret-cell 'Sheet!B3=VAR' ...]
    classify_workbook.py connections book.xlsx --config-out conn.json [--secret-cell ...] [--secrets-out PATH] [--force]
    classify_workbook.py run --secrets FILE -- npx @malloy-publisher/server@latest

The JSON carries customer structure (names, addresses, formulas, server names and
SQL). Keep it local. It never carries a cell value from a hidden, veryHidden or
config-named sheet, nor a password: secret masking is best effort, and rotation
is the real control. `connections` writes secrets only to a 0600 file outside
the work tree and prints variable names, never values.
"""

from __future__ import annotations

import argparse
import base64
import bisect
import datetime
import io
import json
import math
import os
import posixpath
import re
import subprocess
import sys
import unicodedata
import urllib.parse
import zipfile
import zlib
from collections import Counter, defaultdict, namedtuple
from xml.etree import ElementTree as ET

try:
    import lzma
    DECOMPRESS_ERRORS = (zlib.error, lzma.LZMAError)
except ImportError:
    DECOMPRESS_ERRORS = (zlib.error,)

# --------------------------------------------------------------------------
# Caps. All are module attributes so the tests can shrink them.
# --------------------------------------------------------------------------

MAX_ENTRIES = 20000
MAX_PART_BYTES = 256 * 1024 * 1024
MAX_TOTAL_BYTES = 1024 * 1024 * 1024
MAX_EDGES = 2000000
MAX_NUMVALS = 50000
MAX_CFB_SCAN = 64 * 1024 * 1024
MAX_TEXT_ROWS = 300
MAX_NULLED_ROWS = 200
MAX_PY_SCRIPTS = 200
MAX_PY_CODE_CHARS = 20000
MAX_ORACLE_STANZAS = 2000
MAX_VALIDATIONS = 200
MAX_CASE_COLS = 50
MAX_CASE_SCAN_ROWS = 200000
MAX_RECT_INDEX_ROWS = 200000
MAX_VALIDATION_ITEMS = 20
MAX_POSITION_ANALYSES = 20000
MAX_FORMULA_NEST = 100
NONNUMERIC_KINDS = ("s", "inlineStr", "str", "e", "b", "d")
TEXT_CELL_KINDS = ("s", "inlineStr", "str", "b", "d")

MAX_ROW = 1048576
MAX_COL = 16384
CFB_MAGIC = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"

MAX_ERROR_REFS = 20
ROUTE_ORDER = {"T": 0, "C": 1, "X": 2, "NR": 3}

# Tells taken from a spec or a researcher's memory, never from a file Excel
# wrote. Each is marked unconfirmed wherever it is reported.
UNCONFIRMED_TELLS = {
    "xll_udf": "_xll. prefix (and bare unknown names) in <f> for XLL / Excel-DNA / COM add-in functions",
    "unresolved_function": "_xludf. prefix in <f>",
    "addin_link": "[n]!Func(...) formulas into an .xla/.xlam add-in",
    "python_in_excel": "_xlfn._xlws.PY( in <f>",
    "web_extension": "xl/webextensions/ parts (Office.js add-ins)",
    "dde": "<ddeLink> in xl/externalLinks/",
    "custom_ui": "customUI/customUI*.xml onAction",
}


# --------------------------------------------------------------------------
# Bounded, DTD-refusing XML
# --------------------------------------------------------------------------

class Rejected(Exception):
    def __init__(self, reason, detail=""):
        Exception.__init__(self, reason)
        self.reason = reason
        self.detail = detail
        self.bytes = 0


class PackageError(Exception):
    def __init__(self, kind, message):
        Exception.__init__(self, message)
        self.kind = kind
        self.message = message


def ln(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def rid_of(el):
    for k, v in el.attrib.items():
        if k.endswith("}id"):
            return v
    return None


def sniff_encoding(data):
    """Pick the codec expat will pick: a BOM, else a zero byte in byte 0 or 1 means UTF-16."""
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"
    if data[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    if len(data) >= 2:
        if data[0] == 0 and data[1] != 0:
            return "utf-16-be"
        if data[1] == 0 and data[0] != 0:
            return "utf-16-le"
        if data[0] == 0 and data[1] == 0:
            raise Rejected("malformed")
    return "utf-8"


def decode_head(data, limit):
    enc = sniff_encoding(data)
    width = 2 if enc.startswith("utf-16") else 1
    return data[:limit * width].decode(enc, errors="replace")


_DTD_START = re.compile(r"<!(?:DOCTYPE|ENTITY)", re.I)
_DECL_ENC = re.compile(r"""^<\?xml[^>]*?\sencoding\s*=\s*["']([^"']*)["']""", re.I)
NATIVE_ENCODINGS = {"utf-8", "utf8", "us-ascii", "ascii", "iso-8859-1", "latin-1", "latin1", "utf-16", "utf-16le", "utf-16be", "utf16"}
PROLOG_WINDOW = 1 << 16


def _scan_prolog(text, complete):
    """'ok' once the root start tag is reached, 'more' when the window ended first."""
    i, n = 0, len(text)
    if text.startswith("\ufeff"):
        i = 1
    first = True
    while True:
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n:
            return "more"
        if text[i] != "<":
            raise Rejected("malformed")
        if i + 10 > n and not complete:
            return "more"
        if text.startswith("<?", i):
            if first:
                m = _DECL_ENC.match(text[i:i + 512])
                if m and m.group(1).lower() not in NATIVE_ENCODINGS:
                    raise Rejected("malformed")
            j = text.find("?>", i)
            if j < 0:
                return "more"
            i = j + 2
        elif text.startswith("<!--", i):
            j = text.find("-->", i)
            if j < 0:
                return "more"
            i = j + 3
        elif text.startswith("<!", i):
            raise Rejected("dtd" if _DTD_START.match(text, i) else "malformed")
        else:
            return "ok"
        first = False


def check_prolog(data):
    """Reject a DTD before the parser sees it, failing closed: a prolog that outruns the
    window is rescanned wider, and one that never reaches a root tag is refused. Only the
    prolog is read, so a streaming parse stays streaming."""
    limit = PROLOG_WINDOW
    while True:
        complete = limit >= len(data)
        if _scan_prolog(decode_head(data, limit), complete) == "ok":
            return
        if complete:
            raise Rejected("malformed")
        limit *= 8


def parse_xml(data):
    check_prolog(data)
    try:
        return ET.fromstring(data)
    except (ET.ParseError, ValueError, LookupError, UnicodeError):
        raise Rejected("malformed")


PARSE_ERRORS = (ET.ParseError, ValueError, LookupError, UnicodeError)


def iter_xml(data, events=("start", "end")):
    check_prolog(data)
    return ET.iterparse(io.BytesIO(data), events=events)


def bounded_read(zf, info, cap):
    """Count bytes actually read: a crafted header can understate file_size."""
    chunks, n = [], 0
    try:
        with zf.open(info) as fh:
            while True:
                b = fh.read(max(1, min(65536, cap - n + 1)))
                if not b:
                    break
                n += len(b)
                if n > cap:
                    err = Rejected("too_large", str(n))
                    err.bytes = n
                    raise err
                chunks.append(b)
    except (zipfile.BadZipFile, EOFError, RuntimeError, NotImplementedError, OSError) + DECOMPRESS_ERRORS:
        err = Rejected("bad_member")
        err.bytes = n
        raise err
    return b"".join(chunks)


def unsafe_name(n):
    if n.startswith("/") or n.startswith("\\") or re.match(r"^[A-Za-z]:", n):
        return True
    return ".." in n.replace("\\", "/").split("/")


class Package:
    def __init__(self, path):
        self.zf = zipfile.ZipFile(path)
        infos = self.zf.infolist()
        if len(infos) > MAX_ENTRIES:
            raise PackageError("too_many_entries", f"{len(infos)} zip entries exceeds the cap of {MAX_ENTRIES}")
        self.entries = len(infos)
        self.names = {}
        self.unsafe = 0
        for i in infos:
            if i.filename.endswith("/"):
                continue
            if unsafe_name(i.filename):
                self.unsafe += 1
                continue
            self.names[i.filename] = i
        self.bytes_read = 0
        self.parts_read = 0
        self.rejected = []
        self.total_hit = False

    def has(self, name):
        return name in self.names

    def content_types(self):
        if getattr(self, "_ctypes", None) is None:
            self._ctypes = {}
            root = self.xml("[Content_Types].xml") if self.has("[Content_Types].xml") else None
            for el in (root if root is not None else ()):
                if ln(el.tag) == "Override" and el.get("PartName"):
                    self._ctypes[el.get("PartName").lstrip("/")] = (el.get("ContentType") or "").lower()
        return self._ctypes

    def code_parts(self, prefix, ctype_sub, rel_sub=None):
        """Parts under their conventional path, plus any part a content type or the workbook relationships name."""
        found = set(n for n in self.under(prefix) if "/_rels/" not in n)
        found |= {n for n, ct in self.content_types().items() if ctype_sub in ct and n in self.names}
        if rel_sub:
            found |= {t for typ, t in getattr(self, "wb_rel_targets", ()) if rel_sub in typ.lower() and t in self.names}
        return sorted(found)

    def under(self, prefix):
        return sorted(n for n in self.names if n.startswith(prefix))

    def budget(self):
        return min(MAX_PART_BYTES, MAX_TOTAL_BYTES - self.bytes_read)

    def charge(self, n):
        self.bytes_read += n

    def read(self, name):
        info = self.names.get(name)
        if info is None:
            return None
        if self.total_hit:
            return None
        cap = self.budget()
        try:
            data = bounded_read(self.zf, info, max(cap, 0))
        except Rejected as e:
            total = e.reason == "too_large" and cap < MAX_PART_BYTES
            self.bytes_read += max(cap, 0) if total else e.bytes
            if total:
                self.total_hit = True
                self.rejected.append({"part": name, "reason": "total_cap"})
            else:
                self.rejected.append({"part": name, "reason": e.reason})
            return None
        self.bytes_read += len(data)
        self.parts_read += 1
        return data

    def xml(self, name):
        data = self.read(name)
        if data is None:
            return None
        try:
            return parse_xml(data)
        except Rejected as e:
            self.rejected.append({"part": name, "reason": e.reason})
            return None

    def xml_bytes(self, name):
        """Raw bytes for a streaming parse, after the prolog check."""
        data = self.read(name)
        if data is None:
            return None
        try:
            check_prolog(data)
        except Rejected as e:
            self.rejected.append({"part": name, "reason": e.reason})
            return None
        return data

    def rels(self, part):
        """{rId: (type_suffix, absolute_target_or_None, external)} for a part."""
        d, b = posixpath.split(part)
        root = self.xml(posixpath.join(d, "_rels", b + ".rels") if d else posixpath.join("_rels", b + ".rels"))
        out = {}
        if root is None:
            return out
        for r in root:
            if ln(r.tag) != "Relationship":
                continue
            typ = (r.get("Type") or "").rsplit("/", 1)[-1]
            tgt = r.get("Target") or ""
            if (r.get("TargetMode") or "").lower() == "external":
                out[r.get("Id")] = (typ, tgt, True)
                continue
            full = tgt[1:] if tgt.startswith("/") else posixpath.normpath(posixpath.join(d, tgt))
            out[r.get("Id")] = (typ, None if full.startswith("..") else full, False)
        return out


def open_package(path):
    with open(path, "rb") as fh:
        head = fh.read(8)
    if head == CFB_MAGIC:
        raise PackageError(*classify_cfb(path))
    if not zipfile.is_zipfile(path):
        raise PackageError("not_zip", "not a zip file: an .xlsx/.xlsm is a zip. Re-save it as .xlsx from Excel (CSV loses formulas; Google Sheets: File > Download > .xlsx).")
    try:
        pkg = Package(path)
    except zipfile.BadZipFile:
        raise PackageError("bad_zip", "the zip directory is corrupt")
    if "xl/workbook.bin" in pkg.names and "xl/workbook.xml" not in pkg.names:
        raise PackageError("xlsb", ".xlsb is binary and is not read: ask the user to re-save the workbook as .xlsx")
    return pkg


def classify_cfb(path):
    pats = {"enc": "EncryptedPackage".encode("utf-16-le"), "xls": "Workbook".encode("utf-16-le"),
            "xls2": "Book".encode("utf-16-le")}
    found, tail, scanned = set(), b"", 0
    with open(path, "rb") as fh:
        while scanned < MAX_CFB_SCAN:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            scanned += len(chunk)
            buf = tail + chunk
            for k, p in pats.items():
                if p in buf:
                    found.add(k)
            tail = buf[-32:]
    if "enc" in found:
        return ("encrypted", "password-protected workbook (an OLE EncryptedPackage, not a zip): ask the user for an unencrypted save")
    if found & {"xls", "xls2"}:
        return ("legacy_xls", "legacy .xls (BIFF): ask the user to re-save as .xlsx")
    return ("not_zip", "an OLE compound file that is neither an encrypted .xlsx nor a legacy .xls")


# --------------------------------------------------------------------------
# A1 / R1C1 and the formula tokenizer
# --------------------------------------------------------------------------

def col2num(s):
    n = 0
    for ch in s.upper():
        n = n * 26 + ord(ch) - 64
    return n


def num2col(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


_CELL_RE = re.compile(r"^(\$?)([A-Za-z]{1,3})(\$?)(\d{1,7})$")
_COLR_RE = re.compile(r"^(\$?)([A-Za-z]{1,3})$")
_ROWR_RE = re.compile(r"^(\$?)(\d{1,7})$")


def parse_cell(text):
    m = _CELL_RE.match(text)
    if not m:
        return None
    c, r = col2num(m.group(2)), int(m.group(4))
    if c > MAX_COL or r < 1 or r > MAX_ROW:
        return None
    return r, c, bool(m.group(3)), bool(m.group(1))


def parse_ref_a1(text):
    """'B7' -> (7, 2) for a plain cell address, else None."""
    p = parse_cell(text.replace("$", ""))
    return (p[0], p[1]) if p else None


class Area:
    __slots__ = ("r1", "c1", "r2", "c2", "r1a", "c1a", "r2a", "c2a", "shape")

    def __init__(self, r1, c1, r2, c2, r1a, c1a, r2a, c2a, shape):
        self.r1, self.c1, self.r2, self.c2 = r1, c1, r2, c2
        self.r1a, self.c1a, self.r2a, self.c2a = r1a, c1a, r2a, c2a
        self.shape = shape


def parse_area(text):
    if ":" in text:
        a, b = text.split(":", 1)
        ca, cb = parse_cell(a), parse_cell(b)
        if ca and cb:
            return Area(ca[0], ca[1], cb[0], cb[1], ca[2], ca[3], cb[2], cb[3], "range")
        ma, mb = _COLR_RE.match(a), _COLR_RE.match(b)
        if ma and mb:
            c1, c2 = col2num(ma.group(2)), col2num(mb.group(2))
            if c1 > MAX_COL or c2 > MAX_COL:
                return None
            return Area(1, c1, MAX_ROW, c2, True, bool(ma.group(1)), True, bool(mb.group(1)), "col")
        ma, mb = _ROWR_RE.match(a), _ROWR_RE.match(b)
        if ma and mb:
            r1, r2 = int(ma.group(2)), int(mb.group(2))
            if not (1 <= r1 <= MAX_ROW and 1 <= r2 <= MAX_ROW):
                return None
            return Area(r1, 1, r2, MAX_COL, bool(ma.group(1)), True, bool(mb.group(1)), True, "row")
        return None
    c = parse_cell(text)
    if not c:
        return None
    return Area(c[0], c[1], c[0], c[1], c[2], c[3], c[2], c[3], "cell")


def fmt_area(a, absolute_marks=True):
    def cell(r, c, ra, ca):
        return (("$" if ca and absolute_marks else "") + num2col(c) + ("$" if ra and absolute_marks else "") + str(r))
    if a.shape == "cell":
        return cell(a.r1, a.c1, a.r1a, a.c1a)
    if a.shape == "range":
        return cell(a.r1, a.c1, a.r1a, a.c1a) + ":" + cell(a.r2, a.c2, a.r2a, a.c2a)
    if a.shape == "col":
        return (("$" if a.c1a and absolute_marks else "") + num2col(a.c1) + ":" +
                ("$" if a.c2a and absolute_marks else "") + num2col(a.c2))
    return (("$" if a.r1a and absolute_marks else "") + str(a.r1) + ":" + ("$" if a.r2a and absolute_marks else "") + str(a.r2))


def rect_a1(r1, c1, r2, c2):
    if r1 == r2 and c1 == c2:
        return f"{num2col(c1)}{r1}"
    return f"{num2col(c1)}{r1}:{num2col(c2)}{r2}"


def rect_span(r1, c1, r2, c2):
    """Always A1:B2 form: read_xlsx rejects a one-cell range written as 'B2'."""
    return f"{num2col(c1)}{r1}:{num2col(c2)}{r2}"


def rect_text(r1, c1, r2, c2):
    if r1 == 1 and r2 == MAX_ROW:
        return f"{num2col(c1)}:{num2col(c2)}"
    if c1 == 1 and c2 == MAX_COL:
        return f"{r1}:{r2}"
    return rect_a1(r1, c1, r2, c2)


_SHEET = r"(?:'(?:[^']|'')+'|[\w.\\]+)"
_PREFIX = r"(?:(?:\[\d+\])?" + _SHEET + r"(?::" + _SHEET + r")?!)"
_COL = r"\$?[A-Za-z]{1,3}"
_ROW = r"\$?\d+"
_CELLP = _COL + _ROW
_AREA = r"(?:" + _CELLP + ":" + _CELLP + "|" + _COL + ":" + _COL + "|" + _ROW + ":" + _ROW + "|" + _CELLP + ")"
_STRUCT_BODY = r"(?:[^\[\]']|'.|\[(?:[^\[\]']|'.)*\])*"
_TOKEN_RE = re.compile("|".join([
    r"(?P<ws>\s+)",
    r'(?P<str>"(?:[^"]|"")*")',
    r"(?P<err>\#(?:NULL!|DIV/0!|VALUE!|REF!|NAME\?|NUM!|N/A|SPILL!|CALC!|UNKNOWN!|GETTING_DATA))",
    r"(?P<arr>\{[^{}]*\})",
    r"(?P<xfn>\[\d+\]![A-Za-z_][\w.]*(?=\())",
    r"(?P<func>[A-Za-z_\\][\w.]*(?=\())",
    r"(?P<ref>(?P<pre>" + _PREFIX + r")?(?P<area>" + _AREA + r")(?![\w.(\[]))",
    r"(?P<pname>(?P<pnpre>" + _PREFIX + r")[A-Za-z_\\][\w.]*)",
    r"(?P<struct>(?:(?:[^\W\d]|\\)[\w.]*)?\[" + _STRUCT_BODY + r"\])",
    r"(?P<num>(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)",
    r"(?P<name>[A-Za-z_\\][\w.]*)",
    r"(?P<op><>|<=|>=|[-+*/^&=<>%,;():!@#{}])",
    r"(?P<other>.)",
]), re.S)


class Tok:
    __slots__ = ("kind", "text", "start", "end", "sheet", "sheet2", "ext", "area", "pre", "call")

    def __init__(self, kind, text, start, end):
        self.kind, self.text, self.start, self.end = kind, text, start, end
        self.sheet = self.sheet2 = self.ext = self.area = self.pre = self.call = None


def split_prefix(pre):
    """'Jan:Mar!' -> ('Jan', 'Mar', None); "'[1]My Sheet'!" -> ('My Sheet', None, 1)."""
    body = pre[:-1]
    quoted = body.startswith("'")
    if quoted:
        body = body[1:-1].replace("''", "'")
    ext = None
    m = re.match(r"^\[([^\]]+)\](.*)$", body, re.S)
    if m:
        ext, body = (int(m.group(1)) if m.group(1).isdigit() else m.group(1)), m.group(2)
    if ":" in body:
        a, b = body.split(":", 1)
        return a, b, ext
    return body, None, ext


def tokenize(text):
    out = []
    for m in _TOKEN_RE.finditer(text):
        kind = m.lastgroup
        if kind in ("pre", "area"):
            kind = "ref"
        if m.group("ref") is not None:
            kind = "ref"
        elif m.group("pname") is not None:
            kind = "pname"
        t = Tok(kind, m.group(0), m.start(), m.end())
        if kind == "ref":
            area = parse_area(m.group("area"))
            if area is None:
                t.kind = "name"
            else:
                t.area = area
                t.pre = m.group("pre")
                if t.pre:
                    t.sheet, t.sheet2, t.ext = split_prefix(t.pre)
        elif kind == "pname":
            t.pre = m.group("pnpre")
            t.sheet, t.sheet2, t.ext = split_prefix(t.pre)
        out.append(t)
    return out


def r1c1_ref(tok, hr, hc):
    a = tok.area

    def part(n, absolute, host, axis):
        if absolute:
            return f"{axis}{n}"
        d = n - host
        return axis if d == 0 else f"{axis}[{d}]"

    if a.shape == "col":
        body = part(a.c1, a.c1a, hc, "C") + ":" + part(a.c2, a.c2a, hc, "C")
    elif a.shape == "row":
        body = part(a.r1, a.r1a, hr, "R") + ":" + part(a.r2, a.r2a, hr, "R")
    else:
        body = part(a.r1, a.r1a, hr, "R") + part(a.c1, a.c1a, hc, "C")
        if a.shape == "range":
            body += ":" + part(a.r2, a.r2a, hr, "R") + part(a.c2, a.c2a, hc, "C")
    return (tok.pre or "") + body


def norm_func(name):
    n = name.upper()
    while n.startswith("_XLFN.") or n.startswith("_XLWS."):
        n = n[6:]
    return n


def normalize(sig, hr, hc):
    parts = []
    for t in sig:
        if t.kind == "ref":
            parts.append(r1c1_ref(t, hr, hc))
        elif t.kind in ("func", "xfn"):
            parts.append(norm_func(t.text))
        elif t.kind == "name":
            parts.append(t.text[6:] if t.text.lower().startswith("_xlpm.") else t.text)
        else:
            parts.append(t.text)
    return "".join(parts)


def shift_ref(tok, dr, dc):
    a = tok.area
    r1 = a.r1 if a.r1a or a.shape == "col" else a.r1 + dr
    r2 = a.r2 if a.r2a or a.shape == "col" else a.r2 + dr
    c1 = a.c1 if a.c1a or a.shape == "row" else a.c1 + dc
    c2 = a.c2 if a.c2a or a.shape == "row" else a.c2 + dc
    if min(r1, r2, c1, c2) < 1 or r1 > MAX_ROW or r2 > MAX_ROW or c1 > MAX_COL or c2 > MAX_COL:
        return "#REF!"
    return (tok.pre or "") + fmt_area(Area(r1, c1, r2, c2, a.r1a, a.c1a, a.r2a, a.c2a, a.shape))


def expand_shared(text, dr, dc):
    """A shared-formula child: the master's text with relative parts shifted."""
    out, last = [], 0
    for t in tokenize(text):
        if t.kind == "ref":
            out.append(text[last:t.start])
            out.append(shift_ref(t, dr, dc))
            last = t.end
    out.append(text[last:])
    return "".join(out)


# --------------------------------------------------------------------------
# Functions and routes
# --------------------------------------------------------------------------

def _words(s):
    return set(s.split())


KNOWN_FUNCS = _words("""
ABS ACOS ACOSH ACOT ACOTH AGGREGATE ARABIC ASIN ASINH ATAN ATAN2 ATANH BASE CEILING CEILING.MATH CEILING.PRECISE
COMBIN COMBINA COS COSH COT COTH CSC CSCH DECIMAL DEGREES EVEN EXP FACT FACTDOUBLE FLOOR FLOOR.MATH FLOOR.PRECISE
GCD INT ISO.CEILING LCM LN LOG LOG10 MOD MROUND MULTINOMIAL MUNIT ODD PI POWER PRODUCT QUOTIENT RADIANS ROMAN
ROUND ROUNDDOWN ROUNDUP SEC SECH SEQUENCE SERIESSUM SIGN SIN SINH SQRT SQRTPI SUBTOTAL SUM SUMIF SUMIFS SUMPRODUCT
SUMSQ SUMX2MY2 SUMX2PY2 SUMXMY2 TAN TANH TRUNC
AVEDEV AVERAGE AVERAGEA AVERAGEIF AVERAGEIFS COUNT COUNTA COUNTBLANK COUNTIF COUNTIFS DEVSQ FORECAST FORECAST.LINEAR
FREQUENCY GEOMEAN HARMEAN INTERCEPT KURT LARGE MAX MAXA MAXIFS MEDIAN MIN MINA MINIFS MODE MODE.MULT MODE.SNGL
PERCENTILE PERCENTILE.EXC PERCENTILE.INC PERCENTRANK PERCENTRANK.EXC PERCENTRANK.INC PERMUT PERMUTATIONA PEARSON
QUARTILE QUARTILE.EXC QUARTILE.INC RANK RANK.AVG RANK.EQ RSQ SKEW SKEW.P SLOPE SMALL STDEV STDEV.P STDEV.S STDEVA
STDEVP STDEVPA STEYX TRIMMEAN VAR VAR.P VAR.S VARA VARP VARPA CORREL COVAR COVARIANCE.P COVARIANCE.S
AND FALSE IF IFERROR IFNA IFS LET NOT OR SWITCH TRUE XOR
ASC CHAR CLEAN CODE CONCAT CONCATENATE DOLLAR ENCODEURL EXACT FIND FIXED LEFT LEN LOWER MID NUMBERVALUE PROPER
REPLACE REPT RIGHT SEARCH SUBSTITUTE T TEXT TEXTAFTER TEXTBEFORE TEXTJOIN TEXTSPLIT TRIM UNICHAR UNICODE UPPER
VALUE VALUETOTEXT ARRAYTOTEXT REGEXTEST REGEXEXTRACT REGEXREPLACE
DATE DATEDIF DATEVALUE DAY DAYS DAYS360 EDATE EOMONTH HOUR ISOWEEKNUM MINUTE MONTH NETWORKDAYS NETWORKDAYS.INTL
SECOND TIME TIMEVALUE WEEKDAY WEEKNUM WORKDAY WORKDAY.INTL YEAR YEARFRAC
ADDRESS AREAS CHOOSE CHOOSECOLS CHOOSEROWS COLUMN COLUMNS DROP EXPAND FILTER FORMULATEXT GETPIVOTDATA HLOOKUP HSTACK
HYPERLINK INDEX LOOKUP MATCH ROW ROWS SORT SORTBY TAKE TOCOL TOROW TRANSPOSE UNIQUE VLOOKUP VSTACK WRAPCOLS WRAPROWS
XLOOKUP XMATCH
CELL ERROR.TYPE INFO ISBLANK ISERR ISERROR ISEVEN ISFORMULA ISLOGICAL ISNA ISNONTEXT ISNUMBER ISODD ISREF ISTEXT N NA
SHEET SHEETS TYPE ISOMITTED TRIMRANGE PHONETIC LEFTB RIGHTB MIDB LENB FINDB SEARCHB REPLACEB DETECTLANGUAGE
BITAND BITOR BITXOR BITLSHIFT BITRSHIFT BIN2DEC DEC2BIN DEC2HEX HEX2DEC CONVERT DELTA GESTEP
""")

VOLATILE = _words("TODAY NOW RAND RANDBETWEEN RANDARRAY OFFSET INDIRECT INFO")
RAND_FUNCS = _words("RAND RANDBETWEEN RANDARRAY")
LAMBDA_FAMILY = _words("LAMBDA MAP REDUCE SCAN BYROW BYCOL MAKEARRAY")
XLM_FUNCS = _words("EXEC CALL REGISTER REGISTER.ID")
WEB_FUNCS = _words("WEBSERVICE FILTERXML")
CUBE_FUNCS = _words("CUBEVALUE CUBEMEMBER CUBESET CUBESETCOUNT CUBEMEMBERPROPERTY CUBERANKEDMEMBER CUBEKPIMEMBER")
BLOCKED_FUNCS = _words("FIELDVALUE IMAGE")
DYNAMIC_ARRAY_FUNCS = _words("UNIQUE SORT SORTBY FILTER SEQUENCE")
ETS_FUNCS = _words("FORECAST.ETS FORECAST.ETS.CONFINT FORECAST.ETS.SEASONALITY FORECAST.ETS.STAT")

AT_COST = _words("""
ACCRINT ACCRINTM CUMIPMT CUMPRINC DB DDB DISC DOLLARDE DOLLARFR DURATION EFFECT FV FVSCHEDULE INTRATE IPMT IRR ISPMT
MDURATION MIRR NOMINAL NPER NPV PDURATION PMT PPMT PRICE PV RATE RECEIVED RRI SLN SYD TBILLEQ TBILLPRICE TBILLYIELD
VDB XIRR XNPV YIELD
BETA.DIST BETA.INV BINOM.DIST BINOM.INV CHISQ.DIST CHISQ.INV CHISQ.TEST EXPON.DIST F.DIST F.INV GAMMA.DIST GAMMA.INV
GAMMALN LOGNORM.DIST LOGNORM.INV NEGBINOM.DIST NORM.DIST NORM.INV NORM.S.DIST NORM.S.INV POISSON.DIST T.DIST T.INV
WEIBULL.DIST Z.TEST NORMDIST NORMINV NORMSDIST NORMSINV TDIST TINV CHIDIST CHIINV BINOMDIST POISSON
DSUM DAVERAGE DCOUNT DCOUNTA DGET DMAX DMIN DPRODUCT DSTDEV DSTDEVP DVAR DVARP
MMULT MINVERSE MDETERM LINEST LOGEST TREND GROWTH GROUPBY PIVOTBY NETWORKDAYS NETWORKDAYS.INTL WORKDAY WORKDAY.INTL
CONFIDENCE CONFIDENCE.NORM CONFIDENCE.T
ERF ERF.PRECISE ERFC ERFC.PRECISE GAMMA GAMMALN.PRECISE GAUSS PHI STANDARDIZE T.TEST TTEST F.TEST FTEST CHISQ.DIST.RT
CHISQ.INV.RT CHITEST FDIST FINV F.DIST.RT F.INV.RT GAMMADIST GAMMAINV LOGINV LOGNORMDIST CRITBINOM HYPGEOM.DIST HYPGEOMDIST
NEGBINOMDIST WEIBULL EXPONDIST BETADIST BETAINV T.DIST.2T T.DIST.RT T.INV.2T PROB PERCENTRANK ZTEST
IMABS IMAGINARY IMARGUMENT IMCONJUGATE IMCOS IMDIV IMEXP IMLN IMPRODUCT IMREAL IMSIN IMSQRT IMSUB IMSUM COMPLEX
BESSELI BESSELJ BESSELK BESSELY OCT2BIN OCT2DEC OCT2HEX BIN2HEX BIN2OCT DEC2OCT HEX2BIN HEX2OCT
EUROCONVERT AMORDEGRC AMORLINC COUPDAYBS COUPDAYS COUPDAYSNC COUPNCD COUPNUM COUPPCD ODDFPRICE ODDFYIELD ODDLPRICE ODDLYIELD
PRICEDISC PRICEMAT YIELDDISC YIELDMAT
""")

VENDOR = {
    "BDP": "Bloomberg", "BDH": "Bloomberg", "BDS": "Bloomberg",
    "FDS": "FactSet",
    "CIQ": "S&P Capital IQ", "CIQRANGE": "S&P Capital IQ", "CIQAVG": "S&P Capital IQ",
    "RDP.DATA": "Refinitiv", "RHISTORY": "Refinitiv", "TR": "Refinitiv",
    "RTD": "an RTD server", "STOCKHISTORY": "STOCKHISTORY (a Microsoft data service)",
    "HSGETVALUE": "Oracle Smart View (Hyperion/Essbase)", "HSSETVALUE": "Oracle Smart View (Hyperion/Essbase)",
    "DBRW": "IBM TM1 / Planning Analytics (DBRW writes back)", "DBR": "IBM TM1 / Planning Analytics",
    "DBRA": "IBM TM1 / Planning Analytics", "SUBNM": "IBM TM1 / Planning Analytics",
    "SAPGETDATA": "SAP", "SAPBEXGETDATA": "SAP BW", "SAPGETPROPERTY": "SAP",
    "EPMRETRIEVEDATA": "SAP BPC", "EPMSAVEDATA": "SAP BPC",
    "XFGETCELL": "OneStream", "XFSETCELL": "OneStream",
    "ESSCELL": "Essbase",
    "NL": "Jet Reports", "NF": "Jet Reports",
}

CRITERIA_AT = {"SUMIF": (1,), "AVERAGEIF": (1,), "COUNTIF": (1,)}
IFS_FUNCS = _words("SUMIFS AVERAGEIFS MAXIFS MINIFS COUNTIFS")
CI_FUNCS = _words("SUMIF SUMIFS COUNTIF COUNTIFS AVERAGEIF AVERAGEIFS MAXIFS MINIFS VLOOKUP HLOOKUP LOOKUP MATCH XLOOKUP XMATCH")
LOOKUP_FUNCS = _words("VLOOKUP HLOOKUP LOOKUP XLOOKUP XMATCH MATCH INDEX CHOOSE GETPIVOTDATA")
KEYED_LOOKUPS = _words("VLOOKUP HLOOKUP MATCH XLOOKUP XMATCH")
REF_CONSUMERS = _words("SUM SUMIF SUMIFS COUNT COUNTA COUNTIF COUNTIFS AVERAGE AVERAGEIF AVERAGEIFS MIN MAX SUMPRODUCT AREAS ROWS COLUMNS")
PIVOT_RECIPE_REASON = ("GETPIVOTDATA reads a pivot's rendered cell, not the source rows: a recipe exists (cookbook-pivot.md P8), "
                       "rebuild the pivot as a query")
EMPTY_ARG_FUNCS = _words("SUM SUMPRODUCT AVERAGE COUNT COUNTA MAX MIN PRODUCT STDEV VAR MEDIAN")
TOTAL_FUNCS = _words("SUM SUBTOTAL AGGREGATE AVERAGE COUNT COUNTA MAX MIN")
AGGREGATE_VIAS = REF_CONSUMERS | TOTAL_FUNCS
COMPARE_OPS = _words("= <> < > <= >=")
ARITH_OPS = _words("+ - * / ^ & = <> < > <= >=")


class Call:
    __slots__ = ("name", "tok", "args", "parent")

    def __init__(self, name, tok, parent):
        self.name, self.tok, self.parent = name, tok, parent
        self.args = [[]]


def build_tree(sig):
    root = Call(None, None, None)
    stack = [root]
    calls = []
    skip_paren = False
    for t in sig:
        cur = stack[-1]
        if t.kind in ("func", "xfn"):
            c = Call(norm_func(t.text) if t.kind == "func" else t.text, t, cur)
            cur.args[-1].append(c)
            calls.append(c)
            stack.append(c)
            skip_paren = True
            continue
        if t.kind == "op":
            if t.text == "(":
                if skip_paren:
                    skip_paren = False
                    continue
                c = Call(None, None, cur)
                cur.args[-1].append(c)
                stack.append(c)
                continue
            if t.text == ")":
                if len(stack) > 1:
                    stack.pop()
                continue
            if t.text == "," and len(stack) > 1:
                cur.args.append([])
                continue
        skip_paren = False
        t.call = cur
        cur.args[-1].append(t)
    return root, calls


def flat_tokens(items):
    for it in items:
        if isinstance(it, Call):
            for a in it.args:
                for x in flat_tokens(a):
                    yield x
        else:
            yield it


SELECTOR_ARGS = {"IF": lambda i: i == 0, "IFS": lambda i: i % 2 == 0, "CHOOSE": lambda i: i == 0, "SWITCH": lambda i: i == 0,
                 "INDEX": lambda i: i in (1, 2)}
_FIRST_ARG = lambda i: i == 0  # noqa: E731
_PAIRS_AFTER_SUM_RANGE = lambda i: i >= 2 and i % 2 == 0  # noqa: E731
# Arguments that carry an input rather than data: an IF condition, a lookup value, a criteria.
KEY_ARGS = {"IF": _FIRST_ARG, "VLOOKUP": _FIRST_ARG, "HLOOKUP": _FIRST_ARG, "XLOOKUP": _FIRST_ARG, "MATCH": _FIRST_ARG, "XMATCH": _FIRST_ARG,
            "LOOKUP": _FIRST_ARG, "SUMIF": lambda i: i == 1, "COUNTIF": lambda i: i == 1, "AVERAGEIF": lambda i: i == 1,
            "SUMIFS": _PAIRS_AFTER_SUM_RANGE, "AVERAGEIFS": _PAIRS_AFTER_SUM_RANGE, "MAXIFS": _PAIRS_AFTER_SUM_RANGE,
            "MINIFS": _PAIRS_AFTER_SUM_RANGE, "COUNTIFS": lambda i: i % 2 == 1}


def in_selector_position(tok):
    """"key" when the token sits directly in an IF condition, lookup value or criteria argument; True for the other selector
    arguments (IFS, CHOOSE, SWITCH, INDEX); else False."""
    c = tok.call
    if c is None:
        return False
    for table, tag in ((KEY_ARGS, "key"), (SELECTOR_ARGS, True)):
        pick = table.get(c.name)
        if pick and any(pick(i) and any(x is tok for x in items) for i, items in enumerate(c.args)):
            return tag
    return False


def enclosing_func(call):
    while call is not None and call.name is None:
        call = call.parent
    return call.name if call is not None else None


def unquote(s):
    return s[1:-1].replace('""', '"')


def literal(items):
    """The literal an argument spells: '1', '-1', 'TRUE', or None."""
    if len(items) == 1 and isinstance(items[0], Tok):
        t = items[0]
        if t.kind == "num":
            return t.text
        if t.kind == "name" and t.text.upper() in ("TRUE", "FALSE"):
            return t.text.upper()
    if len(items) == 2 and isinstance(items[0], Tok) and items[0].text in "-+" and isinstance(items[1], Tok) and items[1].kind == "num":
        return items[0].text + items[1].text
    return None


def is_truthy_literal(lit):
    return lit == "TRUE" or (lit is not None and lit not in ("FALSE",) and _num(lit) not in (None, 0))


def _lit_num(lit):
    """A literal as a number, with TRUE as 1 and FALSE as 0 the way Excel coerces a match-type flag."""
    return 1.0 if lit == "TRUE" else 0.0 if lit == "FALSE" else _num(lit)


def _small_int(lit):
    v = _num(lit)
    return int(v) if v is not None and math.isfinite(v) and abs(v) < 1e9 else None


def _num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


class RefSpec:
    __slots__ = ("sheet", "r1", "c1", "r2", "c2", "r1a", "c1a", "r2a", "c2a", "shape", "via", "named", "sel")

    def resolve(self, hr, hc):
        r1 = self.r1 if self.r1a else hr + self.r1
        r2 = self.r2 if self.r2a else hr + self.r2
        c1 = self.c1 if self.c1a else hc + self.c1
        c2 = self.c2 if self.c2a else hc + self.c2
        return (min(r1, r2), min(c1, c2), max(r1, r2), max(c1, c2))


def make_spec(sheet, area, hr, hc, via, named=False):
    s = RefSpec()
    s.sheet, s.shape, s.via, s.named, s.sel = sheet, area.shape, via, named, False
    s.r1a, s.c1a, s.r2a, s.c2a = area.r1a, area.c1a, area.r2a, area.c2a
    s.r1 = area.r1 if area.r1a else area.r1 - hr
    s.r2 = area.r2 if area.r2a else area.r2 - hr
    s.c1 = area.c1 if area.c1a else area.c1 - hc
    s.c2 = area.c2 if area.c2a else area.c2 - hc
    return s


def abs_spec(sheet, r1, c1, r2, c2, shape, via, rel_row=False):
    s = RefSpec()
    s.sheet, s.shape, s.via, s.named, s.sel = sheet, shape, via, False, False
    s.r1, s.r2, s.c1, s.c2 = r1, r2, c1, c2
    s.r1a = s.r2a = not rel_row
    s.c1a = s.c2a = True
    return s


def literal_index_position(call, tok):
    """(row, col) when `call` is INDEX(<one ref>, <literal row>[, <literal col>]), else None."""
    if not call.args[0] or call.args[0][0] is not tok or len(call.args[0]) != 1 or call.tok is None:
        return None
    pos = [_small_int(literal(call.args[k])) if len(call.args) > k and call.args[k] else None for k in (1, 2)]
    if len(call.args) > 3 or pos[0] is None or pos[0] < 1 or (len(call.args) > 2 and call.args[2] and (pos[1] is None or pos[1] < 1)):
        return None
    return pos[0], pos[1]


def narrow_index_anchor(spec, tok, sig):
    """INDEX(range, r, c) used as one end of a `:` range reads only the one cell it names, so a literal position shrinks the spec to it."""
    call = tok.call
    pos = literal_index_position(call, tok)
    if pos is None:
        return
    i = next((k for k, x in enumerate(sig) if x is call.tok), None)
    if i is None:
        return
    depth, j = 0, i + 1
    while j < len(sig):
        if sig[j].kind == "op" and sig[j].text == "(":
            depth += 1
        elif sig[j].kind == "op" and sig[j].text == ")":
            depth -= 1
            if depth == 0:
                break
        j += 1
    after = sig[j + 1] if j + 1 < len(sig) else None
    before = sig[i - 1] if i > 0 else None
    if not ((after is not None and after.text == ":") or (before is not None and before.text == ":")):
        return
    row, col = pos[0], pos[1] if pos[1] is not None else 1
    if spec.shape == "row" or (spec.r1 == spec.r2 and spec.r1a == spec.r2a and not (spec.c1 == spec.c2 and spec.c1a == spec.c2a)):
        if len(call.args) < 3 or not call.args[2]:
            row, col = 1, pos[0]
    spec.r2, spec.r2a = spec.r1 + row - 1, spec.r1a
    spec.r1 = spec.r2
    spec.c2, spec.c2a = spec.c1 + col - 1, spec.c1a
    spec.c1 = spec.c2
    spec.r1a, spec.c1a = spec.r2a, spec.c2a
    spec.shape = "cell"


class TableInfo:
    def __init__(self, name, sheet, r1, c1, r2, c2, header_rows, totals_rows, columns):
        self.name, self.sheet = name, sheet
        self.r1, self.c1, self.r2, self.c2 = r1, c1, r2, c2
        self.header_rows, self.totals_rows, self.columns = header_rows, totals_rows, columns

    def contains(self, sheet, r, c):
        return sheet == self.sheet and self.r1 <= r <= self.r2 and self.c1 <= c <= self.c2


def split_top(s, sep):
    out, depth, cur, i = [], 0, [], 0
    while i < len(s):
        ch = s[i]
        if ch == "'" and i + 1 < len(s):
            cur.append(s[i:i + 2])
            i += 2
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        if ch == sep and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        i += 1
    out.append("".join(cur))
    return out


def name_key(s):
    """Excel compares table and column names case-insensitively across scripts; NFC so composed and decomposed letters agree."""
    return unicodedata.normalize("NFC", s or "").casefold()


def _unbracket(p):
    p = p.strip()
    if p.startswith("[") and p.endswith("]"):
        p = p[1:-1]
    return re.sub(r"'(.)", r"\1", p)


def resolve_struct(text, ctx, sheet, hr, hc):
    """Structured reference -> (table, (r1,c1,r2,c2), row_relative) or None."""
    m = re.match(r"^((?:[^\W\d]|\\)[\w.]*)?(\[.*\])$", text.strip(), re.S)
    if not m:
        return None
    tname, body = m.group(1), m.group(2)
    table = ctx.tables.get(name_key(tname)) if tname else None
    if table is None and not tname:
        for t in ctx.tables.values():
            if t.contains(sheet, hr, hc):
                table = t
                break
    if table is None:
        return None
    inner = body[1:-1].strip()
    this_row = False
    if inner.startswith("@"):
        this_row, inner = True, inner[1:]
    specials, cols = [], []
    for part in split_top(inner, ","):
        part = part.strip()
        if not part:
            continue
        rng = split_top(part, ":")
        if len(rng) == 2 and "[" in part:
            cols.extend([_unbracket(rng[0]), _unbracket(rng[1])])
            cols.append(None)
            continue
        name = _unbracket(part)
        if name.startswith("#"):
            specials.append(name.upper())
        elif name:
            cols.append(name)
    names = [name_key(c) for c in table.columns]
    if cols:
        cols = [c for c in cols if c is not None] if None not in cols else cols
        try:
            if None in cols:
                a, b = names.index(name_key(cols[0])), names.index(name_key(cols[1]))
                ci1, ci2 = min(a, b), max(a, b)
            else:
                idx = [names.index(name_key(c)) for c in cols]
                ci1, ci2 = min(idx), max(idx)
        except ValueError:
            return None
    else:
        ci1, ci2 = 0, len(names) - 1
    c1, c2 = table.c1 + ci1, table.c1 + ci2
    top = table.r1 + table.header_rows
    bottom = table.r2 - table.totals_rows
    if this_row or "#THIS ROW" in specials:
        return (table, (hr, c1, hr, c2), True)
    head, data, tot = "#HEADERS" in specials or "#ALL" in specials, "#DATA" in specials or "#ALL" in specials, "#TOTALS" in specials or "#ALL" in specials
    if not (head or data or tot):
        data = True
    hdr_end = table.r1 + max(table.header_rows, 1) - 1
    if tot and table.totals_rows == 0:
        return None
    if head and tot and not data:
        return None
    first = table.r1 if head else top if data else bottom + 1
    last = table.r2 if tot else bottom if data else hdr_end
    return (table, (first, c1, last, c2), False)


class Analysis:
    def __init__(self):
        self.key = ""
        self.funcs = Counter()
        self.refs = []
        self.merged = set()
        self.bound = set()
        self.refkeys = None
        self.flags = {}
        self.route = "T"
        self.reasons = []
        self.nr_reasons = set()
        self.split = "constant"
        self.absolute_refs = []
        self.opaque = False
        self.mechs = set()
        self.py_code = []
        self.volatile = set()
        self.sec = set()
        self.subtotal = None
        self.call_routes = {}
        self.pos = (0, 0)
        self.pos_sens = False
        self.ci_probed = False
        self.ci_sides = {}

    def flag(self, fid, detail=""):
        self.flags.setdefault(fid, [])
        if detail and detail not in self.flags[fid]:
            self.flags[fid].append(detail)

    def bump(self, route, reason=None):
        if ROUTE_ORDER[route] > ROUTE_ORDER[self.route]:
            self.route = route
        if reason and reason not in self.reasons:
            self.reasons.append(reason)
        if reason and route == "NR":
            self.nr_reasons.add(reason)


class Ctx:
    def __init__(self):
        self.sheet_names = []
        self.sheet_lookup = {}
        self.names = {}
        self.tables = {}
        self.lambda_names = set()
        self.name_cache = {}
        self.rel_cache = {}
        self.rel_pos = None
        self.has_vba = False
        self.sheet_data = {}
        self.pos_sens = False
        self.const_area_cache = {}


def canon_sheet(ctx, name):
    return ctx.sheet_lookup.get(name.lower())


def name_def(ctx, sheet, tok):
    """The defined name a name or sheet-qualified name token reads: a qualifier picks that sheet's scope, not the formula's."""
    up = (tok.text[tok.text.rfind("!") + 1:] if tok.kind == "pname" else tok.text).upper()
    scope = (canon_sheet(ctx, tok.sheet) or tok.sheet) if tok.kind == "pname" and tok.sheet is not None else sheet
    return ctx.names.get((scope.lower(), up)) or ctx.names.get((None, up))


def _side_key(side):
    return None if side is None else side if side[0] == "lit" else ("spec", _ref_key(side[1]))


def ci_side(a, ctx, sheet, items, first_col_only=False):
    """One side of a case-insensitive comparison: a plain text literal or a single cell/range ref (relative to the formula's cell), else None."""
    if not items or len(items) != 1 or not isinstance(items[0], Tok):
        return None
    t = items[0]
    if t.kind == "str":
        text = unquote(t.text)
        return None if re.search(r"[*?~]|^\s*[<>=]", text) else ("lit", text)
    if t.kind != "ref" or t.ext is not None or t.sheet2 is not None or t.area.shape not in ("cell", "range"):
        return None
    sh = canon_sheet(ctx, t.sheet) if t.sheet is not None else sheet
    if sh is None:
        return None
    sp = make_spec(sh, t.area, a.pos[0], a.pos[1], None)
    if first_col_only:
        sp.c2, sp.c2a = sp.c1, sp.c1a
    return ("spec", sp)


def _sole_operand(sig, j, step):
    """The token at j is the whole operand of its comparison: what lies beyond it is a boundary, not an operator that would extend it."""
    beyond = sig[j + step] if 0 <= j + step < len(sig) else None
    return beyond is None or (beyond.kind == "op" and (beyond.text in ("(", ",", ")") or beyond.text in COMPARE_OPS))


def ci_flag(a, detail, *sides):
    a.flag("ci_match", detail)
    for sd in sides:
        a.ci_sides.setdefault(_side_key(sd), sd)


def _ref_key(r):
    return (r.sheet, r.r1, r.c1, r.r2, r.c2, r.r1a, r.c1a, r.r2a, r.c2a, r.shape, r.via, r.named, r.sel)


def merge_analysis(a, sub):
    for fn, n in sub.funcs.items():
        a.funcs[fn] += n
    if id(sub) in a.merged:
        return
    a.merged.add(id(sub))
    if a.refkeys is None:
        a.refkeys = {_ref_key(r) for r in a.refs}
    for r in sub.refs:
        k = _ref_key(r)
        if k not in a.refkeys:
            a.refkeys.add(k)
            a.refs.append(r)
    for k, v in sub.flags.items():
        for d in v or [""]:
            a.flag(k, d)
    for r in sub.reasons:
        a.bump(sub.route, r)
    a.bump(sub.route)
    for k, v in sub.ci_sides.items():
        a.ci_sides.setdefault(k, v)
    a.mechs |= sub.mechs
    a.volatile |= sub.volatile
    a.py_code.extend(sub.py_code)
    a.sec |= sub.sec
    a.opaque = a.opaque or sub.opaque
    a.ci_probed = a.ci_probed or sub.ci_probed


_DDE_RE = re.compile(r"^\s*[A-Za-z0-9_.]+\|")


def analyze_formula(text, sheet, hr, hc, ctx, depth=0):
    a = Analysis()
    a.pos = (hr, hc)
    if depth == 0:
        ctx.pos_sens = False
    if _DDE_RE.match(text):
        a.mechs.add("dde")
        a.bump("X", "a DDE formula: never resolved; skipped entirely")
    sig = [t for t in tokenize(text) if t.kind != "ws"]
    root, calls = build_tree(sig)
    a.key = normalize(sig, hr, hc)
    for c_ in calls:
        if c_.name == "LET":
            a.bound.update(x[0].text.upper() for x in c_.args[0::2][:-1] if len(x) == 1 and isinstance(x[0], Tok))
        elif c_.name == "LAMBDA":
            a.bound.update(x[0].text.upper() for x in c_.args[:-1] if len(x) == 1 and isinstance(x[0], Tok))
    inter, inter_skip = {}, set()
    for i in range(len(sig) - 1):
        t0, t1 = sig[i], sig[i + 1]
        if t0.kind in ("ref", "name", "pname") and t1.kind in ("ref", "name", "pname") and t0.end < t1.start \
                and not text[t0.end:t1.start].strip() and i not in inter_skip:
            inter[i] = i + 1
            inter_skip.add(i + 1)

    def add_ref(tok, via):
        if tok.ext is not None:
            a.flag("external_ref")
            a.opaque = True
            a.bump("NR", "reads another workbook (an external workbook reference): its values are a snapshot as of the last link refresh")
            return
        names = [sheet]
        if tok.sheet is not None:
            s1 = canon_sheet(ctx, tok.sheet)
            if s1 is None:
                a.flag("ref_error", "unknown sheet")
                a.opaque = True
                return
            names = [s1]
            if tok.sheet2 is not None:
                s2 = canon_sheet(ctx, tok.sheet2)
                if s2 is None:
                    a.opaque = True
                    return
                i1, i2 = ctx.sheet_names.index(s1), ctx.sheet_names.index(s2)
                names = ctx.sheet_names[min(i1, i2):max(i1, i2) + 1]
                a.flag("3d_ref", f"{tok.sheet}:{tok.sheet2}")
        for n in names:
            a.refs.append(make_spec(n, tok.area, hr, hc, via))
            a.refs[-1].sel = in_selector_position(tok)
        if tok.area.shape == "col" and tok.area.r1 == 1 and tok.area.r2 == MAX_ROW:
            a.flag("full_column", fmt_area(tok.area, False))

    def tok_specs(tok, via):
        if tok.kind == "ref":
            if tok.ext is not None or tok.sheet2 is not None:
                return None
            nm_ = canon_sheet(ctx, tok.sheet) if tok.sheet is not None else sheet
            return [make_spec(nm_, tok.area, hr, hc, via)] if nm_ else None
        d_ = name_def(ctx, sheet, tok)
        if d_ is None or d_["kind"] != "ref" or not d_["specs"]:
            return None
        out_ = []
        for spec in d_["specs"]:
            sp = RefSpec()
            sp.sheet, sp.shape, sp.via, sp.named, sp.sel = spec.sheet, spec.shape, via, True, False
            sp.r1, sp.c1, sp.r2, sp.c2 = spec.r1, spec.c1, spec.r2, spec.c2
            sp.r1a = sp.c1a = sp.r2a = sp.c2a = True
            out_.append(sp)
        return out_

    for i, t in enumerate(sig):
        via = enclosing_func(t.call) if t.call is not None else None
        nxt = sig[i + 1] if i + 1 < len(sig) else None
        prv = sig[i - 1] if i > 0 else None
        if i in inter_skip:
            continue
        if i in inter:
            a.flag("intersection", "space operator")
            s0, s1 = tok_specs(t, via), tok_specs(sig[inter[i]], via)
            if s0 and s1 and len(s0) == 1 and len(s1) == 1 and s0[0].sheet == s1[0].sheet:
                x0, x1 = s0[0].resolve(hr, hc), s1[0].resolve(hr, hc)
                r1_, c1_, r2_, c2_ = max(x0[0], x1[0]), max(x0[1], x1[1]), min(x0[2], x1[2]), min(x0[3], x1[3])
                if r1_ <= r2_ and c1_ <= c2_:
                    a.refs.append(abs_spec(s0[0].sheet, r1_, c1_, r2_, c2_, "cell" if (r1_, c1_) == (r2_, c2_) else "range", via))
                if not all(s.r1a and s.c1a and s.r2a and s.c2a for s in (s0[0], s1[0])):
                    a.opaque = True
            else:
                a.opaque = True
            continue
        if t.kind == "ref":
            n_before = len(a.refs)
            add_ref(t, via)
            if t.call is not None and t.call.name == "INDEX" and len(a.refs) == n_before + 1:
                narrow_index_anchor(a.refs[-1], t, sig)
            if nxt is not None and nxt.kind == "op" and nxt.text == "#":
                a.flag("spill_ref")
                a.opaque = True
            if nxt is not None and nxt.text == ":" and i + 2 < len(sig) and sig[i + 2].kind in ("func", "xfn", "name"):
                a.opaque = True
                a.flag("opaque_dependency", "dynamic range end")
                a.bump("NR", "a range end is built by a function or name: ask which range it selects")
        elif t.kind == "struct":
            a.flag("structured_ref")
            res = resolve_struct(t.text, ctx, sheet, hr, hc)
            if res is None:
                a.opaque = True
                a.flag("unresolved_structured_ref", "no such table or column")
            else:
                table, (r1, c1, r2, c2), rel = res
                shape = "cell" if (r1 == r2 and c1 == c2) else "range"
                if rel:
                    s = abs_spec(table.sheet, r1 - hr, c1, r2 - hr, c2, shape, via, rel_row=True)
                else:
                    s = abs_spec(table.sheet, r1, c1, r2, c2, shape, via)
                a.refs.append(s)
        elif t.kind in ("name", "pname"):
            nm = t.text[t.text.rfind("!") + 1:] if t.kind == "pname" else t.text
            up = nm.upper()
            if up in ("TRUE", "FALSE") or nm.lower().startswith("_xlpm."):
                continue
            d = name_def(ctx, sheet, t)
            if d is None:
                continue
            if d["kind"] == "ref":
                for spec in d["specs"]:
                    sp = RefSpec()
                    sp.sheet, sp.shape, sp.via, sp.named, sp.sel = spec.sheet, spec.shape, via, True, False
                    sp.r1, sp.c1, sp.r2, sp.c2 = spec.r1, spec.c1, spec.r2, spec.c2
                    sp.r1a = sp.c1a = sp.r2a = sp.c2a = True
                    a.refs.append(sp)
                a.flag("named_range", nm)
            elif d["kind"] == "external":
                a.flag("external_ref", nm)
                a.opaque = True
                a.bump("NR", f"`{nm}` is a defined name into another workbook: the cells it reads are a snapshot as of the last link refresh")
            elif d["kind"] == "constant":
                a.flag("named_constant", nm)
            elif d["kind"] == "formula":
                a.opaque = True
                a.flag("named_formula", nm)
                if depth >= 3:
                    a.flag("named_formula_depth", nm)
                    a.bump("NR", "a defined-name chain is deeper than the script follows: ask what it computes")
                elif d.get("text"):
                    ck = (sheet, id(d), depth)
                    if ctx.rel_pos != (sheet, hr, hc):
                        ctx.rel_cache.clear()
                        ctx.rel_pos = (sheet, hr, hc)
                    sub = ctx.name_cache.get(ck) or ctx.rel_cache.get(ck)
                    if sub is None:
                        sub = analyze_formula(d["text"], sheet, hr, hc, ctx, depth + 1)
                        if all(s.r1a and s.c1a and s.r2a and s.c2a for s in sub.refs):
                            ctx.name_cache[ck] = sub
                        else:
                            ctx.rel_cache[ck] = sub
                    merge_analysis(a, sub)
        elif t.kind == "err" and t.text == "#REF!":
            a.flag("ref_error")
        elif t.kind == "op" and t.text in ("=", "<>"):
            a.ci_probed = True
            sides = [ci_side(a, ctx, sheet, [x] if x is not None and _sole_operand(sig, j, step) else None)
                     for x, j, step in ((prv, i - 1, -1), (nxt, i + 1, 1))]
            if (prv is not None and prv.kind == "str") or (nxt is not None and nxt.kind == "str"):
                ci_flag(a, "text " + t.text, *sides)
            elif not compare_is_numeric(ctx, sheet, sig, i):
                ci_flag(a, t.text, *sides)
        elif t.kind == "num":
            val = _num(t.text)
            if val is not None and val not in (0.0, 1.0):
                arith_prev = prv is not None and prv.kind == "op" and prv.text in ARITH_OPS
                arith_next = nxt is not None and nxt.kind == "op" and nxt.text in ARITH_OPS
                if arith_prev or arith_next:
                    a.flag("hardcoded_constant", t.text)

    flag_number_to_text(a, ctx, sheet, root, calls)

    # ---- per-call semantics -------------------------------------------------
    for c in calls:
        name = c.name
        a.funcs[name] += 1
        call_semantics(a, c, name, ctx, sheet)
        if name in KEYED_LOOKUPS and c.args and c.args[0]:
            key = [x for x in c.args[0] if not (isinstance(x, Tok) and x.kind == "op" and x.text in ("-", "+"))]
            if len(key) == 1 and isinstance(key[0], Tok) and key[0].kind == "num" and _num(key[0].text) is not None:
                a.flag("hardcoded_constant", key[0].text)

    agg = False
    lookup = False
    rel_single = False
    abs_single = False
    for s in a.refs:
        if s.shape != "cell":
            if s.via in LOOKUP_FUNCS:
                lookup = True
            else:
                agg = True
                if s.shape == "range" and not s.r1a and not s.r2a and s.r1 == 0 and s.r2 == 0:
                    a.flag("row_range_aggregate", "aggregates across the columns of its own row")
        elif s.r1a and s.c1a:
            abs_single = True
            txt = f"{s.sheet}!{fmt_area(Area(s.r1, s.c1, s.r1, s.c1, True, True, True, True, 'cell'))}"
            if txt not in a.absolute_refs:
                a.absolute_refs.append(txt)
        else:
            rel_single = True
    if agg:
        a.split = "range_aggregate"
    elif rel_single or lookup:
        a.split = "row_local"
    elif abs_single:
        a.split = "absolute"
    else:
        a.split = "constant"
    if depth == 0:
        a.pos_sens = ctx.pos_sens
    return a


MAX_KEYSCAN_CELLS = 4096


def _numeric_const_area(ctx, sheet, tok, first_line):
    """True when every cell of the referenced range is a numeric constant or blank (and at least one is numeric)."""
    if tok.ext is not None or tok.sheet2 is not None:
        return False
    sd = ctx.sheet_data.get(canon_sheet(ctx, tok.sheet) if tok.sheet is not None else sheet)
    ar = tok.area
    ctx.pos_sens = ctx.pos_sens or not (ar.r1a and ar.c1a and ar.r2a and ar.c2a)
    if sd is None or ar.shape == "col" or ar.shape == "row":
        return False
    r1, c1, r2, c2 = min(ar.r1, ar.r2), min(ar.c1, ar.c2), max(ar.r1, ar.r2), max(ar.c1, ar.c2)
    if first_line == "col":
        c2 = c1
    elif first_line == "row":
        r2 = r1
    if (r2 - r1 + 1) * (c2 - c1 + 1) > MAX_KEYSCAN_CELLS:
        return False
    # every copy of a shared formula re-reads the same absolute range
    key = (id(sd), r1, c1, r2, c2)
    if key not in ctx.const_area_cache:
        ctx.const_area_cache[key] = _scan_numeric_const(sd, r1, c1, r2, c2)
    return ctx.const_area_cache[key]


def _scan_numeric_const(sd, r1, c1, r2, c2):
    nums = 0
    for r in range(r1, r2 + 1):
        for c in range(c1, c2 + 1):
            if (r, c) in sd.formulas:
                return False
            k = sd.consts.get((r, c))
            if k == "n":
                nums += 1
            elif k is not None:
                return False
    return nums > 0


DATE_FUNCS = _words("DATE EDATE EOMONTH TODAY NOW DATEVALUE WORKDAY NETWORKDAYS YEAR MONTH DAY")
_NUM_CRITERIA = re.compile(r"^(<=|>=|<>|<|>|=)?\s*[-+]?(\d+\.?\d*|\.\d+)(e[-+]?\d+)?$", re.I)
_CRITERIA_OP = re.compile(r"^(<=|>=|<>|<|>|=)$")
_DATE_FORMULA = re.compile(r"^\s*(?:_xlfn\.)?(?:DATE|EDATE|EOMONTH|TODAY|NOW|DATEVALUE|WORKDAY)\(", re.I)


def _cell_is_number(ctx, sheet, tok):
    """A single cell that holds a number: a numeric constant, a date-function formula, or a formula whose cached value is a number."""
    if tok.kind != "ref" or tok.area.shape != "cell" or tok.ext is not None or tok.sheet2 is not None:
        return False
    if _numeric_const_area(ctx, sheet, tok, None):
        return True
    ctx.pos_sens = ctx.pos_sens or not (tok.area.r1a and tok.area.c1a)
    sd = ctx.sheet_data.get(canon_sheet(ctx, tok.sheet) if tok.sheet is not None else sheet)
    fc = sd.formulas.get((tok.area.r1, tok.area.c1)) if sd is not None else None
    return fc is not None and (bool(_DATE_FORMULA.match(fc.text)) or (fc.t in (None, "n") and fc.has_v and not fc.text.lstrip().startswith('"')))


def _numeric_operand(ctx, sheet, seg):
    if concat_operand_kind(ctx, sheet, seg)[0] == "num":
        return True
    if len(seg) != 1:
        return False
    it = seg[0]
    return it.name in DATE_FUNCS if isinstance(it, Call) else _cell_is_number(ctx, sheet, it)


def criteria_is_numeric(ctx, sheet, items):
    """A criteria that compares numbers: `">="&date_or_number`, an operator-and-number literal, or a bare date-function result."""
    segs, cur = [], []
    for it in items:
        if isinstance(it, Tok) and it.kind == "op" and it.text == "&":
            segs.append(cur)
            cur = []
        else:
            cur.append(it)
    segs.append(cur)
    first = segs[0][0] if len(segs[0]) == 1 else None
    if isinstance(first, Tok) and first.kind == "str":
        text = unquote(first.text)
        if len(segs) == 1:
            return bool(_NUM_CRITERIA.match(text))
        return bool(_CRITERIA_OP.match(text)) and all(_numeric_operand(ctx, sheet, sg) for sg in segs[1:])
    return len(segs) == 1 and _numeric_operand(ctx, sheet, segs[0])


def lookup_is_numeric(ctx, sheet, value, key_range, first_line=None):
    """Excel compares text case-insensitively and numbers exactly, so a numeric key makes ci_match moot; unknown stays flagged."""
    if value:
        lit = literal(value)
        if lit is not None and _num(lit) is not None:
            return True
        if len(value) == 1 and isinstance(value[0], Tok) and value[0].kind == "ref" and value[0].area.shape == "cell":
            if _numeric_const_area(ctx, sheet, value[0], None):
                return True
        if criteria_is_numeric(ctx, sheet, value):
            return True
    if key_range and len(key_range) == 1 and isinstance(key_range[0], Tok) and key_range[0].kind == "ref":
        return _numeric_const_area(ctx, sheet, key_range[0], first_line)
    return False


ZERO_COUNTERS = _words("COUNT COUNTA COUNTIF COUNTIFS SUM SUMIF SUMIFS SUMPRODUCT")


def can_reach_zero(items):
    """A computed INDEX argument that subtracts, or counts or sums, can be 0, and INDEX(range, 0) returns the whole column rather than an error."""
    if literal(items) is not None:
        return False
    for it in items:
        if isinstance(it, Tok) and it.kind == "op" and it.text == "-":
            return True
        if isinstance(it, Call) and (it.name in ZERO_COUNTERS or (it.name is None and any(can_reach_zero(x) for x in it.args))):
            return True
    return False


def _items_text(items):
    out = []
    for it in items:
        if isinstance(it, Call):
            out.append(f"{it.name or ''}(" + ",".join(_items_text(a) for a in it.args) + ")")
        else:
            out.append(it.text)
    return "".join(out)


ZERO_GUARDS = {True: (_words("> >= <>"), _words("ISNUMBER")), False: (_words("= < <="), _words("ISERROR ISERR ISNA"))}


def _walk_items(items):
    for it in items:
        yield it
        if isinstance(it, Call):
            for a in it.args:
                for x in _walk_items(a):
                    yield x


def index_arg_is_guarded(c, row_items):
    """True when INDEX sits under an IFERROR/IFNA, or in the branch of an IF/IFS whose condition tests this same row argument the right way round."""
    row = _items_text(row_items)
    child, p = c, c.parent
    while p is not None:
        pos = next((i for i, items in enumerate(p.args) if any(x is child for x in items)), None)
        if p.name in ("IFERROR", "IFNA") and pos == 0:
            return True
        cond = None
        if p.name == "IF" and pos in (1, 2):
            cond = p.args[0]
        elif p.name == "IFS" and pos is not None and pos % 2 == 1:
            cond = p.args[pos - 1]
        if cond is not None:
            ops, fns = ZERO_GUARDS[pos == 1 or p.name == "IFS"]
            if row in _items_text(cond) and any(
                    (isinstance(x, Tok) and x.kind == "op" and x.text in ops) or (isinstance(x, Call) and x.name in fns)
                    for x in _walk_items(cond)):
                return True
        child, p = p, p.parent
    return False


def is_last_match_idiom(c):
    """LOOKUP(<number>, 1/(<comparison>), <results>): the divide-by-zero errors drop non-matches, so the search lands on the last match."""
    if len(c.args) != 3 or _num(literal(c.args[0]) or "") is None:
        return False
    vec = c.args[1]
    return (len(vec) == 3 and isinstance(vec[0], Tok) and vec[0].kind == "num" and _num(vec[0].text) == 1.0
            and isinstance(vec[1], Tok) and vec[1].text == "/" and isinstance(vec[2], Call) and vec[2].name is None
            and any(isinstance(x, Tok) and x.kind == "op" and x.text in COMPARE_OPS for x in flat_tokens(vec[2].args[0])))


def compare_is_numeric(ctx, sheet, sig, i):
    """An `=`/`<>` between operands that cannot both be text: a number literal on a side, or two all-numeric constant refs."""
    def side(j, step):
        tok = sig[j] if 0 <= j < len(sig) else None
        if tok is None or tok.kind not in ("num", "ref"):
            return None
        beyond = sig[j + step] if 0 <= j + step < len(sig) else None
        if tok.kind == "num":
            return "lit" if not (beyond is not None and beyond.kind == "op" and beyond.text == "&") else None
        return "num" if _numeric_const_area(ctx, sheet, tok, None) else None
    left, right = side(i - 1, -1), side(i + 1, 1)
    return "lit" in (left, right) or (left == "num" and right == "num")


NUMERIC_FUNCS = _words("ROUND ROUNDUP ROUNDDOWN INT ABS AVERAGE AVERAGEIF AVERAGEIFS SUM SUMIF SUMIFS COUNT COUNTA COUNTIF COUNTIFS MAX MIN MEDIAN DAYS")
TEXT_FUNCS = _words("TEXT FIXED DOLLAR")


def concat_operand_kind(ctx, sheet, seg):
    """('num', shape) for an operand that is a number to Excel, ('text', '') for text, else (None, '')."""
    if len(seg) == 1:
        it = seg[0]
        if isinstance(it, Call):
            if it.name in NUMERIC_FUNCS:
                return "num", f"{it.name}(...)"
            if it.name in TEXT_FUNCS:
                return "text", ""
            return None, ""
        if it.kind == "str":
            return "text", ""
        if it.kind == "num":
            return "num", "number"
        if it.kind == "ref" and _numeric_const_area(ctx, sheet, it, None):
            return "num", "cell"
        return None, ""
    toks = [x for x in seg if isinstance(x, Tok)]
    if any(t.kind == "op" and t.text in COMPARE_OPS for t in toks):
        return None, ""
    if any(t.kind == "op" and t.text in ("+", "-", "*", "/", "^") for t in toks[1:]):
        return "num", "arithmetic"
    return None, ""


def flag_number_to_text(a, ctx, sheet, root, calls):
    """`&` joins a number to text, so the digits follow Excel's General format, which a naive port formats differently."""
    for items in [root.args[0]] + [lst for c in calls for lst in c.args]:
        segs, cur = [], []
        for it in items:
            if isinstance(it, Tok) and it.kind == "op" and it.text == "&":
                segs.append(cur)
                cur = []
            else:
                cur.append(it)
        segs.append(cur)
        kinds = [concat_operand_kind(ctx, sheet, sg) for sg in segs]
        for (k1, s1), (k2, s2) in zip(kinds, kinds[1:]):
            if (k1, k2) in (("num", "text"), ("text", "num")):
                a.flag("number_to_text", f"{s1 or 'text'}&{s2 or 'text'}")


def call_semantics(a, c, name, ctx, sheet):
    def arg(i):
        """None when omitted; [] when present but empty (`f(x,,)`), which Excel reads as 0/FALSE."""
        return c.args[i] if i < len(c.args) else None

    if len(c.args) > 1 and name in EMPTY_ARG_FUNCS | {"IF", "IFS"}:
        for i, items in enumerate(c.args, 1):
            if not items and (name not in ("IF", "IFS") or i > 1 and (name == "IF" or i % 2 == 0)):
                a.flag("empty_argument", f"{name} arg {i}")

    if adjacent_colon(c) and not (name == "INDEX" and c.args[0] and literal_index_position(c, c.args[0][0]) is not None):
        a.opaque = True
        a.flag("opaque_dependency", "range end built by a function")
        a.bump("NR", f"{name} builds one end of a range: ask which range it selects")

    if c.tok is not None and c.tok.kind == "xfn":
        a.mechs.add("addin_link")
        a.bump("NR", f"{name} calls an add-in function through an external link: which add-in? the path is never resolved")
        a.opaque = True
        return
    if name.startswith("_XLL."):
        a.mechs.add("xll_udf")
        a.bump("NR", f"function `{name[5:]}` is an XLL/COM add-in function: which add-in? translate only when its logic is supplied")
        return
    if name.startswith("_XLUDF."):
        a.mechs.add("unresolved_function")
        a.bump("NR", f"function `{name[7:]}` could not be resolved when the file was saved; the cached value is not an oracle")
        return
    if name in XLM_FUNCS:
        a.mechs.add("xlm_macro")
        a.sec.add("xlm_macros")
        a.bump("NR", f"{name} is an Excel 4.0 (XLM) macro function: never run; ask the user")
        return
    if name in WEB_FUNCS:
        a.mechs.add("webservice")
        a.sec.add("web_fetch_formula")
        a.bump("X", f"{name} fetches from the network: stays in Excel and is never fetched here")
        return
    if name in VENDOR:
        a.mechs.add("vendor_feed")
        a.bump("X", f"{name} is a live feed from {VENDOR[name]}: the cached value is a dated snapshot, not an oracle. "
                    "Data question: point Malloy at the vendor's warehouse feed if the org has one (NR)")
        if name in VOLATILE:
            a.volatile.add(name)
        return
    if name in CUBE_FUNCS:
        a.mechs.add("cube")
        a.bump("NR", f"{name} reads the Power Pivot data model or an OLAP connection: route with power-pivot.md / the connection")
        return
    if name in BLOCKED_FUNCS:
        a.mechs.add("rich_data")
        a.bump("NR", f"{name} reads a linked data type or image; the base <v> is a placeholder")
        return
    if name == "PY":
        a.mechs.add("python_in_excel")
        a.sec.add("python_in_excel")
        for arg_items in c.args:
            strs = [x for x in arg_items if isinstance(x, Tok) and x.kind == "str"]
            if strs:
                a.py_code.append(unquote(strs[0].text))
                break
        a.bump("C", "Python in Excel: the code is pandas; hand-translate it. The cached scalar is the oracle; an object result has none")
        return
    if name in LAMBDA_FAMILY:
        a.bump("NR", f"{name} is a lambda/array-helper construct with no recipe: ask what it computes")
        return
    if name in ctx.lambda_names:
        a.bump("C", f"`{name}` is a named LAMBDA: inline it at each call site (a dimension/measure when row-local)")
        return
    if name in ("OFFSET", "INDIRECT"):
        a.volatile.add(name)
        a.opaque = True
        a.flag("opaque_dependency", name)
        a.bump("NR", f"{name} builds its reference at run time: ask what range it means")
        return
    if name in RAND_FUNCS:
        a.volatile.add(name)
        a.flag("volatile", name)
        a.bump("X", f"{name} is random: stays in Excel (a seeded DuckDB draw is the C recipe for Monte Carlo)")
        return
    if name in ("TODAY", "NOW"):
        a.volatile.add(name)
        a.flag("volatile", name)
        a.bump("C", f"{name} is volatile: pin it to docProps/core.xml dcterms:modified and expose it as a given")
        return
    if name == "ANCHORARRAY":
        a.opaque = True
        a.flag("spill_ref", "A1# spill reference")
        a.flag("opaque_dependency", "spill range")
        a.bump("NR", "a spill reference (A1#) covers a range whose size is dynamic: ask which spill it follows")
        return
    if name == "SINGLE":
        a.flag("implicit_intersection", "@")
        return
    if name == "INFO":
        a.volatile.add(name)
    if name == "CHOOSE":
        parent_fn = enclosing_func(c.parent)
        has_ref = any(isinstance(x, Tok) and x.kind in ("ref", "struct") for arg_items in c.args[1:] for x in flat_tokens(arg_items))
        if parent_fn in REF_CONSUMERS and c.parent is not None and c.parent.name in REF_CONSUMERS and has_ref:
            a.opaque = True
            a.flag("opaque_dependency", "CHOOSE as a reference")
            a.bump("NR", "CHOOSE returns a reference here: ask which range it selects")
        return
    if name == "SUBTOTAL":
        code = _small_int(literal(arg(0) or []))
        a.subtotal = code if code is not None else 0
    elif name == "AGGREGATE":
        opt = _small_int(literal(arg(1) or []))
        a.subtotal = 101 if opt is None or opt in (1, 3, 5, 7) else 0
    if name in ("VLOOKUP", "HLOOKUP"):
        a.ci_probed = True
        if not lookup_is_numeric(ctx, sheet, arg(0), arg(1), "col" if name == "VLOOKUP" else "row"):
            ci_flag(a, name, ci_side(a, ctx, sheet, arg(0)), ci_side(a, ctx, sheet, arg(1), True) if name == "VLOOKUP" else None)
        m = arg(3)
        lit = literal(m) if m else None
        if m is None or (lit is not None and is_truthy_literal(lit)):
            a.flag("approx_match", name)
            a.bump("C", f"{name} without an exact-match flag is an approximate match over sorted data: a range join, not an equality join; unsorted data returns garbage")
        elif lit is None and m != []:
            a.flag("approx_match", name)
            a.bump("C", f"{name} match mode is not a literal: check both modes; treated as approximate until shown otherwise")
    elif name == "LOOKUP" and is_last_match_idiom(c):
        a.flag("last_match_idiom", name)
        a.bump("C", "LOOKUP(n, 1/(condition), results) is a last-match search, not a sorted lookup: port it as the result on the last row "
                    "(greatest row number) that meets the condition")
    elif name == "LOOKUP":
        a.ci_probed = True
        if not lookup_is_numeric(ctx, sheet, arg(0), arg(1)):
            ci_flag(a, name, ci_side(a, ctx, sheet, arg(0)), ci_side(a, ctx, sheet, arg(1)))
        a.flag("approx_match", name)
        a.bump("C", "LOOKUP is always an approximate match over sorted data: a range join")
    elif name == "MATCH":
        a.ci_probed = True
        if not lookup_is_numeric(ctx, sheet, arg(0), arg(1)):
            ci_flag(a, name, ci_side(a, ctx, sheet, arg(0)), ci_side(a, ctx, sheet, arg(1)))
        m = arg(2)
        lit = literal(m) if m else None
        if m is None or (m != [] and (lit is None or _lit_num(lit) != 0.0)):
            a.flag("approx_match", name)
            if lit is None and m is not None:
                a.bump("C", "MATCH match type is not a literal: treated as approximate until shown otherwise")
            else:
                a.bump("C", "MATCH without an exact (0) match type is an approximate match over sorted data: a range join, not an equality join")
    elif name == "INDEX":
        for pos, what in ((1, "row"), (2, "column")):
            if arg(pos) and can_reach_zero(arg(pos)) and not index_arg_is_guarded(c, arg(pos)):
                a.flag("index_row_zero", what)
    elif name == "GETPIVOTDATA":
        a.flag("getpivotdata", name)
        a.bump("NR", PIVOT_RECIPE_REASON)
    elif name in ("XLOOKUP", "XMATCH"):
        a.ci_probed = True
        if not lookup_is_numeric(ctx, sheet, arg(0), arg(1)):
            ci_flag(a, name, ci_side(a, ctx, sheet, arg(0)), ci_side(a, ctx, sheet, arg(1)))
        mm = arg(4 if name == "XLOOKUP" else 2)
        lit = literal(mm) if mm else None
        if mm and (lit is None or _lit_num(lit) not in (0.0,)):
            if lit is not None and _lit_num(lit) == 2.0:
                a.flag("criteria_wildcard", name)
                a.bump("C", f"{name} match_mode 2 is a wildcard match")
            else:
                a.flag("approx_match", name)
                if lit is None:
                    a.bump("C", f"{name} match_mode is not a literal: treated as exact-or-next smaller/larger until shown otherwise")
                else:
                    a.bump("C", f"{name} match_mode {lit} is exact-or-next-{'smaller' if _lit_num(lit) == -1.0 else 'larger'}: a linear search "
                                "with no sort assumption, correct on unsorted data; a range lookup, not an equality join")
        sm = arg(5) if name == "XLOOKUP" else arg(3)
        sl = literal(sm) if sm else None
        if sl is not None and _lit_num(sl) in (2.0, -2.0):
            a.bump("C", f"{name} binary search mode assumes sorted data")
    if name in CRITERIA_AT or name in IFS_FUNCS:
        if name in CRITERIA_AT:
            pos = CRITERIA_AT[name]
        elif name == "COUNTIFS":
            pos = tuple(range(1, len(c.args), 2))
        else:
            pos = tuple(range(2, len(c.args), 2))
        a.ci_probed = True
        bad = [p for p in pos if not lookup_is_numeric(ctx, sheet, arg(p), arg(p - 1))]
        if not pos or bad:
            ci_flag(a, name, *([ci_side(a, ctx, sheet, x) for p in bad for x in (arg(p), arg(p - 1))] or [None]))
        for p in pos:
            crit = arg(p)
            if crit:
                built = any(isinstance(t, Tok) and t.kind == "op" and t.text == "&" for t in crit) and \
                    any(not (isinstance(t, Tok) and (t.kind in ("str", "num") or t.text == "&")) for t in crit)
                for fid in classify_criteria(crit):
                    a.flag(fid, f"{name} 15sig" if fid == "criteria_comparison" and built else name)
    if name in ("AVERAGE", "AVERAGEIF", "AVERAGEIFS"):
        a.flag("avg_skips_blank_text", name)
    if name == "COUNT":
        a.flag("count_numbers_only", name)
    if name == "COUNTA":
        a.flag("counta_nonblank", name)
    if name == "SUMPRODUCT":
        toks = [t for arg_items in c.args for t in flat_tokens(arg_items)]
        if any(t.kind == "op" and t.text in COMPARE_OPS for t in toks):
            a.flag("sumproduct_mask")
        else:
            a.flag("sumproduct_product")
    if name == "LET":
        a.flag("let")
    if name == "PROPER":
        a.flag("proper_semantics", name)
    if name in DYNAMIC_ARRAY_FUNCS:
        a.bump("C", f"{name} is a dynamic-array function whose result size is dynamic: port the intent as a group_by/where/order_by, not a cell range")
        outer = enclosing_func(c.parent)
        if outer:
            a.flag("dynamic_array_scalar", f"{name} inside {outer}")
    if name in ETS_FUNCS:
        a.bump("NR", f"{name} is Excel's internal exponential-smoothing (AAA ETS) algorithm: no Malloy or DuckDB equivalent. "
                     "The user must choose a model (statsmodels or similar, outside Malloy); the cached values are the only oracle")
    elif name in AT_COST:
        a.bump("C", f"{name} has no DuckDB/Malloy built-in: a closed-form SQL expression or a recursive source")
    elif name in a.bound or name.startswith("_XLPM."):
        pass
    elif name not in KNOWN_FUNCS:
        a.mechs.add("vba_udf" if ctx.has_vba else "xll_udf")
        extra = " (vbaProject.bin is present: it may be a VBA UDF, which the script does not read; export the VBA source)" if ctx.has_vba else ""
        a.bump("NR", f"function `{name}` isn't built in, defined, or in VBA: which add-in?{extra}")


def classify_criteria(items):
    """Only a bare "", "=" or "<>" literal means blank / non-blank; concatenation changes the meaning."""
    out = []
    toks = [x for x in items if isinstance(x, Tok)]
    if not toks:
        return out
    first = toks[0]
    if len(items) == 1 and first.kind == "str":
        s = unquote(first.text)
        if s in ("", "="):
            out.append("criteria_blank")
        elif s == "<>":
            out.append("criteria_nonblank")
        elif re.match(r"^(<=|>=|<>|<|>|=)", s):
            out.append("criteria_comparison")
    elif first.kind == "str" and re.match(r"^(<=|>=|<>|<|>|=)", unquote(first.text)):
        out.append("criteria_comparison")
    if any(t.kind == "str" and ("*" in t.text or "?" in t.text) for t in toks):
        out.append("criteria_wildcard")
    if len(items) == 1 and first.kind == "num":
        out.append("criteria_numeric")
    if any(t.kind in ("ref", "struct", "name") for t in toks):
        out.append("criteria_cell")
    return out


def adjacent_colon(call):
    if call.parent is None:
        return False
    for lst in call.parent.args:
        if call in lst:
            i = lst.index(call)
            before = lst[i - 1] if i > 0 else None
            after = lst[i + 1] if i + 1 < len(lst) else None
            return any(isinstance(x, Tok) and x.kind == "op" and x.text == ":" for x in (before, after))
    return False


# --------------------------------------------------------------------------
# Workbook parts
# --------------------------------------------------------------------------

class Fcell:
    __slots__ = ("text", "ftype", "ref", "si", "has_v", "t", "v", "cm", "sv", "dt")


class SheetData:
    def __init__(self, name):
        self.name = name
        self.formulas = {}
        self.consts = {}
        self.sidx = {}
        self.inline = {}
        self.hidden_rows = set()
        self.outline_rows = 0
        self.hidden_cols = 0
        self.merges = []
        self.autofilter = None
        self.autofilter_filters = []
        self.dimension = None
        self.cf_expression = 0
        self.data_validation = 0
        self.validations = []
        self.scenarios = 0
        self.scenario_list = []
        self.vm_cells = 0
        self.table_rids = []
        self.numvals = {}
        self.low_serials = {}
        self.col_styles = defaultdict(Counter)
        self.numcap = MAX_NUMVALS
        self.row_texty = False
        self.want = frozenset()
        self.raw = {}
        self.raw_style = {}


def _float(v, default):
    try:
        out = float(v)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _int(v, default):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


_PLAIN_CELL_RE = re.compile(r"^([A-Za-z]{1,3})(\d{1,7})$")


def parse_cell_ref(ref):
    m = _PLAIN_CELL_RE.match(ref or "")
    if not m:
        return None
    row, col = int(m.group(2)), col2num(m.group(1))
    return (row, col) if 1 <= row <= MAX_ROW and 1 <= col <= MAX_COL else None


AUTOFILTER_TYPES = {"filters": "values", "customFilters": "custom", "top10": "top10", "dynamicFilter": "dynamic",
                    "colorFilter": "color", "iconFilter": "icon"}


def autofilter_filters(af):
    """The criteria on an <autoFilter>: [{col_id, type, values}], one per filtered column (values only for a value-list filter)."""
    out = []
    for fc in af:
        if ln(fc.tag) != "filterColumn":
            continue
        for ch in fc:
            typ = AUTOFILTER_TYPES.get(ln(ch.tag))
            if not typ:
                continue
            vals = [x.get("val") or "" for x in ch if ln(x.tag) == "filter"] if typ == "values" else []
            if typ == "values" and truthy(ch.get("blank")):
                vals.append("(blank)")
            out.append({"col_id": _int(fc.get("colId"), 0), "type": typ, "values": vals[:20]})
            break
    return out


def sheet_filters(sd):
    """A sheet-level autoFilter's criteria with the column letter each applies to; secret-shaped values are dropped."""
    first = parse_loc(sd.autofilter.split(":")[0]) if sd.autofilter else None
    base = first[1] if first else 1
    return [{"column": num2col(base + f["col_id"]), "type": f["type"], "values": [v for v in f["values"] if not secret_shaped(v)]}
            for f in sd.autofilter_filters]


def parse_sheet(pkg, name, path, want=()):
    data = pkg.xml_bytes(path)
    sd = SheetData(name)
    sd.want = frozenset(want)
    if data is None:
        return None
    in_data = False
    cur_row, cur_col = 0, 0
    try:
        for ev, el in iter_xml(data):
            tag = ln(el.tag)
            if ev == "start":
                if tag == "sheetData":
                    in_data = True
                elif tag == "row":
                    cur_row = _int(el.get("r"), cur_row + 1)
                    if not 1 <= cur_row <= MAX_ROW:
                        cur_row = MAX_ROW
                    cur_col = 0
                    sd.row_texty = False
                continue
            if tag == "c" and in_data:
                pos = parse_cell_ref(el.get("r"))
                if pos is None:
                    cur_col += 1
                    pos = (cur_row, cur_col)
                else:
                    cur_col = pos[1]
                take_cell(sd, el, pos)
                el.clear()
            elif tag == "row":
                if el.get("hidden") in ("1", "true"):
                    sd.hidden_rows.add(cur_row)
                if _int(el.get("outlineLevel"), 0) > 0:
                    sd.outline_rows += 1
                el.clear()
            elif tag == "sheetData":
                in_data = False
            elif tag == "mergeCell":
                sd.merges.append(el.get("ref"))
            elif tag == "autoFilter" and not in_data:
                sd.autofilter = el.get("ref") or ""
                sd.autofilter_filters = autofilter_filters(el)
            elif tag == "col":
                if el.get("hidden") in ("1", "true"):
                    lo, hi = _int(el.get("min"), 1), _int(el.get("max"), 1)
                    sd.hidden_cols += max(hi - lo + 1, 1)
            elif tag == "cfRule":
                if el.get("type") == "expression":
                    sd.cf_expression += 1
            elif tag == "dataValidation":
                sd.data_validation += 1
                if len(sd.validations) < MAX_VALIDATIONS:
                    f1 = next((x for x in el if ln(x.tag) == "formula1"), None)
                    sd.validations.append({"type": el.get("type") or "", "sqref": el.get("sqref") or "", "f1": ((f1.text or "").strip() if f1 is not None else "")})
            elif tag == "scenario":
                sd.scenarios += 1
                if len(sd.scenario_list) < 50:
                    sd.scenario_list.append({"name": el.get("name") or "", "cells": [
                        {"cell": x.get("r") or "", "value": x.get("val") or ""} for x in el if ln(x.tag) == "inputCells"][:50]})
            elif tag == "dimension":
                sd.dimension = el.get("ref")
            elif tag == "tablePart":
                r = rid_of(el)
                if r:
                    sd.table_rids.append(r)
    except PARSE_ERRORS:
        pkg.rejected.append({"part": path, "reason": "malformed"})
        return None
    return sd


def take_cell(sd, el, pos):
    t = el.get("t")
    f = v = isel = None
    for ch in el:
        n = ln(ch.tag)
        if n == "f":
            f = ch
        elif n == "v":
            v = ch
        elif n == "is":
            isel = ch
    if el.get("vm") is not None:
        sd.vm_cells += 1
    if pos in sd.want and v is not None:
        sd.raw[pos] = v.text or ""
        sd.raw_style[pos] = _int(el.get("s"), 0)
    if f is not None:
        fc = Fcell()
        fc.text = f.text or ""
        fc.ftype = f.get("t") or "normal"
        fc.ref = norm_ref(f.get("ref"))
        fc.si = f.get("si")
        fc.has_v = v is not None and (bool(v.text) or t == "str")
        fc.t = t
        fc.v = (v.text or "") if (v is not None and t == "e") else None
        fc.sv = (v.text or "")[:256] if (v is not None and t == "str") else None
        fc.cm = el.get("cm") is not None
        fc.dt = ({"dt2D": f.get("dt2D"), "dtr": f.get("dtr"), "r1": norm_ref(f.get("r1")), "r2": norm_ref(f.get("r2"))}
                 if fc.ftype == "dataTable" else None)
        sd.formulas[pos] = fc
        return
    if v is None and isel is None:
        return
    if t in ("s", "inlineStr", "str"):
        sd.row_texty = True
    if t == "s":
        sd.consts[pos] = "s"
        try:
            sd.sidx[pos] = int(v.text)
        except (TypeError, ValueError, AttributeError):
            pass
    elif t == "inlineStr":
        sd.consts[pos] = "inlineStr"
        if isel is not None:
            sd.inline[pos] = "".join((x.text or "") for x in isel.iter() if ln(x.tag) == "t")
    elif t in ("str", "b", "e", "d"):
        sd.consts[pos] = t
    else:
        sd.consts[pos] = "n"
        style = el.get("s")
        if v is not None and (style not in (None, "0") or (sd.row_texty and sd.numcap > 0)):
            try:
                num = float(v.text)
            except (TypeError, ValueError):
                num = None
            if num is not None and math.isfinite(num):
                if sd.row_texty and sd.numcap > 0:
                    sd.numvals[pos] = num
                    sd.numcap -= 1
                if style not in (None, "0"):
                    sd.col_styles[pos[1]][_int(style, 0)] += 1
                if style not in (None, "0") and 1 <= num < 61 and len(sd.low_serials) < MAX_NUMVALS:
                    sd.low_serials[pos] = (num, _int(style, 0))


def read_shared_strings(pkg, path, needed):
    out = {}
    if not needed or not path:
        return out
    data = pkg.xml_bytes(path)
    if data is None:
        return out
    i = -1
    try:
        for ev, el in iter_xml(data, events=("end",)):
            if ln(el.tag) != "si":
                continue
            i += 1
            if i in needed:
                txt = []
                for ch in el:
                    n = ln(ch.tag)
                    if n == "t":
                        txt.append(ch.text or "")
                    elif n == "r":
                        txt.extend((x.text or "") for x in ch if ln(x.tag) == "t")
                out[i] = "".join(txt)
            el.clear()
    except PARSE_ERRORS:
        pkg.rejected.append({"part": path, "reason": "malformed"})
    return out


# --------------------------------------------------------------------------
# Assembly
# --------------------------------------------------------------------------

def truthy(v):
    return v in ("1", "true", "True")


def read_workbook(pkg):
    root_rels = pkg.xml("_rels/.rels")
    wb_path = "xl/workbook.xml"
    if root_rels is not None:
        for r in root_rels:
            if ln(r.tag) == "Relationship" and (r.get("Type") or "").endswith("/officeDocument"):
                tgt = r.get("Target") or ""
                wb_path = tgt[1:] if tgt.startswith("/") else posixpath.normpath(tgt)
                break
    root = pkg.xml(wb_path)
    if root is None:
        return None, wb_path
    return root, wb_path


class Book:
    pass


def parse_workbook(root, wb_rels):
    b = Book()
    b.sheets, b.names, b.calc = [], [], {}
    b.date1904 = False
    b.pivot_caches = {}
    b.external_refs = 0
    for el in root:
        n = ln(el.tag)
        if n == "workbookPr":
            b.date1904 = truthy(el.get("date1904"))
        elif n == "sheets":
            for s in el:
                rid = rid_of(s)
                tgt = wb_rels.get(rid)
                b.sheets.append({"name": s.get("name") or "", "sheet_id": s.get("sheetId"), "state": s.get("state") or "visible",
                                 "path": tgt[1] if tgt else None})
        elif n == "definedNames":
            for d in el:
                b.names.append({"name": d.get("name") or "", "local": d.get("localSheetId"), "hidden": truthy(d.get("hidden")),
                                "text": (d.text or "").strip()})
        elif n == "calcPr":
            b.calc = dict(el.attrib)
        elif n == "externalReferences":
            b.external_refs = sum(1 for _ in el)
        elif n == "pivotCaches":
            for pc in el:
                b.pivot_caches[pc.get("cacheId")] = rid_of(pc)
    return b


_EXT_MARK = re.compile(r"(?<![A-Za-z0-9_\]])\[\d+\]")


def classify_name(text):
    t = text.strip()
    if re.match(r"^(_xlfn\.)?LAMBDA\(", t, re.I):
        return "lambda", None
    toks = [x for x in tokenize(t) if x.kind != "ws"]
    if len(toks) == 1 and toks[0].kind == "ref":
        return "ref", toks[0]
    if len(toks) == 1 and toks[0].kind in ("num", "str"):
        return "constant", None
    if len(toks) == 2 and toks[0].text in "-+" and toks[1].kind == "num":
        return "constant", None
    if len(toks) == 1 and toks[0].kind == "name" and toks[0].text.upper() in ("TRUE", "FALSE"):
        return "constant", None
    return "formula", None


def find_components(cells_by_row):
    parent = []

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    runs = []
    prev_ids, prev_row = [], None
    for row in sorted(cells_by_row):
        cols = sorted(cells_by_row[row])
        rr = []
        start = prev = cols[0]
        for c in cols[1:]:
            if c == prev + 1:
                prev = c
            else:
                rr.append((start, prev))
                start = prev = c
        rr.append((start, prev))
        ids = []
        for a, b in rr:
            i = len(parent)
            parent.append(i)
            runs.append((row, a, b, i))
            ids.append((a, b, i))
        if prev_row == row - 1:
            for a, b, i in ids:
                for pa, pb, pi in prev_ids:
                    if a <= pb + 1 and b >= pa - 1:
                        ra, rb = find(i), find(pi)
                        if ra != rb:
                            parent[ra] = rb
        prev_ids, prev_row = ids, row
    groups = defaultdict(list)
    for row, a, b, i in runs:
        groups[find(i)].append((row, a, b))
    comps = []
    for rs in groups.values():
        comps.append({"r1": min(r for r, _, _ in rs), "r2": max(r for r, _, _ in rs),
                      "c1": min(a for _, a, _ in rs), "c2": max(b for _, _, b in rs),
                      "runs": rs})
    comps.sort(key=lambda c: (c["r1"], c["c1"]))
    return comps


_CTRL = re.compile(r"[\x00-\x1f\x7f\x85\u2028\u2029]+")


def flat(s):
    """One line, no control characters: workbook text must not start a new report line."""
    return _CTRL.sub(" ", s or "")


def md(s):
    return flat(s).replace("|", "\\|")


def sql_q(s):
    return flat(s).replace("'", "''")


def ident_q(s):
    return '"' + flat(s).replace('"', '""') + '"'


def malloy_unsafe(*texts):
    """True when text would end a Malloy triple-quoted string or open a %{ interpolation inside it."""
    return any('"""' in t or "%{" in t for t in texts)


UNSAFE_NAME_WHY = "the file or sheet name contains a triple quote or %{, which would end or interpolate the Malloy string: rename it"


def safe_ident(name, fallback):
    n = flat(name).strip()
    return n if n and not malloy_unsafe(n) else fallback


# Malloy keywords and date-part words that break an unquoted reference to a column of that name.
MALLOY_RESERVED = frozenset(
    "source run query view join_one join_many join_cross dimension measure group_by aggregate select where having order_by limit nest "
    "extend import declare calculate project index top pick when else is on with and or not true false null "
    "year month quarter week day hour minute second date timestamp now years months quarters weeks days hours minutes seconds".split())


def reserved_columns(names):
    """Names (in order, once each) that Malloy reads as a keyword or date part."""
    out = []
    for n in names:
        if n and n.lower() in MALLOY_RESERVED and n not in out:
            out.append(n)
    return out


def render_stanza(book_name, sheet, p):
    """p: rng, header, layout, lifted (names, or sheet letters when headerless), dropped (header names or None),
    subtotal_rows (in range), trimmed, hidden, mixed, header_rows, flagged. Returns (stanza, refusal)."""
    if malloy_unsafe(book_name, sheet):
        return None, UNSAFE_NAME_WHY
    if not p["lifted"]:
        return None, "no column can be lifted as data"
    alias = p.get("alias")
    varchar = bool(p.get("varchar")) and p["layout"] != "wide"
    src = (f"read_xlsx('{sql_q(book_name)}', sheet = '{sql_q(sheet)}', range = '{p['rng']}', "
           f"header = {'true' if p['header'] and not alias and not p.get('wide_cols') else 'false'}{', all_varchar = true' if varchar else ''})")
    lines = ['duckdb.sql("""']
    if p["layout"] == "wide" and p.get("wide_cols"):
        wc = p["wide_cols"]
        rv = "__r"
        taken = {w["out"].lower() for w in wc} | {w["src"].lower() for w in wc}
        while rv in taken:
            rv += "_"
        vname, pname = "amount", "period"
        while vname in taken:
            vname += "_"
        while pname in taken or pname == vname:
            pname += "_"
        use_r = any(w["null_rows"] for w in wc)
        items = []
        for w in wc:
            e = ident_q(w["src"])
            if w["role"] == "label" and w["null_rows"]:
                e = f"CASE WHEN {rv} IN ({', '.join(map(str, w['null_rows']))}) THEN NULL ELSE {e} END"
            items.append(f"{e} AS {ident_q(w['out'])}")
        labels = [ident_q(w["out"]) for w in wc if w["role"] == "label"]
        if use_r:
            items.append(rv)
            labels.append(rv)
            frm = f"(SELECT *, row_number() OVER () + {int(p.get('first_row') or 1) - 1} AS {rv} FROM {src})"
        else:
            frm = src
        lines.append(f"  SELECT *{' EXCLUDE (' + rv + ')' if use_r else ''} FROM (SELECT {', '.join(items)} FROM {frm})")
        lines.append(f"  UNPIVOT ({vname} FOR {pname} IN (COLUMNS(* EXCLUDE ({', '.join(labels)}))))")
        pairs = [f"({rv} IN ({', '.join(map(str, w['null_rows']))}) AND {pname} = '{sql_q(w['out'])}')" for w in wc if w["role"] == "period" and w["null_rows"]]
        if pairs:
            lines.append("  WHERE NOT (" + " OR ".join(pairs) + ")")
        lines.append("  -- wide layout (periods across columns): one row per label and period; the header row is the period names, never data")
        lines.append("  -- every column is selected by its sheet letter and named here, so a formula column inside the range is not read at all")
    elif p["layout"] == "wide":
        drop = [d for d in p["dropped"] if d]
        inner = f"SELECT * EXCLUDE ({', '.join(ident_q(d) for d in drop)}) FROM {src}" if drop else f"SELECT * FROM {src}"
        label = ", ".join(ident_q(n) for n in p["lifted"])
        taken = {n.lower() for n in list(p["lifted"]) + [d for d in p["dropped"] if d]}
        vname, pname = "amount", "period"
        while vname in taken:
            vname += "_"
        while pname in taken or pname == vname:
            pname += "_"
        lines.append(f"  SELECT * FROM ({inner})")
        lines.append(f"  UNPIVOT ({vname} FOR {pname} IN (COLUMNS(* EXCLUDE ({label}))))")
        lines.append("  -- wide layout (periods across columns): one row per label and period; the header row is the period names, never data")
        if any(d is None for d in p["dropped"]):
            lines.append("  -- TODO: a formula column has a non-text header and is still inside the range: exclude it before the UNPIVOT")
    else:
        cols = p.get("cols")
        if cols is None:
            cols = [{"src": letter, "out": n, "kind": "text", "null_rows": []} for letter, n in alias] if alias else \
                   [{"src": n, "out": n, "kind": "text", "null_rows": []} for n in p["lifted"]]
        pred = p.get("subtotal_pred")
        by_pos = bool(p["subtotal_rows"]) and not pred and bool(p.get("first_row"))
        use_r = by_pos or any(cc["null_rows"] for cc in cols)
        rv = "__r"
        taken = {cc["src"].lower() for cc in cols} | {cc["out"].lower() for cc in cols}
        while rv in taken:
            rv += "_"
        items = []
        for cc in cols:
            e = ident_q(cc["src"])
            base = "1904-01-01" if p.get("date1904") else "1899-12-30"
            x = f"TRY_CAST({e} AS DOUBLE)"

            def conv(b, kind=cc["kind"]):
                if kind == "date":
                    return f"DATE '{b}' + TRY_CAST(floor({x}) AS INTEGER)"
                return f"TIMESTAMP '{b} 00:00:00' + to_seconds(TRY_CAST(round({x} * 86400) AS BIGINT))"
            if cc["kind"] == "number":
                e = x
            elif cc["kind"] in ("date", "datetime") and p.get("low_serial"):
                # Excel counts a 1900-02-29 that never existed: serials below 60 are a day later, 60 is no date
                e = f"CASE WHEN {x} < 60 THEN {conv('1899-12-31')} WHEN {x} < 61 THEN NULL ELSE {conv(base)} END"
            elif cc["kind"] in ("date", "datetime"):
                e = conv(base)
            if cc["null_rows"]:
                e = f"CASE WHEN {rv} IN ({', '.join(map(str, cc['null_rows']))}) THEN NULL ELSE {e} END"
            items.append(e if e == ident_q(cc["out"]) else f"{e} AS {ident_q(cc['out'])}")
        lines.append("  SELECT " + ", ".join(items))
        if use_r:
            lines.append(f"  FROM (SELECT *, row_number() OVER () + {int(p.get('first_row') or 1) - 1} AS {rv} FROM {src})")
        else:
            lines.append(f"  FROM {src}")
        if p["subtotal_rows"] and pred:
            lines.append(f"  WHERE COALESCE({ident_q(pred[0])}, '') NOT ILIKE '%{pred[1]}%'")
            lines.append(f"  -- subtotal rows ({', '.join(map(str, p['subtotal_rows']))}) are excluded by their label; check no data row carries it")
        elif by_pos:
            lines.append(f"  WHERE {rv} NOT IN ({', '.join(map(str, p['subtotal_rows']))})")
            lines.append(f"  -- subtotal/total rows are excluded by sheet row ({rv} is the row number in the sheet, in sheet order); re-derive it if rows move")
        elif p["subtotal_rows"]:
            lines.append(f"  WHERE <exclude subtotal/total rows: sheet rows {', '.join(map(str, p['subtotal_rows']))}>")
        if p.get("inputs"):
            lines.append("  -- inputs in the header row: " + "; ".join(f"{i['cell']} = {i['value']:g} ({i['suggested_given']})" for i in p["inputs"]) +
                         ": formulas below read them, so keep them as givens (or a 3-row source), never as data")
        if alias and p.get("inputs") and not p.get("bad_header"):
            lines.append("  -- read positionally from the first data row (header = false): the names come from the header row")
        elif alias and (p.get("bad_header") or p["header_rows"] <= 1):
            lines.append("  -- header text is unusable as a column name (quotes, blank, leading or trailing space, or duplicate): read positionally and aliased")
        elif not p["header"]:
            lines.append("  -- header = false: DuckDB names columns by sheet column letter")
        if p.get("ambiguous"):
            lines.append("  -- ambiguous: the first row mixes a label with numbers, read here as data; if it is a period header row, this block is wide")
    if p.get("masked_rows"):
        lines.append("  -- cells masked as secrets are set to NULL here, by sheet row: " + ", ".join(map(str, p["masked_rows"])))
    if p.get("first_row_text"):
        lines.append(f"  -- first row is {p['first_row_text']} text and may be a header: verify; filter it out in the wrapper")
    if p.get("reserved"):
        lines.append("  -- Malloy-reserved column name(s): " + ", ".join(p["reserved"]) +
                     ": reference them with backticks (`" + p["reserved"][0] + "`) or rename with SELECT ... AS")
    if p["not_lifted"]:
        lines.append("  -- not lifted (formula columns or array/spill/data-table/pivot output): " + ", ".join(ident_q(c) for c in p["not_lifted"]))
    if p["flagged"]:
        shown = ", ".join(p["flagged"][:20]) + (f" (+{len(p['flagged']) - 20} more)" if len(p["flagged"]) > 20 else "")
        if p["layout"] == "wide" and p.get("wide_cols"):
            lines.append(f"  -- WARNING: formula cells inside lifted columns are set to NULL here, by sheet row and period, so they never enter as data: {shown}; "
                         "write each as a dimension: or measure: instead")
        elif p["layout"] == "wide":
            lines.append(f"  -- WARNING: formula cells inside lifted columns come back as cached values and are NOT excluded in a wide layout: {shown}; drop those (row, period) pairs by hand")
        else:
            lines.append(f"  -- WARNING: formula cells inside lifted columns are set to NULL here, by sheet row, so they never enter as data: {shown}; "
                         "write each as a dimension: or measure: instead")
    if p.get("excluded_rows"):
        lines.append("  -- excluded rows: " + ", ".join(f"{e['row']} (via {e['via']})" for e in p["excluded_rows"]) + "; verify each is a total, not data")
    if p["trimmed"]:
        lines.append(f"  -- trailing total rows left out of the range: sheet rows {', '.join(map(str, p['trimmed']))}")
    if p["header_rows"] > 1:
        if alias:
            lines.append(f"  -- {p['header_rows']}-row header (merged group labels above the column names): the names come from the merged and "
                         f"sub-header cells; the read starts at the first data row ({p['rng'].split(':')[0]}) with header = false")
        else:
            lines.append(f"  -- {p['header_rows']}-row header (merged group labels above the column names): the range starts at the column-name row; "
                         "the group labels above it are left out")
    if p["hidden"]:
        lines.append(f"  -- hidden rows in range: {', '.join(map(str, p['hidden'][:20]))}{' ...' if len(p['hidden']) > 20 else ''}")
    for d in p.get("date_text") or ():
        order = {"ambiguous": "day/month order is ambiguous (both parse): ask the owner, never guess",
                 "conflicting": "the cells disagree on day/month order: ask the owner",
                 "day-first": "day-first, proven by a day above 12", "month-first": "month-first, proven by a day above 12"}.get(d["order"])
        lines.append(f"  -- date_as_text: {ident_q(d['column'])} ({d['ref']}) holds {d['format']} text, not date serials, so it needs an explicit parse"
                     + (f"; {order}" if order else ""))
    if p.get("text_cells"):
        more = p["text_count"] - len(p["text_cells"])
        lines.append("  -- text cells in numeric, date or blank-led columns: " + ", ".join(f"{t['cell']} ({t['in']})" for t in p["text_cells"]) +
                     (f" (+{more} more)" if more > 0 else ""))
        lines.append("  -- a typed read fails on text under a number (DuckDB types a column from its first row), so the read is all_varchar = true with TRY_CAST. "
                     "TRY_CAST turns text such as '1', ' 7 ', '1e3' or 'nan', and a boolean (1/0), into a number, which Excel's SUM ignores: "
                     "the cells above are NULLed by sheet row to match Excel (delete a row from the CASE list to coerce that cell instead); "
                     "a t=\"d\" cell is an ISO date string and is text to this read")
        if p.get("coerced"):
            lines.append(f"  -- more than {MAX_NULLED_ROWS} such cells in a column: the rest are left to TRY_CAST, which reads numeric-looking text as a number")
    elif p["mixed"]:
        lines.append("  -- mixed text/number columns: " + ", ".join(ident_q(m) for m in p["mixed"]) + " (all_varchar = true with TRY_CAST on the numbers)")
    elif varchar and not p.get("has_date_cols"):
        lines.append("  -- all_varchar = true: a column of the range outside this lift holds text, errors or a formula string that a typed read cannot parse; "
                     "the numeric columns are TRY_CAST")
    for cc in (p.get("cols") or ()):
        if cc.get("sibling"):
            lines.append(f"  -- {ident_q(cc['out'])} is the same column read as a number, because it is mostly text: the text column keeps every value as written")
    if varchar and p.get("date1904") and p.get("has_date_cols"):
        lines.append("  -- WARNING: this workbook uses the 1904 date system: a typed read of a date column is four years and a day off, "
                     "so date columns are read as serials and converted from 1904-01-01")
    if p.get("low_serial"):
        lines.append("  -- a date serial below 61 is corrected for Excel's 1900 leap-year bug (1 to 59 are a day later, 60 is NULL): check those cells by hand")
    lines.append('""")')
    if malloy_unsafe("\n".join(lines[1:-1])):
        return None, "the stanza would contain a triple quote or %{, which would end or interpolate the Malloy string"
    return "\n".join(lines), None


def render_oracle_stanza(book_name, sheet, ref):
    """A read of exactly one region's own cached cells; None when the names would end the Malloy string or the ref is not a range."""
    loc = parse_loc(ref) if isinstance(ref, str) else None
    if not loc or malloy_unsafe(book_name, sheet):
        return None
    return "\n".join([
        'duckdb.sql("""',
        "  -- ORACLE read: the workbook's cached values, to compare Malloy output against; not a source to model",
        f"  SELECT * FROM read_xlsx('{sql_q(book_name)}', sheet = '{sql_q(sheet)}', range = '{rect_span(*loc)}', "
        "header = false, all_varchar = true)",
        "  -- all_varchar: a typed read fails when a range mixes text, numbers and error values; TRY_CAST the numbers to compare",
        '""")'])


def absolute_reads(regions, skip_via=frozenset()):
    """{(sheet lower, row, col): formula regions reading that one cell by a fully absolute ref ($B$2)}."""
    hits = Counter()
    for g in regions:
        seen = set()
        for s in g["an"].refs:
            if s.r1a and s.r2a and s.c1a and s.c2a and s.r1 == s.r2 and s.c1 == s.c2 and s.via not in skip_via:
                seen.add((s.sheet.lower(), s.r1, s.c1))
        hits.update(seen)
    return hits


def cell_reads_by(regions, keep):
    """{(sheet lower, row, col): formula regions reading that one cell through a ref `keep` accepts, by any ref style}."""
    hits = Counter()
    for g in regions:
        hr, hc = min(g["cells"])
        seen = set()
        for s in g["an"].refs:
            if not keep(s) or s.shape != "cell":
                continue
            # a relative ref in a many-cell region reads a different cell per row: a column, not a control
            if len(g["cells"]) > 1 and not (s.r1a and s.c1a):
                continue
            r, c = s.resolve(hr, hc)[:2]
            if r >= 1 and c >= 1:
                seen.add((s.sheet.lower(), r, c))
        hits.update(seen)
    return hits


def selector_reads(regions):
    """Cells read in an IF/IFS/CHOOSE/SWITCH/INDEX selector position, a lookup value or a criteria."""
    return cell_reads_by(regions, lambda s: s.sel)


def ci_side_spellings(spec, cells, sources, cols):
    """The {lowercase: spellings} dict of each source text column a ci_match side reads across a region's copies; None when any cell it reads is not known text."""
    rows, colsn = [r for r, _ in cells], [c for _, c in cells]
    a_, b_ = spec.resolve(min(rows), min(colsn)), spec.resolve(max(rows), max(colsn))
    r1, c1, r2, c2 = min(a_[0], b_[0]), min(a_[1], b_[1]), max(a_[2], b_[2]), max(a_[3], b_[3])
    for src in sources:
        loc = parse_loc(src["data_ref"]) if src.get("_spelled") and src["sheet"].lower() == spec.sheet.lower() and src.get("layout") == "long" else None
        if not loc or r1 < src["first_data_row"] or r2 > loc[2] or c1 < loc[1] or c2 > loc[3]:
            continue
        by_letter = dict(zip(src["lifted_columns"], src["lifted"]))
        if "_formula_cols" not in src:
            src["_formula_cols"] = [parse_ref_a1(x) for x in src["formula_cells_in_lifted"]]
        out = []
        for c in range(c1, c2 + 1):
            name = by_letter.get(num2col(c))
            if name not in src["_spelled"] or any(f and f[1] == c and r1 <= f[0] <= r2 for f in src["_formula_cols"]):
                return None
            out.append(src["_spelled"][name])
            cols[(src["sheet"], num2col(c))] = src["case_variant_keys"][name]
        return out
    return None


def _has_variant(spelled):
    return any(len(sp) > 1 for sp in spelled.values())


def _spellings_differ(dicts, seen):
    """Some lowercase text has more than one spelling inside one dict, or across dicts, comparing only the keys two dicts share."""
    for d in dicts:
        # ids of the transient one-entry literal dicts are reused, so only a source column is memoized
        if len(d) == 1:
            if _has_variant(d):
                return True
            continue
        if id(d) not in seen:
            seen[id(d)] = _has_variant(d)
        if seen[id(d)]:
            return True
    for i, x in enumerate(dicts):
        for y in dicts[i + 1:]:
            small, big = (x, y) if len(x) <= len(y) else (y, x)
            for low, sp in small.items():
                other = big.get(low)
                if other and len(sp | other) > 1:
                    return True
    return False


def ci_match_summary(regions, sources):
    """Whether the text a ci_match region compares has one spelling per value across every side it compares, so case-insensitivity cannot change a result."""
    cols, flagged, known, variants, seen = {}, 0, True, False, {}
    for g in regions:
        if "ci_match" not in g["flags_map"]:
            continue
        flagged += 1
        dicts = []
        sides = g["an"].ci_sides
        ok = bool(sides) and None not in sides
        for side in sides.values() if ok else ():
            got = [{side[1].lower(): {side[1]}}] if side[0] == "lit" else ci_side_spellings(side[1], g["cells"], sources, cols)
            if got is None:
                ok = False
                break
            dicts.extend(got)
        known = known and ok
        variants = variants or (ok and _spellings_differ(dicts, seen))
    for src in sources:
        src.pop("_spelled", None)
        src.pop("_formula_cols", None)
    return {"flagged_regions": flagged, "cannot_bite": bool(flagged) and known and not variants and bool(cols),
            "key_columns": [{"sheet": a, "column": b, "case_variants": n} for (a, b), n in sorted(cols.items())]}


def key_reads(regions):
    """Cells read as an IF condition, a lookup value or a criteria: an input whichever way the sheet is laid out."""
    return cell_reads_by(regions, lambda s: s.sel == "key")


def percent_style_ids(root):
    """Indexes of cellXfs whose number format shows a percentage."""
    if root is None:
        return set()
    custom = {}
    for el in root.iter():
        if ln(el.tag) == "numFmt":
            custom[_int(el.get("numFmtId"), -1)] = el.get("formatCode") or ""
    out = set()
    for el in root.iter():
        if ln(el.tag) != "cellXfs":
            continue
        for i, xf in enumerate(x for x in el if ln(x.tag) == "xf"):
            fid = _int(xf.get("numFmtId"), 0)
            if fid in (9, 10) or (fid in custom and "%" in _FMT_NOISE.sub("", custom[fid])):
                out.add(i)
    return out


def sheet_area(text, ctx, default_sheet=None):
    """'Sheet!$B$2:$C$3' -> (sheet, (r1, c1, r2, c2)) for one in-workbook cell or range; None for anything else."""
    kind, tok = classify_name(text.strip())
    if kind != "ref" or tok is None or tok.ext is not None or tok.sheet2 or tok.area.shape not in ("cell", "range"):
        return None
    sheet = canon_sheet(ctx, tok.sheet) if tok.sheet else default_sheet
    a = tok.area
    return (sheet, (a.r1, a.c1, a.r2, a.c2)) if sheet else None


def split_union(text):
    """Split 'S!$A$1,S!$B$2:$B$3' at the commas that sit outside a quoted sheet name."""
    t = text.strip().lstrip("=").strip()
    if t.startswith("(") and t.endswith(")"):
        t = t[1:-1]
    parts, cur, quoted = [], [], False
    for ch in t:
        quoted = quoted != (ch == "'")
        if ch == "," and not quoted:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return [x.strip() for x in parts if x.strip()]


def area_text(sheet, rect, with_sheet=True):
    return (f"{sheet}!" if with_sheet else "") + rect_a1(*rect)


CTRL_KINDS = {"Drop": "choice", "List": "choice", "Radio": "choice", "CheckBox": "bool"}
CTRL_TYPE_RE = re.compile(r"^[A-Za-z]{1,16}$")


def control_inventory(pkg, sheets, ctx):
    """Form controls by type and reference only (no caption is read). Returns (inventory, given extras);
    a control that sits on or links to a masked sheet, or on no sheet, is counted and never listed."""
    host = {}
    for sh in sheets:
        for typ, tgt, ext_ in sh["rels"].values():
            if typ == "ctrlProp" and tgt and not ext_:
                host[tgt] = sh
    by_name = {sh["meta"]["name"]: sh for sh in sheets}
    listed, extras, total = [], {}, 0
    for part in pkg.under("xl/ctrlProps/"):
        if not part.endswith(".xml") or "/_rels/" in part:
            continue
        root = pkg.xml(part)
        el = next((x for x in (root.iter() if root is not None else ()) if ln(x.tag) == "formControlPr"), None)
        if el is None:
            continue
        total += 1
        home = host.get(part)
        link = sheet_area(el.get("fmlaLink") or "", ctx, home["meta"]["name"] if home else None)
        lst = sheet_area(el.get("fmlaRange") or "", ctx, home["meta"]["name"] if home else None)
        names = [x[0] for x in (link, lst) if x] or ([home["meta"]["name"]] if home else [])
        if not names or any(by_name[n]["masked"] for n in names):
            continue
        typ = el.get("objectType") if CTRL_TYPE_RE.match(el.get("objectType") or "") else None
        picked = _int(el.get("sel"), _int(el.get("val"), None))
        if picked is None and el.get("checked"):
            picked = el.get("checked") == "Checked"
        entry = {"type": typ, "sheet": names[0],
                 "linked_cell": area_text(link[0], link[1], False) if link else None,
                 "list_range": area_text(lst[0], lst[1], lst[0] != names[0]) if lst else None,
                 "min": _int(el.get("min"), None), "max": _int(el.get("max"), None), "step": _int(el.get("inc"), None), "selected": picked}
        listed.append(entry)
        if link and link[1][0] == link[1][2] and link[1][1] == link[1][3]:
            extras[(link[0].lower(), link[1][0], link[1][1])] = {
                "forced": True, "label": f"form control: {typ or 'unknown'}", "kind": CTRL_KINDS.get(typ, "number"), "value": picked}
    return {"count": total, "not_listed": total - len(listed), "controls": listed}, extras


SOLVER_RELATIONS = {1: "<=", 2: "=", 3: ">=", 4: "int", 5: "bin", 6: "alldifferent"}
MAX_SOLVER_CELLS = 100


def solver_inventory(defs, ctx, sheets):
    """The saved Solver model (solver_* names) by reference only: objective, changing cells and constraints on visible sheets.
    Returns (inventory, changing-cell extras for given_candidates); targets on a masked sheet are counted, never listed."""
    masked = {sh["meta"]["name"] for sh in sheets if sh["masked"]}
    groups = defaultdict(dict)
    for scope, name, text in defs:
        groups[scope or "workbook"][name.lower()] = text or ""
    models, extras, hidden_total = [], {}, 0
    for scope, d in sorted(groups.items()):
        home = scope if scope in ctx.sheet_names else None
        counts = {"hidden": 0}

        def areas(text):
            """[(sheet, rect)] for a union of refs, or None when any part is hidden or not a plain in-workbook ref."""
            out = []
            for part in split_union(text):
                a = sheet_area(part, ctx, home)
                if a is None:
                    return None
                out.append(a)
            if any(sh in masked for sh, _ in out):
                counts["hidden"] += 1
                return None
            return out or None

        def show(parts):
            return ",".join(area_text(sh, rect) for sh, rect in parts)

        obj = areas(d["solver_opt"]) if "solver_opt" in d else None
        adj = areas(d["solver_adj"]) if "solver_adj" in d else None
        cons = []
        for key in sorted((k for k in d if re.match(r"^solver_lhs\d+$", k)), key=lambda k: int(k[10:])):
            n = key[10:]
            lhs = areas(d[key])
            rel = _int((d.get(f"solver_rel{n}") or "").strip(), None)
            rhs_text = (d.get(f"solver_rhs{n}") or "").strip().lstrip("=")
            if lhs is None or rel not in SOLVER_RELATIONS:
                continue
            if rel in (4, 5, 6):
                cons.append({"lhs": show(lhs), "relation": SOLVER_RELATIONS[rel], "rhs": None})
                continue
            num = _num(rhs_text)
            rhs = areas(rhs_text) if num is None else None
            if num is None and rhs is None:
                continue
            cons.append({"lhs": show(lhs), "relation": SOLVER_RELATIONS[rel],
                         "rhs": (int(num) if num == int(num) else num) if num is not None else show(rhs)})
        models.append({"scope": scope, "objective": show(obj) if obj else None, "changing_cells": [area_text(sh, rect) for sh, rect in adj or ()],
                       "constraints": cons, "hidden_targets": counts["hidden"]})
        hidden_total += counts["hidden"]
        for sh, (r1, c1, r2, c2) in adj or ():
            for r in range(r1, r2 + 1):
                for c in range(c1, c2 + 1):
                    if len(extras) < MAX_SOLVER_CELLS * 2:
                        extras.setdefault((sh.lower(), r, c), {"label": "solver changing cell"})
    return {"count": sum(len(g) for g in groups.values()), "models": models, "hidden_targets": hidden_total}, extras


MAX_RUN_WALK = 200
INPUT_KINDS = ("n", "b", "s", "inlineStr", "str")
TEXT_KINDS = ("s", "inlineStr", "str")


def formula_line(sd, r, c, dr, dc):
    """True when the cells on both sides of (r, c) along one axis, in an unbroken run, are mostly formulas."""
    f = n = 0
    for sign in (-1, 1):
        for k in range(1, MAX_RUN_WALK + 1):
            pos = (r + sign * dr * k, c + sign * dc * k)
            if pos in sd.formulas:
                f += 1
            elif pos in sd.consts:
                n += 1
            else:
                break
    return f > 0 and f >= n


def lifted_areas(sources, derived):
    """{sheet: [(row1, row2, {cols})]} for sources with a stanza: ordinary ones hold data, derived ones hold constants beside formulas."""
    out = defaultdict(list)
    for src in sources:
        loc = parse_loc(src["data_ref"]) if src.get("stanza") and bool(src.get("derived")) == derived else None
        if loc:
            out[src["sheet"]].append((loc[0], loc[2], {col2num(x) for x in src["lifted_columns"]}))
    return out


def inputs_block(book_name, sheet, r1, c1, r2, c2):
    """A source record whose stanza reads exactly the bounding rectangle of typed inputs, cell-addressed."""
    why = UNSAFE_NAME_WHY if malloy_unsafe(book_name, sheet) else None
    rec = refused_block(sheet, r1, c1, r2, c2, 0, why)
    for k in ("_plan", "_names", "_hdr"):
        rec.pop(k)
    rec["kind"] = "inputs"
    rec.setdefault("case_variant_keys", {})
    rec["lifted"] = [num2col(c) for c in range(c1, c2 + 1)]
    if not why:
        rec["stanza"] = "\n".join([
            'duckdb.sql("""',
            f"  SELECT * FROM read_xlsx('{sql_q(book_name)}', sheet = '{sql_q(sheet)}', range = '{rect_span(r1, c1, r2, c2)}', "
            "header = false, all_varchar = true)",
            "  -- INPUTS read: typed constants that formulas read, cell-addressed (columns are sheet letters, one row per sheet row); "
            "TRY_CAST the numbers, or declare each cell as a given:",
            '""")'])
    return rec


def rect_hit(rects):
    """A test for 'is (row, col) inside any (r1, c1, r2, c2) rectangle', indexed by row so thousands of one-cell rectangles stay linear."""
    if sum(r[2] - r[0] + 1 for r in rects) > MAX_RECT_INDEX_ROWS:
        return lambda r, c: any(a <= r <= x and b <= c <= y for a, b, x, y in rects)
    by_row = defaultdict(list)
    for r1, c1, r2, c2 in rects:
        for r in range(r1, r2 + 1):
            by_row[r].append((c1, c2))
    return lambda r, c: any(a <= c <= b for a, b in by_row.get(r, ()))


def typed_inputs(sheets, regions, sources, rects_of, pivot_locs, mask_cells, book_name):
    """Typed constants that a formula reads (any ref style, directly or through a range) and that sit in a formula-dominated block,
    in a column or row of formulas, or in a lone row of constants. Text counts only when read by a cell ref from a formula column,
    so a label column read by SUMIF is not a list of inputs. Returns ({(sheet lower, row, col): annotation}, inputs source records)."""
    by_name = {sh["meta"]["name"]: sh for sh in sheets if not sh["masked"]}
    boxes = defaultdict(list)
    for g in regions:
        rows, cols = [r for r, _ in g["cells"]], [c for _, c in g["cells"]]
        if not rows:
            continue
        corners = [(r, c) for r in (min(rows), max(rows)) for c in (min(cols), max(cols))]
        for sp in g["an"].refs:
            if sp.sheet in by_name and sp.via != "GETPIVOTDATA":
                res = [sp.resolve(r, c) for r, c in corners]
                boxes[sp.sheet].append((min(x[0] for x in res), min(x[1] for x in res), max(x[2] for x in res), max(x[3] for x in res),
                                        g["id"], sp.shape == "cell"))
    data, derived = lifted_areas(sources, False), lifted_areas(sources, True)
    extras, blocks = {}, []
    for nm, bx in boxes.items():
        sd = by_name[nm]["data"]
        by_row = defaultdict(list)
        for (r, c), k in sd.consts.items():
            if k in INPUT_KINDS and (nm, r, c) not in mask_cells:
                by_row[r].append(c)
        rows = sorted(by_row)
        for r in rows:
            by_row[r].sort()
        hits = {}
        for r1, c1, r2, c2, gid, is_cell in bx:
            for r in rows[bisect.bisect_left(rows, r1):bisect.bisect_right(rows, r2)]:
                cols = by_row[r]
                for c in cols[bisect.bisect_left(cols, c1):bisect.bisect_right(cols, c2)]:
                    h = hits.setdefault((r, c), [set(), False])
                    h[0].add(gid)
                    h[1] = h[1] or is_cell
        if not hits:
            continue
        skip = [(e[2], e[3], e[4], e[5]) for e in rects_of(nm)] + [loc for loc in map(parse_loc, pivot_locs.get(nm, [])) if loc]
        occupied = defaultdict(list)
        skipped = rect_hit(skip)
        for (r, c) in list(sd.formulas) + list(sd.consts):
            if not skipped(r, c):
                occupied[r].append(c)
        hit_cols = defaultdict(list)
        for (r, c) in sorted(hits):
            hit_cols[r].append(c)
        for comp in find_components(occupied):
            mine = [(row, c) for (row, a, b) in comp["runs"] for c in hit_cols.get(row, ()) if a <= c <= b]
            if not mine:
                continue
            cellcount = sum(b - a + 1 for _, a, b in comp["runs"])
            fcount = sum(1 for (row, a, b) in comp["runs"] for c in range(a, b + 1) if (row, c) in sd.formulas)
            dominated = fcount * 2 > cellcount
            lone = comp["r1"] == comp["r2"] and fcount == 0 and cellcount >= 2
            kept = []
            for (r, c) in mine:
                if any(a <= r <= b and c in cs for a, b, cs in data.get(nm, ())):
                    continue
                ids, direct = hits[(r, c)]
                vertical = formula_line(sd, r, c, 1, 0)
                if sd.consts[(r, c)] in TEXT_KINDS:
                    where, ok = ("formula_column" if direct and vertical else None), direct and vertical
                else:
                    where = "formula_column" if vertical or formula_line(sd, r, c, 0, 1) else None
                    ok = dominated or lone or bool(where)
                if ok:
                    kept.append((r, c))
                    extras[(nm.lower(), r, c)] = {"where": where, "n": len(ids), "regions": sorted(ids, key=lambda i: (len(i), i))[:20]}
            if len(kept) >= 2:
                r1, r2 = min(r for r, _ in kept), max(r for r, _ in kept)
                c1, c2 = min(c for _, c in kept), max(c for _, c in kept)
                inside = any(a <= r1 and r2 <= b and set(range(c1, c2 + 1)) <= cs for a, b, cs in derived.get(nm, ()))
                if len(kept) == (r2 - r1 + 1) * (c2 - c1 + 1) and not inside:
                    blocks.append(inputs_block(book_name, nm, r1, c1, r2, c2))
    return extras, blocks


MAX_GIVEN_CANDIDATES = 200


def validation_entry(sheet, v):
    """{sheet, ref, type, items | source} for one data validation; inline list items capped, text that is secret-shaped dropped."""
    e = {"sheet": sheet, "ref": flat(" ".join(v["sqref"].split()))[:200], "type": v["type"]}
    f1 = v["f1"]
    if v["type"] == "list" and f1:
        if len(f1) >= 2 and f1[0] == f1[-1] == '"':
            e["items"] = [flat(x.strip()) for x in f1[1:-1].split(",") if not secret_shaped(x.strip())][:MAX_VALIDATION_ITEMS]
        else:
            e["source"] = flat(f1)[:200]
    return e


def validated_cells(sheets, reads):
    """{(sheet lower, row, col): (validation entry, formulas reading it)} for constants a list validation targets and a formula reads; visible sheets only."""
    by_sheet = defaultdict(dict)
    for (sn, r, c), n in reads.items():
        by_sheet[sn][(r, c)] = n
    out = {}
    for sh in sheets:
        nm = sh["meta"]["name"]
        sd = sh["data"]
        if sh["masked"] or not sd.validations:
            continue
        read = by_sheet.get(nm.lower(), {})
        for v in sd.validations:
            if v["type"] != "list":
                continue
            e = validation_entry(nm, v)
            for part in v["sqref"].split():
                loc = parse_loc(part)
                if not loc:
                    continue
                area = (loc[2] - loc[0] + 1) * (loc[3] - loc[1] + 1)
                cells = [(r, c) for r in range(loc[0], loc[2] + 1) for c in range(loc[1], loc[3] + 1)] if area <= 2000 else \
                        [(r, c) for (r, c) in read if loc[0] <= r <= loc[2] and loc[1] <= c <= loc[3]]
                for pos in cells:
                    if pos in read and pos in sd.consts:
                        out.setdefault((nm.lower(),) + pos, (e, read[pos]))
    return out


def given_candidates(pkg, sheets, shared_path, reads, sel_reads, pivot_locs, mask_cells, date_ids, pct_ids, date1904, extras=None, keyed=None, validated=None):
    """Constants that formulas read by absolute ref, as {sheet, ref, label, kind, value, read_by}; visible sheets only.
    `extras` {(sheet lower, row, col): {label, kind, value, forced, ...}} adds cells found another way (controls, Solver) or annotates a pick.
    `keyed` is the cells read as an IF condition, lookup value or criteria and `validated` the list-validated cells a formula reads: both are
    candidates when read once, on any sheet layout. Returns (candidates, omitted_by_cap)."""
    keyed, validated = keyed or Counter(), validated or {}
    by_lower = {sh["meta"]["name"].lower(): sh for sh in sheets}
    extras = extras or {}
    picks = defaultdict(dict)
    pivot_rects = {nm.lower(): [loc for loc in map(parse_loc, locs) if loc] for nm, locs in pivot_locs.items()}
    for (sn, r, c), n in sorted((reads + Counter({k: v for k, v in sel_reads.items() if k not in reads})).items()):
        sh = by_lower.get(sn)
        if not sh or sh["masked"] or (sh["meta"]["name"], r, c) in mask_cells or (r, c) not in sh["data"].consts:
            continue
        if any(l_[0] <= r <= l_[2] and l_[1] <= c <= l_[3] for l_ in pivot_rects.get(sn, ())):
            continue
        picks[sn][(r, c)] = [n, (sn, r, c) not in reads and n < 2 and (sn, r, c) not in keyed and (sn, r, c) not in validated, extras.get((sn, r, c))]
    for (sn, r, c), n in sorted({**keyed, **{k: v[1] for k, v in validated.items()}}.items()):
        sh = by_lower.get(sn)
        if not sh or sh["masked"] or (sh["meta"]["name"], r, c) in mask_cells or (r, c) not in sh["data"].consts or (r, c) in picks[sn]:
            continue
        if any(l_[0] <= r <= l_[2] and l_[1] <= c <= l_[3] for l_ in pivot_rects.get(sn, ())):
            continue
        picks[sn][(r, c)] = [n, False, extras.get((sn, r, c))]
    for (sn, r, c), x in sorted(extras.items()):
        sh = by_lower.get(sn)
        if not sh or sh["masked"] or (sh["meta"]["name"], r, c) in mask_cells or (r, c) in picks[sn]:
            continue
        if any(l_[0] <= r <= l_[2] and l_[1] <= c <= l_[3] for l_ in pivot_rects.get(sn, ())) or not (x.get("forced") or (r, c) in sh["data"].consts):
            continue
        picks[sn][(r, c)] = [x.get("n", reads.get((sn, r, c), 0)), False, x]
    total = sum(len(v) for v in picks.values())
    out, left = [], MAX_GIVEN_CANDIDATES
    for sh in sheets:
        nm = sh["meta"]["name"]
        todo = [(r, c, v[0], v[1], v[2]) for (r, c), v in sorted(picks.get(nm.lower(), {}).items())][:left]
        if not todo or not sh["meta"]["path"]:
            continue
        left -= len(todo)
        wanted = {(r, c) for r, c, _, _, _ in todo} | {(r, c - 1) for r, c, _, _, _ in todo if c > 1}
        wanted |= {(r + d, cc) for r, c, _, blk, _ in todo if blk for d in (-1, 1) for cc in (c, c - 1) if r + d >= 1 and cc >= 1}
        wanted |= {(r - k, c) for r, c, _, _, x in todo if x and "where" in x for k in range(1, 4) if r - k >= 1}
        wanted |= {(r, c - k) for r, c, _, _, x in todo if x and "where" in x for k in range(1, 4) if c - k >= 1}
        sd = guarded(pkg, sh["meta"]["path"], lambda: parse_sheet(pkg, nm, sh["meta"]["path"], wanted))
        if sd is None:
            continue
        texts = read_shared_strings(pkg, shared_path, {sd.sidx[p] for p in sd.sidx if p in sd.raw})

        def text_of(pos):
            k = sd.consts.get(pos)
            if (nm, pos[0], pos[1]) in mask_cells:
                return None
            t = texts.get(sd.sidx.get(pos), "") if k == "s" else sd.inline.get(pos, "") if k == "inlineStr" else sd.raw.get(pos) if k == "str" else None
            return t if t is not None and flat(t) == t and not secret_shaped(t) else None

        def labelled_const(r, c):
            return c > 1 and sd.consts.get((r, c)) in ("n", "b") and text_of((r, c - 1)) is not None

        def near_label(r, c):
            """The closest text constant to the left, else above, within three cells and before any other cell."""
            for dr, dc in ((0, -1), (-1, 0)):
                for k in range(1, 4):
                    pos = (r + dr * k, c + dc * k)
                    if pos[0] < 1 or pos[1] < 1 or (pos not in sd.consts and pos not in sd.formulas):
                        break
                    if sd.consts.get(pos) in ("s", "inlineStr"):
                        return text_of(pos)
                    if pos in sd.formulas or sd.consts.get(pos) in ("n", "b", "str", "e"):
                        break
            return None

        for r, c, n, blk, x in todo:
            if x and "n" in x:
                n = x["n"]
            if blk and not x and not (labelled_const(r, c) and (labelled_const(r - 1, c) or labelled_const(r + 1, c))):
                continue
            if x and x.get("forced"):
                out.append({"sheet": nm, "ref": f"{num2col(c)}{r}", "label": x["label"], "kind": x["kind"], "value": x["value"], "read_by": n})
                continue
            pos, k = (r, c), sd.consts.get((r, c))
            raw = sd.raw.get(pos)
            if k in ("s", "inlineStr", "str"):
                kind, value = "text", text_of(pos)
                if value is None:
                    continue
                value = value[:200]
            elif k == "b":
                kind, value = "bool", raw == "1"
            elif k == "n":
                num = _num(raw)
                style = sd.raw_style.get(pos, 0)
                if num is None or not math.isfinite(num):
                    continue
                kind, value = ("date", serial_date(num, date1904)) if style in date_ids and serial_date(num, date1904) else \
                              ("percent", num) if style in pct_ids else ("number", num)
            else:
                continue
            label = text_of((r, c - 1)) if c > 1 and sd.consts.get((r, c - 1)) in ("s", "inlineStr") else None
            if x and "where" in x and label is None:
                label = near_label(r, c)
            if x and x.get("label"):
                label = x["label"]
            cand = {"sheet": nm, "ref": f"{num2col(c)}{r}", "label": label[:80] if label else None, "kind": kind, "value": value, "read_by": n}
            if x and "where" in x:
                cand["read_by_regions"], cand["where"] = x["regions"], x["where"]
            vd = validated.get((nm.lower(), r, c))
            if vd:
                cand["kind"] = "choice"
                cand.update({"choices" if k == "items" else "choices_source": vd[0][k] for k in ("items", "source") if k in vd[0]})
            out.append(cand)
    return out, max(total - MAX_GIVEN_CANDIDATES, 0)


def header_inputs(sd, nm, regions, hrow, r2, numcols, c1, c2):
    """Numeric header cells that formulas below read by an absolute row (I$4): {cell: formulas reading it}."""
    want = {(hrow, cc) for cc in numcols if sd.numvals.get((hrow, cc)) is not None}
    hits = Counter()
    for g in regions:
        if g["sheet"] != nm:
            continue
        specs = [s for s in g["an"].refs if s.sheet == nm and s.r1a and s.r2a and s.r1 == s.r2 == hrow]
        if not specs:
            continue
        for (r, cc) in g["cells"][:50000]:
            if not (hrow < r <= r2 and c1 <= cc <= c2):
                continue
            for s in specs:
                a = s.resolve(r, cc)
                if a[0] == a[2] and a[1] == a[3] and (a[0], a[1]) in want:
                    hits[(a[0], a[1])] += 1
    return hits


def period_like(vals):
    """Header numbers that read as periods: years or date serials in a monotonic run, or 3+ consecutive integers."""
    if not vals or any(not math.isfinite(v) or abs(v) > 1e15 or v != int(v) for v in vals):
        return False
    steps = [b - a for a, b in zip(vals, vals[1:])]
    if steps and not (all(x > 0 for x in steps) or all(x < 0 for x in steps)):
        return False
    if all(1900 <= v <= 2100 for v in vals) or all(20000 <= v <= 80000 for v in vals):
        return True
    return len(vals) >= 3 and all(abs(x) == 1 for x in steps)


def plan_columns(sd, idx, ex, cols, hrow, r2, subs, calculated, reads=frozenset()):
    """Per column: lift it only when no array/spill/data-table/pivot range touches it and it is not a formula column;
    formula cells that remain inside a lifted column are listed, never silently lifted. A column holding a constant that formulas
    read by absolute ref is an input column and is lifted even when formula outputs share it."""
    out = []
    for i, c in enumerate(cols):
        rows = idx.get(c, [])
        present = [r for r in rows[bisect.bisect_left(rows, hrow + 1):bisect.bisect_right(rows, r2)] if r not in subs]
        fcells = [r for r in present if (r, c) in sd.formulas]
        touched = any(e[2] <= r2 and e[4] >= hrow + 1 and e[3] <= c <= e[5] for e in ex)
        fshare = len(fcells) / float(len(present)) if present else 0.0
        kinds = Counter("text" if sd.consts.get((r, c)) in NONNUMERIC_KINDS else "number" for r in present if (r, c) in sd.consts)
        for r in fcells:
            kinds["text" if sd.formulas[(r, c)].t in ("str", "inlineStr", "s", "e") else "number"] += 1
        if touched:
            lift, why = False, "output of an array, spill, data table or pivot"
        elif (calculated and calculated[i]) or (fshare >= 0.5 and not (fshare < 1.0 and any((r, c) in reads for r in present if (r, c) in sd.consts))):
            lift, why = False, "formula column"
        else:
            lift, why = True, None
        out.append({"col": c, "lift": lift, "why": why, "fcells": [f"{num2col(c)}{r}" for r in fcells] if lift else [],
                    "mixed": bool(kinds["text"] and kinds["number"]),
                    "nnum_const": sum(1 for r in present if (r, c) in sd.consts and sd.consts[(r, c)] not in NONNUMERIC_KINDS),
                    "ntext_const": sum(1 for r in present if sd.consts.get((r, c)) in TEXT_CELL_KINDS),
                    "text_rows": [r for r in present if sd.consts.get((r, c)) in TEXT_CELL_KINDS],
                    "text_risk": bool(kinds["text"]) and sd.consts.get((hrow + 1, c)) not in NONNUMERIC_KINDS,
                    "has_numbers": bool(kinds["number"]) or any((r, c) in sd.formulas for r in present)})
    return out


_DATE_TEXT_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
_DATE_TEXT_SLASH = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_DATE_TEXT_MON = re.compile(r"^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.? +\d{4}$", re.I)
DATE_TEXT_READERS = frozenset(["TEXT", "DATEVALUE", "MONTH", "YEAR"])


def date_text_shape(texts):
    """(format, order) when every string parses as a date, else None; a slash date never gets a guessed day/month order."""
    fmts, first_big, second_big, ambiguous = set(), False, False, False
    for t in texts:
        t = t.strip()
        m = _DATE_TEXT_ISO.match(t)
        if m:
            if not (1 <= int(m.group(2)) <= 12 and 1 <= int(m.group(3)) <= 31):
                return None
            fmts.add("yyyy-mm-dd")
            continue
        m = _DATE_TEXT_SLASH.match(t)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if not (1 <= a <= 31 and 1 <= b <= 31) or (a > 12 and b > 12):
                return None
            first_big, second_big = first_big or a > 12, second_big or b > 12
            ambiguous = ambiguous or (a <= 12 and b <= 12 and a != b)
            fmts.add("d/m/yyyy or m/d/yyyy")
            continue
        if _DATE_TEXT_MON.match(t):
            fmts.add("mmm yyyy")
            continue
        return None
    if not fmts:
        return None
    order = None
    if "d/m/yyyy or m/d/yyyy" in fmts:
        order = "conflicting" if first_big and second_big else "day-first" if first_big else "month-first" if second_big else "ambiguous"
    return (next(iter(fmts)) if len(fmts) == 1 else "mixed"), order


BUILTIN_DATE_FORMATS = frozenset([14, 15, 16, 17, 22] + list(range(27, 37)) + list(range(50, 59)))
_FMT_NOISE = re.compile(r'"[^"]*"|\[[^\]]*\]|\\.|_.|\*.')


def date_style_ids(root):
    """Indexes of cellXfs whose number format shows a calendar date."""
    if root is None:
        return set()
    custom = {}
    for el in root.iter():
        if ln(el.tag) == "numFmt":
            custom[_int(el.get("numFmtId"), -1)] = el.get("formatCode") or ""
    out = set()
    for el in root.iter():
        if ln(el.tag) != "cellXfs":
            continue
        for i, xf in enumerate(x for x in el if ln(x.tag) == "xf"):
            fid = _int(xf.get("numFmtId"), 0)
            if fid in BUILTIN_DATE_FORMATS:
                out.add(i)
            elif fid in custom and re.search(r"[dy]", _FMT_NOISE.sub("", custom[fid]).lower()):
                out.add(i)
    return out


def time_style_ids(root):
    """Indexes of cellXfs whose number format shows a time of day as well as a date."""
    if root is None:
        return set()
    custom = {}
    for el in root.iter():
        if ln(el.tag) == "numFmt":
            custom[_int(el.get("numFmtId"), -1)] = el.get("formatCode") or ""
    out = set()
    for el in root.iter():
        if ln(el.tag) != "cellXfs":
            continue
        for i, xf in enumerate(x for x in el if ln(x.tag) == "xf"):
            fid = _int(xf.get("numFmtId"), 0)
            if fid == 22 or (fid in custom and re.search(r"[hs]|am/pm", _FMT_NOISE.sub("", custom[fid]).lower())):
                out.add(i)
    return out


def date_ids_of(pkg, wb_rels):
    styles = next((r[1] for r in wb_rels.values() if r[0] == "styles" and r[1]), None) or "xl/styles.xml"
    return date_style_ids(pkg.xml(styles) if pkg.has(styles) else None)


def date_system_info(pkg, wb_rels, sheets, date1904, dates):
    """Date-styled constants in the 1900 system that sit on the phantom 1900-02-29 or before it."""
    info = {"date1904": bool(date1904), "serial_60": [], "pre_61_dates": 0}
    if date1904:
        return info
    for sh in sheets:
        if sh["masked"]:
            continue
        for pos, (num, style) in sorted(sh["data"].low_serials.items()):
            if style not in dates:
                continue
            info["pre_61_dates"] += 1
            if 60 <= num < 61 and len(info["serial_60"]) < 50:
                info["serial_60"].append(f"{sh['meta']['name']}!{num2col(pos[1])}{pos[0]}")
    return info


def merge_top_left(sd, r, c, first_row):
    """The top-left cell of the merged range holding (r, c), when that range starts at or below first_row."""
    for m in sd.merges:
        if not m:
            continue
        p = m.split(":")
        a, b = parse_ref_a1(p[0]), parse_ref_a1(p[-1])
        if a and b and a[0] <= r <= b[0] and a[1] <= c <= b[1] and a[0] >= first_row and (a[0], a[1]) != (r, c):
            return (a[0], a[1])
    return None


def formula_layout(sd, comp, c1, c2, r1, r2):
    """A formula-dominated block with a label column and period-like numbers across its first row: wide, but never lifted."""
    texty = ("s", "inlineStr", "str")
    nums = [(c, sd.numvals.get((r1, c))) for c in range(c1 + 1, c2 + 1) if sd.consts.get((r1, c)) == "n"]
    if len(nums) < 2 or any(v is None for _, v in nums) or not period_like([v for _, v in nums]):
        return None
    if not any(sd.consts.get((r, c1)) in texty for r in range(r1 + 1, r2 + 1)):
        return None
    cols = [c for c in range(c1 + 1, c2 + 1) if (r1, c) in sd.consts or (r1, c) in sd.formulas]
    return {"layout": "wide", "period_columns": len(cols), "label_column": num2col(c1),
            "header_ref": rect_a1(r1, min(cols), r1, max(cols)), "ref": rect_a1(r1, c1, r2, c2)}


MAX_KEY_FRAGMENTS = 6


def constant_rects(cells):
    """Greedy cover of constant cells by rectangles holding nothing else (an L of label column plus header row is two); singles are dropped."""
    left, out = set(cells), []
    while left:
        r1, c1 = min(left)
        c2 = c1
        while (r1, c2 + 1) in left:
            c2 += 1
        r2 = r1
        while all((r2 + 1, c) in left for c in range(c1, c2 + 1)):
            r2 += 1
        left -= {(r, c) for r in range(r1, r2 + 1) for c in range(c1, c2 + 1)}
        if (r2 - r1 + 1) * (c2 - c1 + 1) >= 2:
            out.append({"r1": r1, "r2": r2, "c1": c1, "c2": c2, "runs": [(r, c1, c2) for r in range(r1, r2 + 1)]})
    return out


def refused_block(sheet, r1, c1, r2, c2, fcount, why):
    """A source record for a block that gets no stanza, so the reason is in the report instead of the block being absent."""
    return {"sheet": sheet, "kind": "range", "name": None, "derived": False, "lifted_columns": [], "ref": rect_a1(r1, c1, r2, c2), "data_ref": rect_span(r1, c1, r2, c2),
            "header_row": r1, "first_data_row": r1, "header": False, "header_rows": 1, "layout": "long", "wide_ambiguous": False,
            "period_columns": 0, "lifted": [], "not_lifted": [], "not_lifted_reasons": {}, "formula_cells_in_lifted": [],
            "excluded_overlaps": [], "subtotal_rows": [], "excluded_rows": [], "inputs_in_header": [], "trimmed_rows": [], "hidden_rows": [],
            "stanza": None, "stanza_refused": why, "formula_cells": fcount, "_plan": [], "_names": None, "_hdr": (r1, c1, c2)}


def build_sources(sheets, regions, cell_region, rects_of, pivot_locs, pkg, shared_path, bookname, mask_cells=frozenset(), layouts=None, date_ids=frozenset(),
                  time_ids=frozenset(), date1904=False, reads=None):
    sources, pending_headers = [], []
    reads_by_sheet = defaultdict(set)
    for (rs, rr, rc) in reads or ():
        reads_by_sheet[rs].add((rr, rc))

    case_budget = [MAX_CASE_SCAN_ROWS]

    def collect_case(src, sd, masked, first, last, c1, c2):
        """Text cells of a long source's data rows (shared-string indexes or inline text), within the scan caps; the lowercase check runs after the strings are read."""
        if masked or src["layout"] != "long" or c2 - c1 + 1 > MAX_CASE_COLS or last < first:
            return
        cells = [(pos, k) for pos, k in sd.consts.items() if k in ("s", "inlineStr") and first <= pos[0] <= last and c1 <= pos[1] <= c2]
        if len(cells) > case_budget[0]:
            return
        case_budget[0] -= len(cells)
        got = defaultdict(list)
        for (r, c), k in cells:
            if k == "s" and (r, c) in sd.sidx:
                got[c].append(sd.sidx[(r, c)])
                pending_headers.append(sd.sidx[(r, c)])
            elif k == "inlineStr":
                got[c].append(sd.inline.get((r, c), ""))
        src["_case"] = got

    subtotal_cells = defaultdict(dict)
    for g in regions:
        an = g["an"]
        if set(an.funcs) & {"SUM", "SUBTOTAL", "AGGREGATE"}:
            for s in an.refs:
                if s.sheet != g["sheet"] or s.shape not in ("range",) or s.c1 != s.c2:
                    continue
                for (r, c) in g["cells"]:
                    r1, c1, r2, c2 = s.resolve(r, c)
                    if c1 == c and r2 == r - 1:
                        subtotal_cells[g["sheet"]][(r, c)] = r1

    for sh in sheets:
        sd, nm = sh["data"], sh["meta"]["name"]
        masked = sh["masked"]
        sheet_reads = reads_by_sheet.get(nm.lower(), frozenset())
        ex = rects_of(nm)
        exrects = [(e[2], e[3], e[4], e[5]) for e in ex]
        idx = defaultdict(list)
        for (r, c) in sd.formulas:
            idx[c].append(r)
        for (r, c) in sd.consts:
            idx[c].append(r)
        for rows in idx.values():
            rows.sort()
        sheet_subtotals = subtotal_cells.get(nm, {})
        table_rects = []

        def overlaps(r1, c1, r2, c2):
            return [{"kind": e[0], "ref": e[1]} for e in ex if not (e[4] < r1 or e[2] > r2 or e[5] < c1 or e[3] > c2)]

        for info, tcols, _ in sh["tables"]:
            table_rects.append((info.r1, info.c1, info.r2, info.c2))
            # a malformed table can declare fewer tableColumns than its ref is wide
            tcols = list(tcols) + [{"name": "", "calculated": False}] * (info.c2 - info.c1 + 1 - len(tcols))
            hrow = info.r1 + info.header_rows - 1
            last = info.r2 - info.totals_rows
            plan = plan_columns(sd, idx, ex, range(info.c1, info.c2 + 1), hrow, last, set(), [c["calculated"] for c in tcols], sheet_reads)
            hidden = sorted(r for r in sd.hidden_rows if info.r1 <= r <= info.r2)
            src = {"sheet": nm, "kind": "table", "derived": False, "lifted_columns": [], "name": info.name, "ref": rect_a1(info.r1, info.c1, info.r2, info.c2),
                   "data_ref": rect_span(info.r1, info.c1, last, info.c2), "header_row": hrow, "first_data_row": hrow + 1, "header": info.header_rows > 0, "header_rows": info.header_rows,
                   "layout": "long", "wide_ambiguous": False, "period_columns": 0, "lifted": [], "not_lifted": [], "not_lifted_reasons": {},
                   "formula_cells_in_lifted": [], "inputs_in_header": [], "excluded_overlaps": overlaps(info.r1, info.c1, info.r2, info.c2),
                   "subtotal_rows": [], "excluded_rows": [], "trimmed_rows": [], "hidden_rows": hidden, "stanza": None, "stanza_refused": None,
                   "formula_cells": sum(1 for (r, c) in sd.formulas if info.r1 <= r <= info.r2 and info.c1 <= c <= info.c2),
                   "_plan": plan, "_names": [safe_ident("" if secret_shaped(c["name"]) or (nm, hrow, info.c1 + i) in mask_cells else c["name"],
                                                        f"column{i + 1}") if info.header_rows > 0 else num2col(info.c1 + i)
                              for i, c in enumerate(tcols)],
                   "_hdr": (hrow, info.c1, info.c2), "_dropped_names": None}
            sources.append(src)
            collect_case(src, sd, masked, hrow + 1, last, info.c1, info.c2)
            if not masked and last - hrow <= 2000:
                for (rr, cc), kind in sd.consts.items():
                    if kind == "s" and hrow < rr <= last and info.c1 <= cc <= info.c2 and (rr, cc) in sd.sidx:
                        pending_headers.append(sd.sidx[(rr, cc)])
        skip = exrects + table_rects + [tuple(parse_loc(loc)) for loc in pivot_locs.get(nm, []) if parse_loc(loc)]
        by_row = defaultdict(list)
        skipped = rect_hit(skip)
        for (r, c) in list(sd.formulas) + list(sd.consts):
            if not skipped(r, c):
                by_row[r].append(c)
        if not by_row:
            continue
        states = []
        work = []
        for comp in find_components(by_row):
            r1, r2, c1, c2 = comp["r1"], comp["r2"], comp["c1"], comp["c2"]
            if r2 - r1 < 1:
                continue
            cellcount = sum(b - a + 1 for _, a, b in comp["runs"])
            fcount = sum(1 for (row, a, b) in comp["runs"] for c in range(a, b + 1) if (row, c) in sd.formulas)
            if fcount * 2 > cellcount or cellcount < 2:
                if fcount * 2 <= cellcount:
                    continue
                if layouts is not None and not masked:
                    lay = formula_layout(sd, comp, c1, c2, r1, r2)
                    if lay:
                        lay.update({"sheet": nm, "lifted": False, "reason": "formula-derived"})
                        layouts.append(lay)
                # a formula's output is never a source, but the constants around it (keys, period headers) are
                frags = constant_rects({(row, c) for (row, a, b) in comp["runs"] for c in range(a, b + 1) if (row, c) in sd.consts})
                if not frags or len(frags) > MAX_KEY_FRAGMENTS:
                    if not masked:
                        sources.append(refused_block(nm, r1, c1, r2, c2, fcount,
                                                     "formula-dominated block" + (" with its constants scattered over too many fragments to lift" if frags else " with no constant key column or header row")
                                                     + ": its cells are translated output, not a source; if a range is missing from the printed stanzas, report a skill gap, do not widen one by hand"))
                    continue
                work.extend((k, True) for k in frags)
                continue
            work.append((comp, False))
        for comp, derived in work:
            r1, r2, c1, c2 = comp["r1"], comp["r2"], comp["c1"], comp["c2"]
            fcount = sum(1 for (row, a, b) in comp["runs"] for c in range(a, b + 1) if (row, c) in sd.formulas)
            # a total row is evidenced only by an aggregate in this block's own columns that sums rows of this block above it
            evidence = defaultdict(list)
            if not derived:
                for (rr, cc), start in sheet_subtotals.items():
                    if r1 <= start and r1 < rr <= r2 and c1 <= cc <= c2:
                        evidence[rr].append(f"{num2col(cc)}{rr}")
            trimmed = []
            while not derived and r2 - 1 > r1 and r2 in evidence:
                trimmed.append(r2)
                r2 -= 1
            header_rows = 1
            sub = [sd.consts.get((r1 + 1, cc)) for cc in range(c1, c2 + 1) if (r1 + 1, cc) in sd.consts or (r1 + 1, cc) in sd.formulas]
            # a merged cell over data rows is a one-row header: the row below must itself be all text to be a sub-header
            if not derived and any(m and ":" in m and _merge_hits_row(m, r1, c1, c2) for m in sd.merges) and r2 - r1 >= 2 \
                    and sub and all(k in ("s", "inlineStr", "str") for k in sub):
                header_rows = 2
            hrow = r1 + header_rows - 1
            texty = ("s", "inlineStr", "str")
            rowk = {c: (sd.consts.get((hrow, c)) or ("f" if (hrow, c) in sd.formulas else None)) for c in range(c1, c2 + 1)}
            present = [k for k in rowk.values() if k]
            header = bool(present) and all(k in texty for k in present) and not derived
            frt = ""
            if not header and not derived and not masked and present:
                strf = {c for c, k in rowk.items() if k == "f" and sd.formulas[(hrow, c)].t == "str"}
                texts = sum(1 for c, k in rowk.items() if k in texty or c in strf)
                if texts == len(present) and strf:
                    # a computed-text header cell hides the row's type; numeric columns below tell it from data
                    below = sum(1 for c in rowk if rowk[c] and sd.consts.get((hrow + 1, c)) == "n")
                    if below * 2 >= len(present):
                        header = True
                    else:
                        frt = "all"
                elif len(present) >= 3 and texts * 2 > len(present):
                    frt = "mostly"
            subs = sorted(r for r in evidence if r <= r2 + len(trimmed))
            via = {r: sorted(evidence[r])[0] for r in subs}
            inputs = None
            numcols = [cc for cc, k in rowk.items() if k == "n" and (nm, hrow, cc) not in mask_cells]
            if not derived and not masked and not header and numcols and rowk.get(c1) in texty and sum(1 for k in rowk.values() if k in texty) >= 2 and r2 - hrow >= 1:
                def fshare(cc):
                    cells = [(rr, cc) for rr in range(hrow + 1, r2 + 1) if (rr, cc) in sd.formulas or (rr, cc) in sd.consts]
                    return sum(1 for pos in cells if pos in sd.formulas) / float(len(cells)) if cells else 0.0
                if all(fshare(cc) >= 0.5 for cc in numcols):
                    inputs = header_inputs(sd, nm, regions, hrow, r2, numcols, c1, c2) or None
            states.append({"derived": derived, "inputs": inputs, "r1": r1, "r2": r2, "c1": c1, "c2": c2, "fcount": fcount, "trimmed": trimmed, "header_rows": header_rows,
                           "hrow": hrow, "header": header, "subs": subs, "via": via, "frt": frt,
                           "cand": (not derived) and (not header) and rowk.get(c1) in texty and sum(1 for k in rowk.values() if k in ("n", "f")) >= 2
                                   and r2 - r1 >= 1,
                           "numcells": [(hrow, c) for c, k in rowk.items() if k == "n"],
                           "textn": sum(1 for k in rowk.values() if k in texty),
                           "repeats_below": any(cell_region.get((nm, hrow, cc)) is not None and
                                                cell_region.get((nm, hrow, cc)) is cell_region.get((nm, hrow + 1, cc))
                                                for cc in range(c1, c2 + 1) if (hrow, cc) in sd.formulas),
                           "kinds_next": all(sd.consts.get((hrow + 1, cc)) in (None, rowk[cc]) or (hrow + 1, cc) in sd.formulas
                                             for cc in rowk if rowk[cc] in texty or rowk[cc] == "n"),
                           "nperiod": sum(1 for k in rowk.values() if k in ("n", "f"))})
        for st in states:
            r1, r2, c1, c2, trimmed, hrow = st["r1"], st["r2"], st["c1"], st["c2"], st["trimmed"], st["hrow"]
            header, header_rows, subs = st["header"], st["header_rows"], st["subs"]
            layout, period_cols, ambiguous = "long", 0, False
            if st["cand"]:
                vals = [sd.numvals.get(pos) for pos in st["numcells"]]
                product_row = (len(vals) < 2 and st["textn"] >= 2) or st["repeats_below"]
                period_text = any(sd.consts.get((rr, pc)) in ("s", "inlineStr", "str") for (_, pc) in st["numcells"] for rr in range(hrow + 1, r2 + 1))
                if vals and None not in vals and period_like(vals) and not product_row and not period_text:
                    layout, header, period_cols = "wide", True, st["nperiod"]
                elif not (product_row and st["kinds_next"]):
                    ambiguous = True
            if st["inputs"] and layout == "long":
                header, ambiguous = True, False
            inrange = [r for r in subs if r not in trimmed]
            plan = plan_columns(sd, idx, ex, range(c1, c2 + 1), hrow if header else hrow - 1, r2, set(subs), None, sheet_reads)
            src = {"sheet": nm, "kind": "range", "name": None, "derived": st["derived"], "lifted_columns": [], "ref": rect_a1(r1, c1, r2 + len(trimmed), c2),
                   "data_ref": rect_span(hrow, c1, r2, c2), "header_row": hrow, "first_data_row": hrow + 1 if header else hrow, "header": header,
                   "header_rows": header_rows, "layout": layout,
                   "wide_ambiguous": ambiguous,
                   "period_columns": period_cols, "lifted": [], "not_lifted": [], "not_lifted_reasons": {},
                   "formula_cells_in_lifted": [], "excluded_overlaps": overlaps(hrow, c1, r2, c2), "subtotal_rows": subs,
                   "excluded_rows": [{"row": r, "via": st["via"][r]} for r in subs],
                   "inputs_in_header": [{"cell": f"{num2col(cc)}{rr}", "value": sd.numvals[(rr, cc)], "read_by": n,
                                         "suggested_given": f"given: input_{num2col(cc).lower()}{rr} :: number is {sd.numvals[(rr, cc)]:g}"}
                                        for (rr, cc), n in sorted((st["inputs"] or {}).items(), key=lambda kv: kv[0][1])],
                   "trimmed_rows": sorted(trimmed), "hidden_rows": sorted(r for r in sd.hidden_rows if hrow <= r <= r2),
                   "first_row_text": bool(st["frt"]) and not header and not ambiguous and not st["inputs"],
                   "first_row_text_kind": st["frt"] if (st["frt"] and not header and not ambiguous and not st["inputs"]) else "",
                   "stanza": None, "stanza_refused": None, "formula_cells": st["fcount"], "_plan": plan, "_names": None,
                   "_hdr": (hrow, c1, c2), "_inrange": inrange}
            sources.append(src)
            collect_case(src, sd, masked, src["first_data_row"], r2, c1, c2)
            if not masked and header:
                for c in range(c1, c2 + 1):
                    if sd.consts.get((hrow, c)) == "s" and (hrow, c) in sd.sidx:
                        pending_headers.append(sd.sidx[(hrow, c)])
                    elif header_rows > 1 and (hrow, c) not in sd.consts:
                        tl = merge_top_left(sd, hrow, c, hrow - header_rows + 1)
                        if tl and sd.consts.get(tl) == "s" and tl in sd.sidx:
                            pending_headers.append(sd.sidx[tl])
            st_d0 = src["first_data_row"]
            if not masked and inrange and layout == "long" and r2 - hrow <= 2000:
                for (rr, cc), kind in sd.consts.items():
                    if kind == "s" and st_d0 <= rr <= r2 and c1 <= cc <= c2 and (rr, cc) in sd.sidx:
                        pending_headers.append(sd.sidx[(rr, cc)])
    strings = read_shared_strings(pkg, shared_path, set(pending_headers)) if pending_headers else {}
    by_sheet = {sh["meta"]["name"]: sh for sh in sheets}
    masked_at = defaultdict(list)
    for (sn_, r_, c_) in mask_cells:
        masked_at[(sn_, c_)].append(r_)
    for src in sources:
        sh = by_sheet[src["sheet"]]
        sd = sh["data"]
        sd_reads = reads_by_sheet.get(src["sheet"].lower(), frozenset())
        hrow, c1, c2 = src.pop("_hdr")
        plan = src.pop("_plan")
        names = src.pop("_names")
        inrange = src.pop("_inrange", [])
        case = src.pop("_case", None)
        if sh["masked"] or src["stanza_refused"]:
            continue
        raw = {}
        indirect = set()
        r_last = (parse_loc(src["data_ref"]) or (0, 0, hrow, 0))[2]
        d0 = src["first_data_row"]
        if names is None:
            names = []
            for ci in plan:
                c = ci["col"]
                kind = sd.consts.get((hrow, c))
                tl = merge_top_left(sd, hrow, c, hrow - src["header_rows"] + 1) if src["header"] and kind is None and src["header_rows"] > 1 else None
                if src["header"] and (src["sheet"], hrow, c) in mask_cells:
                    raw[c] = ""
                elif src["header"] and kind == "s":
                    raw[c] = strings.get(sd.sidx.get((hrow, c)), "")
                elif src["header"] and kind == "inlineStr":
                    raw[c] = sd.inline.get((hrow, c), "")
                elif tl and (src["sheet"], tl[0], tl[1]) not in mask_cells and sd.consts.get(tl) in ("s", "inlineStr"):
                    indirect.add(c)
                    raw[c] = strings.get(sd.sidx.get(tl), "") if sd.consts.get(tl) == "s" else sd.inline.get(tl, "")
                elif src["header"]:
                    raw[c] = None
                else:
                    raw[c] = num2col(c)
        else:
            raw = {ci["col"]: n for ci, n in zip(plan, names)}
        seen, ok = set(), {}
        for ci in plan:
            r_ = raw.get(ci["col"])
            if r_ and secret_shaped(r_):
                r_ = raw[ci["col"]] = ""
            good = bool(r_ and r_.strip()) and flat(r_) == r_ and r_ == r_.strip() and '"' not in r_ and "%{" not in r_ and r_.lower() not in seen
            ok[ci["col"]] = good
            if r_:
                seen.add(r_.lower())
        names = [raw[ci["col"]] if ok[ci["col"]] else f"column{ci['col'] - c1 + 1}" for ci in plan]
        wide = src["layout"] == "wide"
        label_idx = [i for i, ci in enumerate(plan) if sd.consts.get((hrow, ci["col"])) in ("s", "inlineStr", "str")] if wide else None
        lifted, dropped, bad_lifted = [], [], False
        for i, (n, ci) in enumerate(zip(names, plan)):
            if wide and i not in label_idx:
                if not ci["lift"]:
                    dropped.append(None)
                    src["not_lifted_reasons"][f"column {num2col(ci['col'])}"] = ci["why"]
                    src["not_lifted"].append(f"column {num2col(ci['col'])}")
                else:
                    src["formula_cells_in_lifted"].extend(ci["fcells"])
                continue
            if ci["lift"] and not wide and len(ci["fcells"]) > MAX_NULLED_ROWS:
                ci["lift"], ci["why"] = False, f"{len(ci['fcells'])} formula cells among constants: too many to exclude by row"
            if ci["lift"]:
                lifted.append(n)
                bad_lifted = bad_lifted or not ok[ci["col"]]
                src["formula_cells_in_lifted"].extend(ci["fcells"])
            else:
                dropped.append(n)
                src["not_lifted"].append(n)
                src["not_lifted_reasons"][n] = ci["why"]
        src["lifted"] = lifted
        src["lifted_columns"] = [num2col(ci["col"]) for ci in plan if ci["lift"]]
        mixed = [n for n, ci in zip(names, plan) if ci["mixed"] and ci["lift"]]
        if case is not None and not wide:
            for n, ci in zip(names, plan):
                vals = [strings.get(v) if isinstance(v, int) else v for v in case.get(ci["col"], ())]
                if ci["lift"] and vals and ci["ntext_const"] >= ci["nnum_const"]:
                    spelled = defaultdict(set)
                    for v in vals:
                        if v is not None:
                            spelled[v.lower()].add(v)
                    src.setdefault("case_variant_keys", {})[n] = sum(1 for sp in spelled.values() if len(sp) > 1)
                    src.setdefault("_spelled", {})[n] = spelled
        alias = None
        rng = src["data_ref"]
        if wide and (bad_lifted or any(not ok[plan[i]["col"]] for i in label_idx)):
            src["stanza_refused"] = "a header cannot be quoted safely in the UNPIVOT (quotes, blank or duplicate): rename it in the workbook"
            continue
        if src["header"] and not wide and (bad_lifted or src["inputs_in_header"] or any(ci["col"] in indirect and ci["lift"] for ci in plan)):
            alias = [(num2col(ci["col"]), n) for n, ci in zip(names, plan) if ci["lift"]]
            loc = parse_loc(src["data_ref"])
            rng = rect_span(hrow + 1, c1, loc[2] if loc else hrow + 1, c2)
        masked_rows = set()

        def with_masked(cc, rows):
            """Rows of a lifted column that hold a secret-masked cell join its NULLed rows."""
            hit = [r_ for r_ in masked_at.get((src["sheet"], cc), ()) if d0 <= r_ <= r_last]
            masked_rows.update(hit)
            return sorted(set(rows) | set(hit))

        wide_cols, wide_first = None, None
        if wide:
            parts_, nameable, unlifted = [], True, False
            for i, (n, ci) in enumerate(zip(names, plan)):
                cc = ci["col"]
                if i in label_idx:
                    if ci["lift"]:
                        parts_.append({"src": num2col(cc), "out": n, "role": "label",
                                       "null_rows": with_masked(cc, [int(re.sub(r"\D", "", f)) for f in ci["fcells"]])})
                    continue
                if not ci["lift"]:
                    unlifted = True
                    continue
                v = sd.numvals.get((hrow, cc)) if sd.consts.get((hrow, cc)) == "n" and (src["sheet"], hrow, cc) not in mask_cells else None
                pname = str(int(v)) if v is not None and math.isfinite(v) and abs(v) < 1e15 and v == int(v) else None
                nameable = nameable and pname is not None
                parts_.append({"src": num2col(cc), "out": pname, "role": "period",
                               "null_rows": with_masked(cc, [int(re.sub(r"\D", "", f)) for f in ci["fcells"]])})
            outs = [w["out"].lower() for w in parts_ if w["out"]]
            if nameable and len(outs) == len(set(outs)) and any(w["role"] == "period" for w in parts_):
                wide_cols, wide_first = parts_, hrow + 1
                rng = rect_span(hrow + 1, c1, r_last, c2)
            elif unlifted:
                src["stanza_refused"] = ("a formula column sits inside this wide block and a period header cannot be named safely, so the "
                                         "UNPIVOT cannot leave the column out: lift the periods by hand")
                continue
            elif masked_rows:
                src["stanza_refused"] = "a cell in this wide block is masked as a secret and this layout cannot NULL it by row: lift the block by hand"
                continue
        src["data_ref"] = rng
        # what each lifted column is, and which cells a read of it would get wrong
        text_cells, cols, varchar, coerced = [], None, False, False
        date_text = []
        low_serial = False
        if not wide:
            subset = set(inrange)
            date_cols = {}
            for ci in plan:
                if not ci["lift"] or not ci["nnum_const"]:
                    continue
                sty = sd.col_styles.get(ci["col"], Counter())
                nn = sum(1 for (rr, c_), k in sd.consts.items() if c_ == ci["col"] and k == "n" and d0 <= rr <= r_last and rr not in subset)
                if nn and sum(v for s_, v in sty.items() if s_ in date_ids) * 2 >= nn:
                    date_cols[ci["col"]] = any(s_ in time_ids and v for s_, v in sty.items())
            low_serial = bool(not date1904 and any(c_ in date_cols and s_ in date_ids for (_r, c_), (_n, s_) in sd.low_serials.items()))
            varchar = any(ci["mixed"] or ci["text_risk"] for ci in plan) or (date1904 and bool(date_cols)) or low_serial
            first_row = (parse_loc(rng) or (hrow + 1,))[0] + (1 if src["header"] and not alias else 0)
            cols = []
            used = {n.lower() for n in names}
            for n, ci in zip(names, plan):
                if not ci["lift"]:
                    continue
                cc = ci["col"]
                srcname = num2col(cc) if alias or not src["header"] else n
                fnull = with_masked(cc, [int(re.sub(r"\D", "", f)) for f in ci["fcells"]])
                trows = [r_ for r_ in ci["text_rows"] if r_ not in subset and d0 <= r_ <= r_last]
                if not ci["nnum_const"] and len(trows) >= 2:
                    texts = [strings.get(sd.sidx.get((r_, cc)), "") if sd.consts.get((r_, cc)) == "s" else sd.inline.get((r_, cc), "")
                             if sd.consts.get((r_, cc)) == "inlineStr" else None for r_ in trows]
                    shape = date_text_shape(texts) if all(texts) and len(trows) == ci["ntext_const"] else None
                    if shape:
                        date_text.append({"column": n, "ref": rect_a1(min(trows), cc, max(trows), cc), "cells": len(trows),
                                          "format": shape[0], "order": shape[1]})
                input_text = any((rr_, cc) in sd_reads for rr_ in trows)
                numeric_dom = ci["nnum_const"] > 0 and ci["nnum_const"] >= ci["ntext_const"] and not input_text
                kind = "text"
                if varchar and ci["nnum_const"]:
                    typed = ("datetime" if date_cols[cc] else "date") if cc in date_cols else "number"
                    if numeric_dom:
                        kind = typed
                        nulls = fnull
                        if trows:
                            for r_ in trows:
                                text_cells.append({"cell": f"{num2col(cc)}{r_}", "column": n, "in": "date" if cc in date_cols else "numeric"})
                            if len(set(fnull) | set(trows)) <= MAX_NULLED_ROWS:
                                nulls = sorted(set(fnull) | set(trows))
                            else:
                                coerced = True
                        cols.append({"src": srcname, "out": n, "kind": kind, "null_rows": nulls})
                        continue
                    sib = f"{n}_{'date' if cc in date_cols else 'number'}"
                    while sib.lower() in used:
                        sib += "_"
                    used.add(sib.lower())
                    cols.append({"src": srcname, "out": n, "kind": "text", "null_rows": fnull})
                    cols.append({"src": srcname, "out": sib, "kind": typed,
                                 "null_rows": sorted(set(fnull) | set(trows)) if len(set(fnull) | set(trows)) <= MAX_NULLED_ROWS else fnull,
                                 "sibling": True})
                    continue
                cols.append({"src": srcname, "out": n, "kind": "text", "null_rows": fnull})
        src["date_as_text"] = date_text
        src["text_cell_count"] = len(text_cells)
        src["text_cells"] = text_cells[:10]
        pred = None
        if inrange and not wide and r_last - hrow <= 2000:
            lifted_cols = {ci["col"]: n for n, ci in zip(names, plan) if ci["lift"]}
            for cc, n in lifted_cols.items():
                def label(rr):
                    k = sd.consts.get((rr, cc))
                    if k == "s":
                        return strings.get(sd.sidx.get((rr, cc)), "").lower()
                    return sd.inline.get((rr, cc), "").lower() if k == "inlineStr" else None
                subs_l = [label(rr) for rr in inrange]
                if any(not x for x in subs_l):
                    continue
                others = [label(rr) for rr in range(d0, r_last + 1) if rr not in set(inrange)]
                for kw in ("subtotal", "total"):
                    if all(kw in x for x in subs_l) and not any(o and kw in o for o in others):
                        pred = ((num2col(cc) if alias or not src["header"] else n), kw)
                        break
                if pred:
                    break
        out_names = [w["out"] for w in wide_cols if w["role"] == "label"] if wide_cols else (lifted if wide else [cc["out"] for cc in cols])
        src["reserved_columns"] = reserved_columns(out_names)
        stanza, why = render_stanza(bookname, src["sheet"], {
            "masked_rows": sorted(masked_rows), "reserved": src["reserved_columns"], "first_row_text": src.get("first_row_text_kind", ""), "subtotal_pred": pred, "date_text": date_text, "text_cells": src["text_cells"], "text_count": src["text_cell_count"], "bad_header": bad_lifted,
            "rng": rng, "header": src["header"], "layout": src["layout"], "lifted": lifted, "dropped": dropped if wide else [],
            "not_lifted": src["not_lifted"], "subtotal_rows": inrange, "excluded_rows": src["excluded_rows"], "trimmed": src["trimmed_rows"], "hidden": src["hidden_rows"],
            "mixed": mixed, "header_rows": src["header_rows"], "flagged": src["formula_cells_in_lifted"], "alias": alias,
            "ambiguous": src["wide_ambiguous"], "cols": cols, "varchar": varchar, "first_row": first_row if not wide else wide_first,
            "inputs": src["inputs_in_header"], "wide_cols": wide_cols, "date1904": date1904, "low_serial": low_serial, "coerced": coerced,
            "has_date_cols": bool(cols and any(cc["kind"] in ("date", "datetime") for cc in cols))})
        src["stanza"], src["stanza_refused"] = stanza, why
    for src in sources:
        src.setdefault("reserved_columns", [])
        src.setdefault("first_row_text", False)
        src.setdefault("first_row_text_kind", "")
        src.setdefault("case_variant_keys", {})
    return sources


def guarded(pkg, part, fn, default=None):
    """A part that fails to parse degrades into the not-read list; it never aborts the report."""
    try:
        return fn()
    except Rejected as e:
        pkg.rejected.append({"part": part, "reason": e.reason})
    except Exception:
        pkg.rejected.append({"part": part, "reason": "parse_error"})
    return default


def analyze(path, secret_cells=()):
    specs = parse_secret_cells(secret_cells)
    name = os.path.basename(path)
    rep = {"workbook": name, "status": "ok", "error": None, "security": [], "masking_note": MASKING_NOTE,
           "package": {"entries": 0, "parts_read": 0, "bytes_read": 0, "rejected": [], "unsafe_names": 0}}
    try:
        pkg = open_package(path)
    except PackageError as e:
        rep["status"] = "unreadable"
        rep["error"] = {"kind": e.kind, "message": e.message}
        return rep
    try:
        try:
            return _analyze(pkg, rep, name, specs)
        except CliError:
            raise
        except Exception as e:
            # an unscrubbed partial report must not escape: keep only the fields that never carry workbook text
            rep = {k: rep[k] for k in ("workbook", "masking_note") if k in rep}
            rep["status"] = "unreadable"
            rep["error"] = {"kind": "internal", "message": f"analysis failed unexpectedly ({type(e).__name__}); "
                                                           "the package-level security flags are still reported"}
            rep["package"] = {"entries": pkg.entries, "parts_read": pkg.parts_read, "bytes_read": pkg.bytes_read,
                              "rejected": list(pkg.rejected), "unsafe_names": pkg.unsafe}
            rep["security"] = guarded(pkg, "(security)", lambda: build_security(rep, {}, pkg, [], {}, []), [])
            return rep
    finally:
        pkg.zf.close()


def _analyze(pkg, rep, bookname, specs=()):
    def finish_package():
        rep["package"] = {"entries": pkg.entries, "parts_read": pkg.parts_read, "bytes_read": pkg.bytes_read,
                          "rejected": list(pkg.rejected), "unsafe_names": pkg.unsafe}

    root, wb_path = read_workbook(pkg)
    if root is None:
        finish_package()
        rep["status"] = "unreadable"
        rep["error"] = {"kind": "no_workbook", "message": f"{wb_path} could not be read (see package.rejected)"}
        rep["security"] = build_security(rep, {}, pkg, [], {}, [])
        return rep
    wb_rels = pkg.rels(wb_path)
    pkg.wb_rel_targets = [(typ, tgt) for typ, tgt, ext_ in wb_rels.values() if tgt and not ext_]
    book = parse_workbook(root, wb_rels)

    seen_names = set()
    for s in book.sheets:
        if s["name"].lower() in seen_names:
            finish_package()
            rep["status"] = "unreadable"
            rep["error"] = {"kind": "duplicate_sheet_names", "message": "two sheets share a name (case-insensitively): sheet visibility "
                                                                         "cannot be attributed safely, so nothing is reported"}
            rep["security"] = build_security(rep, {}, pkg, [], {}, [])
            return rep
        seen_names.add(s["name"].lower())

    ctx = Ctx()
    ctx.has_vba = bool(pkg.code_parts("xl/vbaProject.bin", "vbaproject", "vbaproject"))
    for s in book.sheets:
        ctx.sheet_names.append(s["name"])
        ctx.sheet_lookup[s["name"].lower()] = s["name"]

    # ---- sheets + their rels ---------------------------------------------------
    sheets = []
    want = want_positions(specs)
    for s in book.sheets:
        config = is_config_sheet(s["name"])
        masked = s["state"] in ("hidden", "veryHidden") or config
        sd = guarded(pkg, s["path"], lambda: parse_sheet(pkg, s["name"], s["path"], want.get(s["name"].lower(), ()))) if s["path"] else None
        if sd is None:
            sd = SheetData(s["name"])
        srels = pkg.rels(s["path"]) if s["path"] else {}
        sheets.append({"meta": s, "data": sd, "rels": srels, "masked": masked, "config": config, "tables": [], "pivots": []})
    by_name = {sh["meta"]["name"]: sh for sh in sheets}
    ctx.sheet_data = {n: sh["data"] for n, sh in by_name.items()}
    shared_path = shared_strings_path(pkg, wb_rels)
    scan = scan_secrets(pkg, sheets, shared_path, specs)
    comment_secrets = scan_comments(pkg)

    # ---- tables ------------------------------------------------------------------
    tables_out = []
    for sh in sheets:
        for rid in sh["data"].table_rids + [k for k, v in sh["rels"].items() if v[0] == "table" and k not in sh["data"].table_rids]:
            rel = sh["rels"].get(rid)
            if not rel or rel[0] != "table" or not rel[1]:
                continue
            troot = guarded(pkg, rel[1], lambda: pkg.xml(rel[1]))
            if troot is None:
                continue
            ref = troot.get("ref") or ""
            parts = ref.split(":")
            a = parse_cell(parts[0]) if parts else None
            b = parse_cell(parts[-1]) if parts else None
            if not a or not b:
                continue
            totals = _int(troot.get("totalsRowCount"), 0)
            hdr = _int(troot.get("headerRowCount"), 1)
            cols = []
            has_af, af_el = False, None
            for ch in troot:
                n = ln(ch.tag)
                if n == "autoFilter":
                    has_af, af_el = True, ch
                if n == "tableColumns":
                    for tc in ch:
                        calc = None
                        for x in tc:
                            if ln(x.tag) == "calculatedColumnFormula":
                                calc = x.text or ""
                        cols.append({"name": tc.get("name") or "", "calculated": calc is not None})
            info = TableInfo(troot.get("name") or troot.get("displayName") or "", sh["meta"]["name"], a[0], a[1], b[0], b[1],
                             hdr, totals, [c["name"] for c in cols])
            ctx.tables[name_key(info.name)] = info
            if name_key(troot.get("displayName")) not in ctx.tables:
                ctx.tables[name_key(troot.get("displayName"))] = info
            sh["tables"].append((info, cols, has_af))
            af_out = None
            if af_el is not None:
                af_out = {"ref": af_el.get("ref") or ref, "filters": []}
                for f in [] if sh["masked"] else autofilter_filters(af_el):
                    name = cols[f["col_id"]]["name"] if f["col_id"] < len(cols) else ""
                    masked_name = secret_shaped(name) or (sh["meta"]["name"], a[0] + hdr - 1, a[1] + f["col_id"]) in scan.value_cells
                    af_out["filters"].append({"column": MASK if masked_name else name, "type": f["type"],
                                              "values": [] if masked_name else [v for v in f["values"] if not secret_shaped(v)]})
                if af_out["filters"]:
                    sh.setdefault("table_filters", []).append({"table": info.name, "ref": af_out["ref"], "filters": af_out["filters"]})
            tables_out.append({"name": info.name, "sheet": sh["meta"]["name"], "ref": ref,
                               "data_ref": rect_a1(a[0], a[1], b[0] - totals, b[1]), "totals_row_count": totals,
                               "header_row_count": hdr, "autofilter": af_out,
                               "columns": [] if sh["masked"] else [
                                   dict(c, name=MASK if secret_shaped(c["name"]) or (sh["meta"]["name"], a[0] + hdr - 1, a[1] + i) in scan.value_cells
                                        else c["name"]) for i, c in enumerate(cols)],
                               "masked": sh["masked"]})

    # ---- defined names ----------------------------------------------------------
    names_out = []
    secret_names = []
    lambda_count = solver_count = autoopen = 0
    solver_defs = []
    for d in book.names:
        kind, tok = classify_name(d["text"])
        if kind == "constant" and (is_secret_label(d["name"]) or secret_shaped(unquote(d["text"]))):
            secret_names.append(d["name"])
        scope_sheet = None
        if d["local"] is not None and d["local"].isdigit() and int(d["local"]) < len(book.sheets):
            scope_sheet = book.sheets[int(d["local"])]["name"]
        entry = {"name": d["name"], "scope": scope_sheet or "workbook", "kind": kind, "hidden": d["hidden"], "ref": None, "external": False}
        if (kind == "ref" and tok is not None and tok.ext is not None) or (
                kind in ("ref", "formula") and _EXT_MARK.search(d["text"] or "")):
            kind = entry["kind"] = "external"
            entry["external"] = True
        up = d["name"].upper()
        specs = []
        if kind == "ref" and tok is not None and tok.ext is None:
            sname = canon_sheet(ctx, tok.sheet) if tok.sheet else None
            if sname:
                names_in = ctx.sheet_names
                targets = [sname]
                if tok.sheet2:
                    s2 = canon_sheet(ctx, tok.sheet2)
                    if s2:
                        i1, i2 = names_in.index(sname), names_in.index(s2)
                        targets = names_in[min(i1, i2):max(i1, i2) + 1]
                for tn in targets:
                    specs.append(abs_spec(tn, tok.area.r1, tok.area.c1, tok.area.r2, tok.area.c2, tok.area.shape, None))
                entry["ref"] = f"{sname}!{fmt_area(tok.area, False)}"
            else:
                kind = entry["kind"] = "formula"
        if kind == "lambda":
            lambda_count += 1
            ctx.lambda_names.add(up)
        if up.startswith("SOLVER_"):
            solver_count += 1
            solver_defs.append((scope_sheet, d["name"], d["text"]))
        if re.match(r"^(_XLNM\.)?AUTO_OPEN$", up):
            autoopen += 1
        ctx.names[((scope_sheet.lower() if scope_sheet else None), up)] = {"kind": kind, "specs": specs, "text": d["text"]}
        names_out.append(entry)

    # ---- sheet analysis: regions --------------------------------------------------
    regions, cell_region = [], {}
    analysis_cache = {}
    position_budget = MAX_POSITION_ANALYSES
    sheet_index = {n: i for i, n in enumerate(ctx.sheet_names)}
    for sh in sheets:
        sd, nm = sh["data"], sh["meta"]["name"]
        masters = {}
        for pos, fc in sd.formulas.items():
            if fc.ftype == "shared" and fc.text:
                masters[fc.si] = (pos, fc.text)
        shared_key = {}
        groups = {}
        order = sorted(sd.formulas)
        text_of = make_text_of(sd, masters)
        for pos in order:
            fc = sd.formulas[pos]
            if fc.ftype == "shared" and fc.si in masters:
                mpos, mtext = masters[fc.si]
                if fc.si not in shared_key:
                    shared_key[fc.si] = analysis_for(analysis_cache, nm, mtext, mpos, ctx)
                an = shared_key[fc.si]
                if an.pos_sens and pos != mpos:
                    if position_budget > 0:
                        position_budget -= 1
                        analysis_for(analysis_cache, nm, text_of(pos), pos, ctx)
                    elif an.ci_probed:
                        ci_flag(an, "position-dependent", None)
            elif fc.ftype == "shared":
                an = Analysis()
                an.key = f"<shared formula {fc.si} without a master>"
                an.flag("shared_master_missing", f"si={fc.si}")
                an.bump("NR", "a shared-formula child has no master formula, so its text is unknown: ask for a re-save")
            else:
                an = analysis_for(analysis_cache, nm, fc.text, pos, ctx)
            kind = "formula"
            dyn = False
            if fc.ftype == "dataTable":
                kind = "datatable"
            elif fc.cm:
                loc = parse_loc(fc.ref) if fc.ref else None
                if loc and loc[0] == loc[2] and loc[1] == loc[3]:
                    dyn = True
                else:
                    kind = "spill"
            elif fc.ftype == "array":
                kind = "array"
            key = (kind, an.key, dyn) if kind == "formula" else (kind, pos)
            g = groups.get(key)
            if g is None:
                g = groups[key] = {"kind": kind, "an": an, "cells": [], "dyn": dyn}
            g["cells"].append(pos)
        for g in groups.values():
            for cells in (split_components(g["cells"], sd.consts) if g["kind"] == "formula" else [g["cells"]]):
                cells = sorted(cells)
                regions.append({"kind": g["kind"], "an": g["an"], "cells": cells, "sheet": nm, "fc": sd.formulas[cells[0]], "dyn": g.get("dyn", False),
                                "first_text": (lambda t=text_of, p=cells[0]: t(p))})
    regions.sort(key=lambda g: (sheet_index[g["sheet"]], min(g["cells"])))
    for i, g in enumerate(regions):
        g["id"] = f"R{i + 1}"
        for pos in g["cells"]:
            cell_region[(g["sheet"], pos[0], pos[1])] = g

    # ---- formula-cell index ---------------------------------------------------------
    fidx = defaultdict(lambda: defaultdict(list))
    for (sname, r, c) in cell_region:
        fidx[sname][c].append(r)
    fcols = {}
    for sname, cols in fidx.items():
        for c in cols:
            cols[c].sort()
        fcols[sname] = sorted(cols)

    def formula_cells_in(sname, r1, c1, r2, c2):
        cols = fidx.get(sname)
        if not cols:
            return
        cl = fcols[sname]
        lo, hi = bisect.bisect_left(cl, c1), bisect.bisect_right(cl, c2)
        for c in cl[lo:hi]:
            rows = cols[c]
            a, b = bisect.bisect_left(rows, r1), bisect.bisect_right(rows, r2)
            for r in rows[a:b]:
                yield (sname, r, c)

    # ---- excluded ranges ------------------------------------------------------------
    excluded = []
    for g in regions:
        if g["kind"] in ("array", "spill", "datatable"):
            ref = g["fc"].ref
            if ref:
                excluded.append({"sheet": g["sheet"], "kind": {"datatable": "data_table"}.get(g["kind"], g["kind"]), "ref": ref})

    # ---- pivots --------------------------------------------------------------------
    pivots_out, pivot_locs = [], defaultdict(list)
    ext_pivots = []
    for sh in sheets:
        for rid, (typ, tgt, ext) in sh["rels"].items():
            if typ != "pivotTable" or not tgt:
                continue
            pv = guarded(pkg, tgt, lambda: read_pivot(pkg, tgt, sh, book, wb_rels, ctx, by_name))
            if pv is None:
                continue
            pv = mask_pivot_fields(pv, ctx, scan.value_cells)
            if pv["cache_source"].get("type") == "external":
                ext_pivots.append(pv)
            if pv["location"]:
                excluded.append({"sheet": sh["meta"]["name"], "kind": "pivot", "ref": pv["location"]})
                pivot_locs[sh["meta"]["name"]].append(pv["location"])
            pivots_out.append(pv)
    excluded.sort(key=lambda e: (sheet_index.get(e["sheet"], 0), e["kind"], e["ref"]))

    def rects_of(sheet_name):
        out = []
        for e in excluded:
            if e["sheet"] == sheet_name:
                p = e["ref"].split(":")
                a, b = parse_ref_a1(p[0]), parse_ref_a1(p[-1])
                if a and b:
                    out.append((e["kind"], e["ref"], a[0], a[1], b[0], b[1]))
        return out

    # ---- dependency analysis: reads / depends_on / graph -----------------------------
    autofilter_sheets = set()
    for sh in sheets:
        if sh["data"].autofilter is not None or sh["tables"] and any(af for _, _, af in sh["tables"]):
            autofilter_sheets.add(sh["meta"]["name"])
    sheet_edges = Counter()
    incomplete = 0
    for g in regions:
        an = g["an"]
        hr, hc = min(g["cells"])
        lo_r, lo_c = min(r for r, _ in g["cells"]), min(c for _, c in g["cells"])
        hi_r, hi_c = max(r for r, _ in g["cells"]), max(c for _, c in g["cells"])
        reads, deps, seen = [], [], set()
        for s in an.refs:
            r1, c1, r2, c2 = s.resolve(hr, hc)
            r1, c1 = max(r1, 1), max(c1, 1)
            # a relative ref reads a different cell per row, so dependencies span every cell the region's copies read
            a_, b_ = s.resolve(lo_r, lo_c), s.resolve(hi_r, hi_c)
            members = list(_take(formula_cells_in(s.sheet, max(min(a_[0], b_[0]), 1), max(min(a_[1], b_[1]), 1),
                                                  max(a_[2], b_[2]), max(a_[3], b_[3])), 5000))
            ref_txt = rect_text(r1, c1, min(r2, MAX_ROW), min(c2, MAX_COL))
            entry = (s.sheet, ref_txt, s.via, "formula" if members else "data")
            if entry not in seen:
                seen.add(entry)
                reads.append({"sheet": s.sheet, "ref": ref_txt, "via": s.via, "kind": entry[3]})
            for m in members:
                rid = cell_region[m]["id"]
                if rid != g["id"] and rid not in deps:
                    deps.append(rid)
            if s.sheet != g["sheet"]:
                sheet_edges[(g["sheet"], s.sheet)] += len(g["cells"])
        g["reads"], g["depends_on"] = reads, deps
        if an.opaque:
            incomplete += len(g["cells"])

    # ---- cycles (cell level) ---------------------------------------------------------
    graph = graph_cycles(cell_region, fidx, formula_cells_in)
    iterate_on = truthy(book.calc.get("iterate"))
    circular_ids = {rid for c in graph["cycles"] for rid in c["regions"]}
    for c in graph["self_inclusive_range"]:
        c["iterates_when_iterate_on"] = True
        if iterate_on:
            circular_ids.update(c["regions"])

    # ---- plugs, region finalization ---------------------------------------------------
    for g in regions:
        sd = by_name[g["sheet"]]["data"]
        plugs = find_plugs(g, sd) if g["kind"] == "formula" else []
        g["plugs"] = plugs
        g["pivots"] = pivot_deps(g, pivots_out) if "getpivotdata" in g["an"].flags else []
        g["plug_candidates"] = plug_candidates(g, sd) if g["kind"] == "formula" else []
        an = g["an"]
        flags = {k: list(v) for k, v in an.flags.items()}
        route, reasons = an.route, list(an.reasons)
        if g.get("dyn"):
            flags.setdefault("dynamic_array_scalar", [])
        if plugs:
            flags["plug"] = [", ".join(plugs[:20])]
        if g["plug_candidates"]:
            flags["plug_at_end"] = [", ".join(g["plug_candidates"])]
        if g["kind"] in ("array", "spill", "datatable") and ROUTE_ORDER[route] < ROUTE_ORDER["C"]:
            route = "C"
            if g["kind"] == "array" and g["fc"].ref and not set(an.funcs) & DYNAMIC_ARRAY_FUNCS:
                reasons.append(f"CSE array formula: fixed-size array formula over {g['fc'].ref}, so it is never regioned silently")
            else:
                reasons.append({"array": "CSE array formula", "spill": "dynamic-array spill", "datatable": "What-If data table"}[g["kind"]] +
                               ": the output size is dynamic, so it is never regioned silently")
        if g["id"] in circular_ids:
            if ROUTE_ORDER[route] < ROUTE_ORDER["C"]:
                route = "C"
            reasons.append("circular: these cells depend on each other, so the recursion is unrolled to a fixed pass count with a tolerance (SC6)")
        if "full_column" in flags:
            hr, hc = min(g["cells"])
            for s in an.refs:
                if s.shape == "col" and s.r1 == 1 and s.r2 == MAX_ROW:
                    _, c1, _, c2 = s.resolve(hr, hc)
                    cols = fidx.get(s.sheet) or {}
                    if any(cell_region[(s.sheet, r, c)]["id"] != g["id"] and set(cell_region[(s.sheet, r, c)]["an"].funcs) & TOTAL_FUNCS
                           for c in range(c1, c2 + 1) for r in cols.get(c, ())):
                        flags["full_column_total"] = [f"{s.sheet}!{num2col(c1)}:{num2col(c2)}"]
                        break
        read_sheets = {s.sheet for s in an.refs} or {g["sheet"]}
        if an.subtotal is not None and (an.subtotal > 100 or any(
                n in autofilter_sheets or by_name[n]["data"].hidden_rows for n in read_sheets if n in by_name)):
            flags["subtotal_ui_state"] = []
            if ROUTE_ORDER[route] < ROUTE_ORDER["C"]:
                route = "C"
            reasons.append("SUBTOTAL/AGGREGATE reflects filter or hidden-row UI state: never bake that state into the model")
        g["route"], g["reasons"], g["flags_map"] = route, reasons, flags

    # ---- typed overwrites inside a copied-down formula ---------------------------------
    overwrites = defaultdict(set)
    for g in regions:
        sd = by_name[g["sheet"]]["data"]
        if g["kind"] != "formula" or by_name[g["sheet"]]["masked"]:
            continue
        for (r, c) in g["cells"]:
            for dr, dc in ((1, 0), (0, 1)):
                run, k = [], 1
                while k <= 3 and (r + dr * k, c + dc * k) in sd.consts:
                    run.append((r + dr * k, c + dc * k))
                    k += 1
                end = cell_region.get((g["sheet"], r + dr * k, c + dc * k))
                if (run and end is not None and end["kind"] == "formula" and end["an"].key == g["an"].key
                        and any(sd.consts[x] != "n" for x in run) and not any((g["sheet"], x[0], x[1]) in scan.value_cells for x in run)):
                    for gg in (g, end):
                        overwrites[gg["id"]].update(run)
    for g in regions:
        if g["id"] in overwrites:
            g["flags_map"]["typed_overwrite"] = [", ".join(f"{num2col(c)}{r}" for r, c in sorted(overwrites[g["id"]])[:20])]

    # ---- sources --------------------------------------------------------------------
    layouts = []
    date_ids = date_ids_of(pkg, wb_rels)
    styles_part = next((r[1] for r in wb_rels.values() if r[0] == "styles" and r[1]), None) or "xl/styles.xml"
    time_ids = time_style_ids(pkg.xml(styles_part) if pkg.has(styles_part) else None)
    reads = absolute_reads(regions)
    sources = build_sources(sheets, regions, cell_region, rects_of, pivot_locs, pkg, shared_path, bookname, scan.value_cells, layouts, date_ids,
                            time_ids, book.date1904, reads)
    pct_ids = percent_style_ids(pkg.xml(styles_part) if pkg.has(styles_part) else None)
    typed_extras, input_blocks = typed_inputs(sheets, regions, sources, rects_of, pivot_locs, scan.value_cells, bookname)
    sources.extend(input_blocks)
    form_controls, control_givens = control_inventory(pkg, sheets, ctx)
    solver_model, solver_givens = solver_inventory(solver_defs, ctx, sheets)
    givens, givens_omitted = given_candidates(pkg, sheets, shared_path, absolute_reads(regions, {"GETPIVOTDATA"}), selector_reads(regions), pivot_locs,
                                           scan.value_cells, date_ids, pct_ids, book.date1904, {**typed_extras, **solver_givens, **control_givens},
                                           key_reads(regions), validated_cells(sheets, cell_reads_by(regions, lambda s: True)))

    for g in regions:
        hr, hc = min(g["cells"])
        for s_ in g["an"].refs:
            if s_.via not in DATE_TEXT_READERS:
                continue
            r1, c1, r2, c2 = s_.resolve(hr, hc)
            for so in sources:
                for d in so.get("date_as_text") or ():
                    loc = parse_loc(d["ref"])
                    if so["sheet"] == s_.sheet and loc and not (r2 < loc[0] or r1 > loc[2] or c2 < loc[1] or c1 > loc[3]):
                        g["flags_map"].setdefault("date_as_text", [])
                        if d["ref"] not in g["flags_map"]["date_as_text"]:
                            g["flags_map"]["date_as_text"].append(d["ref"])

    # ---- sheets out + classification -------------------------------------------------
    inbound = defaultdict(Counter)
    sheet_stats = defaultdict(Counter)
    sheet_of = {g["id"]: g["sheet"] for g in regions}
    for g in regions:
        sheet_stats[g["sheet"]]["regions"] += 1
        if any(sheet_of.get(d) == g["sheet"] for d in g["depends_on"]):
            sheet_stats[g["sheet"]]["intra"] += 1
        crossed = False
        for rd in g["reads"]:
            if rd["via"] in TABLE_LOOKUPS and ":" in rd["ref"]:
                inbound[rd["sheet"]]["lookup"] += 1
            if rd["sheet"] == g["sheet"]:
                continue
            if rd["via"] in TABLE_LOOKUPS:
                continue
            if re.match(r"^[A-Z]{1,3}\d{1,7}$", rd["ref"]) and any(s.r1a and s.c1a for s in g["an"].refs if s.sheet == rd["sheet"]):
                inbound[rd["sheet"]]["abs"] += 1
            else:
                inbound[rd["sheet"]]["agg"] += 1
                crossed = True
        if crossed:
            sheet_stats[g["sheet"]]["cross"] += 1
    sheets_out = []
    for sh in sheets:
        sd, nm, meta = sh["data"], sh["meta"]["name"], sh["meta"]
        colstats = defaultdict(Counter)
        data_areas = []
        for s in sources:
            loc = parse_loc(s["data_ref"]) if s["sheet"] == nm and s["layout"] != "wide" else None
            if loc:
                data_areas.append((s["first_data_row"], loc[1], loc[2], loc[3]))
        for (r, c), kind in sd.consts.items():
            if not any(a <= r <= b and c1_ <= c <= c2_ for a, c1_, b, c2_ in data_areas):
                continue
            colstats[c]["text" if kind in ("s", "inlineStr", "str") else "number" if kind == "n" else "other"] += 1
        mixed = {}
        for c, cnt in colstats.items():
            if cnt["text"] and cnt["number"]:
                mixed[num2col(c)] = {"number": cnt["number"], "text": cnt["text"]}
        cls, why = classify_sheet(sh, sd, inbound[nm], sheet_stats[nm], sources, pivot_locs, regions)
        sheets_out.append({
            "name": nm, "state": meta["state"], "masked": sh["masked"], "path": meta["path"], "class": cls, "class_reason": why,
            "formula_cells": len(sd.formulas), "constant_cells": len(sd.consts),
            "hidden_rows": len(sd.hidden_rows), "hidden_cols": sd.hidden_cols, "outline_rows": sd.outline_rows,
            "merge_cells": len(sd.merges), "autofilter": sd.autofilter, "dimension": sd.dimension,
            "autofilter_filters": [] if sh["masked"] else sheet_filters(sd), "table_autofilters": sh.get("table_filters", []),
            "mixed_columns": mixed, "regions": [g["id"] for g in regions if g["sheet"] == nm]})

    # ---- code attached, external, oracle --------------------------------------------
    ext = external_inventory(pkg, rep, ext_pivots)
    found = ext.pop("_secrets")
    cache_of = defaultdict(set)
    for sh in sheets:
        for typ, tgt, _e in sh["rels"].values():
            cid = ext["query_table_conn"].get(tgt) if typ == "queryTable" and tgt else None
            if cid:
                cache_of[sh["meta"]["name"]].add(f"conn {cid}")
    for pv in pivots_out:
        if pv["cache_source"].get("connection_id"):
            cache_of[pv["sheet"]].add(f"conn {pv['cache_source']['connection_id']}")
    for so in sheets_out:
        so["cache_of"] = sorted(cache_of.get(so["name"], ()))
    priority = scan.priority | {v for v in found.values() if len(v) >= 4}
    known, weak = scan.known | scan.shaped_text, scan.weak
    scrub_truncated = max(len(known - priority) - MAX_KNOWN, 0) + max(len(weak) - MAX_KNOWN, 0)
    code = code_inventory(pkg, sheets, regions, cell_region, names_out, lambda_count, solver_count, autoopen, ctx)
    date_system = date_system_info(pkg, wb_rels, sheets, book.date1904, date_ids)
    oracle = oracle_gate(pkg, book, sheets, regions, ext, code, autofilter_sheets, date_system)

    # ---- regions out -------------------------------------------------------------------
    mark_volatile_dependents(regions)
    regions_out = []
    oracle_budget, oracle_omitted = MAX_ORACLE_STANZAS, 0
    fn_inv = defaultdict(lambda: {"regions": 0, "cells": 0, "routes": Counter()})
    route_counts = {k: {"regions": 0, "cells": 0} for k in ROUTE_ORDER}
    nr_recipe = {"regions": 0, "cells": 0}
    for g in regions:
        masked = by_name[g["sheet"]]["masked"] or any((g["sheet"], r, c) in scan.value_cells for r, c in g["cells"])
        an = g["an"]
        ref = g["fc"].ref if g["kind"] in ("array", "spill", "datatable") and g["fc"].ref else bbox_ref(g["cells"])
        first = min(g["cells"])
        regions_out.append({
            "id": g["id"], "sheet": g["sheet"], "kind": g["kind"], "ref": ref, "cells": region_cell_count(g),
            "r1c1": None if masked else an.key, "example": None if masked else g["first_text"](),
            "split": "single_cell" if len(g["cells"]) == 1 and an.split == "row_local" else an.split, "absolute_refs": an.absolute_refs,
            "functions": dict(an.funcs), "route": g["route"], "reasons": g["reasons"],
            "reason_id": "pivot_recipe" if g["route"] == "NR" and an.nr_reasons == {PIVOT_RECIPE_REASON} else None,
            "plug_candidates": g["plug_candidates"],
            "flags": [{"id": k, "detail": "" if masked and k == "hardcoded_constant" else "; ".join(v)}
                      for k, v in sorted(g["flags_map"].items())],
            "reads": g["reads"], "depends_on": g["depends_on"], "plugs": g["plugs"], "pivots": g["pivots"],
            "volatile_dep": g["volatile_dep"], "volatile_dep_via": g["volatile_dep_via"],
            "oracle_stanza": None, "hidden_dep": by_name[g["sheet"]]["masked"],
            "hidden_dep_via": "located" if by_name[g["sheet"]]["masked"] else None})
        if not masked:
            if oracle_budget:
                regions_out[-1]["oracle_stanza"] = render_oracle_stanza(bookname, g["sheet"], ref)
                oracle_budget -= regions_out[-1]["oracle_stanza"] is not None
            else:
                oracle_omitted += 1
        if g["kind"] == "datatable":
            regions_out[-1]["data_table"] = None if masked else data_table_info(g["fc"], g["sheet"])
            if regions_out[-1]["data_table"]:
                dtab = regions_out[-1]["data_table"]
                dtab["oracle_stanza"] = regions_out[-1]["oracle_stanza"]
                dtab["axis_oracle_stanzas"] = [{"axis": axis, "ref": dtab[key], "stanza": render_oracle_stanza(bookname, g["sheet"], dtab[key])}
                                               for axis, key in (("row", "row_axis"), ("column", "column_axis")) if dtab.get(key)]
        for fn in an.funcs:
            fn_inv[fn]["regions"] += 1
            fn_inv[fn]["cells"] += region_cell_count(g)
            fn_inv[fn]["routes"][g["route"]] += 1
        route_counts[g["route"]]["regions"] += 1
        route_counts[g["route"]]["cells"] += region_cell_count(g)
        if regions_out[-1]["reason_id"] == "pivot_recipe":
            nr_recipe["regions"] += 1
            nr_recipe["cells"] += region_cell_count(g)
    mark_hidden_dependents(regions, regions_out, by_name)
    for pv in pivots_out:
        pv["oracle_stanza"] = None
        if pv["location"] and pv["row_fields"] is not None and not by_name[pv["sheet"]]["masked"]:
            if oracle_budget:
                pv["oracle_stanza"] = render_oracle_stanza(bookname, pv["sheet"], pv["location"])
                oracle_budget -= pv["oracle_stanza"] is not None
            else:
                oracle_omitted += 1
    oracle_info = {"stanzas": MAX_ORACLE_STANZAS - oracle_budget, "omitted_by_cap": oracle_omitted, "cap": MAX_ORACLE_STANZAS}
    for ro in regions_out:
        dt = ro.get("data_table")
        if not dt:
            continue
        inputs = [(x, parse_loc(x)) for x in dt["input_cells"]]
        axes = [(x, parse_loc(x)) for x in dt["axis_refs"]]
        for s in sources:
            loc = parse_loc(s["data_ref"]) if s["sheet"] == ro["sheet"] else None
            if not loc:
                continue

            def hit(l_):
                return l_ and not (l_[2] < loc[0] or l_[0] > loc[2] or l_[3] < loc[1] or l_[1] > loc[3])
            in_in = [x for x, l_ in inputs if hit(l_)]
            in_ax = [x for x, l_ in axes if hit(l_)]
            if not (in_in or in_ax):
                continue
            s.setdefault("data_table_overlaps", []).append({"region": ro["id"], "input_cells_in_range": in_in, "axis_cells_in_range": in_ax})
            if s["stanza"]:
                note = (f'WARNING: the range holds data table {ro["id"]} ({dt["ref"]}) '
                        f'{"input cell " + ", ".join(in_in) if in_in else ""}{" and " if in_in and in_ax else ""}'
                        f'{"axis values " + ", ".join(in_ax) if in_ax else ""}: those cells are what-if levers, not data; exclude them')
                s["stanza"] = add_stanza_note(s["stanza"], note)
    functions = {}
    for fn, v in sorted(fn_inv.items()):
        by_route = {k: v["routes"][k] for k in ROUTE_ORDER if v["routes"][k]}
        worst = max([function_route(fn)] + list(by_route), key=lambda k: ROUTE_ORDER[k])
        functions[fn] = {"regions": v["regions"], "cells": v["cells"], "route": worst, "routes": by_route}

    graph.update({"sheet_edges": [{"from": a, "to": b, "cells": n} for (a, b), n in sorted(sheet_edges.items())],
                  "incomplete_cells": incomplete, "iterate": iterate_on})
    graph["iterate_note"] = ""
    if graph["iterate"]:
        if graph["cycles"]:
            graph["iterate_note"] = "iterate=1 and a cycle was found: a real circularity"
        elif graph["self_inclusive_range"]:
            graph["iterate_note"] = "iterate=1 and a self-inclusive range was found (an aggregate over a range holding its own cell); no other cycle in the graph; these cells iterate under iterate=1: treat as SC6 loops"
        else:
            graph["iterate_note"] = "iterate=1 is only a setting: no cycle in the graph (a stale setting)"
        if graph["edge_budget_exceeded"] and not graph["cycles"]:
            graph["iterate_note"] += "; cycle detection was cut short by the edge budget"

    power_pivot = pkg.has("xl/model/item.data")
    core = pkg.xml("docProps/core.xml")
    modified = None
    if core is not None:
        for el in core.iter():
            if ln(el.tag) == "modified":
                modified = (el.text or "").strip()
    calc = book.calc
    for pv_ in pivots_out:
        pv_["refresh"] = refreshed_vs_save(pv_.get("refreshed_date"), modified, book.date1904)
    rep.update({
        "workbook_props": {"date1904": book.date1904, "modified": modified, "power_pivot": power_pivot,
                           "calc": {"calcId": calc.get("calcId"), "calcMode": calc.get("calcMode"), "calcOnSave": calc.get("calcOnSave"),
                                    "iterate": truthy(calc.get("iterate")), "iterateCount": calc.get("iterateCount"),
                                    "iterateDelta": calc.get("iterateDelta"),
                                    "fullCalcOnLoad": truthy(calc.get("fullCalcOnLoad")), "present": bool(calc),
                                    "effective": {"calcMode": calc.get("calcMode") or "auto",
                                                  "calcOnSave": calc.get("calcOnSave") not in ("0", "false"),
                                                  "fullCalcOnLoad": truthy(calc.get("fullCalcOnLoad")),
                                                  "iterate": truthy(calc.get("iterate")),
                                                  "iterateCount": _int(calc.get("iterateCount"), 100),
                                                  "iterateDelta": _float(calc.get("iterateDelta"), 0.001)}}},
        "given_candidates": givens, "given_candidates_omitted": givens_omitted, "form_controls": form_controls, "solver": solver_model, "oracle": oracle, "oracle_stanzas": oracle_info, "sheets": sheets_out, "defined_names": names_out, "tables": tables_out, "pivots": pivots_out,
        "scenarios": [dict(sheet=sh["meta"]["name"], **sc) for sh in sheets if not sh["masked"] for sc in sh["data"].scenario_list],
        "regions": regions_out, "sources": sources, "layouts": layouts, "date_system": date_system, "excluded_ranges": excluded, "graph": graph,
        "functions": functions, "routes": route_counts, "routes_nr_pivot_recipe": nr_recipe, "code_attached": code, "external": ext,
        "unconfirmed_tells": sorted(UNCONFIRMED_TELLS),
    })
    rep["ci_match"] = ci_match_summary(regions, sources)
    rep["ci_match_cannot_bite"] = rep["ci_match"]["cannot_bite"]
    finish_package()
    rep["security"] = build_security(rep, code, pkg, sheets, ext, names_out, {
        "labelled": scan.labelled, "shaped": scan.shaped, "names": secret_names, "comments": comment_secrets,
        "label_overflow": scan.label_overflow, "scrub_truncated": scrub_truncated})
    rep["not_read"] = not_read(pkg, rep)
    return scrub(rep, known, weak, priority, scan.shaped_text)


def _take(it, n):
    for i, x in enumerate(it):
        if i >= n:
            return
        yield x


def make_text_of(sd, masters):
    def text_of(pos):
        fc = sd.formulas[pos]
        if fc.ftype == "shared" and fc.si in masters:
            mpos, mtext = masters[fc.si]
            return expand_shared(mtext, pos[0] - mpos[0], pos[1] - mpos[1])
        return fc.text
    return text_of


def pivot_deps(g, pivots):
    """The pivots whose rendered range a GETPIVOTDATA region references."""
    hr, hc = min(g["cells"])
    out = []
    for s in g["an"].refs:
        r1, c1, r2, c2 = s.resolve(hr, hc)
        for pv in pivots:
            loc = parse_loc(pv["location"]) if pv.get("location") else None
            if (loc and pv["sheet"] == s.sheet and r1 <= loc[2] and r2 >= loc[0] and c1 <= loc[3] and c2 >= loc[1]
                    and not any(o["name"] == pv["name"] for o in out)):
                out.append({"name": pv["name"], "location": pv["location"], "source": pv["cache_source"]})
    return out


def plug_gap(S, consts, r, c, dr, dc):
    """Constants (1-3) between two cells of one formula count as plugs only when the run continues past the gap on a side."""
    k = 1
    while k <= 3 and (r + dr * k, c + dc * k) not in S and consts.get((r + dr * k, c + dc * k)) == "n":
        k += 1
    if k > 1 and (r + dr * k, c + dc * k) in S and ((r - dr, c - dc) in S or (r + dr * (k + 1), c + dc * (k + 1)) in S):
        return k
    return 0


def split_components(cells, consts):
    """One formula copied into two separate blocks is two regions; a numeric plug inside a block does not split it."""
    S = set(cells)
    parent = {x: x for x in S}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for (r, c) in S:
        for dr, dc in ((1, 0), (0, 1)):
            nxt = (r + dr, c + dc)
            if nxt not in S:
                k = plug_gap(S, consts, r, c, dr, dc)
                nxt = (r + dr * k, c + dc * k) if k else None
            if nxt is not None:
                parent[find((r, c))] = find(nxt)
    groups = defaultdict(list)
    for x in S:
        groups[find(x)].append(x)
    return list(groups.values())


def merge_position_flags(base, fresh):
    """A flag decided by the content of a relative ref can differ from copy to copy: the shared analysis keeps every copy's flags."""
    for fid, details in fresh.flags.items():
        for d in details or [""]:
            base.flag(fid, d)
    for k, v in fresh.ci_sides.items():
        base.ci_sides.setdefault(k, v)


def too_nested(text):
    # Python's own recursion limit differs by version, so the depth is capped explicitly
    depth = peak = 0
    for ch in text:
        if ch == "(":
            depth += 1
            peak = max(peak, depth)
        elif ch == ")":
            depth -= 1
    return peak > MAX_FORMULA_NEST


def analysis_for(cache, sheet, text, pos, ctx):
    # keyed by R1C1 text, so every copy of one formula shares one analysis
    try:
        if too_nested(text):
            raise RecursionError
        an = analyze_formula(text, sheet, pos[0], pos[1], ctx)
    except RecursionError:
        an = Analysis()
        an.key = normalize([t for t in tokenize(text) if t.kind != "ws"], pos[0], pos[1])
        an.opaque = True
        an.flag("opaque_dependency", "formula nesting too deep to analyze")
        an.bump("NR", "the formula nests too deeply for the script to analyze: ask what it computes")
    ck = (sheet, an.key)
    if ck in cache:
        if an.pos_sens:
            merge_position_flags(cache[ck], an)
        return cache[ck]
    cache[ck] = an
    return an


def bbox_ref(cells):
    r1 = min(r for r, _ in cells)
    r2 = max(r for r, _ in cells)
    c1 = min(c for _, c in cells)
    c2 = max(c for _, c in cells)
    return rect_a1(r1, c1, r2, c2)


def find_plugs(g, sd):
    S = set(g["cells"])
    if len(S) < 2:
        return []
    plugs = []
    for (r, c) in g["cells"]:
        for dr, dc in ((1, 0), (0, 1)):
            for j in range(1, plug_gap(S, sd.consts, r, c, dr, dc)):
                plugs.append(f"{num2col(c + dc * j)}{r + dr * j}")
    return sorted(set(plugs), key=lambda p: (int(re.sub(r"\D", "", p)), p))


def reads_actuals_block(g, sd, r0, c0, dr, dc):
    """True when 2+ numeric constants sit just before the run's first cell and the run reads one of them: actuals feeding a forecast."""
    block = set()
    r, c = r0 - dr, c0 - dc
    while r >= 1 and c >= 1 and sd.consts.get((r, c)) == "n":
        block.add((r, c))
        r, c = r - dr, c - dc
    if len(block) < 2:
        return False
    for s in g["an"].refs:
        if s.sheet != g["sheet"]:
            continue
        r1, c1, r2, c2 = s.resolve(r0, c0)
        if any(r1 <= br <= r2 and c1 <= bc <= c2 for br, bc in block):
            return True
    return False


def typed_block_after(g, sd, r_last, c_last, dr, dc):
    """True when 2+ numeric constants run on from the run's last cell and the run reads none of them: typed inputs past linked history."""
    block = set()
    r, c = r_last + dr, c_last + dc
    while sd.consts.get((r, c)) == "n":
        block.add((r, c))
        r, c = r + dr, c + dc
    if len(block) < 2:
        return False
    r0, c0 = min(g["cells"])
    for s in g["an"].refs:
        if s.sheet != g["sheet"]:
            continue
        r1, c1, r2, c2 = s.resolve(r0, c0)
        if any(r1 <= br <= r2 and c1 <= bc <= c2 for br, bc in block):
            return False
    return True


def plug_candidates(g, sd):
    """A numeric constant just past either end of a straight run of one formula, other than the actuals the run extends."""
    cells = g["cells"]
    if len(cells) < 3:
        return []
    rows, cols = {r for r, _ in cells}, {c for _, c in cells}
    out = []
    if len(cols) == 1:
        c = cells[0][1]
        for r in (min(rows) - 1, max(rows) + 1):
            if r >= 1 and sd.consts.get((r, c)) == "n" and not (r < min(rows) and reads_actuals_block(g, sd, min(rows), c, 1, 0)) \
                    and not (r > max(rows) and typed_block_after(g, sd, max(rows), c, 1, 0)):
                out.append(f"{num2col(c)}{r}")
    elif len(rows) == 1:
        r = cells[0][0]
        for c in (min(cols) - 1, max(cols) + 1):
            if c >= 1 and sd.consts.get((r, c)) == "n" and not (c < min(cols) and reads_actuals_block(g, sd, r, min(cols), 0, 1)) \
                    and not (c > max(cols) and typed_block_after(g, sd, r, max(cols), 0, 1)):
                out.append(f"{num2col(c)}{r}")
    return out


def function_route(fn):
    if fn in RAND_FUNCS or fn in WEB_FUNCS or fn in VENDOR:
        return "X"
    if fn in LAMBDA_FAMILY or fn in ("OFFSET", "INDIRECT") or fn in CUBE_FUNCS or fn in BLOCKED_FUNCS or fn in XLM_FUNCS or fn in ETS_FUNCS:
        return "NR"
    if fn in AT_COST or fn in ("TODAY", "NOW", "PY"):
        return "C"
    if fn in KNOWN_FUNCS:
        return "T"
    return "NR"


def graph_cycles(cell_region, fidx, formula_cells_in):
    adj = {}
    self_agg, self_other = set(), set()
    edges = [0]
    exceeded = [False]

    def neighbors(node):
        got = adj.get(node)
        if got is not None:
            return got
        sname, r, c = node
        out = []
        if exceeded[0]:
            adj[node] = out
            return out
        seen = set()
        for s in cell_region[node]["an"].refs:
            r1, c1, r2, c2 = s.resolve(r, c)
            holds_self = s.sheet == sname and r1 <= r <= r2 and c1 <= c <= c2
            if s.via == "OFFSET" and holds_self:
                continue
            if holds_self:
                (self_agg if s.shape != "cell" and s.via in AGGREGATE_VIAS else self_other).add(node)
            for m in formula_cells_in(s.sheet, max(r1, 1), max(c1, 1), r2, c2):
                if m not in seen:
                    seen.add(m)
                    out.append(m)
                    edges[0] += 1
            if edges[0] > MAX_EDGES:
                exceeded[0] = True
                break
        adj[node] = out
        return out

    index, low, onstack, stack, counter = {}, {}, set(), [], [0]
    by_regions, by_self = {}, {}
    for start in list(cell_region):
        if start in index:
            continue
        work = [(start, iter(neighbors(start)))]
        index[start] = low[start] = counter[0]
        counter[0] += 1
        stack.append(start)
        onstack.add(start)
        while work:
            node, it = work[-1]
            advanced = False
            for nxt in it:
                if nxt not in index:
                    index[nxt] = low[nxt] = counter[0]
                    counter[0] += 1
                    stack.append(nxt)
                    onstack.add(nxt)
                    work.append((nxt, iter(neighbors(nxt))))
                    advanced = True
                    break
                if nxt in onstack:
                    low[node] = min(low[node], index[nxt])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                comp = []
                while True:
                    w = stack.pop()
                    onstack.discard(w)
                    comp.append(w)
                    if w == node:
                        break
                if len(comp) > 1 or node in neighbors(node):
                    key = tuple(sorted({cell_region[n]["id"] for n in comp}, key=lambda s: int(s[1:])))
                    bucket = by_self if len(comp) == 1 and node in self_agg and node not in self_other else by_regions
                    got = bucket.get(key)
                    if got is None:
                        got = bucket[key] = {"cells": 0, "loops": 0, "regions": list(key)}
                    got["cells"] += len(comp)
                    got["loops"] += 1
    cycles = sorted(by_regions.values(), key=lambda c: (-c["cells"], c["regions"]))
    selfs = sorted(({"cells": v["cells"], "regions": v["regions"]} for v in by_self.values()), key=lambda c: (-c["cells"], c["regions"]))
    return {"cycles": cycles[:50], "self_inclusive_range": selfs[:50], "edge_budget_exceeded": exceeded[0]}


def serial_date(v, date1904=False):
    """An Excel serial day as an ISO date, or None when it is not a date this can name."""
    try:
        d = float(v)
        if not math.isfinite(d) or d < 1 or d > 2958465:
            return None
        base = datetime.date(1904, 1, 1) if date1904 else datetime.date(1899, 12, 30)
        return (base + datetime.timedelta(days=int(d))).isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def _a1(r, c):
    return f"{num2col(c)}{r}"


def region_cell_count(g):
    """An array or spill region owns every cell of its ref, though only the anchor carries the formula."""
    loc = parse_loc(g["fc"].ref) if g["kind"] in ("array", "spill") and g["fc"].ref else None
    return (loc[2] - loc[0] + 1) * (loc[3] - loc[1] + 1) if loc else len(g["cells"])


def data_table_info(fc, sheet):
    """What a what-if data table varies and where its axis values sit: ECMA-376 dataTable r1/r2/dt2D/dtr."""
    loc = parse_loc(fc.ref) if fc.ref else None
    dt = fc.dt or {}
    if not loc:
        return None
    r1, c1, r2, c2 = loc
    two_d, row_input = truthy(dt.get("dt2D")), truthy(dt.get("dtr"))
    out = {"ref": fc.ref, "two_d": two_d, "dtr": row_input, "r1": dt.get("r1"), "r2": dt.get("r2")}
    if two_d:
        out.update({"row_input_cell": dt.get("r1"), "column_input_cell": dt.get("r2"),
                    "row_axis": rect_a1(r1 - 1, c1, r1 - 1, c2) if r1 > 1 else None,
                    "column_axis": rect_a1(r1, c1 - 1, r2, c1 - 1) if c1 > 1 else None,
                    "formula_cell": _a1(r1 - 1, c1 - 1) if r1 > 1 and c1 > 1 else None})
        inputs = [x for x in (dt.get("r1"), dt.get("r2")) if x]
        axes = [out["row_axis"], out["column_axis"]]
    elif row_input:
        out.update({"row_input_cell": dt.get("r1"), "column_input_cell": None,
                    "row_axis": rect_a1(r1 - 1, c1, r1 - 1, c2) if r1 > 1 else None, "column_axis": None,
                    "formula_cells": rect_a1(r1, c1 - 1, r2, c1 - 1) if c1 > 1 else None})
        inputs, axes = [x for x in (dt.get("r1"),) if x], [out["row_axis"]]
    else:
        out.update({"row_input_cell": None, "column_input_cell": dt.get("r1"), "row_axis": None,
                    "column_axis": rect_a1(r1, c1 - 1, r2, c1 - 1) if c1 > 1 else None,
                    "formula_cells": rect_a1(r1 - 1, c1, r1 - 1, c2) if r1 > 1 else None})
        inputs, axes = [x for x in (dt.get("r1"),) if x], [out["column_axis"]]
    out["input_cells"] = inputs
    out["axis_refs"] = [a for a in axes if a]
    out["sheet"] = sheet
    return out


def refreshed_vs_save(refreshed, modified, date1904):
    """The pivot cache's refresh date against dcterms:modified: {'refreshed': ISO, 'days_before_save': N or None}."""
    iso = serial_date(refreshed, date1904)
    out = {"refreshed": iso, "modified": None, "days_before_save": None}
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", modified or "")
    if m:
        out["modified"] = m.group(1)
        if iso:
            try:
                out["days_before_save"] = (datetime.date.fromisoformat(m.group(1)) - datetime.date.fromisoformat(iso)).days
            except ValueError:
                pass
    return out


MAX_SLICER_CACHES = 500
MAX_FILTER_LABELS = 200
MAX_SAME_REASON_REFS = 5


def slicer_caches_by_pivot(pkg):
    """{pivot name: [{"source", "items": [(x, selected)] or None, "tab_id"}]} from the slicer caches, read once per package."""
    if getattr(pkg, "_slicer_index", None) is None:
        idx = defaultdict(list)
        for name in [n for n in pkg.under("xl/slicerCaches/") if "/_rels/" not in n][:MAX_SLICER_CACHES]:
            root = pkg.xml(name)
            if root is None:
                continue
            tabular = next((x for x in root.iter() if ln(x.tag) == "tabular"), None)
            items = None
            if tabular is not None:
                items = [(_int(i.get("x"), -1), i.get("s") in ("1", "true")) for i in tabular.iter() if ln(i.tag) == "i"]
            for pt in root.iter():
                if ln(pt.tag) == "pivotTable" and pt.get("name"):
                    idx[pt.get("name")].append({"source": root.get("sourceName"), "items": items, "tab_id": pt.get("tabId")})
        pkg._slicer_index = idx
    return pkg._slicer_index


def timeline_caches_by_pivot(pkg):
    """({pivot name: [timeline entry + tab_id]}, unparsed count) from the timeline caches, read once per package."""
    if getattr(pkg, "_timeline_index", None) is None:
        idx, unparsed = defaultdict(list), 0
        for name in [n for n in pkg.under("xl/timelineCaches/") if "/_rels/" not in n][:MAX_SLICER_CACHES]:
            root = pkg.xml(name)
            if root is None or ln(root.tag) != "timelineCacheDefinition":
                unparsed += 1
                continue
            sel = next((x for x in root.iter() if ln(x.tag) == "selection"), None)
            bnd = next((x for x in root.iter() if ln(x.tag) == "bounds"), None)
            start, end = (sel.get("startDate"), sel.get("endDate")) if sel is not None else (None, None)
            b_start, b_end = (bnd.get("startDate"), bnd.get("endDate")) if bnd is not None else (None, None)
            # ISO timestamps order as text; a selection with no bounds to compare against is not known to narrow
            filtering = bool((start and b_start and start > b_start) or (end and b_end and end < b_end))
            entry = {"field": root.get("sourceName"), "start": start, "end": end, "bounds_start": b_start, "bounds_end": b_end, "filtering": filtering}
            for pt in root.iter():
                if ln(pt.tag) == "pivotTable" and pt.get("name"):
                    idx[pt.get("name")].append(dict(entry, tab_id=pt.get("tabId")))
        pkg._timeline_index = (idx, unparsed)
    return pkg._timeline_index


def _bound_to(entries, sh, book):
    """The cache links that name this pivot's sheet; a link with no resolvable tabId falls back to the name alone."""
    known = {b["sheet_id"] for b in book.sheets}
    return [e for e in entries if e["tab_id"] is None or e["tab_id"] not in known or e["tab_id"] == sh["meta"]["sheet_id"]]


def read_pivot(pkg, path, sh, book, wb_rels, ctx, by_name):
    root = pkg.xml(path)
    if root is None:
        return None
    prels = pkg.rels(path)
    cache_path = None
    for rel in prels.values():
        if rel[0] == "pivotCacheDefinition":
            cache_path = rel[1]
    if cache_path is None:
        rid = book.pivot_caches.get(root.get("cacheId"))
        tgt = wb_rels.get(rid)
        cache_path = tgt[1] if tgt else None
    cache = pkg.xml(cache_path) if cache_path else None
    fields, calc_fields, grouping, num_grouping, shared, cache_items = [], [], [], [], [], []
    group_items, group_base = [], []
    source = {"type": None}
    refreshed, records = None, False
    if cache is not None:
        refreshed = cache.get("refreshedDate")
        for ch in cache:
            n = ln(ch.tag)
            if n == "cacheSource":
                typ = ch.get("type")
                source = {"type": typ}
                if typ == "worksheet":
                    for ws in ch:
                        if ln(ws.tag) == "worksheetSource":
                            if ws.get("name"):
                                source["name"] = ws.get("name")
                            else:
                                source["sheet"] = ws.get("sheet")
                                source["ref"] = ws.get("ref")
                elif typ == "external" and ch.get("connectionId"):
                    source["connection_id"] = ch.get("connectionId")
            elif n == "calculatedItems":
                for ci in ch:
                    if ln(ci.tag) != "calculatedItem" or not ci.get("formula"):
                        continue
                    ref = next((x for x in ci.iter() if ln(x.tag) == "reference" and x.get("field") is not None), None)
                    cache_items.append((ref.get("field") if ref is not None else None, ci.get("formula")))
            elif n == "cacheFields":
                for cf in ch:
                    fname = cf.get("name") or ""
                    fields.append(fname)
                    items = []
                    for x in cf:
                        if ln(x.tag) == "sharedItems":
                            items = [(i.get("v") if i.get("v") is not None else "") for i in x]
                    shared.append(items)
                    labels, base = [], None
                    for x in cf:
                        if ln(x.tag) == "fieldGroup":
                            base = _int(x.get("base"), -1)
                            for gi in x:
                                if ln(gi.tag) == "groupItems":
                                    labels = [(i.get("v") if i.get("v") is not None else "") for i in gi]
                    group_items.append(labels)
                    group_base.append(base)
                    if cf.get("formula"):
                        calc_fields.append({"name": fname, "formula": cf.get("formula")})
                    for x in cf:
                        if ln(x.tag) == "fieldGroup":
                            for rp in x:
                                if ln(rp.tag) == "rangePr" and rp.get("groupBy"):
                                    grouping.append({"field": fname, "group_by": rp.get("groupBy"), "_i": len(fields) - 1})
                                elif ln(rp.tag) == "rangePr" and rp.get("groupInterval") is not None:
                                    start, end, step = _num(rp.get("startNum")), _num(rp.get("endNum")), _num(rp.get("groupInterval"))
                                    if None not in (start, end, step):
                                        num_grouping.append({"field": fname, "group": {"kind": "numeric", "start": start, "end": end, "interval": step},
                                                             "_i": len(fields) - 1})
        for rel in pkg.rels(cache_path).values():
            if rel[0] == "pivotCacheRecords" and rel[1] and pkg.has(rel[1]):
                records = True

    def fname(i):
        try:
            i = int(i)
        except (TypeError, ValueError):
            return None
        return fields[i] if 0 <= i < len(fields) else None

    opaque = []

    def item_name(fi, k):
        # a date-grouped field's pivot items index its group items, not its raw shared items
        names = group_items[fi] if 0 <= fi < len(group_items) and group_items[fi] else shared[fi] if 0 <= fi < len(shared) else []
        if 0 <= k < len(names):
            return names[k] or "(blank)"
        opaque.append(fi)
        return f"item {k}"

    location = None
    row_f, col_f, page_f, data_f = [], [], [], []
    page_items, item_formulas, pivot_items, pivot_meta, hidden_items = [], [], [], [], []
    top_n = calc_items = 0
    for ch in root:
        n = ln(ch.tag)
        if n == "pivotFields":
            for pf in ch:
                pivot_items.append([(_int(i.get("x"), -1), i.get("h") in ("1", "true")) for i in pf.iter()
                                    if ln(i.tag) == "item" and i.get("t") is None])
                pivot_meta.append((pf.get("axis"), truthy(pf.get("multipleItemSelectionAllowed"))))
        elif n == "calculatedItems":
            for ci in ch:
                if ln(ci.tag) == "calculatedItem" and ci.get("formula"):
                    item_formulas.append({"field": fname(ci.get("field")), "formula": ci.get("formula")})
        if n == "location":
            location = ch.get("ref")
        elif n == "rowFields":
            row_f = [x for x in (fname(f.get("x")) for f in ch) if x]
        elif n == "colFields":
            col_f = [x for x in (fname(f.get("x")) for f in ch) if x]
        elif n == "pageFields":
            page_f = [x for x in (fname(f.get("fld")) for f in ch) if x]
            for pf in ch:
                fi, name = _int(pf.get("fld"), -1), fname(pf.get("fld"))
                if not name:
                    continue
                sel = _int(pf.get("item"), -1)
                label = "(All)"
                if sel >= 0:
                    if 0 <= fi < len(pivot_items) and sel < len(pivot_items[fi]):
                        label = item_name(fi, pivot_items[fi][sel][0])
                    else:
                        opaque.append(fi)
                        label = f"item {sel}"
                    page_items.append({"field": name, "item": label})
                elif 0 <= fi < len(pivot_meta) and pivot_meta[fi][1] and any(h for _, h in pivot_items[fi]):
                    shown = [item_name(fi, k) for k, h in pivot_items[fi] if not h]
                    page_items.append({"field": name, "item": ", ".join(shown), "multi": True,
                                       "hidden": [item_name(fi, k) for k, h in pivot_items[fi] if h]})
                else:
                    page_items.append({"field": name, "item": label})
        elif n == "dataFields":
            for d in ch:
                x14 = next((x.get("pivotShowAs") for x in d.iter() if ln(x.tag) == "dataField" and x.get("pivotShowAs")), None)
                data_f.append({"name": d.get("name"), "field": fname(d.get("fld")), "subtotal": d.get("subtotal") or "sum",
                               "show_data_as": x14 or d.get("showDataAs") or "normal"})
    for x in root.iter():
        t = ln(x.tag)
        if t == "top10":
            top_n += 1
        elif t == "calculatedItem":
            calc_items += 1
    value_filters = []
    for flt in root.iter():
        if ln(flt.tag) != "filter" or flt.get("fld") is None:
            continue
        field, typ = fname(flt.get("fld")), flt.get("type")
        t10 = next((x for x in flt.iter() if ln(x.tag) == "top10"), None)
        kind_of = {"count": "n", "percent": "percent", "sum": "sum"}.get(typ or "")
        if t10 is None or kind_of is None:
            value_filters.append({"field": field, "kind": "unsupported", "type": typ})
            continue
        direction = "bottom" if t10.get("top") in ("0", "false") else "top"
        val = _float(t10.get("val"), None)
        di = _int(flt.get("iMeasureFld"), -1)
        value_filters.append({"field": field, "kind": f"{direction}_{kind_of}", "n": int(val) if val is not None and val == int(val) else val,
                              "measure": data_f[di]["name"] if 0 <= di < len(data_f) else None, "direction": direction})
    filters, slicer_unresolved, non_axis = [], False, False

    def add_filter(via, fi, kept_names, hidden_names):
        filters.append({"via": via, "field": fields[fi], "kept": kept_names[:MAX_FILTER_LABELS], "kept_count": len(kept_names),
                        "hidden": hidden_names[:MAX_FILTER_LABELS]})

    def on_axis(fi):
        return fi < len(pivot_meta) and pivot_meta[fi][0] in ("axisRow", "axisCol", "axisPage")

    for fi in range(min(len(pivot_meta), len(fields))):
        if any(h for _, h in pivot_items[fi]):
            names_h = [item_name(fi, k) for k, h in pivot_items[fi] if h]
            hidden_items.append({"field": fields[fi], "hidden": names_h, "count": len(names_h)})
            if not on_axis(fi):
                add_filter("filter", fi, [item_name(fi, k) for k, h in pivot_items[fi] if not h], names_h)
                non_axis = True
    slicers = _bound_to(slicer_caches_by_pivot(pkg).get(root.get("name"), ()), sh, book)
    tl_index, tl_unparsed = timeline_caches_by_pivot(pkg)
    timelines = [{k: v for k, v in t.items() if k != "tab_id"} for t in _bound_to(tl_index.get(root.get("name"), ()), sh, book)]
    for sl in slicers:
        fi = fields.index(sl["source"]) if sl["source"] in fields else -1
        if fi < 0 or not sl["items"]:
            slicer_unresolved = True
            continue
        kept = [item_name(fi, k) for k, sel in sl["items"] if sel]
        if len(kept) < len(sl["items"]):
            add_filter("slicer", fi, kept, [item_name(fi, k) for k, sel in sl["items"] if not sel])
            non_axis = non_axis or not on_axis(fi)
    for sl in slicers:
        fi = fields.index(sl["source"]) if sl["source"] in fields else -1
        family = {j for j in range(min(len(pivot_items), len(group_base)))
                  if j == fi or (fi >= 0 and (group_base[j] == fi or group_base[fi] == j or (group_base[j] is not None and group_base[j] == group_base[fi])))}
        if fi >= 0 and sl["items"] and all(sel for _, sel in sl["items"]) and any(h for j in family for _, h in pivot_items[j]):
            slicer_unresolved = True
    if opaque and slicers:
        slicer_unresolved = True
    for g in grouping + num_grouping:
        i = g.pop("_i")
        g["grouped_on_axis"] = 0 <= i < len(pivot_meta) and pivot_meta[i][0] in ("axisRow", "axisCol", "axisPage")
    for fld, formula in cache_items:
        entry = {"field": fname(fld) if fld is not None else None, "formula": formula}
        if entry not in item_formulas:
            item_formulas.append(entry)
    calc_items = max(calc_items, len(item_formulas))
    masked = sh["masked"] or source_masked(source, ctx, by_name)
    if masked or not (slicers or timelines or tl_unparsed):
        slicer_status = "none"
    elif (any(x["via"] == "slicer" for x in filters) and not opaque) or any(t["filtering"] for t in timelines):
        slicer_status = "filtering"
    else:
        slicer_status = "unresolved" if slicer_unresolved or tl_unparsed else "all_selected"
    return {"_fields": fields, "name": root.get("name"), "sheet": sh["meta"]["name"], "location": location, "cache_source": source,
            "row_fields": None if masked else row_f, "col_fields": None if masked else col_f,
            "page_fields": None if masked else page_f, "page_items": [] if masked else page_items,
            "calculated_item_formulas": [] if masked else item_formulas, "hidden_items": [] if masked else hidden_items,
            "filters": [] if masked else filters, "filter_on_non_axis_field": False if masked else non_axis,
            "slicer_filter_unresolved": False if masked else slicer_unresolved, "slicer_status": slicer_status,
            "timelines": [] if masked else timelines,
            "data_fields": [] if masked else data_f, "value_filters": [] if masked else value_filters,
            "calculated_fields": [] if masked else calc_fields, "date_grouping": [] if masked else grouping,
            "numeric_grouping": [] if masked else num_grouping,
            "top_n_filters": 0 if masked else top_n, "calculated_items": 0 if masked else calc_items, "refreshed_date": refreshed, "records": records}


def mark_volatile_dependents(regions):
    """Flag every region that reads, directly or through other regions, a RAND-family (X) or TODAY/NOW cell; a RAND dependent routes X."""
    readers = defaultdict(list)
    for g in regions:
        g["volatile_dep"], g["volatile_dep_via"] = False, None
        for d in g["depends_on"]:
            readers[d].append(g["id"])
    by_id = {g["id"]: g for g in regions}
    for fns, rand in ((RAND_FUNCS | {"TODAY", "NOW"}, False), (RAND_FUNCS, True)):
        work = [g["id"] for g in regions if g["an"].volatile & fns]
        seen = set(work)
        via = "direct"
        while work:
            nxt = []
            for rid in work:
                for r in readers[rid]:
                    dep = by_id[r]
                    if rand:
                        if "random_dep" not in dep["flags_map"]:
                            dep["flags_map"]["random_dep"] = []
                            dep["reasons"].append("reads random draws: compare dependents statistically, or pin the draws as data")
                            if ROUTE_ORDER[dep["route"]] < ROUTE_ORDER["X"]:
                                dep["route"] = "X"
                    elif not dep["volatile_dep"]:
                        dep["volatile_dep"], dep["volatile_dep_via"] = True, via
                    if r not in seen:
                        seen.add(r)
                        nxt.append(r)
            work, via = nxt, "transitive"


def mark_hidden_dependents(regions, regions_out, by_name):
    """A visible region reading a masked sheet, or a region that does, cannot be verified: flag it over the region graph."""
    out_by_id = {ro["id"]: ro for ro in regions_out}
    readers = defaultdict(list)
    work = []
    for g in regions:
        ro = out_by_id[g["id"]]
        if ro["hidden_dep"]:
            continue
        if any(by_name[rd["sheet"]]["masked"] for rd in g["reads"] if rd["sheet"] in by_name):
            ro["hidden_dep"], ro["hidden_dep_via"] = True, "direct"
            work.append(g["id"])
        for d in g["depends_on"]:
            readers[d].append(g["id"])
    while work:
        for rid in readers[work.pop()]:
            ro = out_by_id[rid]
            if not ro["hidden_dep"]:
                ro["hidden_dep"], ro["hidden_dep_via"] = True, "transitive"
                work.append(rid)


def mask_pivot_fields(pv, ctx, value_cells):
    """A cache field named by a secret cell (or the cell beside a secret label) must not carry its name into the report."""
    fields = pv.pop("_fields", [])
    src = pv["cache_source"]
    head = None
    if src.get("type") == "worksheet":
        if src.get("ref") and src.get("sheet"):
            loc = parse_loc(src["ref"])
            sname = canon_sheet(ctx, src["sheet"])
            head = (sname, loc[0], loc[1]) if loc and sname else None
        elif src.get("name"):
            t = ctx.tables.get(name_key(src["name"]))
            if t is not None:
                head = (t.sheet, t.r1, t.c1)
            else:
                d = next((d for (_, up), d in ctx.names.items() if up == src["name"].upper() and d["kind"] == "ref" and d["specs"]), None)
                if d is not None:
                    sp = d["specs"][0]
                    head = (sp.sheet, sp.r1, sp.c1)
    if head is None:
        return pv
    hidden = {name for i, name in enumerate(fields) if name and (head[0], head[1], head[2] + i) in value_cells}
    if not hidden:
        return pv

    def walk(o):
        if isinstance(o, str):
            for h in hidden:
                o = o.replace(h, MASK)
            return o
        if isinstance(o, list):
            return [walk(x) for x in o]
        if isinstance(o, dict):
            return {k: walk(v) for k, v in o.items()}
        return o
    return walk(pv)


def source_masked(source, ctx, by_name):
    """A pivot reads its source's values, so it is masked unless the source positively resolves to visible sheets."""
    def visible(sheet_name):
        s = by_name.get(canon_sheet(ctx, sheet_name or "") or "")
        return bool(s and not s["masked"])
    typ = source.get("type")
    if typ == "external":
        return False
    if typ != "worksheet":
        return True
    sheet, nm = source.get("sheet"), source.get("name")
    if sheet is None and nm is None:
        return True
    if sheet is not None and not visible(sheet):
        return True
    if nm is not None:
        t = ctx.tables.get(name_key(nm))
        if t is not None:
            return not visible(t.sheet)
        specs = [sp for (scope, up), d in ctx.names.items() if up == nm.upper() and d["kind"] == "ref" for sp in d["specs"]]
        return not specs or not all(visible(sp.sheet) for sp in specs)
    return False


def parse_loc(loc):
    p = loc.split(":")
    if len(p) > 2:
        return None
    a, b = parse_ref_a1(p[0]), parse_ref_a1(p[-1])
    return (a[0], a[1], b[0], b[1]) if a and b else None


def norm_ref(text):
    """A cell or range address as the parser reads it, rebuilt from its numbers; None when it is anything else."""
    loc = parse_loc(text) if isinstance(text, str) else None
    return rect_a1(*loc) if loc else None


def add_stanza_note(stanza, note):
    """Append one comment line before the closing quotes, re-running the checks render_stanza makes."""
    note = flat(note)
    if not stanza or malloy_unsafe(note) or not stanza.endswith('\n""")'):
        return stanza
    return stanza[:-len('""")')] + "  -- " + note + '\n""")'


def _merge_hits_row(m, r1, c1, c2):
    p = m.split(":")
    a, b = parse_ref_a1(p[0]), parse_ref_a1(p[-1])
    return bool(a and b and a[0] == r1 and b[0] == r1 and b[1] > a[1] and a[1] >= c1 and b[1] <= c2)


TABLE_LOOKUPS = _words("VLOOKUP HLOOKUP LOOKUP XLOOKUP XMATCH MATCH INDEX")


def classify_sheet(sh, sd, inb, stats, sources, pivot_locs, regions):
    nm = sh["meta"]["name"]
    if sh["meta"]["state"] == "veryHidden":
        return "config", "veryHidden"
    if sh.get("config"):
        return "config", "config-named sheet"
    total = len(sd.formulas) + len(sd.consts)
    if sh["rels"] and any(v[0] == "pivotTable" for v in sh["rels"].values()) and total <= 500:
        locs = pivot_locs.get(nm, [])
        loc_cells = 0
        for loc in locs:
            p = parse_loc(loc)
            if p:
                loc_cells += (p[2] - p[0] + 1) * (p[3] - p[1] + 1)
        if loc_cells >= total * 0.5 or total == 0:
            return "report", "sheet is a pivot table"
    if total == 0 and not sh["tables"]:
        return "scratch", "empty"
    if sh["tables"]:
        return "data", "holds an Excel Table"
    fshare = len(sd.formulas) / float(total) if total else 0
    if fshare >= 0.5:
        if stats["cross"] > stats["intra"]:
            return "report", "formula-dominated, aggregates other sheets"
        return "calc", "formula-dominated, intra-sheet chains outweigh cross-sheet reads"
    if stats["regions"] >= 3 and stats["cross"] * 2 >= stats["regions"] and stats["cross"] > stats["intra"] and fshare >= 0.25:
        return "report", "mostly formulas that aggregate other sheets"
    if fshare >= 0.4 and stats["intra"]:
        return "calc", "formula-dominated, formulas read other formulas on the sheet"
    if inb["lookup"] and total <= 200 and fshare < 0.4:
        return "lookup", "small, read as the table of a lookup function"
    if inb["abs"] and total <= 100 and fshare < 0.25:
        return "input", "few constants read by absolute reference"
    if any(s["sheet"] == nm for s in sources) and max((s_rows(s) for s in sources if s["sheet"] == nm), default=0) >= 10:
        return "data", "tall constant range"
    if total < 5:
        return "scratch", "almost empty"
    return "data", "constant cells"


def s_rows(src):
    p = src["ref"].split(":")
    a, b = parse_ref_a1(p[0]), parse_ref_a1(p[-1])
    return (b[0] - a[0] + 1) if a and b else 0


# --------------------------------------------------------------------------
# External data, code attached, oracle, security
# --------------------------------------------------------------------------

CONNECTORS = ["Sql.Database", "Sql.Databases", "PostgreSQL.Database", "MySQL.Database", "Snowflake.Databases",
              "GoogleBigQuery.Database", "Databricks.Catalogs", "Excel.Workbook", "Csv.Document", "Odbc.DataSource",
              "Odbc.Query", "OleDb.DataSource", "Oracle.Database", "Teradata.Database", "SapHana.Database", "Web.Contents",
              "SharePoint.Files", "SharePoint.Tables", "AnalysisServices.Database", "Folder.Files", "Json.Document",
              "Parquet.Document", "Access.Database", "Db2.Database", "Web.Page", "OData.Feed", "Salesforce.Data"]
_CRED_RE = re.compile(r"""(?i)\b(?:password|pwd)\s*=\s*(?:"[^"]+"|'[^']+'|\{[^}]+\}|[^;\s"'{][^;]*)""")
_MASHUP_ROOT = re.compile(r"""\A\s*(?:<\?xml[^>]*\?>\s*)?(?:<!--(?:(?!-->).)*-->\s*)*<(?:[\w.-]+:)?DataMashup[\s>/]""", re.S)


def read_mashup(pkg, name, data):
    out = {"queries": 0, "connectors": {}, "embedded_credential": False, "rejected": [], "records": [], "secrets": {}}
    try:
        root = parse_xml(data)
    except Rejected as e:
        out["rejected"].append(e.reason)
        return out
    try:
        blob = base64.b64decode("".join((root.text or "").split()), validate=False)
        n = int.from_bytes(blob[4:8], "little")
        if n <= 0 or 8 + n > len(blob):
            out["rejected"].append("bad_length")
            return out
        inner = zipfile.ZipFile(io.BytesIO(blob[8:8 + n]))
    except Exception:
        out["rejected"].append("undecodable")
        return out
    infos = inner.infolist()
    if len(infos) > MAX_ENTRIES:
        out["rejected"].append("too_many_entries")
        return out
    if sum(1 for i in infos if i.filename == "Formulas/Section1.m") > 1:
        out["rejected"].append("duplicate_section")
    for info in infos:
        if info.filename == "Formulas/Section1.m":
            cap = pkg.budget()
            try:
                body = bounded_read(inner, info, max(cap, 0))
            except Rejected as e:
                total = e.reason == "too_large" and cap < MAX_PART_BYTES
                pkg.charge(max(cap, 0) if total else e.bytes)
                out["rejected"].append("total_cap" if total else e.reason)
                return out
            pkg.charge(len(body))
            text = body.decode("utf-8-sig", errors="replace")
            shared = re.compile(r'\s*shared\s+(?:#"[^"]+"|[A-Za-z_]\w*)\s*=')
            out["queries"] = sum(1 for line in text.splitlines() if shared.match(line))
            counts = Counter()
            for c in CONNECTORS:
                k = len(re.findall(r"(?<![\w.])" + re.escape(c) + r"\s*\(", text))
                if k:
                    counts[c] = k
            out["connectors"] = dict(counts)
            out["embedded_credential"] = bool(_CRED_RE.search(text))
            m_records(text, name, out["records"], out["secrets"], MAX_CONNECTION_RECORDS)
            break
    return out


CACHE_ROUTE = {
    "data_model": "the workbook's own Power Pivot model: see power-pivot.md",
    "relational": "a pivot cache over a relational database: decide the data question (point Malloy at the source), not power-pivot",
    "olap": "a pivot cache over an OLAP cube: Publisher has no cube connection, so ask for the underlying warehouse tables; decide the data question",
    "external": "a pivot cache over an external source this script cannot resolve: decide the data question",
}


def external_cache_kind(pv, recs, has_model):
    cid = pv["cache_source"].get("connection_id")
    rec = next((r for r in recs if r["id"] == f"conn {cid}"), None) if cid else None
    if rec is not None and rec["status"] == "data_model":
        kind = "data_model"
    elif rec is not None and (rec.get("command_type") == "cube" or rec.get("system") == "SSAS"):
        kind = "olap"
    elif rec is not None and rec["status"] != "workbook":
        kind = "relational"
    elif rec is None and has_model:
        kind = "data_model"
    else:
        kind = "external"
    return {"kind": kind, "connection": rec["id"] if rec else None, "route": CACHE_ROUTE[kind]}


def external_inventory(pkg, rep, ext_pivots):
    ext = {"connections": 0, "query_tables": 0, "connections_save_password": 0, "connections_embedded_credential": 0,
           "connection_parameters": 0, "web_queries": 0, "text_queries": 0, "data_mashup": None,
           "external_links": 0, "pivot_external_caches": 0, "custom_xml_parts": 0, "power_pivot": pkg.has("xl/model/item.data"),
           "connection_list": [], "query_table_conn": {}, "cache_of_database": False}
    recs, secrets = ext["connection_list"], {}
    read_connections_xml(pkg, recs, secrets, MAX_CONNECTION_RECORDS)
    croot = pkg.xml("xl/connections.xml") if pkg.has("xl/connections.xml") else None
    if croot is not None:
        for cn in croot:
            if ln(cn.tag) != "connection":
                continue
            ext["connections"] += 1
            if truthy(cn.get("savePassword")):
                ext["connections_save_password"] += 1
            for x in cn.iter():
                n = ln(x.tag)
                if n == "dbPr" and _CRED_RE.search(x.get("connection") or ""):
                    ext["connections_embedded_credential"] += 1
                elif n == "parameter" and x.get("cell"):
                    ext["connection_parameters"] += 1
                elif n == "webPr":
                    ext["web_queries"] += 1
                elif n == "textPr":
                    ext["text_queries"] += 1
    qt_parts = [n for n in pkg.under("xl/queryTables/") if not n.endswith(".rels") and "/_rels/" not in n]
    ext["query_tables"] = len(qt_parts)
    for n in qt_parts:
        qroot = guarded(pkg, n, lambda: pkg.xml(n))
        if qroot is not None and qroot.get("connectionId"):
            ext["query_table_conn"][n] = qroot.get("connectionId")
    links = [n for n in pkg.under("xl/externalLinks/") if "/_rels/" not in n and n.endswith(".xml")]
    ext["external_links"] = len(links)
    for n in pkg.under("customXml/"):
        if "/_rels/" in n or not n.endswith(".xml"):
            continue
        ext["custom_xml_parts"] += 1
        data = pkg.read(n)
        if data is None or not _MASHUP_ROOT.match(decode_head(data, 4096).lstrip("\ufeff")):
            continue
        dm = guarded(pkg, n, lambda: read_mashup(pkg, n, data), {"queries": 0, "connectors": {}, "embedded_credential": False,
                                                                    "rejected": ["parse_error"], "records": [], "secrets": {}})
        recs.extend(dm.pop("records")[:max(MAX_CONNECTION_RECORDS - len(recs), 0)])
        secrets.update(dm.pop("secrets"))
        if ext["data_mashup"] is None:
            ext["data_mashup"] = dm
        else:
            ext["data_mashup"]["queries"] += dm["queries"]
            ext["data_mashup"]["rejected"].extend(dm["rejected"])
            for k, v in dm["connectors"].items():
                ext["data_mashup"]["connectors"][k] = ext["data_mashup"]["connectors"].get(k, 0) + v
            ext["data_mashup"]["embedded_credential"] = ext["data_mashup"]["embedded_credential"] or dm["embedded_credential"]
    finalize_records(recs, secrets.values())
    for pv in ext_pivots:
        pv["external_cache"] = external_cache_kind(pv, recs, ext["power_pivot"])
    ext["pivot_external_caches"] = sum(1 for pv in ext_pivots if pv["external_cache"]["kind"] == "data_model")
    ext["cache_of_database"] = bool(any(r["status"] not in ("stub", "data_model", "workbook") for r in recs)
                                    or any(pv["external_cache"]["kind"] != "data_model" for pv in ext_pivots) or ext["query_tables"])
    ext["_secrets"] = secrets
    return ext


# --------------------------------------------------------------------------
# Secrets: masking, the env-file format, and secret cells
# --------------------------------------------------------------------------

MASK = "[masked]"
MAX_KNOWN = 500
MAX_LABELS_PER_SHEET = 50
MASKING_NOTE = ("Masking is best effort: it covers cells named by --secret-cell, secret-labelled cells and the cells beside them, "
                "and secret-shaped or high-entropy values. A secret that appears only inside a formula string literal, with no label or "
                "recognisable shape, is not detected. It cannot prove nothing leaked, so rotate any credential that was in this file.")


class CliError(Exception):
    """A usage or environment problem whose message is fixed text, safe to print."""

    def __init__(self, message, code=1):
        Exception.__init__(self, message)
        self.code = code


_LABEL_WORDS = re.compile(r"(?i)\b(?:passwords?|passwort|passwd|passphrases?|passcodes?|pwd|secrets?|tokens?|api[\s.-]?keys?|kennwort|"
                          r"contrase[nñ]as?|mot\s+de\s+passe|senha|wachtwoord|l[oö]senord|пароль|парол\w*)\b")
_LABEL_CJK = re.compile("パスワード|パスコード|密码|密碼|비밀번호|암호")
_PWD_ASSIGN = re.compile(r"(?i)(?:password|pwd|passwd)\s*=\s*(\"[^\"]*\"|'[^']*'|\{[^}]*\}|[^;\s\"']*)")
_URL_CRED = re.compile(r"([A-Za-z][A-Za-z0-9+.-]{0,31}://)[^\s/:@]+:[^\s/@]+@")
_BARE_CRED = re.compile(r"(?<![\w.:/@-])[^\s:/@]{1,64}:[^\s:/@]{3,128}@(?=[\w-]+\.[\w.-]+)")
_TOKEN_SHAPES = [re.compile(p) for p in (
    r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b", r"\bsk-[A-Za-z0-9_-]{16,}", r"\bsk_(?:live|test)_[A-Za-z0-9]{16,}",
    r"\bgh[pousr]_[A-Za-z0-9]{20,}", r"\bgithub_pat_[A-Za-z0-9_]{20,}", r"\bxox[abposr]-[A-Za-z0-9-]{10,}",
    r"\bAIza[0-9A-Za-z_-]{35}\b", r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*",
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----")]
_LABEL_ASSIGN = re.compile(r"(?i)(?:passwords?|passwort|pwd|passwd|secret|token|api[\s_.-]?key|kennwort|contrase[nñ]a|mot de passe|senha)"
                           r"\s*[:=]\s*\S{3,}")
_LABEL_SUB = re.compile(r"(?i)((?:passwords?|passwort|pwd|passwd|secret|token|api[\s_.-]?key|kennwort|contrase[nñ]a|mot de passe|senha)"
                        r"\s*[:=]\s*)(?!\[masked\])([^\s;]{3,})")


def _label_masked(m):
    core = m.group(2).rstrip(")]}>")
    return m.group(1) + (MASK + m.group(2)[len(core):] if len(core) >= 3 else m.group(2))
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_ENTROPY_CHARS = re.compile(r"[A-Za-z0-9+/=_-]+")


_PASS_BARE = re.compile(r"(?i)^\s*pass\s*[:：]?\s*$")


def is_bare_pass(text):
    """`Pass` alone is also pass/fail data, so the scan only believes it beside a password-like text value."""
    return isinstance(text, str) and bool(_PASS_BARE.match(text))


_IDENTIFIER = re.compile(r"^[a-z][a-z0-9]*(?:[_-][a-z0-9]+)+$")


def password_like(s):
    if not isinstance(s, str) or len(s) < 6 or re.search(r"\s", s):
        return False
    if _IDENTIFIER.match(s):
        return False
    classes = sum(bool(re.search(p, s)) for p in ("[a-z]", "[A-Z]", "[0-9]", "[^A-Za-z0-9]"))
    return classes >= 2 and bool(re.search(r"[0-9]|[^A-Za-z0-9]", s))


def is_secret_label(text):
    if not isinstance(text, str):
        return False
    if is_bare_pass(text):
        return True
    t = _CAMEL.sub(" ", text.strip().replace("_", " "))
    return bool(t) and len(t) <= 60 and bool(_LABEL_WORDS.search(t) or _LABEL_CJK.search(t))


def label_value(text):
    """The `value` of a single cell reading `Password: value` (or `=`), else None."""
    m = re.match(r"^\s*([^:=]{1,40}?)\s*[:=]\s*(\S.*)$", text, re.S) if isinstance(text, str) else None
    return m.group(2).strip() if m and is_secret_label(m.group(1)) else None


def is_label_cell(text):
    return not is_bare_pass(text) and (is_secret_label(text) or bool(label_value(text)))


def _high_entropy(s):
    if len(s) < 24 or len(s) > 512 or not _ENTROPY_CHARS.fullmatch(s):
        return False
    if not (re.search(r"[A-Za-z]", s) and re.search(r"\d", s)):
        return False
    counts = Counter(s)
    return -sum(v / len(s) * math.log2(v / len(s)) for v in counts.values()) >= 4.2


def has_secret_shape(s):
    return bool(_PWD_ASSIGN.search(s) or _URL_CRED.search(s) or _BARE_CRED.search(s) or any(rx.search(s) for rx in _TOKEN_SHAPES))


def secret_shaped(v):
    """True for a value that looks like a credential: a known shape, or a long high-entropy token."""
    if not isinstance(v, str) or len(v) < 4:
        return False
    s = v[:4096]
    return has_secret_shape(s) or _high_entropy(s.strip())


def text_has_secret(t):
    """Free text (a comment): a shape, `label: value`, or any high-entropy word."""
    s = t[:20000]
    return has_secret_shape(s) or bool(_LABEL_ASSIGN.search(s)) or any(_high_entropy(w) for w in re.split(r"[\s,;]+", s))


def compile_known(strong=(), weak=(), priority=()):
    """One alternation for every value to scrub. `priority` (found passwords, --secret-cell values) is never truncated;
    `strong` matches anywhere, `weak` (a neighbour's text) only as a whole token; those two keep at most MAX_KNOWN each."""
    prio = {k for k in priority if k and len(k) >= 4}
    st = sorted(prio | set(sorted({k for k in strong if k and len(k) >= 4} - prio)[:MAX_KNOWN]), key=len, reverse=True)
    parts = [re.escape(k) for k in st]
    w = sorted(sorted({k for k in weak if k and len(k) >= 4})[:MAX_KNOWN], key=len, reverse=True)
    if w:
        parts.append(r"(?<![A-Za-z0-9_])(?:" + "|".join(re.escape(k) for k in w) + r")(?![A-Za-z0-9_])")
    return re.compile("|".join(parts)) if parts else None


def mask_text(s, known=(), weak=(), _rx=None, priority=()):
    """Replace secret values inside free text, keeping the surrounding label so the line still reads."""
    if not isinstance(s, str) or len(s) < 4:
        return s
    rx = _rx or (compile_known(known, weak, priority) if known or weak or priority else None)
    if rx is not None:
        s = rx.sub(MASK, s)
    s = _PWD_ASSIGN.sub(lambda m: m.group(0)[:m.start(1) - m.start(0)] + MASK, s)
    s = _LABEL_SUB.sub(_label_masked, s)
    s = _URL_CRED.sub(lambda m: m.group(1) + MASK + "@", s)
    s = _BARE_CRED.sub(MASK + "@", s)
    for t in _TOKEN_SHAPES:
        s = t.sub(MASK, s)
    return s


def scrub(o, known=(), weak=(), priority=(), key_terms=None):
    """Values lose strong and weak known text. Dict keys lose only `priority` values and `key_terms` (secret-shaped cells;
    default: all strong text), so a neighbour's text can never rename a schema key like `status`."""
    return _scrub_with(o, compile_known(known, weak, priority), compile_known(known if key_terms is None else key_terms, (), priority))


def _scrub_with(o, rx, rx_keys):
    if isinstance(o, str):
        return mask_text(o, _rx=rx)
    if isinstance(o, list):
        return [_scrub_with(x, rx, rx_keys) for x in o]
    if isinstance(o, dict):
        return {(mask_text(k, _rx=rx_keys) if isinstance(k, str) else k): _scrub_with(v, rx, rx_keys) for k, v in o.items()}
    return o


_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_ESC = {"\\": "\\", '"': '"', "n": "\n", "r": "\r", "t": "\t"}


def render_env(mapping):
    """One `NAME="value"` per line; the value escapes backslash, quote, newline, CR and tab, so any other text round-trips."""
    lines = []
    for k, v in mapping.items():
        if not _ENV_NAME.match(k):
            raise CliError("a secret variable name is not a valid environment variable name")
        if "\0" in v:
            raise CliError("a secret value contains a NUL byte, which an environment variable cannot hold")
        esc = v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
        lines.append(f'{k}="{esc}"')
    return "\n".join(lines) + ("\n" if lines else "")


def parse_env_file(text):
    """NAME=value lines. Unquoted: literal, trimmed. 'single': literal. "double": \\ \" \\n \\r \\t escapes. `#` starts a comment line."""
    out = {}
    for n, raw in enumerate(text.split("\n"), 1):
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("export "):
            s = s[7:].lstrip()
        m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", s)
        bad = CliError(f"the secrets file is malformed at line {n}")
        if not m:
            raise bad
        name, rest = m.groups()
        if rest.startswith('"'):
            buf, i = [], 1
            while True:
                if i >= len(rest):
                    raise bad
                ch = rest[i]
                if ch == "\\":
                    if i + 1 >= len(rest) or rest[i + 1] not in _ENV_ESC:
                        raise bad
                    buf.append(_ENV_ESC[rest[i + 1]])
                    i += 2
                    continue
                if ch == '"':
                    break
                buf.append(ch)
                i += 1
            tail = rest[i + 1:].strip()
            if tail and not tail.startswith("#"):
                raise bad
            out[name] = "".join(buf)
        elif rest.startswith("'"):
            end = rest.find("'", 1)
            if end < 0 or (rest[end + 1:].strip() and not rest[end + 1:].strip().startswith("#")):
                raise bad
            out[name] = rest[1:end]
        else:
            out[name] = rest.strip()
    return out


SecretCell = namedtuple("SecretCell", "sheet row col var")
_VAR_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def parse_secret_cells(specs):
    out = []
    usage = CliError("--secret-cell must look like 'Sheet!B3=VARIABLE_NAME' (upper-case letters, digits and underscores)")
    for raw in specs or ():
        if isinstance(raw, SecretCell):
            out.append(raw)
            continue
        left, eq, var = raw.rpartition("=")
        sheet, bang, cell = left.rpartition("!")
        m = re.match(r"^\$?([A-Za-z]{1,3})\$?(\d+)$", cell)
        pos = parse_cell_ref(m.group(1) + m.group(2)) if m else None
        if not eq or not bang or not _VAR_RE.match(var) or pos is None or not sheet:
            raise usage
        if len(sheet) >= 2 and sheet[0] == "'" and sheet[-1] == "'":
            sheet = sheet[1:-1].replace("''", "'")
        out.append(SecretCell(sheet, pos[0], pos[1], var))
    return out


def resolve_secret_cells(sds_by_lower, specs, texts):
    """{VAR: (value, provenance)} for the cells --secret-cell names."""
    out = {}
    for sp in specs:
        sd = sds_by_lower.get(sp.sheet.lower())
        if sd is None:
            raise CliError("--secret-cell names a sheet that is not in the workbook")
        pos = (sp.row, sp.col)
        val = texts.get(sd.sidx[pos]) if pos in sd.sidx else sd.inline.get(pos, sd.raw.get(pos))
        if not val:
            raise CliError("the cell named by --secret-cell has no value")
        out[sp.var] = (val, f"cell {sp.sheet}!{num2col(sp.col)}{sp.row}")
    return out


def want_positions(specs):
    want = defaultdict(set)
    for sp in specs:
        want[sp.sheet.lower()].add((sp.row, sp.col))
    return want


def scan_shared_strings(pkg, path, needed):
    """One streaming pass: ({index: text} for needed, label indices, secret-shaped indices, {label index: inline value}, bare `Pass`)."""
    texts, labels, shaped, lvals, bare = {}, set(), set(), {}, set()
    data = pkg.xml_bytes(path) if path else None
    if data is None:
        return texts, labels, shaped, lvals, bare
    i = -1
    try:
        for ev, el in iter_xml(data, events=("end",)):
            if ln(el.tag) != "si":
                continue
            i += 1
            txt = []
            for ch in el:
                n = ln(ch.tag)
                if n == "t":
                    txt.append(ch.text or "")
                elif n == "r":
                    txt.extend((x.text or "") for x in ch if ln(x.tag) == "t")
            txt = "".join(txt)
            if i in needed:
                texts[i] = txt
            if len(txt) >= 3:
                if is_bare_pass(txt):
                    bare.add(i)
                elif is_label_cell(txt):
                    labels.add(i)
                    if label_value(txt):
                        lvals[i] = label_value(txt)
                elif secret_shaped(txt):
                    shaped.add(i)
            el.clear()
    except PARSE_ERRORS:
        pkg.rejected.append({"part": path, "reason": "malformed"})
    return texts, labels, shaped, lvals, bare


class SecretScan:
    def __init__(self):
        self.value_cells = set()
        self.labelled = []
        self.shaped = []
        self.known = set()
        self.weak = set()
        self.priority = set()
        self.shaped_text = set()
        self.label_overflow = 0
        self.values = {}


NEIGHBOUR_REACH = 10
SIDES = (((0, 1), True), ((1, 0), True), ((0, -1), False), ((-1, 0), False))


def _first_present(present, r, c, dr, dc):
    for k in range(1, NEIGHBOUR_REACH + 1):
        pos = (r + dr * k, c + dc * k)
        if pos[0] < 1 or pos[1] < 1:
            return None
        if pos in present:
            return pos
    return None


def _cell_text(sd, pos, shared):
    if pos in sd.sidx:
        return shared.get(sd.sidx[pos])
    if pos in sd.inline:
        return sd.inline[pos]
    fc = sd.formulas.get(pos)
    return fc.sv if fc is not None else None


def scan_secrets(pkg, sheets, shared_path, specs):
    """Find the cells whose text must never be emitted. Every one is masked by position, always (the label cap does not
    switch that off). Text joins the global scrub when it is the secret itself: the inline value, secret-shaped cells,
    --secret-cell cells (never truncated), and the first filled cell right or below a label (whole tokens only, unless it
    is password-like). Left and above neighbours are masked by position only."""
    scan = SecretScan()
    by_lower = {sh["meta"]["name"].lower(): sh["data"] for sh in sheets}
    needed = {by_lower[sp.sheet.lower()].sidx[(sp.row, sp.col)] for sp in specs
              if sp.sheet.lower() in by_lower and (sp.row, sp.col) in by_lower[sp.sheet.lower()].sidx}
    texts, labels, shaped, lvals, bare = scan_shared_strings(pkg, shared_path, needed)
    strong_cells, weak_cells, promotable = set(), set(), set()
    events, cands, presents, rowinfo = {}, [], {}, {}
    for sh in sheets:
        sd, nm = sh["data"], sh["meta"]["name"]
        presents[nm] = present = set(sd.consts) | set(sd.formulas)
        rowinfo[nm] = info = defaultdict(lambda: [0, 0])
        for pos in present:
            info[pos[0]][0] += 1
            info[pos[0]][1] += pos in sd.sidx or pos in sd.inline
        cells = [(pos, ("s", idx)) for pos, idx in sd.sidx.items()] + [(pos, ("i", t)) for pos, t in sd.inline.items()]
        events[nm] = []
        for (r, c), (kind, v) in cells:
            if kind == "s":
                is_bare, is_label, is_shaped = v in bare, v in labels, v in shaped
            else:
                is_bare = is_bare_pass(v)
                is_label = is_label_cell(v)
                is_shaped = not is_label and not is_bare and secret_shaped(v)
            if is_label:
                events[nm].append((r, c, kind, v))
            elif is_bare:
                cands.append((nm, r, c, kind, v))
            elif is_shaped:
                scan.shaped.append((nm, r, c))
                scan.value_cells.add((nm, r, c))
                strong_cells.add((nm, r, c))
    by_name = {sh["meta"]["name"]: sh["data"] for sh in sheets}
    if cands:
        near = {}
        for nm, r, c, kind, v in cands:
            near[(nm, r, c)] = [p for p in (_first_present(presents[nm], r, c, 0, 1), _first_present(presents[nm], r, c, 1, 0)) if p]
        want = {by_name[nm].sidx[p] for (nm, _, _), ps in near.items() for p in ps if p in by_name[nm].sidx}
        nb_texts = read_shared_strings(pkg, shared_path, want) if want else {}
        for nm, r, c, kind, v in cands:
            if any(password_like(_cell_text(by_name[nm], p, nb_texts)) for p in near[(nm, r, c)]):
                events[nm].append((r, c, kind, v))
    for nm, evs in events.items():
        present = presents[nm]
        for n, (r, c, kind, v) in enumerate(sorted(evs)):
            capped = n >= MAX_LABELS_PER_SHEET
            if capped:
                scan.label_overflow += 1
            else:
                scan.labelled.append((nm, r, c))
            inline_val = (lvals.get(v) if kind == "s" else label_value(v))
            if inline_val:
                scan.value_cells.add((nm, r, c))
                if not capped:
                    scan.known.add(inline_val)
            n_in_row, n_text = rowinfo[nm][r]
            header_row = n_in_row >= 3 and n_text == n_in_row
            for d, is_weak in SIDES:
                pos = _first_present(present, r, c, *d)
                if pos:
                    scan.value_cells.add((nm, pos[0], pos[1]))
                    if is_weak:
                        weak_cells.add((nm, pos[0], pos[1]))
                        if d == (1, 0) or not header_row:
                            promotable.add((nm, pos[0], pos[1]))
    scan.values = resolve_secret_cells(by_lower, specs, texts)
    for sp in specs:
        pos = (canon_by_lower(sheets, sp.sheet), sp.row, sp.col)
        scan.value_cells.add(pos)
    scan.priority = {v for v, _ in scan.values.values()}
    shared_wanted = defaultdict(list)
    def place(bucket, key, text):
        """A neighbour of a label in a header row is just another header: it is scrubbed only if it looks like a password."""
        if bucket is not scan.weak:
            bucket.add(text)
        elif key in promotable:
            (scan.known if password_like(text) else bucket).add(text)
        elif password_like(text):
            bucket.add(text)

    for bucket, cell_set in ((scan.shaped_text, strong_cells), (scan.weak, weak_cells)):
        for nm, r, c in cell_set:
            sd = by_name.get(nm)
            if sd is None:
                continue
            if (r, c) in sd.sidx:
                shared_wanted[sd.sidx[(r, c)]].append((bucket, (nm, r, c)))
            else:
                text = _cell_text(sd, (r, c), {})
                if text:
                    place(bucket, (nm, r, c), text)
    for idx, text in read_shared_strings(pkg, shared_path, set(shared_wanted)).items():
        for bucket, key in shared_wanted[idx]:
            place(bucket, key, text)
    scan.known = {k for k in scan.known if len(k) >= 4}
    scan.shaped_text = {k for k in scan.shaped_text if len(k) >= 4}
    scan.weak = {k for k in scan.weak if len(k) >= 4}
    scan.labelled.sort()
    scan.shaped.sort()
    return scan


def canon_by_lower(sheets, name):
    for sh in sheets:
        if sh["meta"]["name"].lower() == name.lower():
            return sh["meta"]["name"]
    return name


def addr(item):
    nm, r, c = item
    return f"{nm}!{num2col(c)}{r}"


def scan_comments(pkg):
    """Count comments (legacy and threaded) holding a secret. The text itself is never kept."""
    n = 0
    parts = [p for p in pkg.under("xl/comments") if p.endswith(".xml")] + [p for p in pkg.under("xl/threadedComments/") if p.endswith(".xml")]
    for part in parts:
        root = guarded(pkg, part, lambda: pkg.xml(part))
        if root is None:
            continue
        for el in root.iter():
            if ln(el.tag) in ("comment", "threadedComment"):
                text = " ".join((x.text or "") for x in el.iter() if ln(x.tag) in ("t", "text"))
                if text_has_secret(text):
                    n += 1
    return n


CONFIG_SHEET_RE = re.compile(r"(?i)^[\s_]*(?:api[\s_-]*keys?|(?:api[\s_-]*)?(?:configuration|config|settings|credentials?|secrets?|connections?|passwords?))")


def is_config_sheet(name):
    """A sheet named for a config word, which may run into a digit, a separator or a capital (Config_Prod, SettingsV2) but not into more lowercase letters."""
    m = CONFIG_SHEET_RE.match(name)
    if not m:
        return False
    rest = name[m.end():]
    return not rest or not rest[0].isalpha() or (rest[0].isupper() and name[m.end() - 1].islower())


# --------------------------------------------------------------------------
# External data: connection strings, Power Query M, and what maps to Publisher
# --------------------------------------------------------------------------

MAX_CONNECTION_RECORDS = 200
MAX_M_ARG_SCAN = 8000

REQUIRED_FIELDS = {"snowflake": ("account", "username", "warehouse"), "databricks": ("host", "path", "defaultCatalog")}
SECRET_FIELD = {"postgres": "password", "mysql": "password", "snowflake": "password", "trino": "password", "databricks": "token",
                "bigquery": None}
SYSTEMS = [  # (key, label, regex over Provider + Driver, Publisher type)
    ("sqlserver", "SQL Server", r"sqloledb|sqlncli|msoledbsql|sql server|sqlsrv|sql native client", None),
    ("oracle", "Oracle", r"oraoledb|msdaora|oracle|oraclient", None),
    ("teradata", "Teradata", r"teradata|tdodbc", None),
    ("hana", "SAP HANA", r"hdbodbc|hana", None),
    ("db2", "DB2", r"db2|ibmda", None),
    ("ssas", "SSAS", r"msolap|analysis ?services", None),
    ("sharepoint", "SharePoint", r"sharepoint|microsoft\.office\.list", None),
    ("postgres", "PostgreSQL", r"postgre|psqlodbc", "postgres"),
    ("mysql", "MySQL", r"mysql|mariadb", "mysql"),
    ("snowflake", "Snowflake", r"snowflake", "snowflake"),
    ("bigquery", "BigQuery", r"bigquery", "bigquery"),
    ("databricks", "Databricks", r"databricks|simba spark", "databricks"),
    ("trino", "Trino", r"trino|presto", "trino"),
]
SYSTEM_LABEL = {k: label for k, label, _, _ in SYSTEMS}
SYSTEM_LABEL.update({"access": "Access", "dsn": "ODBC DSN"})
M_SYSTEM = {"Sql.Database": "sqlserver", "Sql.Databases": "sqlserver", "Oracle.Database": "oracle", "Teradata.Database": "teradata",
            "SapHana.Database": "hana", "Db2.Database": "db2", "AnalysisServices.Database": "ssas", "SharePoint.Files": "sharepoint",
            "SharePoint.Tables": "sharepoint", "Access.Database": "access"}
M_FILE = ("Excel.Workbook", "Csv.Document", "Parquet.Document", "Json.Document")
M_WEB = ("Web.Contents", "Web.Page", "OData.Feed", "Salesforce.Data")
COMMAND_TYPES = {"1": "cube", "2": "sql", "3": "table", "4": "default", "5": "web", "6": "list"}
NR_REASON = {
    "sqlserver": "Publisher has no SQL Server connection type: use a replica or export in a supported warehouse, or DuckDB over an extract",
    "dsn": "an ODBC DSN is machine-local: the driver and server behind it are not in the workbook",
}


_NAME_TAIL = re.compile(r"(?i)((?:passwords?|passwort|pwd|passwd|secret|token|api[\s_.-]?key|kennwort|contrase[nñ]a|mot de passe|senha)\s*[:=]\s*\[masked\]).*", re.S)


def _rec(origin, rid, name, kind):
    """A connection name is free text: everything after a secret label's value goes too, since the value may hold spaces."""
    return {"id": rid, "origin": origin, "name": _NAME_TAIL.sub(r"\1", mask_text(name)), "kind": kind, "status": "nr", "type": None, "system": None, "reasons": [],
            "fields": {}, "secret_field": None, "secret_present": False, "save_password": False, "command": None,
            "command_type": None, "proposed_source": None, "parameters": [], "query": None, "connection_name": None,
            "secret_var": None, "missing": []}


def parse_connstr(s):
    """An OLE DB / ODBC string as {lower-case key: value}; honours "..", '..' and {..} values."""
    out, i, n = {}, 0, len(s)
    while i < n:
        j = s.find("=", i)
        if j < 0:
            break
        seg = s[i:j]
        key = " ".join(seg[seg.rfind(";") + 1:].lower().split())
        i = j + 1
        while i < n and s[i] == " ":
            i += 1
        if i < n and s[i] in "\"'":
            qch, buf = s[i], []
            i += 1
            while i < n:
                if s[i] == qch:
                    if i + 1 < n and s[i + 1] == qch:
                        buf.append(qch)
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(s[i])
                i += 1
            val = "".join(buf)
            k = s.find(";", i)
            i = n if k < 0 else k + 1
        elif i < n and s[i] == "{":
            k, buf = i + 1, []
            while k < n:
                if s[k] == "}":
                    if k + 1 < n and s[k + 1] == "}":
                        buf.append("}")
                        k += 2
                        continue
                    k += 1
                    break
                buf.append(s[k])
                k += 1
            val = "".join(buf)
            m = s.find(";", k)
            i = n if m < 0 else m + 1
        else:
            k = s.find(";", i)
            val = (s[i:] if k < 0 else s[i:k]).strip()
            i = n if k < 0 else k + 1
        if key and key not in out:
            out[key] = val
    return out


def _pick(cs, *keys):
    for k in keys:
        if cs.get(k):
            return cs[k]
    return None


def _put(d, k, v):
    if v is not None and v != "":
        d[k] = v


def split_host_port(v):
    v = re.sub(r"(?i)^(?:tcp|np):", "", re.sub(r"^[A-Za-z][A-Za-z0-9+.-]*://", "", v.strip())).split("/")[0]
    m = re.match(r"^([^,:]+)[,:](\d{1,5})$", v)
    return (m.group(1), int(m.group(2))) if m else (v, None)


def _basename(p):
    return re.split(r"[\\/]+", p.strip().rstrip("\\/"))[-1]


def mask_url(u):
    """host and path stay; userinfo, token-like query values and token-like path segments do not."""
    try:
        p = urllib.parse.urlsplit(u.strip())
        host = p.netloc.rpartition("@")[2]
    except ValueError:
        return MASK
    query = []
    for part in p.query.split("&") if p.query else []:
        k, eq, v = part.partition("=")
        query.append(f"{k}={MASK}" if eq and (re.search(r"(?i)token|key|secret|pass|pwd|auth|sig|credential|sas|code", k) or secret_shaped(v)) else part)
    path = "/".join(MASK if secret_shaped(seg) else seg for seg in p.path.split("/"))
    return f"{p.scheme}://{host}{path}" + ("?" + "&".join(query) if query else "")


def _auth_integrated(cs):
    return (cs.get("integrated security", "").lower() in ("sspi", "true", "yes")
            or cs.get("trusted_connection", cs.get("trusted connection", "")).lower() in ("yes", "true", "sspi"))


def _file_record(rec, name_or_path):
    ext = os.path.splitext(name_or_path)[1].lower()
    rec["status"], rec["type"], rec["system"] = "file", "duckdb", "file"
    _put(rec["fields"], "file", _basename(name_or_path))
    rec["file_kind"] = {".xlsx": "read_xlsx", ".xlsm": "read_xlsx", ".csv": "read_csv", ".tsv": "read_csv", ".txt": "read_csv",
                        ".parquet": "read_parquet", ".json": "read_json"}.get(ext)
    if rec["file_kind"] is None and not name_or_path.endswith(("/", "\\")):
        rec["reasons"].append("DuckDB reads xlsx, csv, parquet and json: convert this file or supply it in one of those forms")


def expand_connstr(cs):
    """MSDASQL and friends nest the real ODBC string in Extended Properties: fold its keys in without overriding."""
    nested = cs.get("extended properties", "")
    if "=" in nested:
        for k, v in parse_connstr(nested).items():
            cs.setdefault(k, v)
    return cs


def classify_connstr(rec, cs):
    """Fill rec from an OLE DB / ODBC key=value dict. Returns the password found (any system); it is never stored on rec."""
    prov, drv = cs.get("provider", ""), cs.get("driver", "")
    ext_props = cs.get("extended properties", "")
    cs = expand_connstr(dict(cs))
    prov, drv = cs.get("provider", ""), cs.get("driver", "")
    blob = (prov + " " + drv).lower()
    pw = _pick(cs, "password", "pwd")
    if "mashup" in blob:
        rec["status"], rec["system"] = "stub", "Power Query"
        return pw
    if "$embedded$" in (cs.get("data source") or "").lower():
        rec["status"], rec["system"] = "data_model", "Power Pivot data model"
        rec["reasons"].append("the workbook's own Power Pivot model (not an external server): see power-pivot.md and skill:malloy-powerbi-review")
        return None
    if re.search(r"microsoft\.(?:ace|jet)\.oledb|microsoft (?:access|excel|text) driver", blob):
        hint = (ext_props + " " + (_pick(cs, "data source", "dbq") or "")).lower()
        path = _pick(cs, "data source", "dbq") or ""
        if re.search(r"excel|\.xlsx?\b|\.xlsm", hint) or re.search(r"text|\.csv|\.txt", hint):
            _file_record(rec, path)
            return pw
        rec["system"] = "Access"
        rec["reasons"].append("Access has no Publisher connection type: export the tables to a supported warehouse or to files for DuckDB")
        return pw
    key = next((k for k, _, rx, _ in SYSTEMS if re.search(rx, blob)), None)
    if key is None:
        key = "dsn" if cs.get("dsn") and not (prov or drv) else None
    host_raw = _pick(cs, "server", "host", "data source", "address", "addr", "network address", "hostname", "servername", "dbcname",
                     "servernode")
    host, port = split_host_port(host_raw) if host_raw else (None, None)
    if _pick(cs, "port") and _pick(cs, "port").isdigit():
        port = int(_pick(cs, "port"))
    db = _pick(cs, "database", "initial catalog", "dbname", "db")
    user = _pick(cs, "uid", "user id", "user", "username", "user name")
    typ = next((t for k, _, _, t in SYSTEMS if k == key), None)
    rec["system"] = SYSTEM_LABEL.get(key) or mask_text((prov or drv or "unknown")[:60])
    f = rec["fields"]
    if typ == "postgres":
        _put(f, "host", host), _put(f, "port", port), _put(f, "databaseName", db), _put(f, "userName", user)
    elif typ == "mysql":
        _put(f, "host", host), _put(f, "port", port), _put(f, "database", db), _put(f, "user", user)
    elif typ == "snowflake":
        _put(f, "account", re.sub(r"(?i)\.snowflakecomputing\.com$", "", host or "") or None), _put(f, "username", user)
        _put(f, "warehouse", cs.get("warehouse")), _put(f, "database", db), _put(f, "schema", cs.get("schema")), _put(f, "role", cs.get("role"))
    elif typ == "databricks":
        _put(f, "host", host), _put(f, "path", cs.get("httppath")), _put(f, "defaultCatalog", cs.get("catalog"))
    elif typ == "trino":
        scheme = re.match(r"(?i)^(https?://)", (host_raw or "").strip())
        _put(f, "server", (scheme.group(1).lower() if scheme else "") + (host or "")), _put(f, "port", port), _put(f, "catalog", cs.get("catalog")), _put(f, "schema", cs.get("schema"))
        _put(f, "user", user)
    elif typ == "bigquery":
        _put(f, "defaultProjectId", _pick(cs, "catalog", "project", "projectid", "billingproject", "defaultproject"))
    else:
        _put(f, "server", host), _put(f, "database", db), _put(f, "user", user)
    integrated = _auth_integrated(cs)
    rec["secret_present"] = bool(pw)
    if typ is None:
        rec["reasons"].append(NR_REASON.get(key) or (f"{rec['system']} has no Publisher connection type." if key else
                                                     "the provider or driver is not one Publisher has a connection type for"))
    if integrated:
        rec["reasons"].append("integrated/Windows authentication cannot be used from Publisher: it needs a database login")
    if typ is not None and not integrated:
        rec["status"], rec["type"] = "maps", typ
        rec["secret_field"] = SECRET_FIELD[typ]
    return pw


def mark_data_model(rec):
    rec["status"], rec["system"], rec["type"], rec["secret_field"] = "data_model", "Power Pivot data model", None, None
    rec["reasons"] = ["the workbook's own Power Pivot model (not an external server): see power-pivot.md and skill:malloy-powerbi-review"]


def apply_command(rec, command, ctype):
    if not command:
        return
    rec["command"] = mask_text(command.strip()[:20000])
    rec["command_type"] = ctype
    if rec["status"] == "maps" and ctype in ("cube", "web", "list"):
        rec["status"], rec["type"], rec["secret_field"] = "nr", None, None
        rec["reasons"].append(f"the command is a {ctype} command, not SQL or a table name: Publisher cannot run it")


_CELL_REF = re.compile(r"(?i)^(?:(?:'(?:[^']|'')+'|[A-Za-z0-9_.]+)!)?\$?[A-Z]{1,3}\$?\d+(?::\$?[A-Z]{1,3}\$?\d+)?$")


def _param_cell(cell):
    """A parameter's cell text goes into a SQL comment inside a Malloy string, so only a reference-shaped, comment- and string-safe value is kept."""
    cell = (cell or "").strip()
    return cell if _CELL_REF.match(cell) and "*/" not in cell and not malloy_unsafe(cell) else "(not a cell reference)"


def bind_placeholders(cmd, params):
    """Replace each `?` outside a string literal with a loud TODO naming the proposed given (or saying it is unbound)."""
    out, i, k, n = [], 0, 0, len(cmd)
    while i < n:
        ch = cmd[i]
        if ch == "'":
            j = i + 1
            while j < n:
                if cmd[j] == "'":
                    if j + 1 < n and cmd[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            out.append(cmd[i:j + 1])
            i = j + 1
            continue
        if ch == "?":
            if k < len(params):
                out.append(f"/* TODO given: {params[k]['given']} (workbook cell {params[k]['cell']}); bind it here */ NULL")
            else:
                out.append("/* TODO: a workbook parameter with no bound cell; supply the value */ NULL")
            k += 1
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _squash(s):
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def finalize_records(recs, secrets=()):
    """Secrets found in the connection strings: a name holding one (any case, any separators) is replaced before it can become a slug."""
    seen = set()
    hidden = [h for h in (_squash(v) for v in secrets) if len(h) >= 4]
    for i, r in enumerate(recs, 1):
        sq = _squash(r["name"])
        if any(h in sq for h in hidden):
            r["name"] = f"connection_{i}"
    for r in recs:
        if r["status"] == "maps":
            base = _slug(r["name"], f"{r['type']}_conn")
            base = "duckdb_source" if base == "duckdb" else base
            name, k = base, 2
            while name in seen:
                name, k = f"{base}_{k}", k + 1
            seen.add(name)
            r["connection_name"] = name
            if r["secret_field"]:
                r["secret_var"] = f"MALLOY_{name.upper()}_{r['secret_field'].upper()}"
            r["missing"] = [k for k in REQUIRED_FIELDS.get(r["type"], ()) if k not in r["fields"]]
            if r["type"] == "trino" and not r["fields"].get("server", "").startswith(("http://", "https://")):
                r["missing"].append("server (needs an http:// or https:// scheme; the password is only sent over https)")
            cmd, ct = r["command"], r["command_type"]
            if cmd and ct == "default":
                ct = "sql" if re.match(r"(?is)\s*(?:select|with)\b", cmd) else "table"
            if cmd and ct == "sql":
                sql = bind_placeholders(cmd, r["parameters"])
                if malloy_unsafe(sql):
                    r["reasons"].append("the SQL contains a triple quote or %{, which would end or interpolate the Malloy string")
                else:
                    # a closing quote right after SQL that ends in " would read as four quotes
                    tail = "\n" if sql.endswith('"') else ""
                    r["proposed_source"] = f'{name}.sql("""{sql}{tail}""")'
            elif cmd and ct == "table" and "'" not in cmd and not malloy_unsafe(cmd):
                r["proposed_source"] = f"{name}.table('{cmd.strip()}')"
        elif r["status"] == "file" and r.get("file_kind") and "'" not in r["fields"].get("file", "'") and not malloy_unsafe(r["fields"]["file"]):
            r["proposed_source"] = f'duckdb.sql("""SELECT * FROM {r["file_kind"]}(\'{r["fields"]["file"]}\')""")'
    return recs


def _slug(s, fallback):
    t = re.sub(r"[^a-z0-9]+", "_", flat(s).lower()).strip("_")[:48]
    if not t:
        return fallback
    return "c_" + t if t[0].isdigit() else t


_M_CALL = re.compile(r"(?<![\w.])(" + "|".join(re.escape(c) for c in CONNECTORS) + r")\s*\(")
_M_SHARED = re.compile(r'(?m)^[ \t]*shared\s+(#"(?:[^"]|"")+"|[A-Za-z_][\w.]*)[ \t]*=')
_M_STR = re.compile(r'"(?:[^"]|"")*"')
_M_ESC = re.compile(r"#\((cr,lf|lf|cr|tab|#)\)")
_M_OPT = re.compile(r'(?:\[|,)\s*(#"[^"]+"|\w+)\s*=\s*("(?:[^"]|"")*")')


def _m_decode(lit):
    inner = lit[1:-1].replace('""', '"')
    return _M_ESC.sub(lambda m: {"cr,lf": "\r\n", "lf": "\n", "cr": "\r", "tab": "\t", "#": "#"}[m.group(1)], inner)


def parse_m_args(text, i):
    """Arguments of the call whose '(' ends just before text[i], as ('str', value) | ('rec', raw) | ('other', raw)."""
    args, depth, start, j = [], 0, i, i
    n = min(len(text), i + MAX_M_ARG_SCAN)

    def flush(a, b):
        raw = text[a:b].strip()
        if not raw:
            return
        if _M_STR.fullmatch(raw):
            args.append(("str", _m_decode(raw)))
        elif raw.startswith("["):
            args.append(("rec", raw))
        else:
            args.append(("other", raw))

    while j < n:
        ch = text[j]
        if ch == '"':
            j += 1
            while j < n:
                if text[j] == '"':
                    if j + 1 < n and text[j + 1] == '"':
                        j += 2
                        continue
                    break
                j += 1
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            if depth == 0:
                flush(start, j)
                return args
            depth -= 1
        elif ch == "," and depth == 0:
            flush(start, j)
            start = j + 1
        j += 1
    return args


def _m_opts(args):
    out = {}
    for kind, raw in args:
        if kind == "rec":
            for k, v in _M_OPT.findall(raw):
                out.setdefault(k.lower().strip('#"'), _m_decode(v))
    return out


def m_records(text, origin, recs, secrets, limit):
    starts = [(m.start(), m.group(1)) for m in _M_SHARED.finditer(text)]
    positions = [s for s, _ in starts]
    seen, per_query = set(), Counter()
    for m in _M_CALL.finditer(text):
        if len(recs) >= limit:
            break
        conn = m.group(1)
        args = parse_m_args(text, m.end())
        key = (conn, tuple(args))
        if key in seen:
            continue
        k = bisect.bisect_right(positions, m.start()) - 1
        qname = starts[k][1].strip('#"').replace('""', '"') if k >= 0 else "(unnamed)"
        strs = [v if kind == "str" else None for kind, v in args]
        opts = _m_opts(args)
        per_query[qname] += 1
        rid = f"query {qname}" + (f" #{per_query[qname]}" if per_query[qname] > 1 else "")
        rec = _rec(origin, rid, qname, "m_query")
        rec["query"] = qname
        pw = opts.get("password") or opts.get("pwd")
        if conn in M_SYSTEM:
            rec["system"] = SYSTEM_LABEL[M_SYSTEM[conn]]
            rec["reasons"].append(NR_REASON.get(M_SYSTEM[conn]) or f"{rec['system']} has no Publisher connection type.")
            if conn.startswith("Sql."):
                _put(rec["fields"], "server", split_host_port(strs[0])[0] if strs and strs[0] else None)
                _put(rec["fields"], "database", strs[1] if len(strs) > 1 else None)
        elif conn in ("PostgreSQL.Database", "MySQL.Database"):
            typ = "postgres" if conn.startswith("Post") else "mysql"
            host, port = split_host_port(strs[0]) if strs and strs[0] else (None, None)
            rec["status"], rec["type"], rec["system"] = "maps", typ, SYSTEM_LABEL[typ]
            rec["secret_field"] = SECRET_FIELD[typ]
            f = rec["fields"]
            _put(f, "host", host), _put(f, "port", port)
            _put(f, "databaseName" if typ == "postgres" else "database", strs[1] if len(strs) > 1 else None)
        elif conn == "Snowflake.Databases":
            rec["status"], rec["type"], rec["system"], rec["secret_field"] = "maps", "snowflake", "Snowflake", "password"
            _put(rec["fields"], "account", re.sub(r"(?i)\.snowflakecomputing\.com$", "", split_host_port(strs[0])[0]) if strs and strs[0] else None)
            _put(rec["fields"], "warehouse", strs[1] if len(strs) > 1 else None)
            _put(rec["fields"], "role", opts.get("role"))
        elif conn == "GoogleBigQuery.Database":
            rec["status"], rec["type"], rec["system"] = "maps", "bigquery", "BigQuery"
            _put(rec["fields"], "defaultProjectId", opts.get("billingproject") or opts.get("project"))
        elif conn == "Databricks.Catalogs":
            rec["status"], rec["type"], rec["system"], rec["secret_field"] = "maps", "databricks", "Databricks", "token"
            _put(rec["fields"], "host", split_host_port(strs[0])[0] if strs and strs[0] else None)
            _put(rec["fields"], "path", strs[1] if len(strs) > 1 else None)
        elif conn in ("Odbc.DataSource", "Odbc.Query", "OleDb.DataSource"):
            cs = parse_connstr(strs[0] or "") if strs else {}
            pw = classify_connstr(rec, cs) or pw
            if conn == "Odbc.Query" and len(strs) > 1:
                opts.setdefault("query", strs[1])
        elif conn in M_FILE:
            fm = re.search(r'File\.Contents\(\s*("(?:[^"]|"")*")', args[0][1]) if args else None
            if not fm:
                continue
            _file_record(rec, _m_decode(fm.group(1)))
        elif conn == "Folder.Files":
            _file_record(rec, (strs[0] or "") + "/" if strs and strs[0] else "")
        elif conn in M_WEB:
            rec["kind"], rec["system"] = "web", "Web"
            rec["reasons"].append("an HTTP source: land it in a supported warehouse or a file first")
            lit = next((v for kind, v in args if kind == "str"), None)
            if lit is None:
                nested = re.search(r'"((?:[^"]|"")*)"', " ".join(raw for _, raw in args))
                lit = nested.group(1).replace('""', '"') if nested else None
            _put(rec["fields"], "url", mask_url(lit) if lit else None)
        else:
            rec["system"] = conn
            rec["reasons"].append("a connector Publisher has no connection type for")
        if opts.get("query"):
            apply_command(rec, opts["query"], "sql")
        rec["secret_present"] = bool(pw)
        if pw:
            secrets[(origin, rid)] = pw
        seen.add(key)
        recs.append(rec)
    return recs


def read_connections_xml(pkg, recs, secrets, limit):
    croot = pkg.xml("xl/connections.xml") if pkg.has("xl/connections.xml") else None
    if croot is None:
        return
    for i, cn in enumerate((c for c in croot if ln(c.tag) == "connection"), 1):
        if len(recs) >= limit:
            return
        cid = cn.get("id") if (cn.get("id") or "").isdigit() else str(i)
        rid, name = f"conn {cid}", cn.get("name") or f"connection {cid}"
        db = web = text = None
        params = []
        for x in cn.iter():
            n = ln(x.tag)
            if n == "dbPr" and db is None:
                db = x
            elif n == "webPr" and web is None:
                web = x
            elif n == "textPr" and text is None:
                text = x
            elif n == "parameter" and x.get("cell"):
                params.append({"name": x.get("name") or f"param{len(params) + 1}", "cell": _param_cell(x.get("cell"))})
        rec = _rec("xl/connections.xml", rid, name, "other")
        rec["save_password"] = truthy(cn.get("savePassword"))
        pw = None
        if db is not None:
            cs = parse_connstr(db.get("connection") or "")
            rec["kind"] = "oledb" if cs.get("provider") else "odbc"
            pw = classify_connstr(rec, cs)
            apply_command(rec, db.get("command"), COMMAND_TYPES.get(db.get("commandType") or "2", "sql"))
            if name == "ThisWorkbookDataModel" or (db.get("commandType") == "1" and (db.get("command") or "").strip() == "Model"):
                mark_data_model(rec)
                pw = None
        elif web is not None:
            rec["kind"], rec["system"] = "web", "Web"
            rec["reasons"].append("an HTTP source: land it in a supported warehouse or a file first")
            _put(rec["fields"], "url", mask_url(web.get("url") or "") if web.get("url") else None)
        elif text is not None or cn.get("sourceFile"):
            rec["kind"] = "text"
            src = (text.get("sourceFile") if text is not None else None) or cn.get("sourceFile") or ""
            if "://" in src:
                rec["kind"], rec["system"] = "web", "Web"
                rec["reasons"].append("an HTTP source: land it in a supported warehouse or a file first")
                _put(rec["fields"], "url", mask_url(src))
            else:
                _file_record(rec, src)
        elif cn.get("type") in ("100", "102") or name.startswith("WorksheetConnection_"):
            rec["status"], rec["system"] = "workbook", "workbook range or table"
        else:
            rec["reasons"].append("a connection type this script does not read")
        for p in params:
            p["given"] = _slug(p["name"], "param").strip("_") or "param"
            p["cell"] = mask_text(p["cell"])
        rec["parameters"] = params
        if pw:
            secrets[("xl/connections.xml", rid)] = pw
        recs.append(rec)


def source_sort_key(r):
    return (0 if r["system"] == "SQL Server" else 1, r["id"])


def code_inventory(pkg, sheets, regions, cell_region, names_out, lambda_count, solver_count, autoopen, ctx):
    inv = {}

    def add(mid, route, n, detail):
        if n <= 0:
            return
        e = inv.setdefault(mid, {"count": 0, "route": route, "unconfirmed": mid in UNCONFIRMED_TELLS, "detail": detail})
        e["count"] += n

    custom = pkg.xml("docProps/custom.xml") if pkg.has("docProps/custom.xml") else None
    if custom is not None:
        for p in custom.iter():
            if ln(p.tag) == "property" and (p.get("name") or "") in ("_AssemblyLocation", "_AssemblyName"):
                add("vsto", "X", 1, "VSTO document customization: the assembly writes cells and the values are static; ask for the source")
                break
    if pkg.code_parts("xl/vbaProject.bin", "vbaproject", "vbaproject"):
        add("vba", "C", 1, "VBA project (never parsed): C for pure UDFs once the user exports the VBA source; macro-driven writes stay in Excel")
    macros = len(pkg.code_parts("xl/macrosheets/", "macrosheet", "macrosheet"))
    add("xlm_macro", "NR", macros + autoopen, "Excel 4.0 macro sheets or Auto_Open: never run; ask the user")
    ax = pkg.code_parts("xl/activeX/", "activex")
    add("activex", "X", len([n for n in ax if n.endswith(".xml")]) or len([n for n in ax if not n.endswith(".xml")]),
        "ActiveX controls: skipped entirely")
    add("ole_embedding", "X", len(pkg.code_parts("xl/embeddings/", "oleobject")), "embedded OLE objects: skipped entirely")
    add("web_extension", "NR", len([n for n in pkg.under("xl/webextensions/") if "/_rels/" not in n]),
        "Office.js add-in (store id only, no code): which add-in?")
    cui = [n for n in pkg.under("customUI/") if n.endswith(".xml") and "/_rels/" not in n]
    add("custom_ui", "flag", len(cui), "ribbon customization with onAction callbacks: flag only")
    fl = 0
    for n in pkg.under("xl/ctrlProps/"):
        d = pkg.read(n)
        if d is not None and b"fmlaLink" in d:
            fl += 1
    add("form_control", "T", fl, "form control bound to a cell: the linked cell is an input, so it becomes a given (fmlaRange is its allowed values)")
    rd = len([n for n in pkg.under("xl/richData/") if "/_rels/" not in n])
    vm = sum(sh["data"].vm_cells for sh in sheets)
    add("rich_data", "NR", max(vm, 1 if rd else 0), "linked data types or images: the base <v> is a placeholder")
    dde = addin = 0
    for n in pkg.under("xl/externalLinks/"):
        if "/_rels/" in n:
            if n.endswith(".rels"):
                for rel in pkg.rels(n[:n.rfind("/_rels/")] + "/" + n.rsplit("/", 1)[-1][:-5]).values():
                    if rel[2] and re.search(r"\.xla[m]?$", rel[1] or "", re.I):
                        addin += 1
            continue
        d = pkg.read(n)
        if d is not None and b"ddeLink" in d:
            dde += len(re.findall(rb"<(?:\w+:)?ddeLink\b", d))
    add("dde", "X", dde, "DDE link: never resolved; skipped entirely")
    add("addin_link", "NR", addin, "external link to an .xla/.xlam add-in: never resolved; which add-in?")
    add("solver", "X", solver_count, "Solver model (hidden solver_* names): the cache holds the last optimum; discrete => scenario grid (C), continuous => stays in Excel")
    add("scenario_manager", "T", sum(sh["data"].scenarios for sh in sheets), "Scenario Manager: a scenario table plus a given choosing the row")
    add("lambda_name", "C", lambda_count, "named LAMBDA: inline at each call site")
    add("cf_expression", "flag", sum(sh["data"].cf_expression for sh in sheets), "conditional formatting by expression: flag")
    add("data_validation", "flag", sum(sh["data"].data_validation for sh in sheets), "data validation: a list is a free enum for a given")
    if "data_validation" in inv:
        entries = [validation_entry(sh["meta"]["name"], v) for sh in sheets if not sh["masked"] for v in sh["data"].validations][:MAX_VALIDATIONS]
        if entries:
            inv["data_validation"]["entries"] = entries
    cell_mech = defaultdict(int)
    py_code = []
    unresolved_cells = set()
    mech_routes = {"vba_udf": "NR", "xll_udf": "NR", "unresolved_function": "NR", "addin_link": "NR", "python_in_excel": "C", "vendor_feed": "X",
                   "cube": "NR", "rich_data": "NR", "webservice": "X", "xlm_macro": "NR", "dde": "X"}
    detail = {"vba_udf": "user-defined functions that are probably VBA (vbaProject.bin is present, not read): export the VBA source",
              "xll_udf": "add-in functions (XLL / Excel-DNA / COM): which add-in? translate only when its logic is supplied",
              "unresolved_function": "functions Excel could not resolve: the cache is not an oracle",
              "addin_link": "calls into an .xla/.xlam add-in through an external link",
              "python_in_excel": "Python in Excel: the code (PY( formulas and the workbook script parts) is pandas; hand-translate it",
              "vendor_feed": "live market/planning feeds: the cache is a dated snapshot",
              "cube": "CUBE* functions: route with Power Pivot or the OLAP connection",
              "rich_data": "linked data types or images",
              "webservice": "WEBSERVICE/FILTERXML: stays in Excel, never fetched",
              "xlm_macro": "Excel 4.0 macro functions",
              "dde": "DDE link: never resolved; skipped entirely"}
    for g in regions:
        an = g["an"]
        for m in an.mechs:
            cell_mech[m] += len(g["cells"])
        if "unresolved_function" in an.mechs:
            unresolved_cells.update((g["sheet"],) + p for p in g["cells"])
        if an.py_code and not sheets_by(sheets, g["sheet"])["masked"]:
            py_code.extend(an.py_code)
    for sh in sheets:
        for pos, fc in sh["data"].formulas.items():
            if fc.t == "e" and fc.v == "#NAME?":
                unresolved_cells.add((sh["meta"]["name"],) + pos)
    if unresolved_cells:
        cell_mech["unresolved_function"] = len(unresolved_cells)
    for m, n in cell_mech.items():
        add(m, mech_routes[m], n, detail[m])
    for part in ("xl/pythonScripts.xml", "xl/python.xml"):
        root = pkg.xml(part) if pkg.has(part) else None
        if root is not None:
            py_code.extend((el.text or "") for el in root.iter() if ln(el.tag) == "code" and (el.text or "").strip())
    if py_code and "python_in_excel" not in inv:
        add("python_in_excel", "C", len(py_code), detail["python_in_excel"])
    py_code = [c if len(c) <= MAX_PY_CODE_CHARS else c[:MAX_PY_CODE_CHARS] + "\n# [truncated]" for c in py_code[:MAX_PY_SCRIPTS]]
    if "python_in_excel" in inv:
        inv["python_in_excel"]["code"] = py_code
    return inv


def sheets_by(sheets, name):
    for s in sheets:
        if s["meta"]["name"] == name:
            return s
    return None


def oracle_gate(pkg, book, sheets, regions, ext, code, autofilter_sheets, date_system=None):
    tells = []

    def tell(tid, sev, count, detail):
        tells.append({"tell": tid, "severity": sev, "count": count, "detail": detail, "unconfirmed": False})

    calc = book.calc
    if truthy(calc.get("fullCalcOnLoad")):
        tell("full_calc_on_load", "untrusted", 1, "fullCalcOnLoad=1: the writer expects Excel to recompute, so the cached values are not Excel's")
    if not calc.get("calcId") and any(sh["data"].formulas for sh in sheets):
        tell("missing_calc_id", "untrusted", 1, "no calcPr calcId: Excel always writes one, so a library wrote this file; ask for a recalculated save")
    no_v = sum(1 for sh in sheets for fc in sh["data"].formulas.values() if not fc.has_v)
    if no_v:
        tell("formula_without_v", "untrusted", no_v, f"{no_v} formula cells have no cached value (the file was saved without calculating): no cache oracle; follow the untrusted-cache procedure")
    if calc.get("calcMode") == "manual":
        tell("manual_calc", "untrusted", 1, "calcMode=manual: cached values may be stale; ask for a recalculated save")
    if calc.get("calcOnSave") in ("0", "false"):
        tell("calc_on_save_off", "untrusted", 1, "calcOnSave=0: cached values may predate the last edit; ask for a recalculated save")
    vol = set()
    for g in regions:
        vol |= g["an"].volatile
    if vol:
        pinned = sorted(vol & {"TODAY", "NOW"})
        tail = []
        if pinned:
            tail.append("/".join(pinned) + " pin to docProps/core.xml dcterms:modified")
        if vol - {"TODAY", "NOW"}:
            tail.append("the cache holds one evaluation of " + "/".join(sorted(vol - {"TODAY", "NOW"})) + ": a model cannot reproduce it value for value")
        tell("volatile", "caveat", sum(len(g["cells"]) for g in regions if g["an"].volatile),
             "volatile functions: " + ", ".join(sorted(vol)) + "; " + "; ".join(tail))
    types = Counter()
    errs = Counter()
    err_cells, err_totals = {}, {}
    for sh in sheets:
        for pos, fc in sorted(sh["data"].formulas.items()):
            if not fc.has_v:
                continue
            if fc.t == "e":
                types["error"] += 1
                kind = fc.v or "#ERR"
                errs[kind] += 1
                name = sh["meta"]["name"]
                err_totals.setdefault(kind, {})
                err_totals[kind][name] = err_totals[kind].get(name, 0) + 1
                if not sh["masked"]:
                    got = err_cells.setdefault(kind, {}).setdefault(name, [])
                    if len(got) < MAX_ERROR_REFS:
                        got.append(f"{num2col(pos[1])}{pos[0]}")
            elif fc.t == "b":
                types["boolean"] += 1
            elif fc.t in ("str", "inlineStr", "s"):
                types["string"] += 1
            else:
                types["number"] += 1
    if types["error"]:
        tell("cached_errors", "caveat", types["error"], "cached errors map to null only with the IFERROR routing stated; #N/A is a legitimate no-match oracle")
    af_sheets = [sh for sh in sheets if sh["data"].autofilter is not None or sh.get("table_filters")]
    if af_sheets:
        active = [f"{sh['meta']['name']}!{'table ' + t['table'] + ' ' if 'table' in t else ''}{f['column']} ({f['type']})"
                  for sh in af_sheets if not sh["masked"]
                  for t in sh.get("table_filters", []) + [{"filters": sheet_filters(sh["data"])}] for f in t["filters"]]
        tell("autofilter", "caveat", len(af_sheets), "autoFilter is UI state: never bake it into the model"
             + (f"; filtered: {', '.join(active[:10])}" if active else ""))
    if pkg.under("xl/slicers/"):
        tell("slicers", "caveat", len([n for n in pkg.under("xl/slicers/") if "/_rels/" not in n]), "slicers are pivot UI state, bound to a pivot by sheet and name; each pivot's slicer_status says whether its slicers filter it")
    if pkg.under("xl/timelines/"):
        tell("timelines", "caveat", len([n for n in pkg.under("xl/timelines/") if "/_rels/" not in n]), "timelines are pivot UI state; a pivot's `timelines` entry with filtering true is a date `where:` Excel applied (see slicer_status)")
    if date_system and date_system["serial_60"]:
        tell("date_serial_60", "caveat", len(date_system["serial_60"]),
             "date-styled serial 60 is the phantom 1900-02-29 Excel invented: " + ", ".join(date_system["serial_60"][:5]) +
             "; no real calendar date has it, so a converter cannot map it back")
    sub = sum(len(g["cells"]) for g in regions if "subtotal_ui_state" in g["flags_map"])
    if sub:
        tell("subtotal_ui_state", "caveat", sub, "SUBTOTAL/AGGREGATE over filtered or hidden rows reflects UI state")
    if any(True for g in regions if g["an"].flags.get("external_ref") is not None):
        tell("external_link_snapshot", "caveat", sum(len(g["cells"]) for g in regions if "external_ref" in g["an"].flags),
             "cells read through an external link are snapshots: flagged, not compared")
    piv = len([n for n in pkg.under("xl/pivotTables/") if n.endswith(".xml") and "/_rels/" not in n])
    if piv:
        tell("pivot_snapshot", "caveat", piv, "pivot numbers are as of refreshedDate; pivotCacheRecords is a snapshot: parity names which one it compared")
    if ext["connections"] or ext["query_tables"] or ext["data_mashup"] or ext["pivot_external_caches"]:
        tell("external_data", "caveat", ext["connections"] + ext["query_tables"] + (1 if ext["data_mashup"] else 0) + ext["pivot_external_caches"],
             "the sheet is a cache of an external source: decide the data question")
    snap = sum(code[m]["count"] for m in ("vendor_feed", "xll_udf", "vba_udf", "unresolved_function", "addin_link", "webservice", "python_in_excel") if m in code)
    if snap:
        tell("code_snapshot", "caveat", snap, "values produced by code outside the file are a snapshot as of save, not an oracle")
    status = "trusted"
    if any(t["severity"] == "untrusted" for t in tells):
        status = "untrusted"
    elif tells:
        status = "caveats"
    nform = sum(len(sh["data"].formulas) for sh in sheets)
    excel_saved = bool(calc.get("calcId")) and nform > 0 and not no_v and status != "untrusted"
    return {"status": status, "tells": tells, "cached": {k: types[k] for k in ("number", "string", "boolean", "error") if types[k]},
            "cached_errors": dict(errs), "cached_error_cells": err_cells, "cached_error_cell_totals": err_totals, "calc_id": calc.get("calcId"), "formula_cells": nform, "excel_saved": excel_saved}


SEC_DESC = {
    "xlm_macros": "Excel 4.0 (XLM) macros: never run; ask the user",
    "vba_project": "vbaProject.bin: never open this workbook to recalculate it without telling the user",
    "veryhidden_sheet": "veryHidden sheet(s): invisible in the UI; values are withheld from this report",
    "activex": "ActiveX controls: skip entirely",
    "ole_embedding": "embedded OLE objects: skip entirely",
    "dde": "DDE link: never resolved",
    "external_links": "external workbook links: never resolved",
    "python_in_excel": "Python in Excel formulas: code that runs on a service",
    "web_fetch_formula": "WEBSERVICE/FILTERXML formulas: never fetched",
    "custom_ui": "ribbon customization (customUI)",
    "web_extension": "Office.js web add-in",
    "connections_save_password": "connections that save their password (savePassword is set)",
    "connections_embedded_credential": "connection strings with an embedded Password or Pwd key: a secret ships inside this file, so rotate it",
    "data_mashup": "Power Query (DataMashup) present: its connectors and SQL are summarized under External data; its other text is never emitted",
    "sensitivity_label": "sensitivity label (MSIP_Label) present: this file never enters the corpus or a PR, nor does its JSON, and neither may leave the machine",
    "secret_labelled_cell": "cells labelled password/secret/token/api key (the cell beside or below is masked in this report)",
    "secret_shaped_value": "cells holding a secret-shaped value (masked in this report)",
    "secret_in_defined_name": "defined-name constants that look like secrets (never emitted)",
    "secret_in_comment": "comments or threaded comments that hold a secret (never emitted)",
    "secret_label_cap": f"secret-labelled cells beyond the first {MAX_LABELS_PER_SHEET} on a sheet are still masked by position but not scrubbed by text: review that sheet by hand",
    "secret_scrub_cap": f"more than {MAX_KNOWN} secret-shaped or neighbour values: the rest are masked by position but not scrubbed by text (found passwords and --secret-cell values are never dropped)",
    "xml_dtd_rejected": "XML part with a DOCTYPE/ENTITY declaration: rejected, not parsed",
    "part_too_large": "zip part over the size cap: rejected",
    "part_total_cap": "total decompressed size cap hit: later parts were not read",
    "bad_member": "zip member unreadable or with a forged size header: rejected",
    "unsafe_member_name": "zip member names with `..` or an absolute path: ignored, never extracted",
    "malformed_xml": "XML part that does not parse: skipped",
}


def build_security(rep, code, pkg, sheets, ext, names_out, extra=None):
    out = []

    def add(fid, count, unconf=False, detail=""):
        if count:
            out.append({"flag": fid, "count": count, "detail": SEC_DESC[fid] + (f": {detail}" if detail else ""), "unconfirmed": unconf})

    c = lambda k: code.get(k, {}).get("count", 0)
    add("xlm_macros", c("xlm_macro"))
    add("vba_project", c("vba"))
    add("veryhidden_sheet", sum(1 for sh in sheets if sh["meta"]["state"] == "veryHidden"))
    add("activex", c("activex"))
    add("ole_embedding", c("ole_embedding"))
    add("dde", c("dde"), True)
    add("external_links", ext.get("external_links", 0))
    add("python_in_excel", c("python_in_excel"), True)
    add("web_fetch_formula", c("webservice"))
    add("custom_ui", c("custom_ui"), True)
    add("web_extension", c("web_extension"), True)
    add("connections_save_password", ext.get("connections_save_password", 0))
    cred = ext.get("connections_embedded_credential", 0) + (1 if (ext.get("data_mashup") or {}).get("embedded_credential") else 0)
    add("connections_embedded_credential", cred)
    add("data_mashup", 1 if ext.get("data_mashup") else 0)
    custom = pkg.xml("docProps/custom.xml") if pkg.has("docProps/custom.xml") else None
    if custom is not None:
        add("sensitivity_label", sum(1 for p in custom.iter() if ln(p.tag) == "property" and (p.get("name") or "").startswith("MSIP_Label_")))
        if out and out[-1]["flag"] == "sensitivity_label":
            out.insert(0, out.pop())
    if extra:
        add("secret_labelled_cell", len(extra["labelled"]), detail=", ".join(addr(x) for x in extra["labelled"][:20]))
        add("secret_shaped_value", len(extra["shaped"]), detail=", ".join(addr(x) for x in extra["shaped"][:20]))
        add("secret_in_defined_name", len(extra["names"]), detail=", ".join(extra["names"][:20]))
        add("secret_in_comment", extra["comments"])
        add("secret_label_cap", extra.get("label_overflow", 0))
        add("secret_scrub_cap", extra.get("scrub_truncated", 0))
    rej = Counter()
    for r in pkg.rejected:
        rej[{"dtd": "xml_dtd_rejected", "too_large": "part_too_large", "total_cap": "part_total_cap",
             "bad_member": "bad_member", "malformed": "malformed_xml", "parse_error": "malformed_xml"}.get(r["reason"], "bad_member")] += 1
    for k, n in rej.items():
        add(k, n)
    add("unsafe_member_name", pkg.unsafe)
    return out


def not_read(pkg, rep):
    nr = ["sharedStrings text beyond sources' header rows and the case-variant counts (cell values are never emitted)",
          "styles.xml beyond which cell styles are dates (used only to flag date serials before 61); text dates are not distinguished",
          "drawings and charts",
          "comments and threadedComments text (scanned for secrets, never emitted)",
          "xl/calcChain.xml (the dependency graph is built from the formulas)",
          "xl/metadata.xml (spills are found by the cm attribute)",
          "sheet extLst (x14 conditional formats, sparklines)"]
    if pkg.has("xl/vbaProject.bin"):
        nr.append("xl/vbaProject.bin (detected, never parsed: export the VBA source)")
    if pkg.has("xl/model/item.data"):
        nr.append("xl/model/item.data (Power Pivot VertiPaq backup: detected, never parsed)")
    if pkg.under("xl/embeddings/"):
        nr.append("xl/embeddings/ (skipped entirely)")
    if pkg.under("xl/externalLinks/"):
        nr.append("external link targets (never resolved)")
    if pkg.under("xl/pivotCache/"):
        nr.append("pivotCacheRecords (presence only: a snapshot, never compared here)")
    if rep.get("external", {}).get("custom_xml_parts"):
        nr.append("customXml parts other than a DataMashup (counted, never read)")
    if pkg.under("xl/connections.xml"):
        nr.append("connections.xml: parsed into the fields Publisher needs; the raw connection string and any password are never emitted")
    for r in pkg.rejected:
        nr.append(f"{r['part']} (rejected: {r['reason']})")
    if pkg.unsafe:
        nr.append(f"{pkg.unsafe} zip member(s) with unsafe names")
    return nr


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def _scrub_lines(v):
    """Keeps the line breaks of a block that is rendered inside a code fence; each line is still one safe line."""
    return "\n".join(flat(x) for x in v.splitlines()) if isinstance(v, str) else v


def _scrub(v):
    """Workbook text becomes one line with no table breaks, so a name cannot forge a report section."""
    if isinstance(v, str):
        return md(v)
    if isinstance(v, list):
        return [_scrub(x) for x in v]
    if isinstance(v, dict):
        return {md(k) if isinstance(k, str) else k: (x if k == "stanza" else _scrub_lines(x) if k == "proposed_source" else _scrub(x))
                for k, x in v.items()}
    return v


def render_external(rep, out):
    ext = rep["external"]
    models = [r for r in ext.get("connection_list") or [] if r["status"] == "data_model"]
    recs = [r for r in ext.get("connection_list") or [] if r["status"] not in ("data_model", "workbook")]
    internal = sum(1 for r in ext.get("connection_list") or [] if r["status"] == "workbook")
    for r in models:
        out.append(f"- {r['id']} \"{r['name']}\" is the workbook's own Power Pivot data model, not an external connection: "
                   "see power-pivot.md (skill:malloy-powerbi-review).")
    if internal:
        out.append(f"- {internal} workbook-internal connection(s) (worksheet ranges and tables that feed the data model or Power Query): not external data.")
    if models or internal:
        out.append("")
    if not recs and not ext.get("cache_of_database"):
        return
    out.append("## External data")
    out.append("")
    out.append("This workbook is a cache of a database. **Decide the data question**: point Malloy at the source below "
               "instead of lifting the cached sheets, which are only as current as the last refresh.")
    for p_ in rep.get("pivots", ()):
        ec = p_.get("external_cache")
        if ec and ec["kind"] != "data_model":
            out.append(f"- pivot {p_['name']} on {p_['sheet']}: {ec['route']}.")
    cached = [f"{s['name']} ({', '.join(s['cache_of'])})" for s in rep["sheets"] if s.get("cache_of")]
    if cached:
        out.append("")
        out.append("Sheets that are a cache of a database: " + "; ".join(cached))
    out.append("")
    order = {"nr": 0, "maps": 1, "file": 2, "stub": 3}
    stubs = 0
    for r in sorted(recs, key=lambda r: (order.get(r["status"], 4), source_sort_key(r))):
        label = f"{r['id']} \"{r['name']}\""
        fields = ", ".join(f"{k}={v}" for k, v in r["fields"].items())
        if r["status"] == "stub":
            stubs += 1
            continue
        if r["status"] == "nr":
            out.append(f"- NR {label} ({r['system']}): " + "; ".join(r["reasons"]) + (f" [{fields}]" if fields else ""))
        elif r["status"] == "maps":
            line = f"- {label} -> {r['type']} connection `{r['connection_name']}`" + (f" ({fields})" if fields else "")
            if r["secret_var"]:
                line += f"; credential `${{{r['secret_var']}}}`"
            out.append(line)
        else:
            out.append(f"- {label} -> duckdb ({fields or 'no file named'})" + (" " + "; ".join(r["reasons"]) if r["reasons"] else ""))
        if r["proposed_source"]:
            fence = "`" * max(3, max((len(m) for m in re.findall(r"`+", r["proposed_source"])), default=0) + 1)
            out.append("  - proposed source:")
            out.extend([fence, r["proposed_source"], fence])
        for p in r["parameters"]:
            out.append(f"  - proposed `given: {p['given']}` bound to {p['cell']}")
        if r["status"] == "maps" and r["reasons"]:
            out.append("  - " + "; ".join(r["reasons"]))
        if r["status"] == "maps" and r.get("missing"):
            out.append("  - required by Publisher but not in the workbook: " + ", ".join(r["missing"]))
    if stubs:
        out.append(f"- {stubs} Power Query connection stub(s): the real sources are in the DataMashup queries")
    for r in recs:
        if r["secret_present"]:
            out.append(f"- ROTATE {r['id']} \"{r['name']}\": a password ships inside this file, so rotate it regardless of what happens next")
    out.append("")


def render_text(rep):
    rep = _scrub(rep)
    out = [f"# {rep['workbook']}", ""]
    out.append("## Security flags")
    out.append("")
    if rep["status"] != "ok":
        out.append(f"- unreadable ({rep['error']['kind']}): {rep['error']['message']}")
    for s in rep["security"]:
        out.append(f"- {s['flag']} x{s['count']}: {s['detail']}{' (unconfirmed tell)' if s['unconfirmed'] else ''}")
    if not rep["security"] and rep["status"] == "ok":
        out.append("- none found")
    out.append("")
    out.append(rep.get("masking_note") or MASKING_NOTE)
    out.append("")
    if rep["status"] != "ok":
        p = rep["package"]
        out.append(f"Read {p['parts_read']} part(s), {p['bytes_read']} bytes; rejected {len(p['rejected'])}.")
        return "\n".join(out)
    render_external(rep, out)
    props = rep["workbook_props"]
    out.append("## Sheets")
    out.append("")
    out.append(f"Date system: {'1904' if props['date1904'] else '1900 (serial 60 is the phantom 1900-02-29)'}. "
               f"Formula cells and constants are counted separately; values from hidden and veryHidden sheets are never shown.")
    ds = rep.get("date_system") or {}
    if props["date1904"]:
        out.append("- WARNING: the 1904 date system: DuckDB reads dates with the 1900 base, so a typed `read_xlsx` of a date column is four years and a day off. "
                   "The stanzas read date columns as serials and convert them from 1904-01-01; any read you write yourself must do the same.")
    if ds.get("serial_60"):
        out.append(f"- date-styled serial 60 (the phantom 1900-02-29) at {', '.join(ds['serial_60'][:10])}.")
    if ds.get("pre_61_dates"):
        out.append(f"- {ds['pre_61_dates']} date-styled constant(s) before serial 61: Excel's 1900 leap-year bug makes serials before 61 "
                   "one day off from the proleptic Gregorian calendar DuckDB uses.")
    out.append("")
    out.append("| Sheet | State | Class | Formula cells | Constants | Hidden rows | Merges | Autofilter |")
    out.append("|---|---|---|--:|--:|--:|--:|---|")
    for s in rep["sheets"]:
        out.append(f"| {s['name']} | {s['state']} | {s['class']} | {s['formula_cells']} | {s['constant_cells']} | {s['hidden_rows']} | {s['merge_cells']} | {s['autofilter'] or ', '.join(t['ref'] for t in s['table_autofilters'])} |")
    out.append("")
    filtered = [(s["name"], None, s["autofilter_filters"]) for s in rep["sheets"] if s["autofilter_filters"]] + \
               [(s["name"], t["table"], t["filters"]) for s in rep["sheets"] for t in s["table_autofilters"]]
    if filtered:
        out.append("Autofilters (UI state: never bake a filter into the model):")
        for sheet, table, fs in filtered:
            out.append(f"- {sheet}{' table ' + table if table else ''}: " + "; ".join(
                f"column {f['column']} {f['type']}" + (f" {f['values']}" if f["values"] else "") for f in fs))
        out.append("")
    if rep["tables"]:
        out.append("")
        out.append("Excel Tables: " + "; ".join(f"{t['name']} {t['ref']} (data {t['data_ref']})" for t in rep["tables"]))
    if rep["pivots"]:
        out.append("")
        masked_pivot_noted = False
        for p in rep["pivots"]:
            rf = p.get("refresh") or {}
            when = rf.get("refreshed") or p["refreshed_date"]
            out.append(f"- pivot {p['name']} on {p['sheet']} at {p['location']}: source {json.dumps(p['cache_source'])}, refreshed {when}, "
                       f"{len(p['data_fields'])} value field(s), {len(p['calculated_fields'])} calculated field(s)")
            if p["row_fields"] is None and not masked_pivot_noted:
                masked_pivot_noted = True
                out.append("  - structure masked; ask the user to unhide or share (a pivot on a hidden sheet or over a hidden source)")
            if (rf.get("days_before_save") or 0) > 0:
                out.append(f"  - pivot predates last save by {rf['days_before_save']} day(s) (refreshed {rf['refreshed']}, saved {rf['modified']}): "
                           "its numbers are as of the refresh, not the save")
            elif (rf.get("days_before_save") or 0) < 0:
                out.append(f"  - file saved {-rf['days_before_save']} day(s) before the pivot refreshed ({rf['modified']} against {rf['refreshed']}): "
                           "a clock or time-zone difference, or a date that was not updated; do not read it as stale")
            elif rf.get("days_before_save") == 0:
                out.append(f"  - refreshed on the day of the last save ({rf['modified']})")
            for gp in p.get("date_grouping", ()):
                out.append(f"  - date grouping on {gp['field']}: by {gp['group_by']}, " +
                           ("placed on an axis" if gp["grouped_on_axis"] else "not placed on an axis (the cache groups it, the pivot does not show it)"))
            for gp in p.get("numeric_grouping", ()):
                g_ = gp["group"]
                out.append(f"  - numeric grouping on {gp['field']}: {g_['start']:g} to {g_['end']:g} in steps of {g_['interval']:g}, " +
                           ("placed on an axis" if gp["grouped_on_axis"] else "not placed on an axis (the cache groups it, the pivot does not show it)"))
            if p.get("page_items"):
                out.append("  - page filter: " + ", ".join(f"{x['field']} = {x['item']}" for x in p["page_items"]))
            for x in p.get("hidden_items", ()):
                out.append(f"  - hidden items in {x['field']} ({x['count']}): {', '.join(x['hidden'][:20])}")
            for x in p.get("filters", ()):
                if x["via"] == "slicer":
                    out.append(f"  - slicer on {x['field']} keeps {x['kept_count']}: {', '.join(x['kept'][:20])}")
            vfs = p.get("value_filters") or []
            for vf in vfs:
                if vf["kind"] == "unsupported":
                    out.append(f"  - value filter on {vf['field']} of type {vf['type']} is not translated: read it in Excel")
                else:
                    order = "asc" if vf["direction"] == "bottom" else "desc"
                    out.append(f"  - value filter on {vf['field']}: {vf['kind']} {vf['n']} by {vf['measure']}; Malloy: "
                               f"`order_by: {vf['measure']} {order}` + `limit: {vf['n']}`" +
                               ("" if vf["kind"].endswith("_n") else " (N is a percent or a sum here: work the cut out from the full ranking)"))
            if vfs:
                out.append("  - the Grand Total covers only the visible items of a value-filtered field, not every item (the tie at the cut is undefined)")
            if p.get("filter_on_non_axis_field"):
                out.append("  - filtered on a field that is on no axis: carry it into the Malloy view as a where: or a given, or the totals will not match")
            for t in p.get("timelines", ()):
                out.append(f"  - timeline on {t['field']}: {t['start']} to {t['end']}" +
                           (f" (narrower than {t['bounds_start']} to {t['bounds_end']})" if t["filtering"] else " (the full range)"))
            if p.get("slicer_status", "none") != "none":
                out.append(f"  - slicer status: {p['slicer_status']}")
            if p.get("slicer_filter_unresolved"):
                out.append("  - a slicer targets this pivot but its selection could not be resolved: check the slicer in Excel before trusting the totals")
            for df in p["data_fields"]:
                if df["show_data_as"] != "normal":
                    out.append(f"  - value field {df['name']} is shown as {df['show_data_as']}")
            for x in p.get("calculated_item_formulas", ()):
                out.append(f"  - calculated item in {x['field']}: {x['formula']}")
    for sc in rep.get("scenarios", ()):
        out.append(f"- scenario {sc['name']} on {sc['sheet']}: " + ", ".join(f"{x['cell']} = {x['value']}" for x in sc["cells"]))
    if rep["defined_names"]:
        kinds = Counter(n["kind"] for n in rep["defined_names"])
        out.append("")
        out.append("Defined names: " + ", ".join(f"{v} {k}" for k, v in sorted(kinds.items())))
    out.append("")
    o = rep["oracle"]
    out.append("## Oracle")
    out.append("")
    out.append(f"Cached values: **{o['status']}**. calcId: {o.get('calc_id') or 'none'}. Cached types: " +
               (", ".join(f"{v} {k}" for k, v in o["cached"].items()) or "none") + ".")
    if o.get("excel_saved"):
        out.append(f"- cache looks Excel-saved: calcId {o['calc_id']}, {o['formula_cells']} formula cells all cached "
                   "(a library can copy both, so a recalculated save is still the proof when the numbers matter)")
    for t in o["tells"]:
        out.append(f"- [{t['severity']}] {t['tell']} x{t['count']}: {t['detail']}")
    if o.get("cached_error_cell_totals"):
        out.append("- cached error cells: " + ", ".join(f"{k} {sum(v.values())}" for k, v in o["cached_error_cell_totals"].items()) +
                   ": refs per sheet are in --json as `cached_error_cells` (visible sheets only)")
    if o["status"] == "untrusted":
        out.append("- Ask for a recalculated save before comparing anything to these numbers.")
    out.append("")
    out.append("## Sources")
    out.append("")
    for lay in rep.get("layouts", ()):
        out.append(f"### {lay['sheet']}!{lay['ref']} (wide layout, formula-derived)")
        out.append(f"- wide layout recognised: {lay['period_columns']} period columns ({lay['header_ref']}), row labels in column "
                   f"{lay['label_column']}; its formula output is never lifted; any constant key column or header row is listed below as its own stanza.")
    masked_sheets = {sh["name"] for sh in rep["sheets"] if sh["masked"]}
    for s in rep["sources"]:
        out.append(f"### {s['sheet']}!{s['ref']} ({s['kind']}{': ' + s['name'] if s['name'] else ''})")
        if s["stanza"]:
            out.append("```")
            out.append(s["stanza"])
            out.append("```")
        elif s["stanza_refused"]:
            out.append(f"- no stanza: {s['stanza_refused']}")
        elif s["sheet"] in masked_sheets:
            out.append("- masked sheet: no headers or stanza")
        else:
            out.append("- no stanza")
        if s.get("first_row_text"):
            out.append(f"- first row is {s['first_row_text_kind']} text and may be a header: verify; filter it out in the wrapper")
        if s.get("reserved_columns"):
            out.append("- reserved column name(s): " + ", ".join(md(x) for x in s["reserved_columns"]) + ": Malloy reads these as keywords or date parts; backtick them or rename in the wrapper")
        if s.get("excluded_rows"):
            out.append("- rows excluded as totals (verify each): " + ", ".join(f"{e['row']} (via {e['via']})" for e in s["excluded_rows"]))
        if s["excluded_overlaps"]:
            out.append("- overlaps ranges never lifted: " + ", ".join(f"{e['kind']} {e['ref']}" for e in s["excluded_overlaps"]))
    if rep.get("ci_match_cannot_bite"):
        out.append("")
        out.append("ci_match cannot bite: no case variants in the text key column(s) " +
                   ", ".join(f"{k['sheet']}!{k['column']}" for k in rep["ci_match"]["key_columns"]) +
                   "; the flagged regions can be ported as plain equality (the sheet's own data cannot exercise case-insensitivity)")
    dts = [r for r in rep["regions"] if r.get("data_table")]
    if dts:
        out.append("")
        out.append("Data tables (what-if):")
        for r in dts:
            d = r["data_table"]
            if d["two_d"]:
                how = f"two-way: row input {d['row_input_cell']} (values {d['row_axis']}), column input {d['column_input_cell']} (values {d['column_axis']}), formula {d['formula_cell']}"
            elif d["dtr"]:
                how = f"one-way across: row input {d['row_input_cell']} (values {d['row_axis']}), formulas {d['formula_cells']}"
            else:
                how = f"one-way down: column input {d['column_input_cell']} (values {d['column_axis']}), formulas {d['formula_cells']}"
            out.append(f"- {r['id']} {r['sheet']}!{d['ref']} (r1={d['r1']}, r2={d['r2']}, dt2D={int(d['two_d'])}, dtr={int(d['dtr'])}): {how}")
    if rep["excluded_ranges"]:
        out.append("")
        out.append("Never lifted (array, spill, data table, pivot output): " + ", ".join(f"{e['sheet']}!{e['ref']} ({e['kind']})" for e in rep["excluded_ranges"]))
    if rep["given_candidates"]:
        out.append("")
        out.append("Given candidates (typed constants that formulas read, and control and Solver cells; visible sheets only; declare each as a `given:` or keep it in an inputs source):")
        for g in rep["given_candidates"][:MAX_TEXT_ROWS]:
            out.append(f"- {g['sheet']}!{g['ref']}{' ' + g['label'] if g['label'] else ''}: {g['kind']} {g['value']!r} (read by {g['read_by']} region(s))")
        extra = len(rep["given_candidates"]) - MAX_TEXT_ROWS + rep["given_candidates_omitted"]
        if extra > 0:
            out.append(f"... {extra} more given candidate(s): use --json")
    fc = rep["form_controls"]
    if fc["count"]:
        out.append("")
        out.append(f"Form controls ({fc['count']}; types and references only, no captions{'; ' + str(fc['not_listed']) + ' on hidden sheets or unplaced, not listed' if fc['not_listed'] else ''}):")
        for c in fc["controls"][:MAX_TEXT_ROWS]:
            bits = [f"linked {c['linked_cell']}" if c["linked_cell"] else None, f"list {c['list_range']}" if c["list_range"] else None]
            bits += [f"{k} {c[k]}" for k in ("min", "max", "step", "selected") if c[k] is not None]
            out.append(f"- {c['type'] or 'control'} on {c['sheet']}: " + ", ".join(b for b in bits if b))
    sv = rep["solver"]
    if sv["count"]:
        out.append("")
        out.append(f"Solver (x{sv['count']} names; the saved model by reference, nothing is translated; targets on hidden sheets are not listed):")
        for m in sv["models"]:
            out.append(f"- scope {m['scope']}: objective {m['objective'] or 'not listed'}; changing {', '.join(m['changing_cells']) or 'not listed'}"
                       + (f"; {m['hidden_targets']} hidden target(s)" if m["hidden_targets"] else ""))
            for k in m["constraints"][:MAX_TEXT_ROWS]:
                out.append(f"  - {k['lhs']} {k['relation']}{' ' + str(k['rhs']) if k['rhs'] is not None else ''}")
    out.append("")
    out.append("## Formula regions")
    out.append("")
    out.append("| Id | Sheet | Ref | Kind | Cells | Split | Route | Functions | Flags |")
    out.append("|---|---|---|---|--:|---|---|---|---|")
    for r in rep["regions"][:MAX_TEXT_ROWS]:
        out.append(f"| {r['id']} | {r['sheet']} | {r['ref']} | {r['kind']} | {r['cells']} | {r['split']} | {r['route']} | "
                   f"{' '.join(sorted(r['functions']))} | {' '.join(f['id'] for f in r['flags'])} |")
    if len(rep["regions"]) > MAX_TEXT_ROWS:
        out.append(f"... {len(rep['regions']) - MAX_TEXT_ROWS} more region(s): use --json")
    per_sheet = Counter(x["sheet"] for x in rep["regions"] + rep["pivots"] if x.get("oracle_stanza"))
    if per_sheet:
        out.append("")
        out.append("Oracle reads (cached values to compare against, not sources; the stanzas are in --json as `oracle_stanza`): " +
                   "; ".join(f"{k} {v}" for k, v in per_sheet.items()))
    hidden_deps = sum(1 for x in rep["regions"] if x["hidden_dep_via"] == "located")
    if hidden_deps:
        out.append(f"- {hidden_deps} region(s) on hidden sheets: hidden_dep, not compared")
    dependents = sum(1 for x in rep["regions"] if x["hidden_dep_via"] in ("direct", "transitive"))
    if dependents:
        out.append(f"- {dependents} region(s) depend on hidden sheets: hidden_dep, not compared")
    volatile_deps = sum(1 for x in rep["regions"] if x["volatile_dep"])
    if volatile_deps:
        out.append(f"- {volatile_deps} region(s) depend on volatile cells: volatile_dep; a RAND dependent is compared statistically, not cell for cell")
    if rep["oracle_stanzas"]["omitted_by_cap"]:
        out.append(f"- {rep['oracle_stanzas']['omitted_by_cap']} more oracle read(s) left out: the cap is {rep['oracle_stanzas']['cap']}")
    by_reason = {}
    for r in rep["regions"][:MAX_TEXT_ROWS]:
        if r["reasons"]:
            by_reason.setdefault(" | ".join(r["reasons"]), []).append(r)
    for text, rs in by_reason.items():
        if len(rs) == 1:
            out.append(f"- {rs[0]['id']} {rs[0]['sheet']}!{rs[0]['ref']}: {text}")
        else:
            shown = ", ".join(f"{r['id']} {r['sheet']}!{r['ref']}" for r in rs[:MAX_SAME_REASON_REFS])
            more = f", and {len(rs) - MAX_SAME_REASON_REFS} more (every region is in --json)" if len(rs) > MAX_SAME_REASON_REFS else ""
            out.append(f"- {len(rs)} regions ({shown}{more}): {text}")
    out.append("")
    out.append("## Routes")
    out.append("")
    out.append("| Route | Regions | Formula cells |")
    out.append("|---|--:|--:|")
    rc = rep["routes_nr_pivot_recipe"]
    for k in ("T", "C", "X", "NR"):
        n = rep["routes"][k]
        if k == "NR":
            n = {f: n[f] - rc[f] for f in n}
        out.append(f"| {k} | {n['regions']} | {n['cells']} |")
        if k == "NR":
            out.append(f"| NR (pivot recipe) | {rc['regions']} | {rc['cells']} |")
    out.append("")
    out.append("## Functions")
    out.append("")
    out.append("| Function | Regions | Cells | Route |")
    out.append("|---|--:|--:|---|")
    for fn, v in sorted(rep["functions"].items(), key=lambda kv: (-kv[1]["cells"], kv[0])):
        split = " (" + ", ".join(f"{k} {n}" for k, n in v["routes"].items()) + ")" if len(v["routes"]) > 1 else ""
        out.append(f"| {fn} | {v['regions']} | {v['cells']} | {v['route']}{split} |")
    out.append("")
    out.append("## Code attached")
    out.append("")
    if not rep["code_attached"]:
        out.append("- none found (Office Scripts, Power Automate and COM add-ins can leave no trace)")
    for mid, e in sorted(rep["code_attached"].items()):
        out.append(f"- {mid} x{e['count']} [{e['route']}]{' (unconfirmed tell)' if e['unconfirmed'] else ''}: {e['detail']}")
    ext = rep["external"]
    out.append("")
    out.append(f"External: {ext['connections']} connection(s), {ext['query_tables']} query table(s), {ext['external_links']} external link(s), "
               f"{ext['pivot_external_caches']} data-model pivot cache(s), Power Query: {'yes' if ext['data_mashup'] else 'no'}, "
               f"Power Pivot model: {'yes' if ext['power_pivot'] else 'no'}.")
    if ext["data_mashup"]:
        dm = ext["data_mashup"]
        out.append(f"Power Query: {dm['queries']} query(ies); connectors {json.dumps(dm['connectors'])}.")
    out.append("")
    g = rep["graph"]
    out.append("## Dependency graph")
    out.append("")
    for e in g["sheet_edges"]:
        out.append(f"- {e['from']} -> {e['to']} ({e['cells']} formula cells)")
    if g["incomplete_cells"]:
        out.append(f"dependency graph incomplete: {g['incomplete_cells']} cells")
    else:
        out.append("no dynamic dependencies found (this does not cover names, links or code the script cannot read)")
    if g["cycles"]:
        for c in g["cycles"]:
            out.append(f"- cycle: {c['cells']} cell(s) in {', '.join(c['regions'])}" + (f" ({c['loops']} separate loops)" if c["loops"] > 1 else ""))
    else:
        out.append("no cycle found")
    for c in g["self_inclusive_range"]:
        out.append(f"- self-inclusive range: {c['cells']} cell(s) in {', '.join(c['regions'])}: an aggregate whose range holds its own cell, "
                   "which Excel flags as circular but is not a loop between cells")
    if g["iterate_note"]:
        out.append(g["iterate_note"])
    if g["edge_budget_exceeded"]:
        out.append("edge budget exceeded: cycle detection is incomplete")
    out.append("")
    out.append("## Not read")
    out.append("")
    for n in rep["not_read"]:
        out.append(f"- {n}")
    out.append("")
    out.append("## Unconfirmed tells")
    out.append("")
    out.append("Tells still unconfirmed against a real file (spec-derived): " + ", ".join(rep["unconfirmed_tells"]) + ".")
    out.append("")
    out.append("**This is a priority order, not a verdict.** A route cannot prove a formula safe: parity-test every non-T region and spot-check the T ones.")
    return "\n".join(out)


# --------------------------------------------------------------------------
# connections, run, and the command line
# --------------------------------------------------------------------------

def build_proposals(recs):
    """Publisher connection blocks for the mappable records. Every secret is a ${VAR}; the value never reaches this function."""
    conns, notes = [], []
    for r in recs:
        if r["status"] != "maps" or not r["connection_name"]:
            continue
        body = {}
        for k, v in r["fields"].items():
            if isinstance(v, str) and "${" in v:
                notes.append(f"{r['id']}: field {k} contains a ${{...}} reference, so it was not copied")
                continue
            body[k] = v
        if r["secret_var"]:
            body[r["secret_field"]] = "${" + r["secret_var"] + "}"
        if r["missing"]:
            notes.append(f"{r['id']}: Publisher requires {', '.join(r['missing'])}, which the workbook does not carry")
        conns.append({"name": r["connection_name"], "type": r["type"], f"{r['type']}Connection": body})
    return conns, notes


def _nearest_dir(path):
    d = os.path.dirname(os.path.abspath(path))
    while d and not os.path.isdir(d):
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent
    return d


def git_allows(path):
    """Exit 0 (ignored) and 128 (not a repository) pass, as does no git at all; 1 (tracked or untracked here) and anything else refuse."""
    path = os.path.realpath(path)
    try:
        rc = subprocess.run(["git", "check-ignore", "-q", "--", path], cwd=_nearest_dir(path),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
    except OSError:
        return True
    return rc in (0, 128)


def plan_secrets_path(explicit, book_path, force):
    if explicit:
        path = os.path.abspath(explicit)
    else:
        base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(os.path.expanduser("~"), ".config")
        slug = re.sub(r"[^a-z0-9]+", "-", os.path.splitext(os.path.basename(book_path))[0].lower()).strip("-") or "workbook"
        path = os.path.join(base, "malloy-publisher", slug + ".env")
    if os.path.islink(path):
        raise CliError("the secrets file path is a symbolic link: choose another path")
    if os.path.lexists(path) and not force:
        raise CliError("the secrets file already exists: pass --force to overwrite it")
    if not git_allows(path):
        raise CliError("the secrets file would sit inside a git work tree and is not ignored by it: "
                       "choose a path outside the tree, or ignore it first")
    return path, not explicit


def _make_private_dirs(d):
    """Create missing ancestors 0700; returns True if the leaf was created."""
    if os.path.isdir(d):
        return False
    _make_private_dirs(os.path.dirname(d))
    os.mkdir(d, 0o700)
    os.chmod(d, 0o700)
    return True


def write_secrets_file(path, text, force, tighten):
    """Writes the 0600 file. Returns what happened to the directory: 'created 0700', 'tightened to 0700' or 'left as it was'."""
    parent = os.path.dirname(path)
    if os.path.islink(parent):
        raise CliError("the secrets directory is a symbolic link: choose another path")
    created = _make_private_dirs(parent)
    if tighten and not created:
        os.chmod(parent, 0o700)
    flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if force else os.O_EXCL) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
    except BaseException:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return "created 0700" if created else "tightened to 0700" if tighten else "left as it was"


def write_config_file(path, text, force):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w" if force else "x", encoding="utf-8") as fh:
        fh.write(text)


def secret_cell_values(pkg, specs):
    """{VAR: (value, provenance)} for --secret-cell, without running the whole analysis."""
    if not specs:
        return {}
    root, wb_path = read_workbook(pkg)
    if root is None:
        raise CliError("the workbook part could not be read")
    wb_rels = pkg.rels(wb_path)
    book = parse_workbook(root, wb_rels)
    want = want_positions(specs)
    sds = {}
    for s in book.sheets:
        if s["path"] and s["name"].lower() in want:
            sd = guarded(pkg, s["path"], lambda: parse_sheet(pkg, s["name"], s["path"], want[s["name"].lower()]))
            if sd is not None:
                sds[s["name"].lower()] = sd
    needed = {sds[k].sidx[p] for k, ps in want.items() if k in sds for p in ps if p in sds[k].sidx}
    texts = scan_shared_strings(pkg, shared_strings_path(pkg, wb_rels), needed)[0]
    return resolve_secret_cells(sds, specs, texts)


def shared_strings_path(pkg, wb_rels):
    for rel in wb_rels.values():
        if rel[0] == "sharedStrings" and rel[1]:
            return rel[1]
    return "xl/sharedStrings.xml" if pkg.has("xl/sharedStrings.xml") else None


def add_connections_args(ap):
    ap.add_argument("book", help="an .xlsx or .xlsm workbook")
    ap.add_argument("--config-out", required=True, help="where to write the proposed connection block (secrets are ${VAR} references)")
    ap.add_argument("--secret-cell", action="append", default=[], metavar="SHEET!CELL=VAR",
                    help="a cell holding a secret, bound to the environment variable VAR; repeatable")
    ap.add_argument("--secrets-out", help="where the secret values go (default: $XDG_CONFIG_HOME/malloy-publisher/<workbook>.env)")
    ap.add_argument("--force", action="store_true", help="overwrite existing output files")


def run_connections(args):
    specs = parse_secret_cells(args.secret_cell)
    try:
        pkg = open_package(args.book)
    except PackageError as e:
        print(f"unreadable ({e.kind}): {e.message}", file=sys.stderr)
        return 2
    try:
        cells = secret_cell_values(pkg, specs)
        ext = external_inventory(pkg, {}, [])
    finally:
        pkg.zf.close()
    found = ext.pop("_secrets")
    recs = ext["connection_list"]
    if not recs:
        print(f"{os.path.basename(args.book)}: no external connections found: nothing written")
        return 0
    conns, notes = build_proposals(recs)
    values, prov = {}, {}
    for r in recs:
        var = r["secret_var"]
        if var and (r["origin"], r["id"]) in found:
            values[var], prov[var] = found[(r["origin"], r["id"])], f"{r['origin']} {r['id']}"
    for var, (val, p) in cells.items():
        values[var], prov[var] = val, p
    slots = [r["secret_var"] for r in recs if r["secret_var"]]

    secrets_path = tighten = None
    if values:
        secrets_path, tighten = plan_secrets_path(args.secrets_out, args.book, args.force)
    if conns and os.path.lexists(args.config_out) and not args.force:
        raise CliError("the config file already exists: pass --force to overwrite it")

    def t(x):
        return flat(mask_text(str(x), known))

    known = {v for v in found.values() if len(v) >= 4} | {v for v, _ in cells.values() if len(v) >= 4}
    out = [f"workbook: {t(os.path.basename(args.book))}", f"external connections: {len(recs)}"]
    for r in sorted((r for r in recs if r["status"] == "nr"), key=source_sort_key):
        out.append(f"NR {t(r['id'])} \"{t(r['name'])}\" ({t(r['system'])}): " + t("; ".join(r["reasons"])))
    for r in recs:
        if r["status"] == "maps":
            out.append(f"proposed {t(r['id'])} \"{t(r['name'])}\" -> {r['type']} connection `{r['connection_name']}`")
            if r["missing"]:
                out.append(f"  missing required fields, add them to the block by hand: {t(', '.join(r['missing']))}")
        elif r["status"] == "file":
            out.append(f"file {t(r['id'])} \"{t(r['name'])}\" -> duckdb: " + t(r["proposed_source"] or "supply the file to DuckDB"))
    for n in notes:
        out.append("note: " + t(n))
    if slots or cells:
        out.append("secret variables (names and provenance only; values are never printed):")
    for var in slots:
        out.append(f"  {var} ← {t(prov[var])}" if var in prov else f"  {var}: no value in the workbook; add it to the secrets file yourself")
    for var in cells:
        if var not in slots:
            out.append(f"  {var} ← {t(prov[var])} (not referenced by any proposed connection)")
    if values:
        how = write_secrets_file(secrets_path, render_env(values), args.force, tighten)
        out.append(f"wrote the secrets to {t(secrets_path)} (file 0600, directory {how})")
    if conns:
        write_config_file(args.config_out, json.dumps({"connections": conns}, indent=2) + "\n", args.force)
        out.append(f"wrote a `connections` fragment to {t(args.config_out)}: merge it under an environment in publisher.config.json; it is not a loadable config by itself")
    else:
        out.append("no connection could be proposed: no config written")
    if values:
        out.append("start the server with: classify_workbook.py run --secrets " + t(secrets_path) + " -- npx @malloy-publisher/server@latest")
    for r in recs:
        if r["secret_present"]:
            out.append(f"rotate: {t(r['id'])} \"{t(r['name'])}\" shipped a password inside this file; rotate it regardless of what you do next")
    print("\n".join(out))
    return 0


def add_run_args(ap):
    ap.add_argument("--secrets", required=True, help="an env file written by `connections`")
    ap.add_argument("command", nargs=argparse.REMAINDER, help="-- followed by the command to run with those variables set")


def run_run(args):
    cmd = list(args.command or [])
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        raise CliError("pass a command after --, for example: run --secrets FILE -- npx @malloy-publisher/server@latest")
    try:
        with open(args.secrets, "rb") as fh:
            mode = os.fstat(fh.fileno()).st_mode
            text = fh.read().decode("utf-8-sig")
    except OSError:
        raise CliError("cannot read the secrets file")
    except UnicodeDecodeError:
        raise CliError("the secrets file is not UTF-8")
    if mode & 0o077:
        print("warning: the secrets file is readable by other users; chmod 600 it", file=sys.stderr)
    env = dict(os.environ)
    env.update(parse_env_file(text))
    try:
        os.execvpe(cmd[0], cmd, env)
    except FileNotFoundError:
        raise CliError("the command was not found", 127)
    except PermissionError:
        raise CliError("the command is not executable", 126)
    return 0



def add_classify_args(ap):
    ap.add_argument("book", help="an .xlsx or .xlsm workbook")
    ap.add_argument("--json", action="store_true", help="emit JSON (customer structure: keep it local)")
    ap.add_argument("--secret-cell", action="append", default=[], metavar="SHEET!CELL=VAR",
                    help="a cell holding a secret: its value is masked everywhere in the report; repeatable")


def run_classify(args):
    rep = analyze(args.book, args.secret_cell)
    print(json.dumps(rep, indent=2) if args.json else render_text(rep))
    return 2 if rep["status"] != "ok" else 0


COMMANDS = {"classify": (add_classify_args, run_classify, "classify a workbook (the default)"),
            "connections": (add_connections_args, run_connections, "propose Publisher connections; secrets go to a local file only"),
            "run": (add_run_args, run_run, "run a command with the variables from a secrets file")}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] not in COMMANDS and argv[0] not in ("-h", "--help"):
        argv.insert(0, "classify")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    for name, (add_args, _, helptext) in COMMANDS.items():
        add_args(sub.add_parser(name, help=helptext))
    args = ap.parse_args(argv)
    if not args.cmd:
        ap.error("pass a workbook")
    return COMMANDS[args.cmd][1](args)


def safe_main(argv=None):
    """Prints a CliError's fixed message; any other failure prints only its class, because str(e) or a traceback could carry a secret."""
    try:
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(errors="replace")
        return main(argv)
    except CliError as e:
        print(f"error: {e.args[0]}", file=sys.stderr)
        return e.code
    except Exception as e:
        print(f"error: {type(e).__name__}: the command failed; details are withheld because they could contain a secret", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(safe_main())
