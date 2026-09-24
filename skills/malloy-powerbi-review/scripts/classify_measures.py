#!/usr/bin/env python3
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""Route every DAX measure in a Power BI model to a cookbook recipe.

This does the three things an agent reading measures one at a time cannot do at
scale, and which are the actual source of misclassification:

  * resolves the measure dependency graph, so a wrapper is rated no better than
    its worst dependency;
  * types the return value from the model's own column types, so a measure that
    returns a label is recognised without a string literal in its body;
  * reads relationships.tmdl, because bidirectional / many-to-many / inactive
    flags change a measure's rating and do not appear in its DAX at all.

It emits a routing table, not a verdict. See reference/translate-measures.md for
what the routes mean and reference/cookbook-*.md for the recipes themselves.

Usage:
    classify_measures.py <model-dir>            # a TMDL model: .../definition
    classify_measures.py --json <measures.json> # no TMDL (the .pbix path)
    classify_measures.py <model-dir> --format json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

# --------------------------------------------------------------------------
# Recipes. The key is what lands in the report; the text is what to read.
# --------------------------------------------------------------------------

RECIPES = {
    "FC1": "cookbook-filter-context.md#fc1 - filtered aggregate that must not widen",
    "FC2": "cookbook-filter-context.md#fc2 - percent of total (ALL)",
    "FC3": "cookbook-filter-context.md#fc3 - percent of visible total (ALLSELECTED)",
    "FC4": "cookbook-filter-context.md#fc4 - percent within a group (ALLEXCEPT)",
    "FC5": "cookbook-filter-context.md#fc5 - remove one dimension",
    "FC6": "cookbook-filter-context.md#fc6 - dynamic top-N driven by a slicer",
    "FC7": "cookbook-filter-context.md#fc7 - rank within the visible set",
    "FC8": "cookbook-filter-context.md#fc8 - escaping a query-level filter",
    "T1": "cookbook-time.md#t1 - year to date / month to date",
    "T2": "cookbook-time.md#t2 - same period last year",
    "T3": "cookbook-time.md#t3 - period-over-period growth",
    "T4": "cookbook-time.md#t4 - date spine / densification (STOPGAP)",
    "T5": "cookbook-time.md#t5 - semi-additive closing balance (STOPGAP)",
    "T6": "cookbook-time.md#t6 - CALENDAR() calculated tables",
    "S1": "cookbook-structure.md#s1 - role-playing dimension / USERELATIONSHIP",
    "S2": "cookbook-structure.md#s2 - many-to-many via a bridge table",
    "S3": "cookbook-structure.md#s3 - bidirectional cross-filtering",
    "S4": "cookbook-structure.md#s4 - what-if parameter / disconnected slicer",
    "S5": "cookbook-structure.md#s5 - calculation groups (STOPGAP)",
    "S6": "cookbook-structure.md#s6 - parent-child PATH hierarchy (STOPGAP)",
    "S7": "cookbook-structure.md#s7 - auto date tables",
    "RLS": "rls-roles.md - row-level security predicate",
    "DIRECT": "translate directly - no recipe needed",
    "SKIP": "report-layer: returns a label, not a number",
}

# Recipes no code path can emit. Their zero is guaranteed by construction, so
# reporting it as a measured result is vacuous - `T4` was published that way.
# Every other key in RECIPES must be reachable, which the test suite asserts
# against this module's own source.
TEACHING_ONLY = {
    "T4",  # densification is a report behavior; no DAX function requests it
    "FC8",  # "there is no escape" - a teaching point, not a measure property
}

# Functions whose result is text regardless of their arguments.
STRING_FUNCS = {
    "FORMAT", "CONCATENATE", "CONCATENATEX", "COMBINEVALUES", "UNICHAR",
    "SUBSTITUTE", "REPLACE", "LEFT", "RIGHT", "MID", "UPPER", "LOWER",
    "TRIM", "REPT", "PATH", "PATHITEM",
}

# Functions that pass their argument's type through (so a text column makes the
# measure a label). SELECTEDVALUE over a text column is how most page titles and
# button captions are written, and none of them contain a string literal.
PASSTHROUGH_FUNCS = {"SELECTEDVALUE", "VALUES", "MIN", "MAX", "FIRSTNONBLANK", "LASTNONBLANK"}

TIME_INTELLIGENCE = {
    "TOTALYTD": "T1", "TOTALMTD": "T1", "TOTALQTD": "T1",
    "DATESYTD": "T1", "DATESMTD": "T1", "DATESQTD": "T1",
    "SAMEPERIODLASTYEAR": "T2", "PARALLELPERIOD": "T2",
    "DATEADD": "T3", "PREVIOUSMONTH": "T3", "PREVIOUSYEAR": "T3",
    "PREVIOUSQUARTER": "T3", "PREVIOUSDAY": "T3", "NEXTMONTH": "T3",
    "NEXTYEAR": "T3", "NEXTQUARTER": "T3", "NEXTDAY": "T3",
    "DATESINPERIOD": "T3", "DATESBETWEEN": "T3",
    "OPENINGBALANCEMONTH": "T5", "OPENINGBALANCEQUARTER": "T5",
    "OPENINGBALANCEYEAR": "T5", "CLOSINGBALANCEMONTH": "T5",
    "CLOSINGBALANCEQUARTER": "T5", "CLOSINGBALANCEYEAR": "T5",
    "CALENDAR": "T6", "CALENDARAUTO": "T6",
}

# Functions whose result is a number whatever their arguments are. Used to type
# the RETURN expression: they end the question, so a VAR holding a string for a
# comparison upstream of them cannot make the measure a label.
NUMERIC_FUNCS = {
    "DIVIDE", "SUM", "SUMX", "AVERAGE", "AVERAGEX", "AVERAGEA", "COUNT",
    "COUNTA", "COUNTX", "COUNTAX", "COUNTROWS", "COUNTBLANK", "DISTINCTCOUNT",
    "DISTINCTCOUNTNOBLANK", "MEDIAN", "MEDIANX", "PERCENTILE.INC",
    "PERCENTILE.EXC", "PERCENTILEX.INC", "PERCENTILEX.EXC", "PRODUCT",
    "PRODUCTX", "RANKX", "INT", "ROUND", "ROUNDUP", "ROUNDDOWN", "CEILING",
    "FLOOR", "MROUND", "TRUNC", "ABS", "SIGN", "SQRT", "POWER", "EXP", "LN",
    "LOG", "LOG10", "QUOTIENT", "MOD", "DATEDIFF", "YEAR", "MONTH", "DAY",
    "HOUR", "MINUTE", "SECOND", "WEEKNUM", "WEEKDAY", "QUARTER", "YEARFRAC",
}

# DAX's caller-identity functions. Their presence is dynamic row-level security
# wherever it appears - in a role predicate, or in a measure that reads it.
IDENTITY_FUNCS = {"USERPRINCIPALNAME", "USERNAME", "USEROBJECTID", "CUSTOMDATA"}

# A route is "divergent" when the measure can return a different number in
# Malloy than in Power BI without erroring. These propagate up the graph.
DIVERGENT_ROUTES = {"FC1", "FC2", "FC4", "FC5", "S3"}

# A route is "stopgap" when the cookbook recipe is a workaround rather than an
# equivalent. These propagate too: a wrapper around a stopgap is a stopgap.
STOPGAP_ROUTES = {"T5", "S5", "S6"}


# --------------------------------------------------------------------------
# DAX lexing. Every classifier defect found so far came from matching raw text:
# `CALCULATE` matched the column name `CPUTime (calculated)`, and a report-layer
# test keyed on a `"` missed every label measure written without one.
# --------------------------------------------------------------------------

_STRING_RE = re.compile(r'"(?:[^"]|"")*"')
_BRACE_SET_RE = re.compile(r"\{[^{}]*\}")
# `]]` is DAX's escape for a literal `]`; stopping at the first one truncates
# the identifier and the measure silently drops out of the dependency graph.
_BRACKET_RE = re.compile(r"\[(?:[^\]]|\]\])*\]")
_QUOTED_TABLE_RE = re.compile(r"'(?:[^']|'')*'")


def strip_comments(dax: str) -> str:
    """Blank out comments only, preserving offsets. Real models carry whole
    superseded measures commented out, so anything read from the raw text
    rather than from here is reading code that does not run.

    Scanned rather than matched, because a comment marker inside a string
    literal is not a comment: the `//` in an SVG measure's
    `"<svg xmlns='http://www.w3.org/2000/svg'>"` blanked the rest of the line and
    left the literal unterminated, which desynchronised every `"` after it and
    typed five sparklines as numbers.
    """
    out = list(dax)
    i, n = 0, len(dax)
    while i < n:
        if dax[i] == '"':
            j = i + 1
            while j < n:
                if dax[j] == '"':
                    if dax[j + 1: j + 2] == '"':  # `""` is an escaped quote
                        j += 2
                        continue
                    break
                j += 1
            i = j + 1
            continue
        if dax.startswith("/*", i):
            end = dax.find("*/", i + 2)
            end = n if end < 0 else end + 2
        elif dax.startswith("//", i) or dax.startswith("--", i):
            end = dax.find("\n", i)
            end = n if end < 0 else end
        else:
            i += 1
            continue
        for k in range(i, end):
            if out[k] != "\n":
                out[k] = " "
        i = end
    return "".join(out)


def strip_noise(dax: str) -> str:
    """Blank out comments and string literals, preserving offsets."""
    out = strip_comments(dax)
    return _STRING_RE.sub(lambda m: " " * len(m.group(0)), out)


def call_args(dax: str, fname: str) -> list:
    """The balanced-paren argument text of each `fname(...)` call."""
    body = strip_comments(dax)
    out = []
    for m in re.finditer(rf"\b{re.escape(fname)}\s*\(", body, re.I):
        depth, i = 1, m.end()
        while i < len(body) and depth:
            if body[i] == "(":
                depth += 1
            elif body[i] == ")":
                depth -= 1
            i += 1
        out.append(body[m.end(): i - 1])
    return out


def function_names(dax: str) -> Counter:
    """Every NAME( in the body, with column refs and table names blanked first.

    Blanking `[...]` is what stops `CPUTime (calculated)` reading as CALCULATE,
    and blanking `'...'` stops a table named e.g. 'Top N Selector' contributing.
    """
    body = strip_noise(dax)
    body = _BRACKET_RE.sub(lambda m: " " * len(m.group(0)), body)
    body = _QUOTED_TABLE_RE.sub(lambda m: " " * len(m.group(0)), body)
    return Counter(
        m.group(1).upper()
        for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_.]*)\s*\(", body)
    )


def measure_refs(dax: str, resolve=None) -> set:
    """`[Name]` NOT qualified by a table, i.e. a measure and not a column.

    A table qualifier is *adjacent* - DAX writes `Table[Col]` with no space. So
    whitespace before the `[` means a measure reference, and skipping to the last
    non-space character instead read `AND [Gross]` as a column of a table named
    `AND` and dropped the edge from the dependency graph, which is where
    divergence propagates. The cost of the rule is the reverse shape, `T [Col]`,
    which no TMDL writer produces (`reference/limitations.md`).

    `resolve(table, name) -> bool` optionally rescues the other legal spelling.
    `_Measures[Umsatz]` is a *measure* qualified by the table it is homed on,
    which is how a "measure table" model writes every reference; read as a column
    it dropped 119 edges across the corpus and left 27 definitions reading
    DIRECT over divergent leaves.
    """
    body = strip_noise(dax)
    refs = set()
    for m in re.finditer(r"\[((?:[^\]]|\]\])+)\]", body):
        name = m.group(1).replace("]]", "]").strip()
        prev = body[m.start() - 1] if m.start() else ""
        if prev and (prev.isalnum() or prev in "_'"):
            if resolve is None:
                continue
            q = re.search(r"(?:'((?:[^']|'')*)'|([A-Za-z_][A-Za-z0-9_]*))$",
                          body[: m.start()])
            table = ((q.group(1) or q.group(2)) if q else "").replace("''", "'").strip()
            if not table or not resolve(table, name):
                continue
        refs.add(name)
    return refs


def column_refs(dax: str) -> set:
    """`Table[Col]` / `'Table'[Col]` pairs, normalised to (table, column)."""
    body = strip_noise(dax)
    out = set()
    for m in re.finditer(
        r"(?:'((?:[^']|'')*)'|([A-Za-z_][A-Za-z0-9_]*))\[((?:[^\]]|\]\])+)\]", body
    ):
        table = (m.group(1) or m.group(2) or "").replace("''", "'").strip()
        out.add((table, m.group(3).replace("]]", "]").strip()))
    return out


# --------------------------------------------------------------------------
# TMDL parsing
# --------------------------------------------------------------------------

def _indent(line: str) -> int:
    n = 0
    for ch in line:
        if ch == "\t":
            n += 1
        elif ch == " ":
            n += 1
        else:
            break
    return n


def _unquote(name: str) -> str:
    name = name.strip()
    if name.startswith("'") and name.endswith("'") and len(name) > 1:
        return name[1:-1].replace("''", "'")
    return name


_MEASURE_RE = re.compile(r"^\s*measure\s+('(?:[^']|'')*'|[^=]+?)\s*=\s*(.*)$")
# A calculation item and a user-defined function carry DAX in the same shape as a
# measure. Matching only `measure` read a 7-calculation-group model as having none.
_CALC_ITEM_RE = re.compile(r"^\s*calculationItem\s+('(?:[^']|'')*'|[^=]+?)\s*=\s*(.*)$")
_FUNCTION_RE = re.compile(r"^\s*function\s+('(?:[^']|'')*'|[^=]+?)\s*=\s*(.*)$")
_COLUMN_RE = re.compile(r"^\s*column\s+('(?:[^']|'')*'|\S+)\s*$")
# `column X = <DAX>` is a calculated column. The `\s*$` anchor above matches none
# of them, which lost the DAX *and* the block's `dataType`, so every calculated
# column typed as the empty string and label detection under-fired on exactly the
# columns it exists to catch.
_CALC_COLUMN_RE = re.compile(r"^\s*column\s+('(?:[^']|'')*'|[^=]+?)\s*=\s*(.*)$")
_TABLE_RE = re.compile(r"^\s*table\s+('(?:[^']|'')*'|\S+)\s*$")
# `partition T = calculated` carries DAX in its `source`; `= m` carries Power
# Query, and `= entity` a Direct Lake binding. Only the first is ours.
_PARTITION_RE = re.compile(r"^\s*partition\s+(.+?)\s*=\s*(\S+)\s*$")
_SOURCE_RE = re.compile(r"^\s*source\s*=\s*(.*)$")
# A dynamic format string is DAX attached to a measure, at the same indent as its
# properties. Left to the property loop it became a property *key*, and its body
# lines became more keys.
_FORMAT_STRING_RE = re.compile(r"^\s*formatStringDefinition\s*=\s*(.*)$")


def _read_body(lines, i, base, rest):
    """Consume one `<decl> = <DAX>` block. Returns (body_lines, next_i).

    Two spellings: fenced in ``` ```, or unfenced, in which case the body is
    every line indented deeper than the declaration's own properties, which sit
    at base + 1. Blank lines stay in the body - real exports put one
    mid-expression, and ending there drops the rest of the measure silently.
    """
    body = []
    if rest.startswith("```"):
        i += 1
        while i < len(lines) and "```" not in lines[i]:
            body.append(lines[i])
            i += 1
        return body, i + 1
    if rest:
        body.append(rest)
    i += 1
    while i < len(lines):
        nxt = lines[i]
        if not nxt.strip():
            body.append(nxt)
            i += 1
            continue
        if _indent(nxt) > base + 1:
            body.append(nxt)
            i += 1
            continue
        break
    return body, i


def _read_props(lines, i, base):
    """Consume a declaration's property lines. Returns (props, extra, next_i),
    where `extra` holds any nested `key = <DAX>` block found among them."""
    props, extra = {}, {}
    while i < len(lines):
        nxt = lines[i]
        if not nxt.strip():
            i += 1
            continue
        if _indent(nxt) <= base:
            break
        m = _FORMAT_STRING_RE.match(nxt)
        if m:
            body, i = _read_body(lines, i, _indent(nxt), m.group(1).strip())
            extra["formatStringDefinition"] = "\n".join(body).strip()
            continue
        stripped = nxt.strip()
        if ":" in stripped:
            k, _, v = stripped.partition(":")
            props[k.strip()] = v.strip()
        else:
            props[stripped] = "true"
        i += 1
    return props, extra, i


def _record(table, name, kind, body_lines, props, extra=None):
    rec = {
        "table": table,
        "name": name,
        "kind": kind,
        "dax": "\n".join(body_lines).strip(),
        "hidden": "isHidden" in props,
        "displayFolder": props.get("displayFolder", ""),
    }
    if extra:
        rec.update(extra)
    return rec


def parse_table_file(path: str):
    """Return (table_name, [definition dicts], {column: dataType}).

    The definitions are measures, calculation items, calculated columns and
    calculated-table partitions - every shape in a table file that carries DAX.
    """
    with open(path, encoding="utf-8-sig") as fh:
        lines = fh.read().splitlines()

    table = os.path.splitext(os.path.basename(path))[0]
    defs, columns = [], {}
    i = 0
    while i < len(lines):
        line = lines[i]
        m = _TABLE_RE.match(line)
        if m and _indent(line) == 0:
            table = _unquote(m.group(1))
            i += 1
            continue

        m = _MEASURE_RE.match(line) or _CALC_ITEM_RE.match(line)
        if m:
            kind = "measure" if _MEASURE_RE.match(line) else "calculation_item"
            base = _indent(line)
            body, i = _read_body(lines, i, base, m.group(2).strip())
            props, extra, i = _read_props(lines, i, base)
            defs.append(_record(table, _unquote(m.group(1)), kind, body, props, extra))
            continue

        m = _CALC_COLUMN_RE.match(line)
        if m:
            cname = _unquote(m.group(1))
            base = _indent(line)
            body, i = _read_body(lines, i, base, m.group(2).strip())
            props, extra, i = _read_props(lines, i, base)
            columns[cname] = props.get("dataType", "")
            defs.append(_record(table, cname, "calculated_column", body, props, extra))
            continue

        m = _PARTITION_RE.match(line)
        if m:
            base = _indent(line)
            pname, ptype = _unquote(m.group(1)), m.group(2)
            i += 1
            body = []
            while i < len(lines):
                nxt = lines[i]
                if nxt.strip() and _indent(nxt) <= base:
                    break
                s = _SOURCE_RE.match(nxt)
                if s:
                    # Consume every partition's source, not just a calculated
                    # one: an M body left unread is scanned line by line below,
                    # where a line can look like a declaration it is not.
                    src, i = _read_body(lines, i, _indent(nxt), s.group(1).strip())
                    if ptype == "calculated":
                        body = src
                    continue
                i += 1
            if ptype == "calculated":
                defs.append(_record(table, pname, "calculated_table", body, {}))
            continue

        m = _COLUMN_RE.match(line)
        if m:
            cname = _unquote(m.group(1))
            base = _indent(line)
            i += 1
            dtype = ""
            while i < len(lines):
                nxt = lines[i]
                if nxt.strip() and _indent(nxt) <= base:
                    break
                if nxt.strip().startswith("dataType:"):
                    dtype = nxt.split(":", 1)[1].strip()
                i += 1
            columns[cname] = dtype
            continue

        i += 1

    return table, defs, columns


def parse_functions_file(path: str):
    """User-defined DAX functions (`definition/functions.tmdl`). They are real DAX
    that a measure can call, so leaving the file unread routes the caller on a body
    whose helper is invisible."""
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8-sig") as fh:
        lines = fh.read().splitlines()

    out, i = [], 0
    while i < len(lines):
        m = _FUNCTION_RE.match(lines[i])
        if not m:
            i += 1
            continue
        name = _unquote(m.group(1))
        rest, base = m.group(2).strip(), _indent(lines[i])
        body = [rest] if rest and not rest.startswith("```") else []
        i += 1
        if rest.startswith("```"):
            while i < len(lines) and "```" not in lines[i]:
                body.append(lines[i])
                i += 1
            i += 1
        else:
            while i < len(lines) and (not lines[i].strip() or _indent(lines[i]) > base):
                body.append(lines[i])
                i += 1
        out.append({"table": "(functions)", "name": name, "kind": "function",
                    "dax": "\n".join(body).strip(), "hidden": False,
                    "displayFolder": ""})
    return out


_ROLE_RE = re.compile(r"^\s*role\s+('(?:[^']|'')*'|\S+)\s*$")
# Only `tablePermission` carries a DAX row filter. `columnPermission` and
# `metadataPermission` are object-level security, which is a different control
# with a different answer (reference/rls-roles.md).
_TABLE_PERMISSION_RE = re.compile(r"^\s*tablePermission\s+(.+?)\s*=\s*(.*)$")


def parse_roles_dir(defn: str):
    """Row-level security predicates from `definition/roles/*.tmdl`.

    These are the highest-stakes DAX in the model and the loader never opened the
    directory, so a model whose only `USERPRINCIPALNAME` lives in a role reported
    no RLS at all. See reference/rls-roles.md for what a translated role costs.
    """
    roles_dir = os.path.join(defn, "roles")
    if not os.path.isdir(roles_dir):
        return []

    out = []
    for fn in sorted(os.listdir(roles_dir)):
        if not fn.endswith(".tmdl"):
            continue
        with open(os.path.join(roles_dir, fn), encoding="utf-8-sig") as fh:
            lines = fh.read().splitlines()
        role = os.path.splitext(fn)[0]
        i = 0
        while i < len(lines):
            m = _ROLE_RE.match(lines[i])
            if m and _indent(lines[i]) == 0:
                role = _unquote(m.group(1))
                i += 1
                continue
            m = _TABLE_PERMISSION_RE.match(lines[i])
            if m:
                base = _indent(lines[i])
                body, i = _read_body(lines, i, base, m.group(2).strip())
                props, _extra, i = _read_props(lines, i, base)
                out.append(_record(f"(role) {role}", _unquote(m.group(1)),
                                   "role_permission", body, props))
                continue
            i += 1
    return out


def parse_relationships(path: str):
    """Return (flags, notes). TMDL writes only non-default properties, so an
    absent property means many-to-one, single-direction, active."""
    flags = {"bidirectional": set(), "many_to_many": set(), "inactive": set(),
             "related": set()}
    if not os.path.exists(path):
        return flags, "relationships.tmdl not found - relationship flags unchecked"

    with open(path, encoding="utf-8-sig") as fh:
        lines = fh.read().splitlines()

    cur, props = None, {}

    def flush():
        if cur is None:
            return
        tables = set()
        for key in ("fromColumn", "toColumn"):
            v = props.get(key, "")
            # Table.Column or 'Table'.'Column'
            m = re.match(r"\s*('(?:[^']|'')*'|[^.]+)\.", v)
            if m:
                tables.add(_unquote(m.group(1)))
        flags["related"] |= tables
        if props.get("crossFilteringBehavior") == "bothDirections":
            flags["bidirectional"] |= tables
        if props.get("fromCardinality") == "many" and props.get("toCardinality") == "many":
            flags["many_to_many"] |= tables
        if props.get("isActive", "").lower() == "false":
            flags["inactive"] |= tables

    for line in lines:
        if re.match(r"^\s*relationship\s+\S+", line) and _indent(line) == 0:
            flush()
            cur, props = line, {}
            continue
        if cur is not None and ":" in line:
            k, _, v = line.strip().partition(":")
            props[k.strip()] = v.strip()
    flush()
    return flags, ""


# Power BI generates one hidden `LocalDateTable_<guid>` per date column plus a
# `DateTableTemplate_<guid>`. They are an artifact of a setting, not a modeling
# decision, and they dominate a file listing: 760 of one corpus's table files.
_AUTO_DATE_RE = re.compile(r"^(LocalDateTable|DateTableTemplate)_", re.I)


def load_tmdl(model_dir: str):
    defn = model_dir
    if os.path.isdir(os.path.join(model_dir, "definition")):
        defn = os.path.join(model_dir, "definition")
    tables_dir = os.path.join(defn, "tables")
    if not os.path.isdir(tables_dir):
        sys.exit(f"no definition/tables/ under {model_dir}")

    measures, coltypes, auto_date, all_tables = [], {}, [], set()
    for fn in sorted(os.listdir(tables_dir)):
        if not fn.endswith(".tmdl"):
            continue
        table, ms, cols = parse_table_file(os.path.join(tables_dir, fn))
        if _AUTO_DATE_RE.match(os.path.splitext(fn)[0]) or _AUTO_DATE_RE.match(table):
            auto_date.append(table)
            continue
        all_tables.add(table)
        measures.extend(ms)
        for c, t in cols.items():
            coltypes[(table, c)] = t

    measures.extend(parse_functions_file(os.path.join(defn, "functions.tmdl")))
    measures.extend(parse_roles_dir(defn))

    flags, note = parse_relationships(os.path.join(defn, "relationships.tmdl"))
    flags["auto_date"] = auto_date
    flags["tables"] = all_tables
    # A table in no relationship at all is a disconnected slicer or what-if
    # parameter. Only meaningful once we know the model *has* relationships -
    # otherwise an unreadable relationships.tmdl would make every table one.
    flags["disconnected"] = (all_tables - flags["related"]) if flags["related"] else set()
    return measures, coltypes, flags, note


def load_json(path: str):
    """The .pbix path yields records, not TMDL. Accept
    [{"table":..,"name":..,"dax":..,"dataType"?:..}] plus an optional
    {"measures":[...],"columns":[{"table","name","dataType"}],"relationships":[...]}."""
    with open(path, encoding="utf-8-sig") as fh:
        blob = json.load(fh)

    if isinstance(blob, list):
        measures, columns, rels = blob, [], []
    else:
        measures = blob.get("measures", [])
        columns = blob.get("columns", [])
        rels = blob.get("relationships", [])

    norm = []
    for m in measures:
        norm.append({
            "table": m.get("table", ""),
            "name": m.get("name") or m.get("measure") or "",
            "kind": m.get("kind", "measure"),
            "dax": m.get("dax") or m.get("expression") or "",
            "hidden": bool(m.get("hidden") or m.get("isHidden")),
            "displayFolder": m.get("displayFolder", ""),
        })

    coltypes = {(c.get("table", ""), c.get("name", "")): c.get("dataType", "") for c in columns}

    flags = {"bidirectional": set(), "many_to_many": set(), "inactive": set()}
    for r in rels:
        tables = {r.get("fromTable", ""), r.get("toTable", "")} - {""}
        if r.get("crossFilteringBehavior") == "bothDirections":
            flags["bidirectional"] |= tables
        if r.get("fromCardinality") == "many" and r.get("toCardinality") == "many":
            flags["many_to_many"] |= tables
        if r.get("isActive") is False:
            flags["inactive"] |= tables

    note = "" if columns else (
        "no column types supplied - label measures without a string literal "
        "will be under-detected; pass columns[] to fix"
    )
    if not rels:
        note = (note + "; " if note else "") + "no relationships supplied - flags unchecked"
    return norm, coltypes, flags, note


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------

_ARITH_RE = re.compile(r"[-+*/]")


def _blank_identifiers(dax: str) -> str:
    """Comments, string literals, `[...]` and `'...'` blanked, offsets preserved."""
    body = strip_noise(dax)
    body = _BRACKET_RE.sub(lambda m: " " * len(m.group(0)), body)
    return _QUOTED_TABLE_RE.sub(lambda m: " " * len(m.group(0)), body)


def _has_arithmetic(dax: str) -> bool:
    """An arithmetic operator in the live body, with identifiers blanked first."""
    return bool(_ARITH_RE.search(_blank_identifiers(dax)))


_VAR_DECL_RE = re.compile(r"\bVAR\s+([A-Za-z_][A-Za-z0-9_]*)\s*=", re.I)
_KEYWORD_RE = re.compile(r"\b(VAR|RETURN)\b", re.I)


def _top_level(body: str, pattern):
    """Matches of `pattern` at paren depth zero, which is where DAX's `VAR` and
    `RETURN` keywords live. A `RETURN` nested inside a function argument belongs
    to an inner `VAR` block, not to the measure.

    Matched against the identifier-blanked text: a measure named
    `[Var EBITDA vs Budget %]` otherwise reads as a `VAR` declaration and
    truncates the binding before it. Offsets are preserved, so the spans still
    index the live text.
    """
    blanked = _blank_identifiers(body)
    depth, out, pos = 0, [], 0
    for m in pattern.finditer(blanked):
        depth += blanked.count("(", pos, m.start()) - blanked.count(")", pos, m.start())
        pos = m.start()
        if depth == 0:
            out.append(m)
    return out


def return_expr(dax: str) -> str:
    """The expression a `VAR … RETURN …` body actually returns, with every VAR it
    reaches substituted in.

    DAX types on the RETURN. Typing the whole body instead meant a `VAR` holding
    `"IsFiltered"` for a *comparison* marked the measure a label, and that one
    mistake produced three classes of false report-layer finding at once. A VAR
    the RETURN never reaches has no bearing on the type and is dropped here.
    """
    body = strip_comments(dax)
    keywords = _top_level(body, _KEYWORD_RE)
    returns = [k for k in keywords if k.group(1).upper() == "RETURN"]
    if not returns:
        return body

    # Each VAR's expression runs to the next top-level VAR or RETURN.
    bounds = [k.start() for k in keywords] + [len(body)]
    bindings = {}
    for idx, k in enumerate(keywords):
        if k.group(1).upper() != "VAR":
            continue
        d = _VAR_DECL_RE.match(body, k.start())
        if d:
            bindings[d.group(1)] = body[d.end(): bounds[idx + 1]].strip()

    expr = body[returns[-1].end():].strip()
    # Substitute to a fixed point, so a VAR the RETURN *does* reach still counts.
    # Each name is consumed once: that, plus the depth cap, is what stops a
    # self-referencing pair - illegal DAX, but a parse of a partial file makes one.
    for _ in range(8):
        if not bindings or len(expr) > 200_000:
            break
        # Match against the blanked text so a VAR name that also spells a column
        # or occurs inside a string literal is not substituted; offsets are
        # preserved, so the spans apply to the live text unchanged.
        blanked = _blank_identifiers(expr)
        spans = [(mm.start(), mm.end(), mm.group(1))
                 for mm in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", blanked)
                 if mm.group(1) in bindings]
        if not spans:
            break
        out, last = [], 0
        for start, end, name in spans:
            out.append(expr[last:start])
            out.append(f"({bindings[name]})")
            last = end
        out.append(expr[last:])
        expr = "".join(out)
        bindings = {k: v for k, v in bindings.items()
                    if k not in {name for _, _, name in spans}}
    return expr


# DAX table functions whose arguments include `"Name", <expr>` pairs. A bare
# string literal argument to one of these declares a column name; none of them
# takes a literal string as a *value*. Reading `ADDCOLUMNS(t, "@Rank", …)`'s
# name as a value filed ordinary numeric measures as report-layer.
# Sorted, because this runs on the path that produces a published number and a
# set's iteration order is not stable across runs.
NAME_ARG_FUNCS = sorted({
    "ADDCOLUMNS", "SELECTCOLUMNS", "SUMMARIZE", "SUMMARIZECOLUMNS", "ROW",
    "GROUPBY", "DATATABLE", "NATURALINNERJOIN", "NATURALLEFTOUTERJOIN",
})

# Iterators: the first argument is the table to walk, the second the expression
# whose value comes out. Only the second is a value position.
ITERATOR_FUNCS = {
    "SUMX", "AVERAGEX", "MINX", "MAXX", "COUNTX", "COUNTAX", "PRODUCTX",
    "MEDIANX", "RANKX", "CONCATENATEX", "GENERATEALL", "STDEVX.P", "STDEVX.S",
}

_NUMERIC_LITERAL_RE = re.compile(r"^[-+]?(\d+\.?\d*|\.\d+)$")


def _unwrap_parens(expr: str) -> str:
    """Strip redundant outer parentheses. `return_expr` wraps every substituted
    VAR in a pair, and an expression read as `(IF(...))` rather than `IF(...)`
    types as its own condition."""
    expr = expr.strip()
    while expr.startswith("(") and expr.endswith(")"):
        inner = expr[1:-1]
        blanked = _blank_identifiers(inner)
        if blanked.count("(") != blanked.count(")"):
            break
        depth = 0
        for ch in blanked:
            depth += (ch == "(") - (ch == ")")
            if depth < 0:
                break
        if depth != 0:
            break
        expr = inner.strip()
    return expr


def _split_args(arg_text: str):
    """Split a call's argument text at depth-zero commas, with offsets."""
    blanked = _blank_identifiers(arg_text)
    out, depth, start = [], 0, 0
    for i, ch in enumerate(blanked):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            out.append((start, i))
            start = i + 1
    out.append((start, len(arg_text)))
    return out


def _blank_name_args(dax: str) -> str:
    """Blank the `"Name"` half of every `"Name", <expr>` argument pair."""
    out = dax
    for fname in NAME_ARG_FUNCS:
        for m in re.finditer(rf"\b{fname}\s*\(", _blank_identifiers(out), re.I):
            depth, i = 1, m.end()
            while i < len(out) and depth:
                depth += (out[i] == "(") - (out[i] == ")")
                i += 1
            inner = out[m.end(): i - 1]
            pieces = list(inner)
            for s, e in _split_args(inner):
                arg = inner[s:e].strip()
                if _STRING_RE.fullmatch(arg):
                    at = inner.index(arg, s)
                    pieces[at: at + len(arg)] = " " * len(arg)
            out = out[: m.end()] + "".join(pieces) + out[i - 1:]
    return out


# `CALCULATE` filter arguments that leave the report's filters standing, or that
# already route somewhere of their own. Everything else replaces what the report
# put on the columns it names, which is the FC1 divergence.
PRESERVING_FILTER_FUNCS = {
    "KEEPFILTERS",
    "USERELATIONSHIP", "CROSSFILTER",           # modifiers, not filters
    "FILTER", "VALUES", "DISTINCT",             # evaluated in the current context
    "SUMMARIZE", "ADDCOLUMNS", "SELECTCOLUMNS",
    "ALL", "ALLEXCEPT", "ALLNOBLANKROW", "ALLSELECTED", "REMOVEFILTERS",
} | set(TIME_INTELLIGENCE)

_LEADING_CALL_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_.]*)\s*\(")


def overwriting_filter_args(dax: str) -> list:
    """Every `CALCULATE` filter argument that overwrites rather than intersects.

    A Boolean filter argument is shorthand for `FILTER(ALL(col), …)`, so it
    replaces whatever filter the report had on that column. Decided by what the
    argument is *not*: an earlier version looked for a comparison operator, and
    so read `NOT ISBLANK(T[c])` and `TREATAS(…)` as harmless.
    """
    out = []
    for argtext in call_args(dax, "CALCULATE"):
        for start, end in _split_args(argtext)[1:]:
            arg = _unwrap_parens(argtext[start:end])
            if not arg:
                continue
            lead = _LEADING_CALL_RE.match(arg)
            if lead and lead.group(1).upper() in PRESERVING_FILTER_FUNCS:
                continue
            out.append(arg)
    return out


def _value_exprs(expr: str, depth=0):
    """Every sub-expression whose value can come out of `expr`.

    A passthrough over a text column is a label only where its value is what the
    measure returns. `AVERAGEX(VALUES(T[GradeCode]), DIVIDE(…))` names a text
    column to supply the *table* it walks, and reading that as the return type
    filed a participation rate as a button caption.
    """
    expr = _unwrap_parens(expr)
    if depth > 6 or not expr:
        return [expr]
    name = ""
    m = re.match(r"([A-Za-z_][A-Za-z0-9_.]*)\s*\(", expr)
    if m:
        name = m.group(1).upper()
    args = call_args(expr, name) if name else []
    if not args:
        return [expr]
    parts = [args[0][s:e].strip() for s, e in _split_args(args[0])]

    def down(sub):
        return [x for p in sub for x in _value_exprs(p, depth + 1)]

    if name == "IF" and len(parts) >= 2:
        return down(parts[1:3])
    if name == "IFERROR" and len(parts) >= 1:
        return down(parts[:2])
    if name == "SWITCH" and len(parts) >= 3:
        # value, match1, result1, …, [else]
        results = parts[2::2] + ([parts[-1]] if len(parts) % 2 == 0 else [])
        return down(results)
    if name == "CALCULATE":
        return down(parts[:1])
    if name in ITERATOR_FUNCS and len(parts) >= 2:
        return down(parts[1:2])
    return [expr]


def _is_numeric(expr: str) -> bool:
    """Is this expression a number whatever its inputs are?"""
    expr = _unwrap_parens(expr)
    if _NUMERIC_LITERAL_RE.match(expr) or expr.upper().replace(" ", "") == "BLANK()":
        return True
    if _outermost_call(expr) in NUMERIC_FUNCS:
        return True
    # A comparison returns a boolean, which is a number - but only one at the top
    # level. A comparison buried in an argument says nothing about the result:
    # reading one made every SVG sparkline in a Microsoft model numeric.
    blanked, depth = _blank_identifiers(expr), 0
    for ch in blanked:
        depth += (ch == "(") - (ch == ")")
        if depth == 0 and ch in "<>=":
            return True
    return False


def _outermost_call(expr: str) -> str:
    """The function name the expression's value comes out of, unwrapping the
    wrappers that pass their argument's type through."""
    for _ in range(8):
        expr = _unwrap_parens(expr)
        m = re.match(r"([A-Za-z_][A-Za-z0-9_.]*)\s*\(", expr)
        if not m:
            return ""
        name = m.group(1).upper()
        # CALCULATE returns its first argument's type, so it answers nothing here.
        if name != "CALCULATE":
            return name
        args = call_args(expr, "CALCULATE")
        if not args:
            return name
        start, end = _split_args(args[0])[0]
        expr = args[0][start:end]
    return ""


def _blank_switch_cases(dax: str) -> str:
    """Blank the `match` half of every `SWITCH(expr, match, result, …)` pair.

    A match is compared against, not returned. Read as a value it filed a
    measure returning a *date* as a label, and two numeric measures that
    referenced it cascaded with it. `SWITCH(TRUE(), cond, result, …)` is the
    other idiom, where the odd arguments are conditions rather than literals and
    there is nothing to blank.
    """
    out = dax
    for m in re.finditer(r"\bSWITCH\s*\(", _blank_identifiers(out), re.I):
        depth, i = 1, m.end()
        while i < len(out) and depth:
            depth += (out[i] == "(") - (out[i] == ")")
            i += 1
        inner = out[m.end(): i - 1]
        spans = _split_args(inner)
        if not spans or re.match(r"TRUE\s*\(", inner[spans[0][0]:spans[0][1]].strip(), re.I):
            continue
        pieces = list(inner)
        for start, end in spans[1::2]:
            arg = inner[start:end].strip()
            if _STRING_RE.fullmatch(arg):
                at = inner.index(arg, start)
                pieces[at: at + len(arg)] = " " * len(arg)
        out = out[: m.end()] + "".join(pieces) + out[i - 1:]
    return out


def returns_string(m, coltypes, by_name, seen=None, resolve=None) -> bool:
    """Type the return value rather than looking for a quote character."""
    seen = seen or set()
    key = (m["table"], m["name"])
    if key in seen:
        return False
    seen.add(key)

    dax = return_expr(m["dax"])
    values = _value_exprs(dax)
    # Every position the value can come out of is a number, so nothing further
    # down can make this a label. This ends the question before the heuristics
    # get to guess from a VAR the RETURN merely compared against, or from a text
    # column an iterator named only to say which table to walk.
    if values and all(_is_numeric(v) for v in values):
        return False
    fns = set(function_names(dax))
    if fns & STRING_FUNCS:
        return True

    # `IN ({"a", "b"})` is a comparison against a literal set, so blank the whole
    # set: only its first member is preceded by a `{`. `"Name", <expr>` pairs go
    # too - they declare a column, they are not values.
    live = _BRACE_SET_RE.sub(lambda mm: " " * len(mm.group(0)),
                             _blank_switch_cases(_blank_name_args(strip_comments(dax))))
    # A string literal in a value position. `= "x"` / `<> "x"` / `IN {"x"}` are
    # comparisons and do not make the measure a label; anything else does.
    for lit in _STRING_RE.finditer(live):
        before = live[: lit.start()].rstrip()
        if before.endswith(("=", "<>", ">", "<", "{", "IN", "in")):
            continue
        return True
    # `&` concatenation. `&&` is DAX logical AND, and a `&` inside `[...]` or
    # `'...'` is part of an identifier, not an operator.
    if "&" in _blank_identifiers(dax).replace("&&", "  "):
        return True
    # Arithmetic coerces: `[A] - [B]` is a number whatever [A] and [B] are, and
    # `SELECTEDVALUE(T[c]) + 0` is the DAX idiom for forcing one. Scanned after
    # blanking `[...]` and `'...'`, because measure names contain `-`.
    if _has_arithmetic(dax):
        return False
    # SELECTEDVALUE / VALUES / MIN / MAX over a text column - but only over the
    # column they are actually called on, and only where the call sits in a value
    # position. A body can name a text column for an unrelated reason, which is
    # how `MAX('Top N Selector'[Value])` read as text and how a `VALUES(...)`
    # supplying an iterator's table did.
    for value in values:
        fn = _outermost_call(value)
        if fn not in PASSTHROUGH_FUNCS:
            continue
        for arg in call_args(value, fn):
            for tbl, col in column_refs(arg):
                if coltypes.get((tbl, col), "").lower() == "string":
                    return True
    # Resolves through to a label measure - but only where the reference is in a
    # value position. A label measure named in a *condition* says nothing about
    # what the caller returns.
    for value in values:
        for ref in measure_refs(value, resolve):
            dep = by_name.get(ref)
            if dep and returns_string(dep, coltypes, by_name, seen, resolve):
                return True
    return False


def tables_touched(dax: str, own_table: str, known_tables=()) -> set:
    """Every table this expression reaches, by either spelling.

    `Table[Col]` is the obvious one. A **bare** table argument - `COUNTROWS(T)`,
    `VALUES(T)`, `FILTER(T, …)`, `ALL('T')` - names the table with no column and
    was invisible here, which left `COUNTROWS(fato_exame)` routed DIRECT beside
    an `AVERAGE(fato_exame[…])` routed S3 on the same bidirectional fact.
    """
    out = {t for t, _ in column_refs(dax)} | {own_table}
    if not known_tables:
        return out
    body = strip_noise(dax)
    for m in _QUOTED_TABLE_RE.finditer(body):
        name = m.group(0)[1:-1].replace("''", "'")
        if name in known_tables:
            out.add(name)
    bare = _QUOTED_TABLE_RE.sub(lambda mm: " " * len(mm.group(0)),
                               _BRACKET_RE.sub(lambda mm: " " * len(mm.group(0)), body))
    for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\()", bare):
        if m.group(1) in known_tables:
            out.add(m.group(1))
    return out


def local_routes(m, coltypes, flags, resolve=None):
    """Routes implied by this measure's own body and the tables it touches."""
    dax = m["dax"]
    fns = set(function_names(dax))
    kind = m.get("kind", "measure")
    routes, reasons = [], []

    def add(route, why):
        if route not in routes:
            routes.append(route)
            reasons.append(why)

    tables = tables_touched(dax, m["table"], flags.get("tables", ()))

    # Step 0 signals: these live outside the DAX text entirely - except
    # CROSSFILTER, which turns a relationship bidirectional for one measure and
    # so leaves no trace in relationships.tmdl at all.
    #
    # A calculated column or table is evaluated at refresh in row context, so a
    # relationship's filter direction cannot reach it unless the body performs a
    # context transition. Applying these flags blind put S3 on 20 calculated
    # columns that never enter filter context, and inflated the demand.
    in_filter_context = (kind not in ("calculated_column", "calculated_table")
                         or "CALCULATE" in fns or measure_refs(dax, resolve))
    if "CROSSFILTER" in fns:
        add("S3", "CROSSFILTER sets filter direction inside the measure")
    if in_filter_context and tables & flags["bidirectional"]:
        add("S3", "reaches a bidirectionally cross-filtered table")
    if in_filter_context and tables & flags["many_to_many"]:
        add("S2", "reaches a many-to-many relationship")

    # Step 2: needs a concept outside the model. Before the widen test.
    # Only USERELATIONSHIP routes here. An inactive relationship is inert in DAX
    # until a measure activates it, so merely touching that table is a model-level
    # note - routing on it put 97 of one model's 298 recipe measures on S1 wrongly.
    if "USERELATIONSHIP" in fns:
        add("S1", "USERELATIONSHIP")
    if (m.get("kind") == "calculation_item"
            or "SELECTEDMEASURE" in fns or "SELECTEDMEASURENAME" in fns):
        add("S5", "calculation group")
    if m.get("kind") == "role_permission":
        add("RLS", "row-level security predicate")
    if fns & IDENTITY_FUNCS:
        add("RLS", "reads the caller's identity")
    for fn, route in TIME_INTELLIGENCE.items():
        if fn in fns:
            add(route, fn)
    if fns & {"PATH", "PATHITEM", "PATHCONTAINS", "PATHLENGTH"}:
        add("S6", "parent-child hierarchy")

    # A disconnected parameter table read with MAX/MIN/SELECTEDVALUE is a
    # what-if slicer: a `given:`, not a filter-context problem. The code used to
    # check only `GENERATESERIES`, which is how the table is *built* - so a
    # model whose parameter tables are imported rather than generated reported
    # no what-if parameters at all, though they drove half its measures.
    if "GENERATESERIES" in fns:
        add("S4", "what-if parameter table")
    elif flags.get("disconnected"):
        for fn in fns & {"SELECTEDVALUE", "MIN", "MAX"}:
            for arg in call_args(dax, fn):
                if {t for t, _ in column_refs(arg)} & flags["disconnected"]:
                    add("S4", f"{fn} over a table with no relationships")
                    break

    # Ranking. RANKX over an ALLSELECTED scope with a slicer-driven cutoff is
    # the top-N shape; RANKX alone is the plain one.
    if "RANKX" in fns:
        if "ALLSELECTED" in fns and fns & {"MAX", "MIN", "SELECTEDVALUE"}:
            add("FC6", "RANKX over ALLSELECTED with a slicer-driven cutoff")
        else:
            add("FC7", "RANKX")
    elif "TOPN" in fns:
        add("FC6", "TOPN")

    # Step 3: filter context.
    if "ALLSELECTED" in fns and "FC6" not in routes:
        add("FC3", "ALLSELECTED")
    if "ALLEXCEPT" in fns:
        add("FC4", "ALLEXCEPT")

    body = strip_noise(dax)
    body_nb = _BRACKET_RE.sub(lambda mm: " " * len(mm.group(0)), body)
    all_table = re.search(r"\bALL\s*\(\s*'?[A-Za-z_]", body_nb) is not None
    all_column = re.search(r"\bALL\s*\(\s*[^)]*\[", body) is not None
    if "REMOVEFILTERS" in fns or all_column:
        add("FC5", "ALL/REMOVEFILTERS on a column")
    elif all_table:
        add("FC2", "ALL on a table")

    # FILTER(ALL(...), ...) is the explicit spelling of the overwriting form.
    if re.search(r"\bFILTER\s*\(\s*ALL", body_nb):
        add("FC1", "FILTER(ALL(...)) overwrites the filter on that column")
    elif overwriting_filter_args(dax):
        # Decided per argument, not per measure. Suppressing this whenever any
        # other FC route had fired hid the divergence on 93 measures whose
        # CALCULATE carried an `ALL` *and* independent Boolean predicates - the
        # `ALL` produced FC5 and the predicates, which overwrite a slicer on
        # their own columns, went unreported.
        add("FC1", "CALCULATE with a Boolean filter and no KEEPFILTERS")

    # A measure reference inside an iterator is context transition. Not for
    # RANKX once ranking has already been named: its measure argument is how
    # RANKX works, and step 2 says ranking is not a filter-context problem.
    iterators = fns & ITERATOR_FUNCS
    if iterators and not (iterators == {"RANKX"} and set(routes) & {"FC6", "FC7"}):
        if measure_refs(dax, resolve):
            add("FC1", "measure reference inside an iterator")

    return routes, reasons


# The report-layer test asks whether a *measure* returns a label. The other DAX
# shapes are not scalar measures and answering it for them is a category error: a
# role predicate returns a boolean, a calculated table returns a table, and a
# string calculated column is an ordinary dimension rather than canvas furniture.
LABEL_TESTED_KINDS = {"measure", "calculation_item", "function"}


def classify(measures, coltypes, flags):
    by_name = {}
    for m in measures:
        if m.get("kind", "measure") == "measure":
            by_name.setdefault(m["name"], m)
    for m in measures:
        by_name.setdefault(m["name"], m)

    # `Table[X]` is a measure when X is one and the table has no such column.
    # A model that homes its measures on a `_Measures` table writes every
    # reference this way.
    homes = {(m["table"], m["name"]) for m in measures
             if m.get("kind", "measure") == "measure"}

    def resolve(table, name):
        return (table, name) in homes and (table, name) not in coltypes

    # Step 0: dependency graph.
    deps = {}
    for m in measures:
        key = (m["table"], m["name"])
        deps[key] = {
            (by_name[r]["table"], by_name[r]["name"])
            for r in measure_refs(m["dax"], resolve)
            if r in by_name and (by_name[r]["table"], by_name[r]["name"]) != key
        }

    results = {}
    for m in measures:
        key = (m["table"], m["name"])
        # Step 1: returns a string -> skip. Only that, and only for a measure.
        if (m.get("kind", "measure") in LABEL_TESTED_KINDS
                and returns_string(m, coltypes, by_name, resolve=resolve)):
            results[key] = {
                "m": m, "routes": ["SKIP"], "reasons": ["returns a label"],
                "inherited": [],
            }
            continue
        routes, reasons = local_routes(m, coltypes, flags, resolve)
        if not routes:
            # Step 4: fall through, do not stall.
            routes, reasons = ["DIRECT"], ["no filter-context or out-of-model concept"]
        results[key] = {"m": m, "routes": routes, "reasons": reasons, "inherited": []}

    # Propagate: a measure is no better than its worst dependency. Divergence
    # and stopgap status travel up the graph; a direct wrapper around a
    # divergent leaf is divergent.
    for _ in range(len(results) + 1):
        changed = False
        for key, r in results.items():
            if r["routes"] == ["SKIP"]:
                continue
            for dep in deps.get(key, ()):  # already-resolved leaves
                d = results.get(dep)
                if not d or d["routes"] == ["SKIP"]:
                    continue
                for route in d["routes"]:
                    if route in ("DIRECT", "SKIP"):
                        continue
                    if route in r["routes"] or route in r["inherited"]:
                        continue
                    r["inherited"].append(route)
                    if r["routes"] == ["DIRECT"]:
                        r["routes"] = []
                        r["reasons"] = []
                    r["routes"].append(route)
                    r["reasons"].append(f"via [{dep[1]}]")
                    changed = True
        if not changed:
            break

    for r in results.values():
        if not r["routes"]:
            r["routes"], r["reasons"] = ["DIRECT"], ["no filter-context concept"]

    return results, deps


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

# What to call each `kind` in the report. Summing them under one "measures"
# heading published 1,622 measures for a corpus that held 1,406 - a number a
# reader has no way to take apart again.
KIND_LABELS = [
    ("measure", "measures"),
    ("calculation_item", "calculation items"),
    ("function", "user-defined functions"),
    ("calculated_column", "calculated columns"),
    ("calculated_table", "calculated tables"),
    ("role_permission", "RLS role predicates"),
]


def report_text(results, note, model_name, flags):
    rows = sorted(results.values(), key=lambda r: (r["m"]["table"], r["m"]["name"]))
    total = len(rows)
    skipped = [r for r in rows if r["routes"] == ["SKIP"]]
    direct = [r for r in rows if r["routes"] == ["DIRECT"]]
    routed = [r for r in rows if r not in skipped and r not in direct]
    kinds = Counter(r["m"].get("kind", "measure") for r in rows)

    out = []
    out.append(f"# {model_name}: {kinds['measure']} measures")
    out.append("")
    if note:
        out.append(f"> Coverage caveat: {note}")
        out.append("")
    other = [f"{kinds[k]} {label}" for k, label in KIND_LABELS[1:] if kinds[k]]
    if other:
        out.append(f"Plus {', '.join(other)} - also DAX, and also routed, but not "
                   f"measures. {total} definitions in all.")
        out.append("")
    out.append("| Kind | Total | Report-layer | Direct | Needs a recipe |")
    out.append("|------|------:|-------------:|-------:|---------------:|")
    for kind, label in KIND_LABELS:
        if not kinds[kind]:
            continue
        sel = [r for r in rows if r["m"].get("kind", "measure") == kind]
        out.append(f"| {label} | {len(sel)} "
                   f"| {sum(1 for r in sel if r in skipped)} "
                   f"| {sum(1 for r in sel if r in direct)} "
                   f"| {sum(1 for r in sel if r in routed)} |")
    out.append("")
    auto_date = sorted(flags.get("auto_date", ()))
    if auto_date:
        route = "S7"
        out.append(f"**Model-level: {route}.** {len(auto_date)} auto date table(s) "
                   "(`LocalDateTable_*` / `DateTableTemplate_*`), generated by a "
                   "setting rather than modeled. Skipped here and replaced by one "
                   f"real date dimension ({RECIPES[route]}). It is a model-level "
                   "route by nature: no measure carries it, so it never appears in "
                   "the recipe counts below.")
        out.append("")
    inactive = sorted((flags or {}).get("inactive", ()))
    if inactive:
        out.append(f"**Model-level:** {len(inactive)} table(s) carry an inactive "
                   f"relationship ({', '.join(inactive[:6])}"
                   f"{', ...' if len(inactive) > 6 else ''}). Each becomes a second "
                   "named join path in Malloy (`cookbook-structure.md#s1`). Only the "
                   "measures that call `USERELATIONSHIP` are routed there - an "
                   "inactive relationship is inert until one activates it.")
        out.append("")

    counts = Counter()
    for r in routed:
        for route in r["routes"]:
            counts[route] += 1
    div = sum(1 for r in routed if set(r["routes"]) & DIVERGENT_ROUTES)
    stop = sum(1 for r in routed if set(r["routes"]) & STOPGAP_ROUTES)
    out.append(f"Of the {len(routed)}: {div} can return a different number silently, "
               f"{stop} land on a stopgap recipe.")
    out.append("")
    out.append("## By recipe (a definition can need more than one)")
    out.append("")
    out.append("| Recipe | Definitions | What it is |")
    out.append("|--------|------------:|------------|")
    for route, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        out.append(f"| {route} | {n} | {RECIPES.get(route, '?')} |")
    out.append("")
    out.append("## Routing")
    out.append("")
    out.append("| Table | Name | Kind | Recipe | Why |")
    out.append("|-------|------|------|--------|-----|")
    for r in rows:
        why = "; ".join(r["reasons"])
        out.append(f"| {r['m']['table']} | {r['m']['name']} | "
                   f"{r['m'].get('kind', 'measure')} | "
                   f"{' '.join(r['routes'])} | {why} |")
    out.append("")
    out.append("**This is a priority order, not a verdict.** The routing cannot prove a "
               "measure safe: whether a report ever filters the overwritten column lives "
               "in `report.json`, which this skill does not read. Parity-test every "
               "non-DIRECT row, and spot-check the DIRECT ones.")
    return "\n".join(out)


def report_json(results, note, model_name):
    return json.dumps({
        "model": model_name,
        "note": note,
        "measures": [
            {
                "table": r["m"]["table"],
                "name": r["m"]["name"],
                "kind": r["m"].get("kind", "measure"),
                "routes": r["routes"],
                "reasons": r["reasons"],
                "inherited": r["inherited"],
                "hidden": r["m"]["hidden"],
            }
            for r in sorted(results.values(), key=lambda r: (r["m"]["table"], r["m"]["name"]))
        ],
    }, indent=2)


def unmapped_functions(measures, known):
    seen = Counter()
    holding = defaultdict(set)
    for m in measures:
        for fn, n in function_names(m["dax"]).items():
            if fn in known:
                continue
            seen[fn] += n
            holding[fn].add(m["name"])
    return seen, holding


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("model", nargs="?", help="a TMDL model directory")
    ap.add_argument("--json", dest="json_in", help="measure records, for the .pbix path")
    ap.add_argument("--format", choices=("text", "json"), default="text")
    ap.add_argument("--functions", action="store_true",
                    help="also list every DAX function used, with counts")
    args = ap.parse_args(argv)

    if args.json_in:
        measures, coltypes, flags, note = load_json(args.json_in)
        name = os.path.basename(args.json_in)
    elif args.model:
        measures, coltypes, flags, note = load_tmdl(args.model)
        name = os.path.basename(os.path.abspath(args.model).rstrip("/"))
    else:
        ap.error("pass a model directory or --json")

    if not measures:
        sys.exit("no measures found")

    results, _ = classify(measures, coltypes, flags)

    if args.functions:
        counts, holding = unmapped_functions(measures, set())
        for fn, n in counts.most_common():
            print(f"{n:5d} occurrences  {len(holding[fn]):4d} measures  {fn}")
        return 0

    print(report_json(results, note, name) if args.format == "json"
          else report_text(results, note, name, flags))
    return 0


if __name__ == "__main__":
    sys.exit(main())
