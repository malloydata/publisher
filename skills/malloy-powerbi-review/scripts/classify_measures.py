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
    "DIRECT": "translate directly - no recipe needed",
    "SKIP": "report-layer: returns a label, not a number",
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

# A route is "divergent" when the measure can return a different number in
# Malloy than in Power BI without erroring. These propagate up the graph.
DIVERGENT_ROUTES = {"FC1", "FC2", "FC4", "FC5", "FC8", "S3"}

# A route is "stopgap" when the cookbook recipe is a workaround rather than an
# equivalent. These propagate too: a wrapper around a stopgap is a stopgap.
STOPGAP_ROUTES = {"T4", "T5", "S5", "S6"}


# --------------------------------------------------------------------------
# DAX lexing. Every classifier defect found so far came from matching raw text:
# `CALCULATE` matched the column name `CPUTime (calculated)`, and a report-layer
# test keyed on a `"` missed every label measure written without one.
# --------------------------------------------------------------------------

_STRING_RE = re.compile(r'"(?:[^"]|"")*"')
# DAX has two line-comment forms. Missing `--` reads a commented-out sentence
# as live code, which is how a plain numeric measure read as a label.
_LINE_COMMENT_RE = re.compile(r"(?://|--)[^\n]*")
_BRACE_SET_RE = re.compile(r"\{[^{}]*\}")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.S)
# `]]` is DAX's escape for a literal `]`; stopping at the first one truncates
# the identifier and the measure silently drops out of the dependency graph.
_BRACKET_RE = re.compile(r"\[(?:[^\]]|\]\])*\]")
_QUOTED_TABLE_RE = re.compile(r"'(?:[^']|'')*'")


def strip_comments(dax: str) -> str:
    """Blank out comments only, preserving offsets. Real models carry whole
    superseded measures commented out, so anything read from the raw text
    rather than from here is reading code that does not run."""
    out = _BLOCK_COMMENT_RE.sub(lambda m: " " * len(m.group(0)), dax)
    return _LINE_COMMENT_RE.sub(lambda m: " " * len(m.group(0)), out)


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


def measure_refs(dax: str) -> set:
    """`[Name]` NOT qualified by a table, i.e. a measure and not a column."""
    body = strip_noise(dax)
    refs = set()
    for m in re.finditer(r"\[((?:[^\]]|\]\])+)\]", body):
        before = body[: m.start()].rstrip()
        # `Table[Col]` / `'Table'[Col]` are columns; a bare `[Name]` is a measure.
        if before and (before[-1].isalnum() or before[-1] in "_'"):
            continue
        refs.add(m.group(1).replace("]]", "]").strip())
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
_TABLE_RE = re.compile(r"^\s*table\s+('(?:[^']|'')*'|\S+)\s*$")


def parse_table_file(path: str):
    """Return (table_name, [measure dicts], {column: dataType})."""
    with open(path, encoding="utf-8-sig") as fh:
        lines = fh.read().splitlines()

    table = os.path.splitext(os.path.basename(path))[0]
    measures, columns = [], {}
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
            name = _unquote(m.group(1))
            rest = m.group(2).strip()
            base = _indent(line)
            body_lines = []
            if rest.startswith("```"):
                # fenced: body runs to the closing fence
                i += 1
                while i < len(lines) and "```" not in lines[i]:
                    body_lines.append(lines[i])
                    i += 1
                i += 1
            else:
                if rest:
                    body_lines.append(rest)
                i += 1
                # unfenced: body is every line indented deeper than the
                # measure's own properties, which sit at base + 1
                while i < len(lines):
                    nxt = lines[i]
                    if not nxt.strip():
                        body_lines.append(nxt)
                        i += 1
                        continue
                    if _indent(nxt) > base + 1:
                        body_lines.append(nxt)
                        i += 1
                        continue
                    break
            # properties
            props = {}
            while i < len(lines):
                nxt = lines[i]
                if not nxt.strip():
                    i += 1
                    continue
                if _indent(nxt) <= base:
                    break
                stripped = nxt.strip()
                if ":" in stripped:
                    k, _, v = stripped.partition(":")
                    props[k.strip()] = v.strip()
                else:
                    props[stripped] = "true"
                i += 1
            measures.append({
                "table": table,
                "name": name,
                "kind": kind,
                "dax": "\n".join(body_lines).strip(),
                "hidden": "isHidden" in props,
                "displayFolder": props.get("displayFolder", ""),
            })
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

    return table, measures, columns


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


def parse_relationships(path: str):
    """Return (flags, notes). TMDL writes only non-default properties, so an
    absent property means many-to-one, single-direction, active."""
    flags = {"bidirectional": set(), "many_to_many": set(), "inactive": set()}
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


def load_tmdl(model_dir: str):
    defn = model_dir
    if os.path.isdir(os.path.join(model_dir, "definition")):
        defn = os.path.join(model_dir, "definition")
    tables_dir = os.path.join(defn, "tables")
    if not os.path.isdir(tables_dir):
        sys.exit(f"no definition/tables/ under {model_dir}")

    measures, coltypes = [], {}
    for fn in sorted(os.listdir(tables_dir)):
        if not fn.endswith(".tmdl"):
            continue
        table, ms, cols = parse_table_file(os.path.join(tables_dir, fn))
        measures.extend(ms)
        for c, t in cols.items():
            coltypes[(table, c)] = t

    measures.extend(parse_functions_file(os.path.join(defn, "functions.tmdl")))

    flags, note = parse_relationships(os.path.join(defn, "relationships.tmdl"))
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


def returns_string(m, coltypes, by_name, seen=None) -> bool:
    """Type the return value rather than looking for a quote character."""
    seen = seen or set()
    key = (m["table"], m["name"])
    if key in seen:
        return False
    seen.add(key)

    dax = m["dax"]
    fns = set(function_names(dax))
    if fns & STRING_FUNCS:
        return True

    # `IN ({"a", "b"})` is a comparison against a literal set, so blank the whole
    # set: only its first member is preceded by a `{`.
    live = _BRACE_SET_RE.sub(lambda mm: " " * len(mm.group(0)), strip_comments(dax))
    body = strip_noise(dax)
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
    # column they are actually called on. A body can name a text column for an
    # unrelated reason, which is how `MAX('Top N Selector'[Value])` read as text.
    for fn in fns & PASSTHROUGH_FUNCS:
        for arg in call_args(dax, fn):
            for tbl, col in column_refs(arg):
                if coltypes.get((tbl, col), "").lower() == "string":
                    return True
    # Resolves through to a label measure.
    for ref in measure_refs(dax):
        dep = by_name.get(ref)
        if dep and returns_string(dep, coltypes, by_name, seen):
            return True
    return False


def local_routes(m, coltypes, flags):
    """Routes implied by this measure's own body and the tables it touches."""
    dax = m["dax"]
    fns = set(function_names(dax))
    routes, reasons = [], []

    def add(route, why):
        if route not in routes:
            routes.append(route)
            reasons.append(why)

    tables = {t for t, _ in column_refs(dax)} | {m["table"]}

    # Step 0 signals: these live outside the DAX text entirely - except
    # CROSSFILTER, which turns a relationship bidirectional for one measure and
    # so leaves no trace in relationships.tmdl at all.
    if "CROSSFILTER" in fns:
        add("S3", "CROSSFILTER sets filter direction inside the measure")
    if tables & flags["bidirectional"]:
        add("S3", "reaches a bidirectionally cross-filtered table")
    if tables & flags["many_to_many"]:
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
    for fn, route in TIME_INTELLIGENCE.items():
        if fn in fns:
            add(route, fn)
    if fns & {"PATH", "PATHITEM", "PATHCONTAINS", "PATHLENGTH"}:
        add("S6", "parent-child hierarchy")

    # A disconnected parameter table read with MAX/MIN/SELECTEDVALUE is a
    # what-if slicer: a `given:`, not a filter-context problem.
    if "GENERATESERIES" in fns:
        add("S4", "what-if parameter table")

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

    if "CALCULATE" in fns:
        keepfilters = "KEEPFILTERS" in fns
        # FILTER(ALL(...), ...) is the explicit spelling of the overwriting form.
        explicit_overwrite = re.search(r"\bFILTER\s*\(\s*ALL", body_nb) is not None
        if explicit_overwrite or not keepfilters:
            if not (set(routes) & {"FC2", "FC3", "FC4", "FC5", "FC6", "FC7"}):
                add("FC1", "CALCULATE with no KEEPFILTERS")

    # A measure reference inside an iterator is context transition.
    if fns & {"SUMX", "AVERAGEX", "MINX", "MAXX", "COUNTX", "RANKX", "CONCATENATEX"}:
        if measure_refs(dax):
            add("FC1", "measure reference inside an iterator")

    return routes, reasons


def classify(measures, coltypes, flags):
    by_name = {}
    for m in measures:
        by_name.setdefault(m["name"], m)

    # Step 0: dependency graph.
    deps = {}
    for m in measures:
        key = (m["table"], m["name"])
        deps[key] = {
            (by_name[r]["table"], by_name[r]["name"])
            for r in measure_refs(m["dax"])
            if r in by_name and (by_name[r]["table"], by_name[r]["name"]) != key
        }

    results = {}
    for m in measures:
        key = (m["table"], m["name"])
        # Step 1: returns a string -> skip. Only that.
        if returns_string(m, coltypes, by_name):
            results[key] = {
                "m": m, "routes": ["SKIP"], "reasons": ["returns a label"],
                "inherited": [],
            }
            continue
        routes, reasons = local_routes(m, coltypes, flags)
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

def report_text(results, note, model_name, flags=None):
    rows = sorted(results.values(), key=lambda r: (r["m"]["table"], r["m"]["name"]))
    total = len(rows)
    skipped = [r for r in rows if r["routes"] == ["SKIP"]]
    direct = [r for r in rows if r["routes"] == ["DIRECT"]]
    routed = [r for r in rows if r not in skipped and r not in direct]

    out = []
    out.append(f"# {model_name}: {total} measures")
    out.append("")
    if note:
        out.append(f"> Coverage caveat: {note}")
        out.append("")
    out.append(f"- {len(skipped)} report-layer (return a label, not a number)")
    out.append(f"- {len(direct)} translate directly")
    out.append(f"- {len(routed)} need a recipe")
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
    out.append("## By recipe (a measure can need more than one)")
    out.append("")
    out.append("| Recipe | Measures | What it is |")
    out.append("|--------|---------:|------------|")
    for route, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        out.append(f"| {route} | {n} | {RECIPES.get(route, '?')} |")
    out.append("")
    out.append("## Routing")
    out.append("")
    out.append("| Table | Measure | Recipe | Why |")
    out.append("|-------|---------|--------|-----|")
    for r in rows:
        why = "; ".join(r["reasons"])
        out.append(f"| {r['m']['table']} | {r['m']['name']} | "
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
