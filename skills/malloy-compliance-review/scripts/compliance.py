#!/usr/bin/env python3
"""Check a served Malloy package against its control register. Stdlib only.

    python3 compliance.py profile --server URL --environment ENV --package PKG \\
        --register policies/controls.json [--out profile.json]
    python3 compliance.py propose --profile profile.json \\
        --register policies/controls.json [--out proposals.md]
    python3 compliance.py check   --server URL --environment ENV --package PKG \\
        --register policies/controls.json [--out-dir review/]
    python3 compliance.py attest  --report review/report.json [--out attestation.md]

`profile` reads every published field's values as aggregates and emits the
signals the values carry (SSN-shaped, UUID-shaped, day-precision dates,
five-digit ZIPs, free text, near-unique values, one value per protected
individual). It never emits a value. `propose` turns a profile into the least
class each field's values prove, beside how it is declared today, and exits 1
when a field is declared below that; `skill:malloy-classify` proposes classes
from it.

`check` is the review: it reads the published domains (the package read's
classification summary, the model read, and get_context), runs the static checks,
probes every domain as every persona the register names, runs the value checks,
and writes report.json and report.md with a verdict and evidence per control.
Exit codes: 0 every control holds, 1 any control fails, 2 usage, 3 something could
not be checked and nothing failed (`--allow-review` makes that 0), 4 an internal
error (the traceback is on stderr). `skill:malloy-compliance-review` writes the
register this reads and judges what it reports.

`attest` turns a report into a one-page control attestation a privacy officer
can sign.

WHAT THIS READS, AND AS WHOM

Every query here is an aggregate: counts, distinct counts, shares of values
matching a pattern, a maximum. No row and no value is returned to the caller or
written to the report. Each query runs as a persona the register names, with
that persona's givens. That is only a simulation on a server with no trusted
tier in front of it, where the givens a request carries are the ones used:
the CI server this is meant for. Against a deployment behind a gateway, the
gateway's identity wins and the probes test the gateway's mapping instead.
"""
from __future__ import annotations

import argparse
import re
import datetime as _dt
import json
import pathlib
import sys
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# ── REST ──────────────────────────────────────────────────────────────────


GIVEN_REF = re.compile(r"\$([A-Za-z_]\w*)")


class Server:
    def __init__(self, base: str, environment: str, package: str, model: str,
                 mcp: str = "http://127.0.0.1:4040/mcp", package_dir: str = "."):
        self.base = base.rstrip("/")
        self.environment = environment
        self.package = package
        self.model = model
        self.mcp = mcp
        self.package_dir = package_dir

    def _pkg(self) -> str:
        return (f"{self.base}/api/v0/environments/"
                f"{urllib.parse.quote(self.environment)}/packages/"
                f"{urllib.parse.quote(self.package)}")

    def get_json(self, path: str) -> Any:
        with urllib.request.urlopen(self._pkg() + path, timeout=120) as r:
            return json.load(r)

    def context_fields(self) -> dict[str, list[dict[str, Any]]]:
        """Every published dimension, by domain, with its classes, from
        get_context over MCP (plain JSON-RPC over HTTP)."""
        out: dict[str, list[dict[str, Any]]] = {}
        offset = 0
        for _ in range(50):
            args: dict[str, Any] = {
                "scopes": [{"environment": self.environment, "package": self.package}],
                "search_targets": [{"target_type": "source"}, {"target_type": "dimension"}],
            }
            if offset:
                args["offset"] = offset
            body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                               "params": {"name": "get_context", "arguments": args}}).encode()
            req = urllib.request.Request(self.mcp, data=body, headers={
                "content-type": "application/json", "accept": "application/json, text/event-stream"})
            with urllib.request.urlopen(req, timeout=300) as r:
                raw = r.read().decode()
            if not raw.lstrip().startswith("{"):
                raw = [ln[5:] for ln in raw.splitlines() if ln.startswith("data:")][-1]
            result = json.loads(raw).get("result") or {}
            if result.get("isError"):
                raise RuntimeError(f"get_context failed: {json.dumps(result)[:300]}")
            content = result.get("content") or []
            text = content[0].get("resource", {}).get("text") or content[0].get("text") or "{}"
            page = json.loads(text)
            for card in page.get("sources", []):
                name = card["source_info"]["resource_id"]["source"]
                fields = out.setdefault(name, [])
                for e in card.get("entities", []):
                    if e.get("entity_type") == "dimension":
                        fields.append({"path": e["name"], "kind": "dimension",
                                       "classifications": e.get("classifications") or []})
            offset = page.get("next_offset") or 0
            if not offset:
                break
        return out

    def inventory(self) -> dict[str, Any]:
        """The published domains: the package read's classification summary
        (each domain's #(carries) and its fields by class), the model read of
        each domain's file (gates, and which givens are #(secure)), and
        get_context (each published dimension with its classes). Raises
        HTTPError when the package is not served."""
        meta = self.get_json("")
        summary = meta.get("classification") or {"sources": []}
        downgrades = [w for w in meta.get("warnings") or []
                      if isinstance(w, dict) and w.get("code") == "classification-downgrade"]
        reads: dict[str, Any] = {}
        try:
            fields = self.context_fields()
            fields_error = None
        except Exception as e:  # noqa: BLE001 - reported as a finding, not a crash
            fields, fields_error = {}, str(e)
        sources = []
        secure_all: set[str] = set()
        for d in summary["sources"]:
            path = d["modelPath"]
            if path not in reads:
                reads[path] = self.get_json(f"/models/{urllib.parse.quote(path, safe='')}")
            api = next((x for x in reads[path].get("sources", []) if x["name"] == d["name"]), {})
            secure = {g["name"] for g in api.get("givens", []) if g.get("secure")}
            secure_all |= secure

            def gates(exprs: list[str] | None) -> list[dict[str, Any]]:
                return [{"expression": e,
                         "givens": [{"name": n, "secure": n in secure}
                                    for n in dict.fromkeys(GIVEN_REF.findall(e))]}
                        for e in exprs or []]

            sources.append({
                "name": d["name"], "modelPath": path,
                "carries": d.get("carries"), "classCounts": d.get("classCounts", {}),
                "fieldCount": d.get("fields", 0), "unclassified": d.get("unclassified", 0),
                "authorize": gates(api.get("authorize")), "accessFilter": gates(api.get("accessFilter")),
                "fields": fields.get(d["name"], []),
            })
        return {"sources": sources, "secureGivens": sorted(secure_all),
                "fieldsError": fields_error, "downgrades": downgrades}

    def status(self) -> dict[str, Any]:
        with urllib.request.urlopen(f"{self.base}/api/v0/status", timeout=60) as r:
            return json.load(r)

    def query(self, malloy: str, givens: dict[str, Any] | None,
              model: str | None = None) -> tuple[list[dict[str, Any]] | None, int | None, str | None]:
        """(rows, None, None) on success; (None, status, message) on refusal."""
        body = json.dumps({"query": malloy, "compactJson": True,
                           "givens": givens or {}}).encode()
        req = urllib.request.Request(
            f"{self._pkg()}/models/{urllib.parse.quote(model or self.model, safe='')}/query",
            data=body, headers={"content-type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                payload = json.load(r)
        except urllib.error.HTTPError as e:
            try:
                msg = json.loads(e.read().decode()).get("message", "")
            except Exception:  # noqa: BLE001 - any unreadable body is just a status
                msg = ""
            return None, e.code, msg
        result = payload.get("result")
        rows = json.loads(result) if isinstance(result, str) else result
        return rows, None, None


# ── value signals ─────────────────────────────────────────────────────────

# Anchored regexes, applied to the value cast to text. DuckDB's regexp is RE2.
PATTERNS = {
    "ssn": r"^\d{3}-\d{2}-\d{4}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
    "phone": r"^\(?\d{3}\)?[-. ]?\d{3}[-. ]?\d{4}$",
    "date_day": r"^\d{4}-\d{2}-\d{2}",
    "zip5": r"^\d{5}(-\d{4})?$",
    "zip3": r"^\d{3}$",
    "year": r"^(19|20)\d{2}$",
}

# A signal counts when at least this share of non-null values match it.
SIGNAL_SHARE = 0.5
# A field whose distinct values are at least this share of the domain's rows,
# over at least NEAR_UNIQUE_MIN rows, behaves like a record identifier.
NEAR_UNIQUE = 0.9
NEAR_UNIQUE_MIN = 500
# Values this long, on average, read as prose rather than a code.
FREE_TEXT_AVG = 40
# A text field reached through a subject's join (the register's `subjects`,
# the individuals the policy protects) whose distinct values are at least this
# share of the subjects that have one, over at least PER_SUBJECT_MIN subjects,
# singles out a person: a name, an address, a license number.
PER_SUBJECT = 0.85
PER_SUBJECT_MIN = 50

# What each signal proves about the least a field can be classified, shared by
# `check` (which fails a field declared below it) and `propose` (which shows
# it). Values show THAT a column holds day-level dates; only the policy's class
# table says WHICH dates are direct identifiers, so this is a lower bound.
DIRECT_SIGNALS = ("ssn", "email", "phone", "uuid", "unique_per_subject")
QUASI_SIGNALS = ("date_day", "zip5", "near_unique")
# Shown to the classifier, never enforced: the policy lets a bare year be `none`,
# and three digits are as often a code as a ZIP prefix.
HINT_SIGNALS = ("zip3", "year", "free_text")


def profile_query(domain: str, path: str, subject: bool = False) -> str:
    v = f"{path}::string"
    join = path.rsplit(".", 1)[0] if "." in path else None
    parts = [
        "rows is count()",
        f"filled is count() {{ where: {path} != null }}",
        f"distinct_values is count({path})",
        f"max_length is max(length({v}))",
        # An average through a join must name the join, or it fans out.
        (f"avg_length is {path.rsplit('.', 1)[0]}.avg(length({v}))"
         if "." in path else f"avg_length is avg(length({v}))"),
    ]
    if subject and join:
        # How many of the subjects in view have a value: the denominator for
        # "one distinct value per person".
        parts.append(f"subjects is {join}.count() {{ where: {path} != null }}")
    # Aliased `m_<signal>`: `year` and others are reserved words in Malloy.
    for name, rx in PATTERNS.items():
        parts.append(f"m_{name} is count() {{ where: {v} ~ r'{rx}' }}")
    return f"run: {domain} -> {{ aggregate: {', '.join(parts)} }}"


def numeric_query(domain: str, path: str) -> str:
    return f"run: {domain} -> {{ aggregate: max_value is max({path}), min_value is min({path}) }}"


def signals_of(p: dict[str, Any], own_grain: bool, numeric: bool = False) -> list[str]:
    """The signals a field's aggregate profile carries, strongest first."""
    filled = p.get("filled") or 0
    out: list[str] = []
    if filled == 0:
        return ["empty"]
    for name in ("ssn", "uuid", "email", "phone", "zip5", "zip3", "date_day", "year"):
        if (p.get(name) or 0) / filled >= SIGNAL_SHARE:
            out.append(name)
    distinct = p.get("distinct_values") or 0
    rows = p.get("rows") or 0
    if own_grain and rows >= NEAR_UNIQUE_MIN and distinct >= NEAR_UNIQUE * rows:
        out.append("near_unique")
    subjects = p.get("subjects") or 0
    if not numeric and subjects >= PER_SUBJECT_MIN and distinct >= PER_SUBJECT * subjects:
        # A unique amount is not an identifier; a unique name or number is.
        out.append("unique_per_subject")
    if (p.get("avg_length") or 0) >= FREE_TEXT_AVG and distinct >= 0.5 * filled:
        out.append("free_text")
    return out


# The bodies of a 404 from the query boundary; any other 404 (a typo'd model) is not a refusal.
SURFACE_REFUSAL = re.compile(r"^(Query target is not queryable\.|No queryable (model|source|query) )")


def refused(status: int | None, msg: str | None) -> bool:
    """A refusal before any data: a gate's 403, or the surface's 404."""
    return status == 403 or (status == 404 and bool(SURFACE_REFUSAL.match(msg or "")))


def persona_for(register: dict[str, Any], domain: str) -> tuple[str | None, dict[str, Any]]:
    """A persona the register expects to be admitted to `domain`, preferring an unscoped one."""
    expected = register.get("access", {}).get(domain, {})
    for want in ("rows", "scoped"):
        for pid, outcome in expected.items():
            if outcome == want:
                return pid, register["personas"][pid]["givens"]
    return None, {}


def profile_domain(server: Server, register: dict[str, Any], domain: dict[str, Any],
                   progress=None) -> list[dict[str, Any]]:
    name = domain["name"]
    pid, givens = persona_for(register, name)
    capped = {cap["field"] for cap in register.get("domains", {}).get(name, {}).get("caps", [])}
    out = []
    for f in domain["fields"]:
        if f["kind"] != "dimension":
            continue
        subject = f["path"].split(".")[0] in (register.get("subjects") or []) and "." in f["path"]
        rows, status, msg = server.query(profile_query(name, f["path"], subject), givens)
        entry: dict[str, Any] = {
            "domain": name, "path": f["path"],
            "classifications": f["classifications"],
            "inferred": bool(f.get("inferred")), "as": pid,
        }
        if rows is None:
            entry["error"] = f"{status}: {msg}"
        else:
            p = rows[0]
            entry["profile"] = {k.removeprefix("m_"): p.get(k) for k in p}
            num, nstatus, nmsg = server.query(numeric_query(name, f["path"]), givens)
            if num is None:
                entry["numeric_error"] = f"{nstatus}: {nmsg}"
            numeric = bool(num) and isinstance(num[0].get("max_value"), (int, float))
            if numeric and f["path"] in capped:
                entry["profile"]["max_value"] = num[0]["max_value"]
                entry["profile"]["min_value"] = num[0]["min_value"]
            entry["signals"] = signals_of(entry["profile"], own_grain="." not in f["path"], numeric=numeric)
        out.append(entry)
        if progress:
            progress(entry)
    return out


# ── checks ────────────────────────────────────────────────────────────────


def published(gov: dict[str, Any]) -> list[dict[str, Any]]:
    """The published domains; the inventory lists nothing else."""
    return gov["sources"]


def finding(control: str, status: str, text: str, evidence: str = "",
            domain: str = "") -> dict[str, Any]:
    f = {"control": control, "status": status, "text": text, "evidence": evidence}
    if domain:
        f["domain"] = domain
    return f


def check_coverage(gov, register, _ctx):
    out = []
    for s in published(gov):
        if s["unclassified"]:
            missing = [f["path"] for f in s["fields"] if not f["classifications"]]
            out.append(finding("", "fail", f"{s['name']} publishes {s['unclassified']} unclassified fields",
                               ", ".join(missing[:12]), s["name"]))
    for w in gov.get("downgrades", []):
        out.append(finding("", "review", "a field declares a lower class than the fields it reads",
                           (w.get("message") or "")[:400], w.get("subject") or ""))
    return out or [finding("", "pass", "every published field carries a class",
                           f"{sum(s['fieldCount'] for s in published(gov))} fields on {len(published(gov))} domains")]


def check_gated(gov, register, _ctx):
    out = []
    for s in published(gov):
        if not s["authorize"] and not s["accessFilter"]:
            out.append(finding("", "fail", f"{s['name']} has no gate", "", s["name"]))
    return out or [finding("", "pass", "every published domain is gated")]


def check_secure(gov, register, _ctx):
    out = []
    for s in published(gov):
        for g in s["authorize"] + s["accessFilter"]:
            bad = [x["name"] for x in g["givens"] if not x["secure"]]
            if bad:
                out.append(finding("", "fail", f"{s['name']} gate `{g['expression']}` reads a given the caller sets",
                                   ", ".join(bad), s["name"]))
    return out or [finding("", "pass", "every gate reads only #(secure) attributes",
                           ", ".join(gov.get("secureGivens", [])))]


def everyone(register: dict[str, Any]) -> dict[str, Any]:
    """One identity holding every value of every given any persona holds: the
    most a caller could be admitted to."""
    out: dict[str, Any] = {}
    for p in register.get("personas", {}).values():
        for k, v in p.get("givens", {}).items():
            if isinstance(v, list):
                out[k] = list(dict.fromkeys([*out.get(k, []), *v]))
            else:
                out.setdefault(k, v)
    return out


SOURCE_DECL = re.compile(r"(?m)^\s*source:\s+([A-Za-z_]\w*)\s+is\b")


def declared_sources(server: Server) -> list[tuple[str, str]]:
    """(file, source name) for every source the package's own files declare."""
    root = pathlib.Path(server.package_dir)
    return [(f.relative_to(root).as_posix(), name)
            for f in sorted(root.rglob("*.malloy"))
            for name in dict.fromkeys(SOURCE_DECL.findall(f.read_text()))]


def check_locked(gov, register, ctx):
    """Every source the package does not publish refuses a caller holding
    every role the register names. Read from the package's own files, run
    from its directory, and probed through each file's own model path."""
    server: Server = ctx["server"]
    published_names = {s["name"] for s in published(gov)}
    who = everyone(register)
    out, probed = [], 0
    for rel, name in declared_sources(server):
        if name in published_names:
            continue
        probed += 1
        rows, status, msg = server.query(f"run: {name} -> {{ aggregate: n is count() }}", who, model=rel)
        if rows is not None:
            out.append(finding("", "fail", f"{name} ({rel}) answered a caller holding every role",
                               f"{rows[0].get('n')} rows; an unpublished source should refuse everyone", name))
        elif not refused(status, msg):
            out.append(finding("", "review", f"{name} ({rel}) was not probed: the server neither answered nor refused",
                               f"{status}: {msg}", name))
    if not probed:
        return [finding("", "review", "no unpublished source was found to probe",
                        f"read .malloy files under {pathlib.Path(server.package_dir).resolve()}; "
                        "run from the package directory or pass --package-dir")]
    return out or [finding("", "pass", "every unpublished source refuses a caller holding every role",
                           f"{probed} sources probed")]


def check_ceiling(gov, register, _ctx):
    out = []
    rules = register.get("domains", {})
    for s in published(gov):
        carries = set(s.get("carries") or [])
        if not carries:
            out.append(finding("", "fail", f"{s['name']} declares no #(carries)", "", s["name"]))
            continue
        forbidden = set(rules.get(s["name"], {}).get("must_not_carry", []))
        bad = forbidden & carries
        if bad:
            out.append(finding("", "fail", f"{s['name']} declares {', '.join(sorted(bad))}, which the policy forbids it", "", s["name"]))
        served = set(s["classCounts"])
        if served - carries:
            out.append(finding("", "fail", f"{s['name']} serves {', '.join(sorted(served - carries))} beyond its #(carries)", "", s["name"]))
    return out or [finding("", "pass", "every domain serves only what it declares, and declares nothing the policy forbids")]


def check_access(gov, register, ctx):
    """Probe every domain as every persona; compare with the register's expected table."""
    server: Server = ctx["server"]
    expected = register.get("access", {})
    domains = [s["name"] for s in published(gov)]
    personas = register.get("personas", {})
    results: dict[tuple[str, str], int | None] = {}
    errors = []
    count = "aggregate: n is count()"
    for d in domains:
        for pid, p in personas.items():
            rows, status, msg = server.query(f"run: {d} -> {{ {count} }}", p["givens"])
            if rows is not None:
                results[(d, pid)] = int(rows[0]["n"])
            elif refused(status, msg):
                results[(d, pid)] = None
            else:
                errors.append(f"{d} as {pid}: {status} {msg}")
    ctx["access"] = results
    # Scoped means a row filter is in force on the domain, read from the
    # model rather than guessed from a row count.
    filtered = {s["name"]: bool(s["accessFilter"]) for s in published(gov)}
    out = []
    for d in domains:
        for pid in personas:
            want = expected.get(d, {}).get(pid, "refused")
            got_n = results.get((d, pid), "error")
            if got_n == "error":
                continue
            got = "refused" if got_n is None else ("scoped" if filtered[d] else "rows")
            if got != want:
                out.append(finding("", "fail",
                                   f"{d} as {pid}: the policy says {want}, the server answered {got}",
                                   "403" if got_n is None else f"{got_n} rows, {'a row filter' if filtered[d] else 'no row filter'} in force", d))
    for d in sorted(set(expected) - set(domains)):
        out.append(finding("", "fail", f"the register expects {d}, which the package does not publish",
                           "a renamed or removed domain leaves its expectations unchecked", d))
    # A caller with no identity holds no given, so every gate must refuse it.
    gated = [s["name"] for s in published(gov) if s["authorize"] or s["accessFilter"]]
    for d in gated:
        rows, status, msg = server.query(f"run: {d} -> {{ {count} }}", {})
        if rows is not None:
            out.append(finding("", "fail", f"{d} answered a caller with no identity",
                               f"{rows[0].get('n')} rows with no givens; a gate should refuse them", d))
        elif not refused(status, msg):
            errors.append(f"{d} as no identity: {status} {msg}")
    for e in errors:
        out.append(finding("", "review", "a probe could not run", e, e.split(" ")[0]))
    n = len(domains) * len(personas)
    return out or [finding("", "pass", f"{n} of {n} access decisions match the register",
                           f"{len(domains)} domains x {len(personas)} personas, "
                           f"and {len(gated)} gated domains refuse a caller with no identity")]


def check_values(gov, register, ctx):
    """Declared classes against what the values look like. Aggregates only."""
    server: Server = ctx["server"]
    rules = register.get("domains", {})
    ids = register.get("identifier_classes", {})
    direct_classes = set(ids.get("direct", []))
    any_identifier = direct_classes | set(ids.get("quasi", []))
    if gov.get("fieldsError"):
        return [finding("", "review", "the values could not be profiled: get_context did not list the fields",
                        gov["fieldsError"])]
    profiles = []
    for s in published(gov):
        profiles.extend(profile_domain(server, register, s))
    ctx["profiles"] = profiles
    out = []
    for p in profiles:
        if "error" in p:
            out.append(finding("", "review", f"{p['domain']}.{p['path']} could not be profiled", p["error"], p["domain"]))
            continue
        sig = set(p["signals"])
        classes = set(p["classifications"])
        rule = rules.get(p["domain"], {})
        where = f"{p['domain']}.{p['path']}"
        # A value that names a person or a record needs a direct identifier
        # class; a day-level date, a ZIP or a near-unique value at least a quasi one.
        looks_direct = sig & set(DIRECT_SIGNALS)
        looks_identifying = looks_direct | (sig & set(QUASI_SIGNALS))
        if (looks_direct and not (classes & direct_classes)) or (
            looks_identifying and not (classes & any_identifier)
        ):
            share = ", ".join(
                f"{p['profile']['distinct_values']} distinct of {p['profile']['rows']} rows"
                if s == "near_unique" else
                f"{p['profile']['distinct_values']} distinct values for {p['profile']['subjects']} {p['path'].split('.')[0]} records"
                if s == "unique_per_subject" else
                f"{s} {round(100 * (p['profile'].get(s) or 0) / max(1, p['profile']['filled']))}%"
                for s in sorted(looks_identifying))
            out.append(finding("", "fail",
                               f"{where} is declared {', '.join(sorted(classes)) or 'nothing'}, but its values look like an identifier",
                               share, p["domain"]))
        if rule.get("deidentified"):
            if "date_day" in sig:
                out.append(finding("", "fail", f"{where} serves dates with a month and day on a de-identified domain",
                                   f"{p['profile']['date_day']} of {p['profile']['filled']} values", p["domain"]))
            if "zip5" in sig:
                out.append(finding("", "fail", f"{where} serves five-digit ZIP codes on a de-identified domain",
                                   f"{p['profile']['zip5']} of {p['profile']['filled']} values", p["domain"]))
            if "free_text" in sig:
                out.append(finding("", "review", f"{where} looks like free text on a de-identified domain",
                                   f"average length {round(p['profile']['avg_length'] or 0)}", p["domain"]))
    by_field = {(p["domain"], p["path"]): p for p in profiles}
    for s in published(gov):
        for cap in rules.get(s["name"], {}).get("caps", []):
            where = f"{s['name']}.{cap['field']}"
            p = by_field.get((s["name"], cap["field"]))
            if p is None:
                out.append(finding("", "review", f"the cap on {where} was not checked: the field was not profiled",
                                   "it is not a published dimension of the domain", s["name"]))
                continue
            problem = p.get("error") or p.get("numeric_error")
            mx, mn = p.get("profile", {}).get("max_value"), p.get("profile", {}).get("min_value")
            if not problem and mx is None and p.get("signals") != ["empty"]:
                problem = "the field has no numeric values"
            if problem:
                out.append(finding("", "review", f"the cap on {where} was not checked", problem, s["name"]))
                continue
            if "max" in cap and mx is not None and mx > cap["max"]:
                out.append(finding("", "fail", f"{where} reaches {mx}; the policy caps it at {cap['max']}", "", s["name"]))
            if "min" in cap and mn is not None and mn < cap["min"]:
                out.append(finding("", "fail", f"{where} reaches down to {mn}; the policy floors it at {cap['min']}", "", s["name"]))
    if not out:
        out.append(finding("", "pass", "every profiled field's values match its declared classes",
                           f"{len(profiles)} fields profiled, aggregates only"))
    return out


def check_loads(gov, register, ctx):
    """The package loaded, and is not serving a model older than its files."""
    server: Server = ctx["server"]
    status = server.status()
    errs = [e for e in status.get("loadErrors") or []
            if e.get("package") == server.package and e.get("environment") == server.environment]
    if errs:
        e = errs[0]
        ctx["stale"] = bool(e.get("stale"))
        state = "is serving a stale model: its latest files were refused" if e.get("stale") else "did not load"
        out = []
        # One finding per refusal line, tagged with the domain it names and the
        # kind of refusal, so each lands on the control it breaks.
        for line in (e.get("message") or "").split("\n"):
            line = line.strip()
            if not line:
                continue
            m = re.match(r"source (\S+) \(", line)
            kind = ("ceiling" if " carries [" in line else
                    "secure" if "not declared #(secure)" in line else
                    "join" if (" joins " in line and ("locked" in line or "whose gate reads" in line)) else
                    "other")
            f = finding("", "fail", f"{server.package} {state}", line[:400], m.group(1) if m else "")
            f["kind"] = kind
            out.append(f)
        return out or [finding("", "fail", f"{server.package} {state}")]
    return [finding("", "pass", "the package loads with every load-time refusal satisfied",
                    "no load error, and the served model is the one on disk")]


def check_refusals(gov, register, ctx):
    """A change that breaks a load-time control is refused rather than served.
    A refused, stale package is evidence this holds; the control the change
    breaks is the one that fails."""
    loads = check_loads(gov, register, ctx)
    if loads[0]["status"] == "pass":
        if ctx.get("unloaded"):
            return [{**finding("", "review", f"not checked: {ctx['server'].package} is not being served"),
                     "unscoped": True}]
        return [finding("", "pass", "no refused change is pending; the served model is the one on disk")]
    return [finding("", "pass", "the latest change was refused at load and is not being served",
                    loads[0]["evidence"])]


def check_inventory(gov, register, _ctx):
    if not gov["sources"]:
        return [finding("", "fail", "the package read reports no classified domain",
                        "no classification summary: nothing published carries a class")]
    if gov.get("fieldsError"):
        return [finding("", "fail", "get_context could not list the published fields", gov["fieldsError"])]
    listed = sum(len(s["fields"]) for s in gov["sources"])
    return [finding("", "pass", "the served model's inventory reads in full",
                    f"{len(gov['sources'])} domains, {sum(s['fieldCount'] for s in gov['sources'])} fields, "
                    f"{listed} dimensions listed by get_context")]


CHECKS = {
    "loads": check_loads,
    "inventory": check_inventory,
    "refusals": check_refusals,
    "coverage": check_coverage,
    "gated": check_gated,
    "secure_gates": check_secure,
    "locked_bases": check_locked,
    "ceiling": check_ceiling,
    "access": check_access,
    "values": check_values,
}


LOAD_KINDS = ("ceiling", "secure", "join", "other")


def unknown_scope(kind: str, scope: str, gov, register, ctx) -> dict[str, Any] | None:
    """A finding when `scope` names nothing the check could ever report on, so a
    typo cannot silently pass; None when it is known or cannot be validated."""
    declared = {n for _, n in declared_sources(ctx["server"])}
    if kind == "locked_bases":
        known = declared
        if not known:
            return finding("", "review", f"scope {scope!r} could not be validated: no source declarations were found",
                           f"read .malloy files under {pathlib.Path(ctx['server'].package_dir).resolve()}")
    elif gov is None:
        return None  # the package is not served, so the check is already reported as not checked
    else:
        known = ({s["name"] for s in published(gov)} | declared
                 | set(register.get("access", {})) | set(register.get("domains", {})))
        if kind in ("loads", "refusals"):
            known |= set(LOAD_KINDS)
    if scope in known:
        return None
    return finding("", "fail", f"unknown scope {scope!r} on {kind}: it names no domain or source",
                   f"known: {', '.join(sorted(known)) or 'none'}")


def run_check(server: Server, register: dict[str, Any]) -> dict[str, Any]:
    ctx: dict[str, Any] = {"server": server}
    try:
        gov = server.inventory()
    except urllib.error.HTTPError as e:
        # 404 for a package the server does not know, 400 or 404 for one
        # whose load failed.
        if e.code not in (400, 404):
            raise
        # Not served at all: on a fresh server (CI) a refused change leaves no
        # earlier model to fall back to. The refusal still names the control
        # it breaks; nothing else can be checked, and nothing is let pass.
        gov = None
        ctx["unloaded"] = True
    personas = register.get("personas", {})
    for d, want in register.get("access", {}).items():
        for pid in want:
            if pid not in personas:
                raise SystemExit(f"register access for {d} names persona {pid!r}, which `personas` does not define")
    by_check: dict[str, list[dict[str, Any]]] = {}
    for c in register["controls"]:
        for name in c.get("checks", []):
            kind = name.split(":")[0]
            if kind not in CHECKS:
                raise SystemExit(f"register control {c['id']} names an unknown check {name!r}; known: {', '.join(CHECKS)}")
            if kind in by_check:
                continue
            if gov is None and kind not in ("loads", "refusals"):
                by_check[kind] = [{**finding("", "review", f"not checked: {server.package} is not being served"),
                                   "unscoped": True}]
            else:
                by_check[kind] = CHECKS[kind](gov, register, ctx)
            if gov is None and kind == "loads" and by_check[kind][0]["status"] == "pass":
                by_check[kind] = [{**finding("", "fail", f"the server does not serve {server.package}",
                                             "no load error names it either: check the environment and package names"),
                                   "unscoped": True}]
    controls = []
    for c in register["controls"]:
        fs: list[dict[str, Any]] = []
        for name in c.get("checks", []):
            kind, _, scope = name.partition(":")
            if scope and (bad := unknown_scope(kind, scope, gov, register, ctx)):
                fs.append({**bad, "control": c["id"], "check": name})
                continue
            before = len(fs)
            for f in by_check[kind]:
                # A scoped check (`access:billing`, `values:research`) takes only its domain's findings.
                # A scope names a domain or, for `loads`, a kind of refusal.
                if (scope and f["status"] != "pass" and not f.get("unscoped")
                        and scope not in (f.get("domain"), f.get("kind"))):
                    continue
                fs.append({**f, "control": c["id"], "check": name})
            if scope and len(fs) == before:
                # Every finding of this check was about something else.
                if ctx.get("unloaded"):
                    # A refused load may stop at its first kind of refusal, so
                    # silence about this scope proves nothing.
                    fs.append({**finding(c["id"], "review", f"not checked: nothing {kind} reports touches {scope}, "
                                         "and the package is not being served"), "check": name})
                else:
                    fs.append({**finding(c["id"], "pass", f"nothing {kind} reports touches {scope}"),
                               "check": name})
        if c.get("status") in ("not_provable_here", "review"):
            status = c["status"]
        elif not fs:
            status = "review"
        elif any(f["status"] == "fail" for f in fs):
            status = "fail"
        elif any(f["status"] == "review" for f in fs):
            status = "review"
        else:
            status = "pass"
        controls.append({"id": c["id"], "text": c.get("text", ""), "status": status,
                         "why": c.get("why", ""), "findings": fs})
    return {
        "generated": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "server": server.base, "environment": server.environment, "package": server.package,
        "policy": register.get("policy", ""),
        "register_version": register.get("version", ""),
        "summary": {s: sum(1 for c in controls if c["status"] == s)
                    for s in ("pass", "fail", "review", "not_provable_here")},
        "controls": controls,
        "profiled_fields": len(ctx.get("profiles", [])),
        "stale": bool(ctx.get("stale")),
        "unloaded": bool(ctx.get("unloaded")),
    }


# ── output ────────────────────────────────────────────────────────────────

# ---- propose: a floor per field from its values, for skill:malloy-classify ----

# What a value signal proves about the least a field can be classified.
# Values show THAT a column holds day-level dates; only the policy's class
# table says WHICH dates (a birth date) are direct identifiers. So a floor is
# a lower bound for the agent's call, never the call itself.
def floor_of(signals: list[str], register: dict[str, Any]) -> tuple[str | None, str]:
    ids = register.get("identifier_classes") or {}
    direct = (ids.get("direct") or [None])[0]
    quasi = (ids.get("quasi") or [None])[0]
    for sig in DIRECT_SIGNALS + QUASI_SIGNALS:
        if sig in signals:
            return (direct if sig in DIRECT_SIGNALS else quasi), SIGNAL_TEXT[sig]
    hints = [SIGNAL_TEXT[h] for h in HINT_SIGNALS if h in signals]
    return None, "; ".join(hints)


SIGNAL_TEXT = {
    "ssn": "values are Social Security numbers",
    "email": "values are email addresses",
    "phone": "values are phone numbers",
    "uuid": "values are record identifiers; confirm whose record",
    "unique_per_subject": "one distinct value per protected individual",
    "date_day": "values are dates to the day",
    "zip5": "values are five-digit ZIP codes",
    "near_unique": "one distinct value per row at its domain's grain",
    "zip3": "values are three digits (a ZIP prefix, or a code)",
    "year": "values are years",
    "free_text": "values read as free text, which can carry any identifier",
}


def class_rank(classes: list[str], register: dict[str, Any]) -> int:
    ids = register.get("identifier_classes") or {}
    if set(classes) & set(ids.get("direct") or []):
        return 2
    if set(classes) & set(ids.get("quasi") or []):
        return 1
    return 0


def propose(profiles: list[dict[str, Any]], register: dict[str, Any]) -> list[dict[str, Any]]:
    """One row per field path: its signals across domains, the value floor,
    and how the declared classes (if any) compare."""
    by_path: dict[str, dict[str, Any]] = {}
    for p in profiles:
        row = by_path.setdefault(p["path"], {"path": p["path"], "domains": [], "signals": [],
                                             "declared": p.get("classifications") or [],
                                             "inferred": p.get("inferred", False)})
        row["domains"].append(p["domain"])
        row["signals"] += [x for x in p.get("signals", []) if x not in row["signals"]]
    out = []
    for row in by_path.values():
        floor, why = floor_of(row["signals"], register)
        declared = row["declared"]
        need = 2 if floor in ((register.get("identifier_classes") or {}).get("direct") or []) else 1 if floor else 0
        if not declared:
            verdict = "undeclared"
        elif need and class_rank(declared, register) < need:
            verdict = "below_floor"
        elif floor:
            verdict = "agrees"
        elif why:
            verdict = "hint"
        else:
            verdict = "no_value_evidence"
        out.append({**row, "floor": floor, "basis": why, "verdict": verdict})
    order = {"below_floor": 0, "undeclared": 1, "agrees": 2, "hint": 3, "no_value_evidence": 4}
    return sorted(out, key=lambda r: (order[r["verdict"]], r["path"]))


VERDICT_TEXT = {
    "below_floor": "Declared below what its values prove",
    "undeclared": "Undeclared",
    "agrees": "Declared at or above its value floor",
    "hint": "A hint, not a floor: the class table decides",
    "no_value_evidence": "No value evidence: the class table decides",
}


def proposals_md(rows: list[dict[str, Any]]) -> str:
    lines = ["# Classification evidence", "",
             "A floor is the least class a field's values prove. It is a lower bound; the policy's "
             "class table decides the rest. Every value here is an aggregate.", "",
             "| Field | Domains | Declared | Value floor | Basis | Verdict |",
             "| --- | --- | --- | --- | --- | --- |"]
    for r in rows:
        lines.append("| {} | {} | {} | {} | {} | {} |".format(
            r["path"], ", ".join(r["domains"]), ", ".join(r["declared"]) or "--",
            r["floor"] or "--", r["basis"] or "--", VERDICT_TEXT[r["verdict"]]))
    return "\n".join(lines) + "\n"


MARK = {"pass": "Holds", "fail": "FAILS", "review": "Needs review", "not_provable_here": "Not provable here"}


def report_md(r: dict[str, Any]) -> str:
    s = r["summary"]
    lines = [
        f"# Compliance review: {r['package']}",
        "",
        f"Policy: `{r['policy']}`. Server: {r['server']}, environment `{r['environment']}`. "
        f"Generated {r['generated']}.",
        "",
        f"**{s['fail']} failing, {s['review']} needing review, {s['pass']} holding, "
        f"{s['not_provable_here']} not provable here.** Every value check is an aggregate; "
        "no row or value was read into this report.",
        "",
        *(["> **The server refused this package's latest files and is still serving the model it compiled before.** "
            "The refusals are the failures below. Every other result describes that older model, not the files on disk.",
            ""] if r.get("stale") else []),
        *(["> **The server is not serving this package.** Its files were refused at load with no earlier model "
            "to fall back to, or it is not configured. The load failures below say which; no other check could run.",
            ""] if r.get("unloaded") else []),
        "| Control | Status | Requirement |",
        "| --- | --- | --- |",
    ]
    for c in r["controls"]:
        lines.append(f"| {c['id']} | {MARK[c['status']]} | {c['text']} |")
    for c in r["controls"]:
        if c["status"] == "pass":
            continue
        lines += ["", f"## {c['id']}: {MARK[c['status']]}", "", c["text"], ""]
        if c["why"]:
            lines += [c["why"], ""]
        for f in c["findings"]:
            if f["status"] == "pass":
                continue
            ev = f" ({f['evidence']})" if f["evidence"] else ""
            lines.append(f"- {f['text']}{ev}")
    return "\n".join(lines) + "\n"


def attestation_md(r: dict[str, Any]) -> str:
    s = r["summary"]

    def n(count: int, one: str, many: str) -> str:
        return f"{count} {one if count == 1 else many}"

    version = f", version {r['register_version']}" if r.get("register_version") else ""
    lines = [
        f"# Control attestation: {r['package']}",
        "",
        f"Policy: `{r['policy']}`{version}. Review generated {r['generated']} against the model served by "
        f"{r['server']} (environment `{r['environment']}`).",
        "",
        "| Control | Result | Requirement |",
        "| --- | --- | --- |",
    ]
    for c in r["controls"]:
        lines.append(f"| {c['id']} | {MARK[c['status']]} | {c.get('text', '')} |")
    lines += [
        "",
        f"{n(s['pass'], 'control holds', 'controls hold')}, {s['fail']} fail, "
        f"{n(s['review'], 'needs', 'need')} a reviewer's judgment, and "
        f"{n(s['not_provable_here'], 'is', 'are')} outside what this review can prove. "
        "The full report, with the evidence for each result, accompanies this page.",
        "",
        "I have reviewed the findings and the controls marked for review.",
        "",
        "Name: ______________________________   Role: ______________________________",
        "",
        "Signature: _________________________   Date: ______________________________",
        "",
    ]
    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────


def _server(a) -> Server:
    return Server(a.server, a.environment, a.package, a.model,
                  mcp=a.mcp, package_dir=a.package_dir)


LOCAL_MCP = "http://127.0.0.1:4040/mcp"
LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")


def main(argv: list[str] | None = None) -> int:
    try:
        return _main(argv)
    except SystemExit as e:
        if isinstance(e.code, str):  # a register that cannot be run
            print(e.code, file=sys.stderr)
            return 2
        raise
    except Exception:  # noqa: BLE001 - the one place every failure becomes exit 4
        traceback.print_exc()
        return 4


def _main(argv: list[str] | None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("profile", "check"):
        p = sub.add_parser(name)
        p.add_argument("--server", default="http://localhost:4000")
        p.add_argument("--environment", required=True)
        p.add_argument("--package", required=True)
        p.add_argument("--model", default="index.malloy")
        p.add_argument("--mcp", default=None,
                       help="the server's MCP endpoint, for get_context; required unless --server is "
                            f"localhost (then {LOCAL_MCP})")
        p.add_argument("--package-dir", default=".",
                       help="the package's files, for the unpublished-source probe")
        p.add_argument("--register", required=True)
        if name == "profile":
            p.add_argument("--out")
        else:
            p.add_argument("--out-dir", default="review")
            p.add_argument("--allow-review", action="store_true",
                           help="exit 0 when controls need review and none fail")
    p = sub.add_parser("propose")
    p.add_argument("--profile", required=True, help="the output of `profile`")
    p.add_argument("--register", required=True)
    p.add_argument("--out", help="write proposals.md here; proposals.json beside it")
    p = sub.add_parser("attest")
    p.add_argument("--report", required=True)
    p.add_argument("--out")
    a = ap.parse_args(argv)
    if a.cmd in ("profile", "check") and a.mcp is None:
        if urllib.parse.urlparse(a.server).hostname not in LOCAL_HOSTS:
            ap.error(f"--mcp is required when --server ({a.server}) is not localhost")
        a.mcp = LOCAL_MCP

    if a.cmd == "attest":
        r = json.loads(pathlib.Path(a.report).read_text())
        text = attestation_md(r)
        if a.out:
            pathlib.Path(a.out).write_text(text)
        else:
            sys.stdout.write(text)
        return 0

    register = json.loads(pathlib.Path(a.register).read_text())
    if a.cmd == "propose":
        rows = propose(json.loads(pathlib.Path(a.profile).read_text()), register)
        if a.out:
            pathlib.Path(a.out).write_text(proposals_md(rows))
            pathlib.Path(a.out).with_suffix(".json").write_text(json.dumps(rows, indent=1))
        else:
            sys.stdout.write(proposals_md(rows))
        return 1 if any(r["verdict"] == "below_floor" for r in rows) else 0

    server = _server(a)
    if a.cmd == "profile":
        gov = server.inventory()
        out = []
        for s in published(gov):
            out.extend(profile_domain(server, register, s))
        text = json.dumps(out, indent=1)
        if a.out:
            pathlib.Path(a.out).write_text(text)
        else:
            sys.stdout.write(text + "\n")
        return 0

    report = run_check(server, register)
    out_dir = pathlib.Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "report.json").write_text(json.dumps(report, indent=1))
    (out_dir / "report.md").write_text(report_md(report))
    s = report["summary"]
    print(f"{s['fail']} failing, {s['review']} needing review, {s['pass']} holding, "
          f"{s['not_provable_here']} not provable here. Report: {out_dir / 'report.md'}")
    if s["fail"]:
        return 1
    return 3 if s["review"] and not a.allow_review else 0


if __name__ == "__main__":
    sys.exit(main())
