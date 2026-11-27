#!/usr/bin/env python3
"""The production history, ready for a database that takes no inserts. Stdlib only.

  python3 eval.py history-export --set <set-dir> --label 20261127 [--connection evals]

The history model (model 1 of the optimization package) reads CSVs that only
ever grow. A managed database cannot add rows to a table, so each export is a
batch: the rows added to each table since the last export, written as
`<optimization package>/export-history/<label>/<table>.csv`, to load as the
table `<table>__<label>`. `manifest.json` beside them names each table and its
row count. A table with no new rows has no file and no table.

`data/exported.json` records which rows each label took, so the next export
starts after them, and exporting a label again rewrites the same rows.

It also writes the package to publish beside those tables, in a sibling of
the optimization package, `<optimization package>-hosted/` (never inside it:
a model on a connection the local server lacks would stop the local package
loading): the template's publisher.json and history
app, and `history.malloy` with each CSV read replaced by the union of that
table's loaded batches (`UNION ALL BY NAME`) on the named connection, every
column cast to the type the local model reads it as. The price card comes from
the newest batch alone. A table no batch has yet reads as no rows of the right
types, so the model compiles from the first export.

Load the tables, then publish that package into an environment of its own: the
judgments are verdicts, and the transcripts are production answers.
"""
from __future__ import annotations

import argparse
import csv
import json
import pathlib
import re
import shutil
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
sys.path.insert(0, str(HERE))

import config  # noqa: E402
from optimization_package import APPENDED, TEMPLATE, install_for, read  # noqa: E402

EXPORTED = "exported.json"
SAFE = re.compile(r"^[a-z0-9_]+$")
# read_csv('data/<table>.csv', header = true, columns = { 'col': 'TYPE', ... })
READ_CSV = re.compile(r"read_csv\('data/(\w+)\.csv',\s*header\s*=\s*true,\s*columns\s*=\s*\{(.*?)\}\)", re.S)
COLUMN = re.compile(r"'(\w+)'\s*:\s*'(\w+)'")


def table_name(table: str, label: str) -> str:
    return f"{table}__{label}"


def batches(data: pathlib.Path) -> dict[str, dict[str, list[int]]]:
    path = data / EXPORTED
    return json.loads(path.read_text()) if path.is_file() else {}


def export(data: pathlib.Path, out: pathlib.Path, label: str) -> dict[str, Any]:
    """Write the label's batch. Returns the manifest."""
    if not SAFE.match(label):
        raise SystemExit(f"Invalid --label {label!r}: a table name takes lower-case letters, "
                         f"digits and _ only. Fix: --label 20261127")
    done = batches(data)
    again = done.get(label)   # a label exported before takes exactly its rows again
    span = {}
    for t in list(APPENDED) + ["price_card"]:
        if again is not None:
            span[t] = again.get(t)
            continue
        ends = [r[1] for b in done.values() for tt, r in b.items() if tt == t]
        span[t] = [max(ends, default=0), None]
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    took: dict[str, list[int]] = {}
    manifest: dict[str, Any] = {"label": label, "tables": []}
    for t in list(APPENDED) + ["price_card"]:
        rows = read(data / f"{t}.csv")
        if span[t] is None:
            continue
        start, end = (0, None) if t == "price_card" else span[t]
        new = rows[start:end]
        if not new:
            continue
        with (out / f"{t}.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(new[0]))
            w.writeheader()
            w.writerows(new)
        took[t] = [start, start + len(new)]
        manifest["tables"].append({"file": f"{t}.csv", "table": table_name(t, label), "rows": len(new)})
    done[label] = took
    (data / EXPORTED).write_text(json.dumps(done, indent=1) + "\n")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


def hosted_model(text: str, loaded: dict[str, list[str]], connection: str) -> str:
    """history.malloy over loaded batches: each CSV read becomes a typed union."""
    def swap(m: re.Match) -> str:
        table, cols = m.group(1), COLUMN.findall(m.group(2))
        names = loaded.get(table) or []
        if table == "price_card":
            names = names[-1:]
        cast = ", ".join(f'TRY_CAST("{c}" AS {ty}) AS "{c}"' for c, ty in cols)
        if not names:
            empty = ", ".join(f'CAST(NULL AS {ty}) AS "{c}"' for c, ty in cols)
            return f"(SELECT {empty} WHERE FALSE)"
        union = "\n      UNION ALL BY NAME\n      ".join(f"SELECT * FROM {n}" for n in names)
        return f"(SELECT {cast} FROM (\n      {union}))"
    out = READ_CSV.sub(swap, text)
    out = out.replace("duckdb.sql(", f"{connection}.sql(")
    header = ("// Generated by eval-loop's history_export.py from the template's history.malloy:\n"
              f"// each table is the union of its loaded batches on connection `{connection}`.\n"
              "// Do not edit; export again.\n")
    return header + out


def write_hosted(opt: pathlib.Path, data: pathlib.Path, connection: str) -> pathlib.Path:
    loaded: dict[str, list[str]] = {}
    for label, b in sorted(batches(data).items()):
        for t in b:
            loaded.setdefault(t, []).append(table_name(t, label))
    pkg = opt.parent / f"{opt.name}-hosted"
    shutil.rmtree(pkg, ignore_errors=True)
    (pkg / "public").mkdir(parents=True)
    shutil.copytree(TEMPLATE / "public" / "history", pkg / "public" / "history")
    meta = json.loads((TEMPLATE / "publisher.json").read_text())
    meta["description"] = ("How the served package does in production: the history model over "
                           "the batches history_export.py wrote. Holds verdicts and production "
                           "answers, so it lives in an environment of its own.")
    (pkg / "publisher.json").write_text(json.dumps(meta, indent=1) + "\n")
    (pkg / "history.malloy").write_text(
        hosted_model((TEMPLATE / "history.malloy").read_text(), loaded, connection))
    return pkg


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", dest="set_dir", required=True)
    ap.add_argument("--label", required=True, help="names this batch's tables: <table>__<label>")
    ap.add_argument("--connection", default="evals",
                    help="the database connection the hosted model reads the tables from")
    a = ap.parse_args(argv)
    cfg = config.load(pathlib.Path(a.set_dir))
    data = install_for(cfg)
    opt = cfg.optimization_dir()
    out = opt / "export-history" / a.label
    man = export(data, out, a.label)
    for t in man["tables"]:
        print(f"  {t['table']}: {t['rows']} rows  <- {out / t['file']}")
    if not man["tables"]:
        print("  nothing new since the last export")
    pkg = write_hosted(opt, data, a.connection)
    print(f"hosted package: {pkg}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
