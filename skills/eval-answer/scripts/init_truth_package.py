#!/usr/bin/env python3
"""Scaffold a truth package from the model under test. Stdlib only.

  python3 init_truth_package.py --package <path/to/model/package> \\
      --out <set>/../truth --name <set>-truth [--publisher-config <path>]

WHY

The doctrine mandates raw-table goldens: every golden is computed in a
semantics-free package over the same raw tables, never through the model under
test, so a model bug cannot certify its own golden. `set.json` has had a
`truthPackage` field for that since the schema was written -- and nothing built
one. The VideoAmp author wrote `truth.malloy`, a `publisher.json`, a second
server config and an MCP client by hand before authoring case one: an hour of
plumbing for a structure that is entirely derivable from the package.

WHAT IT PRODUCES

  truth.malloy       one `source: t_<stem> is <connection>.table('<ref>')` per
                     distinct table the model reads, and NOTHING else -- no
                     measures, no joins, no filters, no renames. The author
                     adds scope filters and raw-column dimensions by hand, as
                     the header comment says to; the script will not guess
                     semantics, because guessed semantics are exactly what a
                     truth package exists to exclude.
  publisher.json     the truth package, named, with the standard description.
  publisher.config.json  (--publisher-config) a Publisher server config
                     serving ONLY the truth package, for the second server the
                     answerer has no route to. Proxy connections are copied
                     from the model package's own config when it has one, so
                     the truth server reaches the same warehouse.
  README.md          what is here, what the author still has to do, and the
                     two rules: never serve this on the answerer's Publisher;
                     never import the model under test.

It refuses to write into a directory that already has a truth.malloy, because
the hand-added scope filters are the valuable part and a rerun must not erase
them.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

TABLE_REF = re.compile(r"""(?P<conn>[A-Za-z_][\w]*)\.table\(\s*['"](?P<ref>[^'"]+)['"]\s*\)""")


def table_refs(package: pathlib.Path) -> list[tuple[str, str]]:
    """(connection, table ref) for every distinct table the package reads."""
    seen: dict[tuple[str, str], None] = {}
    for f in sorted(package.rglob("*.malloy")):
        text = f.read_text()
        # Strip comments so a commented-out table does not become a source.
        text = re.sub(r"//[^\n]*", "", text)
        for m in TABLE_REF.finditer(text):
            seen.setdefault((m.group("conn"), m.group("ref")), None)
    return list(seen)


# Data-file extensions a table ref may carry. Stripped before anything else
# looks at the ref, because the order is the whole bug: a file path's last
# DOT-segment IS its extension, so splitting first left `parquet` as the table
# name and a package of parquet files scaffolded as t_parquet, t_parquet_2,
# t_parquet_3 -- names no golden author can write a query against.
DATA_EXT = re.compile(r"\.(parquet|csv|tsv|json|jsonl|ndjson|orc|avro)$", re.I)


def stem(ref: str) -> str:
    """t_<table>, lowercased, with v_/fact_/dim_/tbl_ noise trimmed.

    Two ref shapes reach this. A PATH (`data/users.parquet`) names a file, so
    the table is the basename without its extension. A WAREHOUSE ref
    (`analytics.public.orders`) has no slash, and there the last dot-segment is
    the table. Deciding which on the presence of a slash keeps both correct.
    """
    raw = ref.strip("/")
    cut = DATA_EXT.sub("", raw)
    had_ext = cut != raw
    if "/" in cut:
        last = cut.rsplit("/", 1)[-1]
        # Only guess at a further extension when the known list did not match,
        # so `data/a.b.parquet` keeps the `.b` that is part of the name.
        if not had_ext:
            last = re.sub(r"\.[A-Za-z0-9_]{1,10}$", "", last)
    else:
        last = cut if had_ext else cut.split(".")[-1]
    last = re.sub(r"^(v_|vw_|fact_|dim_|tbl_)+", "", last, flags=re.I)
    return "t_" + re.sub(r"\W+", "_", last).strip("_").lower()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--package", required=True, type=pathlib.Path,
                    help="the model package under test (holds publisher.json)")
    ap.add_argument("--out", required=True, type=pathlib.Path,
                    help="where to write the truth package (a SIBLING of the set, "
                         "never inside the served tree)")
    ap.add_argument("--name", required=True, help="truth package name, e.g. ir-truth")
    ap.add_argument("--publisher-config", type=pathlib.Path, default=None,
                    help="also write a Publisher server config serving only the "
                         "truth package, to this path")
    ap.add_argument("--environment", default="truth",
                    help="environment name in the generated server config")
    a = ap.parse_args()

    if not (a.package / "publisher.json").exists():
        raise SystemExit(f"{a.package} has no publisher.json; point --package at the "
                         f"model package root")
    if (a.out / "truth.malloy").exists():
        raise SystemExit(f"{a.out / 'truth.malloy'} exists. The hand-added scope filters "
                         f"are the valuable part; delete it yourself if you mean to "
                         f"start over")

    refs = table_refs(a.package)
    if not refs:
        raise SystemExit(f"no <connection>.table('...') references found under {a.package}")
    names: dict[str, int] = {}
    lines = [
        f"// Truth sources for the {a.name} eval set. GENERATED by init_truth_package.py",
        f"// from {a.package.name}/ -- then edited by hand, which is the point.",
        "//",
        "// These mirror the RAW tables the model reads and nothing else: no",
        "// measures, no joins, no lenses, no renames, no guards. A golden must not",
        "// be able to inherit the bug it is meant to catch.",
        "//",
        "// TO DO BY HAND, per source:",
        "//   - the package's documented scope filters (a report type, an audience",
        "//     level, a task name) -- as raw column predicates, quoting the model's",
        "//     own doc for where each comes from",
        "//   - raw-column dimensions for anything nested (a VARIANT column, a JSON",
        "//     path) that a golden query will need to group by",
        "// Keep column names in the warehouse's own case so a golden query reads as",
        "// a warehouse query, not as a model query.",
        "",
    ]
    for conn, ref in refs:
        n = stem(ref)
        if n in names:
            names[n] += 1
            n = f"{n}_{names[n]}"
        else:
            names[n] = 1
        lines.append(f"source: {n} is {conn}.table('{ref}')")
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "truth.malloy").write_text("\n".join(lines) + "\n")

    (a.out / "publisher.json").write_text(json.dumps({
        "name": a.name, "version": "0.0.1",
        "description": (f"Truth package for the {a.name} eval set: the raw tables the "
                        f"model reads, with the package's scope filters applied by hand "
                        f"and NO semantic modelling. Goldens are computed here so a "
                        f"model bug cannot certify its own golden. Never served on the "
                        f"answerer's Publisher."),
        "explores": ["truth.malloy"],
    }, indent=2) + "\n")

    (a.out / "README.md").write_text(f"""# {a.name}

Truth package for the eval set beside it. `truth.malloy` was scaffolded from
`{a.package.name}/` -- one source per raw table the model reads -- and must now be
finished by hand: add the package's scope filters as raw predicates and any
raw-column dimensions the goldens will group by. Add nothing else.

Two rules:

1. **Never serve this on the answerer's Publisher.** It is served by a second
   server the answerer has no route to. Serving both from one config puts gold
   within the answerer's reach.
2. **Never import the model under test.** A truth query that reuses a model
   measure lets a bug in the model certify its own golden.

Every golden's `canonicalQuery` runs here. `verify_goldens.py --set <set>`
re-derives them before each run.
""")

    if a.publisher_config:
        cfg: dict = {"frozenConfig": False,
                     "environments": [{"name": a.environment,
                                       "packages": [{"name": a.name,
                                                     "location": str(a.out.resolve())}]}]}
        model_cfg = a.package / "publisher.config.json"
        if not model_cfg.exists():
            model_cfg = a.package.parent / "publisher.config.json"
        if model_cfg.exists():
            try:
                src = json.loads(model_cfg.read_text())
                if src.get("connections"):
                    cfg["connections"] = src["connections"]
            except json.JSONDecodeError:
                pass
        a.publisher_config.parent.mkdir(parents=True, exist_ok=True)
        a.publisher_config.write_text(json.dumps(cfg, indent=2) + "\n")

    print(f"{a.out}/truth.malloy  ({len(refs)} raw table sources)")
    for conn, ref in refs:
        print(f"  {conn}.table('{ref}')")
    print(f"{a.out}/publisher.json  {a.out}/README.md")
    if a.publisher_config:
        print(f"{a.publisher_config}  (serve with: eval-loop/scripts/serve.py "
              f"--server-root <root> --port 4881 --mcp-port 4882 --allow-proxy)")
    rel = [r for _, r in refs if "/" in r and not r.startswith("/") and "://" not in r]
    if rel:
        print(f"\n  ! {len(rel)} table reference(s) are relative paths (e.g. {rel[0]!r}). "
              f"They resolve against the TRUTH package directory, so point them at "
              f"the model's data (../{a.package.name}/data/...) or symlink data/ here.")
    print("\nNext: add scope filters and raw dimensions by hand (see the header), then "
          "set `truthPackage` in set.json.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
