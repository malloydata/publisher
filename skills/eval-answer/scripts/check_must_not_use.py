#!/usr/bin/env python3
"""The mechanical half of `golden.mustNotUse`: did the final query use a field
the golden forbids?

`mustNotUse` names the similar-but-wrong field -- `shipped_at` where the golden
means `created_at`, last year's measure where the question asks about this year.
An answer that used one is wrong however good the number looks, and that is a
question about query TEXT, not about meaning, so a script decides it and the
judge is never asked. Design record: eval-program.md, "a script checks this,
not the judge".

Entries come in three shapes, and only two of them are decidable here:

    shipped_at                                 an identifier -- checked, vetoes
    products.retail_price.avg() through ...    a field path  -- checked, vetoes
    an average of per-SKU prices               prose         -- the judge's

so `check` returns all three lists and the caller hands the prose to the judge
rather than guessing at it. A leaf found without its path (`retail_price` with
no `products.` in front) is reported separately and does NOT veto: the same
short name is often a legitimate field on another source, and a veto that fires
on a correct answer is worse than one that misses.

    from check_must_not_use import check
    r = check(["shipped_at", "an average of per-SKU prices"], "run: x -> ...")
    r["hits"]        # ["shipped_at"]        -> verdict no_match
    r["leaf_hits"]   # []                    -> reported, judged
    r["unchecked"]   # ["an average ..."]    -> goes into the judge prompt

CLI, for a spot check:

    python3 check_must_not_use.py --query-file q.malloy shipped_at delivered_at
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

# One identifier, or a dotted path of them. `products.retail_price` and
# `shipped_at` match; `an average of per-SKU prices` does not, which is the
# whole test -- prose has spaces and this does not.
_PATH = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")

# Authors write "<expression> as <the wrong reading>" and "<field> through <the
# wrong join>". Everything after the connective is prose about WHY it is wrong,
# so the expression in front of it is what a text check can look for.
_SPLIT = re.compile(r"\s+(?:as|through)\s+")


def candidate(entry: str) -> str | None:
    """The field path an entry names, or None when it is prose.

    `products.retail_price.avg() through the product join` -> products.retail_price
    """
    head = _SPLIT.split(entry.strip(), 1)[0].strip()
    # A trailing call is the aggregate applied to the field, not part of its
    # name: `x.y.avg()` forbids `x.y`.
    if head.endswith("()"):
        head = head[:-2].rsplit(".", 1)[0]
    return head if _PATH.match(head) else None


def strip_noise(malloy: str) -> str:
    """Query text with comments and string literals blanked out.

    A forbidden name inside `-- we deliberately avoided shipped_at` is the
    answerer explaining itself, and vetoing on it would punish the explanation
    rather than the query.
    """
    out = re.sub(r"/\*.*?\*/", " ", malloy, flags=re.S)
    out = re.sub(r"(--|//)[^\n]*", " ", out)
    return re.sub(r"'[^'\n]*'|\"[^\"\n]*\"", " '' ", out)


def _present(name: str, text: str) -> bool:
    """`name` as a whole identifier path, not as part of a longer one.

    `total_sales_2021` must not match `total_sales_2021_adj`, and the dot in a
    path is literal: `products.retail_price` does not match `retail_price`.
    """
    return re.search(r"(?<![A-Za-z0-9_.])" + re.escape(name)
                     + r"(?![A-Za-z0-9_])", text) is not None


def check(must_not_use: list[str] | None, final_query: str | None
          ) -> dict[str, Any]:
    """Split `mustNotUse` into what the text proves, suspects, and cannot say.

    `hits` is a veto: the query names a forbidden field. `leaf_hits` is the last
    segment of a forbidden path found on its own, which is a suspicion for the
    judge. `unchecked` is prose, which only the judge can apply.
    """
    entries = list(must_not_use or [])
    if not entries or not final_query:
        return {"hits": [], "leaf_hits": [], "unchecked": entries,
                "checked": []}

    text = strip_noise(final_query)
    hits, leaf_hits, unchecked, checked = [], [], [], []
    for entry in entries:
        path = candidate(entry)
        if path is None:
            unchecked.append(entry)
            continue
        checked.append(path)
        if _present(path, text):
            hits.append(entry)
            continue
        leaf = path.rsplit(".", 1)[-1]
        # A bare leaf is reported, never vetoed, and the judge is told the path
        # it came from -- `cost` on its own is a legitimate field on plenty of
        # sources that are not the one the golden forbids.
        if leaf != path and _present(leaf, text):
            leaf_hits.append(entry)
        else:
            unchecked.append(entry)
    return {"hits": hits, "leaf_hits": leaf_hits, "unchecked": unchecked,
            "checked": checked}


def judge_note(result: dict[str, Any]) -> str:
    """The `mustNotUse` line for the judge prompt: what a script could not decide.

    Only the prose and the bare-leaf suspicions. A vetoed case never reaches the
    judge with the veto hidden, because the veto is applied to the verdict.
    """
    lines = [f"- {e}" for e in result.get("unchecked") or []]
    lines += [f"- {e}  (a bare `{candidate(e).rsplit('.', 1)[-1]}` appears in "
              f"the query; decide whether it is that field)"
              for e in result.get("leaf_hits") or []]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("entries", nargs="*", help="mustNotUse entries")
    ap.add_argument("--query-file", required=True)
    a = ap.parse_args(argv)
    r = check(a.entries, open(a.query_file).read())
    print(json.dumps(r, indent=2))
    return 1 if r["hits"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
