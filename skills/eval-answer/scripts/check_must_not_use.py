#!/usr/bin/env python3
"""The mechanical half of `golden.mustNotUse`: did the final query use a field
the golden forbids?

`mustNotUse` names the similar-but-wrong field -- `shipped_at` where the golden
means `created_at`, last year's measure where the question asks about this year.
An answer that used one is wrong however good the number looks, and that is a
question about query TEXT, not about meaning, so a script decides it and the
judge is never asked. Design record: eval-program.md, "a script checks this,
not the judge".

Only a BARE name is decidable here:

    shipped_at                       an identifier -- checked, vetoes
    products.retail_price            a field path  -- checked, vetoes
    weekly_active_users as a series     prose         -- the judge's
    an average of per-SKU prices     prose         -- the judge's

so `check` returns all three lists and the caller hands the prose to the judge
rather than guessing at it. A leaf found without its path (`retail_price` with
no `products.` in front) is reported separately and does NOT veto: the same
short name is often a legitimate field on another source, and a veto that fires
on a correct answer is worse than one that misses.

**A connective makes an entry prose, however it starts.** `X as <reading>` and
`X through <join>` name a WAY of using X, not a ban on X, and reading them as
"veto X anywhere" fails correct answers: `weekly_active_users as a cumulative
series` vetoed an answer whose cumulative series was exact and that merely
showed the per-period field as an extra column. The same trap sits in the
ecommerce set, where `total_sales as the answer` and `sale_price.avg() as spend
per customer` name the two fields nearly every correct answer to those cases
must use. So the split is mechanical and the authoring rule is simple: write the
bare name when the field must not appear at all, and write prose when the
objection is to a use of it.

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

# "<field> as <the wrong reading>" and "<field> through <the wrong join>" object
# to a USE of the field, not to the field. The head used to be vetoed anyway,
# which turned every such entry into a ban on a field that correct answers use.
_CONNECTIVE = re.compile(r"\s+(?:as|through)\s+")


def candidate(entry: str) -> str | None:
    """The field path an entry names, or None when it is prose.

    `products.retail_price` -> products.retail_price
    `products.retail_price.avg()` -> products.retail_price
    `weekly_active_users as a cumulative series` -> None, the judge's to apply
    """
    head = entry.strip()
    if _CONNECTIVE.search(head):
        return None
    # A trailing call is the aggregate applied to the field, not part of its
    # name: `x.y.avg()` forbids `x.y`.
    if head.endswith("()"):
        head = head[:-2].rsplit(".", 1)[0]
    return head if _PATH.match(head) else None


_NOISE = re.compile(r"/\*.*?\*/|--[^\n]*|//[^\n]*|'[^'\n]*'|\"[^\"\n]*\"", re.S)


def strip_noise(malloy: str) -> str:
    """Query text with comments and string literals blanked out.

    A forbidden name inside `-- we deliberately avoided shipped_at` is the
    answerer explaining itself, and vetoing on it would punish the explanation
    rather than the query.

    ONE left-to-right pass, not a comment pass followed by a string pass:
    whichever token opens first wins. Stripping comments first let a `--` or
    `//` inside a string literal (`'https://x'`, a `'2024-01--2024-06'` range)
    blank the rest of the line, and the veto then went silently dark for every
    forbidden name after it on that line. Stripping strings first has the mirror
    bug, on a quote inside a comment.
    """
    return _NOISE.sub(lambda m: " '' " if m.group(0)[0] in "'\"" else " ",
                      malloy)


def _present(name: str, text: str) -> bool:
    r"""`name` as a whole identifier path, or as the tail of a longer one.

    Three things have to hold at once, and the `.` in the old negative
    lookbehind only bought the middle one:

    - `total_sales_2021` must not match `total_sales_2021_adj` -- a longer NAME
      is a different field (the trailing lookahead).
    - `products.retail_price` must not match a bare `retail_price` -- the dot is
      literal, and a leaf on its own is somebody else's field (the `products.`
      is still required).
    - `products.retail_price` MUST match `order_items.products.retail_price` --
      a longer PATH to the same field is the same field. Reaching a joined field
      from the fact source is the ordinary shape in Malloy, and excluding `.`
      from the lookbehind made one join hop enough to walk out of the veto.

    So the leading segments are matched rather than forbidden, and the boundary
    is only in front of the whole path. `x_products.retail_price` still does not
    match: `\w+\.` can consume `x_products.` but then `products.` has to follow
    and does not.
    """
    return re.search(r"(?:^|[^A-Za-z0-9_.])(?:\w+\.)*" + re.escape(name)
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
        if _present(path, text):
            checked.append(path)
            hits.append(entry)
            continue
        leaf = path.rsplit(".", 1)[-1]
        # A bare leaf is reported, never vetoed, and the judge is told the path
        # it came from -- `cost` on its own is a legitimate field on plenty of
        # sources that are not the one the golden forbids.
        if leaf != path and _present(leaf, text):
            checked.append(path)
            leaf_hits.append(entry)
        else:
            # Not `checked`: the entry is going to the judge as prose, and
            # listing it as checked as well said the script had decided it.
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
