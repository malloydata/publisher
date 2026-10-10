#!/usr/bin/env python3
"""Pull JSON objects out of a model's prose reply. Stdlib only.

Both judges in this loop ask an agent for a JSON verdict and get back prose
with the object somewhere in it, and both must survive the agent quoting a
Malloy snippet on the way: `re.search(r"\\{.*\\}", re.S)` is greedy, spans the
FIRST brace in the document to the LAST, and hands `json.loads` a blob that
starts mid-query. A good reply then reads as unparseable, and the case leaves
every bucket -- including the denominator the pass rate is printed over.

It lives in its own module because the two callers cannot import each other:
`check_coverage` already reaches into `run_baseline` for `BLOCKED_TOOLS`, so
`run_baseline` importing `check_coverage` back is a cycle. They each had their
own scanner, written months apart, differing in algorithm and agreeing only by
luck -- the shape of drift the rest of this harness spent several commits
removing. One definition, imported by both, is the fix.
"""
from __future__ import annotations

import json
from typing import Any


def json_objects(text: str) -> list[dict[str, Any]]:
    """Every JSON object in `text`, in the order they appear.

    Decoded from each `{` with `raw_decode` rather than matched with a regex,
    so a brace that opens no valid object costs one character of scanning
    instead of corrupting the match. A caller that wants the verdict takes the
    LAST object naming one: the judge is told to end with it, and prose
    reasoning toward it may quote a fragment on the way.
    """
    dec, out, i = json.JSONDecoder(), [], 0
    while (i := text.find("{", i)) >= 0:
        try:
            v, end = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            i += 1
            continue
        if isinstance(v, dict):
            out.append(v)
        i = max(end, i + 1)
    return out
