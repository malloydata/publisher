# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT
"""What reaches the provider: egress classes, and the predicate guarantee.

Every request the server sends to the (stand-in) provider is captured. The
package has two gated sources whose gate text is easy to grep for.
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from harness import Pub, mock  # noqa: E402

OUT = pathlib.Path(__import__("os").environ.get("VAL_OUT", "/tmp/llm-val/results"))
results: dict = {}
SECRETS = ["analyst", "$ROLE", "$COUNTRY", "authorize", "access_filter"]

FULL_ALL = {
    "egress": {"preset": "full"},
    "enrichment": {"enabled": True, "sourceSummary": {"enabled": True},
                   "keyphrase": {"mode": "always"}},
    "refine": {"enabled": True}, "rerank": {"enabled": True},
    "dimensionalValues": {"mode": "auto", "include": ["*.*"]},
    "response": {"surfaceGenerated": True},
}


def traffic() -> tuple[str, dict]:
    """Everything sent, as one string, and a count by kind."""
    parts, n = [], {}
    for r in mock("/__log"):
        if r["kind"] == "chat":
            parts.append(r["system"] + "\n" + r["user"])
            n[r["stage"]] = n.get(r["stage"], 0) + 1
        elif r["kind"] == "embeddings":
            parts.extend(r.get("texts", []))
            n["embedding texts"] = n.get("embedding texts", 0) + r["n"]
    return "\n".join(parts), n


def drive(p: Pub) -> None:
    p.warm()
    for t in ([("measure", "total revenue")], [("dimension", "customer country"), ("measure", "customers")],
              [("dimensional_value", "Organic")], [("source", "customer accounts")]):
        p.ask(t, override={})


def scan(label: str, retr: dict) -> None:
    mock("/__reset", {})
    with Pub("privacy", retrieval=retr) as p:
        drive(p)
        text, n = traffic()
    found = {s: text.count(s) for s in SECRETS if s in text}
    results[label] = {"requests": n, "predicate text found": found, "bytes": len(text)}
    print(f"   {label:<28} sent {n}  bytes={len(text):>8}  predicate text found: {found or 'none'}")
    return text


print("== the predicate never leaves the machine")
scan("default", {"enrichment": {"enabled": True, "sourceSummary": {"enabled": True}},
                 "refine": {"enabled": True}, "rerank": {"enabled": True}})
full_text = scan("preset full, everything on", FULL_ALL)
for label, retr in [
    ("code class only", {"egress": {"code": True}, "enrichment": {"enabled": True, "keyphrase": {"mode": "always"}}}),
    ("schemaContext only", {"egress": {"schemaContext": True}, "enrichment": {"enabled": True, "keyphrase": {"mode": "always"}}}),
    ("dimensionalValues only", {"egress": {"dimensionalValues": True}, "dimensionalValues": {"mode": "auto", "include": ["*.*"]}}),
]:
    scan(label, retr)

print("\n== the gated sources' NAMES and docs (not their gates) are ordinary model text")
print("   'restricted_users' in traffic under preset full:", "restricted_users" in full_text,
      "| 'Restricted customer accounts' doc:", "Restricted customer accounts" in full_text)

print("\n== which egress classes change what is sent (bytes, and what the field prompt contains)")
def field_prompt(retr: dict) -> str:
    mock("/__reset", {})
    with Pub("privacy2", retrieval=retr) as p:
        p.warm()
        log = [r for r in mock("/__log") if r["kind"] == "chat" and r["stage"] == "keyphrase_batch"]
    return log[0]["user"] if log else ""


BASE = {"enrichment": {"enabled": True, "keyphrase": {"mode": "always"}}}
for label, egress in [("default", {}), ("code on", {"code": True}), ("schemaContext on", {"schemaContext": True}),
                      ("names off", {"names": False}), ("docs off", {"docs": False}), ("preset full", {"preset": "full"})]:
    text = field_prompt({**BASE, "egress": egress})
    has = lambda s: s in text  # noqa: E731
    row = {"prompt bytes": len(text), "field code shown": "Field code:\n(not provided)" not in text,
           "siblings shown": "Sibling fields (schema context):\n(not provided)" not in text,
           "has descriptions": "Description: When" in text or "Description: Total" in text or "Description: Customer" in text}
    results[f"prompt {label}"] = row
    print(f"   {label:<18} {row}")

(OUT / "privacy.json").write_text(json.dumps(results, indent=1))
mock("/__reset", {})
