<!-- What a diagnose agent must emit. Read this before writing your reply. -->

# Output contract

Both shapes below are read by a script. Emit the object as the LAST thing in
your reply, with nothing after it. Prose before it is fine and expected -- the
reasoning is what the codes have to follow from.

## Per case (Step 1-4)

```json
{"probes": [{"why": "the claim this checks", "query": "query or search text",
             "result": "what came back, briefly"}],
  "reasoning": "how the ladder resolved: what you ruled out, and why",
  "component": "one of the six",
  "primary_code": "one code, verbatim from the skill",
  "contributing_codes": ["zero or more, verbatim"],
  "owner": "model | retrieval | agent-skill | dataset",
  "sufficiency": "sufficient | insufficient | unknown",
  "severity": "high | medium | low",
  "confidence": "high | medium | low",
  "diagnosis": "the suspected entity, file, or root cause, in one or two sentences",
  "sharedWith": "a short phrase naming what other cases would share this cause"}

`probes` must be non-empty: it is the record that you checked rather than
assumed. `reasoning` precedes the codes because the codes must follow from it.
```

## Per run, clustering (Step 5)

```json
{"clusters": [
  {"cluster_id": "short-kebab-slug",
    "qids": ["every case in this cluster"],
    "owner": "model | retrieval | agent-skill | dataset",
    "component": "the shared component",
    "codes": ["the primary codes present"],
    "rootCause": "one or two sentences: the ONE thing explaining all of them",
    "evidence": "why these belong together, and what would prove it wrong",
    "confidence": "high | medium | low"}
 ],
 "reasoning": "what you considered merging and chose not to, and why"}

Order clusters by the number of qids, descending. Every diagnosed case must
appear in exactly one cluster; a case that shares a cause with nothing else is
a cluster of one.
```
