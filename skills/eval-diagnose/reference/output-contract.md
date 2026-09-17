<!-- What a diagnose agent must emit. Read this before writing your reply. -->

# Output contract

Both shapes below are read by a script. Emit the object as the LAST thing in
your reply, with nothing after it. Prose before it is fine and expected -- the
reasoning is what the codes have to follow from.

**Two objects, two jobs. Emit only yours.** Per-case diagnosis emits the first;
run-level clustering emits the second. They are not interchangeable and the
validator for each rejects the other, so a reply in the neighbouring shape is
dropped no matter how good the analysis inside it is. This has been the single
largest source of wasted per-case work: the prompt named the file and not the
section, and the clustering object, documented second, is the one an agent has
read most recently when it starts emitting.

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

`cluster_id` names the DEFECT, never the remedy. An identifier is an
interface: the moment it names a fix, the fix has been chosen by whoever wrote
the slug rather than by whoever weighs the alternatives, and it reaches the
improve step as a decision nobody made deliberately. A real case:
`index-measures-missing-rounding` prescribed rounding a stored measure, when
the package expressed number formatting through render tags in 28 other places
and rounding the value would have destroyed precision for anything computing
with it. `index-values-not-whole` states the same defect and leaves the choice
open. Any id containing `missing`, `should`, `add`, `fix` or `use` is a
prescription.
```
