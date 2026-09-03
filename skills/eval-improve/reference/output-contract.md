<!-- What an improve agent must emit, and the golden check it must run first. -->

# Output contract

WHAT YOUR EDIT MAY HAVE DONE TO OTHER CASES

The full case file is {cases_file} -- all of them, not just this cluster's. Per
the skill's Step 4, if your edit changed what an entity MEANS, grep that file
for the entity name and re-derive every golden that depends on it. Report them;
do not repair them. Editing the answer key you are scored against is the one
thing this loop cannot let you do.

Give the skill's report block -- COMPONENT, EVIDENCE, DISAGREEMENT, DIAGNOSIS,
EDIT, EXPERT-TEST, PROBES, GOLDEN-SUSPECT -- and then, as the last thing in your
reply, ONLY this JSON object:

{"files": ["paths you changed"],
  "probes": [{"why": "the claim this checks", "query": "...", "result": "..."}],
  "edit": "one line, or NONE",
  "editTier": 1-5 from the skill's table, or null,
  "disagreement": "NONE, or what the diagnosis got wrong",
  "compiled": true or false,
  "syncedShaChanged": true or false,
  "meaningChanged": ["entities whose meaning changed; [] for a docs-only edit"],
  "goldenSuspect": [{"qid": "...", "entity": "...",
                      "stored": "...", "rederived": "..."}]}
```
