<!-- How to audit a key you doubt, before you change it. Read it when you are about to repair more than one golden, or a key someone else authored. The repair steps themselves are in golden-side-door.md. -->

# Auditing an answer key

`golden-side-door.md` says what to do once you know a key is wrong. This says
how to know. Read it before a sweep, and before touching a set you did not
author.

A golden can be wrong, and not rarely. On one 69-case set an audit changed **10
keys**. Of the five a human then read one by one, **four were wrong for a
different reason than the auditor recorded**. The changes were mostly
defensible; the recorded causes were not. The audit itself needed auditing.

One claim carries the whole procedure:

> **You cannot audit a golden from the golden's text. Open the artifacts and
> re-run the data.**

## Never change the question

The question is what a human asked. It is the stimulus. Only the key is
arguable.

In the audit above, four questions asking for "the frequency distribution" were
edited to "the **reach** frequency distribution", because the model documented
one default and the answerer kept using the other. On three of the four,
nothing else changed: the key was untouched. The edit deleted the thing those
cases existed to test, which was whether the documented default gets followed,
and handed the model a pass.

An ambiguous question is a finding, not an editing job. Report it, or mark the
case `BAD-QUESTION` per `skill:eval-diagnose` and hold it. Do not resolve the
ambiguity by rewriting the question.

Nothing mechanical guards this yet, so read the authored file. An attempt's
`question_sha` hashes the text the answerer saw, which tells you two runs asked
the same thing but follows any edit to the case, so a drifted question and a
faithful one hash alike. A case may carry `questionSha` from the file it was
converted from, and `run_baseline.py` prefers it when present, which is the
hook the guard needs: nothing stamps that field today, and nothing refuses to
start on a mismatch.

## The order to read in

For each case you are considering repairing. Do not skip to step 5.

1. **Read the judge's full reason, not the verdict.** The verdict names no
   cause; the reason usually does, including when the cause is the harness. One
   `near_match` was recorded as "rubric lacked a tolerance on 100%". The judge
   had actually said every clause was satisfied and that it hedged because the
   query was not re-executed against the model. Same verdict, different owner:
   that is a harness limit, not a rubric defect. Watch for the judge citing its
   own constraints, and count them across the arm. In one arm the judge cited
   inability to re-execute in **12 of 35** verdicts. Ten of those still passed,
   which is why nobody noticed.
2. **Read the answer itself**, not the verdict's summary of it. One case was
   marked down for "silently defaulting" to a scope that the answer disclosed
   in its first sentence. No amount of rubric editing finds that.
3. **Read every query the attempt ran, in order.** The trajectory says whether
   the answer was reached honestly. One answer pinned a single group with
   `where:`, which reads as hiding data. The transcript showed it first grouped
   by every group with nothing pinned, saw empty results, checked the measure
   per group, and pinned only then, because one group was all that had data.
   The pin concealed nothing. The reverse happens too: a trajectory that
   arrives by luck is not a pass, and only the queries show it.
4. **Re-run the data yourself**, against the version the run measured and the
   source the attempt used. Both halves bite. An auditor checked one source,
   found a measure returning `NULL`, and wrote "only one group has this data".
   Through the source the attempt actually queried, the other groups returned
   `0`. Different fact, different meaning, and the note stood wrong for a week.
   Pin the version explicitly: an omitted one resolves to whatever the package
   serves as latest, which during an arm with a published baseline is a
   different build from the one under audit. `--scope env/package@version`
   exists for this.
5. **Only now classify, then decide.**

## Four ways a key goes wrong

Different check, different fix. Picking the wrong class produces a repair that
sounds plausible and fixes nothing.

**A. It names something that does not exist.** A tool, a render tag, a field, a
convention. Check by grep and by the tool list the answerer was granted. Seen:
a clause requiring verification through a tool present in neither the model nor
the answerer's tool surface; a clause requiring a render tag that exists in
neither the package nor the language. The clause was ungradeable, so no correct
answer is protected by it, which makes this the safest repair. Fix: name the
mechanism the model does offer.

**B. It grades the mechanism, not the outcome.** The clause says *how* rather
than *what*, so it fails correct answers that arrive another way. The tell is a
clause naming a query construct (`group_by`, an aggregate, a join path), a
render tag, or one particular source. Worst seen: two clauses demanded scoping
via `group_by` where the model documented pinning with `where:` as equivalent,
so correct answers failed for following the model's own documentation. Fix:
rewrite as the observable consequence ("the reported values are non-NULL and
not blended across groups"). Before relaxing, confirm the other mechanism
really is sanctioned, by a line in the model's documentation and not by your
sense of Malloy. If it is not, the clause is right and the answer was wrong.

**C. It is impossible against this data.** Stronger than over-strict, and a
different finding: no answer can satisfy it. Seen: a clause requiring non-NULL
values produced *by grouping* on a dimension, where one of three groups had the
underlying measure populated, so following the clause exactly yields 16 NULL
rows out of 24. Check by running the clause as written, literally. An
impossibility is a far stronger justification for a change than "this seems
over-strict". Fix: require the outcome and let the answerer find the route.
Record the measurement.

**D. It is ambiguous, and the judge can read it two ways.** The signature is a
verdict that flips across runs on an **identical** final query, which is a free
comparison because the queries are already in the ledger. Seen: a numeric
endpoint with no tolerance ("reaches 100%") while a sibling clause in the same
set said "100% (or near it)"; a clause that never said whether the displayed
rows or the claimed rows are judged, against an answer that displayed 16 and
asserted 25. Fix: give numeric endpoints a tolerance, say which artifact is
judged, and delete a clause that restates another in different words. A judge
reads a restatement as an extra requirement.

`golden-side-door.md` has the two classes that are about timing rather than
wording: a rubric that quotes a model since fixed, and a key written for a
model that does not define the concept yet.

## Before you relax a clause

A relaxation makes the test easier, and the auditor is usually the party whose
model is being tested. Require more of yourself for a relaxation than for a
tightening.

**A relaxation needs a measurement or a citation, not an argument.** One of:

- the data, run against the pinned version and the attempt's source, showing
  the clause cannot be met;
- a line of the model's documentation sanctioning what the clause forbids,
  quoted with its file and line;
- proof the referenced thing does not exist: a grep, or the granted tool list.

"This seems over-strict" is not a justification, and neither is "the model
documents the alternative" without the quote. **The tell that you are
rationalizing: your justification is about what the rubric says rather than
what the data shows.** If the note cites no measurement, nothing was audited.
All four of the wrong causes above were reconstructed from rubric text.

## Rule out the other owners first

`skill:eval-diagnose` owns the vocabulary: `dataset`, `model`, `agent-skill`,
`retrieval`. A bad key is `dataset`, and in one audit the same set of failures
spread across all of them. Two of the confusions are worth naming here.

A clause can require knowledge no semantic model could carry. One required
explaining that a metric depends on a setting chosen when the report was
created: true, useful, and not a property of any model. That is `agent-skill`,
in the host's own skill layer, not a defective key and not a model gap.

A harness defect is not an owner, it invalidates the arm. If the judge could
not re-execute, or the index served a different build, or the answerer was
granted fewer tools than production, then every verdict in that arm is suspect.
Fix the harness and re-score before touching any key. Step 1 is where this
usually surfaces.

## Record the measurement, not the reasoning

Each changed key carries a note. "The clause was over-strict" is worthless in
three months. "Measured X against version V through source S, got
0.9999999999999999" is checkable forever. Name the source and the version:
both have burned an audit.

- **Record reverts.** An audit that made a change and undid it is part of the
  history. Hiding it leaves the next reader unable to tell a considered
  decision from an oversight.
- **An audit that changes nothing is still an audit.** Where your finding
  disagrees with the key and the key is right, record the rejection. On one run
  three findings were struck that way, one of them claiming a model recipe
  produced over 100%. It was run. It produced exactly 100.00%.
- **Keep one definition of each fact.** The reason lives on the case, once.
  `set.json` carries only `datasetVersion`, so what answers "which cases moved
  at this version" is the commit that bumped it, which is why
  `golden-side-door.md` has you commit the ledger change per repair. Do not
  restate a case's reason at the set level to build a second copy of that map.

## If the set came from someone else

- Keep the authored file unmodified beside the converted one.
- Verify every question and every clause byte for byte **at conversion time,
  before any repair**, so a later disagreement is about the key and not about
  the transcription.
- Report the honest headline: **N of M keys changed**, and which.
- Send the disagreements back with the original text quoted, grouped by kind,
  and separate the settled defects from the open questions. Some of what looks
  like a defective key is the author asserting a behavior the system does not
  deliver yet, and then the fix is the system.

## Checklist

```
[ ] Question byte-identical to the authored source?          (if not: revert, always)
[ ] Read the judge's full reason, not the verdict
[ ] Read the answer itself
[ ] Read every query in order
[ ] Re-ran the data: pinned version, the attempt's source
[ ] Ruled out model / agent-skill / retrieval owners, and harness defects
[ ] Classified: nonexistent reference / mechanism / impossible / ambiguous
[ ] Relaxation backed by a measurement or a quoted doc line
[ ] Note records the measurement, the source and the version
[ ] datasetVersion bumped, and the ledger change committed per repair
```

A skipped step makes the audit a guess. Four out of four were.
