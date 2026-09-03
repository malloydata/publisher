<!-- Everything about a golden that is wrong, doubted, or out of step with the model. NOT a sixth step, and never improve. -->

# The golden side door

## Golden side door (not a sixth step)

Bad and ambiguous goldens show up immediately. That is not improve. A
checkpoint that mixes model edits and silent golden rewrites is useless for
rollback. Keep hold and repair here, outside the five steps.

## Repair a bad golden

This is **your** job as conductor, after `eval-diagnose` writes
`BAD-REFERENCE`. It is not the answerer's job, and it is not a reason to
change the model.

Diagnosis is not the only way one arrives. The judge also reports a
`gold_status` on every score (`skill:eval-judge`), and a
`suspect` or `verified_wrong` comes through this same door -- earlier, because it
lands during scoring rather than after. Treat it as a `BAD-REFERENCE` with the
judge's `gold_note` as its evidence. Adjudicate it **before** improve runs: a
doubted key sends a modelling agent to fix a model that is already right, which
is the most expensive wrong turn this loop can take.

The judge scored against the golden as written even where it said `suspect`, so
its verdict is still the verdict. Do not re-open a case merely because the flag
is set; open it because you looked and agreed.

1. **Replay, yourself.** Take the stored `final_query` (or a query you can
   justify from the model) and run it with `execute_query`. Write the
   rows to a gold artifact under `evals/<set>/` (never under the served
   package tree). If you cannot produce a trusted key, follow **Hold an
   ambiguous golden** (or mark the golden `invalid` if the question itself is
   unusable). Do not invent a number.
2. **Patch the case** in `cases.jsonl`: new `golden` (status, kind, value or
   path, `canonicalQuery`, `verifiedBy: replay`) and `goldenRevision`
   incremented. Do not edit any old `score` event.
3. **Bump `datasetVersion`** in `set.json`, and commit the ledger change so
   the repair is attributable.
4. **Close the issue as repaired, not as a model fix**: `issue_status: fixed`
   with a note that the *golden* changed.
5. **Open a new run** whose `run.json` records the new `datasetVersion`. It
   is not comparable to runs on the old version without saying so.
6. **Re-score stored queries first**: `skill:eval-answer` without a new
   answerer (saved predictions, fresh judge, new `golden_revision` stamps).
7. **Re-answer only if you still need a blind look** (discoverability, or the
   stored query was itself the thing under test).

Never mix old-golden and new-golden scores in one aggregate. A before/after
that crosses a golden bump is a rebase, not a model delta.

## A model fix can invalidate a golden, and nothing will tell you

A rubric that explains a trap usually has to quote the model -- "this measure
counts line items despite its name, so it yields the trap value". That sentence
is a claim about the model, and it is false the moment the model is fixed. The
judge keeps enforcing it and starts failing correct answers.

A truth-package check cannot catch this, structurally. It re-derives values from
sources that are independent of the model **on purpose**, so a rubric can
describe a model that no longer exists while every value still re-derives green.

Worse, a model fix can move a golden's *value* without touching the data. If a
dimension is defined in terms of the measure you fixed, the concept it names now
resolves to a different population -- same dimension, same question, different
correct answer -- while a canonical truth query still returns the old number
because it encoded the old definition.

So: **after any model edit, re-read the rubrics of every case that names an
entity you touched.** `verify_goldens.py` audits the mechanical part -- it parses
`X is <expr>` out of the model and flags any rubric asserting a different
definition -- but only for definitions it can parse. Prose claims about grain,
population, or convention are still yours to check.

When one turns up it is `BAD-REFERENCE`, and it goes through this side door.
Never let it reach improve: the model is right, and an edit would be damage.

## A golden must match the state the model is in

A case whose golden holds a value asserts that the value is obtainable. If the
model has no trace of the concept, that assertion is false, and the case is now
asking two questions at once: "did the answer contain the golden" (no) and
"should the answerer have complied" (no). Both readings are defensible, so the
verdict stops being a measurement.

Measured, holding the answer, the model and the rubric fixed and varying only
how the case was authored:

| the case says | verdicts over four samples |
|---|---|
| golden holds three counts, model defines no such concept | `match` / `no_match` / `match` / `near_match` |
| `golden.kind: unanswerable`, pass is a refusal that names what is missing | `match` x4 |

The judge is not being unreliable in the first row. It is being asked a question
with two right answers.

So a coverage case has two states and needs a golden for each:

1. **Before the model defines the concept.** `coverage: absent`,
   `golden.kind: unanswerable`. The pass is a refusal that NAMES what is
   missing; inventing boundaries and reporting them as the company's is
   `no_match`. This is the state that measures whether the model documents its
   conventions.
2. **After improve adds it.** Bump `goldenRevision`, replace the golden with the
   real value, bump `datasetVersion`. A refusal is now a failure, and the run
   measures whether the new entity is discoverable.

Never one case straddling both. The straddle is what produces an oscillating
verdict, and no amount of rubric wording fixes it -- four prompt edits were
tried against exactly this case and none of them did.

**This is the mirror of "A model fix can invalidate a golden".** That section
warns that adding a definition can move a golden nobody was working on. This one
warns of the same seam from the other side: a golden written for a model that
does not exist yet is invalid until the model catches up. Both are golden side
door work, and neither is improve.

## Hold an ambiguous golden

Use this when the current key is unusable as a score *and* you cannot justify
exactly one replacement (two honest replays disagree; a window or tie is
unspecified; later samples might confirm a convention).

1. **Do not invent a key.** Leave the old artifact on the case for
   provenance.
2. **Patch the case**: `golden.status: ambiguous` with a `reason` naming the
   defect and the competing replacements (not a new number). Increment
   `goldenRevision`.
3. **Do not score** this case until a later sample confirms a convention or a
   human picks a replacement. Its attempts get `verdict: null,
   reason: golden_ambiguous`.
4. **`issue_status: deferred`**, not `fixed`. Revisit when another case in
   the same neighborhood confirms a convention.
5. Old `score` events stay. They keep the previous `golden_revision` and must
   not enter an aggregate that claims the model failed.

If later evidence makes one replacement obvious, then Repair a bad golden.
