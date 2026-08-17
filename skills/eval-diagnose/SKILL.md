---
name: eval-diagnose
description: 'Diagnose why an evaluated answer failed, and whose fault it is: a gap in the semantic model, in what the agent searched for, or in the query it built. Use after eval-answer produces a failing record, when asked "why did that answer go wrong", "is this a model problem or an agent problem", when triaging eval results, or before changing a model in response to a bad answer. Assigns one primary category code and an owner. It does NOT change the model (that is eval-improve).'
---

# Diagnose One Answer

Consumes a record from `skill:eval-answer` and answers: **why did this fail, and who owns the
fix?** Output is one primary category code, contributing codes, an owner, and the evidence that
settles it: appended to the same ledger record.

**Scope boundary, enforced:** this skill never edits a model and never proposes a specific edit
beyond naming the gap. Diagnosis that is allowed to edit becomes justification for an edit somebody
already wanted: in the predecessor pilot, the four edits made from an *empty* diagnosis were
exactly the inert and wrong ones. **Write the diagnosis to a file before any edit exists.**

## Three components, in order

Every failure lands in one of three places. Walk them **in order** and stop at the first with
positive evidence: a later-component label requires ruling out the earlier ones.

| | Component | Question |
|---|---|---|
| **C2** | The agent's search | Did it ask for the right things? |
| **C1** | The model + retrieval | Given a good request, did the right entities come back, well ranked? |
| **C3** | The agent's construction | Given the right entities, did it build the right query? |

**A C3 label requires demonstrating the needed context was actually in the window.** Otherwise it is
a C1 or C2 failure wearing a disguise, and the fix goes to the wrong place.

## Step 1: Extract the facts, before interpreting any of them

Diagnosis kept coming out ad-hoc because each answer re-derived the same facts by hand and got them
subtly wrong. Extract this set first, every time, and write it down before you interpret anything:

**Component 2: what was asked.** Every retrieval utterance in order, whether each call was scoped,
and how many results each returned.

**Component 1: what came back.** For each needed entity: was it returned at all, at what **best
rank**, and **via which utterance**. Then `context_recall` = the fraction of needed entities that
appeared in any result.

**Component 3: what was used.** Which sources the final query actually referenced, and which
needed entities were returned and then *not* used.

Two traps in this extraction, both of which produced wrong diagnoses:

- **Resolve aliases to their real source.** `join_one: bldg is fac_building` uses `fac_building`;
  recording only `bldg` hides which of two confusable siblings the query actually touched: and
  sibling confusion is the largest single failure class. An earlier tool made exactly this mistake.
- **Count ranks from the results, not from memory.** Hand-counting positions across a transcript
  with a hundred-plus results is where this goes wrong. If the transcript is large enough that
  careful counting is unreliable, write a short script to extract it: that is a sensible use of
  code, not a failure.

`context_recall` is the **leading indicator**: it should move before end-to-end score does.

`context_recall` is the **leading indicator**: it should move before end-to-end score does. An
answer with score 0 and context recall 1.0 is *prima facie* C3. An answer with low context recall
is C1 or C2 no matter how wrong the query looks.

The `--needed` set is what makes this attribution rather than description. Deriving it is the
caller's job: gold metadata in a benchmark; in the wild, the entities the corrected answer required.

## Step 2: Assign one primary code

Use these codes **verbatim**. Re-wording them destroys the cross-answer pattern signal, which is
the entire reason they are stable strings.

### Q0. Was it a real failure?

| Code | When | Action |
|---|---|---|
| `C0-BAD-REFERENCE` | the stated correct answer is itself wrong | No edit. Record the probe that shows it. |
| `C0-CORRECT-SUPERSET` | every expected row present, plus extra context | No edit. It passed. |

Check this **first**. A reference answer is evidence, not scripture, and editing a model to
reproduce a defect trades one wrong answer for a model that misleads every future question. Cheap
tells: an impossible magnitude; identical values across entities that should differ; a `SUM` or
`COUNT(*)` over a join whose key duplicates on both sides. Note that `AVG`/`STDDEV`/`VARIANCE`/
`MIN`/`MAX` survive uniform duplication unchanged, **so a fan-out alone proves nothing**.

### Q1. Did the agent ask well? (C2)

| Code | When | Owner |
|---|---|---|
| `C2-NEVER-ASKED` | no utterance targeted a needed concept | skill: **plus** model, if nothing would have prompted the question |
| `C2-VAGUE` | compound or generic utterances, so nothing could rank | skill |
| `C2-QUESTION-VOCAB` | utterances parroted the question's words where the data uses different vocabulary | skill: **and** model, if that vocabulary is undocumented |
| `C2-NO-DISAMBIG` | two plausible candidates returned, and it never resolved which | **model**: docs should answer the question, not require it to be asked |
| `C2-ASSUMED` | assumed a scope, population, or convention instead of checking | model if nothing warned it; skill otherwise |

**The asymmetry that matters:** if an agent *could not reasonably have known to ask*, that is not a
skill problem: the model has to volunteer the fact. Do not park a real model gap in the skill
channel.

### Q2. Did retrieval return well? (C1)

| Code | When | Owner |
|---|---|---|
| `C1-COVERAGE` | the concept has no representing entity anywhere in the model | **model** |
| `C1-NOT-RETURNED` | it exists, an on-target utterance was issued, it never came back | **model**: labels, docs, synonyms, index |
| `C1-LOW-RANK` | returned, but buried under noise the agent reasonably skipped | **model**: sharpen the doc, consolidate competitors |
| `C1-AMBIGUOUS` | several near-identical candidates, nothing to choose between | **model**: disambiguating docs ("use X for …, Y when …") |
| `C1-GUIDANCE-NOT-RETRIEVED` | the *entities* came back but the *guidance* governing their correct use did not | **model**: move guidance onto the entities agents search for |
| `C1-GUIDANCE-DECLINED` | guidance was retrieved, read, and consciously judged inapplicable | **model**: state the business **default**, not a caveat |
| `C1-RETRIEVAL` | model looks right, utterance on target, still failed | **not a model fix**: route to the retrieval/tool channel with the probe |

Before choosing `C1-RETRIEVAL`, prove it: search a distinctive phrase from the entity's own doc. If
a rare token retrieves it but ordinary business phrasing does not, that is embedding dilution: say
so, with both queries.

**Entities being returned does not mean the guidance governing them was returned.** That distinction
is `C1-GUIDANCE-*`, and it is invisible unless you look for it specifically.

### Q3. Did the agent select and build well? (C3)

| Code | When | Owner |
|---|---|---|
| `C3-WRONG-PICK` | a needed entity was returned and it used a different one | model if the two were indistinguishable (also `C1-AMBIGUOUS`); skill if the docs did distinguish them |
| `C3-SCOPE` | right entities, wrong population or filter scope | model if the scope rule was undocumented; else skill |
| `C3-GRAIN` | right entities, wrong grain or aggregation level | model (grain docs, a pre-aggregated source) or skill |
| `C3-FILTER-LITERAL` | a filter literal did not match stored values | model (document the stored form) + skill (verify literals) |
| `C3-CONVENTION` | right data, wrong statistical or business convention | **model**: expose a named measure so nobody guesses |
| `C3-SYNTAX` | could not express it; execute errors, or never submitted | skill / harness |

### Code → tier

The ledger carries both a fine-grained `primary_code` and a coarse `tier`. Derive one from the other;
never assign them independently:

| Tier | Codes |
|---|---|
| `T1a` coverage | `C1-COVERAGE`, and **any missing relationship** |
| `T1b` retrieval recall | `C1-NOT-RETURNED`, `C1-LOW-RANK`, `C1-GUIDANCE-NOT-RETRIEVED` |
| `T1c` retrieval precision | `C1-AMBIGUOUS`, `C1-GUIDANCE-DECLINED` |
| `T2` call quality | `C2-*` where the owner is skill |
| `T3` construction | `C3-*` where the owner is skill |
| `none` | `C0-*` |

**An agent cannot search for a relationship that was never declared.** A missing join is `T1a`
coverage, not `T2`: the model has to volunteer it. This exact mislabel happened twice in a row
because the correction lived in one of two prompt files. It is stated here once, and nowhere else.

## Step 3: Read the failure's shape, and the agent's own prose

The shape usually names the place to look before you read a single line of the query:

| Signature | What it almost always means |
|---|---|
| **Extremes match, means don't**: `min`/`max`/`range`/`count` exact while `avg`/`sum`/`variance` differ | Formulas were right, the **row set** was wrong. Look at population, filter scope, join scope. |
| **Same row count, values differ** | Right grain and filters, **wrong column or literal**. Do not go looking for joins. |
| **Row count differs, all expected rows present** | A superset: probably correct. Extra context is not an error. |
| **Right keys, wrong aggregates on a minority of rows** | An undeclared or wrong-cardinality relationship affecting some entities. |
| **Off by a clean integer multiple** | Fan-out. Find which side of the join is non-unique. |
| **Zero errors, few calls, fast, confidently wrong** | **The model steered it wrong.** The most dangerous shape, because everything looks healthy. |
| **Identical high-precision values across entities that should differ** | Cross-contamination: a join on a shared non-identifying attribute pooling other entities' rows. |
| **A magnitude that cannot be true** | Fan-out somewhere: possibly in the reference answer itself. |

**Mine the agent's reasoning, not only its calls.** The transcript often states the diagnosis
outright: *"the model has no pre-declared joins, so I had to bridge two tables"* (missing join);
*"I used X because it is the complete inventory"* (sibling ambiguity resolved by guesswork);
*"the stored value is abbreviated, so I wrote the filter exactly as the question states"* (it saw
the mismatch and deferred to the question anyway: both a model gap and a skill gap).

## Step 4: Write the diagnosis, then stop

Append to the ledger record (see `skill:eval-answer` for the schema): `tier`, `primary_code`,
`contributing_codes`, `owner`, `context_recall`, and `diagnosis`: the evidence that settles it,
quoting the utterance, rank, or entity.

**Only `owner: model` proceeds to `eval-improve`.** Everything else means no model edit: skill and
retrieval findings go to their own channel, where they improve the agent or the tool rather than the
customer's model. This separation is what keeps a model from accumulating edits that paper over
harness problems.

## Anti-patterns

- **Don't diagnose from the answer alone.** Two of one run's best edits came from probing *why* a
  number differed; one would have been misdiagnosed as an arithmetic error.
- **Don't trust your own flag list.** A fan-out heuristic produced a false positive on the question
  that yielded that run's best edit. Verify before excluding.
- **Don't treat a passing answer as uninformative.** One passing answer spent 18 retrieval and 27
  execute calls and named two real model gaps in its reasoning. Efficiency is a signal: a drop in
  calls per answer across versions is a real improvement even when score is flat.
- **Don't conclude from one blind agent.** Two blind agents facing the same undocumented sibling
  pair coin-flip onto the same wrong source for the same reason, so agreement is not evidence about
  a model gap.

## Settled: do not re-raise

- **Float representation noise cannot fail a correct answer.** Numerics compare under relative
  tolerance, not as strings. Tested; do not contort a measure chasing last-digit agreement.
- **A declared join is not indexed as an entity.** It appears in no retrieval result, scoped or
  unscoped, so a join's doc is an invisible place to put a rule. Verified against a model that
  declared one.
- **Doc length trades against the entity's own rank.** Appending a 64-character pointer to a
  dimension dropped it out of the top 12 for its own natural query. You cannot append guidance to
  every field for free.
- **Scoped vs unscoped calls: judge per query, not per call style.** An earlier version of this
  guidance claimed scoped calls always return the scoped source's own entity (carrying its doc)
  while unscoped calls do not. **Re-measured 2026-08-09: that is wrong.** A scoped call returns the
  source entity only when that source is *relevant to the query*: a scope on one source returned
  0 source-entities in 12 results for one query and 1 for another. Do not infer what an agent could
  see from whether it scoped.

## Related skills

- `skill:eval-answer`: produces the record this consumes, and owns the ledger schema.
- **eval-improve**: the smallest model edit that closes a gap this skill diagnosed, for
  `owner: model` only.
