---
name: eval-improve
description: 'Make the smallest safe Malloy model edit that closes a diagnosed model-owned gap, with a probe receipt for every factual claim. Use after eval-diagnose, or when asked to fix a model so an agent can discover the right answer. Never accepts its own edit; the acceptance check belongs to eval-loop. Does not decide whether an answer was wrong (eval-answer) or why (eval-diagnose).'
---

# Improve the Model

Takes an issue with `owner: model` and produces **one smallest edit** that closes
the gap. Every factual claim is backed by a query you ran.

**Two hard boundaries:**

1. **No diagnosis evidence, no edit.** If the issue cannot name a concrete gap
   with a trace or probe, record that and stop. Edits from an empty diagnosis
   have been the inert and wrong ones.
2. **This skill never accepts its own edit.** You propose and verify. The
   acceptance check
   in `skill:eval-loop` admits or reverts. An improver writing the query it
   already knows proves the fix is possible, not that the next blind agent
   will find it.

## Step 0: What the evidence entitles you to change

| Evidence | Edits permitted |
|---|---|
| Verified golden, or a user who states the answer | Any tier. Probes required. Check the golden first. |
| Wrong answer, then a corrected one the user accepted | Prefer docs over structure. The diff between attempts is the missing knowledge. |
| User accepted, later contradicted | Docs, labels, index only. No structural change. |
| Doubt only, or retrieval-only (no verdict) | Docs, labels, index only, and only where the transcript shows a concrete confusion. |
| Silence | **No edit.** |

Do not edit for `BAD-REFERENCE` or `AMBIGUOUS-REFERENCE`. Those are the
golden side door in `skill:eval-loop`: repair or hold the golden, bump
`goldenRevision` on the case, and open a new baseline run. Being right and unmatched
beats encoding a defect or an unsettled key. Do not edit for a skill,
retrieval, or dataset owner.

## Step 1: What a correct answer may teach

Encode what a domain expert would volunteer unprompted: systems of record,
vocabulary to stored codes, what a metric means and at what grain, which
relationship is the real one.

The expert test, per edit: *would a domain expert have said this about their
data with no question in front of them?* Reject:

- a field that hard-codes this question's filter and serves no other question
- this question's text, qid, or expected numbers in a doc, comment, or name
- a join copied from gold SQL that you have not probed as a real relationship

The golden is a hypothesis source. The data is still the verifier.

## Step 2: Probe receipts

Every structural claim needs a query you ran: join key, primary key, filter
value existence, snapshot assumption, value space, cardinality.

```sql
SELECT COUNT(*), COUNT(DISTINCT col) FROM t;
SELECT a.k, COUNT(*) FROM a JOIN b ON … GROUP BY 1 ORDER BY 2 DESC;
SELECT col, COUNT(*) FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

A false `primary_key` compiles and silently corrupts every aggregate. Of one
pilot's 11 accepted edits, 4 of 5 wrong ones died to a single
`COUNT(*)` vs `COUNT(DISTINCT …)` probe that was never run.

Compile-check the edit before saving (scope `file` for an edit), then reload
the package. Confirm it is not serving a stale model.

This step needs a target you control: a local server, or a host that can
execute a draft. A run whose answerers queried a published model cannot be
improved in place, because publishing to score an edit is not something this
loop does. `skill:eval-loop` picks the target before the run starts, so if you
have arrived here against a published target, stop and say so rather than
publishing.

Know which copy of the file the server actually reads. Hosts commonly serve a
copy of the package rather than your working tree, so editing the model repo
and reloading recompiles the unchanged copy: the reload succeeds, nothing
changes, and a verification probe quietly tests the old model. Confirm the
edit reached what is served before you trust a probe, and keep the model repo
the source of truth that gets committed. On open-source Publisher the served
copy lives under `publisher_data/<env>/<pkg>/` unless the environment is
watch-mounted; other hosts distinguish a draft from a published version.

## Step 3: One smallest edit

Prefer edits that add no entities. New sources compete for retrieval and
displace answers that already worked.

| Rank | Edit |
|---|---|
| 1 | Disambiguating doc on confusable siblings: "use X for …, Y when …" |
| 2 | Named dimension or measure in user vocabulary |
| 3 | Doc reword, rename, or `#(index)` annotation |
| 4 | Declared join on a *probed* key |
| 5 | A new source: last resort, at most one |

Make the correct thing the default. Guidance phrased as a caveat
("pair with X", "note that Y also includes Z") is retrieved, read, and
declined. A source parameter or named measure that is already the safe
scope does not invite a judgment call.

You cannot append guidance to every field for free. Doc length trades
against the entity's own rank. A declared join is invisible to retrieval;
put the rule on the entities agents search for.

Follow the `malloy-gotchas-modeling` skill so the edit does not introduce a
new modeling mistake. `improve.py` installs it with the rest of the `modeling`
manifest group, so it is loaded alongside this skill rather than reached from
here.

## Step 4: Check what your edit did to the answer key

An edit that changes what a field *means* can silently invalidate goldens for
questions you were not working on. The rubric still describes the old meaning,
the stored value is still the old number, and **nothing fails** -- the case just
starts scoring wrong, against the model, in the direction of your edit. The set's
own value re-derivation will not catch it either, because it re-runs a
`canonicalQuery` that encodes the same stale definition.

Real instance: fixing `lifetime_orders` from line items to distinct orders was
correct and targeted. It also silently moved `top_customer` (defined over it)
from 108 customers to 87, and left two rubrics asserting the pre-fix behaviour.
Two correct answers were marked wrong for a full run before anyone noticed.

So before handing off, for **every entity whose meaning you changed** -- not
every entity you touched; a doc reword changes no meaning:

1. Grep the case file for the entity name. Any rubric, `canonicalQuery` or
   stated value that mentions it is now in question.
2. For each hit, re-derive the value under the new definition and compare it to
   the stored golden. Different means the golden is stale, not that you are
   wrong.
3. Run the set's golden verification if it has one, which catches the mechanical
   subset (drift, and rubric sentences that contradict the model).

Report every hit as `golden_suspect` in the handoff, with the entity, the case,
and the old and new values. **Do not repair them yourself.** Goldens are the
side door in `skill:eval-loop`, and an improver that edits the answer key its own
edit is scored against has removed the only independent check on the edit.

A non-empty `golden_suspect` list blocks the acceptance check until the
conductor settles
each one, because a rerun against stale goldens measures nothing.

## Step 5: Verify, report, hand off

Compile, reload, run one trivial query against each source you touched.
Append a `candidate` event to the run's `events.jsonl` (shape in
`skill:eval-answer` `reference/ledger-schema.md`): the files touched, a
one-line diff summary per file, the issue_ids, probe receipts, and this
report. Every proposal gets its event, accepted or not; a rejected direction
keeps its record. Then stop and wait for the acceptance check in `skill:eval-loop`. That
skill writes the `acceptance_check` event, accepts or reverts, and **only on accept**
checkpoints. This skill never checkpoints and never self-accepts.

```
COMPONENT / PRIMARY_CODE / OWNER
EVIDENCE:    class you worked from, and how it limited the edit
DISAGREEMENT: NONE, or anything in the diagnosis probing showed was wrong
DIAGNOSIS:   2-3 sentences, written before the edit
EDIT:        one line, or NONE with why
EXPERT-TEST: the business fact this encodes
PROBES:      each probe query and its result
GOLDEN-SUSPECT: NONE, or one line per case: qid, entity, stored -> re-derived
```

`DISAGREEMENT` is load-bearing. An improver that cannot push back encodes
its instructions' mistakes. Report from the files on disk, not from memory.

## Related skills

- `skill:eval-diagnose`: the issue this requires.
- `skill:eval-loop`: the acceptance check that accepts or reverts, then
  checkpoints on accept. Golden hold/repair lives there, not here.
- `skill:eval-answer`: scoring after a blind re-answer.
- The `malloy-gotchas-modeling` skill: mistakes an edit must not introduce.
  It arrives with the `modeling` manifest group, not the `eval` group.
