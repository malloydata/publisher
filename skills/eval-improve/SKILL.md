---
name: eval-improve
description: 'Make the smallest safe edit to a semantic model that closes a diagnosed gap, backed by data probes. Use after eval-diagnose attributes a failure to the model, when asked to fix a model so an agent answers correctly, to encode a business convention or metric definition an agent got wrong, or to disambiguate confusable fields. Requires a written diagnosis first and a probe receipt for every factual claim. It does NOT decide whether an answer was wrong (eval-answer) or why (eval-diagnose), and it never accepts its own edit.'
---

# Improve the Model

Takes a diagnosis with `owner: model` and produces **one smallest edit** that closes the gap. Every
factual claim in that edit is backed by a query this skill ran.

**Two hard boundaries:**

1. **No diagnosis evidence, no edit.** If the diagnosis cannot name a concrete gap with evidence,
   record the finding and stop. In the predecessor pilot the four edits made from an empty diagnosis
   were exactly the inert and wrong ones: the improver improvised, competently, into fiction.
2. **This skill never accepts its own edit.** It proposes and verifies; the gate that admits the
   edit belongs to the orchestrator. An improver verifying its own fix proves the fix is *possible*,
   not that it is *discoverable*: it writes the query it already knows to write. One edit passed
   its author's own verification and the next blind agent ignored it entirely, costing two versions
   before anyone noticed.

## Step 0: What does your evidence entitle you to change?

**Evidence strength bounds edit risk.** This is the rule that stops a self-improving loop from
compounding its own errors.

| `evidence_class` | What you have | Edits permitted |
|---|---|---|
| `stated_answer` | A known-correct answer: benchmark gold, or a user who says "the answer is X" | Any tier, probes required. Check the answer itself first. |
| `resolved_after_iteration` | A wrong answer, then a corrected one accepted under scrutiny | Any tier, probes required, target treated as provisional; prefer docs over structure. **The diff between attempts is the signal**: what changed is the knowledge the model lacked, often supplied verbatim by the user's nudge. |
| `accepted_but_suspect` | An answer a user accepted, later contradicted out of band | Docs, labels, index only. **No structural change on an inferred target.** |
| `doubt_only` | A user was unsure; nobody knows the right answer | Docs, labels, index only, and only where the transcript shows a concrete confusion. |
| `retrieval_only` | No verdict on the answer at all | Docs, labels, index only. Never structural. |
| `none` | An answer was delivered and nothing contradicts it | **No edit.** Silence is not evidence of success, and improving on silence is how models drift. |

## Step 1: What a correct answer legitimately teaches you

A known-correct answer stands in for a domain expert in the room. **Encoding human knowledge is the
entire point**: some conventions are simply not inferable from the data, and a model restricted to
data-derived facts will document its way *away* from the truth. This was measured: probing said one
table was the complete population; the business reported from a different one; the data-only version
of this skill wrote a confident, wrong recommendation and the next agent obeyed it.

**Encode** what such an expert would volunteer: business conventions and systems of record;
vocabulary → data mappings (a term's stored code, abbreviation, or flag value); what a metric means
and at what grain; which relationship between two tables is the real one.

**The expert test: apply it per edit:** *would a domain expert have volunteered this, unprompted,
about their data?* If it is only "the answer to the question in front of me," it does not go in:

- a dimension or measure hard-coding this question's filter value, serving no other question
- this question's text, id, or expected numbers, in a doc, comment, or name
- a join copied from a reference answer that you have **not** verified is a real relationship

**The correct answer is a hypothesis source; the data is still the verifier.**

## Step 2: Probe receipts, for every factual claim

Every structural claim: a join key, a primary key, a filter value's existence, a snapshot or
latest-record assumption, a value space, any cardinality asserted in a doc: needs a query you ran,
with its result recorded.

This is the most expensive lesson in this project's history. Of one pilot's 11 accepted edits, 5
were wrong, and **4 of those 5 die to a single `COUNT(*)` vs `COUNT(DISTINCT …)` probe**: a false
primary key at 19,378/20,000; a filter on a column that is 100% NULL; a "latest snapshot" filter
over a table with exactly one load date. The improver asserted all of them in doc prose without ever
querying the data. A false `primary_key` compiles perfectly while silently corrupting every
aggregate downstream.

```sql
SELECT COUNT(*), COUNT(DISTINCT col) FROM t;                     -- is this actually a key?
SELECT a.k, COUNT(*) FROM a JOIN b ON … GROUP BY 1 ORDER BY 2 DESC;  -- what does the join multiply?
SELECT col, COUNT(*) FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 5;  -- the stored form of a value
```

## Step 3: One smallest edit

In preference order. **Prefer edits that add no entities**: every new source competes for retrieval
and can displace answers that already worked. Retrieval displacement, not incorrectness, is the real
regression mechanism: in one run, 18 new sources shifted which source agents landed on for
previously-passing questions.

| Rank | Edit | Risk |
|---|---|---|
| 1 | Disambiguating doc text on confusable siblings: "use X for …, Y when …" | safe |
| 2 | A named dimension or measure encoding a business rule, in user vocabulary | medium |
| 3 | Doc reword / rename / index annotation | safe / medium |
| 4 | A declared join on a **probed** key | medium |
| 5 | A new source: last resort, at most one, only if no existing source can carry the fix | high |

**Make the correct thing the default, rather than documenting that the default is wrong.** This is
the highest-leverage lesson available. The same gap was attacked three ways: guidance in a source
doc (never retrieved); the same guidance moved onto the queried fields (retrieved, read, and
**declined** as inapplicable); and finally a source parameter making the safe scope the default
(fixed it outright, and made the docs *shorter*). A doc phrased as a caveat: "pair with X", "note
that Y also includes Z": invites a judgment call a reasonable agent can decline. A doc stating the
business default does not.

Two corollaries, both measured:

- **You cannot append guidance to every field for free.** Doc length trades against the entity's own
  retrieval rank; a 64-character pointer dropped a dimension out of the top 12 for its own natural
  query. Pick the fields whose rank you can afford to spend.
- **A declared join is invisible to retrieval.** It appears in no retrieval result, so a join's doc
  is an unreachable place to put a rule. Put the rule on the entities agents actually search for.

## Step 4: Verify, report, and hand off

Confirm the model compiles by running one trivial query against each source you touched. Then report
in this shape: short, because long sessions get interrupted and an unreported edit is an
unreviewable one:

```
PRIMARY_CODE / CONTRIBUTING_CODES / OWNER
EVIDENCE-CLASS:  the class you worked from, and how it limited the edit
DISAGREEMENT:    NONE, or anything in the diagnosis handed to you that probing showed
                 to be wrong: including the tier rubric itself
DIAGNOSIS:       2-3 sentences, written to file BEFORE the edit
EDIT:            one line, or NONE with why
EXPERT-TEST:     which business fact a domain expert would have volunteered, that this encodes
PROBES:          each probe query and its result, verbatim
```

**`DISAGREEMENT` is not a formality.** Across one run the improving agent contradicted its
orchestrator four times and was right four times: it declined to document a false claim about value
casing, corrected a constraint the orchestrator had wrongly treated as binding, found an undeclared
language dependency, and corrected a stated row count. Probe receipts are what made that possible.
An improver that cannot push back is an improver that encodes its instructions' mistakes.

**Report from the file, never from memory.** An improver subagent died mid-run twice on API errors,
once with its edit already written to disk. Whoever gates the edit should diff the model against the
last snapshot to learn what actually happened, and treat the report as commentary.

## What this skill must never do

- **Edit for a `C0-BAD-REFERENCE`.** Never change a model so it reproduces a defect in the reference
  answer: that trades one wrong answer for a model that misleads every future question. Being right
  and unmatched beats being wrong and matched.
- **Edit for a skill or retrieval owner.** Those findings improve the agent or the tool, not the
  customer's model. Routing them into the model is how models accumulate scar tissue for problems
  they never had.
- **Accept its own edit.** See the top of this file.

## Related skills

- `skill:eval-diagnose`: produces the diagnosis this requires, and owns the category codes.
- `skill:eval-answer`: owns the ledger schema this appends `action` to.
- `skill:malloy-gotchas-modeling`: the modelling mistakes an edit should not introduce.
