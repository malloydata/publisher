---
name: eval-diagnose
description: 'Diagnose why a scored answer failed and who owns the fix. Walk dataset, agent-call, get_context/model, get_context/retrieval, construction, then model-definition. Append issue events to the file ledger, linked by traceId. Use after eval-answer, when triaging a run, or before changing a model. Does not edit the model (eval-improve).'
---

# Diagnose One Answer

Consumes a `score` event from `skill:eval-answer` and answers: why did this fail,
and who owns the fix?

**Scope boundary:** write the diagnosis before any edit exists. This skill never
edits a model and never proposes a patch beyond naming the gap. Diagnosis that
is allowed to edit becomes justification for an edit somebody already wanted.

Do not diagnose a contaminated attempt or an environment failure. Those are
harness or ops, not model work.

## Components, in order

Walk **in this order** and stop at the first with positive evidence. A later
label requires ruling out the earlier ones. Write `component` with these strings,
never "C1" / "C2" / "C3":

| `component` | Question |
|---|---|
| `dataset` | Bad question, bad or missing golden, or environment drift? |
| `agent-call` | Did the agent ask for the needed concepts, with the right type and scope? |
| `get_context/model` | Is the needed entity absent, undocumented, weakly labeled, duplicated, or missing guidance? |
| `get_context/retrieval` | Was an on-target request against a well-described entity ranked or grouped wrong? |
| `construction` | Did sufficient context arrive, and the agent still built the wrong query? |
| `model-definition` | Is a measure, join, filter convention, or source semantically wrong? |

`owner` is separate: `model`, `retrieval`, `agent-skill`, or `dataset`. There
is no environment owner: an environment failure stops the run before
diagnosis (see the boundary above), so no issue can carry it.

`construction` requires proving the needed entities and governing guidance were
in the returned context. A server trace proves what Publisher returned, not what
the host kept after compaction. If the rendered tool response is gone, mark
sufficiency `unknown` and do not assign `construction`.

Always report construction eligibility as `eligible / total`. That is a
diagnostic conditional, not a causal comparison.

## Step 1: Extract facts from traces, not from memory

For each `get_context` call, load the stored retrieval trace by the `traceId` on the
`tool_call` event. Write down, before you interpret anything:

- **Asked:** every retrieval utterance, target types, scopes, and result counts,
  in order.
- **Returned:** for each needed entity, whether it appeared, its best
  within-target rank, and under which utterance. Read this off the
  `rankedSummary` on the attempt's `tool_call` events (its `targets` list
  carries per-target ranks); the full trace body is behind your host's
  trace lookup.
  Count from the trace, never from recollection.
- **Used:** sources and fields the final query referenced, and needed entities
  that were returned and then unused.

Resolve aliases to the real source (`join_one: bldg is fac_building` uses
`fac_building`). Count ranks from the trace, not from recollection.

The needed set comes from golden metadata or from the entities the corrected
answer required. Do not invent it from the question's nouns alone.

Presence leads; rank refines. Every needed entity present with a wrong
answer is prima facie `construction`. A needed entity that never appeared is
never `construction`, no matter how wrong the query looks. Everything
present but buried deep under noise the agent reasonably skipped is
`get_context/retrieval` once the request itself was on-target.

## Step 2: Assign one primary code

Use these codes verbatim. Re-wording them destroys the cross-answer pattern.

### Dataset first

| Code | When | Owner |
|---|---|---|
| `BAD-REFERENCE` | the golden itself is wrong, and you can name the defect | dataset |
| `AMBIGUOUS-REFERENCE` | the key is untrustworthy as a score, but a replacement is not uniquely determined (two honest replays disagree; later cases may confirm a convention) | dataset |
| `BAD-QUESTION` | the question is unanswerable as written, or underspecified (ties, rank without order) | dataset |
| `CORRECT-SUPERSET` | every expected row present, plus extra context | none (it passed) |

A cheap tell for a bad golden: impossible magnitude; identical values across
entities that should differ; `SUM` / `COUNT(*)` over a join that duplicates on
both sides. `AVG` / `STDDEV` / `MIN` / `MAX` survive uniform duplication, so
fanout alone proves nothing.

**BAD-REFERENCE and AMBIGUOUS-REFERENCE are first-class outcomes, not awkward
misses.** Goldens often encode assumptions we want in the model. They can also
be wrong. Flag the case when the key is defective or when two justified
replays disagree: do not edit the model to match a bad or unsettled key, and
do not invent a replacement number. `BAD-REFERENCE` goes to Repair a bad
golden. `AMBIGUOUS-REFERENCE` goes to Hold an ambiguous golden. Do not leave
the run looking like the model failed.

This skill **classifies and hands off**. Write the issue with
`owner: dataset` and stop for that case. The conductor (`skill:eval-loop`)
either repairs the golden or holds it as `ambiguous`. Do not capture a
replacement golden from inside diagnosis if you are not also conducting; a
diagnosis that writes a new key without a version bump silently changes what
earlier scores meant.

Prior `score` events are not rewritten. They keep the old `golden_revision`.

### Agent call

| Code | When | Owner |
|---|---|---|
| `NEVER-ASKED` | no utterance targeted a needed concept | agent-skill, and model if nothing would have prompted the ask |
| `VAGUE` | compound or generic utterances, so nothing could rank | agent-skill |
| `QUESTION-VOCAB` | utterances parroted the question where the data uses other words | agent-skill, and model if that vocabulary is undocumented |
| `NO-DISAMBIG` | two plausible candidates, never resolved | model: docs should answer, not require the question |
| `ASSUMED` | assumed a scope or convention instead of checking | model if nothing warned; agent-skill otherwise |
| `WRONG-TYPE-OR-SCOPE` | asked, but with the wrong target type or an empty/wrong scope | agent-skill |

If the agent could not reasonably have known to ask, that is a model gap.

### get_context / model

| Code | When | Owner |
|---|---|---|
| `COVERAGE` | no representing entity anywhere | model |
| `NOT-RETURNED` | it exists, the ask was on target, it never came back | model: labels, docs, synonyms, index |
| `LOW-RANK` | returned, buried under noise the agent reasonably skipped | model |
| `AMBIGUOUS` | several near-identical candidates | model: "use X for …, Y when …" |
| `GUIDANCE-NOT-RETRIEVED` | entities came back, governing guidance did not | model: put guidance on the entities agents search for |
| `GUIDANCE-DECLINED` | guidance was retrieved and judged inapplicable | model: state the business default, not a caveat |

A missing join is coverage, not an agent-call miss. The model has to volunteer
relationships. A declared join is not a retrieval entity; do not look for it in
`get_context` results.

### get_context / retrieval

| Code | When | Owner |
|---|---|---|
| `RETRIEVAL` | model looks right, utterance on target, rank or grouping still failed | retrieval |

Prove it before you use this code: search a distinctive phrase from the entity's
own doc. If a rare token retrieves it and ordinary phrasing does not, say so
with both queries. Otherwise it is still `NOT-RETURNED` / `LOW-RANK`.

### Construction (only after sufficiency)

| Code | When | Owner |
|---|---|---|
| `WRONG-PICK` | needed entity returned, used a different one | model if indistinguishable; agent-skill if docs distinguished them |
| `SCOPE` | right entities, wrong population | model if the scope rule was undocumented |
| `GRAIN` | right entities, wrong grain | model or agent-skill |
| `FILTER-LITERAL` | filter literal did not match stored values | model (document the stored form) and agent-skill |
| `CONVENTION` | right data, wrong statistical or business convention | model: expose a named measure |
| `SYNTAX` | could not express it; execute errors; never submitted | agent-skill |

### model-definition

Use when the entity was found and used, and the definition or the data behind it
is wrong (bad grain, wrong join key, inverted filter). Owner: model. A doc whose
factual claim the data contradicts (a population statement, a grain claim) is
also model-definition: the SQL may be right while the stated contract is false,
and an agent that trusts the doc answers wrongly without ever failing a query.
Probe the claim before writing the issue.

## Step 3: Read the failure shape

| Signature | Look here |
|---|---|
| Extremes match, means do not | Population, filter, or join scope |
| Same row count, values differ | Wrong column or literal, not joins |
| Row count differs, all expected rows present | Superset; often not an error |
| Right keys, wrong aggregates on a minority | Undeclared or wrong-cardinality relationship |
| Off by a clean integer multiple | Fanout; which side of the join is non-unique |
| Zero errors, few calls, fast, confidently wrong | The model steered it |
| Identical high-precision values across entities that should differ | Cross-contamination join |
| A magnitude that cannot be true | Fanout, possibly in the golden |

Mine the agent's prose, not only its calls. It often names the gap.

## Step 4: Append issue events, then stop

Append to `evals/<set>/runs/<runId>/events.jsonl` with `kind: issue`
(shapes in `skill:eval-answer` `reference/ledger-schema.md`):

- `issue_id`, affected `qids`, `primary_code`, `contributing_codes`
- `component`, `owner`, `severity`, `confidence`
- `sufficiency` (`sufficient` / `insufficient` / `unknown`)
- `traceId`s, not copied trace payloads
- `diagnosis`: the suspected shared entity, file, or root cause, written
  before any edit exists

Then `issue_status` with `status: open`. Status is always an event. Readers
take the latest `issue_status` for that `issue_id`.

Cluster duplicates that share an owner, entity, or file before anyone
improves. The issue backlog in the event log is the output, not per-question
prose. Diagnose reads dev cases only; a holdout case with a bad score stays
undiagnosed so the gate keeps something the improve step never saw.

**Only `owner: model` proceeds to `eval-improve`.** Skill findings go back into
the analysis or phrase-detection skill. Retrieval findings go to the tool.
`BAD-REFERENCE` and `AMBIGUOUS-REFERENCE` go to the golden side door in
`skill:eval-loop` (repair or hold). Do not send them to improve. Other dataset
findings (a bad question, a case worth excluding) go back to the case in
`cases.jsonl` via the conductor. Routing a skill bug into the model is
how models accumulate scar tissue.

## Anti-patterns

- Do not diagnose from the answer alone. Probe why a number differed.
- Do not treat a passing answer as uninformative. High call counts on a pass
  still name gaps.
- Do not conclude a model gap from two agents agreeing. They coin-flip onto the
  same undocumented sibling for the same reason.
- Do not assign `construction` when sufficiency is unknown.

## Related skills

- `skill:eval-answer`: the score this consumes.
- `skill:eval-improve`: smallest model edit, `owner: model` only.
- `skill:eval-loop`: golden hold/repair, the gate, and checkpoint.
