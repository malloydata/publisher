# Ledger schema (v1)

The ledger is the contract between the evaluation stages. `eval-answer` **creates** each record;
`eval-diagnose` and `eval-improve` **append** their own fields to it. This file is the single
definition: other skills reference it by invoking `skill:eval-answer`, they do not restate it.

## Format

**JSONL**: one JSON object per line, one line per answer, append-only, one file per run
(`<run>/ledger.jsonl`).

Append-only JSONL rather than a database or CSV, for three reasons that were all paid for:

1. **It survives a crash mid-run.** Improver subagents died on API errors twice in one run. A torn
   write costs one answer, not the file.
2. **A record is complete when written.** No update-in-place means no half-updated row, and no
   ordering assumptions between stages.
3. **End-of-run numbers come from a query, never from agent arithmetic** :
   `duckdb -c "select avg(f1) from read_json('ledger.jsonl') where phase='final'"`.

## Why this is versioned and validated

The first seven runs of this loop wrote **41 distinct fields, of which only 11 appeared in every
run.** `precision`, `recall`, `matched`, and `final_query`: the evidence you need to re-derive a
verdict: were absent from two runs entirely. Diagnostic fields appeared only after the taxonomy
that named them was invented, mid-experiment.

That is survivable for one person's notebook and fatal as a contract between skills: an unenforced
schema means cross-run comparison breaks silently, which is the exact failure mode the predecessor
pilot died of. Validate every record before appending it, and bump `schema_version` when the
required set changes.

## Required: written by `eval-answer`

Every record has these. A record missing any of them is invalid, not merely sparse.

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | `1` for this document. |
| `qid` | string | Stable answer id. With `sample`, uniquely identifies one answer. |
| `sample` | int \| null | Which repeat this is. `null` for k=1. **Required even when null**: its absence is what makes k>1 runs unreadable later. |
| `ts` | string | ISO-8601, UTC. |
| `mode` | string | `measure` \| `triage` \| `improve`: which mode of `skill:eval-loop` produced this record. Required, because a run that silently mixes modes is uninterpretable afterwards: a record produced while the model was being edited is not comparable with one from a frozen measurement. |
| `phase` | string | `baseline` \| `loop` \| `blind_gate` \| `canary` \| `final` \| `null_check`. Drives which records a measurement query selects. |
| `model_version` | int | Which model version answered. |
| `model_sha` | string | **Content hash of the model artifact actually served.** Not optional: a run once measured a same-named decoy package for hours. |
| `submitted` | bool | `false` = no final query was produced. Never merged into "wrong". |
| `f1` | float | Row-level F1. The optimization signal. |
| `ex_strict` | bool | The `f1 == 1` case. Reported, not optimized. |
| `precision` | float | |
| `recall` | float | |
| `n_gold` | int | Rows in the target. |
| `n_pred` | int | Rows in the answer. |
| `matched` | int | Rows paired. `precision`/`recall`/`matched` are what let anyone re-derive `f1` without re-running the query. |
| `failure_bucket` | string \| null | `null` iff `ex_strict`. |
| `final_query` | string \| null | The query actually scored. **Without this a record cannot be replayed**, and replay is the cheap half of the gate. |
| `n_get_context` | int | |
| `n_execute` | int | |
| `n_execute_errors` | int | |

## Optional: written by `eval-answer` when applicable

| Field | Type | Notes |
|---|---|---|
| `f1_adjudicated` | float \| null | Set only when marginal adjudication ran. **Never overwrites `f1`.** |
| `column_agreement` | object | Which target columns were reproduced exactly, plus a hint. |
| `gold_status` | string | `verified` \| `verified_benign` \| `suspect` \| `verified_wrong`. |
| `gold_defect` | string | Defect class when `gold_status` is `suspect`/`verified_wrong`. |
| `exclude_from_scoring` | bool | Set when the target is `verified_wrong`. Measurement queries must respect it. |
| `evidence_class` | string | Which oracle produced the verdict: `stated_answer` \| `resolved_after_iteration` \| `accepted_but_suspect` \| `doubt_only` \| `retrieval_only` \| `none`. Bounds what any downstream edit may do. |
| `question_sha` | string | Hash of the question text as given to the answerer, so a fabricated or truncated question is detectable after the fact. |
| `notes` | string | Free text. Never load-bearing. |

## Appended by `eval-diagnose`

Never modifies a field above. Adds:

| Field | Type | Notes |
|---|---|---|
| `tier` | string | `T1a` \| `T1b` \| `T1c` \| `T2` \| `T3` \| `none`. One primary tier. |
| `primary_code` | string | Fine-grained signature, e.g. `C1-COVERAGE`, `C1-GUIDANCE-NOT-RETRIEVED`, `C1-GUIDANCE-DECLINED`, `C3-GRAIN`. |
| `contributing_codes` | list[string] | Secondary signatures. |
| `owner` | string | `model` \| `retrieval` \| `agent-skill` \| `dataset`. Routes the finding. |
| `context_recall` | float | Fraction of needed entities that appeared in any retrieval result. The leading indicator: it should move before end-to-end F1 does. |
| `diagnosis` | string | The written diagnosis. **Recorded before any edit exists.** |

## Appended by `eval-improve`

| Field | Type | Notes |
|---|---|---|
| `action` | string \| null | What was changed. `null` when the correct move was to change nothing. |
| `gate` | string | `accepted` \| `rejected` \| `none`. |
| `cost_usd` | float | |

## Rules that are part of the schema, not conventions

- **One artifact per record.** The result CSV, transcript, and prompt for a record are keyed by
  `qid` + `sample` (+ attempt, if re-answered). **Never overwrite a previous attempt's artifact** :
  a re-answered answer that reuses its filename makes the earlier record un-reproducible, and a
  later re-grade will report the second answer's score against the first record. Observed: two
  records sharing one file, with the first attempt's truth surviving only under `voided/`.
- **Append, never edit.** To void a record, append a new one and mark the old with `voided: true`
  and `void_reason`. Editing history in place destroys the audit trail the whole loop rests on.
- **A stage never rewrites another stage's fields.** If diagnosis disagrees with the score, that is
  a finding to record, not a number to change.
