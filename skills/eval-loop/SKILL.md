---
name: eval-loop
description: 'Run an evaluation loop over a semantic model in one of three modes: measure (score a model against a question set, changing nothing), triage (also diagnose why it fails), or improve (also edit the model, behind a gate). Use to establish a baseline, score or benchmark a model, check for regressions after a model change, work out whether failures are a model / agent / retrieval problem, or improve a model question by question. Triggers on "how good is this model", "run the eval set", "score this model", "benchmark it", "why is it failing", "run the improvement loop". Conducts eval-answer, eval-diagnose and eval-improve; owns the gate, the run ledger, and the measurement protocol.'
---

# The Evaluation Loop

Conducts three skills over a question set, one question at a time, and owns the three things none of
them can own: **the gate**, **the sequence**, and **the measurement**.

```
per question:  answer (blind)  →  skill:eval-answer  →  skill:eval-diagnose  →  skill:eval-improve
                └── measure ──────────────┘                    │                        │
                └── triage ────────────────────────────────────┘                        │
                └── improve ────────────────────────────────────────────────────────────┘
                                                                                        ↓
                                                                               GATE (this skill)
                                                                          accept → snapshot + reload
                                                                          reject → revert
```

**This skill conducts; it does not restate.** The scoring rules live in `skill:eval-answer`, the
category codes in `skill:eval-diagnose`, the edit rules in `skill:eval-improve`. Do not copy them
here. A rule that lives in two files is not a rule: a tier definition corrected in one of two prompt
files, but not the other, produced the same mislabel twice in a row.

## Modes: say which one you are running, before you start

**Most runs should be `measure`.** The stages are cumulative, so stopping early is a first-class way
to use this skill, not a degraded one. Pick the mode explicitly and record it on every ledger record:
mixing modes within one run is how a measurement quietly becomes unusable.

| Mode | Runs | Changes the model? | Use it to |
|---|---|---|---|
| **`measure`** | answerer → `skill:eval-answer` | **No** | Score a model against a question set. Baselines, before/after comparisons, regression checks, CI. |
| **`triage`** | + `skill:eval-diagnose` | **No** | Understand *why* a model is failing, and whether the fix belongs to the model, the agent, or retrieval: without touching anything. |
| **`improve`** | + `skill:eval-improve` + the gate | **Yes** | Close diagnosed model gaps, one question at a time. |

### `measure`: eval quality only

The whole skill minus diagnosis, improvement, and the gate. **Keep** from the sections below: freeze
the harness, record the model hash, sample at k≥3 with Neyman reallocation, and report deltas with
confidence intervals. **Skip**: the gate, the canary, the changelog, and every reference to an edit.

Two notes specific to this mode:

- **You may still compute `context_recall`**: the fraction of needed entities that appeared in any
  retrieval result (see `skill:eval-diagnose` step 1). It is a mechanical count over the transcript,
  not a judgment, and it is the leading indicator worth having in a baseline: it moves before
  end-to-end score does. Counting it is not diagnosing: do not assign category codes in `measure`
  mode.
- **A measurement run is useful even with no correct-answer oracle.** Without one you still get
  `submitted` rates, execution-error rates, calls per question, and context recall: enough to catch
  a regression, though not enough to call any single answer right.

### `triage`: explain, change nothing

`measure` plus `skill:eval-diagnose`. Runs at k=1: triage tolerates a noisy individual verdict, a
measurement does not.

The valuable output is not any single diagnosis but the **histogram of codes and owners**. That is
what tells you whether you have a model problem, an agent problem, or a retrieval problem before
committing to fix any of them: and if 40% of failures are `C1-AMBIGUOUS` on the same pair of sibling
sources, that is one edit, not forty.

## Roles, and the one boundary that cannot move

| Role | Sees |
|---|---|
| **Answerer** | The question and the tools. **Never** the correct answer, the ledger, the model file, or any hint it is being evaluated. |
| **Evaluate / diagnose / improve** | Everything, including the correct answer. |
| **Gate** | The edit and the evidence: never the improver's self-assessment alone. |

**The answerer stays blind. That boundary is non-negotiable**: a grader-visible answerer crafts
queries toward the expected answer, and every measurement after that is fiction. Everything else in
this design has been revised at least once; this has not.

## Before the run

1. **Freeze the harness and record it.** Round/call budget, prompt text, model slug, retrieval
   settings. Held constant for the whole experiment. Raising an answerer's round cap from 16 to 50
   moved mean score by 0.30 on an unchanged model: harness config dominates model quality if it
   floats.
2. **Split held-in from held-out.** The loop sees TRAIN only. TEST is never seen, and is the
   measurement that matters: general knowledge lifts held-out score, question-specific hacks lift
   only held-in. That gap is what overfitting *means* here, and it is the only real defence now that
   the improver sees correct answers.
3. **Freeze a canary set**: previously-passing questions, for regression checks.
4. **Record the model artifact's content hash.** See `skill:eval-answer` step 1.

## Per question

1. **Health-check the environment.** Before *every* answer. A publisher answers HTTP and reloads
   packages happily with a dead database connection, so "it responds" is not "it works". On failure,
   fix the environment: **never let an environment failure become a diagnosis.**
2. **Generate the answerer's prompt from data**, never by typing the question yourself. Then spawn a
   fresh blind subagent.
3. **`skill:eval-answer`**: verdict and ledger record.
4. **`skill:eval-diagnose`**: tier, code, owner. Only `owner: model` continues.
5. **`skill:eval-improve`**: one edit with probe receipts.
6. **Gate** (below), then snapshot and reload, or revert.
7. **Route the findings** that were not model edits (below).

**Abort rule:** four consecutive environment failures or no-result runs means the environment is
broken. Stop and fix it. Do not diagnose results produced by a sick system.

## The gate

**The gate belongs to this skill, not to the improver.** An agent must not accept its own edit.

**Every edit: cheap and deterministic:**

a. The model compiles.
b. **Data-shape validation passes.** A compile gate is structurally blind: a false primary key
   compiles perfectly and corrupts every aggregate downstream. This check is the one that matters
   most for a structural edit.
c. **Replay** every stored final query from previously-passing questions: all must still execute and
   still match. Replay is cheap, deterministic, and it caught the one edit in a prior run that
   produced genuinely corrupt arithmetic while compiling cleanly.
d. **Blind re-answer of this same question.** The fix must be *discoverable*, not merely
   possible. The improver verifying its own fix proves only that the fix exists: it writes the query
   it already knows to write. One edit passed its author's verification and the next blind agent
   ignored it entirely, costing two model versions before anyone noticed. If the re-answer does not
   improve, keep the edit only if independently justified, and log the follow-up diagnosis: usually
   `C1-GUIDANCE-NOT-RETRIEVED`.

**Every K questions: expensive and statistical:**

e. **Canary re-answer**: the frozen set, k=3 fresh answerers, compared on **mean score**. Never on
   any-binary-regression: a passing question repeats only ~56% of the time on an identical model,
   and a binary guard fired on pure noise ~90% of the time in a prior run, reverting 8 of 10 edits
   including ones whose retrieval surface provably did not change. Tolerance band from the run's own
   measured null variance.

**On accept:** snapshot the model version, reload **and re-index**, then write the changelog entry
(the edit, the diagnosis that motivated it, the probe receipts, the gate results). If retrieval is
embedding-backed, the index sync may be lazy: whichever agent goes first would otherwise silently
absorb it. Pay it deliberately and confirm the counts. **Never start a new question or a canary batch on
an unindexed edit**, including after a revert.

## Routing what is not a model edit

Three destinations. Using the wrong one loses the learning:

- **Failure-shape craft**: a signature you had to reason out, a probe that settled something, a
  diagnosis anti-pattern → back into `skill:eval-diagnose`. This transfers to the next model.
- **Agent-behaviour and tool-description findings** → a per-run findings file, applied *after* the
  model work is frozen. Two moving systems make attribution impossible.
- **A verdict on a reference answer** → a durable registry with the evidence, so it survives a
  rebuild and nobody re-litigates it.

**Tool-description findings are the highest-leverage of the three**: a fix there improves every
agent against every model, not just this one.

## Measurement

**This section is `measure` mode.** It is also what `improve` mode uses at its start and end: an
improvement run is bracketed by two measurement runs, and everything here applies to both.

Improvement is a claim about a distribution, and this is where the predecessor pilot died: it
reported effects of −0.145 and +0.101 with a minimum detectable effect of 0.29.

- **Baseline (k=3 uniform)** over held-in and held-out. Its real job is producing the per-question
  standard-deviation table.
- **Final** measurement uses **Neyman allocation** off that table: k ∝ sd, so k=1 for the stable
  questions and k=6–8 for the unstable ones, same budget, ~2.5× variance reduction.
- **Null check (revert control):** re-run held-out against the *original* model at the **end** of the
  run. A before/after difference is only believable if the original scores the same at the end as it
  did at the start; this bounds environment and model drift over the run's wall-clock.
- **Never report a delta without its confidence interval**, and refuse to call a direction when the
  interval includes zero. That specific omission is what made the prior pilot's findings unusable.

Report three things: held-in change, **held-out change** (the one that matters), and **context
recall** on both. If context recall rises while end-to-end score does not, model coverage improved
and the bottleneck moved to the agent: that is a *successful* outcome for a model-improvement loop,
and distinguishing it is the entire point of the component split.

## Prime directives

- **The model is the only thing the loop improves.** The eval set must leave no fingerprints in it :
  no question text, ids, or expected values in any name, doc, or comment.
- **When the environment misbehaves, stop and fix it.** Never let an environment failure become a
  diagnosis.
- **When a subagent disagrees with you, that is signal.** Across one run the improving agent
  contradicted its orchestrator four times and was right four times. Resolve disagreements by
  probing, not by authority: and when the subagent was right, fix the skill.
- **When a rule here proves wrong, change this file** and note it in the run record, rather than
  quietly behaving differently.

## Related skills

- `skill:eval-answer`: the verdict, and the ledger schema everything else appends to.
- `skill:eval-diagnose`: why it failed, and who owns the fix.
- `skill:eval-improve`: the smallest edit, with probe receipts.
