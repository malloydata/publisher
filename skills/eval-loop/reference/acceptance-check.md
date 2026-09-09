<!-- How to decide whether ONE edit stays. Read this before accepting or reverting anything. -->

# The acceptance check

## The acceptance check (inside improve)

You own the acceptance check. The improver does not accept its own edit.

**Before any of it: is the answer key still valid?** An edit that changed what
an entity means can have moved goldens for cases nobody was working on, and a
rerun against a stale key measures nothing -- it reads as a win or a regression
with equal confidence and neither is real. `skill:eval-improve` Step 4 reports
these as `golden_suspect` on the candidate; the judge reports its own doubts as
`gold_status`. **Any unadjudicated one halts the acceptance check.** Settle
each through the golden side door -- repair and bump `goldenRevision`, or
dismiss it explicitly -- and only then re-answer. Do not net a suspect golden
against the flip count; an uncertain key is not noise you can average out.

Cheap and deterministic, every edit:

1. A compile check (scope `file` for an edit, `package` if importers must
   survive).
2. Save, then reload the package. Confirm it is not serving a stale model.
3. Replay stored final queries from previously-passing cases. They must still
   execute, and a judge must still call them a match.
4. A *fresh* blind re-answer of the affected question. The fix must be
   discoverable, not merely possible. The improver writing the query it
   already knows proves only that the edit exists.

Acceptance rules (replacing any vague "results improve"):

- **Per-case, not aggregate.** No previously-passing case may regress: diff the
  new run's verdicts against the baseline, case by case (`jq` over the two
  `events.jsonl` files). `regressions` on the acceptance check event must be
  empty to accept, and the regressed qids go in the checkpoint commit message
  if you proceed anyway after a human call.
- **Confident verdicts only.** `needs_human` and null verdicts are neither
  passes nor failures; the delta is computed without them.
- **Both splits.** The acceptance check runs the affected dev cases AND the holdout
  slice. Diagnose and improve never saw holdout; that is what makes its delta
  evidence rather than memorization.
- **Twice.** An improvement must survive a second independent run with fresh
  blind answerers before acceptance. A delta that appears once and vanishes
  on re-run was answerer or judge variance, not a fix.
- Documentation / discoverability edits may accept on a deterministic
  `get_context` probe now returning the entity, provided no replay
  regresses. Measure, join, or definition edits need the full rules above,
  including the flip-count bar in Measurement, which means enough affected
  cases to clear it.
- Independent deterministic justification (a probed-wrong definition
  corrected) may accept without a measured win. Record that as the acceptance check
  `reason`.

Write the `acceptance_check` event (decision, class, baseline and final run ids,
regressions, holdout delta, reason) BEFORE any commit, so a rejected
direction leaves a record. On reject: revert the files (`git checkout --`
or `git restore`) and reload. On accept: `issue_status: fixed` for what the
edit actually closed, **then checkpoint**.
