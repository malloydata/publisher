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
2. Save, then `eval.py publish` (or `eval.py experiment start <name>` to serve
   it beside the main package). It publishes, confirms the package is not
   serving a stale model, and builds anything persisted.
3. Replay stored final queries from previously-passing cases. They must still
   execute, and a judge must still call them a match. `eval.py query-replay
   --from <stored replay> --baseline <stored replay>` does it: a query whose
   rows are unchanged needs no judge, and the ones it lists as changed or
   erroring go to the judge against their goldens. Build the package's
   materializations first if it persists anything, since a reload unbinds
   them (`query-performance.md`).
4. A *fresh* blind re-answer of the affected question. The fix must be
   discoverable, not merely possible. The improver writing the query it
   already knows proves only that the edit exists.

Acceptance rules (replacing any vague "results improve"). `eval.py accept --run
<a> --run <b>` checks the ones a script can decide (twice, no regression, the
targets fixed in every run, both splits, nothing but the model changed, and a
query replay's rows when given `--replay`), against `--baseline record` by
default, and prints the checkpoint message. The rest stay yours: the judge's
doubts about a key, and whether the flips clear the bar for their size.

It measures noise when it can: given a second run of the baseline, found by
its pins or named with `--aa`, a case the two disagree on is unstable, and
neither a fix nor a regression on it counts (measurement.md). A case on the
`test` split is left out of every rule.

Every check, accepted or rejected, is appended to `<set>/attempts.jsonl`: the
edit (`--what`, `--patch`), the issue (`--issue`), the cases it aimed at, the
rule it failed and a one-line `--lesson`. The improve step reads that file, so
give it the patch and a lesson. A rejected edit that nobody recorded is an
edit somebody will pay to reject again.

"Nothing but the model changed" covers the judge, its prompt, the answerer, the
turn cap and the effort: a run at `--max-turns 40` against a record made at 30
is two changes. The two runs must be two runs: the same label named twice, or
the baseline named as a candidate, is refused. It exits 0 when every rule
holds, 1 when one does not, and 3 when it could not check at all.

- **Per-case, not aggregate.** No previously-passing case may regress: diff the
  new run's verdicts against the baseline, case by case (`jq` over the two
  `events.jsonl` files). `regressions` on the acceptance check event must be
  empty to accept, and the regressed qids go in the checkpoint commit message
  if you proceed anyway after a human call.
- **Confident verdicts only.** `needs_human` and null verdicts are neither
  passes nor failures; the delta is computed without them.
- **Both splits.** The acceptance check runs the affected dev cases AND the holdout
  slice. Diagnose and improve never saw holdout; that is what makes its delta
  evidence rather than memorization. Name the affected cases as a slice in
  `slices.json` before the run (`skill:eval-diagnose` proposes one per
  cluster), and pass `--slice` to `run_baseline.py` and `flip_table.py`, so
  the cases are fixed in writing before any verdict exists.
- **Against the stored baseline.** The affected cases need not be re-run
  before the edit when a full run of the parent model already scored them
  (`--baseline record`, the newest, pulled from the team's store when there is
  one). A set edit since then does not void it: the changed cases are set
  aside and the rest compare:
  `flip_table.py --a <full-run> --b <post-edit> --set <set> --slice <name>`.
  A case whose question or key changed since is set aside, not compared.
- **Twice.** An improvement must survive a second independent run with fresh
  blind answerers before acceptance. A delta that appears once and vanishes
  on re-run was answerer or judge variance, not a fix.
- Documentation / discoverability edits may accept on a deterministic
  `get_context` probe now returning the entity, provided no replay
  regresses. Measure, join, or definition edits need the full rules above,
  including the flip-count bar in Measurement, which means enough affected
  cases to clear it.
- **Speed edits** (`#@ persist`, `#@ preaggregate`, made only after the user
  approves each one) accept on a query replay instead of an answer run: build,
  replay every stored query that reads the changed source, no rows change, the
  targeted queries are faster than the latency band and none is slower.
  `query-performance.md` has the steps. A speed edit that also renames a field
  or changes a doc is two edits, and the second takes the rules above.
- Independent deterministic justification (a probed-wrong definition
  corrected) may accept without a measured win. Record that as the acceptance check
  `reason`.
- **Package-skill edits (`class: skill`) get the full rules, never the
  documentation shortcut.** A `get_context` probe proves an entity is
  discoverable. Nothing deterministic proves an agent read a guide and behaved
  differently, so the only evidence a skill edit has is a fresh blind
  re-answer: both splits, twice, clearing the flip-count bar. Treat the
  documentation shortcut as unavailable here even when the edit reads like
  documentation, because a guide is prompt text written next to the case it
  failed and is the easiest artifact in this loop to overfit.

  Read the diff before measuring it, too. A guide that names the case, quotes
  its expected number, or encodes the shape of one question fails the expert
  test in `skill:eval-improve` and is rejected without a run.

Write the `acceptance_check` event (decision, class, baseline and final run ids,
regressions, holdout delta, reason) BEFORE any commit, so a rejected
direction leaves a record. On reject: revert the files (`git checkout --`
or `git restore`) and reload. On accept: `issue_status: fixed` for what the
edit actually closed, **then checkpoint**.
