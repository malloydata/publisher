# Nightly speed improve

Time the package's queries, then try one materialization on the source that
costs most, and leave it as a pull request if it is a real speed-up.

1. The model's repository must have nothing uncommitted. Check with
   `git status --porcelain`. If it prints anything, report that and stop.
2. Write the day's workload file if your host has a skill for it, as in the
   nightly speed run.
3. Run, adding `--queries <file>` when you have one:

       python3 <skills>/eval-loop/scripts/eval.py routine --kind speed --mode improve --set <set> --serve

   After the replay and its diagnosis, it takes the finding whose source costs
   most. One agent adds a `#@ persist` or a `#@ preaggregate`. The edit is
   built as an experiment beside the main package and replayed twice.
4. It is accepted only if both replays agree: no rows changed, nothing slower,
   and the total faster than the band. Read the result file's `improve`
   section for which.
5. If it names a branch, open a pull request from it. Its body gives the total
   time before and after and what the new table costs to build on each
   refresh, both from the commit message the routine wrote. If you cannot open
   one, name the branch.

Report:

- Tonight's numbers, as in the nightly speed run.
- The source the edit was for, and why that one: its share of the time.
- The edit, and whether it was accepted, rejected or never measured.
- If accepted: time saved and build cost, side by side.
- If rejected: the rule it failed. "Not faster than the band" on a package
  whose queries already take a few milliseconds is the right answer, not a
  failure. Say there is nothing worth materializing.

Do not merge the pull request. A persisted table costs a build on every
refresh, and whether that is worth it is the owner's call.

Done when the result file exists, and an accepted edit is on a branch that a
person can review.
