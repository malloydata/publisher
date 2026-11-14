# Nightly quality improve

Measure the package's answer quality, then try one edit that raises it, and
leave an accepted edit as a pull request.

1. The model's repository must have nothing uncommitted. Check with
   `git status --porcelain`. If it prints anything, report that and stop: the
   routine would refuse to edit, and you must not stash or discard work that
   is not yours.
2. If your host has a skill that fetches the day's request logs for this
   package, use it and follow `eval-import` to add the questions people
   actually asked as new cases. A case with no verified answer stays
   provisional and is not scored. Commit the new cases before step 3. With no
   such skill, skip this step and say so.
3. Run:

       python3 <skills>/eval-loop/scripts/eval.py routine --kind quality --mode improve --set <set> --serve --max-cost-usd 25

   After the run and its diagnosis, it takes the largest cluster the model's
   repository owns. One agent makes one edit. The edit is asked twice as an
   experiment beside the main package, and the acceptance rules decide.
4. Read the result file. Its `improve` section says what happened to the edit.
5. If it names a branch, open a pull request from it. Its title is the edit in
   one line. Its body gives the cases the edit aimed at, the rules it passed,
   and the runs, all from the commit message the routine wrote. If you cannot
   open one, name the branch.

Report:

- Tonight's number, as in the nightly run.
- The edit: what it changed and why, and whether it was accepted, rejected or
  never measured.
- If rejected, the rule it failed, in the routine's words.
- If never measured, why. "The agent doubts the answer key on these cases" is
  a finding about the set: name the cases and say a person has to settle them.
- The pull request's link, or the branch.

Do not merge the pull request. Do not make a second edit by hand when the
routine's edit was rejected: the rejection is recorded in `attempts.jsonl` so
the next night starts from it.

Done when the result file exists, and an accepted edit is on a branch that a
person can review.
