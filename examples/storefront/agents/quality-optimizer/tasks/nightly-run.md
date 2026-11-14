# Nightly quality run

Measure the package's answer quality and report the number. Change nothing.

1. Run:

       python3 <skills>/eval-loop/scripts/eval.py routine --kind quality --set <set> --serve --max-cost-usd 25

   It starts the servers it needs, answers and scores every case, diagnoses
   the failures, and stops the servers. It takes a few minutes on a local
   package.
2. Read the result file whose path is the routine's last line.
3. If your host has a skill for saving eval runs, save this run with it.

Report, in this order:

- The pass rate: passed of decided. The `test` split's rate first if the set
  has one.
- How it compares with the previous run, from
  `eval.py versions --set <set>`. Name the cases that changed, not only the
  difference.
- What the diagnosis found: each cluster, how many cases, and who owns it
  (the model, a package skill, retrieval, the set itself).
- What it cost: answering, judging, diagnosing.
- The exit code and its reason, when it is not 0.

Done when the result file exists and every figure in your report is from it.

There is no saved state to rely on. If no earlier run exists, say this is the
first and give the number alone.
