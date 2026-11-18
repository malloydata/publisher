# Nightly speed run

Time the package's queries, compare them with the last run, and report.
Change nothing.

1. If your host has a skill that fetches the day's executed queries for this
   package, use it to write a workload file: one JSON object per line with
   `query` and `count`. With no such skill, skip this and time the stored
   queries.
2. Run, adding `--queries <file>` when you have one:

       python3 <skills>/eval-loop/scripts/eval.py routine --kind speed --set <set> --serve

   If it exits 3 saying there is no run to take queries from, there is no
   quality run yet and no workload file. Report that and stop.
3. Read the result file whose path is the routine's last line.
4. If your host has a skill for saving eval runs, save this replay with it.

Report, in this order:

- Any query whose rows changed or that now errors. Exit code 4 means there is
  one. Name each. This comes before any timing.
- Total time, and time weighted by how often each query ran. p50 and p95.
- Against the last replay: how many queries are faster, slower, within the
  band, and the total before and after.
- Build cost: how many tables, and the time to build them. "Not reported"
  where the server gave nothing.
- What the diagnosis found: each source it named, the code, and what the code
  means in one line.

Done when the result file exists and every figure in your report is from it.

There is no saved state to rely on. The first replay of a set has nothing to
compare with: give its numbers alone and say so.
