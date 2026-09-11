<!-- The worked command sequence. Read it when you are about to run one. -->

# Running a run, concretely

## Running one, concretely

`scripts/run_baseline.py` does steps 3 and 7 and the whole of **Per question**:
one fresh answerer per case with only the Publisher MCP tools, a contamination
check, a judge, and a conformant `events.jsonl`.

```bash
# 1. serve the model under test -- in its own session, so the shell's exit
#    cannot take it down, and returning only once it answers a query
python3 skills/eval-loop/scripts/serve.py --publisher-dir <publisher>/packages/server \
  --server-root <root> --port 4811 --mcp-port 4040 --trace-retrieval \
  [--allow-proxy]   # required for a `publisher`-type (proxied) connection
#    a second server for the TRUTH package, on other ports, that the answerer
#    has no route to:
python3 skills/eval-loop/scripts/serve.py --publisher-dir <publisher>/packages/server \
  --server-root <truthroot> --port 4881 --mcp-port 4882 [--allow-proxy]

# 2. smoke one case first ($0.13), then the arm. Goldens are re-derived from
#    the truth server before either starts; a drifted set refuses to run.
python3 skills/eval-loop/scripts/run_baseline.py \
  --set <repo>/evals/ecommerce --out results/smoke --only <qid> --no-judge \
  --truth-publisher http://localhost:4881
python3 skills/eval-loop/scripts/run_baseline.py \
  --set <repo>/evals/ecommerce --out results/<arm> \
  --parallel 4 --truth-publisher http://localhost:4881
#    the run names itself <set>-<phase>-<nn> (ecommerce-baseline-01, then -02
#    for the second arm of the A/A). Pass --label only for a run that needs a
#    human name; hand-typed arm names stop being readable within an afternoon.

# 3. compare two arms, or two runs of one arm
python3 skills/eval-loop/scripts/flip_table.py --a results/<a> --b results/<b>

# 4. FIRST: any golden the judge did not believe. `jq .doubtedGoldens
#    results/<arm>/run.json` -- non-empty means settle those through the golden
#    side door before diagnosing, or you send a modelling agent at a model that
#    is already right.
python3 skills/eval-diagnose/scripts/diagnose.py \
  --run results/<arm> --set <repo>/evals/ecommerce --model-dir <package>
#    (cluster_failures.py gives a free mechanical first look, as
#     clusters-mechanical.jsonl; it groups by retrieval outcome and is not a
#     diagnosis)

# 5. build the browsable package
python3 skills/eval-loop/scripts/build_run_package.py \
  --run results/<a> --run results/<b> --set <repo>/evals/ecommerce --out <pkg>
```

Order of magnitude for planning, **calibrated on ecommerce over local duckdb**:
a Sonnet arm over a few dozen cases costs single-digit dollars and finishes in
minutes, at roughly a dime and a handful of turns per case. A proxied warehouse
is a different regime: the VideoAmp set ran at $0.33 per case on Sonnet and
$0.57–0.71 on Opus, ~100 s per case, driven by warehouse latency and query
errors -- budget 4x when the data is not local. Budget **five** such arms for a
defensible claim -- a baseline, two for the A/A, and two post-edit -- plus the
diagnose and improve agents, which are far cheaper per case but use a larger
model. Measured per-arm figures for a given set belong in that set's
`CALIBRATION.md`.

`--rebuild` re-derives the ledger from saved transcripts without calling a model,
and `--rebuild --rejudge` re-scores existing answers in place. `--from <run>
--out <new>` does the same into a NEW run directory -- the answers copied, the
judge fresh, the old verdicts untouched -- which is what a golden repair or a
rubric change calls for. Use them after a scoring or schema change; re-running
the answerers would confound the change you are measuring with fresh answerer
variance.

The scripts import each other by path (`ledger`, `mcp_payload`,
`score_retrieval` live in `eval-answer/scripts`; the loop scripts insert that
path). Run them **in place** from the skills checkout; a copy patched elsewhere
chases `ModuleNotFoundError` three times.

Two failure modes worth pre-empting, because both produce a clean-looking run:

- **Pre-approve the tools.** A headless answerer that has to ask permission for
  `get_context` stalls until the timeout and lands as a harness error.
- **Check the served revision is the one you edited.** Publisher serves a
  snapshot copy, so a model fix can be absent from the run that is supposed to
  measure it. Query the changed measure once before spending an arm on it.
