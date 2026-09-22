<!-- The worked command sequence. Read it when you are about to run one. -->

# Running a run, concretely

## Running one, concretely

`scripts/run_baseline.py` does steps 3 and 7 and the whole of **Per question**:
one fresh answerer per case with only the Publisher MCP tools, a contamination
check, a judge, and a conformant `events.jsonl`.

Every step reads the set's `eval.toml`: the model server's environment,
package, repo and ports, and the truth server's. Write it once per set, beside
`set.json`. Relative paths resolve against the file.

```toml
[model]                  # the Publisher the answerer queries
environment = "<env>"
package     = "<pkg>"
repo        = "<path to the model package>"
port        = 4811
mcp_port    = 4040

[truth]                  # a second Publisher, serving only the truth package
environment = "truth"
port        = 4881
mcp_port    = 4882
```

No script defaults to any particular set's environment or package. A missing
one stops with the key to add. Runs, built packages and server roots go under
`~/.malloy-eval/<set>/` unless `[paths] workdir` says otherwise, never inside
the repository: a run holds a `model.malloy` snapshot and a built package is a
Malloy package, and nested in the package under test either can put that
package into `loadErrors`. Each command below is `skills/eval-loop/scripts/eval.py`
(`bun run eval --` from the repository root), and any flag the underlying
script takes passes through.

```bash
# 1. serve the model under test, and the TRUTH package on a second server the
#    answerer has no route to. Each runs in its own session, so the shell's
#    exit cannot take it down, and returns only once it answers a query. The
#    script writes each server's publisher.config.json from eval.toml, and
#    refuses a truth server on the model server's ports.
#    --warm-retrieval also waits for the embedding index to settle and exits 3
#    if it does not reach `ready`: the sync is lazy, so without it the first
#    cases are answered LEXICALLY and the run reports that as the model's
#    number.
eval.py serve model --set <set> --warm-retrieval \
  [--allow-proxy]   # required for a `publisher`-type (proxied) connection
eval.py serve truth --set <set> [--allow-proxy]

# 2a. audit the set against the package it is about to be scored on. Free, and
#     it catches the failure that reads as a model regression: an entity id
#     naming a field this package does not have scores as a retrieval miss on
#     every run. Pass --model on a platform target too, from a checkout of the
#     served version, since the harness cannot see the model text there.
#     Exits 3 when set.json names no truthPackage: the audits ran, no
#     golden was re-derived, and 0 would have claimed otherwise. With
#     --definitions <ledger> (verify_definitions.py), a set whose every
#     value-bearing case rests on validated definitions exits 0 instead:
#     the composition rule, values not re-derived but their definitions
#     checked.
eval.py verify --set <set> --model <package> --target-package <pkg>

# 2b. ONLY on a set whose keys are still provisional -- an imported one is,
#     throughout, by design. Nothing else in this toolchain writes
#     golden.status, so without this the set scores 0 of 0 forever and
#     run_baseline refuses to start. It promotes only what re-derived cleanly
#     AND carries a second derivation, and prints the reason for every golden
#     it left alone. `--refresh` is NOT this: it rewrites a drifted value and
#     never touches a status.
eval.py verify --set <set> --target-package <pkg> --promote

# 2. smoke one case first ($0.13), then the arm. Goldens are re-derived from
#    the truth server before either starts; a drifted set refuses to run.
#    [model] repo is recorded as modelGitSha. It cannot be inferred: Publisher
#    serves a copy under publisher_data/, whose surrounding tree is the
#    server's storage, not the model's history.
eval.py run --set <set> --label smoke --only <qid> --no-judge
eval.py run --set <set> --parallel 4
#    the run names itself <set>-<phase>-<nn> (ecommerce-baseline-01, then -02
#    for the second arm of the A/A) and prints its directory. Pass --label
#    only for a run that needs a human name; hand-typed arm names stop being
#    readable within an afternoon.

# 3. compare two arms, or two runs of one arm. It exits 2 when the arms used
#    different retrievers, because those flips measure the retriever.
python3 skills/eval-loop/scripts/flip_table.py --a <run a> --b <run b>

# 3a. on an A/A, write the band into the set's calibration record. Nothing
#     else may quote a band, and a band only covers runs whose pins match.
python3 skills/eval-loop/scripts/flip_table.py \
  --a <aa-1 run> --b <aa-2 run> --calibration >> <set>/CALIBRATION.md

# 3b. check the JUDGE, not the model, after any change to the judge skill, a
#     rubric, or what the judge is shown. It also reports which fixtures are
#     unpinned and which decision classes nothing covers.
python3 skills/eval-loop/scripts/check_judge.py --set <set> --repeat 3

# 4. FIRST: any golden in doubt, from the judge or the set. `jq .doubtedGoldens
#    <run>/run.json` -- non-empty means settle those through the golden side
#    door before diagnosing, or you send a modelling agent at a model that is
#    already right. It probes the server the run recorded.
eval.py diagnose --set <set> --label <label>
#    (cluster_failures.py gives a free mechanical first look, as
#     clusters-mechanical.jsonl; it groups by retrieval outcome and is not a
#     diagnosis)

# 5. build the browsable package. Refused until every run has a diagnosis
#    (--without-diagnosis overrides); pass --run twice for an A/B.
eval.py package --set <set> --label <label>
```

### What the run prints at the end

Three layers, in this order, and the order is what makes it readable:

1. **RESULTS.** Passed of decided, the near_match/needs_human remainder, cost.
2. **Alarms, if any.** A golden the judge does not believe, or an answer that
   used a forbidden field. Both are DATASET problems: acting on them as model
   failures sends a modelling agent at a model that is already right.
3. **COVERAGE & RETRIEVAL.** Which retriever answered, entity recall, and where
   failures attribute to. Coverage is named here but not computed by the run:
   it reads the MODEL rather than the answers, asking whether a correct answer
   is expressible at all, so it is the first question a low score raises and
   the summary prints the command for it.
4. **DEEP DIVE.** A run directory is JSONL, which is a record and not a
   report, so this layer is the two commands that turn it into something you
   read: `build_run_package.py` (a Malloy model over the run's CSVs,
   `eval_run.malloynb` for the aggregate tables, and an in-package HTML app for
   the case matrix and its per-case drawer), then a `POST .../packages` that
   registers it on a Publisher already running, with no restart.
   `build_run_package.py` prints that command and both URLs with the run's own
   paths filled in, and writes them into the package's README.md.

   The command registers the package on the TRUTH server when the set has
   one. The package holds the answer key, and that is the server the answerer
   has no route to.

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
and `--rebuild --rejudge` re-scores existing answers in place. Re-deriving can
CHANGE an attempt's `final_query` -- a fix to the query capture is exactly such
a change -- so the re-executed prediction is keyed on the query and is re-run
whenever the derived query differs from the cached one. That needs the server
up; without it the judge is told the prediction was not re-executed rather than
being handed rows from the previous query. `--from <run>
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
