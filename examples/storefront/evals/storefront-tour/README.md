# storefront-tour

Twelve natural-language questions over the bundled `storefront` package, with
verified goldens, as a worked example of the eval loop (`skill:eval-loop`).
Run it to see what an evaluation produces: a score, retrieval recall, a
diagnosis of each failure, and a servable report.

`RESULTS.md` is the write-up of the first run, if you would rather read one
than pay for one.

## What is here

| | |
|---|---|
| `as-received/questions.md` | the questions as authored, plus the business conventions that arrived with them |
| `cases.jsonl` | one case per question: the sealed question text, its golden, and the entities its answer depends on |
| `gold/` | both derivations of every value-bearing golden, and their agreement |
| `truth-package/` | the raw tables the goldens are derived from, with no modelling. Served on a SECOND server the answerer cannot reach |
| `runs/` | the first run's report package and console log |

Every golden is `verified`: derived once as SQL over the raw parquet, once as
Malloy through the truth package, on axes that differ, and promoted only where
the two agreed.

## Run it

From a clone, with Node 20+ and Bun. Four terminals' worth of setup, then one
command that spends money.

**1. Serve the model the answerer will query.**

```bash
bun install && bun run build && bun run start     # REST :4000, MCP :4040
curl -s http://localhost:4000/api/v0/status | jq -r .operationalState   # -> serving
```

**2. Serve the truth package, separately.** It holds the answer key's
derivations, so it must never share a server with the model under test.

```bash
mkdir -p /tmp/truthroot && cat > /tmp/truthroot/publisher.config.json <<JSON
{"frozenConfig": false, "environments": [{"name": "truth", "connections": [],
  "packages": [{"name": "storefront-tour-truth",
    "location": "$PWD/examples/storefront/evals/storefront-tour/truth-package"}]}]}
JSON
python3 skills/eval-loop/scripts/serve.py \
    --publisher-dir "$PWD/packages/server" \
    --server-root /tmp/truthroot --port 4881 --mcp-port 4882 --reinit
```

`--publisher-dir` must be absolute: the script runs from elsewhere and a
relative path resolves against the wrong directory.

**3. Check the answer key still matches the data.** Free, and it refuses the
run rather than spending on a drifted key.

```bash
python3 skills/eval-answer/scripts/verify_goldens.py \
    --set examples/storefront/evals/storefront-tour \
    --publisher http://localhost:4881 --environment truth
```

`--environment truth` is not optional; the default is `samples` and every
case 404s without it.

**4. Run the arm.** About $3 for twelve cases with sonnet answering and
judging.

```bash
python3 skills/eval-loop/scripts/run_baseline.py \
    --set examples/storefront/evals/storefront-tour \
    --out examples/storefront/evals/storefront-tour/runs/baseline-02 \
    --environment examples --package storefront \
    --publisher http://localhost:4000 --mcp-url http://localhost:4040/mcp \
    --truth-publisher http://localhost:4881 --truth-environment truth \
    --model-repo examples/storefront --skills-root . \
    --label baseline-02 --max-turns 40 --parallel 4
```

**Commit the run directory as soon as it finishes**, before building anything
from it. The first run's was untracked when an unrelated branch switch removed
it, and its transcripts and diagnosis could not be recovered.

**5. Diagnose what failed**, and only then build the report: the run package
reads `clusters.jsonl`, so building first gives you empty cluster views.

```bash
python3 skills/eval-diagnose/scripts/diagnose.py \
    --run examples/storefront/evals/storefront-tour/runs/baseline-02 \
    --set examples/storefront/evals/storefront-tour \
    --model-dir examples/storefront \
    --environment examples --package storefront \
    --verdicts no_match,near_match
```

**6. Build the report and open it.**

```bash
python3 skills/eval-loop/scripts/build_run_package.py \
    --run examples/storefront/evals/storefront-tour/runs/baseline-02 \
    --set examples/storefront/evals/storefront-tour --out /tmp/eval-baseline-02
curl -sS -X POST http://localhost:4000/api/v0/environments/examples/packages \
    -H 'content-type: application/json' \
    -d '{"name":"eval-baseline-02","location":"/tmp/eval-baseline-02"}'
```

Two artifacts, and they live in **different path spaces** -- guessing one from
the other is a 404:

- the case matrix, an in-package HTML app:
  `http://localhost:4000/environments/examples/packages/eval-baseline-02/`
- the aggregate notebook, a model rendered by the Console:
  `http://localhost:4000/examples/eval-baseline-02/eval_run.malloynb`

Then write the run up per `skill:eval-report`, beside this file.

## Running it on your own questions

Replace `as-received/questions.md` and `cases.jsonl`, point `set.json` at your
package, and drop the goldens. A set of bare questions runs: the answers it
produces are what keys get derived from, and
`verify_goldens.py --promote` fixes them afterwards. `skill:eval-import` is
that job in full.

The truth package is the part worth copying rather than reinventing: one
source per raw table your model reads, no measures, no joins, nothing else. A
golden derived through the model can inherit the bug it is meant to catch.
