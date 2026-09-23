# storefront-tour

Twelve natural-language questions over the bundled `storefront` package, with
verified goldens, as a worked example of the eval loop (`skill:eval-loop`).
Run it to see what an evaluation produces: a score, retrieval recall, a
diagnosis of each failure, and a servable report.

This directory is the **questions and their answer key, and nothing else**.
Running it is what you do with it; what an arm produces is yours and stays out
of this repository (`evals/.gitignore`).

## What is here

| | |
|---|---|
| `as-received/questions.md` | the questions as authored, plus the business conventions that arrived with them |
| `cases.jsonl` | one case per question: the sealed question text, its golden, and the entities its answer depends on |
| `gold/` | both derivations of every value-bearing golden, and their agreement |
| (`examples/storefront-tour-truth/`) | the raw tables the goldens are derived from, with no modelling. It lives OUTSIDE the storefront package on purpose: Publisher serves every `.malloy` under a package directory, so a truth package kept in here is served as one of storefront's own models and the answerer can query the raw tables |

Every golden is `verified`: derived once as SQL over the raw parquet, once as
Malloy through the truth package, on axes that differ, and promoted only where
the two agreed.

## Some of these SHOULD fail

A perfect score here would mean the set was not worth running. Three questions
ask for business knowledge that `storefront.malloy` does not encode, and
against the bundled model they are expected to come back wrong. That is the
measurement, not a defect in the model and not a bug in the set.

**Do not fix the model to make them pass.** The eval exists to show the gap
between what the business means and what the model says. Closing it by hand
deletes the finding and leaves a set that proves nothing. If you want the gap
closed, close it through `skill:eval-improve` behind an acceptance check, and
expect these cases to flip — that flip is the evidence the edit worked.

| Question | The business means | The model offers | Why it cannot get there |
|---|---|---|---|
| What were our summer sales in 2025? | 25 May – 15 Sep, the sales calendar: **$267,423** | a date column and a revenue measure | no field, given, filter or doc expresses a season, so an agent reads "summer" as June–August and lands 25% low |
| How many customers do we have? | people the company has **delivered** to: **943** | `customer_count`, which returns 974 through `order_items` and 1,000 on the customers source | nothing expresses delivery. 966 and 960 are the other near misses |
| What were net sales in 2025, excluding cancelled and returned items? | **$834,216** | `total_sales`, which carries no status filter and returns gross | reachable, because `status` is a dimension — this one tests whether the agent builds the filter rather than trusting the named measure |

The conventions themselves are written down in
[`as-received/questions.md`](as-received/questions.md), as part of the material
the questions arrived with. They are deliberately nowhere in the model.

### One wrong doc, left wrong on purpose

`storefront.malloy` documents its `customers` source as "People who have placed
orders". That is false for 26 of the 1,000 rows, which have no order line at
all. It stays as it is: a model that misdescribes its own table is exactly the
condition the customer question is measuring, and correcting the sentence here
would hide it. A run should surface it; `skill:eval-improve` is where it gets
fixed, if you decide to fix it.

### Two warnings that are expected

Every run reports both, and neither is a defect:

- `verify_goldens.py` warns that `signup_date` and `retail_price` "appear
  nowhere in the served model". They are columns the sources expose implicitly
  from the parquet, invisible to a grep of the `.malloy`.
- `check_coverage.py` marks `signup-cohort-2025` and `avg-discount` as not
  answerable, for the same reason — it judges from the model text. Both cases
  pass. This drags reported coverage down by two and is a limitation of the
  coverage check, not of the model.

Do not declare those fields to silence either warning: the cases pass, and
declaring a field to quiet a checker changes the thing being measured.

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
    "location": "$PWD/examples/storefront-tour-truth"}]}]}
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
judging. It first measures **coverage** -- whether the model can express an
answer to each question at all, read from the model with no answerer and no
warehouse -- because a question the model cannot express was never winnable,
and a score that cannot separate those from wrong answers is not worth much.
`--no-coverage` skips it.

```bash
python3 skills/eval-loop/scripts/run_baseline.py \
    --set examples/storefront/evals/storefront-tour \
    --out examples/storefront/evals/storefront-tour/runs/baseline-01 \
    --environment examples --package storefront \
    --publisher http://localhost:4000 --mcp-url http://localhost:4040/mcp \
    --truth-publisher http://localhost:4881 --truth-environment truth \
    --model-repo examples/storefront --skills-root . \
    --label baseline-03 --max-turns 40 --parallel 4
```

The run directory holds the transcripts, the verdicts and the diagnosis. It is
not committed here, so copy it somewhere durable if you want to keep it.

**5. Diagnose what failed**, and only then build the report: the run package
reads `clusters.jsonl`, so building first gives you empty cluster views.

```bash
python3 skills/eval-diagnose/scripts/diagnose.py \
    --run examples/storefront/evals/storefront-tour/runs/baseline-01 \
    --set examples/storefront/evals/storefront-tour \
    --model-dir examples/storefront \
    --environment examples --package storefront \
    --verdicts no_match,near_match
```

**6. Build the report and open it.**

```bash
python3 skills/eval-loop/scripts/build_run_package.py \
    --run examples/storefront/evals/storefront-tour/runs/baseline-01 \
    --set examples/storefront/evals/storefront-tour --out /tmp/eval-baseline-03
curl -sS -X POST http://localhost:4000/api/v0/environments/examples/packages \
    -H 'content-type: application/json' \
    -d '{"name":"eval-baseline-03","location":"/tmp/eval-baseline-03"}'
```

Two artifacts, and they live in **different path spaces** -- guessing one from
the other is a 404:

- the case matrix, an in-package HTML app:
  `http://localhost:4000/environments/examples/packages/eval-baseline-03/`
- the aggregate notebook, a model rendered by the Console:
  `http://localhost:4000/examples/eval-baseline-03/eval_run.malloynb`

Then write the run up per `skill:eval-report`. It is your report on your run;
it does not belong in this directory.

## Running it on your own questions

Replace `as-received/questions.md` and `cases.jsonl`, point `set.json` at your
package, and drop the goldens. A set of bare questions runs: the answers it
produces are what keys get derived from, and
`verify_goldens.py --promote` fixes them afterwards. `skill:eval-import` is
that job in full.

The truth package is the part worth copying rather than reinventing: one
source per raw table your model reads, no measures, no joins, nothing else. A
golden derived through the model can inherit the bug it is meant to catch.
