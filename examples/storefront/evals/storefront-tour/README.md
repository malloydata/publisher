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
| `eval.toml` | where each step finds its servers: the model package and its ports, the truth package and the truth server's ports |

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

From a clone, with Node 20+, Bun, Python 3.11+ and a Java runtime (the SDK
build runs openapi-generator). Every command reads `eval.toml`, so none of
them takes a server flag. Build once:

```bash
bun install && bun run build
```

**1. Serve the model the answerer will query, and the truth package.** Two
servers: the truth package holds the answer key's derivations, so it must
never share a server with the model under test.

```bash
bun run eval -- serve model --set examples/storefront/evals/storefront-tour   # :4000 / :4040
bun run eval -- serve truth --set examples/storefront/evals/storefront-tour   # :4881 / :4882
```

Each returns once its server answers, and keeps it running after the shell
exits. `--stop` stops it. Export `EMBEDDING_API_KEY` before serving the model:
without it retrieval is lexical, and the start line says so.

**2. Check the answer key still matches the data.** Free, and it refuses the
run rather than spending on a drifted key.

```bash
bun run eval -- verify --set examples/storefront/evals/storefront-tour
```

**3. Run the arm.** About $3 for twelve cases with sonnet answering and
judging. It first measures **coverage** -- whether the model can express an
answer to each question at all, read from the model with no answerer and no
warehouse -- because a question the model cannot express was never winnable,
and a score that cannot separate those from wrong answers is not worth much.
`--no-coverage` skips it.

```bash
bun run eval -- run --set examples/storefront/evals/storefront-tour \
    --label baseline-01 --max-turns 40
```

The run goes to `~/.malloy-eval/storefront-tour/runs/baseline-01`, outside
this repository. It holds the transcripts, the verdicts and the pins, and the
path is printed at the start.

**4. Diagnose what failed.**

```bash
bun run eval -- diagnose --set examples/storefront/evals/storefront-tour \
    --label baseline-01 --verdicts no_match,near_match
```

**5. Build the report.** It refuses a run that has not been diagnosed, then
prints the command that registers the report and its two URLs: the case
matrix, and the notebook of aggregate tables.

```bash
bun run eval -- package --set examples/storefront/evals/storefront-tour \
    --label baseline-01
```

Then write the run up per `skill:eval-report`. It is your report on your run;
it does not belong in this directory.

## Running it on your own questions

Replace `as-received/questions.md` and `cases.jsonl`, point `eval.toml` at
your package and truth package, and drop the goldens. A set of bare questions runs: the answers it
produces are what keys get derived from, and
`verify_goldens.py --promote` fixes them afterwards. `skill:eval-import` is
that job in full.

The truth package is the part worth copying rather than reinventing: one
source per raw table your model reads, no measures, no joins, nothing else. A
golden derived through the model can inherit the bug it is meant to catch.
