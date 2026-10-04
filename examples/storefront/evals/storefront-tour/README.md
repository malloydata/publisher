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

### What is expected, and not a defect

- `verify` prints `ok=11  skipped=1`, then 17 "rubric figures to review" and
  2 "other review items" on `best-customers`. Both lists are prompts to read a
  rubric against its rows, not failures; the figures are the tolerance band
  each rubric accepts and the wrong answers it names.
- With `--model examples/storefront`, `verify` also audits every entity id
  against the model text, and reports that `signup_date` and `retail_price`
  appear nowhere in it. They are columns the sources expose implicitly from the
  parquet, invisible to a grep of the `.malloy`, and both cases pass.
- `check_coverage.py` marks `signup-cohort-2025` and `avg-discount` as not
  answerable, for the same reason: it judges from the model text. This drags
  reported coverage down by two and is a limitation of the coverage check, not
  of the model.

Do not declare those fields to silence either warning: the cases pass, and
declaring a field to quiet a checker changes the thing being measured.

## Run it

You need a clone of this repository with Publisher built (Node 20+, Bun, and
a Java runtime for the SDK build), and Python 3.11+ for the eval scripts. Bun
builds and runs Publisher; everything else here is Python. Every command reads
`eval.toml`, so none of them takes a server flag. From the repository root:

```bash
bun install && bun run build          # once: builds Publisher
EVAL=skills/eval-loop/scripts/eval.py
SET=examples/storefront/evals/storefront-tour

python3 $EVAL check --set $SET
```

`check` names every gap before anything starts, including a port another
process already holds. The two servers use 4811/4812 and 4881/4882, which a
Publisher started the usual way (4000/4040) does not. If one is taken, change
the port in `eval.toml`, which every later step reads. `serve` refuses a
`--port` flag that disagrees with the file, because the later steps would not
find that server.

**1. Start the two servers.** One serves the model the answerer queries. The
other serves the truth package, the raw tables the answer key is derived
from, so it must never be a server the answerer can reach.

```bash
export EMBEDDING_API_KEY=...          # optional; without it retrieval is keyword matching
python3 $EVAL serve model --set $SET  # :4811 / :4812
python3 $EVAL serve truth --set $SET  # :4881 / :4882
```

Each returns once its server answers, and keeps it running after the shell
exits; `--stop` stops it. With an embedding key, `serve model` also waits for
the retrieval index to finish building, so the run's first questions are
ranked semantically rather than by keyword. Without a key it says the run will
measure keyword matching.

**2. Check the answer key still matches the data.** Free, and it refuses the
run rather than spending on a drifted key.

```bash
python3 $EVAL verify --set $SET
```

**3. Run the arm.** About $3 for twelve cases with sonnet answering and
judging. It first measures **coverage** -- whether the model can express an
answer to each question at all, read from the model with no answerer and no
warehouse -- because a question the model cannot express was never winnable,
and a score that cannot separate those from wrong answers is not worth much.
`--no-coverage` skips it.

```bash
python3 $EVAL run --set $SET --label baseline-01 --max-turns 40 --parallel 4
```

The run goes to `~/.malloy-eval/storefront-tour-<hash>/runs/baseline-01`,
outside this repository. The hash is of the set's path. It holds the transcripts, the verdicts and the pins, and the
path is printed at the start.

**4. Diagnose what failed.** About $0.45 and three minutes per failed case: an
agent reads each failure's transcript, then one more clusters them. A run where
everything passed records an empty diagnosis, so step 5 still builds.

```bash
python3 $EVAL diagnose --set $SET --label baseline-01 --verdicts no_match,near_match
```

**5. Build the report.** It refuses a run that has not been diagnosed, then
builds the report, registers it on the truth server, and prints its two links:
the case matrix, and the notebook of aggregate tables.

```bash
python3 $EVAL package --set $SET --label baseline-01
```

On the bundled model a run scores about 10 of 12: summer sales and customer
count fail, as the section above says they should. Then write the run up per
`skill:eval-report`. It is your report on your run; it does not belong in this
directory.

## Running it on your own questions

[`skills/eval-loop/reference/setting-up-a-set.md`](../../../../skills/eval-loop/reference/setting-up-a-set.md)
is that path in order: the files a set needs, the truth package, `eval.toml`,
and when to run `check`. `skill:eval-import` turns questions in any shape into
cases.

The truth package is the part worth copying rather than reinventing: one
source per raw table your model reads, no measures, no joins, nothing else. A
golden derived through the model can inherit the bug it is meant to catch.
