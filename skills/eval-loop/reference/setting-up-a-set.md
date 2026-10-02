<!-- The first run of your own set, in order. Read it when a set does not exist yet. -->

# Setting up a set on your own model

`examples/storefront/evals/storefront-tour` is a finished set. This is the path
to one of your own: which files a set needs, what writes each one, and the
order they go in. Every command is `python3 skills/eval-loop/scripts/eval.py`,
run from the Publisher clone, unless it names another script.

`eval.py check --set <set>` reads everything below and names every gap at once,
without starting a server or calling a model. Run it after each step; the set is
ready when it prints `ready: nothing blocks a run`.

## What you need first

- A Malloy package with a `publisher.json`, in a git repository. Git is how a
  run pins the model it measured and how an improve step is rolled back.
- A built Publisher clone (`bun install && bun run build`), Python 3.11+, and a
  Java runtime for the SDK build.
- `EMBEDDING_API_KEY` exported, for semantic retrieval. Without it the model
  server ranks lexically and says so on start.

## 1. Make the set directory

One directory per set, in the same repository as the model package.
`skill:eval-import` "Where the set goes" has the rule about how close to the
package it may sit. It holds:

| File | What it is | Written by |
|---|---|---|
| `set.json` | the set's name, the package and model under test, the truth package | you |
| `cases.jsonl` | one case per question: text, split, golden | `skill:eval-import` |
| `as-received/` | the questions exactly as they arrived | you, untouched |
| `eval.toml` | where each step finds its servers | you |
| `gold/<qid>.json` | a golden's second derivation | you, when you derive one |

`set.json`, at minimum (`reference/ledger-schema.md` in `skill:eval-answer`
has every field):

```json
{"name": "<set>", "description": "<what it measures>", "datasetVersion": 1,
 "targetPackage": "<package name>", "targetModelPath": "<model>.malloy"}
```

## 2. Import the questions

`skill:eval-import` turns questions in any shape into `cases.jsonl` and says what
each one is worth. A set with no questions yet has nothing to import: write the
questions a person would actually ask into `as-received/` first, then import
them, rather than writing cases by hand. Then stamp and validate:

```bash
python3 skills/eval-import/scripts/import_cases.py --set <set> --stamp
```

It refuses what later steps would fail on, with the fix: no `split`, a value
claiming `verified` on arrival, a `scalar` value that is a bare number rather
than `{"<column>": <value>}`. A set of bare questions is valid. It runs and
measures coverage, and cannot score accuracy until keys exist.

## 3. Build the truth package, if any case holds a value

A golden holding a value is re-derived before every run from a truth package:
the raw tables your model reads, with no modelling, served on a second server
the answerer cannot reach. Scaffold it OUTSIDE the model package, because
Publisher serves every `.malloy` under a package directory:

```bash
python3 skills/eval-answer/scripts/init_truth_package.py \
    --package <model package> --out <outside it>/<set>-truth --name <set>-truth
```

It writes one source per raw table and links the model's data directory in, so
file paths still resolve once Publisher serves its copy of the package. The link
holds this machine's path; to commit the truth package, copy the data in
instead, as `examples/storefront-tour-truth/` does. Add the package's scope
filters by hand, as its header says, and nothing else. Then set
`"truthPackage": "<set>-truth"` in `set.json`.

A set of criteria goldens or bare questions needs no truth package; `check`
notes its absence and moves on.

## 4. Write eval.toml

```toml
[model]                  # the Publisher the answerer queries
environment = "<env>"
package     = "<package name>"
repo        = "<path to the model package>"   # relative to this file
port        = 4811
mcp_port    = 4812

[truth]                  # only with a truth package; an empty [truth] takes 4881/4882
environment = "truth"
package_dir = "<path to the truth package>"
port        = 4881
mcp_port    = 4882
```

`package` here and `targetPackage` in `set.json` must name the same package.
Where both are set, `eval.toml` wins, and `check` refuses the set until they
agree.

Pick ports nothing else on the machine holds; `check` names any that are taken.
Every later step reads them from this file, so change them here rather than
passing `--port`, which `serve` refuses when it disagrees with the file.

## 5. Check, serve, verify

```bash
eval.py check --set <set>                  # until: ready
eval.py serve model --set <set> --warm-retrieval
eval.py serve truth --set <set>            # with a truth package
eval.py verify --set <set>                 # free: re-derives every golden
```

`verify` exits 0 when every golden re-derives, 1 when one drifted, and 3 when it
could not check (no truth package, or no truth server). Read 3 as "not run",
never as a pass.

## 6. Turn provisional keys into scorable ones

An imported value is `provisional`, and a provisional golden takes no verdict.
`--promote` marks one `verified` only when it re-derived cleanly through the
truth package AND a second, differently shaped derivation exists: a
`golden.verification` block, or `gold/<qid>.json` holding `verifyRows`
(`reference/ledger-schema.md`, the `golden` row, says what the second one must
vary).

```bash
eval.py verify --set <set> --promote
```

It prints why it left each golden alone. `check` shows the scorable count.
Where a person has checked a value by hand instead of deriving it a second way,
`--promote --attest "<who checked, when, how>"` promotes it and writes that text
on the golden, so a reader sees a person stood behind it rather than a query.

## 7. The first run

```bash
eval.py run --set <set> --label smoke --only <qid>[,<qid>...] --max-turns 40   # a case or two first
eval.py run --set <set> --label baseline-01 --max-turns 40
eval.py diagnose --set <set> --label baseline-01
eval.py package --set <set> --label baseline-01
```

Then `reference/running-a-run.md` for what the run prints and how to read it,
and `skill:eval-report` for the write-up.

Measured on the tour's local DuckDB data, with sonnet answering and judging:
about $0.35 a case to run, and $0.60 and three minutes to diagnose one failed
case. A warehouse over a proxy costs several times more per case.
