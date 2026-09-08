<!-- What a coverage number does and does not measure. Read before quoting one. Two independent limits: per-case accuracy, measured against a labelled set, and scale, measured against model size. -->

# Coverage limits

`check_coverage.py` was measured against a real labelled set and at a range of
model sizes. Both limits below are measured, and the method is named beside each
number so it can be re-measured when the harness changes. Neither is visible in
the output the script prints today.

The short version. **The per-case verdict agrees with an authored label 65% of
the time, and the headline percentage agrees far better than the verdicts do,
because its two error directions cancel.** Read the number as a rough signal
over many cases, never as a verdict on one, and never as a small movement
between versions.

## Measured against a labelled set

The `evals/ecommerce` set in `credibledata/malloy-samples` labels every case
`covered`, `derivable` or `absent` by hand, and names the entity that would
close each one. That makes it a ground truth to score the script against. All 49
cases, `--repeat 1` (the set default), model text 19,258 chars:

| | |
|---|---|
| script | coverage 45%, 22 of 49 |
| set, counting `covered` | 43%, 21 of 49 |
| **per-case agreement** | **32 of 49, 65%** |

The two headline numbers land two points apart and that is a coincidence. There
are 8 cases the script calls a gap and the set calls covered, and 9 the script
calls `ok` and the set does not. They very nearly cancel, so the aggregate looks
trustworthy while a third of the individual verdicts are not. A number built
that way can move without the model changing and hold still when it does, which
is the one thing a per-version trend must not do.

The full matrix, authored label down the side and verdict across:

| | `ok` | `CONVENTION` | `NO-DISAMBIG` | `COVERAGE` |
|---|---|---|---|---|
| `covered` (21) | 13 | 3 | 3 | 2 |
| `derivable` (24) | 9 | 9 | 4 | 2 |
| `absent` (4) | 0 | 0 | 0 | 4 |

Two things to read off it.

**`absent` is the one class it gets right, 4 for 4.** Where the model holds
nothing at all, `COVERAGE` is reliable. That is also the easiest judgement in
the set, so it is weak evidence for the rest.

**`derivable` splits four ways.** One authored class, 9 `ok`, 9 `CONVENTION`, 4
`NO-DISAMBIG`, 2 `COVERAGE`. This is the class the metric exists to separate
from `covered`, and the verdict on it is close to unreproducible.

## Two named defects behind those errors

**A named measure plus a filter reads as a gap.** Three of the eight false gaps
are exactly that shape, and the set's own note says so:

| case | verdict | the entity that covers it |
|---|---|---|
| `ecom_unsold_inventory` | `COVERAGE` | `inventory_item_count`, filtered to unsold |
| `ecom_unsold_stock_value` | `COVERAGE` | `total_cost`, filtered to unsold |
| `ecom_levis_sales` | `NO-DISAMBIG` | `total_sales`, filtered to brand |

Verified against the model, not taken from the label: `inventory_item_count is
count(id)` and `total_sales is sale_price.sum()` are both declared, and
`sold_at` is public and documented, so the filter has something to bind to. Two
of these came back `COVERAGE`, which asserts no entity represents the concept
ANYWHERE. That is the strongest claim the script makes and it is false, and
because `COVERAGE` is a cause code it sends diagnosis at a modelling gap that
does not exist. The prompt has no rule telling the judge that filtering a named
measure is still expressing it.

**The prompt's own rule 2 is not applied, in the other direction.** It says
finding the numerator is not coverage, and that a denominator which must be
assembled or chosen is `CONVENTION` or `NO-DISAMBIG`, "never `ok`". Nine cases
break it, each one a ratio the set records as having no measure:
`ecom_shipped_item_share` (note: no measure, shipped over all lines),
`ecom_return_rate` (no `return_rate` measure), `ecom_avg_spend_per_customer` (no
measure, `total_sales / user_count`), and six more. So the two error directions
are not noise in opposite directions; they are one rule missing and one rule
ignored.

## It stops RUNNING at about 3,500 lines, on Linux only

Measured separately, on model size rather than on accuracy. Density first,
measured by concatenating example packages and reading `usage` off a real judge
call: Malloy text runs **2.70 chars per token** and **37 chars per line**, so a
10,000-line model is about 370 KB and 136,000 tokens.

The prompt goes in `argv`. Linux caps a single argument string at
`MAX_ARG_STRLEN`, 131,072 bytes, independently of `ARG_MAX`; measured in
`python:3.12-slim`, 131,000 bytes execs and 131,072 raises `OSError 7 Argument
list too long`. macOS has no per-argument cap below its roughly 1 MB total, so
the same model that works on a developer's laptop fails in CI and in a Linux
checkout.

`MAX_PROMPT` is 400,000, so it does not catch this: three times the real Linux
ceiling, and it counts CHARACTERS while `argv` spends UTF-8 BYTES, so a model
with non-ASCII doc comments passes the guard by a wider margin still. Anything
over roughly **3,500 lines** clears the guard and then fails the exec.

The failure is shaped like a different bug. `run_cli` retries on `no_text`, so a
deterministic `E2BIG` is retried with the full backoff and lands as `no reply
from the model ([Errno 7] Argument list too long)`, once per case, which is what
a rate limit also looks like. Every case comes back undecided, `coverage` prints
`n/a`, and the process still **exits 0**.

## It stops being STABLE at about 100 KB, everywhere

The shipped fixture exists to prove the measurement judges expressibility rather
than matching the question's words against field names. Growing only the
haystack around it, needle and prompt untouched:

| model text | verdict | |
|---|---|---|
| 2.8 KB, as shipped | `CONVENTION` | correct |
| 103 KB | `ok` | WRONG, reproduced at `--repeat 5` |
| 373 KB | `CONVENTION` | correct |

At 103 KB the fixture reads `ok`, precisely the field-name matching it was
written to detect, and the majority of five samples did not rescue it. The
pattern is instability rather than decay: above roughly 100 KB the verdict stops
being a function of the model's content, and it drifts toward `ok`, the
optimistic direction `majority()` is written to avoid. 103 KB is a quarter of
`MAX_PROMPT` and under a third of a 10,000-line model, so a package can sit
inside every limit the script enforces and still produce a number that means
nothing.

One caveat on method: the haystack was example packages repeated with renamed
sources, so this measures dilution and recall rather than a genuinely distinct
200-file package. Treat the direction as established and the threshold as
approximate.

## What it costs

The model text and the question share ONE user message block, so the question,
which changes per case, sits inside the cached prefix and the model is re-cached
every call. Measured on two calls with the same 100 KB model and different
questions: `cache_creation_input_tokens` 37,033 and `cache_read` 0 for the
model. Moving the model into `--append-system-prompt` and leaving only the
question in the message gave `cache_read` 50,754 and `cache_creation` 1,159 on
the second call.

| per case | as written | model cached |
|---|---|---|
| 19 KB model (the ecommerce set) | $0.08 | $0.05 |
| 100 KB model | $0.181 | $0.016 |
| 370 KB model, 10k lines | $0.55 | $0.08 |

The 49-case run above cost about $5.72 and took about 11 minutes at
`--parallel 8`. The script reports neither, which for a measurement sold on
being cheap is its own gap: nothing in the output says what the number spent.

## Do NOT narrow to one file to get under the cap

The over-size message suggests pointing `--model` at a single `.malloy` file. Do
not, and do not read a coverage number produced that way. `model_text()`
concatenates every `*.malloy` under the tree and does **not** resolve `import`;
real packages split across files exactly that way (in
`credibledata/malloy-samples`, `ecommerce/brand_synergy.malloy` opens with
`import "ecommerce.malloy"`). Narrowing to one file drops every imported source,
so the judge correctly answers `COVERAGE`, no entity represents the concept
anywhere, for cases the model answers perfectly well. That is a false gap
indistinguishable from a real one, and it lands in the numerator of a published
trend.

If the model does not fit, the honest outcome is a failed measurement, not a
measurement of a fragment.

## Where to take it

Ordered by what each fixes.

**The two prompt defects first, because they are what makes the number wrong at
every model size.** Give the judge a rule that filtering or grouping a named
measure is still expressing it, so a measure plus a filter stops reading as
`COVERAGE`. Then make rule 2 bite: nine ratio cases reached `ok` against an
explicit "never `ok`", which suggests the rule needs to be a checked step in the
output rather than a line of prose, for example an assembled-denominator field
the parser can refuse an `ok` against, the way `parse_reply` already refuses an
`ok` with several unresolved candidates.

**Score the script against the authored labels as a fixture.** The set already
carries 49 hand-labelled cases and a `coverageNote` naming the closing entity.
Agreement against those labels is a cheap regression test on the prompt, and it
is the test that would have caught both defects above.

**Pass the prompt on stdin.** `claude -p` reads it there (verified), which
removes the `argv` ceiling outright. Then `MAX_PROMPT` should bound bytes
against the agent model's context window rather than characters against a Linux
limit it does not match.

**Exit 1 when `decided` is 0.** A run that decided nothing did not run, and the
docstring already promises exit 1 for that case.

**Note that the two cheap fixes pull against each other.**
`--append-system-prompt` is what buys the caching above, and it is also `argv`,
so on Linux it is stuck under the same 131,072 bytes. There is no
`--append-system-prompt-file`. Both cannot be had from this CLI surface, which
is the argument for not making this judge a `claude -p` call at all: it holds no
tools, is granted a second turn only to absorb a stray tool call, and scans
braces out of prose because the reply is not structured. A cached system block
for the model, a volatile user block for the question, and a structured output
format address the cost, the ceiling and `json_objects()` together. Stdlib
`urllib` keeps the stdlib-only promise; the real cost is auth, since `claude -p`
rides the CLI's credentials.

**And for a large package, stop sending the whole model.** Rule out `COVERAGE`
against a complete inventory of names and one-line docs, a small fraction of the
bytes, which keeps "no candidate anywhere" decidable; then send full text for
the candidate sources only, to settle `AMBIGUOUS`, `NO-DISAMBIG` and
`CONVENTION`. Deliberately NOT retrieval ranking: folding retrieval into this
number would collapse the distinction from recall that the metric exists to
draw.
