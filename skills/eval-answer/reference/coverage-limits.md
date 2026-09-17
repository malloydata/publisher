<!-- What a coverage number does and does not measure. Read before quoting one. Measured against a labelled set and across model sizes; none of it is visible in the output the script prints. -->

# Coverage limits

`check_coverage.py` was measured against a real labelled set and at a range of
model sizes. Every number below is measured, and the method is named beside it
so it can be re-measured when the harness changes. None of it is visible in the
output the script prints today.

The short version. **Read it as a rough signal over many cases, never as a
verdict on one, and never as a small movement between versions.** Where it
disagrees with a set's authored `coverage` label, either side can be the stale
one, and on the one set measured the LABEL was wrong more often than the verdict
was. Separately, the whole model goes into one prompt per case, which caps the
package size it can run on at all and destabilises the verdict well before that
cap.

## Measured against a labelled set: the label drifts too

The `evals/ecommerce` set in `credibledata/malloy-samples` labels every case
`covered`, `derivable` or `absent` by hand, and names the entity that would
close each one. Running all 49 cases against that package's model, `--repeat 1`,
19,258 chars of model text over three files:

| | |
|---|---|
| script | coverage 45%, 22 of 49 |
| set, counting `covered` | 43%, 21 of 49 |
| cases where the two disagree | 17 |

**Read the disagreements, not the two-point gap.** The headlines land close
because the two directions of disagreement nearly cancel, 8 cases the script
calls a gap that the label calls covered against 9 the other way. A number that
agrees for that reason can move without the model changing and hold still when
it does.

**And do not assume the checker is the wrong side.** Every disagreement was
checked against the model by hand. On the majority the LABEL was stale or
wrong, because the model gained measures and the standing judgement was not
revisited:

| case | label says | the model actually declares |
|---|---|---|
| `ecom_time_to_ship` | "no measure; shipped_at - created_at" | `avg_days_to_ship is avg(days_to_ship)`, documented |
| `ecom_gross_margin_pct` | "average_gross_margin is per item, not margin / sales" | `margin_rate is total_gross_margin / nullif(total_sales, 0)` |
| `ecom_top_margin_pct_categories` | "no margin-pct measure to group by category" | the same `margin_rate`, groupable |
| `ecom_return_rate` | "no return_rate measure, the model has no rate measure at all" | `percent_purchases_returned`, a returned-over-all ratio |

The reverse direction is the same story with the sides swapped.
`ecom_unsold_inventory` is labelled `covered` with the note
"`inventory_item_count`, filtered to unsold", and the checker returns a gap. The
checker is right: the model's doc on `sold_at` says "Despite the name it is not
evidence of a sale: it is a copy of order_items.created_at and is set even when
that order was cancelled", and `inventory_items` declares no join back to
`order_items`, so the anti-join the question needs cannot be expressed at all.
The note prescribes exactly the filter the model warns against.

So the honest reading of this run is that it audited the SET as much as the
model, and that a coverage judgement is only as good as the docs it reads.
`--compare-labels` exists for this: it joins the verdicts against the authored
field and prints the disagreements with their notes, taking no position on which
side is wrong. Check the label first, against the model, before treating a
disagreement as a coverage regression.

One number does hold up on its own. `absent` came back `COVERAGE` 4 times out
of 4, so where the model holds nothing at all the verdict is dependable. That is
also the easiest call in the set.

Do NOT read agreement with these labels as a score for the checker. The two
answer different questions on purpose, which the script's own docstring says,
and this run is the evidence that either side can be the one that has gone
stale.

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
| 2.8 KB, the fixture of the day | `CONVENTION` | correct |
| 103 KB | `ok` | WRONG, reproduced at `--repeat 5` |
| 373 KB | `CONVENTION` | correct |

**These three rows predate the current fixture and have not been re-measured.**
They were taken against a 2.8 KB excerpt that was replaced, when a customer
model was scrubbed out of `check_coverage.py`, by the 1.4 KB `support_desk`
fixture that ships today. What survives the swap is the SHAPE of the curve --
the needle was untouched in all three rows and only the haystack grew -- so read
the 100 KB cliff as the finding and the specific verdicts as history. The
shipped fixture on its own reads `CONVENTION` 3 of 3 (2026-09-08, sonnet).

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

**Run `--compare-labels` whenever a set carries authored `coverage` labels, and
triage what it prints.** It is the cheapest audit available on both the set and
the model, and it needs no extra model calls beyond the run itself. Resolve each
disagreement by reading the model, and expect to fix labels as often as
verdicts. What it must NOT become is a score for the checker: agreement is not
the goal, since the two answer different questions on purpose.

**Do not add a prompt rule on this evidence.** The first reading of that run
was that the checker had two defects, a named measure plus a filter reading as
a gap and the prompt's ratio rule going unapplied. Checking each case against
the model dissolved both: the model documents `sold_at` as NOT evidence of a
sale and declares no join for the anti-join, and the measures the labels called
missing exist. A prompt rule written from that first reading would have taught
the judge to ignore a caveat the model states plainly.

**Pass the prompt on stdin.** `claude -p` reads it there (verified), which
removes the `argv` ceiling outright. Then `MAX_PROMPT` should bound bytes
against the agent model's context window rather than characters against a Linux
limit it does not match.

**Exit 1 when `decided` is 0.** Done: a run that decided nothing now exits 1
and says so, instead of reporting `n/a` and a success. This is the whole-set
shape of the `argv` failure above, where every case fails identically.

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
