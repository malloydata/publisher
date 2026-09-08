<!-- What check_coverage.py does on a large package: where it stops running, where it stops being true, and what it costs. Read before pointing it at anything bigger than a few thousand lines. -->

# Coverage at scale

`check_coverage.py` hands the WHOLE model text to one judge call, once per case.
That is what makes it cheap on a small package and what breaks on a large one.
Every number below was measured, not estimated; the method is named beside each
one so it can be re-measured when the harness changes.

The density figures the rest of this file uses, measured by concatenating the
bundled example packages and reading `usage` back off a real judge call:

| | measured |
|---|---|
| Malloy text | 2.70 chars per token |
| Malloy text | 37 chars per line |
| so a 10,000-line model is | ~370 KB, ~136,000 tokens |

## It stops RUNNING at about 3,500 lines, on Linux only

The prompt goes in `argv`. Linux caps a single argument string at
`MAX_ARG_STRLEN`, 131,072 bytes, independently of `ARG_MAX`; measured in
`python:3.12-slim`, 131,000 bytes execs and 131,072 raises `OSError 7 Argument
list too long`. macOS has no per-argument cap below its ~1 MB total, so the same
model that works on a developer's laptop fails in CI and in a Linux checkout.

`MAX_PROMPT` is 400,000, so it does not catch this: it is three times the real
Linux ceiling, and it counts CHARACTERS while `argv` spends UTF-8 BYTES, so a
model with non-ASCII doc comments passes the guard by an even wider margin.
Anything over roughly **3,500 lines** clears the guard and then fails the exec.

The failure is loud enough to read but shaped like a different bug. `run_cli`
retries on `no_text`, so a deterministic `E2BIG` is retried with the full
backoff and lands as `no reply from the model ([Errno 7] Argument list too
long)`, once per case, which is what a rate limit also looks like. Every case
comes back undecided, `coverage` prints `n/a`, and the process still **exits
0**.

## It stops being TRUE at about 100 KB, everywhere

This is the limit that matters, because nothing reports it.

The shipped fixture exists to prove the measurement judges expressibility rather
than matching the question's words against field names. Growing only the
haystack around it, needle and prompt untouched:

| model text | verdict | |
|---|---|---|
| 2.8 KB, as shipped | `CONVENTION` | correct |
| 103 KB | `ok` | WRONG, and reproduced at `--repeat 5` |
| 373 KB | `CONVENTION` | correct |

At 103 KB the fixture reads `ok`, which is precisely the field-name matching it
was written to detect, and `--repeat` does not rescue it: the majority of five
samples was `ok`. The pattern is not decay but instability. Above roughly 100 KB
the verdict stops being a function of the model's content, and it drifts toward
`ok`, the optimistic direction `majority()` is written to avoid. The default for
a set is `--repeat 1`, so at that size each case is close to a coin toss.

103 KB is a quarter of `MAX_PROMPT` and under a third of a 10,000-line model, so
a package can sit well inside every limit the script enforces and still produce
a number that means nothing.

One caveat on the method: the haystack was the bundled example packages repeated
with renamed sources, so this measures dilution and recall rather than a
genuinely distinct 200-file package. Treat the direction as established and the
exact threshold as approximate.

## What it costs

The model text and the question share ONE user message block, so the question,
which changes per case, sits inside the cached prefix. Measured on two calls
with the same 100 KB model and different questions, the model text is re-cached
every call: `cache_creation_input_tokens` 37,033, `cache_read` 0 for the model.
Moving the model into `--append-system-prompt` and leaving only the question in
the message gave `cache_read` 50,754 and `cache_creation` 1,159 on the second
call.

| per case | as written | model cached |
|---|---|---|
| 100 KB model | $0.181 | $0.016 |
| 370 KB model (10k lines) | $0.55 | $0.08 |

| a whole run, 370 KB model | as written | model cached |
|---|---|---|
| 25 cases, `--repeat 1` | $13.82 | $2.06 |
| 50 cases, `--repeat 3` | $82.91 | $12.39 |

Multiply by every published version you want on the trend line. The claim this
metric is sold on, cheap enough to point at every version, holds at a few
thousand lines and not at ten thousand.

## Do NOT narrow to one file to get under the cap

The over-size message suggests pointing `--model` at a single `.malloy` file.
Do not, and do not read a coverage number produced that way. `model_text()`
concatenates every `*.malloy` under the tree and does **not** resolve `import`;
the bundled packages split across files exactly that way
(`examples/storefront/data_app.malloy` imports `storefront.malloy` and
`givens.malloy`). Narrowing to one file drops every imported source, so the
judge correctly answers `COVERAGE`, no entity represents the concept anywhere,
for cases the model answers perfectly well. That is a false gap indistinguishable
from a real one, and it lands in the numerator of a published trend.

If the model does not fit, the honest outcome is a failed measurement, not a
measurement of a fragment.

## Where to take it

Ordered by what each fixes.

**Pass the prompt on stdin.** `claude -p` reads it there (verified), which
removes the `argv` ceiling outright. Then `MAX_PROMPT` should bound bytes
against the agent model's context window rather than characters against a Linux
limit it does not match.

**Exit 1 when `decided` is 0.** A run that decided nothing did not run, and the
docstring already promises exit 1 for that case.

**Note that the two cheap fixes pull against each other.**
`--append-system-prompt` is what buys the caching above, and it is also `argv`,
so on Linux it is stuck under the same 131,072 bytes. There is no
`--append-system-prompt-file`. Both cannot be had from this CLI surface.

**Which is the argument for not making this a `claude -p` call at all.** This
judge holds no tools, is granted a second turn only to absorb a stray tool call,
and scans braces out of prose because the reply is not structured. Those are
symptoms of driving an agent harness where a Messages API call belongs: a cached
`system` block for the model, a volatile `user` block for the question, and a
structured output format instead of `json_objects()`. Stdlib `urllib` keeps the
stdlib-only promise. The real cost is auth, since `claude -p` rides the CLI's
credentials and raw HTTP needs a key or an OAuth bridge.

**None of which fixes the 100 KB problem, and that one decides whether the
metric means anything on a large package.** Handing a judge 136,000 tokens and
asking it to enumerate every candidate for every quantity invites exactly the
recall failure measured above. The fix is to stop sending the whole model: rule
out `COVERAGE` against a complete inventory of names and one-line docs, which is
a small fraction of the bytes and keeps "no candidate anywhere" decidable, then
send full text for the candidate sources only, to settle `AMBIGUOUS`,
`NO-DISAMBIG` and `CONVENTION`. Deliberately NOT `get_context` ranking: folding
retrieval into this number would collapse the distinction from recall that the
metric exists to draw.

**And give the fixture a diluted variant.** The metric's only self-check runs at
2.8 KB and fails at the size the metric is advertised for.
