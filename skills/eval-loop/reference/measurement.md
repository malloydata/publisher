<!-- Sampling, the flip-count bar, the A/A noise band, and targeted fixes. Read this before quoting any number. -->

# Measurement

## Measurement

**Sample each case once. Spend the budget on more and more varied cases
instead.** Repeats past the first buy very little: variance decompositions of
LLM evaluation put the reduction from extra repeats at a small fraction of
what extra items buy, and a set of five cases run three times cannot support
a claim that fifteen distinct cases can. If a case is genuinely borderline,
re-run that case, not the whole set.

Because a single sample cannot carry a mean, do not report before/after as a
score delta. **Count the cases whose verdict changed** between the baseline
and the post-edit run, discard the unchanged ones, and read the result off
this table:

| Cases that got worse | Cases that must get better to accept |
|---|---|
| 0 | 5 |
| 1 | 7 |
| 2 | 9 |
| 3 | 10 |

Below that bar the change is **unresolved**, not an improvement, and saying
so is the honest report. Note the consequence before you scope a run: a set
of six cases can essentially never clear this bar, so a set that small can
measure a baseline and diagnose failures but cannot defend an edit.

## Calibrate the bar before you trust it

This table was asserted, not measured, and the number it needs is a property of
your harness and your set -- not of this skill. Measure it with an **A/A run**:
the same model, same config, same set, twice, compared with
`scripts/flip_table.py`. Every flip it reports is noise by construction, since
nothing changed. Record the result with the set, in `CALIBRATION.md`, and cite
that file when you quote a band.

Re-measure whenever the model, judge, or set changes. This is not a formality:
observed bands have moved by a factor of three across a fortnight of ordinary
work, so a band carried over from a previous configuration is a number with no
claim on the present one.

One A/A is one sample of the flip count, not a distribution. It can show a bar
is too low; it cannot show one is high enough. Treat any measured band as a
floor.

Two consequences worth separating:

- **For acceptance**, the band is the threshold untargeted flips must sit under.
- **For diagnosis**, it is a warning that a single run's failure list is partly
  luck. Pick what to fix from the failures that fail in **both** A/A runs.
  Ranking a backlog by one run's clusters partly ranks which cases were unlucky
  that afternoon.

When you inspect the flips, attribute them before you accept them as
irreducible. A band dominated by the **judge** re-reading an ambiguous rubric is
not answerer noise, and it is not a floor you have to live under: sharpening
those rubrics buys more measurement power than any change to the answerer.

An A/A is not a repeat in the sense the sampling rule forbids. It is a one-off
calibration of the instrument, and the loop's whole acceptance rule rests on
the constant it produces.

## A targeted fix needs a targeted test

The flip-count table is the right instrument for a broad change and the wrong
one for a narrow fix. A fix that repairs three cases on a 49-case set moves the
total by three -- inside the noise band an A/A already produces -- so a
mechanically-verified repair reports as no effect and gets abandoned.

This is not a hypothetical failure mode: a mechanically verified repair, where
each fixed case now matches its golden exactly, can read as "no effect" on both
of two set-total comparisons. Worked examples are in the set's `CALIBRATION.md`.

So for a narrow fix use `scripts/flip_table.py --targets --noise-band`:

1. **Name the cases before the run.** Pick them from the stable failures of the
   A/A, never from a single run. Choosing them afterwards is choosing the answer.
2. Accept on the targeted cases: they were failing, they now pass, and none of
   them broke.
3. Separately require the untargeted flips to sit **at or below the A/A band**.
   That is what rules out a fix that trades one set of cases for another --
   above the band, investigate before accepting, however good the targets look.
4. **Run the post-edit arm twice and pass both** (`--b --b2`). The band counts
   flips; it never asks which cases flipped, and that is the hole. Noise
   scatters, so an untargeted case that breaks in *both* post arms is a real
   regression however small the count is.

Report both. A targeted win with untargeted flips above the band is not a win,
and a set-total that moved by less than the band is not evidence of anything
either way.

Step 4 exists because the band alone has accepted a real regression: an edit
whose untargeted flip count sat inside the band, but where the same untargeted
case broke in every post arm. One arm cannot tell that from a coin toss.

The reason to expect this, rather than treat it as bad luck: **a correct new
entity is not a safe one.** Adding a measure changes what agents reach for on
questions nobody was thinking about, so a well-named addition can pull a
neighbouring question onto the wrong denominator. That makes the untargeted
half of the acceptance check the half that matters, and it needs two arms to be
readable at all.

The one retrieval number this loop reports is **per-question entity recall**:
of the entities each golden answer depends on, how many did the agent's own
`get_context` calls deliver (`skill:eval-answer`, `scripts/score_retrieval.py`;
delivered means returned as a ranked entity, under a sibling source, or named
in a returned source's documentation). It is measured on the agent's real
search text against real questions, so it needs no hand-written terms. Read
it within an arm, to attribute a failure; it moves with the answerer, so a
cross-arm comparison of retrieval *itself* is not this loop's job -- that is
the engine-side `eval-retrieval` skill, which ships to no customer.
Coverage (can the model answer this at all) is a property of the model and its
data and is never reported under a retrieval heading.

If the contaminated fraction of attempts exceeds 0.1 (or any contamination,
on a run smaller than 10), the run is a harness failure. Do not publish a
model score.
