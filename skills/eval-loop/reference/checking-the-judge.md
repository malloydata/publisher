<!-- The judge measures the model; this measures the judge. Read after any change to judge doctrine or its inputs. -->

# Checking the judge

## Nothing else checks the judge

The A/A band measures whether the judge is *repeatable*. It says nothing about
whether it is *right* -- a judge answering `no_match` every time posts a perfect
band. Those come apart in practice, and when they do the loop keeps running and
every number it emits is wrong in the same direction.

So keep a small file of frozen predictions pinned to verdicts a human settled,
and re-run them after any edit to the judge prompt, a rubric, or what the judge
is given (`scripts/check_judge.py`, `judge-regressions.jsonl` in the set). Seed it
from the cases an A/A pair disagreed on: those are the contested ones, so they
are where a change will show first.

Two things about its shape:

- **The unit is a prediction, not a question.** One question earns different
  verdicts for different answers, legitimately. Key the fixture on the answer.
- **Judge through the same code path a run uses.** A reimplementation inside the
  checker can pass while the thing it stands for is broken.

A fixture that has never failed is not yet known to be a test. Break a rubric on
purpose once and confirm the right entry fails.

When a fixture fails, rule out judge nondeterminism (`--repeat`) before you
believe it. Then either you moved a verdict you did not mean to, or the fixture
was wrong -- re-settle it and record why. Deleting it throws away the only case
you had evidence about.
