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

Three things about its shape:

- **The unit is a prediction, not a question.** One question earns different
  verdicts for different answers, legitimately. Key the fixture on the answer.
- **Judge through the same code path a run uses.** A reimplementation inside the
  checker can pass while the thing it stands for is broken.
- **Pin every fixture to a `goldenRevision`.** The verdict was settled against
  one answer key, so a golden repair can leave it asserting something true about
  a question nobody is asking. `check_judge.py` names the unpinned ones, and
  skips a fixture whose pin the case has moved past rather than failing it: the
  judge did not regress, the key moved. Re-settle and re-pin those.

## Cover the classes the verdicts turn on

A file of a dozen fixtures that all test the same decision checks one decision.
`check_judge.py` counts fixtures by their `protects` value and warns for each of
these that nothing covers:

| `protects` | What it holds |
|---|---|
| `required_disclosure` | An answer with the right number that omits a `REQUIRED` caveat. Both of the ecommerce set's headline failures turned on this. |
| `refusal_correct` | A refusal against an `unanswerable` golden, which is the pass. |
| `refusal_wrong` | A refusal against an answerable case that sounds unanswerable, which is `no_match`. Measuring only the first rewards a judge that blesses every refusal. |
| `gold_status` | A prediction whose golden is wrong, where the judge must score against the key as written and separately say it does not believe it. |
| `near_match_boundary` | An answer sitting on the `match` / `near_match` line. This is where most measured judge noise lives. |

Seed each from a real prediction and settle the verdict by hand. Never author a
prediction to fill a slot: a fixture is a record of a judgement about something
an answerer actually produced, and an invented one tests the judge against
fiction.

## Prove a fixture bites

A fixture that has never failed is not yet known to be a test. Break the
doctrine on purpose once and confirm the right entry fails, on a copy so the
repo is never the broken thing:

```bash
cp -R skills /tmp/broken-skills
# remove ONE rule from /tmp/broken-skills/eval-judge/SKILL.md, such as the
# REQUIRED-disclosure clause, then:
python3 skills/eval-loop/scripts/check_judge.py --set <set> \
  --skills-root /tmp/broken-skills --only <the qid that rule protects>
```

The fixture protecting that rule should now FAIL. If it passes, the fixture is
not testing what its `protects` claims and the coverage count above is
overstating what you have. The judge pin follows `--skills-root`, so the report
names the broken doctrine rather than the repo's.

When a fixture fails, rule out judge nondeterminism (`--repeat`) before you
believe it. Then either you moved a verdict you did not mean to, or the fixture
was wrong -- re-settle it and record why. Deleting it throws away the only case
you had evidence about.
