<!-- How to set gold_status. Read this when the golden itself looks wrong. -->

# When the answer key looks wrong

Score against the golden as written. Then say, separately, whether you believe
it. Those are two different jobs and `gold_status` is the second one.

**The verdict never bends.** If the prediction does not contain the golden, that
is `no_match`, whatever you think of the golden. An answer does not pass because
you suspect the key. Doubt goes in `gold_status`, and something downstream
adjudicates it; a judge that quietly graded against its own better answer would
be the only record of having done so.

| Value | Meaning |
|---|---|
| `verified` | No reason to doubt it. The default, and the honest answer nearly always. |
| `verified_benign` | Reachable defect that cannot change this verdict -- e.g. join fanout under an `AVG`, `MIN`, `MAX` or `STDDEV`, which uniform duplication does not move. |
| `suspect` | Something does not add up and you cannot settle it from what you were given. |
| `verified_wrong` | You can demonstrate the key is wrong, and say how. Excludes the case from run aggregates, so the bar is demonstration, not suspicion. |

What earns more than `verified`:

- **The rubric contradicts the model.** You have the model source. A rubric
  saying "`lifetime_orders` counts line items despite its name" against a model
  reading `lifetime_orders is count(order_id)` is a rubric written before a fix
  and never revisited. That is `suspect` at least, and the judge is the only
  station positioned to notice -- this exact case failed two correct answers for
  a full run.
- **The golden and its own `canonicalQuery` disagree**, where you can see both.
- **The golden is impossible against the re-executed rows** -- a total below one
  of its own parts, a rate outside 0 to 1, a count above the population.
- **Fanout you can identify**, benign or otherwise, per the classification above.

What does not: the answer being more useful, better presented, or more recent
than the key. Disagreeing with the question's premise is not a defect in the
answer to it.

`gold_note` says what you saw, concretely enough to check -- the two values, or
the model line against the rubric sentence. "Golden looks off" routes nothing.
