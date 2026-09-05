<!-- For whoever AUTHORS a case. Not needed to judge one. -->

# Writing a rubric the judge can execute

A case rubric is not prose for a human to weigh. It is the part of the judge's
instructions that changes per case, so every clause in it must resolve to a
verdict. Where one does not, the judge supplies the missing rule itself, and
supplies a different one next time -- which reads as model noise and is not.

Two clause types cause almost all of it. Both must carry their consequence.

**An alternate reading** -- a second defensible answer to the same question.
Mark each one, and never leave the set open:

| Marker | Verdict | Use when |
|---|---|---|
| `PREFERRED` | `match` | The reading the golden encodes. Exactly one. |
| `ACCEPT` | `match` | Equally right. A different but faithful route to the same claim. |
| `DIVERGENT` | `near_match` | Defensible, and not what was asked for. Usually a population or grain the model does not distinguish. |
| `WRONG` | `no_match` | Plausible and incorrect. Name the trap value so the judge can recognise it. |

**A disclosure** -- something the answer must SAY, beyond the number. Say what
silence costs:

| Marker | Verdict when omitted | Use when |
|---|---|---|
| `REQUIRED` | `no_match` | Without it the answer misleads. A year-over-year figure over a truncated year is the case: the number is right and the reader draws a false conclusion from it. |
| `CREDITED` | `match`, no deduction | It adds context a good analyst would give. Its absence leaves the reader correct but less informed. |

Rules that follow from this:

- **Write the question so its answer is data.** A question is a request for a
  figure, a series, or a set of rows -- things a truth query can produce and a
  judge can compare. "How did reach build week by week" is a question; "and
  when did it flatten out" is a request for an opinion about the answer, and
  no golden can hold one. Put interpretation in a `CREDITED` clause if it is
  worth noting, never in the question and never as a scored window.
- **Fix the grain in the question, or accept every grain in the rubric.** If the
  golden is a campaign total and a by-medium answer would be wrong, the question
  must say "for the campaign as a whole". If it does not, the rubric must accept
  a correct figure at any stated grain (judge rule 11). A rubric that quietly
  assumes the golden's grain fails correct answers.
- **A right value plus a missing `CREDITED` disclosure is a `match`.** Not a
  near match. Do not deduct for it.
- **`DIVERGENT` is about definitions, not arithmetic.** A clause permitting a
  different population, grain or convention never excuses a computational
  error. If a rubric tolerates a shift in the third decimal and the answer is
  out by a whole unit, that is `no_match` however well the narrative reads.
- **An unmarked clause is `CREDITED`.** The judge must not invent a
  requirement. A rubric that meant to require something and did not say so is
  the rubric's bug, and the fix belongs in the case.
- **Stable `near_match` is a finding, not an outcome.** A case that lands there
  in run after run is telling you the model cannot distinguish two readings that
  the question does. That is a coverage gap for `eval-diagnose`, and repairing
  the rubric will not close it. `diagnose.py` selects `no_match` only by
  default, so these fall straight through unless you hand them over: take the
  stable list `flip_table.py` prints for a pair of arms and pass
  `--only <qids> --verdicts near_match`. Stability across two arms is the whole
  qualification. A `near_match` in one arm is noise, and diagnosing it sends an
  agent to fix a model that is already right.
