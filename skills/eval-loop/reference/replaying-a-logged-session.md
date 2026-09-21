# Scoring a session that ran somewhere else

`reference/log-scrape.md` covers turning logged traffic into CASES: questions
with goldens, to be answered fresh. This file covers the other half. The agent
already ran, somewhere you do not control, and what you have is its record. You
want that scored by the same loop, so a local run and a hosted one can be read
side by side.

The rule that makes it safe is one sentence: **what the source recorded decides
what the run may claim.** Everything below is that sentence applied.

## The three tiers

A source is not a detail of the fetch. It is the ceiling on the measurement, so
it is pinned in `run.json` as `source` and `sourceTier` and stated in the
report.

| Tier | The source carries | You can measure | You cannot |
|---|---|---|---|
| **T1** | the tool calls: what was searched for, what came back, what query ran | retrieval recall and precision, the bare-target rate, scoping mistakes, whether the query was constructible | anything about the answer |
| **T2** | plus the agent's visible prose | all of the above, and a judged verdict | contamination |
| **T3** | plus a host-side tool log of every tool, not just the MCP ones | all of the above, and contamination decided | nothing |

A run this harness spawned is T3, always. It sees the agent's own tool uses,
including a `Read` of a file, which is what makes contamination a decision
rather than a guess.

**T1 has no pass rate.** Every attempt scores `verdict: null` with reason
`no_answer_captured`, so the denominator is zero and any percentage computed
from it is invented. Report retrieval numbers and say the tier. This is not a
formality: at T1 the agent can receive exactly the right entities, run exactly
the right query, and then state the wrong number in prose nobody logged. That
case reads as recall 1.0 with no verdict, and a reader who sees "recall 1.0"
next to a pass rate borrowed from somewhere else will conclude the model is
fine.

**T2 cannot decide contamination**, so `contaminated` is `"unknown"`, and the
ledger treats unknown as not-clean. A server saw the MCP calls; it could not
have seen the agent read a gold CSV. Do not write `false` there to make a run
look complete.

## How to run one

1. **Have cases first.** `fetch_transcripts.py` reads each case's `source` as
   the id of the session that produced it, which is the id
   `skill:eval-import` tells a log scrape to keep. A case with no `source` is
   named and skipped, not guessed at.
2. **Fetch.** `fetch_transcripts.py --set <set> --out <runDir> --tier T1|T2`
   writes `artifacts/<qid>/answerer.jsonl` per case. It writes no ledger event
   and makes no scoring decision.
3. **Score with the existing command.** `run_baseline.py --set <set> --out
   <runDir> --rebuild --target platform`. There is no log-specific scoring
   path; `--rebuild` derives the run from these transcripts exactly as from
   spawned ones.
4. **Read the empty-session warning.** A session whose id matched no rows
   produces a transcript with no calls. That is a fetch problem and it is not
   an agent that did nothing; scoring it records the second when the truth is
   the first.

## Pass the per-call error, or the judge gets the wrong query

The attempt's final query is chosen as the last call the server ANSWERED. A
transcript in which every call looks successful therefore hands the judge
whatever ran LAST, and for an agent that made a syntax error and then fixed it
that is the broken one.

Measured, replaying a real 8-case arm through the log path with no per-call
outcomes: 4 of 8 attempts changed their final query, 3 lost their error counts,
and only 1 of 8 tool-call streams matched the spawned run. One attempt was
handed a query whose aggregate list was semicolon-separated and had errored.

Hosts help here by accident: the common pattern is to log a query's COMPILE
ERROR and nothing at all for a success, and that asymmetry is exactly what is
needed. Map it to the row's `error` field. With errors carried and the calls in
observed order, the same replay matched the spawned arm on 8 of 8 tool-call
streams, 0 of 8 error counts, and 7 of 8 final queries.

## What the last one costs, and why it cannot be fixed

The single attempt that still differs is the shape of the whole tier. Its
baseline `final_query_source` was `declared`: the agent PRINTED its final query
in the answer, and that beats any guess from the call log. At T1 there is no
prose, so the choice falls back to the last answered call, which in that
attempt was a broad exploratory probe rather than the narrow query the answer
actually rested on.

This is the concrete form of "the log cannot see what the agent did after the
rows came back". It is not a bug to fix; it is what `sourceTier` is for. Read a
T1 `final_query` as the last thing that ran, not as the answer's query, and do
not build a construction argument on it alone.

## Re-execution: do not build one

The answerer and the conductor must hit the same target, and for a logged
session the answerer hit the host. Re-executing its query against a local copy
of the model scores neither.

This needs no work, because `--target platform` already does the right thing:
it sets `reexec = False`, tells the judge the predictions were not re-executed
and to lower its confidence accordingly, and skips the model-source lint. Use
it, and say in the report what it costs. A T2 hosted run's verdicts rest on the
answer text and the golden alone, with no rows and no model text, and the
`expectedEntities` staleness lint does not run either, so a set written against
an older package reads as retrieval misses until someone checks by hand.

## What does not carry across targets

**Entity ids may not be portable.** On some hosts an entity's name is a full
Malloy field path, so a joined field arrives as `hiring_manager.employee_count`
where Publisher reports `employee_count` on the joined source. The ids differ
for the same logical entity, so a set's `expectedEntities` is not portable
between those targets until both emit a canonical id. A local/hosted comparison
of retrieval recall on the same set is not valid across that gap. Check a
handful of ids by hand before quoting one.

**Query results are not logged**, only the query text and, on some hosts, its
compile errors. That is by design and it is why re-execution exists at all.

## Sanity checks before you trust a pull

- Take one session you can identify and read its transcript by hand. Does the
  search text match what the person actually asked? A stitch that joins the
  wrong rows returns data and looks like a success.
- Compare the call count against the host's own view of that session. A row
  limit silently truncating a long session makes an agent look decisive.
- Read the tier line. A T2 fetch that came back with no prose for a session is
  written as T1 for that session and says so, because claiming
  `answer_captured: true` over an empty answer is the one combination that
  scores a real agent as having said nothing. If EVERY session downgrades, the
  host is not serving the prose source and you have a T1 run, whatever you
  asked for.
