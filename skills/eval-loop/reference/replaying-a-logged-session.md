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
   the id of the TURN that produced it: the request id `skill:eval-import`
   tells a log scrape to keep. A case with no `source` is named and skipped,
   not guessed at.
2. **Fetch.** `fetch_transcripts.py --set <set> --out <runDir> --tier T1|T2
   --mcp-url <url> --hosted-mcp-server <name> --logs-scope <org>/<env>/<pkg>`
   writes `artifacts/<qid>/answerer.jsonl` per case. It writes no ledger event
   and makes no scoring decision. Every case is fetched in one query per
   source, because each query is a spawned `claude -p` at about $0.10.
3. **Score with the existing command.** `run_baseline.py --set <set> --out
   <runDir> --rebuild --rejudge --target platform --environment <org>
   --package <workspace>`. There is no log-specific scoring path; `--rebuild`
   derives the run from these transcripts exactly as from spawned ones.
4. **Read the empty-case warning.** An id that matched no rows produces a
   transcript with nothing in it. That is a fetch problem and it is not an
   agent that did nothing; scoring it records the second when the truth is
   the first.

## A case is a turn, not a session

A session is a whole conversation. Measured on real in-app traffic, one
session held a refusal, then a second question, then a third, so fetching the
session for one case hands the judge three answers to grade as one, and lets a
refusal borrow the searches a later turn made. Fetch the turn (`--unit turn`,
the default). `--unit session` is for a host that logs no turn id.

A host may log the ranked results somewhere other than the agent's tool log.
Credible does: the agent's tool log names each call, its order and its outcome,
and the retrieval service logs the results under its own request id. The fetch
takes the agent's log as the authority on which calls a turn made and attaches
each search's results from the service's log when the session matches, the
time is within a few seconds, and the search targets are identical. A search
with no match is counted and reported: its retrieval is unmeasured, not empty.

## The same question, asked many times

Real traffic repeats itself. In one 96-prompt pull, one question was asked
seven times and got four refusals and three different answers from three
different packages. That spread is the finding, so score every one.

A run holds one attempt per case, so each logged answer to the same question is
its own run. `--source-map` takes a JSON object of qid to a turn id or a list of
them. With lists, one fetch (still one query per source) writes
`<out>/attempt-1/`, `<out>/attempt-2/` ..., each an ordinary run holding the
Nth logged answer of every case that has one. Score each with the unchanged
`run_baseline.py --rebuild --rejudge`, then read them together:

```bash
python3 agreement.py --runs <out>/attempt-*
```

It prints, per case, how many logged answers there were, the verdicts by
outcome, how many ran no query, and which packages answered, and it names the
cases that both passed and failed. Such a case has no single quotable verdict:
its pass share is the measurement. `flip_table.py` remains the tool for two
runs of a changed configuration.

## A logged run is not pinned to one package

`--scope` says where a spawned answerer may look. A logged session was never
told, and in the pull this guide was written from, one question was answered
from three packages. So for a run whose `source` is `logs`, `run.json` leaves
`targetVersion` empty and records `queriedPackages` instead, and every attempt
carries the `environment/package@version` its answered queries ran against
(no `@` when the call named no version, which means whatever the workspace
pinned at that moment). The summary prints them, and warns when a set was
answered from more than one package, because a verdict then describes the
package that answered it rather than one model.

Two summary warnings do not apply to a logged run and are replaced: the
answerer's skills (a logged session ran with the host's skills, not ones this
harness granted) and the lexical-retrieval warning (the host's logged
responses do not say which retriever ranked them, which is not a local fallback
to lexical).

## Third-party clients: `--unit window`

The claude.ai connector and Claude Desktop send no session or turn id, and the
host logs none of their prose. What it does log is the person's searches, with
the question when the client passes it, and their MCP queries. `--unit window`
takes a case's id as the search that carried the question and collects that
person's later session-less searches and `surface = 'mcp'` queries in the same
organization, stopping at a 30-minute gap (`--window-gap-minutes`), a search
for a different question, or two hours (`--window-max-minutes`).

That is a reconstruction from timing, not a record, and it can merge two
questions a person asked without a new search between them. Measured on a real
pull: a window opened by one question also took two queries the person
ran twenty minutes later for a different purpose. So a window case is always
T1, scores retrieval and never takes a verdict, and its `final_query` is the
last thing that ran, not the answer's query.

**Your own checks are in the logs.** A conductor who queries a customer
organization through the same MCP while preparing a set produces exactly these
rows, under their own email. Filter your own address out of a pull, or the set
measures your session.

## Number keys against a hosted truth package

A key holding a number is verified when it re-derives through a truth package,
a package of raw tables with no modelling that the answerer cannot reach. That
rule holds on a hosted target too; only where the truth package is served
changes. Publish it on the host, outside the workspace the answerer is scoped
to, and point the check at it:

```bash
python3 verify_goldens.py --set <set> --truth-mcp-url <url> \
    --hosted-mcp-server <name> --truth-organization <org> --environment <env>
```

`run_baseline.py` takes the same as `--truth-mcp-url`, `--truth-organization`
and `--truth-environment` for its pre-run check. Queries go through
`hosted_query.py`, the same transport the fetch uses. A truth package with the
same name as the package under test is refused, and a result the host cut at
its row limit is an error rather than rows to compare.

## The defaults are Credible's request-log package

`--logs-model-path` and the four `--source-*` flags default to the published
Credible request-log package (`request_logs.malloy`: `get_context_calls`,
`agent_tool_calls`, `agent_replies`, `conversations`). Its `execute_query`
returns `columnar-v1` rows, which the fetch reads alongside Publisher's
`result` list. The column maps also accept the older names the access design
used. Another host passes its own flags; nothing host-specific is in
`log_transcript.py`.

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
