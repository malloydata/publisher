# Friction log: driving Publisher as an eval backend

Notes from using a local Publisher as the model server for an automated
evaluation loop — agents answering questions over a package through MCP, then
diagnosing and editing the model. That workload leans on Publisher differently
from interactive use: it is unattended, it runs for hours, and it needs to know
*which* model and *which* retriever answered, because the whole point is
attributing a wrong answer to a cause.

Kept as a running list so the fixes land where the code is. Append rather than
rewrite; note the date when something is fixed.

Last updated 2026-08-30.

## Bugs

**1. Startup announces semantic retrieval before validating the credential, so a
bad key presents as an index that never finishes.** Startup logs `Semantic
get_context enabled: model text-embedding-3-small at api.openai.com` purely from
the presence of `EMBEDDING_API_KEY` — it never calls the provider. Meanwhile
`evidence.index.semantic` reads `indexing` and the `generation` timestamp stays
frozen, which looks exactly like a slow or wedged index.

It is not. The credential is only exercised lazily, on the first `getContext`
call, and only then does the real error appear:

```
warn: [MCP Tool getContext] Embedding sync failed; semantic ranking cooling down
  {"error":"Embedding request to https://api.openai.com/v1/embeddings failed
   (401): authentication failed; check EMBEDDING_API_KEY"}
```

So the sequence a user sees is: a confident success line at startup, then an
unbounded `indexing` state, then — if they happen to be reading logs at the
moment of the first retrieval — one `warn` naming a 401 that has been true since
boot. Everything in between silently answers lexically.

This cost the better part of a debugging session and sent me looking at
`syncPackageEmbeddings`' chunking and per-facet inserts, which were never
involved. Two cheap fixes, either of which would have collapsed it: validate the
key at startup with a one-token embedding request and fail loudly, or make
`evidence.index.semantic` carry a `failed` state with the provider error rather
than resting in `indexing` for a condition that will never resolve. `indexing`
should mean "in progress", not "in progress, or permanently broken".

**2. `mcp_traces` eviction contradicts its own documented contract.**
`config.ts` describes the retention cap as *"Oldest unreferenced traces are
evicted … Traces linked from an eval run are exempt."* `evictIfNeeded` in
`packages/server/src/service/mcp_trace_store.ts` is a plain
`ORDER BY created_at ASC LIMIT ?` with no exemption clause. An eval run's traces
can therefore be deleted out from under the ledger that references them, and the
`traceId` links dangle with no error — the rows are simply gone next time
anyone looks. Either implement the exemption or correct the comment; right now
the comment is load-bearing for anyone deciding whether their run's evidence
will survive the night.

**3. The `retrieval` evidence key is not stable across builds.** One build
returned the readiness object under `evidence.retrieval`, another under
`evidence.index`. Consumers end up walking the whole payload looking for a
`semantic` key rather than reading a documented path, and a consumer that
guessed one path silently reported the wrong retriever.

**4. Entity shape varies within `get_context` responses.** `sources[].entities[]`
came back as plain strings in some responses and as objects with a `name` field
in others, so every consumer needs a `str`-or-`dict` branch. This one is cheap
to hit and expensive to notice: a consumer that assumed dicts scored 0% recall
against a response that was actually correct.

## Papercuts

**5. `--watch-env` is the answer to the snapshot trap, and it is nearly
invisible.** Without it, a package's `location` is a one-time *import*:
Publisher copies the package under `publisher_data/<env>/<pkg>/` and serves the
copy. Edit the source and nothing changes, while every log line says the load
succeeded. With `--watch-env <name>` (or `PUBLISHER_WATCH`) local-dir packages
are symlinked in place and edits are live. Nothing in the failure path points at
the flag, and it is documented only in `--help`. This is the single biggest
time sink in the list — it presents as "my fix didn't work" rather than as a
server behaviour, so the natural response is to go re-debug a correct fix.

**6. Only the first `--watch-env` auto-reloads.** Additional environments are
mounted in place but not watched. Warned about at startup only, so by the time
it matters the message has scrolled away.

**7. Two Publishers cannot share a `SERVER_ROOT`.** The second dies on a DuckDB
`Conflicting lock` at startup. Reasonable in itself, but the error does not
suggest the remedy (use a separate root), and running two instances is the
normal way to compare a modified model against a baseline.

**8. `PORT` is ignored; the variable is `PUBLISHER_PORT`.** Silent — the server
binds its default and nothing reports an unrecognised variable, so you get a
working server on the wrong port and a client that cannot reach it.

**9. Publisher dies when its launching shell exits** unless started with `exec`
or `nohup`. The failure mode is the problem: the next MCP request returns an
*empty body* rather than a connection error, so a dead server reads downstream
as an agent that said nothing. In an eval that is 35 uniformly terrible answers
rather than an infrastructure error.

**10. No REST endpoint for query execution.** `execute_query` has to go over
MCP; the REST attempt returns `Unknown API endpoint`. Fine for agents, awkward
for the scripts around them, which otherwise never need an MCP client.

**11. A notebook's `modelInfo` reports one compiled query cell** for a 14-cell
`.malloynb`. It looks like a compile failure and is not — all 14 execute fine.

**12. The MCP JSON payload is a JSON string inside the JSON envelope.** A
`tools/call` reply is SSE wrapping a JSON object whose `content[].resource.text`
is *itself* serialised JSON, so the inner keys arrive backslash-escaped
(`\"results\"`). Anything doing a cheap substring check on the response body —
a health probe, a log grep — has to match the outer envelope or double-escape.
Easy to work around once known, and invisible until a health check reports a
healthy server as dead.

## Capability gaps for eval work

**13. No way to wait for, or force, index readiness.** Answering lexically on a
cold start is the right default for interactive use. An eval run needs the
opposite: "block until the index is ready", or it silently measures a different
retriever than the one it is reporting on. Today the only signal is polling
`evidence.index.semantic`, and per finding 1 that can poll forever against a
condition that will never resolve.

The sharper version for eval work: **there is no way to assert which retriever
answered.** A run can complete, report retrieval metrics, and be entirely
lexical, with the only evidence a `warn` line in the server log. Anything
claiming to measure semantic retrieval has to check the mode per call and refuse
to publish otherwise — which is what `score_intents.py` now does, but every
consumer has to reinvent it.

**14. `execute_query` is not persisted to traces** — `logger.info` to console
only. The query an agent actually ran is therefore not recoverable from the
trace store, which is exactly the artifact needed to tell a retrieval failure
from a construction failure after the fact.

**15. Traces carry no session or conversation id**, only a per-call `traceId`,
so a multi-turn episode cannot be reassembled. Reading an inbound
`Mcp-Session-Id` header would be much cheaper than enabling the stateful
transport, and would be enough to group a run.

**16. Tracing is wired into one call site rather than at tool registration.**
It lives inside `packageScopedResource()`, so tools that do not route through
it — and some `getContext` paths — produce no trace at all. Registration-time
wrapping would make coverage total by construction rather than by remembering.

**17. Traces carry no duration, status, or success.** So the store can say a
call happened but not whether it worked or how long it took, which are the two
things any latency or error-rate question starts from.

**18. No readiness endpoint that reflects package load state.** `/health`
answers while a package is failing to load, so the only trustworthy health
probe for an eval driver is a real `getContext` call against a known package.
That works, but it costs a retrieval and makes "is the server up" and "is my
package loaded" the same question.
