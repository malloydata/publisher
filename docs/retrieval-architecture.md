<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# How get_context retrieval is built

This explains how `get_context` finds and orders results, and where each setting acts. For the list of
settings see [configuration.md](configuration.md#llm-assisted-retrieval-for-get_context). For what each
one does to results see [retrieval-validation/README.md](retrieval-validation/README.md).

## The one-paragraph version

Retrieval has two halves. **At index time**, in the background, Publisher builds searchable text for
every model entity: its name, its doc, and optionally an LLM-written keyphrase and source summary, plus
the distinct values of dimensions you tag. **At query time**, a question is embedded, matched against
that index, and the candidates pass through a fixed line of optional stages that each keep, drop or
reorder them: word-match fusion, value matching, LLM refine, LLM rerank, then size cuts. Every stage
is off by default, every LLM stage fails soft, and with nothing configured the response is byte-for-byte
what it was before.

## Picture

```
                        INDEX TIME  (background; a question never waits for it)

  model files ──► entities ──► name facet ─┐
                    │          doc facets ─┤
                    │                      ├─► embed ──► entity_embeddings   (DuckDB, in publisher.db)
                    ├─► enrichment.ts ─────┤
                    │    LLM keyphrase  → kw facet
                    │    LLM summary    → sum facet     cached in entity_enrichment
                    │
                    └─► dim_values.ts ── one group-by query per tagged dimension
                         (never a gated source)  ──► dimension_values  (+ embeddings, optional)

                        QUERY TIME  (get_context_tool.ts, in this order)

  question ─► embed the search texts
            ─► scan vectors: best facet per entity, per target, above the floor     [embedding.*, candidates.*]
            ─► fuse with lunr word match                                            [hybrid.*]
            ─► attach matching dimension values                                     [dimensionalValues.*]
            ─► refine: LLM rates LOW / MEDIUM / HIGH, drops the rest                [refine.*]
            ─► rerank: LLM orders the top sources                                   [rerank.*]
            ─► gap cut, per-source cap, page, character budget                      [response.*]
            ─► response  (+ retrieval_stages, retrieval_config, retrieval_trace when asked)

  Every LLM call goes through llm_runner.ts:  concurrency limit, retries, circuit breaker,
  result cache, per-request time and call budget.   Text sent to a provider passes egress.ts first.
```

## The pieces

### Configuration: `retrieval/retrieval_config.ts`, `retrieval/boot.ts`

One typed schema holds every setting with its default. It is read once at start-up from the
`retrieval` block of `publisher.config.json`. Anything unknown, of the wrong type or out of range stops
the server with a message that names the setting and suggests a fix. Secrets and endpoints (the LLM and
embedding URLs, keys, models) come only from environment variables and never from this file.

`boot.ts` checks the config against the environment: a stage switched on with no LLM at all is a logged
warning (so one config works with or without a key); an LLM with no model for an enabled stage is a
startup error.

A **fingerprint** (a short hash of every non-default value) is stamped on responses whose settings
differ from the defaults, so two runs can be told apart.

**Per-request overrides** (`retrieval/run.ts`): with `PUBLISHER_RETRIEVAL_OVERRIDES=1`, the
`X-Publisher-Retrieval` header can change the query-time settings for one call. An allow-list of
prefixes decides what is overridable. It cannot change egress, endpoints, keys or anything that affects
the index. An override is merged over the running config and validated by the same schema, so a bad one
is an error and never silently ignored.

### Providers: `service/embedding_provider.ts`, `service/llm_provider.ts`, `service/llm_runner.ts`

Both providers speak the OpenAI wire format, so OpenAI, Ollama, vLLM and LM Studio all work. A key is
optional when a base URL is set.

`llm_runner.ts` is where every LLM call is governed:
- **Concurrency**: at most `llm.concurrency` calls in flight; the rest queue.
- **Budget**: each request or index run has a deadline and a call count. The budget is taken after a
  call gets its slot, so queued calls cannot outlive the deadline. A timeout caused by a nearly spent
  budget is reported as the budget, not as a fault of the endpoint.
- **Retries** with backoff for errors that can succeed on a second try.
- **Circuit breaker**: after `breaker.failures` failures in a row, calls stop for `cooldownMs`.
- **Cache**: results are cached by a hash of the exact prompt, model and settings.

### Index time

**Representation.** By default (`embedding.representation: facets`) each entity is stored as several rows,
described next. With `single` it is stored as ONE row: the generated keyphrase if there is one, else its
doc, else its name. That is what Credible's hosted retrieval embeds, so `single` is the setting to compare
against it. Changing it re-embeds the package.

**Entity facets** (`mcp/tools/embedding_index.ts`). Each entity is stored as several rows in
`entity_embeddings`: `name`, one or more `doc:N` chunks, and, when enrichment has written them, `kw`
(keyphrase) and `sum:N` (summary). An entity scores as its best facet, so more text can add recall and
can never lower an entity's score. Rows are keyed by a hash of their text, so only changed text is
re-embedded, across restarts. A change to a prefix or extra body sent with the text changes the model
key and re-embeds everything.

**Enrichment** (`retrieval/enrichment.ts`). Plans which entities need a keyphrase (no doc, or a doc
longer than `wordThreshold` words) and which sources need a summary, asks the LLM for what is not already
cached, and installs the results as extra facets. The plan is ordered summaries first, then the emptiest
docs, and is cut by the limits: `indexing.maxItemsPerPackage` (rows), `maxLlmCallsPerSync` (calls) and
`deadlineMs` (time). What a limit cuts off is left for the next sync and is never redone once finished.
Generated text lives in the `entity_enrichment` table, so a restart costs no LLM calls, and it is
installed **before** the first sync so that sync keeps the rows instead of deleting them.

**Dimension values** (`retrieval/dim_values.ts`, `retrieval/index_annotation.ts`). Finds dimensions to
index (`#(index)` tags, or globs in `auto` mode), skips every source that has an access gate, and runs
one group-by query per dimension for its most frequent values, up to the caps. Values are stored in
`dimension_values` with their embeddings, if there is an embedding provider. They are matched by words
(exact, prefix, substring, near spelling) and by meaning.

**Egress** (`retrieval/egress.ts`). The single place text becomes provider-bound. It knows the data
classes (names, docs, schema context, code, dimension values) and builds prompts from only the classes
that are on. Access predicates (`#(access_filter)`, `#(authorize)`) are not a class: embedded text is
built from `#(doc)` lines only, and field code has every `#` annotation line stripped first.

### Query time: `mcp/tools/get_context_tool.ts` and `retrieval/`

1. **Candidates.** One SQL pass scores every entity against every search target, keeps its best facet,
   applies the similarity floor, and cuts a per-target window: the best rows of the whole package
   (`candidates.window: global`), or the best rows of each source (`per-source`, Credible's window, so a weak
   source still contributes). Ties break on name, source, then kind so
   the answer is the same every time. `below_cutoff_count` against `total_entities` is what lets an empty
   result mean "the package does not model that".
2. **Hybrid** (`retrieval/hybrid.ts`). Optionally ranks the same entities by lunr word match too and fuses
   the two lists by rank. In `rerank-only` mode it only reorders; in `union` mode it can add entities only
   the word match found.
3. **Values.** Value targets are matched against the value index and attached to the dimension that holds
   them. Optionally an LLM rates each matched value against its phrase and drops the omitted ones
   (`retrieval/stages/value_refine.ts`, Credible's value refine), before the values are attached.
4. **Refine** (`retrieval/stages/refine.ts`). Sends each target's candidates to the LLM in batches and gets
   back LOW, MEDIUM or HIGH plus a one-line reason for each. Candidates under `minLevel`, or left out of the
   reply, are dropped. The score becomes level plus similarity, so the level decides and similarity breaks
   ties.
5. **Rerank** (`retrieval/stages/rerank.ts`). One call over the top sources (their docs, generated summary,
   and best entities, with matched values if allowed) returning a 0 to 3 score per source.
6. **Size cuts.** Gap cut, per-source cap, paging, and a final character budget.

`retrieval/trace.ts` records how many entities entered and left each gate and why, plus LLM calls, tokens
and time; the `full` level also records every candidate's level so a threshold sweep can be replayed
without new LLM calls.

## Rules the design keeps

- **Off means unchanged.** With no retrieval settings, the response is identical to before this work. A
  golden test (`get_context_payload_pin.spec.ts`) pins it. New response keys appear only when the setting
  that produces them is on.
- **An LLM never fails a question.** A stage that errors, times out, returns something unreadable, or
  meets an open breaker keeps its input order and says so in `retrieval_stages` (`failed:<kind>` or
  `skipped:<why>`) and a warning.
- **The floor keeps its meaning.** Stages only reorder or prune candidates that already cleared the floor,
  so "everything fell below the floor" still means "not modelled here".
- **Generated text only adds.** It becomes extra facets; it never replaces authored docs and shows in a
  response only with `response.surfaceGenerated`.
- **Predicates never leave the machine,** and a source with any access gate is never value-indexed, because
  the value index is shared by every caller and a gate depends on the caller.
- **Bounded work.** Every background job has row, call and time limits, and stops cleanly at them.
- **The trace adds up.** For each gate, entities in minus entities out equals the sum of the drop reasons;
  anything unexplained is counted as `unattributed` so a gap is visible.

## Where things are stored

| table (in `publisher.db`) | holds |
|---|---|
| `entity_embeddings` | one vector per facet row: name, doc chunks, keyphrase, summary; the text embedded is kept beside it |
| `entity_enrichment` | LLM-written keyphrases and summaries, with a hash of their inputs and the model, so a change regenerates only what changed |
| `dimension_values` | the indexed values of tagged dimensions, their counts and (optionally) vectors |
| `dimension_value_state` | per dimension: values seen, kept, whether it was cut at a cap, and when it was read |

All four are caches. Deleting a package or environment removes its rows, and `--init` rebuilds them.

## Adding a setting or a stage

- **A setting:** add it to the type, the defaults and the schema in `retrieval_config.ts` (all three are
  needed or the server refuses to start or accepts a value nothing reads); read it where it acts; add a
  row to the configuration reference. If it is safe to change per request, add its prefix to the override
  allow-list.
- **A stage:** give it its own module under `retrieval/stages/` that takes rows and returns rows plus a
  status, and call it from `get_context_tool.ts` at the right point. It must keep its input on any
  failure, report a status, and add its gate to the trace.
- **Anything sent to a provider:** build it in `egress.ts` from a named data class, so the privacy test
  covers it.

## Known limits

- Value indexing has no progress report when there is no embedding provider (the package resource carries
  no `embeddingIndex` object then).
- The same entity is returned once per model file that declares its source.
- The base entity embedding sync has no wall-clock limit of its own; only the 5,000-entity cap bounds it.
