# get_context: a stage pipeline and LLM-assisted retrieval design

**Status:** Draft design, 2026-10-02. The first part is implemented in this pull request (section 2.3, the stage
pipeline, and section 2.8, the embedding sync). The rest is the design that the follow-up pull requests build.

`get_context` is the MCP tool that finds the entities (sources, dimensions, measures, views) a question needs.
A _stage_ is one step in its pipeline, such as "drop weak matches with an LLM". A _sync_ is the background step
that turns a package's entities into stored embedding vectors.

## 1. Summary

- **The goal.** With an LLM and an embedding provider configured, `get_context` ranks by meaning and an LLM
  checks the candidates. With no keys it is plain lexical search with no LLM steps. A small set of settings lets
  you tune the algorithm locally.
- **This pull request** reshapes `get_context` into a short pipeline (resolve, retrieve, rank stages, card
  stages, shape) with no stage registered and no change to lexical results, and makes the embedding sync
  sturdier: it starts when a package loads, saves each batch as it goes, retries transient failures with
  backoff, reports progress, and has a configurable cap. When an embedding provider is configured, a search
  during a first index returns "indexing in progress" with progress, not a lexical answer.
- **The design** (sections 2.3 to 2.7) covers the settings (two files, no per-request overrides), one provider
  layer for OpenAI, Anthropic, Google (Gemini and Vertex) and Ollama built direct with no SDK, and how the
  LLM steps map onto the pipeline.
- **Follow-ups** are listed in section 3.

## 2. Design

### 2.1 Baseline (`main` at commit 2a625702)

`get_context_tool.ts` is 2,839 lines. `runContextQuery` (about 620 lines) does everything inline: load the
package index, add a stale note, the listing branch, the semantic attempt, the lexical fallback, warnings,
windowing and shaping, and the response envelope.

What already works and must not be rebuilt:

- **Lexical search needs no keys.** lunr is always built. Semantic search runs only if `EMBEDDING_API_KEY` is
  set. Without it the response simply has no `retrieval` field.
- **Embeddings are incremental.** A facet row is keyed by a hash of its prepared text; a changed hash, a
  changed model or a missing row triggers a re-embed, and rows absent from the desired set are deleted.
  Vectors persist in `publisher.db` across restart and reload. One changed entity costs one small call.
- **Readiness and fallback reasons** (`indexing`, `cooldown`, `too-many-entities`, `provider-error`,
  `unavailable`) and a per-package 60-second cooldown after a provider failure.
- **Status** is on the REST package resource (`embeddingIndex`).

What is missing for fast local iteration:

- The first sync makes `ceil(rows/512)` calls one after another, with no concurrency and no retry, and keeps
  nothing until the whole batch returns. One failed chunk loses everything from that sync.
- No progress counter inside a sync.
- Above 5,000 entities the package stays lexical forever. Nothing is partially embedded.
- Computing the content fingerprint takes more than 500 ms on the event loop for 5,000 doc-heavy entities. It is computed once per package load (the result is cached per entity array, `embedding_index.ts:614-637`), so the cost is a one-time pause, not a per-request cost.
- The sync starts only on the first ranking call, not on package load.

### 2.2 Why a pipeline

Before this change, `runContextQuery` was one function of about 620 lines that did everything inline: load the
index, the listing branch, the semantic attempt, the lexical fallback, warnings, windowing, shaping and the
response envelope. Adding any LLM step to it means adding another inline block with its own timing, status and
failure handling. A pipeline with a fixed order of phases means each step is a separate file, and the
default path (no stages) stays small and easy to pin with tests.

### 2.3 Target structure

```
resolve request
  -> QueryStage*        (before retrieval; may rewrite the search texts. Rephrase goes here.)
  -> load package index
  -> listing branch  |  Retriever: semantic, falling back to lexical
  -> RankStage*         (after retrieval: refine/prune, rerank, value attach)
  -> window -> shape -> envelope

index time (separate, in the embedding sync)
  -> IndexStage*        (keyphrase generation, source summary)
```

- A stage is `{ name, enabled(ctx), run(...) }`. The runner in this pull request is minimal (if enabled, run). Later steps extend it so that one loop does the timing, the status and the trace for
  every stage, so adding a stage means adding one file.
- PR 1 registers zero stages. It moves code and changes no behaviour.
- The retriever order stays as today: semantic if configured and ready, else lexical, with the same
  fallback reasons.

### 2.4 Where settings live

Two files, two owners. No per-request overrides. The only environment variables we add are the two API keys. The existing `EMBEDDING_*` variables keep working as a fallback; when a file key and a variable are both set, the file wins.

- **Server operator: `publisher.config.json`, `retrieval` block.** Credentials, what may leave the machine, and
  ceilings. The operator decides what the server is allowed to do.
- **Package author: `publisher.json`, `retrieval` block.** How this package is searched and indexed. Publisher
  serves the package exactly as configured. Different packages can differ (a small package with short docs and
  a large one need different settings), and the settings travel with the package in git.

A package cannot enable what the operator has not allowed. If a package turns on `refine` and the server has no
LLM configured, the stage is skipped and the response and the status say so. A package can never make the
server send text to an endpoint the operator did not configure. That is why egress and credentials stay in the
operator's file: otherwise a package published to someone else's server could switch on sending its docs to an
LLM.

**Why not environment variables.** There are too many of them to track, they cannot differ per package, and
they do not travel with the package. The existing `EMBEDDING_*` variables keep working unchanged, so nothing
breaks. New settings go in the files. API keys are environment variables and nothing else: `EMBEDDING_API_KEY`
(already used on `main`) and `LLM_API_KEY`. No key is ever written in a config file, and there is no file
entry that points at a variable.

**Why not per-request overrides.** The settings a package is served with should be the settings that are
measured. A tuning run therefore edits a package's `publisher.json` and reloads the package. A reload keeps the
stored embeddings (the sync is keyed on content), so a change to a query-time setting costs one model
recompile, not a re-embed; a change to an index-time setting re-embeds only what it affects. The one
request-level control that stays is the trace header, because it only adds diagnostic output and changes no
result.

**Package settings (`publisher.json`):**

| Key                                                                       | Values                                                                                                     | Default                   |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------- |
| `retrieval.representation`                                                | `single`, `facets`                                                                                         | `single`                  |
| `retrieval.keyphrases`                                                    | `auto`, `never`, `always`                                                                                  | `auto`                    |
| `retrieval.sourceSummary`                                                 | on/off                                                                                                     | off                       |
| `retrieval.refine`                                                        | `enabled`, `minLevel`                                                                                      | off, `MEDIUM`             |
| `retrieval.rerank`                                                        | `enabled`, `topSources`                                                                                    | off, 8                    |
| `retrieval.rephrase`                                                      | `enabled`                                                                                                  | off (stage not built yet) |
| `retrieval.minSimilarity`, `perTargetLimit`, `maxEntitiesPerSourceTarget` | numbers                                                                                                    | today's behaviour         |
| `retrieval.prompts`                                                       | `refine`, `rerank`, `rephrase`, `keyphrase`, `summary`: a file path inside the package (no inline strings) | built-in prompts          |
| `retrieval.values`                                                        | `mode`, `include`, `exclude`                                                                               | off (feature deferred)    |

**Server settings (`publisher.config.json`):**

| Key                   | Meaning                                                                                                                                                                                             |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `retrieval.llm`       | `baseUrl`, `model` (one model for every stage for now; per-stage models are added when a measurement asks for them), `timeoutMs`, `concurrency`. The key is the environment variable `LLM_API_KEY`. |
| `retrieval.embedding` | `queryPrefix`, `documentPrefix` (tied to the embedding model, which is process-wide)                                                                                                                |
| `retrieval.egress`    | `preset`                                                                                                                                                                                            |
| `retrieval.indexing`  | `maxEntities` (the cap, replacing the 5,000 constant), `deadlineMs`                                                                                                                                 |

That is about 35 keys in all once the provider keys in 2.6 and the ceilings in 2.4.4 are counted, down from 103. Everything else is a constant in code: retry numbers, batch sizes,
thresholds, scoring, temperature (0), JSON mode, breaker and cache tuning, `maxChars`, and hybrid
ranking (cut).

#### 2.4.1 Representation and keyphrases are one question

Both answer "what text stands for this entity in the search?". They are two separate choices.

- **Keyphrase generation** decides whether an LLM writes an extra short search-oriented phrase for the entity.
  It creates text.
- **Representation** decides how the entity's texts become vectors. `single`: one vector per entity, built from
  the keyphrase if there is one, else the doc, else the name.
  `facets`: separate vectors for the name, each doc chunk (up to eight), the keyphrase and the summary; the
  best match counts.

So with `single`, a keyphrase replaces the doc as the thing embedded. With `facets`, it is one more vector.
Without an LLM (`keyphrases: never`), `single` embeds the doc, or the name when there is no doc.

Defaults: `single`, because most strings are short today. `facets` stays available. The
test is a 2 by 2: {`single`, `facets`} x {`keyphrases` never, auto}. `single` also gives roughly one
vector per entity, which is the biggest lever on index time for a model with tens of thousands of entities.

**The keyphrase rule.** A description of 8 words or fewer (12 for views) is used as the keyphrase unchanged,
with no LLM call. A description that is empty or longer than that gets an LLM-written keyphrase, so
long docs are condensed to a short phrase. The embedded vector is the keyphrase only, never the name. The
setting `retrieval.keyphrases` is `auto` (the default: apply the rule if an LLM is configured, do nothing
if not), `never`, or `always`. `auto` costs nothing without a key.

#### 2.4.2 Embedding prefixes

Some models need a label before the text: `nomic-embed-text` expects `search_query: ` before a search and
`search_document: ` before indexed text, and quality drops without them. OpenAI's model does not use them, and
adding them hurt in the earlier validation. They belong to the embedding model, which is process-wide, so they
are server settings. Changing the document prefix re-embeds.

#### 2.4.3 Response size

`response.maxChars` is a constant. `maxEntitiesPerSourceTarget` bounds response size and stays as
package settings.

#### 2.4.4 Operator ceilings, egress and what a package may do

A package author controls the docs that reach the LLM and now also the settings. "A package cannot enable what
the operator has not allowed" needs a mechanism, not a promise. It is enforced in the LLM client, so no stage
can bypass it.

- **Egress.** `retrieval.egress.preset` is `default` (entity names, `#(doc)` text and schema context may be sent)
  or `full` (also code and dimension values). Access predicates (`#(access_filter)`, `#(authorize)`) have no
  class and are never sent. Operator only.
- **Spend ceilings.** `retrieval.llm.maxCallsPerSync` and `maxCallsPerRequest`, set by the operator. A package can
  turn a stage on, or set `keyphrases: always`, but cannot spend past a ceiling.
- **Prompts.** A package may point a stage at a prompt file inside the package. A package author already controls the
  text the LLM sees; the ceilings bound what that can cost. Model docs are fenced in the prompt and marked as
  data. The LLM's `match_reason` text is not passed to the calling agent (to confirm when the LLM stages land).
- **Vector-affecting settings are not per package.** The embedding model, its dimensions and its prefixes are
  per server, because one process-wide provider embeds for every package. `representation` and `keyphrases`
  are per package. So "settings travel with the package" is true for how a package is searched, not for the
  keys that decide the vectors; the same package on a server with another embedding model re-embeds.
- **Vectors are not portable.** Vectors from one embedding model are not comparable with another's, so a
  change of provider, model or dimensions re-embeds every package.

### 2.5 Prompts

Today the refine, rerank, summary and value-refine prompts are code in `prompts/*.ts`, with only a
`promptVersion` string. The `template` keys (`"{name}: {keyphrase}"`, `"{value}"`) are text templates for what
is embedded, not prompts. There is no rephrase stage.

Plan: each stage has a built-in prompt in code. A package overrides it in `retrieval.prompts` with a file next
to its models (no inline strings; the path must stay inside the package). Prompts therefore live with the package, version with it in git, and take
effect on reload. The hash of the prompt text goes into the cache key: editing a refine prompt invalidates
exactly the cached refine answers, and editing the keyphrase prompt regenerates keyphrases and re-embeds only
the entities affected. This is how "recompute only what changed" holds when the configuration changes.

### 2.6 Providers: one layer for LLMs and embeddings

P0 providers: **OpenAI, Anthropic, Google (Gemini API and Vertex AI), and Ollama**. Others can follow.

The audit found the PR hand-rolled an OpenAI-compatible HTTP client, JSON repair, retries, a breaker and a
cache. OpenAI and Ollama both speak the OpenAI-style API, so one client covers them. Anthropic has its own
message format and structured-output mechanism. Vertex needs Google credentials (Application Default
Credentials, not an API key) and its own request shape. Writing and maintaining those adapters ourselves is
the reinvention to avoid.

Capabilities by provider:

| Provider                                        | Chat (LLM stages) | Embeddings                           |
| ----------------------------------------------- | ----------------- | ------------------------------------ |
| OpenAI                                          | yes               | yes                                  |
| Anthropic                                       | yes               | no (Anthropic has no embeddings API) |
| Google Gemini API / Vertex AI                   | yes               | yes                                  |
| Ollama                                          | yes               | yes (for example `nomic-embed-text`) |
| any OpenAI-compatible server (vLLM, Azure, ...) | yes               | yes                                  |

**One layer, two capabilities.** A `providers/` module defines each provider once (its connection and
credentials) and exposes two things: a chat model per stage and an embedding model. The LLM stages and the
embedding index both ask the registry; neither knows which vendor answers. A fake provider for tests lives
beside the real ones. The existing `EmbeddingProvider` class and its `fetch` test seam stay: the
OpenAI-compatible adapter delegates to it, so the embedding path on `main` does not change behaviour and
its tests keep working.

**Config.** Server file, `retrieval.llm`: `provider`, `model`, `baseUrl` (for Ollama and
compatible servers), `timeoutMs`, `concurrency`. `retrieval.embedding`: `provider`, `model`, `dimensions`,
`queryPrefix`, `documentPrefix`. The existing `EMBEDDING_*` variables keep working and map to the
`openai-compatible` provider. Keys are two environment variables, `LLM_API_KEY` and `EMBEDDING_API_KEY`,
separate because the LLM and the embedding model are often from different vendors (Anthropic plus OpenAI is a
common pair) and have separate budgets. Vertex uses Google credentials. Ollama needs no key. The rule on
`main` of ignoring ambient `OPENAI_API_KEY`-style variables stays, so nothing leaks by accident.

**Embedding model changes re-embed.** The index is keyed on model and dimensions, as on `main`. A different
provider's vectors are not comparable with the stored ones.

**Implementation: direct to the providers, no SDK and no unifying library.** Publisher is
MIT-licensed and `main` already calls embeddings with plain `fetch`. The surface we need is two operations: a
chat call that returns small JSON, and a batch embedding call. A unifying layer earns its cost when the
provider count grows; ours is four. Vertex authentication is the usual hard part, and the server already
depends on `google-auth-library` directly. Stages see only our interface, so a library such as Token.js could
replace the adapters later without touching a stage.

- **Interface.** Chat: system text, prompt, optional result schema and abort signal; returns text and usage.
  Embed: a batch of texts; returns vectors.
- **Three adapter families.** OpenAI-compatible (OpenAI, Ollama, vLLM; embeddings delegate to the existing
  `EmbeddingProvider`). Anthropic (Messages API; chat only). Google (Gemini API with a key; Vertex with
  `google-auth-library` credentials; chat and embeddings).
- **Shared code.** Retry with backoff, per-call timeout, JSON from text with schema validation and one repair
  retry (works for any model, including small Ollama ones; a vendor's native JSON mode is used only as an
  optimisation), usage normalisation, a per-package cooldown (the pattern in `embedding_index.ts`), the
  stored index-time LLM output keyed by prompt hash (no query-time cache: its hits would hide latency and nondeterminism when a request repeats), and OpenTelemetry metrics in the repo's `*_metrics.ts` style.
- **Tests.** Every adapter is tested with a `fetch` stub and recorded request and response fixtures, the same
  seam `main` uses for embeddings. No real calls in CI. Optional live smoke tests sit behind an environment flag.
- **Size.** About 700 to 900 lines plus tests (an estimate, not measured).

Considered and not chosen: the Vercel AI SDK (Apache-2.0; its `ai` package depends on `@ai-sdk/gateway`, the
client for Vercel's AI Gateway, and plain model-id strings route through that gateway by default; a spike was
started and stopped when we chose to go direct), Token.js (MIT), LangChain.js (MIT, heavy), Genkit
(Apache-2.0), LiteLLM (Python). None was evaluated in depth.

### 2.7 What changes in retrieval

| Behaviour            | Publisher `main` before                                           | Now                                                                  |
| -------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------- |
| Index text           | several vectors per entity (name and doc chunks)                  | `single` by default (one vector per entity); `facets` is the option  |
| Candidate window     | global window                                                     | per source, 10 rows per target                                       |
| Entities searched    | joined copies are indexed, embedded and ranked as separate rows   | direct entities only; joined copies are made at assembly             |
| Join damping         | none                                                              | `0.9 ** (hops + 1)` on the whole score, so one hop is 0.81           |
| Refine               | absent                                                            | an LLM rates LOW, MEDIUM or HIGH (2.10)                              |
| Score                | cosine only                                                       | level (LOW 1, MEDIUM 2, HIGH 3) plus cosine, through the knots (2.10) |
| Source rerank        | absent                                                            | an LLM scores whole sources 0 to 3 and drops below 2 (2.10)          |
| Source-target search | embedding or lexical path                                         | a separate LLM match over all sources (2.11)                         |
| Source summaries     | absent                                                            | an LLM writes a summary and a one-line summary per source (2.12)     |
| Join topology depth  | 2                                                                 | 10, at assembly only                                                 |
| Response budget      | none                                                              | 35,000 characters with a 1,000 reserve; whole cards only             |
| Failure policy       | falls back to lexical                                             | an indexing or error result when embeddings are configured (2.8)     |
| Lexical              | the only fallback                                                 | the mode when no embedding provider is configured                    |

The join topology is read from the compiled model: each join's alias from the model's join tree, and its target
source (and the file that defines it) from the join entry's `sourceID`. A join with no named target (an inline
table) reaches nothing at assembly, and its fields stay in the index so they can still be found. The lexical
path still ranks the index's own joined copies, to depth 2.

Moving the candidate window, scoring and join handling changes what `main` returns when embeddings are
configured. The no-key path (lexical) does not change and keeps its byte-identical test.

### 2.8 Embedding sync design

The goal is simple and predictable, not clever.

- **Trigger.** The sync starts in the background when a package loads or reloads, after it compiles. At most one
  sync runs per package. A search never starts one.
- **One sync at a time per server.** The embedding provider and its rate limit are process-wide, so syncs queue
  across packages. A restart replays the fingerprint and database scan for every package, spread out by the queue.
- **When "load" happens.** Packages load at server boot: `EnvironmentStore.initialize` calls `listPackages()`, which
  calls `getPackage` for every package and runs `Package.create` before the server is marked ready
  (`environment_store.ts:705, 812`; `environment.ts:1644-1662, 2120`). Later loads (add, install, reload) are lazy.
  The hook is an `onPackageLoaded` callback at the three places a package enters the map
  (`environment.ts:2137, 2229, 2428`), passed in by `EnvironmentStore`. It only enqueues work on the serial sync
  queue, because a restart fires it for every package at once.
- **States.** `lexical` (no embedding provider configured: a mode by design), `indexing` (embedded X of Y),
  `ready`, `error` (reason and retry time). One shape on the REST package resource.
- **While indexing.** If an embedding provider is configured, the answer should use it. A
  search during a first index returns an "indexing in progress, N of M" response with no results, not a
  lexical answer. On `error` the response names the reason. Lexical is the mode only when no embedding
  provider is configured. `main` today answers lexically during indexing, with `retrieval: "lexical"` and a
  `retrieval_reason` label (`get_context_tool.ts:293-303`); this changes that. Reason: a configured embedding model that quietly answers some searches lexically makes results hard to
  trust, and makes any measurement of retrieval quality unreliable. Cost: after a package's first index or a model change, the tool returns no results for the minutes
  the index takes. A republish that changes a few entities re-embeds in seconds.
- **Serial, not concurrent.** Batches run one after another. Each adapter declares its own maximum batch size,
  so endpoints with lower limits simply take more calls. To verify in the adapter work: Gemini's batch
  endpoint takes about 100 inputs and Vertex about 250; `main` uses 512 per call.
- **Save as you go.** Each batch is written in one transaction (multi-row insert) when it returns, so a failure
  keeps everything before it. A restart resumes by content hash.
- **Retry.** Exponential backoff with jitter on 429, 5xx and timeouts, honouring `Retry-After`, with a fixed
  maximum number of attempts. Auth and bad-request errors fail at once. After the attempts are used up the
  state is `error`, and the existing per-package cooldown decides when the next try starts.
- **Cap.** A hard cap, counted in vectors (rows), not entities, because `facets` multiplies them. Configurable
  in `publisher.config.json`. The value comes from measurement: query latency against synthetic vectors at
  10k, 50k and 100k rows (free), and first-sync time with one real key. Proposed targets: a first full index in
  about 10 minutes or less, and p95 query under about 300 ms. Placeholder guess 20,000 to 30,000.
- **Fingerprint.** Already computed once per package load and cached on `main`. This pull request only chunks that one-time
  computation so it yields to other requests.
- **Tie order.** Equal semantic scores are ordered by name, not left to DuckDB.

Consequence: this pull request is not a pure refactor plus faster indexing. The behaviour change (indexing and
error states instead of a lexical fallback) goes in its own commit, with only the affected golden entries updated.

## 3. Pull request sequence

1. **This pull request.** The pipeline, the sync, the configurable cap, and the indexing and error results.
   A 40-entry payload test was added first, on unmodified code, and the refactor commits do not change it. Only
   the seven fallback entries change, in one commit that says so.
2. **Direct-field retrieval, no LLM.** Search direct entities only and expand joins at assembly with the
   distance discount, the per-source candidate window, join depth, and the response budget.
3. **Providers and configuration.** The provider layer and the `retrieval` blocks in both config files, with
   no stages.
4. **LLM stages.** Keyphrases, refine, rerank and source-target search, each on when an LLM is configured, with
   prompts as files in the package.

Each step is reviewable alone, and with no keys configured the result is identical to the step before.

## 4. Risks

- The refactor touches the default path of a tool agents use. The payload test is the guard.
- Servers with an embedding provider configured see changed behaviour (indexing and error results instead of a
  lexical answer).
- The provider adapters are tested with `fetch` stubs only; each real provider can still surprise us.

## 5. Not verified

- The direct provider adapters against each real provider (not built yet).
- Per-provider batch limits (Gemini about 100 inputs, Vertex about 250).
- Embedding time for a package with tens of thousands of entities.

## Prior art

- Unifying client layers considered and not chosen (not evaluated in depth): Vercel AI SDK, Token.js,
  LangChain.js, Genkit, LiteLLM.
- The OpenAI Node SDK with a configurable base URL, usable against Ollama and vLLM.
