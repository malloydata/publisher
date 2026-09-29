<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# What each retrieval setting does: a validation run

This guide describes the `get_context` retrieval settings from running each one on its own and
watching what changed. Its job is to give you a feel for the knobs before the eval-driven tuning
starts. For how the pieces fit together, see [../retrieval-architecture.md](../retrieval-architecture.md).
For the reference list of settings, see [../configuration.md](../configuration.md).

## The short version

**Every setting now does what it is meant to do.** 42 pass/fail checks on the settings that had not
been seen acting live all pass, and the main sweep, indexing, values, privacy and failure scenarios all ran against
real models. Getting there found and fixed **eight defects** and removed **four settings that did
nothing** (see "What the run found").

**What was tested.** OpenAI `text-embedding-3-small` for embeddings and `gpt-4o-mini` for the LLM, on
the ecommerce sample (400 entities, 11 sources), the whole run for well under a dollar. Failure and
timing scenarios used a stand-in that answers the same endpoints and can be made to fail or be slow,
forwarding to the real models when it is not failing them. **One model pair is one data point.** A
larger embedding model, a local Ollama model or a smarter LLM will move these numbers. Use them as
directions and rough sizes.

**The five things worth knowing before you tune:**

1. **Refine is the biggest precision lever, and it is the LLM omitting candidates that does the work.**
   Refine took the default response from 50 entities and 35,700 characters to about 10 entities and
   10,600 characters, with all six known answers still found. Turning off `dropOmitted` puts the
   response back to 50 entities.
2. **`hybrid.mode: rerank-only` fixes ordering for free.** The known answer's average position went
   from 14.8 to 2.3, with no LLM and no change in size.
3. **The default similarity floor (0.2) is too loose for this embedding model.** A question the package
   does not model ("weather forecast temperature") still returned 6 entities. At 0.3 it returned
   nothing and no answer was lost.
4. **`response.gapCut` at 0.8 cuts the response by about 70% and still finds all six answers.** It
   needs no LLM.
5. **Do not set embedding prefixes for a model that does not want them.** Setting both prefixes on
   the OpenAI model made the unrelated question return 35 entities instead of 6.

## What the run found

Each of these showed up as a result that did not match the design. All are fixed and covered by tests.

1. **Ties were ordered at random.** The same request, asked three times, returned its cards in different
   orders. The same field name in several sources scores identically and nothing broke the tie. Ties now
   break on source, then kind. This would have added noise to every eval run.
2. **The indexing deadline did not apply to queued calls.** Every job checked the budget before waiting for
   one of the concurrency slots and never again, so with a 1.5 second deadline all 29 calls still ran. The
   check now happens after the wait. The same flaw weakened the per-request time budget at query time.
3. **A timeout caused by the deadline counted against the LLM.** A call cut short because the budget had
   50 ms left was recorded as a failure and pushed the circuit breaker toward opening. It is now reported
   as the budget running out and retried next time.
4. **`maxItemsPerPackage` only limited dimension values.** Keyphrase and summary rows ignored it. It now
   covers all generated text.
5. **The full trace never recorded the LLM's level,** so a `refine.minLevel` sweep could not be replayed
   offline. Every candidate now records its level per target, and why it was dropped.
6. **`egress.names: false` did not stop enrichment,** so keyphrase prompts still contained field names.
   Enrichment now refuses to run and says why.
7. **`refine.concurrency` was never read.** Capping it changed nothing (8.4 s against 8.1 s). It now
   limits how many refine batches are in flight, and a test measures it.
8. **The rerank prompt never got the generated source summary or the matched dimension values,** although
   the prompt has a place for both. Both are wired, each behind its own egress class.

**Removed because nothing used them:** `egress.sampleValues`, `egress.userPrompt`,
`dimensionalValues.sampleCount`, and `dimensionalValues.refine` (an LLM step for values that was planned
and not built).

Also corrected in the docs: a stage switched on with no LLM at all is a startup *warning*, not an error;
keyphrases are written for fields with **no doc or a long doc**, not a short one; `schemaContext` sends
the sibling fields of a source, not its description.

## Known gaps

1. **No progress report without an embedding provider.** The package resource has no `embeddingIndex`
   object when embeddings are off, so the lexical-only value index cannot be polled for "ready". Value
   search still works: the first question says "still being indexed" and a later one succeeds.
2. **The same entity is returned once per model file.** On this package the revenue question delivers 60
   rows for 35 distinct entities, because `order_items` is declared in three model files and each gets its
   own card. No setting controls this. It is a large share of response size.
3. **The base entity embedding sync has no wall-clock limit** of its own. Only the 5,000-entity cap bounds
   it, so a very slow local embedding model could take a long time. `indexing.deadlineMs` covers the LLM
   work and the value index, not this. Value indexing in `auto` mode also has no cap on the number of
   dimensions (one warehouse query each), and the source-summary prompt has no size cap.
4. **`dimensionalValues.queryTimeoutMs`** could not be triggered here: its minimum is 100 ms and local
   queries finish faster. It is covered by unit tests only.
5. **One embedding model and one LLM.** Everything here should be re-checked on the models you plan to run.

## How this was run

- A scratch copy of the ecommerce sample (`setup_package.py`) with four dimensions tagged `#(index)` and
  two extra gated sources (one `#(authorize)`, one `#(access_filter)`) to check they stay out of value
  search and provider traffic.
- `mock_provider.py` stands between Publisher and the model endpoints. It logs every request, can inject
  failures and delays, and, started with `--forward-base`, passes requests on to a real provider.
- `harness.py` starts a real Publisher build with a given `retrieval` block. Index-time settings
  (prefixes, keyphrases, values, limits) each got a fresh server. Query-time settings were swept on one
  warm server through the `X-Publisher-Retrieval` header, one setting changed per row.
- Seven fixed questions: six with a known right answer and one ("weather forecast temperature") that the
  package does not model and should return nothing for.
- Scripts: `scenario_core.py` (query-time sweep), `scenario_scoring.py`, `scenario_settings.py` (45 pass/fail
  checks), `scenario_enrich.py`, `scenario_values.py`, `scenario_privacy.py`, `scenario_failures.py`,
  `scenario_prefix.py`, run in order by `run_all.py`. `measure_index_cost.py` measures indexing time and
  memory.
- To repeat: `FORWARD_API_KEY=<key> python3 mock_provider.py --port 4977 --forward-base https://api.openai.com/v1`,
  build the server (`bun run build:server-only` in `packages/server`), run
  `python3 setup_package.py <ecommerce sample dir> /tmp/llm-val/pkg/ecommerce`, then
  `VAL_OUT=/tmp/llm-val/results-real python3 run_all.py`. Without `--forward-base` the stand-in answers by
  word overlap, which is useful for mechanics and misleading for tuning.

## Reading the numbers

Each row of the sweep is the average over the seven questions.

- **entities** and **chars**: how much comes back.
- **found**: how many of the six known answers appear at all.
- **rank**: the average position of the known answer, in distinct entities (lower is better).
- **LLM calls**: the cost of the row across the seven questions.

**Baseline (embeddings only, no LLM): 50.4 entities, 35,700 characters, 6 of 6 found, rank 14.8, no LLM
calls.** A single-target question returns about 35 entities and 23,000 characters; a three-target question
about 120 entities and 78,000. The settings below are how you bring that down.

## The settings, one at a time

### The similarity floor

**`embedding.minSimilarity`** (default 0.2) drops any match scoring below it. It is what lets an empty
result mean "the package does not model that".

| floor | entities | chars | found | rank | "weather" question returns |
|---|---|---|---|---|---|
| 0.1 | 54.6 | 38,300 | 6/6 | 14.8 | 35 entities |
| 0.2 (default) | 50.4 | 35,700 | 6/6 | 14.8 | 6 entities |
| 0.3 | 49.0 | 34,300 | 6/6 | 14.7 | none |
| 0.4 | 37.7 | 27,500 | 6/6 | 8.2 | none |
| 0.5 | 20.1 | 15,600 | 6/6 | 8.7 | none |
| 0.7 | 4.6 | 4,500 | 4/6 | 4.8 | none |

For this model, 0.3 is where an unrelated question first returns nothing, and 0.4 cuts a quarter of the
response and improves the answer's position with nothing lost. At 0.7 answers start disappearing. The
right floor belongs to the embedding model, not to Publisher. Check it by asking a question the package
does not model and reading `below_cutoff_count` against `total_entities`: it should be all of them.

**`embedding.facets`** chooses which parts of an entity are searched. Scoring only `doc` gave 6/6 at the
same rank as the baseline (14.5). Scoring only `name` lost an answer (5/6, rank 22.0). With this model
the doc text carries more of the meaning than the field name does. Use it to measure what each facet is
worth: turn one off and see what you lose.

**Prefixes** (`embedding.queryPrefix`, `documentPrefix`). Some models want `search_query: ` and
`search_document: ` around text. The OpenAI model does not, and adding them hurt:

| setup | "weather" question returns | exact match (revenue) top score |
|---|---|---|
| no prefixes | 6 entities | 0.73 |
| both prefixes | 35 entities | 0.74 |
| query prefix only | 9 entities | 0.56 |

Only set prefixes for a model documented to need them (for example `nomic-embed-text`). Changing
`documentPrefix` re-embeds the whole package on the next start (739 texts). Changing `queryPrefix`
re-embeds nothing. An earlier run on a model that *does* need them showed the opposite: without
prefixes everything scored high and the floor could no longer separate related from unrelated.

### Making the response smaller (no LLM)

**`response.gapCut`** drops entities scoring below a fraction of the best one for that target. It is the
strongest size lever and costs nothing.

| gapCut | entities | chars | found | rank |
|---|---|---|---|---|
| off | 50.4 | 35,700 | 6/6 | 14.8 |
| 0.6 | 32.9 | 24,800 | 6/6 | 6.5 |
| 0.8 | 10.7 | 10,400 | 6/6 | 2.3 |
| 0.95 | 5.3 | 6,200 | 6/6 | 2.3 |

0.3 changes nothing here. At 0.8 the response is 70% smaller and every answer is still found, and the
answer's average position is 2.3. It cuts by score, so it works best when the top hit is clearly better
than the rest. It can lose an answer when many things score close together, so check it on your own set.

**`response.maxEntitiesPerSourceTarget`** (default 10) caps how many entities one source contributes per
target. At 3: 21.6 entities, 17,800 characters, all six found. At 1: 9.4 entities, all six found, rank 2.7.
At 30: 73.7 entities and 49,400 characters. A cap is blunt: it cuts the tail of each card, good or bad.

**`candidates.perTargetLimit`** caps how many entities each target pulls in before grouping. It is the
earliest cut, so it also cuts LLM work. At 5: 7.1 entities, 7,800 characters, all six found. At 2 it lost
one. Low values are cheap and fast but leave the LLM stages nothing to correct a bad first ranking with.

**`response.maxChars`** trims to a character budget, lowest entities first, then whole cards. It
guarantees a ceiling but trims by position, not by relevance, and lost answers in every run (found 2 to 3
of 6 at 1,500 to 8,000 characters). Treat it as a safety net, not a tuning knob.

**`response.matchReason`** controls the one-line reason refine attaches to each match. Turning it off
cut a refine response from 10,600 to 9,200 characters (13%). If your agent does not read the reasons,
turn them off.

### Refine (LLM rates each candidate)

Refine asks the LLM to rate each candidate LOW, MEDIUM or HIGH against a phrase. Candidates below
`minLevel`, or left out of the reply, are dropped. The rest are ordered by level, with similarity breaking
ties.

**With gpt-4o-mini it is the strongest filter in the system**, and the filtering is mostly the model
leaving candidates out:

| setting | entities | chars | found | rank | LLM calls |
|---|---|---|---|---|---|
| off (baseline) | 50.4 | 35,700 | 6/6 | 14.8 | 0 |
| refine on (MEDIUM) | 9.7 | 10,600 | 6/6 | 3.0 | 31 |
| `minLevel=LOW` | 9.9 | 10,600 | 6/6 | 3.2 | 31 |
| `minLevel=HIGH` | 6.0 | 7,600 | 6/6 | 2.8 | 31 |
| `dropOmitted=false` | 50.4 | 36,900 | 6/6 | 14.8 | 31 |
| `maxPerSource=2` | 6.7 | 8,300 | 6/6 | 2.5 | 13 |
| `maxCandidates=5` | 4.4 | 6,500 | 6/6 | 2.2 | 10 |
| `batchSize=3` | 20.3 | 18,100 | 6/6 | 7.2 | 103 |
| `skipIfAtMost=100` | 29.1 | 23,300 | 6/6 | 7.2 | 17 |
| `matchReason=false` | 10.6 | 9,200 | 6/6 | 3.7 | 31 |

- **`dropOmitted`** is the setting doing the pruning. On (the default) a candidate the LLM leaves out is
  dropped. Off, it is kept at the bottom and the response goes straight back to the baseline size. Keep
  it on unless you distrust the model's omissions. An omitted candidate is kept *regardless of `minLevel`*
  when it is off.
- **`minLevel`**: LOW and MEDIUM are close here because the model omits rather than rates LOW. HIGH cuts
  another 40% of what is left.
- **`maxPerSource` and `maxCandidates`** decide how many candidates are *sent* to the LLM. Candidates past
  the cap are dropped, not passed through unrated. That is why they shrink the response and the call
  count together. The risk is a good entity ranked 11th by similarity never being shown to the LLM.
- **`batchSize`**: smaller batches cost far more calls (103 against 31) and, here, kept more entities
  (20 against 10) with a worse rank. Larger batches are cheaper. Only lower it if a small model loses
  track in long lists.
- **`skipIfAtMost`** skips refine when there are that few candidates *in total across all targets*. At
  100 only the three-target question was refined, which is why 17 calls still happened.
- **`unscoredLevel`** applies only to a candidate whose whole batch *failed*, not to one the LLM omitted.
  It had no effect with a working LLM.
- **`concurrency`** caps how many batches run at once below `llm.concurrency`. With every call held for
  400 ms, capping it to 1 took 62 s against 8 s.

Cost and time: about 1.8 seconds for a single-target question with refine on, and about 4 LLM calls per
question at the default batch size of 15.

### Rerank (LLM orders the top sources)

Rerank sends the top sources (each with its doc, generated summary if any, best entities and, with the
right egress class, matched values) and asks for a 0 to 3 score, then orders the cards. It costs **one
call per question**.

| setting | entities | chars | found | rank | LLM calls |
|---|---|---|---|---|---|
| off (baseline) | 50.4 | 35,700 | 6/6 | 14.8 | 0 |
| on (defaults) | 45.7 | 31,400 | 6/6 | 12.7 | 7 |
| `topSources=1` or `2` | 50.4 | 35,400 to 35,600 | 6/6 | 14.8 | 7 |
| `minScore=1` | 49.4 | 34,700 | 6/6 | 8.2 | 7 |
| `minScore=3` | 35.0 | 25,300 | 5/6 | 14.4 | 7 |
| `beyondTop=drop`, `topSources=2` | 15.6 | 8,500 | 3/6 | 10.0 | 7 |

Rerank helped less than refine or hybrid on this package, and it cost a call per question. At
`topSources` 1 or 2 the improvement vanished, because the right card was often not among the first two by
similarity. `minScore` and `beyondTop=drop` trade recall for size, and lost answers here. `maxEntityLines`
caps how many entities of a source the LLM sees, and `valuesPerEntity` how many matched values per
dimension (with `egress.dimensionalValues` on). Both were verified in the prompt.

**Refine and rerank together** gave 8.3 entities and 8,800 characters, but lost one of the six answers
(5/6), at 37 calls. More stages is not automatically better.

### Hybrid (words merged into meaning)

Hybrid ranks entities by embedding similarity and by lunr word match and fuses the two by rank.

| setting | entities | chars | found | rank | LLM calls |
|---|---|---|---|---|---|
| off (baseline) | 50.4 | 35,700 | 6/6 | 14.8 | 0 |
| `rerank-only` | 50.4 | 36,100 | 6/6 | 2.3 | 0 |
| `union` | 54.0 | 38,500 | 6/6 | 2.3 | 0 |
| `rerank-only`, `rrfK=1` | 50.7 | 35,800 | 6/6 | 2.2 | 0 |

`rerank-only` reorders what the embedding search already found: the known answer moved from an average of
14.8th to 2.3rd for free, and `below_cutoff_count` keeps its meaning. `union` also admits entities only the
word match found (they carry no `relevance` of their own) and here added 3.6 entities for no better order.
`rrfK` (60) sets how much the top of each list dominates and needs little tuning. **Try `rerank-only`
first.**

### Scoring

- **`scoring.joinDepthDamping`** (1 = off) lowered the score of a field reached through a join
  (0.9614 to 0.9307 at 0.5) and left a field of the source itself alone (0.9733 both). It acts only on
  refined scores.
- **`scoring.knots`** change the published `relevance` numbers (0.9614 to 0.9035 with a flat map) but not
  the order.
- **`scoring.sourceRelevance`** `coverage` changes source-card relevance (for example 0.9527 to 0.9353)
  when a source answers several targets.

These matter when an agent or a metric reads `relevance` values, not for order.

### Keyphrases and summaries (LLM at indexing time)

Keyphrases and summaries add search text without touching the authored docs. Cost on a 400-entity
package: 137 items to write (11 summaries and 126 keyphrases) in **29 LLM calls**, about 140 extra
embedding rows, and 26 seconds against 9 for embeddings alone (about a second per call, four at a time).

- **Which fields get a keyphrase.** `keyphrase.mode` `when-sparse` (default) covers fields with no doc or
  a doc longer than `wordThreshold` words: 126 fields. `always` covers every entity: 378 fields, 42 calls,
  1,117 rows. `never` writes none. A lower `wordThreshold` covers more (3: 213 fields; 40: 76 fields). A
  short doc is treated as already its own best keyphrase. `viewWordThreshold` does the same for views
  (169 eligible at 0, 123 at 1,000).
- **`batchSize`** is fields per call: 2 took 66 calls for the same 126 fields.
- **`sourceSummary`** costs one call per source (11). Summaries help source-level questions and, now, the
  rerank prompt.
- **The model writes faithful keyphrases, not creative ones.** For a field with no doc, `created_at`, it
  wrote "User account creation timestamp." A question phrased "signup registration" scored that keyphrase
  alone at position 3, but with every facet scored it stayed at position 32, no better than without
  enrichment, because other entities' name and doc scores were higher. Keyphrases add recall for
  questions that rephrase what a field means; they do not invent synonyms.
- **`keyphrase.template`** shapes the embedded text (`{name} => {keyphrase}` was embedded as
  `sold at => Order line attachment timestamp.`). **`maxCodeChars`** caps the code in a prompt (longest 41
  characters at a cap of 40), only with `egress.code` on.
- **Generated text is invisible by default.** It appears only with `response.surfaceGenerated`, as
  `generated_description` on a field and `generated_summary` on a source card.
- **It survives a restart.** After a restart the cached text was reused with zero LLM calls and zero
  re-embedding.

Limits (all held in testing):

| setting | what happened |
|---|---|
| `indexing.maxLlmCallsPerSync=2` | 2 summaries written, 135 items deferred, package still answers; the next sync did 2 more; lifting the cap finished the remaining 133 |
| `indexing.deadlineMs=1500` with real, slower calls | the first calls ran out of budget, all 137 items deferred, package still answers |
| `indexing.maxItemsPerPackage=300` | no generated rows added (the entities' own 739 rows already exceed it); package still serves |
| LLM down while indexing | every item marked failed, package still answers; back after the retry delay once the LLM returned |

Summaries go first when a limit bites, then the emptiest docs. Finished work is never redone.

### Dimension values

Tag a dimension `#(index)` and a question naming one of its values finds the dimension.

- **What is indexed.** Six dimensions were tagged; four were indexed (133 values). The two on gated sources
  were left out: `valueIndex.dimensions` said 4, and no value hit came from a gated source, in any mode.
- **How a hit looks.** The dimension comes back with matching values in `values` and `values_indexed: true`.
  A value-only request carries no `retrieval` marker.
- **Meaning matches.** With real embeddings, `Denim` found `Jeans` at 0.76, ahead of anything else at 0.50.
  Words still matter: `Jeans` scored 1.0, a near spelling (`Jeanss`) 0.88, a prefix (`Jea`) 0.95, and a
  substring 0.9 (`Men` matched the brand `Ed Garments`).
- **The default value floor is too loose.** With no floor of its own, values use the embedding floor
  (0.2), and short words score above that against almost everything: `Nonexistent` returned ten brand names
  at 0.21 to 0.29, and `Organic` also returned `Female` and `Male` at 0.28. **Set
  `dimensionalValues.minSimilarity` to about 0.45 and sweep it.** It is overridable per request.
- **`mode`**: `annotated` (only tagged) or `auto` with `include` and `exclude` globs. `auto` with everything
  indexed 32 dimensions and 1,824 values. Gated sources stayed excluded in every mode.
- **Caps**: `maxValuesPerDimension=2` kept 8 values and flagged 3 dimensions `values_truncated`.
  `maxValuesPerPackage=5` stopped at 5 values and left dimensions for a later run (`partial`).
  `onOverflow=skip` drops a dimension that overflows instead of keeping its top values. `maxValueChars=5`
  drops longer values: `Organic` disappeared and `Jeans` stayed.
- **Words versus meaning.** `lexical` handles exact, prefix, substring and near-spelling matches and needs no
  embedding provider. Embeddings alone missed `Jeanss` and `Jea`. Words alone missed `Denim`. The default
  uses both.
- **`template`** shapes the embedded text (`Intimates (category in product)`). **`refreshMinutes`** was
  checked in the database: a restart inside the window did not re-read the values (same timestamp), and a
  restart after it did.
- **`maxHitsPerTarget`** limits how many dimensions one value can match.

### The LLM connection

- **A failing LLM never fails the call.** HTTP errors, malformed replies and timeouts leave the stage's
  input order, add a `retrieval_stages` entry (`failed:http`, `failed:malformed`, `failed:timeout`) and a
  warning. A malformed reply is retried once first.
- **What goes on the wire.** `llm.temperature`, `seed`, `extraBody` and `jsonMode` were each seen in the
  request; `jsonMode: json_object` still gave a readable refine result. `llm.models.<stage>` sent each stage
  its own model, and a per-request override changed it. `embedding.extraBody` and `queryExtraBody` were seen
  on index and query requests respectively.
- **`llm.concurrency`**: with every call held for 400 ms, 1 slot took 58 s, 4 took 15 s and 8 took 8 s for
  the same 48 refine calls.
- **`maxAttempts` and `backoffMs`**: at 3 attempts with 300 ms backoff a failing call took 3 tries over
  1.3 s (300 then 600 ms).
- **The circuit breaker.** After three failures in a row it stops all LLM calls for `cooldownMs` (60 seconds
  by default). Calls are skipped (`skipped:cooldown`), not retried, and a recovered LLM is not tried again
  until the cooldown passes.
- **Time.** An LLM that took 5 seconds against a 1.5 second `timeoutMs` still answered the question in
  about 1.9 seconds with `failed:timeout`. Each call's timeout is capped by what is left of
  `requestBudgetMs` (20 seconds by default).
- **Calls.** `maxCallsPerRequest` (24 by default) bounds the cost of a wide question. At 4, a three-target
  question made 4 calls and left 44 of 48 refine batches unrated, with a warning saying so.
- **The cache.** The same question asked again made 0 LLM calls. It is keyed by the exact prompt.
  `llm.cache.maxEntries` matters: with 2 entries a repeat made 46 calls, with 2,000 it made none.
  **Turn the cache off (`llm.cache.enabled: false`) when you measure LLM cost or compare runs,** or a second
  run looks free.
- **Embeddings failing.** The answer falls back to word matching with `retrieval: "lexical"` and a
  `retrieval_reason` (`provider-error`, then `cooldown`). Word matching only answers what shares words with
  the question, so a paraphrase returns nothing while the provider is down.
- **`EMBEDDING_MIN_SIMILARITY`** in the environment is the floor when the config leaves
  `embedding.minSimilarity` null (7 entities at 0.55 against 35 at the default), and the config beats it.

### What is sent to a provider

- **Access predicates never leave.** Across the default settings, `preset: full` with everything on, and
  three single-class settings, the gate text (`$ROLE`, `$COUNTRY`, `authorize`, `access_filter`) appeared in
  no request, over about 1.3 MB of captured traffic. (The word "analyst" appeared only because the
  restricted source's own `#(doc)` says "for the analyst role".)
- **Gated sources' names and docs are ordinary text** and are sent like any other source. Gates protect
  rows, not the schema.
- **What each class changes** in a keyphrase prompt: `code` adds the field's Malloy source;
  `schemaContext` adds the sibling fields (about 3,900 bytes per prompt); `preset: full` adds both.
  `docs: false` sent no descriptions to refine. `names: false` stopped refine and rerank
  (`skipped:egress`) and now stops enrichment.
- **The rerank prompt** carries the generated source summary with the `docs` class, and matched dimension
  values only with `egress.dimensionalValues`.

### Changing settings per request

- With the gate on, an override changes only the query-time settings. A typo, a bad value, an index-time
  key, an egress key and a non-object each returned an error naming the field and how to fix it. None was
  silently ignored.
- With the gate off (`PUBLISHER_RETRIEVAL_OVERRIDES` unset), the header is ignored entirely.
- At start-up, a wrong key, a wrong value or an out-of-range number stops the server with a message naming
  the setting and a suggested fix (including "Did you mean ..." for typos).
- `trace.defaultLevel: summary` adds a `retrieval_trace` with no header; by default there is none.

## Which setting for which goal

| you want | try, in this order |
|---|---|
| A shorter response, same answers | `response.gapCut` 0.8; then `embedding.minSimilarity` 0.3 to 0.4 |
| A shorter response with the LLM | `refine` on (keep `dropOmitted`), then `maxPerSource` 2 to 5; turn `matchReason` off |
| Better order, no LLM | `hybrid.mode: rerank-only` |
| Better order, one LLM call | `rerank` on, leave `topSources` at 8, and check it against `hybrid` first |
| Answer "not modelled here" reliably | raise `embedding.minSimilarity` until an unrelated question returns nothing; do not set prefixes the model does not need |
| Find fields with no or long docs | `enrichment` on; leave `keyphrase.mode` `when-sparse` |
| Find a field by a value it holds | tag `#(index)`; `dimensionalValues.mode: annotated`; set `dimensionalValues.minSimilarity` near 0.45 |
| Cut LLM cost | `candidates.perTargetLimit`, `refine.maxCandidates`, larger `refine.batchSize`, `skipIfAtMost` |
| Measure cleanly | `llm.cache.enabled: false`, one setting per arm, wait for `enrichment` and `valueIndex` to settle |

## What this means for the eval

- **Sweep one knob per arm from a single warm server**, using the override header. Nothing drifted between
  rows in these runs, and the order is now the same on every run.
- **Check the index has settled** (`embeddingIndex.enrichment` and `valueIndex`) before each index-time
  arm, and use a fresh server root when changing prefixes, enrichment or values.
- **Disable the LLM cache in any arm that reports cost.**
- **Start the sweeps where the effect was largest:** `minSimilarity`, `gapCut`, `hybrid.mode`,
  `refine.dropOmitted`, `refine.maxPerSource`, and `dimensionalValues.minSimilarity`.
- **Use the `full` trace to replay level thresholds offline.** Each candidate records its level per target,
  so `refine.minLevel` can be swept without new LLM calls.
- **Repeat the core sweep on the models you will run.** The two things most likely to move are how often
  the LLM omits or rates LOW (which decides `minLevel`, `dropOmitted` and `batchSize`) and where the
  similarity floor sits.
