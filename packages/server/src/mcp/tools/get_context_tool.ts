// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../../service/environment_store";
import { embeddingConfigured } from "../../service/embedding_provider";
import { InvalidArgumentError } from "../../errors";
import { buildMalloyUri } from "../handler_utils";
import { jsonResource } from "../tool_response";
import { logger } from "../../logger";
import { assembleCards } from "./get_context_assembly";
import {
   PIPELINE_SETTINGS,
   runCardStages,
   runQueryStages,
   runRankStages,
   type CardState,
   type CardStage,
   type PipelineContext,
   type QueryStage,
   type RankStage,
   type RankedState,
   type Retriever,
} from "./get_context_pipeline";
import { activeLlmSettings } from "../../providers/active";
import { LlmMeter, StageError } from "./get_context_llm";
import { refineStage } from "./get_context_refine";
import { rerankStage } from "./get_context_rerank";
import {
   convergedContextShape,
   matchesScope,
   resolveRequest,
   type GetContextParams,
} from "./get_context_request";
import {
   budgetCutWarning,
   contextError,
   entityCutWarning,
   indexingResponse,
   listingPageWarning,
   makeWarningsFor,
   projectEntity,
   rerankCutWarning,
   semanticUnavailableError,
   shapeCards,
   skillsFor,
   sourceCutWarning,
   stageFailureError,
   toSourceResults,
   truncationWarning,
   unsupportedTargetWarnings,
} from "./get_context_response";
import {
   sourceMatchActive,
   sourceMatchStage,
} from "./get_context_source_match";
import { resolveLlmStages } from "./get_context_stage_settings";
import { lexicalRetriever, semanticRetriever } from "./get_context_retrievers";
import type { PackageIndex, ResolvedRequest } from "./get_context_types";
import { indexSettingsOf } from "./index_settings";
import { getPackageIndex, sourceContextKey } from "./package_entities";
import {
   currentSummaryHashes,
   loadSourceSummaries,
   type StoredSourceSummary,
} from "./source_summaries";

// server.ts and package.controller.ts import these from here.
export {
   getPackageEmbeddingStatus,
   startPackageEmbeddingSync,
} from "./package_embedding";

/**
 * The converged tool's description. Same budget and same ordering rule as the
 * one below it: contract rules first, reference last, because a tail-truncating
 * client drops whatever is last. See server.protocol.spec.ts.
 */
const GET_CONTEXT_DESCRIPTION = `Retrieve the entities in a Malloy package most relevant to a plain-English question.

## Contract rules
- Use the names it returns verbatim; never invent one that is not in the results.
- One call answers: describe the fields you need as search_targets; each matching source returns with those fields nested. No drill-down call.
- scopes is REQUIRED: exactly one, naming an environment and package. list_packages lists them.
- Read warnings and any error/stale field before trusting a number or calling data absent.
- A source's joins are all listed: empty means none. In an answer, never write a join; tell the user.
- Read a source's doc before querying: it carries grain and population rules its fields do not.
- accessFilter/authorize mean gated; a deny-all source never appears here.
- Empty sources: nothing cleared the floor, or the LLM steps pruned every candidate.

## Parameters
search_targets: one per concept, {target_type, search_text}; target_type is source|dimension|measure|view|join|dimensional_value, omitting search_text enumerates that type. scopes: {environment, package} + optional model_path, source, entity_name. limit caps sources (max 150). offset pages a listing. user_prompt: the question asked. include_code or a pinned entity_name returns code.

## Response
sources[], best first; a source repeats once per model_path resolving it, query any. source_info: resource_id (environment/package/model_path/source) -> execute_query's environmentName/packageName/modelPath/sourceName; docs (… = truncated), one_line_summary, complete joins, givens, accessFilter/authorize, filter_params. entities[] nest under it: name, entity_type, description, data_type, relationship (fan-out), join_path, aliases, relevance, entity_id, matched_targets[].match_reason (why the model kept it).
ranking, returned of total_available sources, next_offset on a listing, warnings[]. Over 35,000 characters, whole sources are dropped and warnings say so.
below_cutoff_count of total_entities rejected. retrieval "indexing" = still building, ask again soon. Errors carry retrieval_reason; "llm-stage-failed" names the failed step in retrieval_stage.`;

/**
 * Whether the package's most recent reload failed, as a warning to attach.
 */
async function staleNoteFor(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
): Promise<string | undefined> {
   // A stale package answers every tier below exactly like a current one:
   // the index is the last model that compiled, so the names are real and
   // the queries succeed, and the numbers are from before the last save.
   // Nothing else in this payload can say so, and telling the agent to go
   // call get_status is weaker than saying it here, where it is
   // already looking. Attached to tiers 3 and 4 alike, because tier 4 is
   // the path that goes straight from a question to field names to a
   // query.
   //
   // Best effort: this is a health annotation, so a lookup that fails
   // must not take discovery down with it. Logged, never thrown.
   let staleNote: string | undefined;
   try {
      const environment = await environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const stale = environment.getStaleCompileErrors().get(packageName);
      if (stale) {
         staleNote = `This package is STALE: its most recent reload failed to compile at ${stale.failedAt}, so these names, and any query you run against them, come from the model compiled BEFORE that save, not from the files on disk. Fix the model and call reload_package; get_status has the compile error.`;
      }
   } catch (error) {
      logger.debug("[MCP Tool getContext] staleness lookup failed", {
         environmentName,
         packageName,
         error: error instanceof Error ? error.message : String(error),
      });
   }
   return staleNote;
}

/** Tier 3: nothing to rank, so enumerate what the targets name. */
function runListing(
   ctx: PipelineContext,
   uri: string,
): ReturnType<typeof jsonResource> {
   const { request, pkgIndex } = ctx;
   const { environmentName, packageName, sourceName } = request;
   const { byId, sourceContext, droppedSources } = pkgIndex;
   const warningsFor = makeWarningsFor(ctx);
   // With sourceName set this is the drill-down, so it lists every
   // entity the named source offers: the source row itself, then its
   // views, dimensions, measures, and any named query built on it.
   // Filtering to kind === "source" here as well returned exactly one
   // row — the source — which the caller already had from the tier-3
   // listing, and never the fields the tool description promises.
   // collectEntities pushes a source ahead of its own fields, and
   // Array.from preserves that insertion order, so the source's doc
   // still leads the drill-down.
   //
   // Enumeration: return everything unless the caller sets an explicit
   // limit. slice(0, undefined) keeps the whole list, so discovery is
   // not silently capped the way ranked retrieval (tier 4) is.
   // A drill-down (sourceName, no query) lists every entity the
   // named source offers, so the card carries its views, dimensions,
   // measures, joins and named queries. Listing only the source row
   // there returned what the caller already had from the package
   // listing and never the fields the tool description promises.
   // collectEntities pushes a source ahead of its own fields, and
   // Array.from preserves that insertion order, so toSourceResults
   // sees the source before the entities that nest under it.
   //
   // Enumeration: return everything unless the caller sets an explicit
   // limit. slice(0, undefined) keeps the whole list, so discovery is
   // not silently capped the way ranked retrieval (tier 4) is.
   const inScope = Array.from(byId.values()).filter(
      (e) =>
         // A scoped source's own row always survives: it becomes the
         // card rather than competing for an entity slot.
         ((sourceName && e.kind === "source" && e.source === sourceName) ||
            request.kinds.has(e.kind)) &&
         matchesScope(e, request),
   );
   // The limit caps entities. A drill-down's source row becomes the
   // card rather than an entity, so it does not spend a slot; a
   // package listing has only source rows, so there the cap is on
   // cards, which is what the listing is made of.
   // A browse is the one listing with a stable order, so it is the one
   // that can be resumed. Skipping into any other shape would silently
   // drop rows whose position is not reproducible between two calls.
   const paged = request.pureSourceListing
      ? inScope.slice(request.offset)
      : inScope;
   let entityBudget = request.limit;
   const capped = paged.filter((e) => {
      if (sourceName && e.kind === "source") return true;
      if (entityBudget <= 0) return false;
      entityBudget -= 1;
      return true;
   });
   /**
    * Rows the cap actually spends from, which is the unit both sides of the
    * truncation warning have to be counted in.
    *
    * A scoped source's own row never spends a slot, so it is EXCLUDED from
    * both counts rather than subtracted from one. Subtracting assumed the
    * row was there: scope to a source that does not exist and `capped` is
    * empty, so `capped.length - 1` was -1, and the warning fired claiming
    * "Returned -1 of 0 matching entities" on a result that lost nothing.
    */
   const spendable = (rows: typeof inScope) =>
      sourceName ? rows.filter((e) => e.kind !== "source").length : rows.length;
   const cappable = spendable(inScope);
   // The overview is where an agent decides how to combine sources,
   // so each one states its relationships here rather than making
   // that a second call. toSourceResults carries the complete joins
   // list onto every entry, so empty means none declared.
   const sources = toSourceResults(
      capped.map((e) => projectEntity(e, environmentName, packageName)),
      sourceContext,
      environmentName,
      packageName,
      new Map(),
      request.includeCode,
      droppedSources,
   );
   // A listing is deterministic catalog order, not a ranking, and
   // Publisher has no query-usage signal to fill the hosted API's
   // `prominence` with, so it names the ordering and omits the
   // score rather than inventing a number nobody should rank on.
   // Both counts are in CARDS, the unit the payload is made of, so the
   // drill-down counts its source ROWS rather than the whole scope (which
   // also holds the entities nesting inside them; the entity cap is
   // reported separately). One row per model path resolving the source, so
   // a drill-down is N-of-N, not the 1-of-1 it was when a source could only
   // report one path.
   // next_offset is present only while sources remain past this page, and
   // only on the browse, matching where `offset` is honoured.
   const consumed = request.offset + sources.length;
   // Counted as distinct pairings rather than as source rows, so it stays
   // right if a drill-down ever reaches an entity whose own source row was
   // filtered out: toSourceResults would still raise a card for it.
   const cardsInScope = sourceName
      ? new Set(
           inScope.map((e) => sourceContextKey(e.modelPath, e.source ?? "")),
        ).size
      : inScope.length;
   const listingEnvelope = {
      ranking: "prominence" as const,
      total_available: cardsInScope,
      returned: sources.length,
      ...(request.pureSourceListing && consumed < inScope.length
         ? { next_offset: consumed }
         : {}),
   };
   // An empty enumeration is ambiguous to an agent: "no data here" and
   // "the package exposes nothing" look identical. The package DID
   // load (a failed load throws out of getPackageIndex above), so an
   // empty result means its models expose no sources: a curation gap
   // (index.malloy's export {}), not an empty database. Say so, only in the
   // empty case, so the populated payload stays byte-identical.
   // `kinds` is empty only when EVERY target named a type this server does
   // not index, and unsupportedTargetWarnings has already said so exactly.
   // Adding the curation line there contradicts it and misdiagnoses a
   // healthy package: a `dimensional_value` search against storefront
   // reported that its seven sources were a curation gap.
   if (sources.length === 0 && !sourceName && request.kinds.size > 0) {
      return jsonResource(uri, {
         sources,
         ...listingEnvelope,
         ...skillsFor(ctx),
         ...warningsFor(
            "This package loaded but exposes no sources. That is a curation gap, not an empty database: check what the package's index.malloy exports (its export { ... }), and call get_status for load errors and stale packages.",
         ),
      });
   }
   return jsonResource(uri, {
      sources,
      ...listingEnvelope,
      ...skillsFor(ctx),
      ...warningsFor(
         // A pure browse pages; every other listing shape is capped in
         // entities and says so.
         request.pureSourceListing
            ? listingPageWarning(sources.length, inScope.length)
            : truncationWarning(spendable(capped), cappable),
      ),
   });
}

// The stages and retrievers runContextQuery runs; a new stage is one file and
// one line here.
const QUERY_STAGES: QueryStage[] = [];
const RETRIEVERS: readonly Retriever[] = [semanticRetriever, lexicalRetriever];
// Source match is listed only for a request it applies to, so a trace shows
// no row for it otherwise (refine and rerank report "skipped" when off; this
// one has nothing to say unless it has work).
const rankStages = (ctx: PipelineContext): RankStage[] =>
   sourceMatchActive(ctx) ? [sourceMatchStage, refineStage] : [refineStage];
const CARD_STAGES: readonly CardStage[] = [rerankStage];

/**
 * Tier 3 (enumerate) and tier 4 (rank) over ONE package, driven by an already
 * resolved request, so a ranking decision and a request-parsing decision stay
 * separately reviewable. Retrieval runs over the package's model entities
 * (sources, views, dimension/measure fields, joins, named queries), semantic
 * when an embedding provider is configured and lexical (lunr/BM25) otherwise.
 * The entity index is built once per Package and cached (see getPackageIndex),
 * rebuilding automatically when the package reloads.
 *
 * There is no progressive-discovery tier here any more: the catalog is
 * `list_packages`, and every request that reaches this names one package,
 * because `scopes` requires it.
 */
async function runContextQuery(
   resolved: ResolvedRequest,
   environmentStore: EnvironmentStore,
   extraWarnings: string[] = [],
   traceSummary = false,
): Promise<ReturnType<typeof jsonResource>> {
   const { environmentName, packageName, sourceName } = resolved;
   logger.info("[MCP Tool getContext] Retrieving context", {
      environmentName,
      packageName,
      sourceName,
      searches: resolved.searches.length,
      listingOnly: resolved.listingOnly,
   });

   // Tiers 3 and 4 need the package's entity index.
   let pkgIndex: PackageIndex;
   try {
      pkgIndex = await getPackageIndex(
         environmentStore,
         environmentName,
         packageName,
      );
   } catch (error) {
      logger.warn("[MCP Tool getContext] index build failed", {
         environmentName,
         packageName,
         sourceName,
         error: error instanceof Error ? error.message : String(error),
      });
      return contextError(
         buildMalloyUri(
            { environment: environmentName, package: packageName },
            "get-context",
         ),
         `${environmentName}/${packageName}`,
         error,
      );
   }

   const uri = buildMalloyUri(
      { environment: environmentName, package: packageName },
      "get-context",
   );

   const staleNote = await staleNoteFor(
      environmentStore,
      environmentName,
      packageName,
   );
   const ctx: PipelineContext = {
      request: resolved,
      environmentStore,
      pkgIndex,
      warnings: [staleNote, ...extraWarnings].filter(
         (warning): warning is string => Boolean(warning),
      ),
      embeddingConfigured: embeddingConfigured(),
      settings: PIPELINE_SETTINGS,
      trace: [],
      meter: new LlmMeter(activeLlmSettings()?.maxCallsPerRequest ?? null),
   };
   const warningsFor = makeWarningsFor(ctx);
   // Spread into a ranked payload. Without the request header it is {}, so
   // the payload is the one every caller already gets. With
   // PUBLISHER_MCP_TRACE=retrieval the same summary is also written to the
   // server log, which no caller sees.
   const traceFields = () => {
      const logTrace = traceToLog();
      if (!traceSummary && !logTrace) return {};
      const retrieval_trace = {
         stages: (ctx.trace ?? []).map((t) => ({
            name: t.name,
            status: t.status,
            ms: t.ms,
            in: t.in,
            out: t.out,
            llm_calls: t.llmCalls,
            tokens: t.tokens,
         })),
      };
      if (logTrace) {
         logger.info("[MCP Tool getContext] Retrieval trace", {
            environmentName,
            packageName,
            retrieval_trace,
         });
      }
      return traceSummary ? { retrieval_trace } : {};
   };

   // Query stages may rewrite what is searched for; none is registered yet.
   ctx.request = await runQueryStages(QUERY_STAGES, ctx.request, ctx);
   const request = ctx.request;

   // Tier 3: nothing to rank -> enumerate what the targets name.
   if (request.listingOnly) return runListing(ctx, uri);

   // Tier 4: retrieval over the package's entities. With an
   // embedding provider configured, ranking is semantic (DuckDB
   // cosine over cached entity embeddings) and ONLY semantic: when the
   // index is still building the answer says so (`retrieval:
   // "indexing"`, no results), and when it cannot serve the answer is
   // an error that names the reason. A configured server never answers
   // a search lexically, because a ranking that quietly differs from
   // the one the finished index gives makes results hard to trust.
   // Lexical lunr is the mode only when no provider is configured, and
   // then the payload carries no `retrieval` marker and no per-entity
   // `score`, byte-identical to the lexical-only releases.
   // The LLM stage settings are read before retrieval because source match
   // takes the source targets away from the retrievers.
   try {
      ctx.llmStages = resolveLlmStages(pkgIndex.pkg, ctx.meter as LlmMeter);
   } catch (error) {
      if (error instanceof StageError) {
         return stageFailureError(uri, error, traceFields());
      }
      throw error;
   }
   // Stored source summaries, read once for the response and the source match
   // prompt. Only when the package has them on; none stored means no change.
   ctx.sourceSummaries = await loadStoredSummaries(
      environmentStore,
      pkgIndex,
      environmentName,
      packageName,
   );
   // With source match on, source targets are answered by the model, so the
   // retrievers rank only the other targets. With none left there is nothing
   // to retrieve; the response keeps the shape of the mode the server is in.
   const matchingSources = sourceMatchActive(ctx);
   const retrievalCtx: PipelineContext = matchingSources
      ? {
           ...ctx,
           request: {
              ...request,
              searches: request.searches.filter(
                 (s) => s.targetType !== "source",
              ),
           },
        }
      : ctx;
   let ranked: RankedState | undefined;
   if (matchingSources && retrievalCtx.request.searches.length === 0) {
      ranked = {
         rows: [],
         belowCutoffCount: 0,
         retrieval: ctx.embeddingConfigured ? "semantic" : "lexical",
      };
   }
   for (const retriever of ranked ? [] : RETRIEVERS) {
      const result = await retriever.retrieve(retrievalCtx);
      if ("unavailable" in result) {
         // "unconfigured" means no provider: the next retriever is the mode.
         if (result.unavailable === "unconfigured") continue;
         return result.unavailable === "indexing"
            ? indexingResponse(uri, ctx, result)
            : semanticUnavailableError(uri, result);
      }
      ranked = { ...result, retrieval: retriever.name };
      break;
   }
   if (!ranked) throw new Error("No retriever produced a result");
   // The LLM stages run only on a ranked search (a listing returned above).
   // A failure in one is the answer: an error result naming the stage, never
   // an unrefined ranking.
   let cards: CardState;
   try {
      ranked = await runRankStages(rankStages(ctx), ranked, ctx);
      // Grouping into cards, paging and serialization are shared by both
      // retrievers so the two cannot drift.
      cards = await runCardStages(CARD_STAGES, assembleCards(ranked, ctx), ctx);
   } catch (error) {
      if (error instanceof StageError) {
         return stageFailureError(uri, error, traceFields());
      }
      throw error;
   }
   const { sources, totalSources, entitiesDropped, budgetDropped } = shapeCards(
      cards,
      ctx,
   );
   // Sources the size budget removed were returned by the page, so the page
   // warning counts them as returned and only the budget warning speaks of them.
   const pageReturned = sources.length + budgetDropped;
   const budgetWarning = budgetCutWarning(budgetDropped, ctx.settings.maxChars);
   // Cards rerank kept out of its top N are in total_available but not in the
   // page's reach, so raising `limit` would not show them: they get their own
   // warning and are left out of the page-cut one.
   const rerankDropped = cards.discarded ?? 0;
   const rerankWarning = rerankCutWarning(
      rerankDropped,
      ctx.llmStages?.rerank?.topSources,
   );
   if (ranked.retrieval === "semantic") {
      return jsonResource(uri, {
         sources,
         ranking: "relevance" as const,
         total_available: totalSources,
         returned: sources.length,
         retrieval: "semantic",
         // Always present on a semantic response, including 0: the
         // reading depends on being able to tell 0 from absent. Paired
         // with total_entities, without which the count is a bare number
         // an agent cannot scale -- see SemanticSearchResult for why
         // the ratio, not the count, carries the signal.
         below_cutoff_count: ranked.belowCutoffCount,
         ...(ranked.totalEntities !== undefined
            ? { total_entities: ranked.totalEntities }
            : {}),
         ...skillsFor(ctx),
         // Each cut in its own unit: `limit` drops whole sources, the
         // per-source cap drops entities inside the ones it kept.
         ...warningsFor(
            // Counted in CARDS, the same unit `returned` reports, so the
            // two cannot disagree.
            sourceCutWarning(pageReturned, totalSources - rerankDropped),
            rerankWarning,
            budgetWarning,
            entityCutWarning(entitiesDropped),
         ),
         ...traceFields(),
      });
   }
   // Lexical, which is only reached with no embedding provider: no
   // `retrieval` marker, so the payload is what it has always been.
   return jsonResource(uri, {
      sources,
      ranking: "relevance" as const,
      total_available: totalSources,
      returned: sources.length,
      ...skillsFor(ctx),
      ...warningsFor(
         sourceCutWarning(pageReturned, totalSources),
         budgetWarning,
         entityCutWarning(entitiesDropped),
      ),
      ...traceFields(),
   });
}

/**
 * The package's stored source summaries that still describe its sources, or
 * undefined when summaries are off for it (the package turned them off, or no
 * LLM is configured) or none is current. A summary is served only if its input
 * hash is the hash of the source as it is now: a reload that changed a source
 * leaves the old summary in the table until the next sync rewrites it, and a
 * request with only source targets does not wait for that sync. A read failure
 * is logged and answered without them: a summary only adds to a card, and the
 * sync, not this read, is where a failure is loud.
 */
async function loadStoredSummaries(
   environmentStore: EnvironmentStore,
   pkgIndex: PackageIndex,
   environmentName: string,
   packageName: string,
): Promise<ReadonlyMap<string, StoredSourceSummary> | undefined> {
   const settings = indexSettingsOf(pkgIndex.pkg).sourceSummary;
   if (!settings) return undefined;
   try {
      const stored = await loadSourceSummaries(
         environmentStore.storageManager.getDuckDbConnection(),
         environmentName,
         packageName,
         currentSummaryHashes(pkgIndex.directEntities, settings),
      );
      return stored.size > 0 ? stored : undefined;
   } catch (error) {
      logger.warn(
         "[MCP Tool getContext] Stored source summaries could not be read; answering without them",
         {
            environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return undefined;
   }
}

/** The part of the MCP request context this tool reads: the HTTP headers. */
interface RequestExtra {
   requestInfo?: { headers?: Record<string, string | string[] | undefined> };
}

/**
 * Diagnostic only: `X-Publisher-Retrieval-Trace: summary` adds a
 * `retrieval_trace` block to a ranked response and changes no result. It is a
 * header and not a tool argument so an agent never sees it and a tuning run
 * can set it from the client.
 */
export const TRACE_HEADER = "x-publisher-retrieval-trace";

function wantsTrace(extra: RequestExtra | undefined): boolean {
   const raw = extra?.requestInfo?.headers?.[TRACE_HEADER];
   const value = Array.isArray(raw) ? raw[0] : raw;
   return value?.trim().toLowerCase() === "summary";
}

/**
 * `PUBLISHER_MCP_TRACE=retrieval` in the server's environment writes the same
 * summary to the server log, one line per ranked call, and adds nothing to any
 * response. The eval loop's `serve.py --trace-retrieval` sets it. It is read on
 * each call so a test can change it.
 */
export const TRACE_ENV = "PUBLISHER_MCP_TRACE";

function traceToLog(): boolean {
   return process.env[TRACE_ENV]?.trim().toLowerCase() === "retrieval";
}

export function registerGetContextTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "get_context",
      GET_CONTEXT_DESCRIPTION,
      convergedContextShape,
      async (params: GetContextParams, extra?: RequestExtra) => {
         const request = resolveRequest(params);
         if (request.offset > 0 && !request.pureSourceListing) {
            // Refused rather than ignored. A ranked response has no
            // reproducible order to resume from, so honouring the offset
            // would drop rows the caller could never get back, and dropping
            // the offset silently would hand them page 1 while they believed
            // they were reading page 2.
            return contextError(
               buildMalloyUri(
                  {
                     environment: request.environmentName,
                     package: request.packageName,
                  },
                  "get-context",
               ),
               `${request.environmentName}/${request.packageName}`,
               // InvalidArgumentError, not a bare Error. This is a caller
               // mistake with a stated fix, and an unclassified throw lands on
               // classifyToolError's internal-fault branch -- so a deliberate
               // refusal came back as "An unexpected internal error occurred
               // during getContext.: Invalid offset: ...", which reads as a
               // server bug to report and invites the retry that cannot work.
               new InvalidArgumentError(
                  "Invalid offset: paging works only on a pure source listing (every search_target of type `source`, none with search_text), which is the one response with a resumable order. Fix: drop `offset`, and narrow with search_text or scopes instead.",
               ),
            );
         }
         return runContextQuery(
            request,
            environmentStore,
            unsupportedTargetWarnings(request),
            wantsTrace(extra),
         );
      },
   );
}
