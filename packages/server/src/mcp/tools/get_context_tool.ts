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
   runRankStages,
   type CardState,
   type CardStage,
   type PipelineContext,
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
import { valuesStage } from "./get_context_values";
import { getDimensionValueSettings } from "../../retrieval_config";
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
 * Contract rules first, reference last, because a tail-truncating client drops
 * whatever is last. The length budget is pinned in server.protocol.spec.ts.
 */
const GET_CONTEXT_DESCRIPTION = `Retrieve the entities in a Malloy package most relevant to a plain-English question.

## Contract rules
- Use returned names verbatim; never invent one.
- One call answers: describe the fields you need as search_targets; sources return with them nested.
- scopes is REQUIRED: exactly one environment and package (list_packages).
- Read warnings and any error/stale field before trusting a number or calling data absent.
- A source's joins are all listed: empty means none. In an answer, never write a join; tell the user.
- Read a source's doc before querying: it holds grain and population rules.
- accessFilter/authorize mean gated; a deny-all source never appears here.
- Empty sources: nothing cleared the floor, or the LLM steps pruned all.

## Parameters
search_targets: one per concept, {target_type, search_text}; target_type is source|dimension|measure|view|join|dimensional_value, omitting search_text enumerates that type. dimensional_value: search_text is the value as typed; scope it to its source; stored strings return in entities[].values (gated sources need givens, as in execute_query). scopes: {environment, package} + optional model_path, source, entity_name. limit caps sources (max 150). offset pages a listing. include_code or a pinned entity_name returns code.

## Response
sources[], best first; a source repeats once per model_path resolving it, query any. source_info: resource_id (environment/package/model_path/source) -> execute_query's environmentName/packageName/modelPath/sourceName; docs (… = truncated), one_line_summary, complete joins, givens, accessFilter/authorize, filter_params. entities[] nest under it: name, entity_type, description, data_type, relationship (fan-out), join_path, aliases, relevance, entity_id, values[] {value, relevance}, matched_targets[].match_reason.
ranking, returned of total_available sources, next_offset on a listing, warnings[]. Past 35,000 characters whole sources are dropped; warnings say so.
below_cutoff_count of total_entities rejected. retrieval "indexing": still building, retry soon. Errors carry retrieval_reason ("llm-stage-failed" names retrieval_stage).`;

/**
 * Whether the package's most recent reload failed, as a warning to attach.
 */
async function staleNoteFor(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
): Promise<string | undefined> {
   // A stale package answers exactly like a current one, from the model
   // compiled before the last save; nothing else in the payload can say so.
   // Best effort: a failed lookup must not take discovery down with it.
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
   // A drill-down (sourceName set) lists every entity the named source
   // offers, not just the source row the caller already has.
   // collectEntities pushes a source ahead of its own fields and Array.from
   // keeps that order, so toSourceResults sees the source before the
   // entities that nest under it.
   const inScope = Array.from(byId.values()).filter(
      (e) =>
         // A scoped source's own row always survives: it becomes the
         // card rather than competing for an entity slot.
         ((sourceName && e.kind === "source" && e.source === sourceName) ||
            request.kinds.has(e.kind)) &&
         matchesScope(e, request),
   );
   // The limit caps entities; a drill-down's source row becomes the card and
   // does not spend a slot. Only a pure browse has a stable order, so only it
   // honours `offset`.
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
    * Rows the cap spends from: the unit both sides of the truncation warning
    * are counted in. A scoped source's own row is excluded from both counts,
    * not subtracted from one, because it may not exist.
    */
   const spendable = (rows: typeof inScope) =>
      sourceName ? rows.filter((e) => e.kind !== "source").length : rows.length;
   const cappable = spendable(inScope);
   const sources = toSourceResults(
      capped.map((e) => projectEntity(e, environmentName, packageName)),
      sourceContext,
      environmentName,
      packageName,
      new Map(),
      request.includeCode,
      droppedSources,
   );
   // A listing is catalog order, not a ranking: it names the ordering and
   // omits a score rather than inventing one. Both counts are in cards (one
   // per model path resolving a source), the unit the payload is made of.
   const consumed = request.offset + sources.length;
   // Distinct (model path, source) pairings, so the count stays right even
   // for an entity whose own source row was filtered out.
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
   // The package loaded (a failed load throws from getPackageIndex), so an
   // empty listing is a curation gap, not an empty database; say so. Not when
   // `kinds` is empty: then every target named an unindexed type and
   // unsupportedTargetWarnings has already explained it.
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
         request.pureSourceListing
            ? listingPageWarning(sources.length, inScope.length)
            : truncationWarning(spendable(capped), cappable),
      ),
   });
}

const RETRIEVERS: readonly Retriever[] = [semanticRetriever, lexicalRetriever];
// Source match is listed only for a request it applies to, so a trace shows
// no row for it otherwise. The values stage is last: it adds rows from the
// data, which the LLM steps before it must neither see nor reorder, and it is
// listed only for a request with value targets.
const rankStages = (ctx: PipelineContext): RankStage[] => [
   ...(sourceMatchActive(ctx) ? [sourceMatchStage] : []),
   refineStage,
   ...(ctx.request.valueSearches.length > 0 ? [valuesStage] : []),
];
const CARD_STAGES: readonly CardStage[] = [rerankStage];

/**
 * Tier 3 (enumerate) and tier 4 (rank) over one package, driven by an already
 * resolved request. Ranking is semantic when an embedding provider is
 * configured and lexical (lunr/BM25) otherwise, over the package's cached
 * entity index (see getPackageIndex).
 */
async function runContextQuery(
   resolved: ResolvedRequest,
   environmentStore: EnvironmentStore,
   extraWarnings: string[] = [],
   traceSummary = false,
): Promise<ReturnType<typeof jsonResource>> {
   const { environmentName, packageName, sourceName } = resolved;
   logger.debug("[MCP Tool getContext] Retrieving context", {
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
   // Spread into a ranked payload; {} without the trace header. With
   // PUBLISHER_MCP_TRACE=retrieval the summary also goes to the server log.
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

   const request = ctx.request;

   // Tier 3: nothing to rank -> enumerate what the targets name.
   if (request.listingOnly) return runListing(ctx, uri);

   // Tier 4. With an embedding provider configured, ranking is ONLY semantic:
   // a still-building index answers "indexing" and an unusable one an error,
   // never a lexical fallback that quietly ranks differently. Lexical is the
   // mode only when no provider is configured.
   // LLM stage settings are read before retrieval because source match takes
   // the source targets away from the retrievers.
   try {
      ctx.llmStages = resolveLlmStages(pkgIndex.pkg, ctx.meter as LlmMeter);
   } catch (error) {
      if (error instanceof StageError) {
         return stageFailureError(uri, error, traceFields());
      }
      throw error;
   }
   // Read once, for both the response and the source match prompt.
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
   // Nothing for the retrievers to rank: every entity target went to source
   // match, or the request is only value searches. The values stage, not the
   // entity index, answers the second case, so it must not wait on the index.
   if (retrievalCtx.request.searches.length === 0) {
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
   // A failed LLM stage is the answer (an error naming the stage), never an
   // unrefined ranking.
   let cards: CardState;
   try {
      ranked = await runRankStages(rankStages(ctx), ranked, ctx);
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
   // Cards rerank kept out of its top N are beyond any page's reach, so they
   // get their own warning and are left out of the page-cut one.
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
         // Always present, including 0, and paired with total_entities: the
         // ratio carries the signal (see SemanticSearchResult).
         below_cutoff_count: ranked.belowCutoffCount,
         ...(ranked.totalEntities !== undefined
            ? { total_entities: ranked.totalEntities }
            : {}),
         ...skillsFor(ctx),
         ...warningsFor(
            sourceCutWarning(pageReturned, totalSources - rerankDropped),
            rerankWarning,
            budgetWarning,
            entityCutWarning(entitiesDropped),
         ),
         ...traceFields(),
      });
   }
   // Lexical (no embedding provider): no `retrieval` marker.
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
 * undefined when summaries are off or none is current. A summary is served
 * only if its input hash matches the source as it is now, since a reload
 * leaves the old one stored until the next sync. A read failure is logged and
 * answered without them: the sync, not this read, is where failure is loud.
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
 * `PUBLISHER_MCP_TRACE=retrieval` writes the trace summary to the server log,
 * one line per ranked call, and adds nothing to any response. Read on each
 * call so a test can change it.
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
            // Refused rather than ignored: a ranked response has no
            // reproducible order to resume from, and silently ignoring the
            // offset would hand back page 1 as if it were page 2.
            return contextError(
               buildMalloyUri(
                  {
                     environment: request.environmentName,
                     package: request.packageName,
                  },
                  "get-context",
               ),
               `${request.environmentName}/${request.packageName}`,
               // InvalidArgumentError, not a bare Error: an unclassified throw
               // is reported by classifyToolError as an internal fault.
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
