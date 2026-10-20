// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../../service/environment_store";
import { InvalidArgumentError } from "../../errors";
import { buildMalloyUri } from "../handler_utils";
import { jsonResource } from "../tool_response";
import { logger } from "../../logger";
import { runFcjQuery, type TraceMode } from "../context/engine";
import {
   convergedContextShape,
   matchesScope,
   resolveRequest,
   type GetContextParams,
} from "./get_context_request";
import {
   contextError,
   listingPageWarning,
   makeWarningsFor,
   projectEntity,
   skillsFor,
   toSourceResults,
   truncationWarning,
   unsupportedTargetWarnings,
} from "./get_context_response";
import type {
   ListingContext,
   PackageIndex,
   ResolvedRequest,
} from "./get_context_types";
import { getPackageIndex, sourceContextKey } from "./package_entities";

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
search_targets: one per concept, {target_type, search_text}; target_type is source|dimension|measure|view|join|dimensional_value, omitting search_text enumerates that type. dimensional_value: search_text is the value as typed; scope it to its source; stored strings return in entities[].values (gated sources need givens, as in execute_query). scopes: {environment, package} + optional model_path, source, entity_name. limit caps sources (max 150). offset pages results. include_code or a pinned entity_name returns code.

## Response
sources[], best first; a source repeats once per model_path resolving it, query any. source_info: resource_id (environment/package/model_path/source) -> execute_query's environmentName/packageName/modelPath/sourceName; docs (… = truncated), one_line_summary, complete joins, givens, accessFilter/authorize, filter_params. entities[] nest under it: name, entity_type, description, data_type, relationship (fan-out), join_path, aliases, relevance, entity_id, values[] {value, relevance}, matched_targets[].match_reason.
ranking, returned of total_available sources, next_offset: more pages, warnings[]. Past 35,000 characters whole sources are dropped; warnings say so.
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
   ctx: ListingContext,
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

/**
 * One call, resolved: a request with no search text enumerates what its
 * targets name (a listing, answered here from the package's entity index);
 * anything else is ranked by the find, list, judge engine (../context/engine.ts).
 */
async function runContextQuery(
   resolved: ResolvedRequest,
   environmentStore: EnvironmentStore,
   extraWarnings: string[] = [],
   traceMode: TraceMode = "none",
): Promise<ReturnType<typeof jsonResource>> {
   const { environmentName, packageName, sourceName } = resolved;
   logger.debug("[MCP Tool getContext] Retrieving context", {
      environmentName,
      packageName,
      sourceName,
      searches: resolved.searches.length,
      listingOnly: resolved.listingOnly,
   });

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

   // A ranked result is held for a while (../context/pages.ts), so `offset`
   // resumes it. A listing other than the catalog browse has no order to
   // resume from.
   if (
      resolved.offset > 0 &&
      resolved.listingOnly &&
      !resolved.pureSourceListing
   ) {
      // Refused rather than ignored: silently ignoring the offset would hand
      // back page 1 as if it were page 2.
      return contextError(
         uri,
         `${environmentName}/${packageName}`,
         // InvalidArgumentError, not a bare Error: an unclassified throw is
         // reported by classifyToolError as an internal fault.
         new InvalidArgumentError(
            "Invalid offset: paging works on a ranked search (a target with search_text) and on a pure source listing (every search_target of type `source`, none with search_text). Fix: drop `offset`, or add search_text to a target.",
         ),
      );
   }

   const staleNote = await staleNoteFor(
      environmentStore,
      environmentName,
      packageName,
   );
   const ctx: ListingContext = {
      request: resolved,
      pkgIndex,
      warnings: [staleNote, ...extraWarnings].filter(
         (warning): warning is string => Boolean(warning),
      ),
   };

   // Nothing to rank -> enumerate what the targets name.
   if (resolved.listingOnly) return runListing(ctx, uri);

   return runFcjQuery({
      request: resolved,
      environmentStore,
      pkgIndex,
      uri,
      warnings: ctx.warnings,
      traceMode,
   });
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

/**
 * The trace the request asked for. `summary` is the block of stage rows;
 * `full` adds what each stage kept.
 */
function traceModeOf(extra: RequestExtra | undefined): TraceMode {
   const raw = extra?.requestInfo?.headers?.[TRACE_HEADER];
   const value = (Array.isArray(raw) ? raw[0] : raw)?.trim().toLowerCase();
   return value === "summary" || value === "full" ? value : "none";
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
         return runContextQuery(
            request,
            environmentStore,
            unsupportedTargetWarnings(request),
            traceModeOf(extra),
         );
      },
   );
}
