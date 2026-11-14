// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerTool } from "../register_tool";
import type { EnvironmentCatalog } from "../../service/environment_ports";
import { InvalidArgumentError } from "../../errors";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import {
   type AccessIdentity,
   accessIdentityOf,
} from "../../service/access_record";
import type { RequestExtra } from "../request_extra";
import { jsonResource, jsonToolError } from "../tool_response";
import { logger } from "../../logger";
import type { TraceMode } from "../../retrieval/search/engine";
import type { RetrievalService } from "../../retrieval/service";
import {
   convergedContextShape,
   matchesScope,
   resolveRequest,
   type GetContextParams,
} from "./get_context_request";
import {
   listingPageWarning,
   makeWarningsFor,
   projectEntity,
   skillsFor,
   toSourceResults,
   unsupportedTargetWarnings,
} from "../../retrieval/search/response";
import type {
   ListingContext,
   PackageIndex,
   ResolvedRequest,
   SearchResult,
} from "../../retrieval/model/types";
import { sourceContextKey } from "../../retrieval/model/package_entities";
import { lockedForCaller } from "../../service/discovery_lock";
import {
   packageInfoOf,
   resolveLevel,
   type LevelOutcome,
} from "./get_context_levels";
import type { GivenValue } from "@malloydata/malloy";
import { parseEnvironmentName } from "../../service/identity";

/**
 * Contract rules first, reference last, because a tail-truncating client drops
 * whatever is last. The length budget is pinned in server.protocol.spec.ts.
 */
const GET_CONTEXT_DESCRIPTION = `Retrieve the entities in a Malloy package most relevant to a plain-English question.

## Contract rules
- Use returned names verbatim; never invent one.
- One target per concept, not a list in one search_text. A source returns only its matched fields, not all of them.
- No search_targets lists environments, packages, sources or fields. A search needs one package in scopes.
- Read warnings and any error/stale field before trusting a number or calling data absent.
- A source's joins are all listed: empty means none. In an answer, never write a join; tell the user.
- Read a source's doc before querying: it holds grain and population rules.
- accessFilter/authorize mean gated; a deny-all source never appears here.
- Empty sources: nothing answered; see unresolved_targets.

## Parameters
search_targets: {target_type, search_text}; target_type is source|dimension|measure|view|join|dimensional_value, omitting search_text enumerates that type. dimensional_value: search_text is one value as typed, beside its dimension target; stored strings return in entities[].values (gated sources need givens, as in execute_query). Once its field is known, scope to it (source, entity_name). scopes: {environment, package} + optional model_path, source, entity_name. offset pages results. include_code or a pinned entity_name returns code.

## Response
sources[], best first; a source repeats once per model_path resolving it, query any. source_info: resource_id (environment/package/model_path/source) -> execute_query's environmentName/packageName/modelPath/sourceName; docs (… = truncated), one_line_summary, complete joins, givens, accessFilter/authorize, filter_params. entities[] nest under it: name, entity_type, description, data_type, classifications, relationship (fan-out), join_path, aliases, relevance, entity_id, values[] {value, relevance}, matched_targets[].match_reason.
ranking, returned of total_available sources, next_offset: more pages (35,000 chars each), warnings[], clusters, unresolved_targets. retrieval "indexing": still building, retry soon. Errors carry retrieval_reason ("llm-stage-failed" names retrieval_stage).`;

/**
 * An error payload keeps the success shape's empty `sources: []`, so a caller
 * can read it unconditionally. Routed through classifyToolError so each error
 * class carries real remediation (e.g. an unknown package says so).
 */
function contextError(uri: string, identifier: string, error: unknown) {
   return jsonToolError(
      uri,
      classifyToolError("getContext", identifier, error),
      { sources: [] },
   );
}

/** The MCP result for a ranked search: the one place it gets its envelope. */
function searchResource(
   uri: string,
   result: SearchResult,
): ReturnType<typeof jsonResource> {
   return result.ok
      ? jsonResource(uri, result.payload)
      : jsonToolError(uri, result.error, result.payload);
}

/**
 * Whether the package's most recent reload failed, as a warning to attach.
 */
async function staleNoteFor(
   environmentStore: EnvironmentCatalog,
   environmentName: string,
   packageName: string,
): Promise<string | undefined> {
   // A stale package answers exactly like a current one, from the model
   // compiled before the last save; nothing else in the payload can say so.
   // Best effort: a failed lookup must not take discovery down with it.
   let staleNote: string | undefined;
   try {
      const environment = await environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
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

/**
 * The (model path, source) keys of every published source whose lock cannot
 * admit a caller holding `givens`, over every listed model of the package.
 */
async function lockedSourceKeys(
   pkg: PackageIndex["pkg"],
   givens: Record<string, GivenValue>,
): Promise<Set<string>> {
   const keys = new Set<string>();
   const models = await pkg.listModels();
   for (const apiModel of models) {
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      let sources;
      try {
         sources = model?.getSources?.();
      } catch {
         continue;
      }
      for (const name of lockedForCaller(sources, givens)) {
         keys.add(sourceContextKey(modelPath, name));
      }
   }
   return keys;
}

/**
 * Why the request's paging control is the wrong one for it, or undefined when
 * it is right. A field listing has no source order to resume, and a search or
 * a source browse has no field rows.
 */
function misusedOffset(request: ResolvedRequest): string | undefined {
   const fieldListing = request.listingOnly && !request.pureSourceListing;
   if (fieldListing && request.offset > 0) {
      return "Invalid offset: offset counts sources, and this is a field listing (entity targets with no search_text). Fix: send the previous response's next_entity_offset as entity_offset instead.";
   }
   if (!fieldListing && request.entityOffset > 0) {
      return "Invalid entity_offset: entity_offset pages a field listing (dimension, measure, view or join targets with no search_text). Fix: drop entity_offset; a search or a source listing pages with offset.";
   }
   return undefined;
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
   const matching = Array.from(byId.values()).filter(
      (e) =>
         // A scoped source's own row always survives: it becomes the
         // card rather than competing for an entity slot.
         ((sourceName && e.kind === "source" && e.source === sourceName) ||
            request.kinds.has(e.kind)) &&
         matchesScope(e, request) &&
         // A source the caller cannot enter spends no slot and is not counted.
         !droppedSources.has(sourceContextKey(e.modelPath, e.source ?? "")),
   );
   // A record's members (`delivery.city`) after everything else, so a wide
   // record cannot spend the cap before the source's other fields, joins and
   // views. Every member still follows its source's row.
   const inScope = [
      ...matching.filter((e) => !e.recordPath),
      ...matching.filter((e) => e.recordPath),
   ];
   // A listing is catalog order, so it pages: a source browse by `offset`
   // (sources), a field listing by `entity_offset` (rows). The order holds for
   // a loaded package; a reload between pages can shift it. A drill-down's
   // source row becomes the card on every page, so it is not a row to page.
   const isCard = (e: (typeof inScope)[number]) =>
      Boolean(sourceName) && e.kind === "source";
   const pageable = inScope.filter((e) => !isCard(e));
   const skip = request.pureSourceListing
      ? request.offset
      : request.entityOffset;
   const page = new Set(pageable.slice(skip, skip + request.limit));
   const capped = inScope.filter((e) => isCard(e) || page.has(e));
   const next = skip + page.size;
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
   // Distinct (model path, source) pairings, so the count stays right for a
   // field listing, whose rows are fields, and for an entity whose own
   // source row was filtered out.
   const cardsInScope = new Set(
      inScope.map((e) => sourceContextKey(e.modelPath, e.source ?? "")),
   ).size;
   const listingEnvelope = {
      ranking: "prominence" as const,
      total_available: cardsInScope,
      returned: sources.length,
      ...(next < pageable.length
         ? request.pureSourceListing
            ? { next_offset: next }
            : { next_entity_offset: next }
         : {}),
   };
   // The package loaded (a failed load throws from packageIndex), so an
   // empty listing is a curation gap, not an empty database; say so. Not when
   // `kinds` is empty: then every target named an unindexed type and
   // unsupportedTargetWarnings has already explained it. Counted before
   // paging, so an offset past the end is not mistaken for one.
   if (inScope.length === 0 && !sourceName && request.kinds.size > 0) {
      return jsonResource(uri, {
         sources,
         ...listingEnvelope,
         ...(ctx.packageInfo ? { package_info: ctx.packageInfo } : {}),
         ...skillsFor(ctx),
         ...warningsFor(
            "This package loaded but exposes no sources. That is a curation gap, not an empty database: check what the package's index.malloy exports (its export { ... }), and call get_status for load errors and stale packages.",
         ),
      });
   }
   return jsonResource(uri, {
      sources,
      ...listingEnvelope,
      ...(ctx.packageInfo ? { package_info: ctx.packageInfo } : {}),
      ...skillsFor(ctx),
      ...warningsFor(
         listingPageWarning({
            param: request.pureSourceListing ? "offset" : "entity_offset",
            unit: request.pureSourceListing ? "sources" : "entities",
            skipped: skip,
            returned: page.size,
            total: pageable.length,
         }),
      ),
   });
}

/**
 * One call, resolved: a request with no search text enumerates what its
 * targets name (a listing, answered here from the package's entity index);
 * anything else is ranked by the find, list, judge engine (retrieval/search/engine.ts).
 */
async function runContextQuery(
   resolved: ResolvedRequest,
   environmentStore: EnvironmentCatalog,
   retrieval: RetrievalService,
   extraWarnings: string[] = [],
   traceMode: TraceMode = "none",
   withPackageInfo = false,
   identity?: AccessIdentity,
   clientSignal?: AbortSignal,
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
      pkgIndex = await retrieval.packageIndex(environmentName, packageName);
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

   // Each listing pages by one control, in one unit: a source browse and a
   // held ranked result by `offset` (sources), a field listing by
   // `entity_offset` (rows). The other is refused rather than ignored:
   // ignoring it would hand back page 1 as if it were page 2.
   const pagingError = misusedOffset(resolved);
   if (pagingError) {
      return contextError(
         uri,
         `${environmentName}/${packageName}`,
         // InvalidArgumentError, not a bare Error: an unclassified throw is
         // reported by classifyToolError as an internal fault.
         new InvalidArgumentError(pagingError),
      );
   }

   const staleNote = await staleNoteFor(
      environmentStore,
      environmentName,
      packageName,
   );
   // Discovery under a lock: with the caller's givens in hand, a source whose
   // lock cannot admit them on the secure givens is omitted like a deny-all
   // source. The cached index is not touched; the request sees a copy whose
   // dropped set is wider. See service/discovery_lock.ts.
   if (resolved.givens) {
      const locked = await lockedSourceKeys(pkgIndex.pkg, resolved.givens);
      if (locked.size > 0) {
         pkgIndex = {
            ...pkgIndex,
            droppedSources: new Set([...pkgIndex.droppedSources, ...locked]),
         };
      }
   }
   const ctx: ListingContext = {
      request: resolved,
      pkgIndex,
      warnings: [staleNote, ...extraWarnings].filter(
         (warning): warning is string => Boolean(warning),
      ),
   };
   // From the package already in hand: naming one package never reads the
   // rest of the server.
   if (withPackageInfo) {
      const built = packageInfoOf(
         environmentName,
         packageName,
         (
            pkgIndex.pkg as {
               getPackageMetadata?: () => { description?: string };
            }
         ).getPackageMetadata?.()?.description,
      );
      ctx.packageInfo = built.info;
      if (built.warning) ctx.warnings.push(built.warning);
   }

   // Nothing to rank -> enumerate what the targets name.
   if (resolved.listingOnly) return runListing(ctx, uri);

   return searchResource(
      uri,
      await retrieval.search({
         request: resolved,
         pkgIndex,
         warnings: ctx.warnings,
         traceMode,
         ...(identity ? { identity } : {}),
         ...(clientSignal ? { clientSignal } : {}),
      }),
   );
}

/**
 * Diagnostic only: `X-Publisher-Retrieval-Trace: summary` adds a
 * `retrieval_trace` block to a ranked response and changes no result. It is a
 * header and not a tool argument so an agent never sees it and a tuning run
 * can set it from the client.
 */
const TRACE_HEADER = "x-publisher-retrieval-trace";

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
   environmentStore: EnvironmentCatalog,
   retrieval: RetrievalService,
): void {
   registerTool(
      mcpServer,
      "get_context",
      GET_CONTEXT_DESCRIPTION,
      convergedContextShape,
      async (params: GetContextParams, extra?: RequestExtra) => {
         let level: LevelOutcome;
         try {
            level = await resolveLevel(params, environmentStore);
         } catch (error) {
            return contextError(
               buildMalloyUri({}, "get-context"),
               "environments",
               error,
            );
         }
         if (level.kind !== "scoped") {
            const uri = buildMalloyUri(
               level.environment ? { environment: level.environment } : {},
               "get-context",
            );
            return level.kind === "listing"
               ? jsonResource(uri, level.payload)
               : contextError(
                    uri,
                    level.environment ?? "environments",
                    level.error,
                 );
         }
         const request = resolveRequest(level.params);
         return runContextQuery(
            request,
            environmentStore,
            retrieval,
            [...level.warnings, ...unsupportedTargetWarnings(request)],
            traceModeOf(extra),
            level.withPackageInfo === true,
            accessIdentityOf(extra?.requestInfo?.headers),
            extra?.signal,
         );
      },
   );
}
