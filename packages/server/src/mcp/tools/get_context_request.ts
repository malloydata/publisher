// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { z } from "zod/v3";
import { entityRowKey } from "./embedding_index";
import { getDimensionValueSettings } from "../../retrieval_config";
import type {
   Entity,
   ResolvedRequest,
   ResolvedSearch,
   ResolvedValueSearch,
} from "./get_context_types";

/**
 * The most sources one call may return. Shared by the parameter schema, the
 * truncation warnings and the scan, which must agree.
 */
export const MAX_LIMIT = 150;

/**
 * Sources a ranked search returns when the caller sets no limit. A listing
 * defaults to MAX_LIMIT: its cards are cheap. Both match the published
 * defaults.
 */
const DEFAULT_RANKED_LIMIT = 20;

/**
 * The request shape, matching the hosted retrieval API's `GetContextRequest`
 * field for field so one caller and eval harness serve both.
 *
 * Divergences: `scopes` is required (exactly one environment + package),
 * because indexes are built per package and lazily, so an unscoped search
 * could not tell "not modelled" from "not indexed yet"; and `target_type` adds
 * `join`. `dimensional_value` is answered by reading the data through the
 * query path (see get_context_values.ts), not from an index; with
 * `retrieval.dimensionValues.enabled: false` it is accepted and answered with
 * a warning.
 */
export const SEARCH_TARGET_TYPES = [
   "source",
   "dimension",
   "measure",
   "view",
   "dimensional_value",
   "join",
] as const;

export type SearchTargetType = (typeof SEARCH_TARGET_TYPES)[number];

/**
 * Which indexed entity kinds a target selects. `view` covers named queries, as
 * the published shape defines it. `dimensional_value` selects nothing: with
 * value search on, resolveRequest routes it to the values stage before this
 * table is read; the empty list is what remains when it is turned off.
 */
const KINDS_BY_TARGET: Record<SearchTargetType, string[]> = {
   source: ["source"],
   dimension: ["dimension"],
   measure: ["measure"],
   view: ["view", "query"],
   join: ["join"],
   dimensional_value: [],
};

export const convergedContextShape = {
   search_targets: z
      .array(
         z.object({
            target_type: z
               .enum(SEARCH_TARGET_TYPES)
               .describe(
                  "What to look for. `dimension`/`measure`/`view` match fields and the response groups them by source; `source` matches or lists sources; `join` matches a declared relationship.",
               ),
            search_text: z
               .string()
               .max(500)
               .nullish()
               .describe(
                  'Plain-English description of what you need, e.g. "the total revenue". Omit or null to enumerate this type instead of ranking it.',
               ),
            example_values: z
               .array(z.string().max(200))
               .max(20)
               .nullish()
               .describe(
                  'For a dimension or dimensional_value target: the literal values the user named, as typed (e.g. ["cancelled"]). Where this server indexes stored values, it looks them up to find the dimension that holds them; elsewhere they are ignored.',
               ),
         }),
      )
      .min(1)
      .describe(
         "What to find. One target per concept; several targets of different types answer one question in one call.",
      ),
   scopes: z
      .array(
         z.object({
            environment: z.string().describe("Environment name."),
            package: z.string().describe("Package name."),
            version: z
               .string()
               .nullish()
               .describe("Package version. Omit to use the served version."),
            model_path: z
               .string()
               .nullish()
               .describe('Model file within the package, e.g. "model.malloy".'),
            source: z
               .string()
               .nullish()
               .describe("Narrow to one source within the model."),
            entity_name: z
               .string()
               .nullish()
               .describe("Narrow to one entity within the source."),
         }),
      )
      .length(1)
      .describe(
         "Required, exactly one: the environment and package to search, optionally narrowed to a model, source, or entity. Call list_packages for the names.",
      ),
   filter_params: z
      .record(z.union([z.string(), z.array(z.string())]))
      .nullish()
      .describe(
         "Values for filters a source declares via #(filter), keyed by filter name.",
      ),
   givens: z
      .record(z.unknown())
      .nullish()
      .describe(
         "Given values, as execute_query takes them. Read only by `dimensional_value` lookups, which run through the same access gates as a query: send the givens a gate needs, or that source's values are withheld.",
      ),
   user_prompt: z
      .string()
      .nullish()
      .describe(
         "The user's question that led to this call, verbatim on the first turn. Used for observability; does not affect matching.",
      ),
   include_code: z
      .boolean()
      .nullish()
      .describe(
         "Return each entity's definition as `code`: a dimension's or measure's Malloy expression, or a view's definition as written. Off by default -- a source's #(doc) should say what a field means, and definitions are long. Turn it on to inspect what a measure actually computes. Pinning `scopes[].entity_name` turns it on by itself.",
      ),
   limit: z
      .number()
      .int()
      .min(1)
      .max(MAX_LIMIT)
      .nullish()
      .describe(
         `Sources per page (max ${MAX_LIMIT}). PREFER OMITTING: ranked search defaults to ${DEFAULT_RANKED_LIMIT}, a pure listing to ${MAX_LIMIT}. Where a ranked response carries next_offset, a page also ends before 35,000 characters, and the sources past it are on the next page, not dropped.`,
      ),
   offset: z
      .number()
      .int()
      .min(0)
      .nullish()
      .describe(
         "Sources to skip: a previous response's next_offset, sent with every other argument unchanged, returns the next page. Works on a pure listing, and on a ranked result whose response carried next_offset: that result is held for 10 minutes, so its pages are one result. Later, the search runs again and a warning says so. Other ranked results cannot be resumed.",
      ),
};

export type GetContextParams = z.infer<
   z.ZodObject<typeof convergedContextShape>
>;

export function resolveRequest(params: GetContextParams): ResolvedRequest {
   const scope = params.scopes[0];
   const kinds = new Set<string>();
   const searches: ResolvedSearch[] = [];
   const unsupported: SearchTargetType[] = [];

   const valueSearches: ResolvedValueSearch[] = [];
   const valuesOn = getDimensionValueSettings().enabled;
   let textlessValueTargets = 0;
   const exampleValues = new Map<number, string[]>();

   params.search_targets.forEach((target, targetIndex) => {
      const examples = (target.example_values ?? [])
         .map((v) => v.trim())
         .filter(Boolean);
      if (
         examples.length > 0 &&
         (target.target_type === "dimension" ||
            target.target_type === "dimensional_value")
      ) {
         exampleValues.set(targetIndex, examples);
      }
      if (target.target_type === "dimensional_value" && valuesOn) {
         // Answered by the values stage, which reads the data through the
         // query path; it selects no indexed entity kind. A target with no
         // text has nothing to look up: listing a dimension's values is a
         // query's job, and the response says so.
         const text = target.search_text?.trim();
         if (text) valueSearches.push({ targetIndex, text });
         else textlessValueTargets += 1;
         return;
      }
      const targetKinds = KINDS_BY_TARGET[target.target_type];
      if (targetKinds.length === 0) {
         unsupported.push(target.target_type);
         return;
      }
      for (const kind of targetKinds) kinds.add(kind);
      // Whitespace-only text is not a search; treat it as an enumeration of
      // that type rather than ranking every entity against nothing.
      const text = target.search_text?.trim();
      if (text) {
         searches.push({
            targetIndex,
            targetType: target.target_type,
            text,
            kinds: targetKinds,
         });
      }
   });

   // A value search is a ranked request even with no entity target beside it.
   const listingOnly = searches.length === 0 && valueSearches.length === 0;
   const pureSourceListing =
      listingOnly &&
      params.search_targets.length > 0 &&
      params.search_targets.every((t) => t.target_type === "source");
   return {
      pureSourceListing,
      environmentName: scope.environment,
      packageName: scope.package,
      ...(scope.model_path ? { modelPath: scope.model_path } : {}),
      ...(scope.source ? { sourceName: scope.source } : {}),
      ...(scope.entity_name ? { entityName: scope.entity_name } : {}),
      kinds,
      searches,
      valueSearches,
      ...(textlessValueTargets > 0 ? { textlessValueTargets } : {}),
      ...(params.givens ? { givens: params.givens } : {}),
      ...(params.filter_params ? { filterParams: params.filter_params } : {}),
      ...(exampleValues.size > 0 ? { exampleValues } : {}),
      listingOnly,
      // Pinning an entity by name turns code on, even over an explicit
      // `include_code: false`, matching the hosted retrieval API.
      includeCode: (params.include_code ?? false) || Boolean(scope.entity_name),
      unsupported,
      limit: params.limit ?? (listingOnly ? MAX_LIMIT : DEFAULT_RANKED_LIMIT),
      offset: params.offset ?? 0,
   };
}

/** Does this entity survive the scope's model/source/entity refinements? */
export function matchesScope(
   e: { source?: string; name: string; modelPath: string; kind: string },
   request: ResolvedRequest,
): boolean {
   if (request.modelPath && e.modelPath !== request.modelPath) return false;
   if (request.sourceName && e.source !== request.sourceName) return false;
   // A source row survives an entity_name scope: it is the card the named
   // entity nests in, not a competitor for the slot.
   if (
      request.entityName &&
      e.kind !== "source" &&
      e.name !== request.entityName
   ) {
      return false;
   }
   return true;
}

/**
 * The distinct vector-cache row keys (kind, source, name) the request's scope
 * admits. A row is in scope when any entity sharing it is; the fan-out later
 * keeps only the in-scope model path.
 */
export function scopeKeysFor(
   entities: Iterable<Entity>,
   request: ResolvedRequest,
): Array<{ kind: string; source: string; name: string }> {
   const seen = new Set<string>();
   const keys: Array<{ kind: string; source: string; name: string }> = [];
   for (const e of entities) {
      if (!matchesScope(e, request)) continue;
      const source = e.source ?? "";
      const key = entityRowKey(e.kind, source, e.name);
      if (seen.has(key)) continue;
      seen.add(key);
      keys.push({ kind: e.kind, source, name: e.name });
   }
   return keys;
}
