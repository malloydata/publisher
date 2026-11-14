// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { z } from "zod/v3";
import type { GivenValue } from "@malloydata/malloy";
import { getDimensionValueSettings } from "../../retrieval/config";
import {
   MAX_LIMIT,
   SEARCH_TARGET_TYPES,
   type SearchTargetType,
   type ResolvedRequest,
   type ResolvedSearch,
   type ResolvedValueSearch,
} from "../../retrieval/model/types";

/**
 * Sources a ranked search returns when the caller sets no limit. A listing
 * defaults to MAX_LIMIT: its cards are cheap. Both match the published
 * defaults.
 */
const DEFAULT_RANKED_LIMIT = 20;

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
                  "What to look for. `dimension`/`measure`/`view` match fields and the response groups them by source; `source` matches or lists sources; `join` matches a declared relationship; `dimensional_value` matches one literal value against the values stored in `#(index)` dimensions.",
               ),
            search_text: z
               .string()
               .max(500)
               .nullish()
               .describe(
                  'Plain-English description of what you need, e.g. "the total revenue". For a `dimensional_value` target, one value exactly as the user typed it, e.g. "Denvr": it is matched by spelling, typos included, and with an embedding provider by meaning, and the stored string ("Denver") returns in `entities[].values` on the dimension that holds it. Send each value as its own target beside the `dimension` target for its field ("the city of the store"), and keep it out of that target\'s text. Once you know the field, scope the value search to it with `scopes[].source` and `entity_name`: an unscoped value search is allowed but slower, and can return look-alike values from other fields. Omit or null to enumerate this type instead of ranking it.',
               ),
         }),
      )
      .min(1)
      .nullish()
      .describe(
         "What to find. One target per concept; several targets of different types answer one question in one call. Omit to list instead: a source in `scopes` lists its fields, a package its sources; with no `scopes`, `environment` lists that environment's packages, and nothing at all lists the environments.",
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
      .nullish()
      .describe(
         "Exactly one when given: the environment and package to search or list, optionally narrowed to a model, source, or entity. Omit it to find those names: the response lists environments, or `environment`'s packages. A search needs it, unless only one package is served.",
      ),
   environment: z
      .string()
      .nullish()
      .describe(
         "With no `scopes`: list this environment's packages. A level with one choice is entered, so an environment with one package returns that package's sources (or searches it). Must match `scopes[0].environment` when both are given.",
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
         "The givens in force for this caller, as the trusted tier set them, and as execute_query takes them. A source whose #(authorize) lock cannot admit the caller on the #(secure) givens supplied, or reads a secure given not supplied, is omitted, as a source locked with `false` is. A `dimensional_value` lookup on a gated source runs through the same gates as a query, so its values are withheld without the givens it needs. Plain givens and row filters hide nothing. Omit to see every published source.",
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
         `Sources per page (max ${MAX_LIMIT}), or fields per page in a field listing. PREFER OMITTING: ranked search defaults to ${DEFAULT_RANKED_LIMIT}, a pure listing to ${MAX_LIMIT}. Where a ranked response carries next_offset, a page also ends before 35,000 characters, and the sources past it are on the next page, not dropped.`,
      ),
   offset: z
      .number()
      .int()
      .min(0)
      .nullish()
      .describe(
         "Sources to skip: a previous response's next_offset, sent with every other argument unchanged, returns the next page. Works on a pure listing, and on a ranked result whose response carried next_offset: that result is held for 10 minutes, so its pages are one result. Later, the search runs again and a warning says so. Other ranked results cannot be resumed.",
      ),
   entity_offset: z
      .number()
      .int()
      .min(0)
      .nullish()
      .describe(
         "Fields to skip in a field listing (dimension, measure, view or join targets with no search_text): a previous response's next_entity_offset, sent with every other argument unchanged, returns the next page. Not for a search or a source listing, which page with offset.",
      ),
};

export type GetContextParams = z.infer<
   z.ZodObject<typeof convergedContextShape>
>;

/** Params with the package named and the targets set: what resolveRequest reads. */
export type ScopedParams = GetContextParams & {
   scopes: NonNullable<GetContextParams["scopes"]>;
   search_targets: NonNullable<GetContextParams["search_targets"]>;
};

export function resolveRequest(params: ScopedParams): ResolvedRequest {
   const scope = params.scopes[0];
   const kinds = new Set<string>();
   const searches: ResolvedSearch[] = [];
   const unsupported: SearchTargetType[] = [];

   const valueSearches: ResolvedValueSearch[] = [];
   const valuesOn = getDimensionValueSettings().enabled;
   let textlessValueTargets = 0;

   params.search_targets.forEach((target, targetIndex) => {
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
      ...(params.filter_params ? { filterParams: params.filter_params } : {}),
      listingOnly,
      // Pinning an entity by name turns code on, even over an explicit
      // `include_code: false`, matching the hosted retrieval API.
      includeCode: (params.include_code ?? false) || Boolean(scope.entity_name),
      unsupported,
      limit: params.limit ?? (listingOnly ? MAX_LIMIT : DEFAULT_RANKED_LIMIT),
      offset: params.offset ?? 0,
      entityOffset: params.entity_offset ?? 0,
      ...(params.givens && Object.keys(params.givens).length > 0
         ? { givens: params.givens as Record<string, GivenValue> }
         : {}),
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
