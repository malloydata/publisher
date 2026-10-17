// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { z } from "zod/v3";
import { entityRowKey } from "./embedding_index";
import type {
   Entity,
   ResolvedRequest,
   ResolvedSearch,
} from "./get_context_types";

/**
 * The most entities one call may return, and so also the ceiling on what the
 * semantic query may fetch: the ranked scan over-fetches to fill its source
 * window, and this bounds the scan it can ask for. Defined once because the
 * parameter schema, the truncation warning and that scan must agree.
 */
export const MAX_LIMIT = 150;

/**
 * Sources a ranked search returns when the caller sets no limit. A listing
 * defaults to MAX_LIMIT instead: a browse that stops early is not a browse,
 * and its cards are cheap because they carry no entities. Both match the
 * published defaults.
 */
const DEFAULT_RANKED_LIMIT = 20;

/**
 * The request shape, matching the hosted retrieval API's `GetContextRequest`
 * field for field, so one caller, one skill and one eval harness serve a local
 * server and a hosted one. See the response-shape note on SourceCard for the
 * other half.
 *
 * Two behavioural divergences, both stated rather than left to be discovered:
 *
 * 1. `scopes` is optional in the published schema; here exactly one entry is
 *    REQUIRED, and it must name an environment and a package. Publisher builds
 *    its retrieval indexes per package and lazily -- a package nothing has
 *    queried has no vectors at all -- so an unscoped search would answer from
 *    whatever happened to be warm and could not tell "not modelled" from "not
 *    indexed yet". Refusing is the honest answer; guessing is not. Relaxing
 *    this later is purely additive and changes no caller.
 * 2. `target_type` is a SUPERSET, adding `join`: a model's declared joins are
 *    retrievable entities here, and an agent that cannot see one concludes the
 *    model has none. `dimensional_value` is accepted and answered with a
 *    warning, because Publisher indexes no dimensional values.
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
 * Which indexed entity kinds a target selects. `view` covers named queries as
 * well, because the published shape defines that target as "pre-built analyses
 * or named queries" and a model-level named query is exactly that.
 * `dimensional_value` selects nothing: there is no value index to select from.
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
         `Sources per page (max ${MAX_LIMIT}). PREFER OMITTING: ranked search defaults to ${DEFAULT_RANKED_LIMIT}, a pure listing to ${MAX_LIMIT}.`,
      ),
   offset: z
      .number()
      .int()
      .min(0)
      .nullish()
      .describe(
         "Sources to skip, from a previous response's next_offset. Pure listings only; ranked results cannot be resumed.",
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

   params.search_targets.forEach((target, targetIndex) => {
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

   const listingOnly = searches.length === 0;
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
      listingOnly,
      // Pinning an entity by name turns code on, matching the hosted
      // retrieval API: a caller who has narrowed to one entity is asking what
      // it IS, and the definition is the answer. Deliberately overrides an
      // explicit `include_code: false` rather than deferring to it, because
      // parity is the point -- the same request must mean the same thing on
      // both surfaces. The flag still decides every unpinned call.
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
 * The distinct vector-cache row keys the request's scope admits, as the
 * triples that cache is keyed on.
 *
 * A row is one (kind, source, name); several live entities can share it, one
 * per model path. A row is in scope when ANY of them is, so a model_path scope
 * keeps the row and the fan-out then keeps only that path's entity — dropping
 * the row here would take the pinned card with it.
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
