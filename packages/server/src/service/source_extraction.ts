/**
 * Shared source / query introspection extracted from a compiled `ModelDef`.
 *
 * Both the in-process `Model.create` path (`service/model.ts`) and the
 * package-load worker (`package_load/package_load_worker.ts`, which runs in a
 * separate bundle and serializes the result over the worker protocol) need to
 * walk a `ModelDef` and produce the same `sources` / `queries` shapes plus the
 * `#(filter)` `filterMap`. This module is the single source of truth for that
 * walk so the two call sites can't drift out of lockstep — the two callers
 * differ only in how they type the result
 * (generated API types vs. worker wire types — structurally identical, so each
 * casts at its boundary) and in how they report a filter parse failure (the
 * service logs a warning; the worker has no logger and stays silent), which is
 * threaded through the optional `onParseError` callback.
 */

import {
   isSourceDef,
   ModelDef,
   NamedModelObject,
   NamedQueryDef,
   StructDef,
   TurtleDef,
} from "@malloydata/malloy";
import { annotationTexts, modelAnnotations } from "./annotations";
import { collectAuthorizeExprs, type AuthorizeMap } from "./authorize";
import { parseFilters, type FilterDefinition } from "./filter";

/** A `#(filter)` definition enriched with the dimension's Malloy type. */
export interface ExtractedFilter {
   name: string;
   dimension: string;
   type: string;
   implicit: boolean;
   required: boolean;
   dimensionType: string | undefined;
}

export interface ExtractedView {
   name: string;
   annotations: string[] | undefined;
}

/**
 * Structural source shape both callers cast to their own typed view
 * (`ApiSource` in the service, `ApiSourceWire` in the worker). `givens` is
 * attached verbatim from the caller-supplied list, so it stays `unknown` here.
 */
export interface ExtractedSource {
   name: string;
   annotations: string[] | undefined;
   views: ExtractedView[];
   filters: ExtractedFilter[] | undefined;
   givens: unknown;
   /**
    * Effective `#(authorize)` / `##(authorize)` expressions gating this source:
    * file-level expressions first, then the source's own. Undefined when the
    * source carries no authorize annotations. Surfaced for introspection;
    * enforcement happens server-side.
    */
   authorize: string[] | undefined;
}

export interface ExtractedQuery {
   name: string;
   sourceName: string | undefined;
   annotations: string[] | undefined;
}

/**
 * Extract every source from a compiled model, parsing `#(filter)` annotations
 * along the way.
 *
 * Filters are collected by walking the `annotations.inherits` chain so that
 * filters declared on a base source flow to an extending source. The chain runs
 * child → parent, so we collect child-first then reverse — `parseFilters` uses
 * "last wins" dedup, which lets a child's `#(filter)` override the base's.
 *
 * `givens` is attached unchanged to every source (Malloy exposes givens at the
 * model level, not per-source). `onParseError`, when supplied, is invoked with
 * the source name and error if a source's `#(filter)` annotations fail to parse;
 * filter extraction then continues. Authorize parse errors are NOT routed here —
 * they propagate (a malformed gate fails model load) so a security gate is never
 * silently dropped.
 *
 * Authorize (`#(authorize)` / `##(authorize)`) is collected from the source's
 * own `blockNotes` only — we do NOT walk the `inherits` chain. Note Malloy's
 * behavior for `X is Y extend {...}`: if X declares its own `#(authorize)`,
 * X.blockNotes holds only X's gates (Y's are dropped — the intended "curated
 * re-exposure"); if X declares none, Malloy surfaces Y's blockNotes on X, so
 * the base gate carries to the un-annotated extension (a safe default — a
 * locked base stays locked unless an extension explicitly re-exposes itself).
 * This carry happens through `blockNotes`, not the `inherits` chain, so reading
 * own-blockNotes is sufficient. Joins are a separate concern and are not gated.
 * The effective list per source is the file-level `##(authorize)` expressions
 * (from `modelDef.annotations.notes`) followed by the source's own
 * `#(authorize)` expressions, evaluated as one OR disjunction at request time.
 */
export function extractSourcesFromModelDef(
   modelDef: ModelDef,
   givens: unknown,
   onParseError?: (sourceName: string, err: unknown) => void,
): {
   sources: ExtractedSource[];
   filterMap: Map<string, FilterDefinition[]>;
   authorizeMap: AuthorizeMap;
} {
   const filterMap = new Map<string, FilterDefinition[]>();
   const authorizeMap: AuthorizeMap = new Map();

   // File-level ##(authorize) is collected once and prepended to every source.
   // Unlike filters, a malformed authorize annotation is NOT swallowed: the
   // parse error propagates so the model fails to load loudly (caught per-model
   // upstream and turned into a compilationError). Silently dropping a gate —
   // and in the worker path there is no onParseError callback, so it would be
   // truly silent — could leave a source that the author meant to lock looking
   // unrestricted.
   const fileLevelAuthorize = collectAuthorizeExprs(
      (modelAnnotations(modelDef).notes ?? []).map((note) => note.text),
   );

   const sources: ExtractedSource[] = Object.values(modelDef.contents)
      .filter((obj) => isSourceDef(obj))
      .map((sourceObj) => {
         const struct = sourceObj as StructDef;
         const sourceName = struct.as || struct.name;
         const annotations = annotationTexts(struct.annotations);

         const collected: string[][] = [];
         let cur = struct.annotations;
         while (cur) {
            if (cur.blockNotes) {
               collected.push(cur.blockNotes.map((note) => note.text));
            }
            cur = cur.inherits;
         }
         const allAnnotations = collected.reverse().flat();

         let filters: ExtractedFilter[] | undefined;
         if (allAnnotations.length > 0) {
            try {
               const parsed = parseFilters(allAnnotations);
               if (parsed.length > 0) {
                  filterMap.set(sourceName, parsed);
                  const fields = struct.fields;
                  filters = parsed.map((f) => {
                     const field = fields.find(
                        (fd) => (fd.as || fd.name) === f.dimension,
                     );
                     return {
                        name: f.name,
                        dimension: f.dimension,
                        type: f.type,
                        implicit: f.implicit,
                        required: f.required,
                        dimensionType: field?.type as string | undefined,
                     };
                  });
               }
            } catch (err) {
               onParseError?.(sourceName, err);
            }
         }

         // Authorize: the source's OWN #(authorize) annotations only — no
         // inherits walk. File-level ##(authorize) is prepended so file gates
         // and source gates form one OR disjunction. A malformed annotation
         // propagates (model fails to load) rather than silently dropping the
         // gate — see the file-level note above.
         const ownNotes = (struct.annotations?.blockNotes ?? []).map(
            (note) => note.text,
         );
         const effective = [
            ...fileLevelAuthorize,
            ...collectAuthorizeExprs(ownNotes),
         ];
         let authorize: string[] | undefined;
         if (effective.length > 0) {
            authorizeMap.set(sourceName, effective);
            authorize = effective;
         }

         const views: ExtractedView[] = struct.fields
            .filter((field) => field.type === "turtle")
            .filter((turtle) =>
               // Filter out non-reduce views (e.g. indexes).
               (turtle as TurtleDef).pipeline
                  .map((stage) => stage.type)
                  .every((type) => type === "reduce"),
            )
            .map((turtle) => ({
               name: turtle.as || turtle.name,
               annotations: annotationTexts(turtle.annotations),
            }));

         return {
            name: sourceName,
            annotations,
            views,
            filters,
            givens,
            authorize,
         };
      });

   return { sources, filterMap, authorizeMap };
}

/** Extract every named query from a compiled model. */
export function extractQueriesFromModelDef(
   modelDef: ModelDef,
): ExtractedQuery[] {
   const isNamedQuery = (obj: NamedModelObject): obj is NamedQueryDef =>
      obj.type === "query";
   return Object.values(modelDef.contents)
      .filter(isNamedQuery)
      .map((queryObj) => ({
         name: queryObj.as || queryObj.name,
         sourceName:
            typeof queryObj.structRef === "string"
               ? queryObj.structRef
               : undefined,
         annotations: annotationTexts(queryObj.annotations),
      }));
}
