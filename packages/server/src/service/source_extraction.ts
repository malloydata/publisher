/**
 * Shared source / query introspection extracted from a compiled `ModelDef`.
 *
 * Both the in-process `Model.create` path (`service/model.ts`) and the
 * package-load worker (`package_load/package_load_worker.ts`, which runs in a
 * separate bundle and serializes the result over the worker protocol) need to
 * walk a `ModelDef` and produce the same `sources` / `queries` shapes plus the
 * `#(filter)` `filterMap`. These two call sites used to carry byte-for-byte
 * copies of this logic; keeping them in lockstep by hand was a standing hazard
 * (a change to one silently diverged from the other). This module is the single
 * source of truth — the two callers differ only in how they type the result
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
import { annotationTexts } from "./annotations";
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
 * the source name and error if a source's filter or authorize annotations fail
 * to parse; extraction continues regardless.
 *
 * Authorize (`#(authorize)` / `##(authorize)`) is collected differently from
 * filters: it does NOT walk the `inherits` chain (a base source's gate is not
 * inherited by an extension — that is intentional, enforcement is top-level
 * only). The effective list per source is the file-level `##(authorize)`
 * expressions (from `modelDef.annotations.notes`) followed by the source's own
 * `#(authorize)` expressions (from its own `blockNotes`), evaluated as one OR
 * disjunction at request time.
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
   let fileLevelAuthorize: string[] = [];
   try {
      const fileNotes = (modelDef.annotations?.notes ?? []).map(
         (note) => note.text,
      );
      fileLevelAuthorize = collectAuthorizeExprs(fileNotes);
   } catch (err) {
      onParseError?.("(file-level)", err);
   }

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
         // and source gates form one OR disjunction.
         let authorize: string[] | undefined;
         try {
            const ownNotes = (struct.annotations?.blockNotes ?? []).map(
               (note) => note.text,
            );
            const effective = [
               ...fileLevelAuthorize,
               ...collectAuthorizeExprs(ownNotes),
            ];
            if (effective.length > 0) {
               authorizeMap.set(sourceName, effective);
               authorize = effective;
            }
         } catch (err) {
            onParseError?.(sourceName, err);
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
