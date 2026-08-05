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
    * file-level expressions first, then the source's own — or, when it declares
    * none, the nearest `extend` ancestor's. Undefined only when nothing gates the
    * source. Surfaced for introspection; enforcement happens server-side.
    *
    * "Effective" has to include the inherited case or this understates
    * protection, and a consumer treating an absent value as "unrestricted" — the
    * natural reading — gets it wrong for a locked base whose extension carries
    * any stray annotation.
    */
   authorize: string[] | undefined;
}

/**
 * A source paired with the authorize expressions DECLARED on it (file-level ++
 * its own), which is the input `validateAuthorizeProbes` wants. Deliberately not
 * part of {@link ExtractedSource}: that object is cast to the wire/API `Source`
 * shape and serialized, and this is an internal load-time detail, not a field the
 * API promises.
 */
export interface OwnAuthorizeSource {
   name: string;
   authorize: string[];
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
 * Authorize (`#(authorize)` / `##(authorize)`) is collected own-gate-first and
 * then from the nearest `inherits` ancestor that declares one. For
 * `X is Y extend {...}`: if X declares its own `#(authorize)`, that replaces
 * Y's (the intended "curated re-exposure"); if X declares none, Y's gate
 * carries to X — a locked base stays locked unless an extension explicitly
 * re-exposes itself.
 *
 * Reading X's own `blockNotes` alone is NOT sufficient for that second case, and
 * used to be all this did. Malloy surfaces Y's blockNotes on X only while X
 * carries no annotation of its OWN; any annotation — a render tag, a doc
 * comment, an unrelated `#(filter)` — demotes Y's to `annotations.inherits`, at
 * which point own-blockNotes reports a locked source as ungated. So the chain is
 * walked, nearest declaration wins, matching `Model.gateExprsForOwnAnnotations`.
 * Joins are a separate concern and are not gated.
 *
 * Two lists come out of this, and the distinction is load-bearing:
 *  - `authorize` is the EFFECTIVE gate (file-level `##(authorize)` from
 *    `modelDef.annotations.notes`, then own-or-inherited), evaluated as one OR
 *    disjunction at request time. This is what introspection reports, so it must
 *    not understate what gates a source.
 *  - `ownAuthorizeSources` is the subset DECLARED here, and is the only thing
 *    handed to `validateAuthorizeProbes` — see its note on why load-time
 *    validation deliberately does not widen to inherited gates.
 */
export function extractSourcesFromModelDef(
   modelDef: ModelDef,
   givens: unknown,
   onParseError?: (sourceName: string, err: unknown) => void,
): {
   sources: ExtractedSource[];
   filterMap: Map<string, FilterDefinition[]>;
   authorizeMap: AuthorizeMap;
   ownAuthorizeSources: OwnAuthorizeSource[];
} {
   const filterMap = new Map<string, FilterDefinition[]>();
   const authorizeMap: AuthorizeMap = new Map();
   const ownAuthorizeSources: OwnAuthorizeSource[] = [];

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

         // Authorize: own gate if this source declares one, else the nearest
         // ancestor's (see the header note on why own-blockNotes alone is not
         // enough). File-level ##(authorize) is prepended either way so file
         // gates and source gates form one OR disjunction. A malformed
         // annotation propagates (model fails to load) rather than silently
         // dropping the gate — see the file-level note above.
         const ownNotes = (struct.annotations?.blockNotes ?? []).map(
            (note) => note.text,
         );
         const ownGates = collectAuthorizeExprs(ownNotes);
         let inheritedGates: string[] = [];
         if (ownGates.length === 0) {
            for (
               let cur = struct.annotations?.inherits;
               cur;
               cur = cur.inherits
            ) {
               const exprs = collectAuthorizeExprs(
                  (cur.blockNotes ?? []).map((note) => note.text),
               );
               if (exprs.length > 0) {
                  inheritedGates = exprs;
                  break;
               }
            }
         }
         const effective = [
            ...fileLevelAuthorize,
            ...(ownGates.length > 0 ? ownGates : inheritedGates),
         ];
         let authorize: string[] | undefined;
         if (effective.length > 0) {
            authorizeMap.set(sourceName, effective);
            authorize = effective;
         }
         // Validation scope: file-level ++ own only. An inherited gate is NOT
         // added — it belongs to the base, whose own given namespace may not be
         // reachable from here (Malloy merges one import level, so a base two or
         // more hops away can reference a given this model cannot see). Probing
         // it against THIS model would report a perfectly good annotation as
         // invalid and fail the load with a 424. It still fails CLOSED at request
         // time: `Model.gateExprsForOwnAnnotations` turns a parse failure into a
         // single unsatisfiable `"false"`.
         const ownEffective = [...fileLevelAuthorize, ...ownGates];
         if (ownEffective.length > 0) {
            ownAuthorizeSources.push({
               name: sourceName,
               authorize: ownEffective,
            });
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

   return { sources, filterMap, authorizeMap, ownAuthorizeSources };
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
