// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Source / query introspection from a compiled `ModelDef`, shared by
 * `Model.create` and the package-load worker so the two cannot drift. They
 * differ only in how they type the result (structurally identical shapes,
 * cast at each boundary) and in how they report a `#(filter)` parse failure
 * (`onParseError`).
 */

import {
   isJoined,
   isSourceDef,
   ModelDef,
   NamedModelObject,
   NamedQueryDef,
   type SourceDef,
   StructDef,
   TurtleDef,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   annotationTexts,
   modelAnnotations,
   notePositionKey,
   ownLevelNotes,
   ownLevelNoteTexts,
   type AnnotationNote,
} from "./annotations";
import {
   assertNoAuthorizeNearMisses,
   authorizeAnnotationRoute,
   collectAuthorizeExprsForRoute,
   collectAuthorizeNearMissesAllRoutes,
   containsAuthorizeAnnotationTag,
   type AuthorizeMap,
   type AuthorizeMapGroup,
   type AuthorizeOwnNotesMap,
   type MisplacedAuthorizeAnnotation,
} from "./authorize";
import {
   CANONICAL_AUTHORIZE_ROUTES,
   ACCESS_FILTER_ROUTE,
   AUTHORIZE_ROUTE,
} from "./authorize_routes";
import { modelInfoOf } from "./model_info";
import { parseFilters, type FilterDefinition } from "./filter";
import {
   derivedStructsReachable,
   effectiveAncestorGateExprs,
} from "./gate_registry_walk";

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
 * Structural source shape each caller casts to its own type. `givens` is
 * attached verbatim from the caller, so it stays `unknown` here.
 */
export interface ExtractedSource {
   name: string;
   annotations: string[] | undefined;
   views: ExtractedView[];
   filters: ExtractedFilter[] | undefined;
   givens: unknown;
   /**
    * Effective `#(authorize)` LOCK expressions: the source's own, or the
    * nearest ancestor's when it declares none, so an absent value really
    * means "not locked". Introspection only; enforcement reads
    * `authorizeMap`. A source gated only by `#(access_filter)` reports
    * undefined here, so "is this source gated at all" must read BOTH fields.
    */
   authorize: string[] | undefined;
   /** Effective `#(access_filter)` row-filter expressions, as `authorize`. */
   accessFilter: string[] | undefined;
}

export interface ExtractedQuery {
   name: string;
   sourceName: string | undefined;
   annotations: string[] | undefined;
}

/**
 * Whether a join field's `referenceID`/`sourceID` names a declaration THIS
 * model cannot resolve to a real source. That is the shape of a selective
 * import (joining `sal` without importing it) or a two-hop import (Malloy
 * merges only one import level): the id still names the true declaration,
 * but nothing in this model's registry resolves it. A field with neither id
 * set (an inline join target) returns false. Malloy sets at least one id; an
 * `extend`ed join sets only `sourceID`.
 *
 * A wrong `true` can only suppress a misplaced-annotation refusal, never
 * hide an enforcement gap: a join is never traced for authorize purposes.
 */
function joinFieldNamesUnresolvableDeclaration(
   field: { referenceID?: string; sourceID?: string },
   modelDef: ModelDef,
): boolean {
   const ids = [field.referenceID, field.sourceID].filter(
      (id): id is string => !!id,
   );
   if (ids.length === 0) return false;
   for (const id of ids) {
      const entry = modelDef.sourceRegistry?.[id]?.entry;
      if (!entry) continue;
      const declared =
         entry.type === "source_registry_reference"
            ? modelDef.contents[entry.name]
            : entry;
      if (declared && isSourceDef(declared)) return false;
   }
   return true;
}

/**
 * Whether `a` sits strictly before `b` in a document. `considerNoteOwner`
 * keeps the earliest candidate because iteration over `contents` /
 * `sourceRegistry` is not guaranteed to be declaration order.
 */
function isEarlierPosition(
   a: { line: number; character: number },
   b: { line: number; character: number },
): boolean {
   return a.line < b.line || (a.line === b.line && a.character < b.character);
}

/**
 * Record which top-level struct actually WROTE a shared annotation note, so
 * `attributedAuthorizeOwnNotes` does not mistake "carries the note" for
 * "declared the note" (see `validateAuthorizeProbes`: that decides whether a
 * bad derivation denies only itself or aborts the package load).
 *
 * Keyed by location because nothing else works: Malloy copies a base's whole
 * `annotations` object by reference onto a derivation that adds none of its
 * own, `resolveDeclaredSource` resolves no ancestor for extend-based
 * derivations, and fields are not shared by reference. But each note's `at`
 * names where it was PARSED, and each top-level struct's `location` where it
 * was declared, so the same-file candidate is the author; same-file ties go
 * to the earliest declaration (a source must exist before it is extended).
 * A candidate with no `location` is skipped; an unresolved note then reads
 * as inherited everywhere, which stays fail-closed.
 */
function considerNoteOwner(
   declaredBy: Map<AnnotationNote, StructDef>,
   note: AnnotationNote,
   candidate: StructDef,
): void {
   const location = candidate.location;
   // Raw string equality, not URL normalization: enough for file-loader URLs
   // and notebook cells' `internal://` URLs.
   if (!location || location.url !== note.at.url) return;
   const currentLocation = declaredBy.get(note)?.location;
   // Strict `<`, so on an identical position the first-visited candidate
   // wins (the `contents` sweep before `sourceRegistry`).
   if (
      !currentLocation ||
      isEarlierPosition(location.range.start, currentLocation.range.start)
   ) {
      declaredBy.set(note, candidate);
   }
}

/**
 * The `SourceInfo` for every source THIS model can resolve: `modelDef.contents`,
 * its own declarations plus exactly the names its imports selected (the same
 * set as Malloy's `Model.explores`). Not every source of each imported file,
 * which would report names that resolve nowhere here.
 *
 * `modelDefToModelInfo` emits only `exports`, and imported names are stored
 * `exported: false`, so `exports` is widened for the call
 * (`sourceDefToSourceInfo` is not exported).
 */
export function collectSourceInfos(modelDef: ModelDef): Malloy.SourceInfo[] {
   const names = Object.keys(modelDef.contents).filter((name) =>
      isSourceDef(modelDef.contents[name]),
   );
   const exports = [...new Set([...modelDef.exports, ...names])];
   return modelInfoOf({ ...modelDef, exports }).entries.filter(
      (entry) => entry.kind === "source",
   ) as Malloy.SourceInfo[];
}

/**
 * Extract every source from a compiled model, with its `#(filter)` and
 * authorize gates.
 *
 * Filters: the `annotations.inherits` chain is collected child-first then
 * reversed, so `parseFilters`' last-wins dedup lets a child override its
 * base. A filter parse error goes to `onParseError` and extraction
 * continues. The filter walk reads `blockNotes` only, so a block-form
 * `#(filter) ... required` is dropped; reading both note keys would start
 * rejecting requests live models serve, so that is a known limitation.
 *
 * Authorize: per route, the source's own gate, else the nearest ancestor's
 * via {@link effectiveAncestorGateExprs}. An extension that declares its own
 * gate replaces the base's (curated re-exposure); one that declares none
 * keeps it, so a locked base stays locked. Own `blockNotes` alone would miss
 * this: any annotation of the extension's own demotes the base's notes to
 * `annotations.inherits`. Within one level, which note key a declaration
 * lands in depends on syntax, so reads go through {@link ownLevelNoteTexts}.
 * Authorize parse errors propagate, failing the load, so a gate is never
 * silently dropped. `authorize`/`authorizeMap` is the EFFECTIVE gate: what
 * introspection reports (it must not understate) and what
 * `validateAuthorizeProbes` checks.
 *
 * `misplacedAuthorize` lists `#(authorize)` annotations in positions nothing
 * enforces: on a field (one line too low, or typed on a join line) or on the
 * model (`##(authorize)`). These fail OPEN, so the load refuses them (see
 * `assertNoMisplacedAuthorizeAnnotations`). An unannotated join of a gated
 * source carries Malloy's by-reference copy of that source's gate note,
 * which is not misplaced; see the join handling in the field loop.
 */
export function extractSourcesFromModelDef(
   modelDef: ModelDef,
   givens: unknown,
   onParseError?: (sourceName: string, err: unknown) => void,
): {
   sources: ExtractedSource[];
   filterMap: Map<string, FilterDefinition[]>;
   authorizeMap: AuthorizeMap;
   misplacedAuthorize: MisplacedAuthorizeAnnotation[];
   /**
    * source name → route → every authorize-tagged note on the source's own
    * annotation level, by PRESENCE. Feeds the load refusals
    * (`assertNoLegacyStringGate`, `assertAuthorizeGrammarValid`), which must
    * fire on what a source literally carries. Do NOT swap in
    * {@link attributedAuthorizeOwnNotes} there: attribution can credit another
    * source, turning a load refusal into a fail-open.
    */
   authorizeOwnNotes: AuthorizeOwnNotesMap;
   /**
    * The subset of {@link authorizeOwnNotes} this struct actually WROTE (see
    * `considerNoteOwner`). Feeds only `validateAuthorizeProbes`' choice
    * between throwing (the declaring source's mistake) and warning (a
    * derivation inheriting a gate it cannot express). Never use it for a load
    * refusal.
    */
   attributedAuthorizeOwnNotes: AuthorizeOwnNotesMap;
} {
   const filterMap = new Map<string, FilterDefinition[]>();
   const authorizeMap: AuthorizeMap = new Map();
   const misplacedAuthorize: MisplacedAuthorizeAnnotation[] = [];

   // Every authorize-tagged note that is some source's OWN annotation, across
   // the whole model. An unannotated join of a gated source carries that
   // source's note BY REFERENCE, byte-identical to a misplaced one; this set
   // tells them apart (an author's stray annotation is a fresh note that
   // appears nowhere else). Model-wide, because a join field's copied note
   // belongs to the JOINED source, not the joiner.
   const gatedSourceOwnAuthorizeNotes = new Set<AnnotationNote>();
   // The same notes by parse position, for a file compiled more than once (a
   // diamond import), where each compile yields its own note objects and
   // identity alone would misread a copy as typed on the join line. Position
   // is unique because one compile never parses two texts under one URL
   // (synthetic compiles get their own `internal://<method>/<uuid>`); the
   // text is folded in as redundancy.
   const gatedSourceOwnAuthorizeNoteKeys = new Set<string>();
   const noteKey = (note: AnnotationNote): string =>
      `${notePositionKey(note.at)}#${note.text}`;
   const isGatedSourceOwnNote = (note: AnnotationNote): boolean =>
      gatedSourceOwnAuthorizeNotes.has(note) ||
      gatedSourceOwnAuthorizeNoteKeys.has(noteKey(note));
   // Each note resolved to the one struct that wrote it (`considerNoteOwner`).
   const authorizeNoteDeclaredBy = new Map<AnnotationNote, StructDef>();
   // Every position read for authorize is also swept for a NEAR-MISS spelling
   // that Malloy routes elsewhere, so nothing would enforce it. Refused first,
   // so the author sees the spelling error rather than a downstream one.
   const nearMissAuthorize: string[] = [];
   // Roots of the near-miss sweep, extended below by derivation hops.
   const sweptStructs: SourceDef[] = [];
   for (const obj of Object.values(modelDef.contents)) {
      if (!isSourceDef(obj)) continue;
      const struct = obj as StructDef;
      sweptStructs.push(obj);
      for (const note of ownLevelNotes(struct.annotations)) {
         if (containsAuthorizeAnnotationTag([note.text])) {
            gatedSourceOwnAuthorizeNotes.add(note);
            gatedSourceOwnAuthorizeNoteKeys.add(noteKey(note));
            considerNoteOwner(authorizeNoteDeclaredBy, note, struct);
         }
      }
      nearMissAuthorize.push(
         ...collectAuthorizeNearMissesAllRoutes(
            [
               ...ownLevelNotes(struct.annotations),
               // A near miss one line too low is doubly unenforced, and the
               // spelling is the more actionable of the two mistakes to report.
               ...struct.fields.flatMap((field) =>
                  ownLevelNotes(field.annotations),
               ),
            ].map((note) => note.text),
         ),
      );
   }
   // Inline `SourceDef` registry entries can be reachable from a join while
   // absent from `contents`. Without them here, such a source would count as
   // resolvable yet its gate note would be missing from the set, falsely
   // failing the misplaced-join check below.
   for (const value of Object.values(modelDef.sourceRegistry ?? {})) {
      const entry = value.entry;
      if (entry.type === "source_registry_reference") continue;
      if (!isSourceDef(entry)) continue;
      sweptStructs.push(entry);
      for (const note of ownLevelNotes((entry as StructDef).annotations)) {
         if (containsAuthorizeAnnotationTag([note.text])) {
            gatedSourceOwnAuthorizeNotes.add(note);
            gatedSourceOwnAuthorizeNoteKeys.add(noteKey(note));
            considerNoteOwner(
               authorizeNoteDeclaredBy,
               note,
               entry as StructDef,
            );
         }
      }
      nearMissAuthorize.push(
         ...collectAuthorizeNearMissesAllRoutes(
            ownLevelNotes((entry as StructDef).annotations).map(
               (note) => note.text,
            ),
         ),
      );
   }
   // A struct reached only by a derivation hop (a `query_source`'s imported
   // base, a composite member) still has its gate read by
   // `effectiveAncestorGateExprs`, so sweep it for near misses too.
   for (const struct of derivedStructsReachable(sweptStructs, modelDef)) {
      nearMissAuthorize.push(
         ...collectAuthorizeNearMissesAllRoutes(
            [
               ...ownLevelNotes(struct.annotations),
               ...struct.fields.flatMap((field) =>
                  ownLevelNotes(field.annotations),
               ),
            ].map((note) => note.text),
         ),
      );
   }
   // The model's `##` notes, folded across imports as the `##(authorize)`
   // refusal below reads them.
   {
      const folded = modelAnnotations(modelDef);
      nearMissAuthorize.push(
         ...collectAuthorizeNearMissesAllRoutes(
            [...(folded.notes ?? []), ...(folded.blockNotes ?? [])].map(
               (note) => note.text,
            ),
         ),
      );
   }
   assertNoAuthorizeNearMisses(nearMissAuthorize);

   // See the return type's docs for these two maps.
   const authorizeOwnNotes: AuthorizeOwnNotesMap = new Map();
   const attributedAuthorizeOwnNotes: AuthorizeOwnNotesMap = new Map();

   // Nothing reads a model's own notes for authorize, so a `##(authorize)` is
   // always a misplacement. Folded across imports so one in an imported file
   // is refused too. At most one finding: a `"file"` finding has no name to
   // tell two apart.
   {
      const fileNoteTexts = ownLevelNoteTexts(modelAnnotations(modelDef));
      if (containsAuthorizeAnnotationTag(fileNoteTexts)) {
         misplacedAuthorize.push({
            kind: "file",
            route:
               fileNoteTexts
                  .map((text) => authorizeAnnotationRoute(text))
                  .find((r): r is string => r !== undefined) ??
               ACCESS_FILTER_ROUTE,
         });
      }
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

         // Own-wins-or-inherits independently PER ROUTE: an own note on one
         // route never sheds an inherited gate on the other.
         const ownNotes = ownLevelNoteTexts(struct.annotations);
         const ownAuthorizeNotes = ownLevelNotes(struct.annotations).filter(
            (note) => containsAuthorizeAnnotationTag([note.text]),
         );
         const ownNotesByRoute = new Map<string, AnnotationNote[]>();
         const attributedOwnNotesByRoute = new Map<string, AnnotationNote[]>();
         for (const route of CANONICAL_AUTHORIZE_ROUTES) {
            const notesForRoute = ownAuthorizeNotes.filter(
               (note) => authorizeAnnotationRoute(note.text) === route,
            );
            ownNotesByRoute.set(route, notesForRoute);
            // The gate itself is still read by text below: it genuinely
            // applies to an inheriting entry point; only the diagnostic
            // signal uses attribution.
            attributedOwnNotesByRoute.set(
               route,
               notesForRoute.filter(
                  (note) => authorizeNoteDeclaredBy.get(note) === struct,
               ),
            );
         }
         authorizeOwnNotes.set(sourceName, ownNotesByRoute);
         attributedAuthorizeOwnNotes.set(sourceName, attributedOwnNotesByRoute);
         // Uses the shared `gate_registry_walk`, not a hand walk of
         // `annotations.inherits`: a `query_source` (`Z is X -> {...}`) has no
         // `annotations`, and only the shared walk's `sourceRegistry` fallback
         // reaches `X`'s gate. Inherited gates may come as several groups
         // (one per declaring source, see `AuthorizeMap`); a source's own
         // notes on one route are always one group.
         const groupsForSource: AuthorizeMapGroup[] = [];
         // Per route, so each wire field reports only its own route.
         const routeGroupsByRoute = new Map<string, string[][]>();
         for (const route of CANONICAL_AUTHORIZE_ROUTES) {
            const ownExprsForRoute = collectAuthorizeExprsForRoute(
               ownNotes,
               route,
            );
            const groupsForRoute =
               ownExprsForRoute.length > 0
                  ? [ownExprsForRoute]
                  : effectiveAncestorGateExprs(
                       struct as SourceDef,
                       modelDef,
                       route,
                    );
            for (const exprs of groupsForRoute) {
               groupsForSource.push({ route, exprs });
            }
            routeGroupsByRoute.set(route, groupsForRoute);
         }
         if (groupsForSource.length > 0) {
            authorizeMap.set(sourceName, groupsForSource);
         }
         // The wire shape is a flat `string[]` per route.
         const routeGroupsToFlatWire = (
            groups: string[][] | undefined,
         ): string[] | undefined =>
            groups && groups.length > 0 ? groups.flat() : undefined;
         const authorize = routeGroupsToFlatWire(
            routeGroupsByRoute.get(AUTHORIZE_ROUTE),
         );
         const accessFilter = routeGroupsToFlatWire(
            routeGroupsByRoute.get(ACCESS_FILTER_ROUTE),
         );
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

         // `#(authorize)` gates only from the `source:` line, so any field's
         // own authorize note is misplaced, except Malloy's by-reference copy
         // on an unannotated join of a gated source (joins never carry a gate).
         for (const field of struct.fields) {
            const fieldAuthorizeNotes = ownLevelNotes(field.annotations).filter(
               (note) => containsAuthorizeAnnotationTag([note.text]),
            );
            if (fieldAuthorizeNotes.length === 0) continue;
            // Malloy's copy: every note is some source's own note, by identity
            // or parse position. One typed on the join line matches neither.
            if (fieldAuthorizeNotes.every(isGatedSourceOwnNote)) {
               continue;
            }
            const fieldName = field.as || field.name;
            // `isJoined` alone also matches array/record dimensions (nested
            // structs with a `join` marker), which have no joined source;
            // they fall through to the plain misplaced-field refusal.
            if (isJoined(field) && isSourceDef(field)) {
               const joinedStruct = field as unknown as {
                  referenceID?: string;
                  sourceID?: string;
               };
               if (
                  joinFieldNamesUnresolvableDeclaration(joinedStruct, modelDef)
               ) {
                  // Declared beyond this model's imports, so the own-note set
                  // cannot tell an authored note from Malloy's copy, and every
                  // legitimate cross-file join of a gated source lands here.
                  // Ignored silently: refusing or warning would fire on
                  // correct packages. Accepted cost: a gate typed on such a
                  // join line goes unenforced with no signal.
                  continue;
               }
               // Declared inside this model, so the own-note set is
               // authoritative: a mismatched note was typed on the join line,
               // and the gate it meant would silently vanish. Refuse it.
               misplacedAuthorize.push({
                  kind: "field",
                  name: sourceName,
                  fieldName,
                  route:
                     fieldAuthorizeNotes
                        .map((note) => authorizeAnnotationRoute(note.text))
                        .find((r): r is string => r !== undefined) ??
                     ACCESS_FILTER_ROUTE,
               });
               continue;
            }
            // On a dimension/measure/view it is never enforced: refuse the load
            // rather than let it silently protect nothing.
            misplacedAuthorize.push({
               kind: "field",
               name: sourceName,
               fieldName,
               route:
                  fieldAuthorizeNotes
                     .map((note) => authorizeAnnotationRoute(note.text))
                     .find((r): r is string => r !== undefined) ??
                  ACCESS_FILTER_ROUTE,
            });
         }

         return {
            name: sourceName,
            annotations,
            views,
            filters,
            givens,
            authorize,
            accessFilter,
         };
      });

   return {
      sources,
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
      attributedAuthorizeOwnNotes,
   };
}

/** Extract every named query from a compiled model. */
export function extractQueriesFromModelDef(modelDef: ModelDef): {
   queries: ExtractedQuery[];
   /** `#(authorize)` on a `query:` statement: the entry-point walk resolves a
    *  query through its `structRef` and never reads the query's own
    *  annotations, so it protects nothing and is refused. */
   misplacedAuthorize: MisplacedAuthorizeAnnotation[];
} {
   const isNamedQuery = (obj: NamedModelObject): obj is NamedQueryDef =>
      obj.type === "query";
   const namedQueries = Object.values(modelDef.contents).filter(isNamedQuery);
   const misplacedAuthorize: MisplacedAuthorizeAnnotation[] = namedQueries
      .filter((queryObj) =>
         containsAuthorizeAnnotationTag(
            ownLevelNoteTexts(queryObj.annotations),
         ),
      )
      .map((queryObj) => ({
         kind: "query" as const,
         name: queryObj.as || queryObj.name,
         route:
            ownLevelNoteTexts(queryObj.annotations)
               .map((text) => authorizeAnnotationRoute(text))
               .find((r): r is string => r !== undefined) ??
            ACCESS_FILTER_ROUTE,
      }));
   const queries: ExtractedQuery[] = namedQueries.map((queryObj) => ({
      name: queryObj.as || queryObj.name,
      sourceName:
         typeof queryObj.structRef === "string"
            ? queryObj.structRef
            : undefined,
      annotations: annotationTexts(queryObj.annotations),
   }));
   return { queries, misplacedAuthorize };
}
