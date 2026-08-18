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
   isJoined,
   isSourceDef,
   ModelDef,
   NamedModelObject,
   NamedQueryDef,
   type SourceDef,
   StructDef,
   TurtleDef,
} from "@malloydata/malloy";
import {
   annotationTexts,
   modelAnnotations,
   ownLevelNotes,
   ownLevelNoteTexts,
   type AnnotationNote,
} from "./annotations";
import {
   assertNoAuthorizeNearMisses,
   collectAuthorizeExprs,
   collectAuthorizeNearMisses,
   containsAuthorizeAnnotationTag,
   type AuthorizeMap,
   type MisplacedAuthorizeAnnotation,
} from "./authorize";
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
    * Effective `#(authorize)` expressions gating this source: its own — or,
    * when it declares none, the nearest `extend` ancestor's. Undefined only
    * when nothing gates the source. Surfaced for introspection; enforcement
    * happens server-side.
    *
    * "Effective" has to include the inherited case or this understates
    * protection, and a consumer treating an absent value as "unrestricted" — the
    * natural reading — gets it wrong for a locked base whose extension carries
    * any stray annotation.
    */
   authorize: string[] | undefined;
}

export interface ExtractedQuery {
   name: string;
   sourceName: string | undefined;
   annotations: string[] | undefined;
}

/**
 * Whether a `join_one:`/`join_many:` field names a declaration (its own
 * `referenceID`/`sourceID`) that THIS model cannot resolve to a real source —
 * no `modelDef.sourceRegistry` entry for either id, or an entry that names
 * something absent from `modelDef.contents`.
 *
 * This is the exact shape a selective one-hop import (`import { mid } from
 * "a.malloy"`, joining `sal` without also importing it) or a two-hop
 * transitive import (Malloy merges only ONE import level, so a source two
 * hops away never enters `modelDef.contents` at all) produces: the joined
 * struct's `referenceID` or `sourceID` still names its true declaration (e.g.
 * `"sal@file:///a.malloy"`) — Malloy sets at least one of the two (an
 * `extend`ed join, `join_one: sal2 is sal extend {…}`, sets only `sourceID`,
 * leaving `referenceID` undefined) — but nothing in THIS model's registry
 * resolves it, because the declaration lives beyond what this model imported.
 * A field with NEITHER id set (an inline, unnamed join target) returns
 * `false`: there is no declaration to have gone missing, so this is not that
 * shape.
 *
 * False NEGATIVE cost: an author-written `#(authorize)` on a join line whose
 * `referenceID` HAPPENS to collide with an unresolvable id for some other
 * reason (never observed, but not provably impossible given Malloy's IR is
 * read structurally here, not via a public API) would be silently dropped
 * rather than warned. Bounded and one-directional — it can only suppress a
 * warning, never fail a load or hide an enforcement gap, since a join is
 * never traced for authorize purposes regardless of this check's answer.
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
 * Authorize (`#(authorize)`) is collected own-gate-first and then from the
 * nearest ancestor that declares one, via {@link effectiveAncestorGateExprs}
 * (`./gate_registry_walk`) — its `annotations.inherits`/`sourceRegistry` half
 * is shared verbatim with `Model.gateExprsForOwnAnnotations`; its
 * `query_source` hop is NOT (see that module's doc comment for why
 * `Model.collectEntryPointGates` takes it as its own separate, already-tested
 * recursion instead). For `X is Y extend {...}`: if X declares its own
 * `#(authorize)`, that replaces Y's (the intended "curated re-exposure"); if X
 * declares none, Y's gate carries to X — a locked base stays locked unless an
 * extension explicitly re-exposes itself. The same holds for `X is Y ->
 * {...}` (a query source): if X's own projection carries no annotation, Y's
 * gate carries to X.
 *
 * Reading X's own `blockNotes` alone is NOT sufficient for that second case, and
 * used to be all this did. Malloy surfaces Y's blockNotes on X only while X
 * carries no annotation of its OWN; any annotation — a render tag, a doc
 * comment, an unrelated `#(filter)` — demotes Y's to `annotations.inherits`, at
 * which point own-blockNotes reports a locked source as ungated. So the chain is
 * walked, nearest declaration wins, matching `Model.gateExprsForOwnAnnotations`.
 * Joins are a separate concern and are not gated.
 *
 * `blockNotes` alone is also not sufficient WITHIN one level, because which
 * note key a declaration lands in is decided by the author's syntax. The
 * authorize reads below therefore go through {@link ownLevelNoteTexts}, which
 * covers both keys.
 *
 * The `#(filter)` walk deliberately still reads `blockNotes` only. It has the
 * same live gap: a block-form `#(filter) ... required` is dropped, so
 * `buildFilterClause` never raises the "required filter not provided" error the
 * author asked for. Closing it would start rejecting requests that live models
 * have been serving, so it is left for a change that can carry that break.
 *
 * `authorize` (and its `authorizeMap` twin) is the EFFECTIVE gate
 * (own-or-inherited), evaluated as one OR disjunction at request time. It is
 * both what introspection reports — so it must not understate what gates a
 * source — and what `validateAuthorizeProbes` validates, per entry point;
 * `authorizeOwnNotes` is the companion that tells it which of those entry
 * points DECLARED the gate rather than inheriting it.
 *
 * A separate list, `misplacedAuthorize`, is not about a source's gate at all: it
 * is every `#(authorize)` annotation this walk found attached to a
 * `dimension:`/`measure:`/`view:` FIELD rather than the `source:` line itself
 * — a position nothing here, or anywhere else, ever reads for enforcement.
 * See `assertNoMisplacedAuthorizeAnnotations`'s doc for why that fails OPEN
 * and has to be refused at load rather than silently ignored. An unannotated
 * `join_one:`/`join_many:` of a gated source is deliberately EXCLUDED from
 * this list, even though it carries the identical annotation text on the
 * join field — see the note-identity check just above the main loop. A
 * `##(authorize)` on the MODEL itself (file-level, deprecated) is also folded
 * into `misplacedAuthorize`, as a `"file"`-kind finding, rather than
 * collected as a gate — see the check ahead of the per-source walk below.
 *
 * A `join_one:`/`join_many:` FIELD carrying a note this walk cannot identify
 * as that copy is routed by whether its declaration resolves INSIDE this
 * model. When `joinFieldNamesUnresolvableDeclaration` says the declaration is
 * beyond this model's own `contents`/`sourceRegistry` — a source imported
 * selectively, or two import levels away (Malloy merges only one) — it is
 * silently dropped (never added to `misplacedAuthorize`, so it never fails
 * the load): this walk has twice proved unable to tell an authored join-line
 * annotation from Malloy's by-reference copy of a gate declared out of sight.
 * See that helper's doc for the exact test and what it costs in false
 * negatives. When the declaration DOES resolve inside this model,
 * `gatedSourceOwnAuthorizeNotes` is authoritative — an inherited copy would
 * have matched it — so a still-mismatched note is author-written and is
 * added to `misplacedAuthorize`, failing the load like any other misplaced
 * annotation: leaving it a warning would let the containing source's own
 * gate silently vanish (it lands on the field, which nothing enforces).
 *
 * Between those two branches there is nothing left over, which is why this no
 * longer reports a join-field warning at all: the undecidable case such a
 * warning used to carry is the unresolvable branch, and every legitimate
 * cross-file join of a gated source reaches it, so warning there fired on
 * correct packages (pinned by the cross-file tests).
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
   authorizeOwnNotes: Map<string, AnnotationNote[]>;
} {
   const filterMap = new Map<string, FilterDefinition[]>();
   const authorizeMap: AuthorizeMap = new Map();
   // `#(authorize)` written one line too low — on a `dimension:`/`measure:`/
   // `join_one:`/`view:` INSIDE a source rather than on the `source:` line
   // itself — lands on that FIELD's own annotations, which nothing walks for
   // authorize purposes; see `assertNoMisplacedAuthorizeAnnotations`'s doc.
   //
   // EXCEPT for the one shape that only looks like this: an unannotated
   // `join_one:`/`join_many:` of a gated source. Malloy embeds the joined
   // source as a nested `StructDef` on the join field, and copies that
   // source's own `#(authorize)` note object onto the join field's own
   // annotations BY REFERENCE whenever the join line adds no annotation of
   // its own — same mechanism as an unannotated `extend {}` (see
   // `gate_registry_walk.ts`'s module doc), just landing on a field instead
   // of a source. A gate on `salaries`, plainly joined by `join_one: salaries
   // on …` with no annotation of its own, therefore surfaces on the join
   // field's `annotations` looking BYTE-IDENTICAL to a misplaced one —
   // `docs/authorize.md`'s own worked join example is exactly this shape.
   // `gatedSourceOwnAuthorizeNotes` below is the identity set that tells the
   // two apart: a copied note is (by reference) also some source's OWN note
   // somewhere in this model; an author's stray annotation is a freshly
   // parsed note object that appears nowhere else.
   const misplacedAuthorize: MisplacedAuthorizeAnnotation[] = [];

   // Every `#(authorize)`-tagged note object that is some source's OWN
   // annotation (blockNotes/notes at that source's own level), across the
   // whole model. Built once, up front: the misplaced-field scan below needs
   // to test EVERY field against EVERY source's own notes, not just the
   // source the field happens to live on — a join field's copied note names
   // the JOINED source, not the joiner.
   const gatedSourceOwnAuthorizeNotes = new Set<AnnotationNote>();
   // Every annotation position this function reads for authorize purposes is
   // also swept for a NEAR-MISS spelling — one that reads as an attempt at a
   // gate but that Malloy routes elsewhere, so nothing would ever enforce it.
   // Refused before anything else here runs, so the author gets the spelling
   // error rather than a downstream complaint about a gate this model does not
   // actually carry. See `collectAuthorizeNearMisses`.
   const nearMissAuthorize: string[] = [];
   // The roots of the near-miss sweep, so the derivation hops below can extend
   // it past what `contents` u `sourceRegistry` names.
   const sweptStructs: SourceDef[] = [];
   for (const obj of Object.values(modelDef.contents)) {
      if (!isSourceDef(obj)) continue;
      const struct = obj as StructDef;
      sweptStructs.push(obj);
      for (const note of ownLevelNotes(struct.annotations)) {
         if (containsAuthorizeAnnotationTag([note.text])) {
            gatedSourceOwnAuthorizeNotes.add(note);
         }
      }
      nearMissAuthorize.push(
         ...collectAuthorizeNearMisses(
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
   // Also walk `sourceRegistry` entries that are themselves an inline
   // `SourceDef` — a `source_registry_reference` names something already
   // covered by the `contents` loop above, but a non-reference entry IS the
   // declared source and can be reachable from a join's `referenceID`/
   // `sourceID` while being absent from `modelDef.contents` entirely (the
   // two-import-hops shape `joinFieldNamesUnresolvableDeclaration`'s doc
   // cites). Without this, such a source is simultaneously "resolvable" (its
   // registry entry IS a real `SourceDef`) and "identity-mismatched" (its own
   // gate note was never added to this set) — a false positive for the
   // resolvable-and-mismatched fatal check below.
   for (const value of Object.values(modelDef.sourceRegistry ?? {})) {
      const entry = value.entry;
      if (entry.type === "source_registry_reference") continue;
      if (!isSourceDef(entry)) continue;
      sweptStructs.push(entry);
      for (const note of ownLevelNotes((entry as StructDef).annotations)) {
         if (containsAuthorizeAnnotationTag([note.text])) {
            gatedSourceOwnAuthorizeNotes.add(note);
         }
      }
      nearMissAuthorize.push(
         ...collectAuthorizeNearMisses(
            ownLevelNotes((entry as StructDef).annotations).map(
               (note) => note.text,
            ),
         ),
      );
   }
   // A struct reached only by a derivation hop — a `query_source`'s imported
   // base, a composite's resolved member — is in neither collection above, and
   // its own gate is read (by `effectiveAncestorGateExprs`) all the same. Sweep
   // it too, or a near miss written there is silently the one spelling mistake
   // this refusal doesn't catch.
   for (const struct of derivedStructsReachable(sweptStructs, modelDef)) {
      nearMissAuthorize.push(
         ...collectAuthorizeNearMisses(
            [
               ...ownLevelNotes(struct.annotations),
               ...struct.fields.flatMap((field) =>
                  ownLevelNotes(field.annotations),
               ),
            ].map((note) => note.text),
         ),
      );
   }
   // The model's own `##` notes, folded across the import lineage exactly as the
   // file-level `##(authorize)` refusal below reads them — so `## (authorize)` in
   // an import is refused too, not just one written locally.
   {
      const folded = modelAnnotations(modelDef);
      nearMissAuthorize.push(
         ...collectAuthorizeNearMisses(
            [...(folded.notes ?? []), ...(folded.blockNotes ?? [])].map(
               (note) => note.text,
            ),
         ),
      );
   }
   assertNoAuthorizeNearMisses(nearMissAuthorize);

   // source name → the source's OWN-level `#(authorize)`-tagged note
   // objects (possibly empty — e.g. a `query_source` struct carries no
   // `annotations` at all, so it has none of its own by construction). Fed to
   // `validateAuthorizeProbes` so it can tell a gate genuinely DECLARED at one
   // entry point from one merely CARRIED there by Malloy's by-reference copy
   // — see that function's doc for why gate TEXT and source identity both
   // fail this test and object identity is what's left.
   const authorizeOwnNotes = new Map<string, AnnotationNote[]>();

   // A `##(authorize)` on the model itself (file-level) is deprecated: no
   // code path reads a model's own notes for authorize purposes any more, so
   // one found here is always a misplacement, never a gate — see
   // `MisplacedAuthorizeAnnotation`'s `"file"` kind and
   // `assertNoMisplacedAuthorizeAnnotations`'s refusal message. Read through
   // `modelAnnotations` (folded across the import lineage), matching where
   // this used to collect file-level gates from, so a `##(authorize)` in an
   // IMPORTED file is refused too — not just one written in the local file.
   // At most ONE finding however many `##(authorize)` notes are folded in: a
   // `"file"`-kind finding carries no name to tell two of them apart, so
   // pushing one per note would only repeat the same bullet line N times in
   // the refusal message.
   if (
      containsAuthorizeAnnotationTag(
         (modelAnnotations(modelDef).notes ?? []).map((note) => note.text),
      )
   ) {
      misplacedAuthorize.push({ kind: "file" });
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

         // Authorize: own gate if this source declares one, else the nearest
         // ancestor's (see the header note on why own-blockNotes alone is not
         // enough). A malformed annotation propagates (model fails to load)
         // rather than silently dropping the gate.
         const ownNotes = ownLevelNoteTexts(struct.annotations);
         const ownGates = collectAuthorizeExprs(ownNotes);
         authorizeOwnNotes.set(
            sourceName,
            ownLevelNotes(struct.annotations).filter((note) =>
               containsAuthorizeAnnotationTag([note.text]),
            ),
         );
         // Shared with `Model.gateExprsForOwnAnnotations` (`./gate_registry_walk`)
         // rather than walking `struct.annotations.inherits` by hand here: that
         // chain alone misses a `query_source` entry point (`Z is X -> {...}`),
         // whose compiled `StructDef` carries no `annotations` at all, so the
         // ONLY link back to `X`'s gate is the `sourceRegistry` fallback the
         // shared walk also follows. A hand-rolled copy that covered only the
         // annotations chain is exactly how `Z`'s gate went missing from this
         // extraction while still being enforced at request time.
         //
         // `inheritedGroups` may hold MORE THAN ONE group (a query-source base
         // gate and, separately, its composite-member's own gate) — see
         // `AuthorizeMap`'s doc for why those stay separate groups rather than
         // one concatenated list. `effectiveGroups` is always exactly one
         // group when `ownGates` is used: a source's own annotations are
         // genuinely one disjunction, grouping only ever splits gates that
         // came from different declaring sources.
         const inheritedGroups =
            ownGates.length === 0
               ? effectiveAncestorGateExprs(struct as SourceDef, modelDef)
               : [];
         const effectiveGroups =
            ownGates.length > 0 ? [ownGates] : inheritedGroups;
         let authorize: string[] | undefined;
         if (effectiveGroups.length > 0) {
            authorizeMap.set(sourceName, effectiveGroups);
            // The wire shape stays a flat `string[]` for introspection — see
            // `AuthorizeMap`'s doc. Flattening here (rather than keeping
            // groups on the wire) is safe because nothing downstream of this
            // field re-derives enforcement from it; only `authorizeMap`
            // (kept internal) drives `validateAuthorizeProbes`.
            authorize = effectiveGroups.flat();
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

         // Every field's OWN annotations, regardless of kind (dimension,
         // measure, join, view) — an author who meant to gate the whole
         // source but placed `#(authorize)` one line too low, in front of a
         // field instead of the `source:` line, gets an annotation Malloy
         // happily attaches and nothing ever reads.
         for (const field of struct.fields) {
            const fieldAuthorizeNotes = ownLevelNotes(field.annotations).filter(
               (note) => containsAuthorizeAnnotationTag([note.text]),
            );
            if (fieldAuthorizeNotes.length === 0) continue;
            // An unannotated `join_one:`/`join_many:` of a gated source: every
            // authorize-tagged note this field carries is (by reference) also
            // some source's OWN note elsewhere in the model — Malloy's copy,
            // not an annotation anyone wrote here. See the function header and
            // `gatedSourceOwnAuthorizeNotes`'s doc above. If the author instead
            // typed `#(authorize)` directly on the join line themselves, the
            // note is a freshly parsed object that matches nothing else, so
            // this still falls through and reports it.
            if (
               fieldAuthorizeNotes.every((note) =>
                  gatedSourceOwnAuthorizeNotes.has(note),
               )
            ) {
               continue;
            }
            const fieldName = field.as || field.name;
            // A join is never traced for authorize purposes either way (see
            // `assertNoMisplacedAuthorizeAnnotations`'s message), so gating
            // one has no effect regardless of whether the note here is
            // authored or copied. But the false-positive cost of GUESSING
            // "author-written" wrong when the declaration is beyond this
            // model's own visibility is a whole package refusing to load,
            // twice proved for real import shapes (see the function doc) — so
            // ONLY a note this walk cannot resolve to a declaration inside
            // this model is let through (`joinFieldNamesUnresolvableDeclaration`
            // below); a resolvable one falls through to the same fatal
            // `misplacedAuthorize` every other misplaced annotation gets,
            // because there the identity check is authoritative and a
            // mismatch means the annotation really is author-written here.
            //
            // `isJoined` alone is not enough: it is `'join' in def`, which is
            // also true of an array- or record-typed dimension (Malloy's IR
            // represents both as a nested struct with a `join` marker), and
            // neither is a `join_one:`/`join_many:` line — there is no JOINED
            // source for the fatal message's remedy to name. Require
            // `isSourceDef` too, so only an actual join field takes the warn
            // path; an array/record dimension falls through to the same
            // fatal `misplacedAuthorize` every other misplaced `dimension:`
            // annotation gets.
            if (isJoined(field) && isSourceDef(field)) {
               const joinedStruct = field as unknown as {
                  referenceID?: string;
                  sourceID?: string;
               };
               if (
                  joinFieldNamesUnresolvableDeclaration(joinedStruct, modelDef)
               ) {
                  // Names a declaration (a `referenceID`/`sourceID`) that
                  // resolves neither in `modelDef.contents` nor
                  // `modelDef.sourceRegistry` — beyond an import boundary
                  // this model's registry doesn't reach (a selective import
                  // of just the joiner, or a source two-plus hops away).
                  // Presumed Malloy's copy of a gate declared out of sight,
                  // not authored here — see the helper's doc. Ignored
                  // SILENTLY, not warned: the identity set cannot speak for a
                  // declaration it never saw, and every legitimate cross-file
                  // join of a gated source lands here, so a warning would fire
                  // on correct packages as a matter of course. The residual
                  // cost is real and accepted — a gate the author DID type on
                  // such a join line goes unenforced with no signal.
                  continue;
               }
               // The join's declaration resolves INSIDE this model (in
               // `contents` or `sourceRegistry`), so `gatedSourceOwnAuthorizeNotes`
               // is authoritative for it — an inherited (by-reference) copy
               // would have matched there. A note that still doesn't match is
               // freshly parsed, i.e. author-written directly on the join
               // line: the containing source's own gate silently vanishes
               // (it lands on the field, which nothing enforces) unless this
               // fails the load like every other misplaced annotation.
               misplacedAuthorize.push({
                  kind: "field",
                  name: sourceName,
                  fieldName,
               });
               continue;
            }
            misplacedAuthorize.push({
               kind: "field",
               name: sourceName,
               fieldName,
            });
         }

         return {
            name: sourceName,
            annotations,
            views,
            filters,
            givens,
            authorize,
         };
      });

   return {
      sources,
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
   };
}

/** Extract every named query from a compiled model. */
export function extractQueriesFromModelDef(modelDef: ModelDef): {
   queries: ExtractedQuery[];
   /** A `#(authorize)` annotation on the `query:` statement itself — see
    *  `assertNoMisplacedAuthorizeAnnotations`'s doc: Malloy attaches it to the
    *  `NamedQueryDef`, but the entry-point walk resolves a run target through
    *  the compiled query's `structRef` (the source it derives from) and never
    *  reads the query's OWN annotations, so it protects nothing. */
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
