// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Entry-point `#(authorize)` gate classification against a `{modelDef,
 * materializer}` pair, so a caller with no live `Model` (the build-plan
 * compile path) classifies gates the same way the request path does. `Model`
 * wraps these entry points, supplying its own long-lived caches (see
 * {@link GateClassificationDeps}).
 *
 * Also the load-time plumbing for the `#(authorize)` body grammar:
 * {@link assertAuthorizeGrammarValid} (the parser plus the fan-out join check
 * that needs the compiled struct) and {@link assertNoRetiredRouteMarkers} (a
 * retired route marker such as `#(partition)`, which Malloy still parses,
 * must fail load rather than fail open).
 */

import {
   isJoined,
   isSourceDef,
   type FilterCondition,
   type ModelDef,
   type ModelMaterializer,
   type SourceDef,
} from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";
import { logger } from "../logger";
import {
   annotationTexts,
   modelAnnotations,
   ownLevelNotes,
   ownLevelNoteTexts,
   type AnnotationNote,
   type AnnotationsDef,
} from "./annotations";
import {
   buildRowLevelProbe,
   collectAuthorizeExprsForRoute,
   gateFilterText,
   isLegacyQuotedPayload,
   liftProbeFilterCondition,
   referencedGivenNames,
   type AuthorizeMap,
   type AuthorizeOwnNotesMap,
   type RowLevelGateClassification,
   type RowLevelGateRejectionCause,
} from "./authorize";
import {
   assertAuthorizeGrammarTermsCoherent,
   AuthorizeGrammarError,
   containsRetiredRouteTag,
   parseAuthorizeGrammarBody,
   reachesRetiredRouteTagBelow,
   RETIRED_ROUTES,
   type AuthorizeGrammarRoutedTerm,
} from "./authorize_grammar";
import {
   AUTHORIZE_ROUTE,
   CANONICAL_AUTHORIZE_ROUTES,
} from "./authorize_routes";
import type { CallerJoinPath } from "./caller_joins";
import { expandRefSummaryGivenIds } from "./gate_dimension";
import {
   ANCESTOR_WALK_MAX_DEPTH,
   ancestorGateExprs,
   resolveDeclaredSource,
   resolveQuerySourceBase,
} from "./gate_registry_walk";

/** One reachable authorize gate found by {@link collectEntryPointGates}. */
export type GateEntry = {
   label: string;
   exprs: string[];
   /**
    * Whether this gate was authored somewhere other than the entry point it
    * applies to (a derivation base or `extend` ancestor). Its only reader is
    * `Model.collectAuthorizeEntryPointGates`'s dedup key, where it keeps two
    * same-text gates of different provenance from collapsing.
    */
   selfContained: boolean;
   /**
    * The route this gate was collected under (`CANONICAL_AUTHORIZE_ROUTES`):
    * `"access_filter"` (which rows) or `"authorize"` (may this caller reach the
    * source at all). {@link resolveGateShape} classifies an `authorize` entry
    * as a `lock` (decided, 403) and an `access_filter` entry as `row_level`
    * (grafted as a filter). Dedup keys include it so the routes never conflate.
    */
   route: string;
   /**
    * The entry point this gate applies to (the run target or its resolved
    * composite branch), not necessarily the struct the annotation was read
    * off. Absent only for the synthetic "unresolvable query-source base" deny
    * entry. {@link resolveGraftTarget} uses it to find where to graft.
    */
   struct?: SourceDef;
};

/**
 * The model def + materializer a row-level gate is classified, lifted and
 * grafted against. The query path uses the model's cumulative scope; a
 * notebook cell passes an earlier cell's scope or its own (see
 * `Model.graftScopeForCell` and friends).
 *
 * `cacheScope` namespaces the gate-shape and grafted-materializer caches: two
 * scopes that name a source identically must never share a classification or
 * a materializer grafted against the wrong modelDef.
 */
export interface GraftScope {
   modelDef: ModelDef;
   materializer: ModelMaterializer;
   cacheScope: string;
}

/**
 * Inputs {@link resolveGateShape} needs beyond its arguments, injected rather
 * than module state because callers need different cache lifetimes (`Model`
 * keeps one for its life; a build-time classification uses a fresh one).
 *
 * Build only through {@link createGateClassificationDeps}, and never reassign
 * a field. Cache entries are computed from `givenDeclaredTypes` but the key
 * carries no fingerprint of the given surface, so a cache paired with a
 * different given map would return a stale classification silently:
 * `row_level` where `rejected` is correct is fail-open. The factory mints the
 * two together so they cannot disagree.
 */
export interface GateClassificationDeps {
   /**
    * Shape memo, keyed
    * `${cacheScope}\u0000${route}\u0000${graftTarget}\u0000${filterText}`
    * (see `Model.gateShapeCache` for why `cacheScope` is in the key).
    *
    * `route` is in the key because both routes synthesize the same
    * fail-closed `["false"]` sentinel for a struct: without it a lock lookup
    * could find the row walk's entry and graft the lock as `where: false`
    * (200, zero rows) instead of refusing. Reversing
    * `CANONICAL_AUTHORIZE_ROUTES` must leave every lock test green.
    */
   gateShapeCache: Map<
      string,
      { classification: RowLevelGateClassification; condition: FilterCondition }
   >;
   /** See `Model.givenDeclaredTypes`. */
   givenDeclaredTypes: Map<string, string>;
   /** Debug-log context only; never read for a decision. */
   modelPath?: string;
}

/**
 * The one sanctioned way to build {@link GateClassificationDeps}: a fresh
 * `gateShapeCache` and the given-type map derived from the same `givens`.
 * `Model` calls it once (its givens are fixed for its life; a reload builds a
 * new `Model`); a build-time classification calls it per build.
 */
export function createGateClassificationDeps(
   givens: ReadonlyArray<{ name?: string; type?: string; default?: unknown }>,
   modelPath?: string,
): GateClassificationDeps {
   return {
      gateShapeCache: new Map(),
      givenDeclaredTypes: computeGivenDeclaredTypes(givens),
      modelPath,
   };
}

/**
 * Effective authorize exprs read off a struct's own annotations (else the
 * nearest ancestor's, via {@link ancestorGateExprs}). Used by the derivation
 * and composite walks, whose structs a `modelDef.contents` name lookup would
 * miss.
 *
 * Fails closed: load-time validation only probes top-level sources, so a
 * malformed gate on a non-top-level base or composite member reaches here
 * unvalidated. A parse failure returns `["false"]`, never "no gate". The
 * sentinel is synthesized on either route because each route decides
 * own-wins-over-ancestor independently and may reach the unreadable branch
 * when the other does not.
 *
 * `excludeNotes` is subtracted by identity from `struct`'s own notes: Malloy's
 * composite resolver copies a `query_source` base's note objects onto the
 * resolved member, and reading them would fold the base's gate into the
 * member's group instead of the separate AND'd entries
 * {@link collectEntryPointGates} produces. Empty except for that recursion.
 *
 * `fromAncestor` reports a gate from a derivation base (see `selfContained`).
 */
function gateExprsForOwnAnnotations(
   struct: SourceDef,
   route: string,
   modelDef?: ModelDef,
   excludeNotes: readonly AnnotationNote[] = [],
): {
   exprs: string[];
   fromAncestor: boolean;
} {
   // Both note keys: which one a gate lands in is decided by the author's
   // syntax, not by scope. See {@link ownLevelNoteTexts}.
   const ownNotes = ownLevelNotes(struct.annotations).filter(
      (note) => !excludeNotes.includes(note),
   );
   try {
      // The legacy quoted-string form is refused at load
      // (`assertNoLegacyStringGate`), but a caller without that preflight
      // (`build_plan.ts`) can still reach here with one. No special case is
      // needed: the payload is returned verbatim and fails as an ordinary
      // expression in `resolveGateShape`'s lift (see the comment there).
      const own = collectAuthorizeExprsForRoute(
         ownNotes.map((note) => note.text),
         route,
      );
      if (own.length > 0) {
         return { exprs: own, fromAncestor: false };
      }
      const ancestor = ancestorGateExprs(struct, modelDef, route);
      return { exprs: ancestor, fromAncestor: ancestor.length > 0 };
   } catch {
      return {
         exprs: ["false"],
         fromAncestor: false,
      };
   }
}

/**
 * The gates that apply to `struct` as an entry point:
 *  - its own `#(authorize)`, else the nearest `extend` ancestor's;
 *  - if query-derived (`source: x is y -> {...}`), the base's gates, since a
 *    `QuerySourceDef`'s own annotations do not carry `y`'s gate and
 *    `source: mine is locked -> {...}` would otherwise launder it away;
 *  - if that base is a composite, the member Malloy resolved for this
 *    derivation (`query.compositeResolvedSourceDef`).
 * Derivation recurses here, so chains are covered at any depth; `seen`
 * (struct identity) guards cycles.
 *
 * Not collected: joined sources, members of a joined composite, query-local
 * joins and a derivation's inner-pipeline joins. A gate governs the source it
 * is declared on, not what its author joined beneath it. Caller-written joins
 * are gated separately (`./caller_joins`, `Model.probeCallerJoinGates`).
 *
 * `excludeNotes`: see {@link gateExprsForOwnAnnotations}. Only the
 * composite-member recursion passes one.
 *
 * `treatAsOwnGate` is true only for the run target itself and its resolved
 * composite branch, the call sites that are the entry point, and marks those
 * entries not self-contained.
 *
 * Returns entries for both routes. Many consumers call this walk directly, so
 * a separate per-route collector one of them forgot to call would fail open.
 * Each route runs the whole walk ({@link collectEntryPointGatesForRoute})
 * with its own fresh `seen` and its own own-wins-over-ancestor decision, so
 * one route's note never sheds the other route's inherited gate.
 */
export function collectEntryPointGates(
   struct: SourceDef | undefined,
   modelDef: ModelDef | undefined,
   seen: Set<SourceDef> = new Set(),
   treatAsOwnGate = false,
   entryPointStruct: SourceDef | undefined = struct,
   excludeNotes: readonly AnnotationNote[] = [],
): GateEntry[] {
   const results: GateEntry[] = [];
   for (const route of CANONICAL_AUTHORIZE_ROUTES) {
      results.push(
         ...collectEntryPointGatesForRoute(
            struct,
            modelDef,
            route,
            new Set(seen),
            treatAsOwnGate,
            entryPointStruct,
            excludeNotes,
         ),
      );
   }
   return results;
}

/** The single-route walk {@link collectEntryPointGates} runs per route. */
function collectEntryPointGatesForRoute(
   struct: SourceDef | undefined,
   modelDef: ModelDef | undefined,
   route: string,
   // The struct that started this walk (run target or resolved composite
   // branch), held fixed through the recursion: it, not the struct a gate
   // was read off, is what {@link resolveGraftTarget} grafts onto. Grafting
   // the base instead cannot reach a model-declared derivation
   // (`Z is X -> {...}`), which snapshotted its base at declaration.
   seen: Set<SourceDef>,
   treatAsOwnGate: boolean,
   entryPointStruct: SourceDef | undefined,
   // Non-empty only for the composite-member recursion; see
   // {@link gateExprsForOwnAnnotations}.
   excludeNotes: readonly AnnotationNote[],
): GateEntry[] {
   if (!struct || !modelDef || seen.has(struct)) return [];
   seen.add(struct);

   const results: GateEntry[] = [];
   const label = (struct as { as?: string }).as ?? struct.name;
   const { exprs: ownExprs, fromAncestor } = gateExprsForOwnAnnotations(
      struct,
      route,
      modelDef,
      excludeNotes,
   );
   if (ownExprs.length > 0) {
      results.push({
         label,
         exprs: ownExprs,
         route,
         // A gate carried in from a derivation base counts as authored
         // elsewhere even when `struct` is the entry point.
         selfContained: fromAncestor || !treatAsOwnGate,
         // The entry point, not `struct`: where a row-level gate is grafted.
         struct: entryPointStruct,
      });
   }

   // Joined sources are deliberately not walked (see the doc above).

   const duck = struct as unknown as {
      type: string;
      query?: {
         structRef?: SourceDef | string;
         compositeResolvedSourceDef?: SourceDef;
      };
   };
   if (duck.type === "query_source") {
      // The lookup is shared with `effectiveAncestorGateExprs`; this recursion
      // (composite handling, `entryPointStruct` threading) is not.
      const base = resolveQuerySourceBase(struct, modelDef);
      if (base) {
         results.push(
            ...collectEntryPointGatesForRoute(
               base,
               modelDef,
               route,
               seen,
               false,
               entryPointStruct,
               [],
            ),
         );
      } else {
         // A `query_source` derives from something by construction, so an
         // unresolvable base is IR we failed to read, not an ungated source;
         // contributing nothing would launder the base's gate away. Deny, on
         // both routes (see `ancestorGateExprs`). An already-visited base
         // still resolves and takes the branch above.
         results.push({
            label,
            exprs: ["false"],
            route,
            selfContained: true,
         });
      }
      // A query-source's base may be a composite (`qs is compose(a, b) ->
      // {...}`); Malloy resolves it to one member for this derivation, on
      // `query.compositeResolvedSourceDef` (not `query.structRef`). Without
      // walking it, a query-source over a locked member would launder that
      // member's gate away. Recursing here covers any nesting depth.
      const resolved = duck.query?.compositeResolvedSourceDef;
      if (resolved) {
         results.push(
            ...collectEntryPointGatesForRoute(
               resolved,
               modelDef,
               route,
               seen,
               false,
               entryPointStruct,
               // Identity-subtract the base's own notes (see `excludeNotes`).
               base ? ownLevelNotes(base.annotations) : [],
            ),
         );
      }
      // The derivation's inner-pipeline joins are not walked either.
   }

   return results;
}

/**
 * Resolve one {@link GateEntry} to its enforcement shape: `row_level` (a row
 * filter with its graft target and lifted condition), `lock` (an `authorize`
 * route gate, decided per request), or `rejected` (fail closed: disallowed
 * shape or nowhere to graft). An entry without `struct` is the synthetic
 * unresolvable-base deny and is rejected outright.
 *
 * `filterText` folds the entry's conjunction into one expression,
 * `(e1) and (e2)`, so repeated gate terms are enforced as one filter with AND
 * semantics. `or` is refused in gate bodies, so a disjunction (e.g. an admin
 * override) is always two extension sources over a locked base, never a
 * second arm. A given-only conjunct is constant per request, so folding it in
 * changes nothing about which rows are admitted.
 *
 * Memoized in `deps.gateShapeCache` (see {@link GateClassificationDeps}); a
 * `rejected` result is cached, but a throw from the lift denies this call
 * only and is retried next time.
 *
 * The graft target is resolved against `graftScope.modelDef`, the stable model
 * the scope grafts onto and lifts through, never the caller's ephemeral
 * compiled `originModelDef`: that model mints fresh copies of every source
 * (a caller's `source: mine is X extend {}` becomes a real `contents` entry
 * only there), so its keys may not exist in the graft scope.
 * `originModelDef` is still what the ancestor walk uses, since only its
 * `sourceRegistry` links `entry.struct` to its bases. The two coincide on the
 * query path and differ for notebook cells.
 *
 * `graftScope` undefined (nothing to graft against) always rejects: a filter
 * with nowhere to attach cannot be enforced.
 */
export async function resolveGateShape(
   entry: GateEntry,
   originModelDef: ModelDef,
   graftScope: GraftScope | undefined,
   deps: GateClassificationDeps,
): Promise<
   | {
        shape: "row_level";
        graftTarget: string;
        filterText: string;
        condition: FilterCondition;
        /** The givens the gate compares. */
        givenNames: readonly string[];
     }
   | {
        shape: "lock";
        /** The compiled condition {@link decideLock} evaluates. */
        condition: FilterCondition;
        /** The givens the gate compares. */
        givenNames: readonly string[];
        /** Given id → name, resolved against the model the lift ran through. */
        givenNamesById: ReadonlyMap<string, string>;
     }
   | { shape: "rejected"; cause?: RowLevelGateRejectionCause }
> {
   if (!entry.struct) return { shape: "rejected" };
   if (!graftScope) {
      // No scope at all to graft against: for a notebook cell, no earlier code
      // cell and no compiled model of its own. The caller still gets the same
      // opaque 403 (it must not leak whether a gate exists); the log line
      // tells operators this case apart.
      logger.debug(
         "Row-level gate has no graft scope to attach to (no scope was available at all, not the gate's own condition); denying",
         { modelPath: deps.modelPath, label: entry.label },
      );
      return { shape: "rejected" };
   }

   const graftTarget = resolveGraftTarget(
      entry.struct,
      originModelDef,
      graftScope.modelDef,
   );
   // No key to graft onto: an ephemeral run target (an independently
   // recompiled `/compile` model, a notebook cell's `source: mine is
   // base_locked extend {…}`) can fail this. Both routes need the lifted
   // condition, so reject rather than fall back.
   if (!graftTarget) {
      return { shape: "rejected" };
   }
   const filterText = gateFilterText(entry.exprs);
   // `entry.route` is in the key although classification never reads it (the
   // route is applied after the memo). Both routes synthesize the same
   // `["false"]` sentinel for a struct, so they present identical
   // (scope, target, text); if classification ever read the route, a shared
   // entry would hand the lock the filter's answer (200, zero rows) instead
   // of a refusal.
   const cacheKey = `${graftScope.cacheScope}\u0000${entry.route}\u0000${graftTarget}\u0000${filterText}`;

   let cached = deps.gateShapeCache.get(cacheKey);
   if (!cached) {
      let condition: FilterCondition;
      try {
         condition = await liftGateCondition(
            graftTarget,
            filterText,
            graftScope.materializer,
         );
      } catch (err) {
         // A legacy quoted-string gate (`#(authorize) "org_id = 999"`) that
         // reaches here without a load-time preflight (`build_plan.ts`) lands
         // here: its payload compiles to a string literal, not a boolean, so
         // the lift throws and the gate is rejected. That keeps agreement with
         // `assertNoLegacyStringGate` structural rather than a special case.
         logger.debug("Row-level gate condition failed to lift; denying", {
            modelPath: deps.modelPath,
            graftTarget,
            error: err instanceof Error ? err.message : String(err),
         });
         return { shape: "rejected" };
      }
      // A bare `false` is the fail-closed sentinel the gate walks synthesize
      // when ancestry cannot be confirmed. It must still graft (as
      // `where: false`, deny everyone) rather than reject, which would throw
      // `AccessDeniedError` before the graft. Recognized by the lifted
      // condition's shape, which the lift has already compiled.
      let classification: RowLevelGateClassification;
      // `condition.e` is typed as always present, but a malformed prepared
      // query response must fail closed (403), not throw a TypeError (500).
      // Checked once here so both branches below can trust it.
      const hasUsableExpr =
         condition.e !== undefined &&
         condition.e !== null &&
         typeof condition.e === "object" &&
         typeof (condition.e as { node?: unknown }).node === "string";
      if (!hasUsableExpr) {
         classification = {
            shape: "rejected",
            cause: "unclassifiable_condition",
            detail:
               "this entry's lifted condition carries no usable expression",
         };
      } else if (
         isBareFalseLiteral(condition.e as { node: string; e?: unknown })
      ) {
         classification = {
            shape: "row_level",
            givenNames: [],
            givenNamesById: new Map(),
         };
      } else {
         // The source-line form: a compiled predicate tree. Its top-level
         // `refSummary.givenUsage` is not transitive: `#(authorize) authorized`
         // over `dimension: authorized is org_id in $GROUPS` carries only
         // `fieldUsage`. Reading `givenUsage` directly would under-count
         // `givenNames`; the gate still enforces, but the request-time
         // "gate given unbound; deny opaquely" backstop would miss it and an
         // unsupplied `$GROUPS` would surface as a raw compile error naming
         // the given instead of an opaque 403. So expand against the graft
         // target's struct in `graftScope.modelDef` (the model the lift ran
         // through; `originModelDef`'s given ids are minted per load and
         // would not match).
         //
         // Do not reject merely because `givenUsage` is absent: `1 = 1` reads
         // no given legitimately. Only an unresolvable path (`ok: false`)
         // denies.
         const targetStruct = graftScope.modelDef.contents[graftTarget];
         if (!isSourceDef(targetStruct)) {
            classification = {
               shape: "rejected",
               cause: "given_usage_unresolvable",
               detail:
                  "this gate's graft target does not resolve to a source on this model",
            };
         } else {
            const expansion = expandRefSummaryGivenIds(
               targetStruct,
               condition.refSummary,
            );
            if (!expansion.ok) {
               classification = {
                  shape: "rejected",
                  cause: "given_usage_unresolvable",
                  detail: `this gate references \`${expansion.unresolvedPath}\`, which could not be resolved on the graft target`,
               };
            } else {
               const givenNamesById = new Map<string, string>();
               for (const id of expansion.givenIds) {
                  const name = graftScope.modelDef.givens?.[id]?.name;
                  if (name) givenNamesById.set(id, name);
               }
               const givenNames = Array.from(givenNamesById.values());
               // Belt-and-braces: every `$NAME` literally in the filter text
               // must be among the resolved names; a mismatch means the IR
               // walk missed something, so fail closed.
               const literalNames = referencedGivenNames(filterText);
               const accountedFor = new Set(givenNames);
               const unaccounted = literalNames.filter(
                  (name) => !accountedFor.has(name),
               );
               if (
                  givenNames.length !== expansion.givenIds.size ||
                  unaccounted.length > 0
               ) {
                  classification = {
                     shape: "rejected",
                     cause: "unreachable_given",
                     detail:
                        "this gate references a given id that does not resolve to a name on this model",
                  };
               } else {
                  classification = {
                     shape: "row_level",
                     givenNames,
                     givenNamesById,
                  };
               }
            }
         }
      }
      // `Model.filterGivensToModelSurface` drops caller givens off this
      // model's surface, which is safe only if an accepted gate never
      // references one. Re-checked here rather than assumed.
      if (classification.shape === "row_level") {
         const unreachable = classification.givenNames.find(
            (name) => !deps.givenDeclaredTypes.has(name),
         );
         if (unreachable !== undefined) {
            logger.warn(
               "Gate accepted a given off the model surface; denying",
               {
                  modelPath: deps.modelPath,
                  graftTarget,
                  givenName: unreachable,
               },
            );
            classification = {
               shape: "rejected",
               cause: "unreachable_given",
               detail: `\`$${unreachable}\` is not on this model's given surface`,
            };
         }
      }
      cached = { classification, condition };
      deps.gateShapeCache.set(cacheKey, cached);
   }

   if (cached.classification.shape === "rejected") {
      return { shape: "rejected", cause: cached.classification.cause };
   }
   // The lift, expansion and surface check are shared; only the answer
   // differs by route.
   if (entry.route === AUTHORIZE_ROUTE) {
      return {
         shape: "lock",
         condition: cached.condition,
         givenNames: cached.classification.givenNames,
         givenNamesById: cached.classification.givenNamesById,
      };
   }
   return {
      shape: "row_level",
      graftTarget,
      filterText,
      condition: cached.condition,
      givenNames: cached.classification.givenNames,
   };
}

/**
 * The `modelDef.contents` key to graft a gate entry's condition onto, or
 * `undefined` (the caller denies).
 *
 * `struct` is the entry point of a gate that applies to this request (the run
 * target or its resolved composite branch, never the struct a chained gate
 * was read off), or a model source a caller-written join reaches. Grafting
 * propagates into every copy compiled from the grafted model afterward. That
 * is safe because the grafted materializer is built per request gate set and
 * only this request's text is recompiled against it; author joins embedded in
 * other model sources keep their load-time copies
 * (`Model.buildGraftedMaterializer`). Do not widen this to sources no gate of
 * this request reaches: that would fire gates on author joins that must not
 * fire. One author path is filtered (the safe direction): a join through a
 * named query over an imported source reads the snapshot
 * `Model.graftIntoNamedQuerySnapshots` grafts.
 *
 * A `struct` that is itself a `contents` entry is grafted directly. That
 * covers `Y is X extend {}` and `Z is X -> {...}`; `Z` must be grafted at
 * `Z`, not `X`, because its compiled `SourceDef` snapshotted `X` when defined.
 *
 * Otherwise `struct` is an ad-hoc caller-declared run target (`source: mine
 * is X extend {…}` + `run: mine`). Links are tried, nearest first, until one
 * lands on a `graftModelDef.contents` entry:
 *  - {@link resolveDeclaredSource}'s registry walk (a plain join or
 *    unmodified rename);
 *  - {@link findSourceByOwnAnnotationIdentity}: a trivial `extend {}` gets a
 *    self-referencing registry entry, so there is no link to follow, but
 *    Malloy copies `X`'s note objects onto `mine` by reference. The entry
 *    owning that same note object is the base; two independently authored
 *    blocks with identical text are distinct objects, so this cannot match
 *    the wrong source.
 *
 * The final key is looked up in `graftModelDef` (the stable model the graft
 * and lift compile against), never `originModelDef`: there `mine` is a real
 * entry and would be returned directly, though it does not exist in
 * `graftModelDef`. `originModelDef` is still what the walk reads, since only
 * it links `struct` to its base. The two are the same on the query path and
 * differ for notebook cells (see {@link GraftScope}).
 *
 * Matches by identity, then `sourceID`, never by name alone: distinct
 * `SourceDef`s can share a name across an inheritance chain.
 */
export function resolveGraftTarget(
   struct: SourceDef,
   originModelDef: ModelDef,
   graftModelDef: ModelDef,
): string | undefined {
   const direct = findContentsKey(struct, graftModelDef);
   if (direct) return direct;

   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const declared = resolveDeclaredSource(current, originModelDef);
      let next: SourceDef | undefined;
      if (declared.kind === "resolved" && !seen.has(declared.source)) {
         next = declared.source;
      } else if (declared.kind === "none") {
         next = findSourceByOwnAnnotationIdentity(current, graftModelDef, seen);
      }
      if (!next) return undefined;
      const key = findContentsKey(next, graftModelDef);
      if (key) return key;
      seen.add(next);
      current = next;
   }
   return undefined;
}

/** `modelDef.contents` key whose value IS `struct` (identity), else whose
 *  value shares `struct`'s `sourceID`, else `undefined`. */
function findContentsKey(
   struct: SourceDef,
   modelDef: ModelDef,
): string | undefined {
   for (const [key, value] of Object.entries(modelDef.contents)) {
      if (value === struct) return key;
   }
   if (struct.sourceID) {
      for (const [key, value] of Object.entries(modelDef.contents)) {
         if (isSourceDef(value) && value.sourceID === struct.sourceID) {
            return key;
         }
      }
   }
   return undefined;
}

/**
 * The `modelDef.contents` entry whose own notes include, by object reference
 * (never text), one of `struct`'s own notes (see {@link resolveGraftTarget}).
 * `exclude` holds structs already visited by the walk.
 */
export function findSourceByOwnAnnotationIdentity(
   struct: SourceDef,
   modelDef: ModelDef,
   exclude: Set<SourceDef>,
): SourceDef | undefined {
   const ownNotes = [
      ...(struct.annotations?.blockNotes ?? []),
      ...(struct.annotations?.notes ?? []),
   ];
   if (ownNotes.length === 0) return undefined;
   for (const value of Object.values(modelDef.contents)) {
      if (!isSourceDef(value) || value === struct || exclude.has(value)) {
         continue;
      }
      const candidateNotes = [
         ...(value.annotations?.blockNotes ?? []),
         ...(value.annotations?.notes ?? []),
      ];
      if (candidateNotes.some((note) => ownNotes.includes(note))) {
         return value;
      }
   }
   return undefined;
}

/**
 * Lift a gate's compiled `FilterCondition` by compiling a one-row probe that
 * applies `filterText` as a `where:` on `graftTarget`, through the same
 * materializer the graft targets.
 *
 * It must be that materializer, not a fresh `runtime.loadModel(...)`: each
 * load mints new given identities even for identical text, so a condition
 * lifted elsewhere would reference a given the real model never surfaced and
 * fail at request time. Hence callers supply a live materializer per call.
 *
 * Reading the last `filterList` entry is made safe by the checks in
 * {@link liftProbeFilterCondition} (see its doc). A failure throws, which the
 * caller turns into a deny.
 */
async function liftGateCondition(
   graftTarget: string,
   filterText: string,
   materializer: ModelMaterializer,
): Promise<FilterCondition> {
   // The probe shape and lift are shared with load-time validation: reading
   // the condition back from `filterList` only works while both probes stay
   // byte-identical, and the checks on that read must not drift apart.
   const probe = materializer.loadQuery(
      buildRowLevelProbe(graftTarget, filterText),
   );
   const prepared = (await probe.getPreparedQuery()) as {
      _query?: { structRef?: { filterList?: FilterCondition[] } };
   };
   return liftProbeFilterCondition(
      prepared,
      `lifted probe for "${graftTarget}"`,
      filterText,
   );
}

/**
 * Whether a compiled condition is a bare `false` literal (Malloy's node kind
 * `"false"`), unwrapping `()` only. A literal inside an author's boolean tree
 * is a live expression classified by the source-line branch instead.
 *
 * Deliberately `false`-only: the fail-closed sentinel is the only synthesized
 * bare literal, and accepting a bare `true` would admit whatever residual
 * shape produced it. A fallback that can only deny costs nothing to widen
 * later.
 */
function isBareFalseLiteral(expr: { node: string; e?: unknown }): boolean {
   let node: { node: string; e?: unknown } = expr;
   while (node.node === "()" && node.e && typeof node.e === "object") {
      node = node.e as { node: string; e?: unknown };
   }
   return node.node === "false";
}

/**
 * Given name → declared Malloy type, from a model's given surface. Used by
 * `resolveGateShape`'s re-check that an accepted gate never references a given
 * off this surface (see `Model.filterGivensToModelSurface`). Covers givens
 * within one import hop of the model only. Not memoized here; `Model` caches
 * it for its life.
 */
export function computeGivenDeclaredTypes(
   givens: ReadonlyArray<{ name?: string; type?: string }> | undefined,
): Map<string, string> {
   return new Map(
      (givens ?? [])
         .filter(
            (g): g is { name: string; type: string } =>
               g.name != null && g.type != null,
         )
         .map((g) => [g.name, g.type] as [string, string]),
   );
}

/** One resolved row-level filter, as {@link GateEntry}'s row-level
 *  classification produces it. */
export type RowLevelGraftEntry = {
   label: string;
   graftTarget: string;
   filterText: string;
   condition: FilterCondition;
   givenNames: readonly string[];
   /** Set for a caller join's gate: where the graft must be proven to land. */
   callerJoinPath?: CallerJoinPath;
};

/**
 * Validate every entry point's effective `#(authorize)` body against the
 * grammar (`parseAuthorizeGrammarBody`), then check each row-level term's
 * field path does not reach through a fan-out join
 * ({@link assertNoFanoutFieldPath}).
 *
 * Walks `authorizeMap` (own or inherited), not only own notes: a derivation
 * that inherits its gate by reference has no note at its own level, and
 * skipping it would let `ancestorGateExprs` graft unvalidated, grammar-banned
 * text there (fail open).
 *
 * `onAdmitAllGate` reports each unconditional `true` a source declares (the
 * one body that turns a gate off); reported, never refused, and a callback so
 * this module stays out of telemetry. "Declares" uses
 * `attributedAuthorizeOwnNotes`, not the presence-based `authorizeOwnNotes`:
 * Malloy copies notes by reference onto plain derivations, so the presence
 * map would count one declared `true` once per inheriting entry point. It is
 * a separate parameter because narrowing the presence map at the `"false"`
 * filter below would turn a load refusal into a fail-open (see
 * `source_extraction.ts`). Fired after the coherence checks, so a refused
 * model does not tick the counter.
 *
 * `authorizeOwnNotes` decides whether a group is the source's own declaration
 * (validate every expr, `"false"` included) or inherited (skip the synthetic
 * `"false"` sentinel, which is not authored text). A legacy quoted-string
 * payload ({@link isLegacyQuotedPayload}) is skipped too: it is refused once,
 * for its declaring source ({@link findLegacyStringGates}).
 *
 * Cross-term coherence ({@link assertAuthorizeGrammarTermsCoherent}) runs per
 * group: a query-source base and its composite member are different declaring
 * sources whose gates AND, and flattening them would refuse legal models that
 * share a given name or mix scopes. The exception is the source's own groups
 * on both routes, which are one declaring source and are checked together;
 * that is how `deny_all_with_sibling` catches `#(authorize) false` alongside
 * the same source's `#(access_filter) org_id in $GROUPS`.
 */
export function assertAuthorizeGrammarValid(
   modelDef: ModelDef | undefined,
   authorizeMap: AuthorizeMap,
   authorizeOwnNotes: AuthorizeOwnNotesMap,
   givenDeclaredTypes: ReadonlyMap<string, string>,
   onAdmitAllGate?: (sourceName: string, route: string) => void,
   attributedAuthorizeOwnNotes?: AuthorizeOwnNotesMap,
): void {
   if (!modelDef) return;
   for (const [sourceName, groups] of authorizeMap) {
      const struct = modelDef.contents[sourceName];
      const ownGroupTerms: AuthorizeGrammarRoutedTerm[] = [];
      const declaredAdmitAllRoutes: string[] = [];
      for (const { route, exprs } of groups) {
         const isOwn =
            (authorizeOwnNotes.get(sourceName)?.get(route)?.length ?? 0) > 0;
         const filteredExprs = exprs.filter(
            (expr) =>
               (isOwn || expr !== "false") && !isLegacyQuotedPayload(expr),
         );
         const groupTerms: AuthorizeGrammarRoutedTerm[] = [];
         for (const expr of filteredExprs) {
            const terms = parseAuthorizeGrammarBody(
               sourceName,
               expr,
               givenDeclaredTypes,
               route,
            );
            for (const term of terms) {
               groupTerms.push({ term, route });
               if (
                  term.scope === "admit_all" &&
                  (attributedAuthorizeOwnNotes?.get(sourceName)?.get(route)
                     ?.length ?? 0) > 0
               ) {
                  declaredAdmitAllRoutes.push(route);
               }
               if (term.scope === "row_level" && isSourceDef(struct)) {
                  assertNoFanoutFieldPath(
                     sourceName,
                     struct,
                     term.fieldPath,
                     term.fieldPathSegments,
                  );
               }
            }
         }
         if (isOwn) {
            ownGroupTerms.push(...groupTerms);
         } else {
            assertAuthorizeGrammarTermsCoherent(sourceName, groupTerms);
         }
      }
      if (ownGroupTerms.length > 0) {
         assertAuthorizeGrammarTermsCoherent(sourceName, ownGroupTerms);
      }
      for (const route of declaredAdmitAllRoutes) {
         onAdmitAllGate?.(sourceName, route);
      }
   }
}

/** Why {@link fanoutSegment} could not prove a path clean. An unresolvable
 *  segment is unknown, not safe, so both are refused. */
type FanoutFinding = { segment: string; reason: "fanout" | "unresolved" };

/**
 * The first non-final segment of `path` that is a fan-out join (`join_many`,
 * `join_cross`, or a repeated record, all `join: "many"` in the IR) or does
 * not resolve. `undefined` means only `join_one` hops up to the last segment.
 *
 * An unresolved segment fails closed: this cannot tell an author typo from a
 * resolution bug in its caller (e.g. a segment still carrying quotes).
 */
function fanoutSegment(
   struct: SourceDef,
   path: readonly string[],
): FanoutFinding | undefined {
   let current = struct;
   for (let i = 0; i < path.length - 1; i++) {
      const seg = path[i];
      const field = (current.fields ?? []).find(
         (f) => ((f as { as?: string }).as ?? f.name) === seg,
      );
      if (!field) return { segment: seg, reason: "unresolved" };
      if (isJoined(field) && (field as { join?: string }).join !== "one") {
         return { segment: seg, reason: "fanout" };
      }
      if (
         !isSourceDef(field) &&
         (field as { type?: string }).type !== "record" &&
         (field as { type?: string }).type !== "array"
      ) {
         return { segment: seg, reason: "unresolved" };
      }
      current = field as unknown as SourceDef;
   }
   return undefined;
}

/**
 * Refuse a row-level `#(access_filter)` term whose field path reaches through
 * a fan-out join (the entry row has many values for what follows, so there is
 * no single key to compare to a given) or does not resolve. Lives here, not in
 * `./authorize_grammar`, because it needs the compiled struct.
 *
 * Walks `fieldPathSegments` (split on top-level dots, backticks stripped),
 * never `fieldPath.split(".")`, which would break on a dot inside backticks
 * and keep the backticks, silently no-opping the check. `fieldPath` is for
 * the message only.
 */
export function assertNoFanoutFieldPath(
   sourceName: string,
   struct: SourceDef,
   fieldPath: string,
   fieldPathSegments: readonly string[],
): void {
   const finding = fanoutSegment(struct, fieldPathSegments);
   if (finding === undefined) return;
   const detail =
      finding.reason === "fanout"
         ? `which reaches through \`${finding.segment}\`, a fan-out join ` +
           `(\`join_many\`/\`join_cross\`, or a repeated record) — the ` +
           `entry row has many values for what follows, so there is no ` +
           `single key to compare against a given. Gate on a field reached ` +
           `only through \`join_one\` joins.`
         : `whose segment \`${finding.segment}\` does not resolve against ` +
           `this source — refused rather than assumed safe.`;
   throw new AuthorizeGrammarError(
      "fanout_path",
      `Source "${sourceName}" declares an authorize gate on \`${fieldPath}\`, ${detail}`,
   );
}

/**
 * Describes each source, top-level query, or the model itself whose IR
 * carries a marker from a retired annotation route (`RETIRED_ROUTES`). Malloy
 * still parses such a marker (its bracket routing is generic), and nothing
 * enforces it any more, so it would otherwise load clean and serve every row.
 *
 * Checks own notes and, via {@link reachesRetiredRouteTagBelow}, the rest of
 * a source's IR, plus top-level `query:` statements and the model's file-level
 * annotations (the same positions `assertNoMisplacedAuthorizeAnnotations`
 * covers for `#(authorize)`).
 */
export function collectRetiredRouteMarkers(
   modelDef: ModelDef | undefined,
): string[] {
   const found: string[] = [];
   if (!modelDef) return found;

   const fileMatch = containsRetiredRouteTag(
      ownLevelNoteTexts(modelAnnotations(modelDef)),
   );
   if (fileMatch) {
      found.push(
         `the model itself declares a retired \`##(${fileMatch})\` annotation`,
      );
   }

   for (const [key, obj] of Object.entries(modelDef.contents)) {
      const isQuery = (obj as { type?: string }).type === "query";
      if (!isSourceDef(obj) && !isQuery) continue;
      const label = (obj as { as?: string }).as ?? obj.name ?? key;
      const noun = isQuery ? "query" : "source";
      // The full `inherits` chain: a marker may live only on a base absent
      // from `modelDef.contents` (reached via a transitive import), and
      // `reachesRetiredRouteTagBelow` skips the root annotation chain.
      const ownMatch = containsRetiredRouteTag(
         annotationTexts(
            (obj as { annotations?: AnnotationsDef }).annotations,
         ) ?? [],
      );
      if (ownMatch) {
         found.push(
            `${noun} "${label}" declares a retired \`#(${ownMatch})\` annotation`,
         );
         continue;
      }
      if (isQuery) {
         // A named query has no joins or fields below its own annotations.
         continue;
      }
      let belowMatch: string | undefined;
      try {
         belowMatch = reachesRetiredRouteTagBelow(obj);
      } catch {
         // An unread chain is unknown, not absent: refuse, naming the route
         // this list exists for.
         belowMatch = RETIRED_ROUTES[0];
      }
      if (belowMatch) {
         found.push(
            `source "${label}" carries a retired \`#(${belowMatch})\` ` +
               `annotation somewhere in its IR (not on the source line itself)`,
         );
      }
   }
   return found;
}

/**
 * Refuse a model load carrying any {@link collectRetiredRouteMarkers}
 * finding. (`assertNoAuthorizeNearMisses` keys on the word "authorize" and
 * cannot be reused.)
 */
export function assertNoRetiredRouteMarkers(found: readonly string[]): void {
   if (found.length === 0) return;
   throw new ModelCompilationError({
      message:
         `These sources carry a marker from a retired annotation route:\n` +
         `${found.map((f) => `  - ${f}`).join("\n")}\n` +
         `The route is no longer enforced by anything, so Malloy would ` +
         `otherwise parse it, load the model clean, and serve every row as ` +
         `if the marker were never written. Remove the annotation.`,
   });
}
