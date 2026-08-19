/**
 * Entry-point `#(authorize)` gate classification against a `{modelDef,
 * materializer}` pair, extracted out of `Model` (`service/model.ts`) so a
 * caller with no live `Model` instance — the build-plan compile path
 * (`build_plan.ts`), which only ever has a compiled `ModelDef` and a
 * `ModelMaterializer` — can classify a gate the same way the request-serving
 * path does. `Model` wraps the entry points it still calls directly
 * (`collectEntryPointGates`, `resolveGateShape`, `resolveGraftTarget`) as thin
 * per-instance private methods, supplying its own long-lived caches; see
 * {@link GateClassificationDeps}'s doc for why the cache is an explicit input
 * rather than module state.
 *
 * `ancestorGateExprs`/`resolveQuerySourceBase` (the `sourceRegistry`-following
 * half of this walk) already live in `./gate_registry_walk`, shared with
 * `source_extraction.ts` — this module builds on those rather than
 * duplicating them.
 */

import {
   isSourceDef,
   type FilterCondition,
   type ModelDef,
   type ModelMaterializer,
   type SourceDef,
} from "@malloydata/malloy";
import { logger } from "../logger";
import { ownLevelNotes, type AnnotationNote } from "./annotations";
import {
   buildRowLevelProbe,
   classifyAuthorizeGate,
   collectAuthorizeExprs,
   gateFilterText,
   liftProbeFilterCondition,
   type RowLevelGateClassification,
   type RowLevelGateRejectionCause,
} from "./authorize";
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
   selfContained: boolean;
   /**
    * The ENTRY POINT this gate applies to — the run target itself, or its
    * resolved composite branch — NOT necessarily the struct the gate's own
    * annotations were read off (a gate carried in from a `query_source`
    * base lives on a different struct than the entry point it gates).
    * Absent only for the synthetic "unresolvable query-source base" deny
    * entry, which names no real struct at all. Row-level gate resolution
    * needs this to find where in `modelDef.contents` to graft the gate's
    * condition ({@link resolveGraftTarget}).
    */
   struct?: SourceDef;
};

/**
 * The model def + materializer a row-level gate's classification, lift
 * probe, and graft resolve against — see `resolveGateShape`,
 * `resolveGraftTarget`, `liftGateCondition`. `Model`'s query path and the
 * compile-time probe backstop use its own cumulative (modelDef,
 * modelMaterializer); a notebook cell instead passes an earlier cell's own
 * scope, or its own post-declaration scope — see `Model.defaultGraftScope` /
 * `Model.graftScopeForCell` / `Model.selfGraftScopeForCell` /
 * `Model.resolveNotebookCellGraftScope` for how those are built.
 *
 * `cacheScope` namespaces the caller's own gate-shape and grafted-materializer
 * caches: two scopes that happen to name a source identically (the model-wide
 * model and a different cell's earlier snapshot of it, or a cell's own
 * snapshot of itself) must never share a cached classification or a
 * materializer grafted against the wrong modelDef.
 */
export interface GraftScope {
   modelDef: ModelDef;
   materializer: ModelMaterializer;
   cacheScope: string;
}

/**
 * Inputs {@link resolveGateShape} needs beyond its own arguments, injected
 * rather than module-level state because
 * the two current callers need different cache lifetimes: `Model` reuses one
 * `gateShapeCache` for the life of its (long-lived, shared-across-requests)
 * instance, while a one-shot build-time classification must pass its own
 * (typically fresh, single-pass) cache rather than silently inheriting or
 * polluting a request-serving `Model`'s.
 *
 * Construct this ONLY through {@link createGateClassificationDeps} — never
 * assemble the three fields by hand, and never reassign one on an existing
 * instance. `gateShapeCache`'s entries are computed FROM
 * `givenDeclaredTypes`/`givenDeclaredDefaults` (a classification reads them
 * inside {@link resolveGateShape}'s cache-miss branch), but the cache key
 * carries no fingerprint of the given surface — before this type had a
 * single constructor, that was safe only because its one caller (`Model`)
 * kept the cache and the given maps as three fields of the SAME `this`,
 * which could not disagree. Once a caller can build this struct directly (a
 * one-shot build-time classification, or any future second caller), a cache
 * computed under one given surface but paired with a DIFFERENT one would
 * return a stale classification with no error — `row_level` where the
 * correct answer is `rejected` is fail-OPEN. The factory closes this
 * structurally: a `gateShapeCache` and the given maps it was computed
 * against are always minted together, in the same call, so one can never
 * outlive or cross the given surface it belongs to.
 */
export interface GateClassificationDeps {
   /**
    * Row-level shape memo, keyed `${cacheScope}\u0000${graftTarget}\u0000${filterText}`
    * — see `Model.gateShapeCache`'s doc for why the key must include
    * `cacheScope` and why this is safe to cache for the life of whatever
    * scope the caller keys it to.
    */
   gateShapeCache: Map<
      string,
      { classification: RowLevelGateClassification; condition: FilterCondition }
   >;
   /** See `Model.givenDeclaredTypes`. */
   givenDeclaredTypes: Map<string, string>;
   /** See `Model.givenDeclaredDefaults`. */
   givenDeclaredDefaults: Map<string, string>;
   /** Debug-log context only; never read for a decision. */
   modelPath?: string;
}

/**
 * The one sanctioned way to build a {@link GateClassificationDeps} — see its
 * doc for the drift hazard this closes. `givens` is read ONCE, here, into a
 * brand-new `gateShapeCache` (never a `Map` the caller already had lying
 * around) and the given-declared-type/default maps derived from that SAME
 * `givens` array, so the cache and the given surface it classifies against
 * can never be assembled from two different calls and therefore never
 * disagree.
 *
 * A caller that needs one long-lived deps struct per given surface (`Model`)
 * calls this ONCE and holds onto the result for as long as that surface is
 * valid (`Model`'s own `givens` is fixed for the life of the instance — a
 * package reload constructs a fresh `Model`, never patches this one); a
 * one-shot build-time classification calls it fresh per build, same as it
 * would have passed a fresh `Map()` before this existed.
 */
export function createGateClassificationDeps(
   givens: ReadonlyArray<{ name?: string; type?: string; default?: unknown }>,
   modelPath?: string,
): GateClassificationDeps {
   return {
      gateShapeCache: new Map(),
      givenDeclaredTypes: computeGivenDeclaredTypes(givens),
      givenDeclaredDefaults: computeGivenDeclaredDefaults(givens),
      modelPath,
   };
}

/**
 * Effective authorize exprs read directly off a struct's block annotations
 * (its own `#(authorize)`, else the nearest ancestor's — see
 * {@link ancestorGateExprs}). Used by the derivation and composite-member
 * walks below, which read a struct's gate straight off its own annotations
 * rather than through a name lookup (a name lookup only covers top-level
 * `modelDef.contents` sources and would miss these).
 *
 * Fails CLOSED: `extractSourcesFromModelDef` only validates authorize
 * annotations for top-level `modelDef.contents` sources at model load, so a
 * malformed gate on a derivation base or a composite member that is not
 * itself top-level is never probed there — a parse failure here can't be
 * assumed unreachable/already-validated. Force denial (a single
 * unsatisfiable `"false"` expr) rather than treating the parse failure as "no
 * gate" (fail-open).
 *
 * `fromAncestor` reports that the gate came from the derivation base rather
 * than from `struct`'s own notes, which decides how the caller must PROBE it
 * (see {@link collectEntryPointGates}'s `selfContained`): an ancestor's gate
 * lives in a different source, and possibly a different given namespace, even
 * when the struct carrying it is the entry point.
 *
 * `excludeNotes` is subtracted, by object IDENTITY, from `struct`'s own notes
 * before they are read — see {@link collectEntryPointGates}'s doc for why:
 * Malloy's composite resolver copies a `query_source` base's own annotation
 * note OBJECTS onto its resolved member struct's own `blockNotes`, alongside
 * the member's own notes. Reading them unfiltered folds the base's gate into
 * the member's own OR group — a different declaring source's condition
 * landing in THIS source's disjunction, which is the AND-becomes-OR leak this
 * parameter exists to close. Empty for every caller except the
 * composite-member recursion in {@link collectEntryPointGates}.
 */
function gateExprsForOwnAnnotations(
   struct: SourceDef,
   modelDef?: ModelDef,
   excludeNotes: readonly AnnotationNote[] = [],
): { exprs: string[]; fromAncestor: boolean } {
   // Both note keys: which one a gate lands in is decided by the author's
   // syntax, not by scope. See {@link ownLevelNoteTexts}.
   const ownNotes = ownLevelNotes(struct.annotations).filter(
      (note) => !excludeNotes.includes(note),
   );
   try {
      const own = collectAuthorizeExprs(ownNotes.map((note) => note.text));
      if (own.length > 0) {
         return { exprs: own, fromAncestor: false };
      }
      const ancestor = ancestorGateExprs(struct, modelDef);
      return { exprs: ancestor, fromAncestor: ancestor.length > 0 };
   } catch {
      return { exprs: ["false"], fromAncestor: false };
   }
}

/**
 * The gates that apply to `struct` AS AN ENTRY POINT. Collects:
 *  - its own annotations ({@link gateExprsForOwnAnnotations}) — its own
 *    `#(authorize)`, or, when `struct` declares none of its own, the nearest
 *    `extend` ancestor's ({@link ancestorGateExprs});
 *  - if `struct` is query-derived (`source: x is y -> {...}`), the base it
 *    derives from (`query.structRef`, resolved the way
 *    `Model.resolveRunTargetStruct` resolves a run target's structRef) — a
 *    `QuerySourceDef`'s own `.fields`/`.annotations` reflect the DERIVED
 *    shape, not `y`'s gate, so without this `source: mine is locked -> {...}`
 *    would launder the base's gate away at the entry point itself;
 *  - if `struct` is query-derived AND its base is a composite, the ONE member
 *    branch Malloy resolved for THIS derivation
 *    (`query.compositeResolvedSourceDef`).
 *
 * Derivation recurses through this same function, so a chained derivation is
 * covered at any depth.
 *
 * What this deliberately does NOT collect (Q16 — joins are not traced):
 * joined sources (`struct.fields`), members of a joined composite,
 * query-local joins inside a `-> { join_one: ... }` refinement, and a
 * derivation's own inner-pipeline joins. A gate is a statement about who may
 * query the source it is declared on, not about everything reachable beneath
 * it.
 *
 * `seen` (struct-identity keyed) guards cycles and repeat structs.
 *
 * `excludeNotes` — forwarded to {@link gateExprsForOwnAnnotations} — is the
 * IDENTITY-subtraction the composite-member recursion below needs (see that
 * method's doc): Malloy copies a `query_source` base's own annotation note
 * OBJECTS onto its resolved composite member's own `blockNotes`, alongside
 * the member's own notes, so reading the member's own gate without excluding
 * the base's copy would fold two different declaring sources' gates into one
 * OR group instead of the two separate (AND'd) `GateEntry` results this
 * function already produces for the plain base-vs-composite split. Every
 * OTHER recursive call passes none: a query-source's own base (as opposed to
 * that base's composite-resolved member) carries no such copy to subtract.
 *
 * `QuerySourceDef` isn't re-exported from the package root (same situation as
 * `given.ts`'s `MalloyGiven` duck type), so query-source detection checks
 * `.type` and reaches `.query.structRef` through a local shape rather than
 * importing the real type.
 *
 * Each returned entry carries `selfContained`, telling the caller which order
 * `evaluateAuthorize` should try for that gate (see
 * `assertAuthorizedExprs`'s `selfContainedFirst`). `treatAsOwnGate` — true
 * only for the run target's own struct and its own resolved composite branch,
 * the two call sites that represent the entry point ITSELF — marks that
 * entry's own annotations `selfContained: false` (ambient-first, matching
 * `Model.assertAuthorized`). A gate reached through a derivation is tagged
 * `selfContained: true`: it lives in a DIFFERENT source (possibly a different
 * file/given-namespace), so evaluating it ambiently against the entry model's
 * namespace risks a name collision silently granting access off the wrong
 * given (see `evaluateAuthorize`'s `selfContainedFirst` doc).
 */
export function collectEntryPointGates(
   struct: SourceDef | undefined,
   modelDef: ModelDef | undefined,
   seen: Set<SourceDef> = new Set(),
   treatAsOwnGate = false,
   // The struct that STARTED this walk — the run target itself, or its
   // resolved composite branch. Held fixed across the query-source recursion
   // below (never reassigned to `base`/`resolved`), because it, not whichever
   // struct a gate's OWN annotations happened to live on, is what
   // {@link resolveGraftTarget} must graft onto: a gate carried in from a
   // derivation base applies to THIS entry point as an entry point, and
   // grafting the base instead cannot reach a model-declared derivation
   // (`Z is X -> {...}`), which snapshotted its base at declaration time.
   entryPointStruct: SourceDef | undefined = struct,
   // See this function's doc. Non-empty only for the composite-member
   // recursion below, which passes the query-source base's own notes so
   // Malloy's by-reference copy of them onto the member's own `blockNotes`
   // doesn't fold the base's gate into the member's own OR group.
   excludeNotes: readonly AnnotationNote[] = [],
): GateEntry[] {
   if (!struct || !modelDef || seen.has(struct)) return [];
   seen.add(struct);

   const results: GateEntry[] = [];
   const label = (struct as { as?: string }).as ?? struct.name;
   const { exprs: ownExprs, fromAncestor } = gateExprsForOwnAnnotations(
      struct,
      modelDef,
      excludeNotes,
   );
   if (ownExprs.length > 0) {
      results.push({
         label,
         exprs: ownExprs,
         // A gate CARRIED IN from a derivation base is self-contained even
         // when `struct` is the entry point: it was authored in a different
         // source, possibly a different file's given namespace, so probing it
         // ambiently would let a colliding entry-model given of the same name
         // decide it (see `evaluateAuthorize`'s `selfContainedFirst` doc).
         selfContained: fromAncestor || !treatAsOwnGate,
         // The ENTRY POINT, not `struct` — see `entryPointStruct`'s doc
         // above. Carried so a row-level classification of THIS entry knows
         // where to graft — see `resolveGraftTarget`.
         struct: entryPointStruct,
      });
   }

   // Joined sources are deliberately NOT walked — see this function's doc. A
   // gate is evaluated on the ENTRY POINT only; reaching a gated source
   // through `join_*` does not bring its gate along.

   const duck = struct as unknown as {
      type: string;
      query?: {
         structRef?: SourceDef | string;
         compositeResolvedSourceDef?: SourceDef;
      };
   };
   if (duck.type === "query_source") {
      // Shared with `extractSourcesFromModelDef`'s `effectiveAncestorGateExprs`
      // (`./gate_registry_walk`) — see that module's doc for why the lookup is
      // shared but this recursion (composite-branch handling,
      // `entryPointStruct` threading) is not.
      const base = resolveQuerySourceBase(struct, modelDef);
      if (base) {
         results.push(
            ...collectEntryPointGates(
               base,
               modelDef,
               seen,
               false,
               entryPointStruct,
            ),
         );
      } else {
         // A `query_source` derives from something by construction, so a base
         // we cannot resolve is IR we failed to read — not an ungated source.
         // Contributing no gate here would launder the base's gate away
         // exactly like the pre-Q16 laundering this walk exists to stop, so
         // deny instead. An already-visited base is not this case: it still
         // resolves and takes the branch above, where the `seen` check makes
         // the recursion a no-op.
         results.push({
            label,
            exprs: ["false"],
            selfContained: true,
         });
      }
      // A query-source's own base may itself be a composite
      // (`source: qs is compose(a, b) -> {...}`) — Malloy resolves that
      // composite to exactly one concrete member branch for THIS
      // query-source's derivation (surfaced the same way as a run target's
      // own composite resolution, see `Model.assertAuthorizedForAllSources`),
      // carried on the query-source's OWN `query.compositeResolvedSourceDef`,
      // not on `query.structRef` (the raw composite) or `query.pipeline`.
      // Without walking it, a query-source derived from a locked composite
      // member laundered that member's gate away. Recursing through this same
      // function means a query-source nested at any depth (query-source over
      // composite over query-source, etc) is covered uniformly.
      const resolved = duck.query?.compositeResolvedSourceDef;
      if (resolved) {
         results.push(
            ...collectEntryPointGates(
               resolved,
               modelDef,
               seen,
               false,
               entryPointStruct,
               // Identity-subtract the base's own notes — see this
               // function's `excludeNotes` doc.
               base ? ownLevelNotes(base.annotations) : [],
            ),
         );
      }
      // The derivation's own inner-pipeline `join_one`s are NOT walked — same
      // rule as every other join.
   }

   return results;
}

/**
 * Resolve ONE {@link GateEntry} to its enforcement shape: `row_level` (a row
 * FILTER, carrying the graft target and the lifted compiled condition), or
 * `rejected` (fail closed — either the compiled shape is not an allowed one,
 * or there is nowhere to graft it).
 *
 * `entry.struct` is absent only for the synthetic "unresolvable query-source
 * base" entry `collectEntryPointGates` manufactures, whose sole expression is
 * the literal `"false"` — there is no real struct here to graft anything
 * onto, so this rejects outright rather than attempting a classification.
 *
 * `filterText` folds the entry's whole OR disjunction into ONE expression:
 * `exprs.map(e => "(" + e + ")").join(" or ")`. This is deliberate, not
 * incidental — it is what keeps the admin-override idiom working under
 * row-level enforcement. `#(authorize) "$ROLE = 'admin'"` OR'd with
 * `#(authorize) "org_id in $GROUPS"` becomes
 * `($ROLE = 'admin') or (org_id in $GROUPS)`, ONE filter that preserves OR
 * semantics exactly: an admin's `$ROLE` check makes the whole disjunction
 * (and therefore the row filter) constant-true, not a second gate an admin
 * must ALSO satisfy. A given-only predicate inside a `where:` is legal Malloy
 * and constant for the life of one request, so folding a given-only disjunct
 * into the same filter text changes nothing about what rows it admits.
 *
 * Classification is memoized per `(cacheScope, graftTarget, filterText)` in
 * `deps.gateShapeCache` — see {@link GateClassificationDeps}'s doc for why
 * the cache is an explicit input, and `Model.gateShapeCache`'s doc for why
 * caching on the MODEL INSTANCE is what makes this correct across a package
 * reload, and why `cacheScope` is part of the key. A cache miss lifts the
 * condition through `graftScope`'s OWN materializer ({@link liftGateCondition})
 * and classifies it (`classifyAuthorizeGate`); a `rejected` classification is
 * cached too; a THROW from the lift itself is NOT cached (it denies for this
 * call, but is retried on the next one) and is treated as a straight deny,
 * never a fallback to the probe — the probe cannot correctly evaluate a gate
 * that references a row field at all.
 *
 * The FINAL graft target key is always resolved against `graftScope.modelDef`
 * — the STABLE model this SCOPE grafts onto and lifts through ({@link
 * liftGateCondition} compiles via `graftScope.materializer`) — never against
 * the caller's own compiled query's ephemeral `originModelDef`. That
 * ephemeral model mints its own fresh copy of every declared source (a
 * caller-declared `source: mine is X extend {}` becomes a REAL
 * `contents["mine"]` entry there, but nowhere else), so resolving the graft
 * target against it can find a key that does not exist in
 * `graftScope.modelDef.contents` at all — the graft and the lift probe both
 * require a `graftScope.modelDef.contents` key. `originModelDef` — the model
 * `entry.struct` actually compiled against — is still needed for the
 * ancestor WALK itself (see {@link resolveGraftTarget}): that is the only
 * model whose `sourceRegistry` can possibly link `entry.struct` to whatever
 * it derives from. For the query path these two coincide
 * (`graftScope.modelDef === this.modelDef === originModelDef`); for a
 * notebook cell they deliberately do not — see {@link GraftScope}.
 *
 * `graftScope` is `undefined` only when the caller has nothing to graft
 * against at all (no compiled model, or — for a notebook cell — no earlier
 * cell to graft onto); that always rejects here, same as an unresolvable
 * graft target — every gate is a row filter now, and a filter with nowhere to
 * attach cannot be enforced.
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
        /**
         * Whether this gate's compiled condition reduces to the bare
         * literal `false` and nothing else — the accepted constant-predicate
         * idiom `classifyAuthorizeGate` already recognizes (see its `kind ===
         * "false"` branch), read back off the classification rather than
         * re-derived. True only when every accepted literal atom is `"false"`
         * and no given was referenced, so an OR'd admin-override disjunct
         * (`false or $ROLE = 'admin'`) is correctly NOT constant-false. A
         * caller can use this to skip dispatching the graft to the warehouse
         * at all: the row set it would return is provably empty.
         */
        constantFalse: boolean;
     }
   | { shape: "rejected"; cause?: RowLevelGateRejectionCause }
> {
   if (!entry.struct) return { shape: "rejected" };
   if (!graftScope) {
      // There is no scope here to even ATTEMPT a graft against. For a
      // notebook cell this means NEITHER of `resolveNotebookCellGraftScope`'s
      // two scopes was available — no earlier code cell, AND this cell has no
      // compiled model of its own (see that method's doc; a self-declaring
      // cell, first cell or not, resolves via its OWN scope and never
      // reaches this branch). The caller-facing error stays the same opaque
      // 403 either way (deliberate — it must not leak whether a gate exists
      // or what it names), but an operator reading logs should be able to
      // tell "there was nowhere at all to carry a graft" from the log line.
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
   // No key to graft anything onto — an ad-hoc/ephemeral run target (an
   // independently recompiled `/compile` model, a notebook cell's `source:
   // mine is base_locked extend {…}`) can fail this resolution. Every gate is
   // a row filter now, and a filter with nowhere to attach cannot be
   // enforced, so this rejects rather than attempting a fallback
   // classification.
   if (!graftTarget) {
      return { shape: "rejected" };
   }
   const filterText = gateFilterText(entry.exprs);
   const cacheKey = `${graftScope.cacheScope}\u0000${graftTarget}\u0000${filterText}`;

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
         logger.debug("Row-level gate condition failed to lift; denying", {
            modelPath: deps.modelPath,
            graftTarget,
            error: err instanceof Error ? err.message : String(err),
         });
         return { shape: "rejected" };
      }
      let classification = classifyAuthorizeGate(
         condition,
         deps.givenDeclaredTypes,
         deps.givenDeclaredDefaults,
      );
      // `Model.filterGivensToModelSurface` drops a caller-supplied given that
      // is off this model's own surface, and its safety rests on this: an
      // ACCEPTED gate never references one, because `classifyAuthorizeGate`
      // rejects a given absent from `declaredTypes` (which is that same
      // surface). Re-checked here rather than assumed — a walk branch that
      // records a given without routing it through `declaredTypeOf` would
      // otherwise let the filter drop a value the grafted gate still reads,
      // silently falling back to the declaration default.
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
   return {
      shape: "row_level",
      graftTarget,
      filterText,
      condition: cached.condition,
      constantFalse:
         cached.classification.givenNames.length === 0 &&
         cached.classification.literalAtoms.length > 0 &&
         cached.classification.literalAtoms.every((atom) => atom === "false"),
   };
}

/**
 * The `modelDef.contents` KEY to graft a gate entry's condition onto, or
 * `undefined` if none resolves (⇒ the caller denies).
 *
 * `struct` here is always the RUN TARGET's own entry-point struct (see
 * {@link collectEntryPointGates}'s `entryPointStruct`) — never the struct a
 * chained gate's OWN annotations happened to be read off. That is what makes
 * this call site P0-safe BY CONSTRUCTION, not just by scoping which gates get
 * collected: the run target is the entry point by definition and is never a
 * source reached through a join, so grafting it can never make a joined
 * source's gate fire. Do not widen this to graft anything other than the run
 * target (or its resolved composite branch) — that reintroduces the exact
 * join-propagation leak this scoping closes: grafting a condition onto a
 * SourceDef propagates it into every joined copy of that source, firing a
 * gate P0 says must not fire.
 *
 * If `struct` IS itself a `contents` entry, the entry point is graftED
 * DIRECTLY — this covers `Y is X extend {}` inheriting `X`'s gate (`Y` is the
 * run target AND a top-level declaration, so grafting `Y` is both correct and
 * sufficient) and `Z is X -> {...}` (`Z` is a `contents` entry whose gate
 * arrived via the `query_source` recursion but which must still be grafted at
 * `Z` itself, not at its base `X` — `Z`'s compiled `SourceDef` is a
 * compile-time snapshot of `X` taken when `Z` was defined, so a condition
 * appended to `X` afterward never reaches it, and `org_id` resolves fine in
 * `Z`'s field space when `Z`'s projection kept it).
 *
 * Otherwise `struct` is an AD-HOC caller-declared run target (`source: mine
 * is X extend {…}` + `run: mine`) that never became a `contents` entry of its
 * own. Two links are tried, nearest first, until one lands on a
 * `graftModelDef.contents` entry:
 *  - {@link resolveDeclaredSource}'s `sourceRegistry` walk — real for a plain
 *    join or an unmodified rename.
 *  - {@link findSourceByOwnAnnotationIdentity} — needed because a TRIVIAL
 *    `extend {}` (no rename/except/accept/dimension/join addition) compiles
 *    to a `sourceRegistry` entry that only references ITSELF
 *    (`{type: "source_registry_reference", name: "mine"}`, no `referenceID`
 *    at all) — Malloy elides the derivation entirely rather than recording a
 *    link to `X`, so the registry walk has nothing to follow. What IS
 *    preserved is the struct's own `#(authorize)` annotation note: Malloy
 *    copies X's note onto `mine` by REFERENCE (the exact same object, not a
 *    re-parsed equal one), so it can be traced back to whichever
 *    `graftModelDef.contents` entry owns that same note object — and only
 *    that entry, since two independently authored `#(authorize)` blocks with
 *    identical text are two distinct objects (parsed separately, at separate
 *    locations). This is an object-identity match, same spirit as "never by
 *    name alone" below — it happens to key off an annotation node rather than
 *    the struct itself only because that is the one thing Malloy's IR still
 *    shares by reference in this shape.
 * `mine` is compiled fresh from `X` every time the caller's text is
 * recompiled, so grafting `X` is what a caller-declared derivation needs.
 *
 * The FINAL key is always looked up in `graftModelDef.contents` — the STABLE
 * model the graft and the lift probe both compile against — never in
 * `originModelDef`, the caller's own query's ephemeral model. `mine` above is
 * a real `contents` entry in THAT ephemeral model (the caller declared it
 * inline), so resolving against it would return `"mine"` directly instead of
 * falling through to the ancestor walk that lands on `"X"` — and `"mine"`
 * does not exist in `graftModelDef.contents` at all, so the graft and the
 * lift probe both fail. `originModelDef` is still needed for the WALK itself:
 * it's the only model whose `sourceRegistry` (and, for the annotation
 * fallback, whose own `struct.annotations`) can possibly link `struct` to its
 * base. For the query path `graftModelDef` and `originModelDef` are the same
 * (`this.modelDef`); a notebook cell's graft scope passes an EARLIER cell's
 * own modelDef as `graftModelDef` while `originModelDef` stays the cell's own
 * (post-declaration) compiled modelDef — see {@link GraftScope} and
 * `resolveGateShape`'s doc.
 *
 * Matches identity first, then `sourceID` — never by name alone: two distinct
 * `SourceDef`s can share a name across an inheritance chain, and a name-only
 * match could graft the wrong one.
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
 * Find the `modelDef.contents` entry whose OWN annotation notes include, by
 * object REFERENCE (never by text), one of `struct`'s own notes — see
 * {@link resolveGraftTarget}'s doc for why this exists and why it is safe.
 * `exclude` keeps this from re-matching a struct already visited in the
 * ancestor walk.
 */
function findSourceByOwnAnnotationIdentity(
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
 * Lift a gate's compiled `FilterCondition` through `materializer` — the SAME
 * model instance {@link resolveGateShape} is grafting onto
 * (`graftScope.materializer`), by compiling a one-row probe that applies
 * `filterText` as a source-level `where:` on `graftTarget` and reading back
 * the LAST entry of the compiled query's `structRef.filterList`.
 *
 * This has to be the SAME materializer the graft targets, not a fresh
 * `runtime.loadModel(...)`. Every `loadModel` call mints a NEW identity for
 * each declared given (`given/6:GROUPS,57:internal://loadModel/<uuid>`), even
 * for byte-identical text — so a condition lifted from a separately loaded
 * probe model and grafted onto the real model would reference a given
 * identity the real model never surfaced, and fail at request time with
 * "references a given ... which is not surfaced in this model". Sharing
 * `materializer` is what keeps the lifted condition's given references
 * resolvable against the model it will be grafted onto. THIS is why the
 * caller must supply a live materializer per call rather than lifting once
 * and reusing the result across a different one.
 *
 * `__authorize_probe` is a reserved, deliberately obscure select name — same
 * convention as `buildAuthorizeProbe` in `./authorize` — so it is unlikely to
 * collide with a real field on `graftTarget`.
 *
 * Takes the LAST entry of `filterList` on faith that it is the `where:` this
 * probe just added — which is exactly why {@link liftProbeFilterCondition}
 * (`./authorize`) asserts both properties that make that safe rather than
 * assuming them. See its doc for what each one rules out, and for how a
 * missing check ends up running a query UNGATED through
 * `Model.assertGateLanded`'s proof. A failure throws, which
 * {@link resolveGateShape}'s caller already turns into a deny.
 */
async function liftGateCondition(
   graftTarget: string,
   filterText: string,
   materializer: ModelMaterializer,
): Promise<FilterCondition> {
   // The probe SHAPE and the lift are both shared with load-time validation
   // (`./authorize`'s `buildRowLevelProbe` / `liftRowLevelCondition`) rather
   // than spelled out twice: reading the compiled `FilterCondition` back out
   // of `_query.structRef.filterList` only works while the two probe shapes
   // stay byte-identical, and the three checks that make that read
   // trustworthy must not be able to drift apart between the two paths.
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
 * Given name → declared Malloy type, from a model's given surface (its
 * `ApiGiven[]` list). Passed to `classifyAuthorizeGate`'s self-contained
 * probe fallback so it prefers the gate author's DECLARED type over inferring
 * one from the caller's JS value (e.g. `$LEVEL > 3` compares numerically even
 * if the caller sends `"5"`). Only reaches a given declared within one import
 * hop of the model — a gate on a source reached through a deeper transitive
 * import isn't on this surface, so that case still falls back to inferring
 * from the value.
 *
 * Not memoized here — the input `givens` list is fixed for the life of a
 * `Model` (a package reload constructs a fresh one) so `Model` caches the
 * result itself (`givenDeclaredTypesCache`); a one-shot build-time caller has
 * nothing to gain from a memo it would only ever read once.
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

/**
 * Given name → declared default (rendered Malloy source text), mirroring
 * {@link computeGivenDeclaredTypes} — same `givens` surface, same "not
 * memoized here" reasoning. Feeds `classifyAuthorizeGate`'s `declaredDefaults`
 * on the request-time graft re-classification ({@link resolveGateShape}), so
 * a field-vs-given gate reached through a join/derivation is refused the same
 * way `validateAuthorizeProbes` refuses it at load time — needed here too
 * because a grafted gate can resolve against a DIFFERENT model's given
 * surface than the one `validateAuthorizeProbes` validated.
 */
export function computeGivenDeclaredDefaults(
   givens: ReadonlyArray<{ name?: string; default?: unknown }> | undefined,
): Map<string, string> {
   return new Map(
      (givens ?? [])
         .filter(
            (g): g is { name: string; default: string } =>
               g.name != null && g.default != null,
         )
         .map((g) => [g.name, g.default as string] as [string, string]),
   );
}
