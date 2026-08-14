/**
 * The `sourceRegistry`-following fallback for finding a struct's inherited
 * `#(authorize)` gate, shared between `Model` (`service/model.ts`, request
 * time) and `extractSourcesFromModelDef` (`service/source_extraction.ts`,
 * package load) — plus, separately, {@link resolveQuerySourceBase}, the
 * `query_source` base lookup ALSO shared between the two.
 *
 * Malloy's `annotations.inherits` chain covers an `extend {}` derivation
 * whose OWN deriving statement carries an annotation (which demotes the
 * base's to `.inherits`). It does NOT run for the more common case measured
 * here: `extend {}`/`extend { rename: … }`/`extend { except: … }`/
 * `extend { accept: … }` with NO annotation of their own — Malloy instead
 * copies the base's annotation NOTE OBJECTS directly onto the deriving
 * struct's OWN `annotations`, by reference (confirmed against
 * `@malloydata/malloy` 0.0.427), which is why `ownLevelNoteTexts` on the
 * struct ITSELF already finds the gate for that shape and neither this
 * function nor `resolveDeclaredSource` is reached for it in practice — see
 * `validateAuthorizeProbes`'s doc for why that made source-identity the wrong
 * signal for "does this entry point declare its own gate".
 *
 * What `annotations.inherits` genuinely does NOT cover — confirmed the same
 * way — is a `query_source` (`Z is X -> {...}`): a `query_source`-typed
 * `StructDef` carries no `annotations` at all, and its OWN `sourceRegistry`
 * entry self-references (Malloy elides a real link for this shape exactly
 * like it elides one for a trivial `extend {}` — `resolveDeclaredSource`
 * resolves it to `{kind: "none"}`, not to its base). The only surviving link
 * from `Z` back to `X` is `Z.query.structRef` — see
 * {@link resolveQuerySourceBase}, which is NOT folded into
 * {@link ancestorGateExprs} itself: `Model.collectEntryPointGates` already
 * walks a `query_source`'s `query.structRef` as its OWN separate, already-
 * tested recursion (composite-branch handling included) rather than through
 * `gateExprsForOwnAnnotations`/`ancestorGateExprs` at all, so changing what
 * `ancestorGateExprs` returns for a `query_source` struct would double up
 * that recursion's own result rather than fill a gap. `extractSources
 * FromModelDef` has no such separate recursion, so it calls
 * {@link resolveQuerySourceBase} directly.
 *
 * Kept light on purpose: this is imported by `source_extraction.ts`, which is
 * bundled into the package-load worker (see that module's doc comment) — no
 * runtime/materializer/telemetry imports here, only `@malloydata/malloy`
 * types plus the pure annotation helpers.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";
import { ownLevelNoteTexts } from "./annotations";
import { collectAuthorizeExprs } from "./authorize";

/**
 * Link budget for {@link ancestorGateExprs}'s derivation walk. Exceeding it
 * denies/stops (the chain wasn't read to its end), so it only has to be
 * larger than any real chain.
 */
export const ANCESTOR_WALK_MAX_DEPTH = 32;

/**
 * The DECLARED source a struct was created from, via `ModelDef.sourceRegistry`
 * (`referenceID` — set for a plain join or unmodified rename — then the
 * struct's own `sourceID`).
 *
 * Three outcomes, and collapsing the last two would fail OPEN:
 *  - `resolved` — the declaration this struct derives from.
 *  - `none` — there is nothing to follow: no id, no registry, or every entry
 *    resolves back to `struct` itself (it IS its own declaration). The
 *    overwhelmingly common case for an ordinary top-level source, so this
 *    has to mean "no gate here", not "deny".
 *  - `unresolvable` — an entry WAS found for one of the ids and did not yield
 *    a usable `SourceDef` (a `source_registry_reference` naming something
 *    absent from `modelDef.contents`, or a non-source entry). The base exists
 *    and we cannot read it, so the caller denies.
 */
export function resolveDeclaredSource(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
):
   | { kind: "resolved"; source: SourceDef }
   | { kind: "none" }
   | { kind: "unresolvable" } {
   if (!modelDef) return { kind: "none" };
   let sawBrokenEntry = false;
   for (const id of [struct.referenceID, struct.sourceID]) {
      const entry = id ? modelDef.sourceRegistry?.[id]?.entry : undefined;
      if (!entry) continue;
      const declared =
         entry.type === "source_registry_reference"
            ? modelDef.contents[entry.name]
            : entry;
      // Its own declaration — nothing to inherit from, and not a failure.
      if (declared === struct) continue;
      if (!declared || !isSourceDef(declared)) {
         sawBrokenEntry = true;
         continue;
      }
      return { kind: "resolved", source: declared };
   }
   return sawBrokenEntry ? { kind: "unresolvable" } : { kind: "none" };
}

/**
 * The gate a struct carries from the source it was derived FROM, used when
 * the struct declares no `#(authorize)` of its own.
 *
 * Malloy does not leave a base's annotations at top level once the deriving
 * statement carries any annotation of its own — ANY annotation, not just an
 * authorize one:
 *  - `source: x is base extend {}` with its own note demotes the base's
 *    annotations to `annotations.inherits` (`define-source.ts`);
 *  - an annotated `join_one:`/`join_many:` REPLACES the joined struct's
 *    annotations outright, with no `inherits` at all (`join.ts`) — there the
 *    struct's own `sourceID`/`referenceID` is the only surviving link back;
 *  - a `query_source` (`Z is X -> {...}`) carries no `annotations` at all —
 *    see this module's doc comment — so the ONLY link back to `X` is the
 *    `sourceRegistry` fallback below; the `annotations.inherits` chain never
 *    runs for this shape.
 * Reading OWN `blockNotes` alone therefore loses the base's gate to a stray
 * render tag, doc comment, or a `query_source` derivation, whoever wrote it.
 * All three links are followed here, nearest first, and the first ancestor
 * that declares a gate wins.
 *
 * Each level is read with {@link ownLevelNoteTexts} rather than `blockNotes`,
 * so a base whose gate landed under `notes` is still found on every link.
 *
 * "Own wins over ancestor" is what keeps the documented locked-base +
 * curated-extension idiom working (an extension declaring its own gate
 * replaces the base's).
 *
 * Fail-closed on unreadable IR: an exhausted `annotations.inherits` walk (the
 * cap was hit) or an `unresolvable` registry link returns `["false"]` rather
 * than `[]` — the chain exists and was not read to its end, so "no gate" would
 * be a silent allow on a source whose base may be locked.
 */
export function ancestorGateExprs(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   seen: Set<SourceDef> = new Set(),
): string[] {
   let inherited = struct.annotations?.inherits;
   for (let depth = 0; inherited && depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const exprs = collectAuthorizeExprs(ownLevelNoteTexts(inherited));
      if (exprs.length > 0) return exprs;
      inherited = inherited.inherits;
   }
   if (inherited) return ["false"];
   // The registry link is followed as deep as the inherits chain, not one
   // hop: `seen` (struct identity) is what stops a cycle, so truncating the
   // recursion would just lose a gate two declarations up (fail open).
   seen.add(struct);
   if (seen.size > ANCESTOR_WALK_MAX_DEPTH) return ["false"];
   const declared = resolveDeclaredSource(struct, modelDef);
   // A registry entry we found but could not read is NOT the same as "this
   // struct has no base" — it means the link to a base exists and the walk
   // failed to follow it, so the gate on the other end is unknown. Deny.
   if (declared.kind === "unresolvable") return ["false"];
   if (declared.kind === "none" || seen.has(declared.source)) return [];
   const exprs = collectAuthorizeExprs(
      ownLevelNoteTexts(declared.source.annotations),
   );
   return exprs.length > 0
      ? exprs
      : ancestorGateExprs(declared.source, modelDef, seen);
}

/**
 * The `SourceDef` a `query_source` struct (`Z is X -> {...}`) derives from,
 * via `query.structRef` — `undefined` if `struct` is not a `query_source`, or
 * if the ref cannot be resolved to a usable `SourceDef` (a string ref naming
 * something absent from `modelDef.contents`, or a non-source entry).
 *
 * Duck-typed rather than imported: `QuerySourceDef` isn't re-exported from
 * the package root (same situation as `given.ts`'s `MalloyGiven`). Mirrors,
 * and is called by, `Model.collectEntryPointGates`'s identical resolution —
 * see this module's doc comment for why the recursion around it stays
 * separate per caller while the lookup itself is shared.
 */
export function resolveQuerySourceBase(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
): SourceDef | undefined {
   const duck = struct as unknown as {
      type: string;
      query?: { structRef?: SourceDef | string };
   };
   if (duck.type !== "query_source") return undefined;
   const ref = duck.query?.structRef;
   const base = typeof ref === "string" ? modelDef?.contents[ref] : ref;
   return base && isSourceDef(base) ? base : undefined;
}

/**
 * The `SourceDef` a `query_source` struct's own base resolved to when that
 * base is itself a composite (`source: qs is compose(a, b) -> {...}`) —
 * Malloy resolves the composite to exactly one concrete member for THIS
 * derivation, carried on `query.compositeResolvedSourceDef`, NOT on
 * `query.structRef` (the raw composite) or `query.pipeline`. Mirrors, and is
 * called by, `Model.collectEntryPointGates`'s identical resolution — see
 * this module's doc comment.
 */
export function resolveCompositeResolvedBase(
   struct: SourceDef,
): SourceDef | undefined {
   const duck = struct as unknown as {
      type: string;
      query?: { compositeResolvedSourceDef?: SourceDef };
   };
   return duck.type === "query_source"
      ? duck.query?.compositeResolvedSourceDef
      : undefined;
}

/**
 * {@link ancestorGateExprs}, extended with the TWO hops it deliberately
 * doesn't take — see this module's doc comment for why: a `query_source`'s
 * base via {@link resolveQuerySourceBase}, and (additively — Malloy models
 * these as two independently meaningful links, not alternatives) that base's
 * own composite-member resolution via {@link resolveCompositeResolvedBase}.
 * Used only by `extractSourcesFromModelDef`, which (unlike
 * `Model.collectEntryPointGates`) has no separate recursion of its own to
 * take either hop.
 *
 * A base that cannot be resolved denies (`["false"]`) rather than reporting
 * "no gate": a `query_source` derives from something by construction, so
 * failing to read its base is IR this walk failed to follow, not evidence
 * the source is ungated — same posture as `collectEntryPointGates`'s
 * identical case. The composite-member hop has no equivalent deny: it is
 * additive, so a query-source with no composite member to resolve simply
 * contributes nothing extra, exactly as `collectEntryPointGates` treats it.
 */
export function effectiveAncestorGateExprs(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   seen: Set<SourceDef> = new Set(),
): string[] {
   const direct = ancestorGateExprs(struct, modelDef, new Set(seen));
   if (direct.length > 0) return direct;
   if (seen.has(struct)) return [];
   seen.add(struct);
   const exprs: string[] = [];
   const base = resolveQuerySourceBase(struct, modelDef);
   if (!base) {
      const duck = struct as unknown as { type: string };
      if (duck.type === "query_source") exprs.push("false");
   } else if (!seen.has(base)) {
      const ownExprs = collectAuthorizeExprs(
         ownLevelNoteTexts(base.annotations),
      );
      exprs.push(
         ...(ownExprs.length > 0
            ? ownExprs
            : effectiveAncestorGateExprs(base, modelDef, seen)),
      );
   }
   const composite = resolveCompositeResolvedBase(struct);
   if (composite && !seen.has(composite)) {
      const compositeOwn = collectAuthorizeExprs(
         ownLevelNoteTexts(composite.annotations),
      );
      exprs.push(
         ...(compositeOwn.length > 0
            ? compositeOwn
            : effectiveAncestorGateExprs(composite, modelDef, seen)),
      );
   }
   return exprs;
}
