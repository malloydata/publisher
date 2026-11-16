// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `gatePlan`: the ONE gate-inheritance walk. Which `#(authorize)` /
 * `#(access_filter)` gates apply to a struct as an entry point, per route, and
 * where each was read. Three consumers read it through thin adapters:
 *
 *  - `collectEntryPointGates` (`../gate_classification.ts`): the request path,
 *    what enforcement decides and grafts;
 *  - `gateExprsForRoute` (`../gate_registry_walk.ts`): package load, what
 *    `source_extraction.ts` validates and reports per source;
 *  - `resolveGates` (`../entities/access_gates.ts`): the value index, over raw
 *    `structDef` JSON.
 *
 * The walk, per route, with its own `seen` set (struct identity) and its own
 * own-wins-over-ancestor decision, so one route's note never sheds the other
 * route's inherited gate:
 *
 *  1. the struct's own notes on this route, else the nearest ancestor's: the
 *     `annotations.inherits` chain, then the `sourceRegistry` declaration it
 *     was created from ({@link ancestorExprs});
 *  2. for a `query_source` (`Z is X -> {...}`), PLUS the base it derives from
 *     (derivation is additive, so `mine is locked -> {...}` cannot launder
 *     `locked`'s gate), walked from step 1;
 *  3. for a `query_source` over a composite, PLUS the member Malloy resolved
 *     for this derivation, walked from step 1 with the base's own notes
 *     subtracted by identity (Malloy copies them onto the member).
 *
 * Not walked: joined sources and a derivation's inner-pipeline joins (a gate
 * governs the source it is declared on; caller joins are gated separately).
 *
 * The one option is the composite rule. `resolved` (enforcement and load):
 * a bare composite reports its own gate only, since Malloy resolves one
 * member per query and that member is gated on the query path. `anyMember`
 * (the value index, which cannot know the branch): a bare composite with no
 * gate of its own reports its FIRST GATED MEMBER's, a deliberate over-report.
 *
 * Fail-closed: anything the walk cannot read is a `false` entry carrying a
 * `failure`, never a missing gate. Each adapter turns a failure into what its
 * consumer did before the walks were one: a `false` gate (enforcement), a
 * rethrown parse error (load), or a throw (the index).
 */

import type { ModelDef, SourceDef } from "@malloydata/malloy";
import {
   ownLevelNotes,
   type AnnotationNote,
   type AnnotationsDef,
} from "../annotations";
import { collectAuthorizeExprsForRoute } from "../authorize";
import { CANONICAL_AUTHORIZE_ROUTES } from "../authorize_routes";
import {
   resolveCompositeResolvedBase,
   resolveDeclaredSource,
   resolveQuerySourceBase,
} from "./ir/source_graph";

/**
 * Link budget for the ancestor walk. Exceeding it denies (the chain wasn't
 * read to its end), so it only has to be larger than any real chain.
 */
export const ANCESTOR_WALK_MAX_DEPTH = 32;

/** The derivation hops a walk takes; see `./ir/source_graph.ts`. */
export interface GateGraph {
   /** The `sourceRegistry` declaration `struct` was created from. */
   declaredSource(
      struct: SourceDef,
   ):
      | { kind: "resolved"; source: SourceDef }
      | { kind: "none" }
      | { kind: "unresolvable" };
   /**
    * A `query_source`'s base. `byName`: the ref is a name (counted against
    * `maxNameHops`). Called only for a `query_source`.
    */
   queryBase(
      struct: SourceDef,
   ):
      | { kind: "resolved"; base: SourceDef; byName: boolean }
      | { kind: "unresolvable"; byName: boolean; message: string };
   /** A `query_source`'s resolved composite member, if any. */
   compositeResolved(struct: SourceDef): SourceDef | undefined;
}

/**
 * The graph over a compiled `ModelDef`: registry declarations, and a
 * `query_source` base by `contents` name or inline struct (a `SourceDef`
 * either way).
 */
export function modelDefGateGraph(modelDef: ModelDef | undefined): GateGraph {
   return {
      declaredSource: (struct) => resolveDeclaredSource(struct, modelDef),
      queryBase: (struct) => {
         const ref = (struct as unknown as { query?: { structRef?: unknown } })
            .query?.structRef;
         const byName = typeof ref === "string";
         const base = resolveQuerySourceBase(struct, modelDef);
         return base
            ? { kind: "resolved", base, byName }
            : {
                 kind: "unresolvable",
                 byName,
                 message: "query_source has an unresolvable structRef",
              };
      },
      compositeResolved: (struct) => resolveCompositeResolvedBase(struct),
   };
}

/** Why a walk could not read a gate; the entry carries `["false"]`. */
type GateFailure =
   /** A note at this level (own or ancestor) did not parse. */
   | { kind: "unreadable"; error: unknown }
   /** The `inherits` chain, or the registry chain, ran past its budget. */
   | { kind: "ancestor_exhausted"; message: string }
   /** A registry link exists and does not yield a source. */
   | { kind: "ancestor_unresolvable"; message: string }
   /** A `query_source` base that does not resolve, or a name chain past its budget. */
   | { kind: "base_unresolvable"; message: string };

/** One gate the walk found, read at one level. */
interface GatePlanGate {
   kind: "gate";
   route: string;
   /** The level's label: `as ?? name`. */
   label: string;
   /** The expressions, one declaring level's conjunction; `["false"]` on a failure. */
   exprs: string[];
   /** Read off an ancestor of the level (inherits or registry), not its own notes. */
   fromAncestor: boolean;
   /** The level is the entry point itself (the run target or its resolved branch). */
   treatAsOwn: boolean;
   /** The entry point the gate applies to; absent for an unresolvable base. */
   entryPoint?: SourceDef;
   failure?: GateFailure;
}

/** A shape the walk does not descend into; only the index refuses on it. */
interface GatePlanUnwalked {
   kind: "unwalked";
   route: string;
   reason: "sql_embed" | "unsupported_type";
   message: string;
}

type GatePlanStep = GatePlanGate | GatePlanUnwalked;

interface GatePlan {
   /** Every step, per route in `routes` order, each in walk order. */
   steps: GatePlanStep[];
   /** The walk met a composite (any route), so which branch gates a query is unknown. */
   metComposite: boolean;
}

interface GatePlanOptions {
   composite: "resolved" | "anyMember";
   /** The routes to walk, in order. Default {@link CANONICAL_AUTHORIZE_ROUTES}. */
   routes?: readonly string[];
   /**
    * One level's gate exprs for `route`; throws on a note that does not parse
    * (the walk records an `unreadable` failure). Default
    * `collectAuthorizeExprsForRoute`, the query path's own reader.
    */
   readRoute?: (texts: string[], route: string) => string[];
   /** Name-ref `query_source` hops along one path before a failure. Default unbounded. */
   maxNameHops?: number;
   /** Structs already visited; copied per route. */
   seen?: ReadonlySet<SourceDef>;
   /** The root is the entry point itself. */
   treatAsOwnGate?: boolean;
   /** The entry point gates apply to; default the root. */
   entryPoint?: SourceDef;
   /** Root own notes subtracted by identity. */
   excludeNotes?: readonly AnnotationNote[];
   /** Replaces the root's own note texts (a join call site's pre-filtered taglines). */
   ownTexts?: string[];
}

interface Walk {
   graph: GateGraph;
   composite: "resolved" | "anyMember";
   readRoute: (texts: string[], route: string) => string[];
   maxNameHops: number;
   seen: Set<SourceDef>;
   metComposite: boolean;
}

const TERMINAL_TYPES = new Set(["table", "record", "array"]);

/** Structural stand-in for `@malloydata/malloy`'s `isSegmentSQL` (`'sql' in f`). */
const isSQLStringSegment = (segment: unknown): boolean =>
   typeof segment === "object" && segment !== null && "sql" in segment;

const levelTexts = (annotations: AnnotationsDef | undefined): string[] =>
   ownLevelNotes(annotations).map((note) => note.text);

/** The one gate-inheritance walk; see the module doc. */
export function gatePlan(
   struct: SourceDef | undefined,
   graph: GateGraph,
   options: GatePlanOptions,
): GatePlan {
   const steps: GatePlanStep[] = [];
   let metComposite = false;
   for (const route of options.routes ?? CANONICAL_AUTHORIZE_ROUTES) {
      const walk: Walk = {
         graph,
         composite: options.composite,
         readRoute: options.readRoute ?? collectAuthorizeExprsForRoute,
         maxNameHops: options.maxNameHops ?? Number.POSITIVE_INFINITY,
         seen: new Set(options.seen ?? []),
         metComposite: false,
      };
      steps.push(
         ...walkRoute(walk, struct, route, {
            treatAsOwn: options.treatAsOwnGate ?? false,
            entryPoint: options.entryPoint ?? struct,
            excludeNotes: options.excludeNotes ?? [],
            walkMembers: true,
            nameHops: 0,
            ownTexts: options.ownTexts,
         }),
      );
      metComposite ||= walk.metComposite;
   }
   return { steps, metComposite };
}

interface Visit {
   treatAsOwn: boolean;
   entryPoint: SourceDef | undefined;
   excludeNotes: readonly AnnotationNote[];
   /** False only for a `query_source`'s own base: Malloy already resolved its branch. */
   walkMembers: boolean;
   nameHops: number;
   ownTexts?: string[];
}

function walkRoute(
   walk: Walk,
   struct: SourceDef | undefined,
   route: string,
   visit: Visit,
): GatePlanStep[] {
   if (!struct) return [];
   // A restricted visit of a composite under `anyMember` recurses nowhere, so
   // it must not claim the struct: a later unrestricted visit WOULD walk its
   // members, and claiming it would launder a gated member away.
   const claimsSeen =
      walk.composite === "resolved" ||
      visit.walkMembers ||
      struct.type !== "composite";
   if (claimsSeen) {
      if (walk.seen.has(struct)) return [];
      walk.seen.add(struct);
   }

   const steps: GatePlanStep[] = [];
   const label = (struct as { as?: string }).as ?? struct.name;
   const level = readLevel(walk, struct, route, visit);
   if (level.exprs.length > 0) {
      steps.push({
         kind: "gate",
         route,
         label,
         exprs: level.exprs,
         fromAncestor: level.fromAncestor,
         treatAsOwn: visit.treatAsOwn,
         entryPoint: visit.entryPoint,
         ...(level.failure ? { failure: level.failure } : {}),
      });
   }

   // Joined sources are deliberately not walked (see the module doc).

   if (struct.type === "composite") {
      walk.metComposite = true;
      if (
         walk.composite === "anyMember" &&
         visit.walkMembers &&
         steps.length === 0
      ) {
         for (const member of (struct as unknown as { sources?: SourceDef[] })
            .sources ?? []) {
            const memberSteps = walkRoute(walk, member, route, {
               ...visit,
               treatAsOwn: false,
               excludeNotes: [],
               walkMembers: true,
               ownTexts: undefined,
            });
            if (memberSteps.length > 0) return memberSteps;
         }
      }
      return steps;
   }
   if (TERMINAL_TYPES.has(struct.type)) return steps;
   if (struct.type === "sql_select") {
      // A plain `duckdb.sql(...)` has no query-bearing segment. A segment that
      // is not raw SQL text embeds a query/source this walk does not descend
      // into.
      const segments =
         (struct as unknown as { selectSegments?: unknown[] }).selectSegments ??
         [];
      if (segments.some((segment) => !isSQLStringSegment(segment))) {
         steps.push({
            kind: "unwalked",
            route,
            reason: "sql_embed",
            message:
               "resolveGates does not walk an embedded query/source in sql_select.selectSegments",
         });
      }
      return steps;
   }
   if (struct.type !== "query_source") {
      steps.push({
         kind: "unwalked",
         route,
         reason: "unsupported_type",
         message: `resolveGates does not support struct.type '${struct.type}'`,
      });
      return steps;
   }

   // A `query_source` derives from something by construction, so a base that
   // cannot be read is IR we failed to read, not an ungated source: deny.
   // Recorded even when the struct has its own gate: skipping the base would
   // drop its gate.
   const hop = walk.graph.queryBase(struct);
   const baseFailure = (message: string): GatePlanGate => ({
      kind: "gate",
      route,
      label,
      exprs: ["false"],
      fromAncestor: false,
      treatAsOwn: false,
      failure: { kind: "base_unresolvable", message },
   });
   const base = hop.kind === "resolved" ? hop.base : undefined;
   if (hop.byName && visit.nameHops >= walk.maxNameHops) {
      steps.push(
         baseFailure(
            `query_source structRef name chain exceeds ${walk.maxNameHops} levels`,
         ),
      );
   } else if (hop.kind === "resolved") {
      steps.push(
         ...walkRoute(walk, hop.base, route, {
            treatAsOwn: false,
            entryPoint: visit.entryPoint,
            excludeNotes: [],
            walkMembers: false,
            nameHops: hop.byName ? visit.nameHops + 1 : visit.nameHops,
         }),
      );
   } else {
      steps.push(baseFailure(hop.message));
   }
   // Over a composite, the member Malloy resolved for this derivation
   // (`query.compositeResolvedSourceDef`, not `query.structRef`).
   const resolved = walk.graph.compositeResolved(struct);
   if (resolved) {
      walk.metComposite = true;
      steps.push(
         ...walkRoute(walk, resolved, route, {
            treatAsOwn: false,
            entryPoint: visit.entryPoint,
            // Identity-subtract the base's own notes, which Malloy copied on.
            excludeNotes: base ? ownLevelNotes(base.annotations) : [],
            walkMembers: true,
            nameHops: visit.nameHops,
         }),
      );
   }
   return steps;
}

/**
 * One level's gate: its own notes on `route` (minus `excludeNotes`), else the
 * nearest ancestor's. A note that does not parse fails the level closed.
 */
function readLevel(
   walk: Walk,
   struct: SourceDef,
   route: string,
   visit: Visit,
): { exprs: string[]; fromAncestor: boolean; failure?: GateFailure } {
   const texts =
      visit.ownTexts ??
      ownLevelNotes(struct.annotations)
         .filter((note) => !visit.excludeNotes.includes(note))
         .map((note) => note.text);
   try {
      const own = walk.readRoute(texts, route);
      if (own.length > 0) return { exprs: own, fromAncestor: false };
      const ancestor = ancestorExprs(walk, struct, route, new Set());
      return {
         exprs: ancestor.exprs,
         fromAncestor: ancestor.exprs.length > 0,
         ...(ancestor.failure ? { failure: ancestor.failure } : {}),
      };
   } catch (error) {
      return {
         exprs: ["false"],
         fromAncestor: false,
         failure: { kind: "unreadable", error },
      };
   }
}

/**
 * The gate a struct inherits, used when it declares none of its own on
 * `route`.
 *
 * Once the deriving statement carries ANY annotation, Malloy no longer leaves
 * the base's annotations at top level: an annotated `extend {}` demotes them
 * to `annotations.inherits`; an annotated join replaces them, leaving
 * `sourceID`/`referenceID` (the registry) as the only link. All links are
 * followed, nearest first; the first ancestor declaring a gate wins.
 *
 * Fail-closed: an exhausted walk or an unresolvable registry link is
 * `["false"]`, never `[]`.
 *
 * A cycle returns `[]`, sound only because every caller read the struct's
 * OWN gate first: on an A->B->A cycle the far end was already read. Do not
 * make it `["false"]` (diamonds and self-references legitimately revisit).
 */
function ancestorExprs(
   walk: Walk,
   struct: SourceDef,
   route: string,
   seen: Set<SourceDef>,
): { exprs: string[]; failure?: GateFailure } {
   let inherited = struct.annotations?.inherits;
   for (let depth = 0; inherited && depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const exprs = walk.readRoute(levelTexts(inherited), route);
      if (exprs.length > 0) return { exprs };
      inherited = inherited.inherits;
   }
   if (inherited) {
      return {
         exprs: ["false"],
         failure: {
            kind: "ancestor_exhausted",
            message: `annotations.inherits chain exceeds ${ANCESTOR_WALK_MAX_DEPTH} levels`,
         },
      };
   }
   seen.add(struct);
   if (seen.size > ANCESTOR_WALK_MAX_DEPTH) {
      return {
         exprs: ["false"],
         failure: {
            kind: "ancestor_exhausted",
            message: `source registry chain exceeds ${ANCESTOR_WALK_MAX_DEPTH} levels`,
         },
      };
   }
   const declared = walk.graph.declaredSource(struct);
   if (declared.kind === "unresolvable") {
      return {
         exprs: ["false"],
         failure: {
            kind: "ancestor_unresolvable",
            message: "a source registry link did not resolve to a source",
         },
      };
   }
   if (declared.kind === "none" || seen.has(declared.source)) {
      return { exprs: [] };
   }
   const exprs = walk.readRoute(levelTexts(declared.source.annotations), route);
   return exprs.length > 0
      ? { exprs }
      : ancestorExprs(walk, declared.source, route, seen);
}
