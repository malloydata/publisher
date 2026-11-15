// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a stored table keeps its source's gates when read: the one policy,
 * as a table of rows by kind, asked by its three callers.
 *
 * - Plan asks {@link tableKeepsGates} before it builds a table over a gated
 *   source, so a table is never built that cannot be read past its gate
 *   (`gateEnforcedAtRead`, `rollupGateEnforcedAtRead`).
 * - The serve verdict asks {@link legMayPassGate} for the served copy and the
 *   plan's answer for a rollup, to say whether a gated entry point sends a
 *   table live (`row_level_gate`).
 * - The router asks {@link legMayPassGate} before it attempts a leg under an
 *   entry-point gate, and {@link answerKeepsGates} of the tables the answer
 *   read.
 *
 * Fail closed: a kind or leg with no row keeps no gate. The policy only
 * permits an attempt. The proof stays in `govern()`, which grafts the live
 * entry point's gates onto the routed compile and re-proves them; an answer
 * whose gate set it cannot prove is denied, so a wrong row is a denial, never
 * a leak.
 *
 * Pure: the switches are read once by the caller's shell (`gateSwitches()`
 * in `config.ts`) and passed in.
 */

/** The deployment switches gate keeping reads, as one value. */
export interface GateSwitches {
   /** `materialize.enabled` (`PUBLISHER_MATERIALIZE`). */
   readonly materialize: boolean;
   /** `materialize.gatedSources` (`PUBLISHER_MATERIALIZE_GATED_SOURCES`). */
   readonly gatedSources: boolean;
   /**
    * Whether a `storage=` rollup over a gated base may be read past its gate:
    * not until the managed-tier review, which covers `#(materialize)` tables
    * and is proposed to cover rollups too (a stored rollup puts tenant-keyed
    * aggregates in a storage destination). No setting turns it on; only a
    * test does.
    */
   readonly gatedStorageRollupsReviewed: boolean;
}

/** What a stored table holds. */
type GateKind = "persist" | "materialize" | "pair" | "rollup" | "view";

/** Where a stored table lives. */
type GateTier = "colocated" | "storage";

/** A stored table over a gated source, as the policy reads it. */
export interface GatedTable {
   readonly kind: GateKind;
   readonly tier: GateTier;
   /** A v1 declaration (`#@ persist`, `#@ preaggregate`), not a v2 one. */
   readonly v1: boolean;
   /**
    * For a `materialize` or `pair` table: its entry point's gate, as plan
    * classified it against the author's model.
    */
   readonly gate?: {
      readonly rowLevel: boolean;
      /** The gate is the entry point's own. */
      readonly attributed: boolean;
      /** The entry point inherits a row filter the served copy cannot graft. */
      readonly inheritedRowFilter: boolean;
   };
   /**
    * For a `rollup`: its base is gated and every base gate was proven exact
    * on the rollup at plan (each field a grain dimension, no gated join).
    */
   readonly baseGateProven?: boolean;
}

type Row = (t: GatedTable, s: GateSwitches) => boolean;

/**
 * One row per kind: whether a table of that kind, read past its source's
 * gate, keeps the gate.
 */
const ROWS: Record<GateKind, Row> = {
   // A v1 table is substituted by Malloy (colocated) or re-rooted by the
   // served copy, which the router lets past a gate per leg; plan refuses a
   // gated one by its own tier checks, never by keeping its gates.
   persist: () => false,
   // Read only through the served copy, which keeps the author's gates;
   // admitted only for the entry point's own row-level gate, with v2's switch
   // and the gated-sources switch (the managed-tier review).
   materialize: (t, s) => materializedKeepsGates(t, s),
   pair: (t, s) => materializedKeepsGates(t, s),
   // A `#(preaggregate)` rollup's variant redeclares its base with its gates,
   // proven on the rollup; colocated under the gated-sources switch, in a
   // storage destination only once that is reviewed. A v1 rollup keeps none.
   rollup: (t, s) =>
      !t.v1 &&
      t.baseGateProven === true &&
      s.gatedSources &&
      (t.tier === "colocated" || s.gatedStorageRollupsReviewed),
   // A persisted view's gate is decided at the router, per leg.
   view: () => false,
};

function materializedKeepsGates(t: GatedTable, s: GateSwitches): boolean {
   return (
      s.materialize &&
      s.gatedSources &&
      t.gate?.rowLevel === true &&
      t.gate.attributed &&
      !t.gate.inheritedRowFilter
   );
}

/**
 * Whether a table over a gated source keeps the gate when read, so it may be
 * built and served past it. False for anything with no row.
 */
export function tableKeepsGates(t: GatedTable, s: GateSwitches): boolean {
   const row: Row | undefined = Object.hasOwn(ROWS, t.kind)
      ? ROWS[t.kind]
      : undefined;
   return row ? row(t, s) === true : false;
}

/** A routed leg. */
type GateLeg = "view" | "rollup" | "served";

/** What the router's caller adds to the switches. */
interface LegGateContext {
   /**
    * The v1 cutover's contracts at read for colocated tables (G3): a served
    * answer reading only colocated tables keeps the gates even with the
    * gated-sources switch off, since a colocated table never leaves the
    * author's warehouse. Every model passes it; the serve verdict does not.
    */
   readonly colocatedReadsKeepGates: boolean;
}

/**
 * Whether a leg may be attempted past an entry-point gate. Each leg keeps the
 * author's gates on its own compile (the served copy's text, a variant's
 * redeclared base), under the gated-sources switch; the served copy also
 * under G3, decided per answer by {@link answerKeepsGates}.
 */
export function legMayPassGate(
   leg: GateLeg,
   s: GateSwitches,
   ctx: LegGateContext,
): boolean {
   switch (leg) {
      case "view":
      case "rollup":
         return s.gatedSources;
      case "served":
         return s.gatedSources || ctx.colocatedReadsKeepGates;
      default:
         return false;
   }
}

/**
 * Whether an answer a leg produced past an entry-point gate keeps the gates,
 * given the tables it read: always under the gated-sources switch; for the
 * served copy under G3 alone, only when it read at least one table and every
 * one is colocated.
 */
export function answerKeepsGates(
   leg: GateLeg,
   tablesRead: readonly { readonly tier: GateTier }[],
   s: GateSwitches,
   ctx: LegGateContext,
): boolean {
   if (!legMayPassGate(leg, s, ctx)) return false;
   if (s.gatedSources) return true;
   return (
      leg === "served" &&
      ctx.colocatedReadsKeepGates &&
      tablesRead.length > 0 &&
      tablesRead.every((t) => t.tier === "colocated")
   );
}
