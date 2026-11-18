// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a stored table keeps its source's gates when read: the one policy,
 * as a table of rows by kind, asked by its three callers.
 *
 * - Plan asks {@link tableKeepsGates} before it builds a table over a gated
 *   source, so a table is never built that cannot be read past its gate
 *   (`gateEnforcedAtRead`, `rollupGateEnforcedAtRead`, and a database
 *   `#@ persist` source's own gate).
 * - The serve verdict asks {@link answerKeepsGates} of a source's own table
 *   on the served leg, and {@link legMayPassGate} with the plan's answer for a
 *   rollup, to say whether a gated entry point sends a table live
 *   (`row_level_gate`), exactly as the router decides it.
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
 *
 * Frozen until the security review (plan question 14): gated sources stay
 * behind `PUBLISHER_MATERIALIZE_GATED_SOURCES`, and no new leg, tier or kind
 * may keep its gates, nor a row be loosened, without that review's sign-off.
 * Rows were loosened outside it by maintainer decision, for consistency,
 * ahead of the review: G3 keeps a source's own row-level gate (an attributed
 * row filter or lock, no inherited row filter) on the served leg for a
 * database `#(materialize)` table, as v1 database does, and for a `storage=`
 * table under exactly the same conditions, v1 `#@ persist` and
 * `#(materialize)` alike; and on the rollup leg for a database rollup whose
 * base's gates are all its own and each proven exact on the rollup (every
 * field a grain dimension), which the gated-sources switch already admits.
 * The review then covers that consistent behaviour. g-mixed, a gated entry
 * point over an ungated stored source, is the same rule on the `storage=`
 * tier: a table proven to hold only ungated rows counts as a G3 table, as a
 * database table does (reviewed in the question-14 security review,
 * including the P2 lock-gated join widening). The rollup
 * leg's join branch over an ungated base's database rollup answers past a
 * gated query source on the existing rollup row (a database table), its gate
 * grafted on the variant (`joinGateDecline` in rollup_variants.ts). Pending
 * that review's sign-off: a rollup, on either tier, keeps a base gate set
 * made only of `#(authorize)` locks reading no row field, own or inherited
 * (`lockOnly`), and the rollup leg keeps an answer on any tier whose entry
 * point's gates read no column (`gatesReadNoColumn`). Also pending it: a
 * `storage=` rollup keeps its base's gates on the same terms as a database
 * one, its own under G3 and any proven one under the switch, read only when
 * its gate columns are stored exactly and in no other zone.
 * `gate_policy_frozen.spec.ts` enumerates every row this file allows and
 * fails on any change, naming the review.
 */

/** The deployment switches gate keeping reads, as one value. */
export interface GateSwitches {
   /** `materialize.enabled` (`PUBLISHER_MATERIALIZE`). */
   readonly materialize: boolean;
   /** `materialize.gatedSources` (`PUBLISHER_MATERIALIZE_GATED_SOURCES`). */
   readonly gatedSources: boolean;
}

/** Every switch gate keeping reads, one entry per field of {@link GateSwitches}. */
export const GATE_SWITCH_KEYS = {
   materialize: true,
   gatedSources: true,
} as const satisfies Record<keyof GateSwitches, true>;

/** What a stored table holds, every kind the policy has a row for. */
export const GATE_KINDS = [
   "persist",
   "materialize",
   "pair",
   "rollup",
   "view",
] as const;
type GateKind = (typeof GATE_KINDS)[number];

/** Where a stored table lives. */
export const GATE_TIERS = ["database", "storage"] as const;
type GateTier = (typeof GATE_TIERS)[number];

/** A stored table over a gated source, as the policy reads it. */
export interface GatedTable {
   readonly kind: GateKind;
   readonly tier: GateTier;
   /**
    * A v1 declaration (`#@ persist`), not a v2 one; for a rollup, one the
    * rollup leg did not plan (a v1 `#@ preaggregate` line is planned as a
    * variant too, so it is not `v1` here).
    */
   readonly v1: boolean;
   /**
    * For a `materialize` or `pair` table: its entry point's gate, as plan
    * classified it against the author's model. For a `rollup`: its base's
    * gates, `attributed` when every one is the base's own (none inherited
    * from a source it extends, none through a join).
    */
   readonly gate?: {
      readonly rowLevel: boolean;
      /** The gate is the entry point's own. */
      readonly attributed: boolean;
      /** The entry point inherits a row filter the served copy cannot graft. */
      readonly inheritedRowFilter: boolean;
      /**
       * For a `rollup`: every base gate is an `#(authorize)` lock whose
       * expression reads only givens and literals, no row field, own or
       * inherited. Such a lock admits or refuses the whole query, decided per
       * request by `govern()` on the live entry point, so pre-grouped rows
       * cannot change its outcome. Absent reads false: fail closed.
       */
      readonly lockOnly?: boolean;
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
   // A v1 table is substituted by Malloy (database) or re-rooted by the
   // served copy, which the router lets past a gate per leg. Either tier
   // keeps its source's own row-level gate under G3.
   persist: (t) => persistKeepsGates(t),
   // Read only through the served copy, which keeps the author's gates;
   // admitted only for the entry point's own row-level gate, with v2's
   // switch, on either tier under G3.
   materialize: (t, s) => materializedKeepsGates(t, s),
   pair: (t, s) => materializedKeepsGates(t, s),
   // A `#(rollup)` rollup's variant redeclares its base with its gates in
   // an overlay of its declaring file, on either tier, proven on the
   // rollup; under the gated-sources switch, any gate so proven. A v1
   // `#@ preaggregate` line read as one alike. A rollup no variant was
   // planned for keeps none.
   //
   // G3 for a rollup on either tier, with the switch off: its base's own
   // gates, each proven exact on the rollup (every field a grain dimension),
   // as a stored source keeps its own row-level gate. At read, a `storage=`
   // rollup's gate columns must also be stored exactly and compared as its
   // source compares them (`answerKeepsGates`, the router's comparison
   // rules).
   //
   // A base whose every gate is a lock reading no row field, own or
   // inherited, on either tier and either switch setting: the lock decides
   // the whole query on the live entry point, and the rollup holds no row a
   // caller the lock admits would not see live.
   rollup: (t, s) =>
      !t.v1 &&
      t.baseGateProven === true &&
      (t.gate?.lockOnly === true || s.gatedSources || ownRowLevelGate(t)),
   // A persisted view's gate is decided at the router, per leg.
   view: () => false,
};

/**
 * G3: the table's own row-level gate (an attributed row filter or lock, no
 * row filter inherited from a source it derives from or extends), which the
 * served copy grafts and `govern()` re-proves on every read, so the table
 * holds every caller's rows and each caller gets live's.
 */
function ownRowLevelGate(t: GatedTable): boolean {
   return (
      t.gate?.rowLevel === true &&
      t.gate.attributed &&
      !t.gate.inheritedRowFilter
   );
}

function materializedKeepsGates(t: GatedTable, s: GateSwitches): boolean {
   // G3 for `#(materialize)`, database or `storage=`, with the gated-sources
   // switch on or off. A maintainer-approved exception made ahead of the
   // question-14 review, for consistency: database and `storage=` behave the
   // same under a source's own row-level gate, and the review covers that.
   return s.materialize && ownRowLevelGate(t);
}

function persistKeepsGates(t: GatedTable): boolean {
   // G3 for a v1 table, database (as since the v1 cutover) or `storage=` on
   // the same conditions, the maintainer-approved exception made ahead of
   // the question-14 review. No switch is read.
   return ownRowLevelGate(t);
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
export const GATE_LEGS = ["view", "rollup", "served"] as const;
type GateLeg = (typeof GATE_LEGS)[number];

/** What the router's caller adds to the switches. */
interface LegGateContext {
   /**
    * The v1 cutover's contracts at read (G3): a served answer reading only
    * G3 tables keeps the gates even with the gated-sources switch off
    * ({@link SERVING_GATE_CONTEXT}).
    */
   readonly databaseReadsKeepGates: boolean;
}

/**
 * The context every model routes with, and the serve verdict judges with:
 * G3 holds at read, so an answer reading only G3 tables keeps the gates.
 */
export const SERVING_GATE_CONTEXT: LegGateContext = {
   databaseReadsKeepGates: true,
};

/** A table an answer read, as the policy judges it. */
export interface TableRead {
   readonly tier: GateTier;
   /**
    * A `storage=` table plan admitted past its source's own row-level gate
    * under G3 (`Binding.ownGateKept`), or a `storage=` rollup admitted past
    * its base's gates. Absent reads false: fail closed.
    */
   readonly ownGateKept?: boolean;
   /**
    * A `storage=` table every source stored in it was proven to hold only
    * ungated rows (`Binding.holdsOnlyUngatedRows`). Absent reads false: fail
    * closed.
    */
   readonly holdsOnlyUngatedRows?: boolean;
   /**
    * The stored types of the table's columns that the entry point's row-level
    * gates read, and every column those are computed from
    * (`serve/gate_reads.ts`). Absent when they could not be listed: a
    * `storage=` table then keeps no gate. A database table is built by the
    * source's own warehouse in the source's own types, so its columns compare
    * as live's do; absent there reads as exact.
    */
   readonly gateColumnTypes?: readonly string[];
   /**
    * The entry point's gates read no column at all: each is an
    * `#(authorize)` lock over givens and literals, decided per request on the
    * live entry point, so no stored row changes its outcome. Set by the
    * router from the author's compile (`serve/gate_reads.ts`); absent reads
    * false.
    */
   readonly gatesReadNoColumn?: boolean;
}

/**
 * Stored types under which a gate's equality is not live's: a binary float
 * (an exact `numeric` or `bigint` stored as one rounds past 2^53, so two
 * tenants' ids compare equal), and a 128-bit integer, which a DuckLake
 * destination keeps at rest as a float. Matched anywhere in the type, so a
 * record or list holding one counts too.
 */
const INEXACT_GATE_TYPE =
   /\b(DOUBLE|FLOAT[0-9]*|REAL|HUGEINT|UHUGEINT|U?INT128)\b/i;

/** Whether a gate compares a column stored as `type` as live compares it. */
export function gateColumnTypeExact(type: string): boolean {
   return !INEXACT_GATE_TYPE.test(type);
}

/**
 * Whether every column the entry point's gates read is stored exactly for
 * its type, so the gate admits on the stored table exactly the rows it
 * admits live. Fail closed: a `storage=` table whose gate columns could not
 * be listed is not.
 */
export function gateColumnsExact(t: TableRead): boolean {
   if (t.gateColumnTypes === undefined) return t.tier === "database";
   return t.gateColumnTypes.every(gateColumnTypeExact);
}

/**
 * Whether a table an answer read is a G3 table: a database one (as since
 * the v1 cutover), a `storage=` one plan admitted past its own gate, or a
 * `storage=` one that holds only ungated rows (g-mixed: a gated entry point
 * over an ungated stored source is served from its table on every tier, the
 * gate grafted and proven on the answer as on a database table).
 */
function g3Table(t: TableRead): boolean {
   return (
      t.tier === "database" ||
      t.ownGateKept === true ||
      t.holdsOnlyUngatedRows === true
   );
}

/**
 * Whether a table a rollup answer read keeps the gates under G3: a database
 * one, a `storage=` one plan admitted past its base's own gates
 * (`ownGateKept`), or any one when the entry point's gates read no column
 * (locks only).
 */
function g3RollupTable(t: TableRead): boolean {
   return (
      t.tier === "database" ||
      t.ownGateKept === true ||
      t.gatesReadNoColumn === true
   );
}

/**
 * Whether a leg may be attempted past an entry-point gate. Each leg keeps the
 * author's gates on its own compile (the served copy's text, a variant's
 * redeclared base), under the gated-sources switch; the served copy and the
 * rollup leg also under `databaseReadsKeepGates`, decided per answer by
 * {@link answerKeepsGates}.
 */
export function legMayPassGate(
   leg: GateLeg,
   s: GateSwitches,
   ctx: LegGateContext,
): boolean {
   switch (leg) {
      case "view":
         return s.gatedSources;
      case "rollup":
      case "served":
         return s.gatedSources || ctx.databaseReadsKeepGates;
      default:
         return false;
   }
}

/**
 * Whether an answer a leg produced past an entry-point gate keeps the gates,
 * given the tables it read. Never when a column the gates read is stored
 * inexactly ({@link gateColumnsExact}), under any switch. Otherwise always
 * under the gated-sources switch; under G3 alone, only when it read at least
 * one table and every one is a G3 table: for the served copy, database,
 * `storage=` admitted past its own gate, or `storage=` holding only ungated
 * rows; for the rollup leg, database, `storage=` admitted past its base's
 * own gates, or any tier when the entry point's gates are locks reading no
 * column.
 */
export function answerKeepsGates(
   leg: GateLeg,
   tablesRead: readonly TableRead[],
   s: GateSwitches,
   ctx: LegGateContext,
): boolean {
   if (!legMayPassGate(leg, s, ctx)) return false;
   if (!tablesRead.every(gateColumnsExact)) return false;
   if (s.gatedSources) return true;
   if (!ctx.databaseReadsKeepGates || tablesRead.length === 0) return false;
   if (leg === "served") return tablesRead.every(g3Table);
   return leg === "rollup" && tablesRead.every(g3RollupTable);
}
