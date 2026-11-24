// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The gate policy's frozen leg rows, and the reads they are judged over: the
 * one list `gate_policy_frozen.spec.ts` holds the policy to, and
 * `admission_characterization.spec.ts` holds admission and the serve
 * verdict to. Data for specs; read by nothing at run time. A row
 * changes only with the security review's sign-off.
 */

import {
   GATE_SWITCH_KEYS,
   type GateSwitches,
   type TableRead,
} from "./gate_policy";

/** Every setting of every switch, from the switch keys. */
export function* switches(): Generator<GateSwitches> {
   const keys = Object.keys(GATE_SWITCH_KEYS) as (keyof GateSwitches)[];
   for (let bits = 0; bits < 1 << keys.length; bits++) {
      yield Object.fromEntries(
         keys.map((k, i) => [k, ((bits >> i) & 1) === 1]),
      ) as unknown as GateSwitches;
   }
}

/** One switch setting as a frozen row spells it (`mz=1 gs=0`). */
export function switchesLabel(x: GateSwitches): string {
   return (Object.keys(GATE_SWITCH_KEYS) as (keyof GateSwitches)[])
      .map((k) => `${SHORT[k] ?? k}=${+x[k]}`)
      .join(" ");
}
const SHORT: Partial<Record<keyof GateSwitches, string>> = {
   materialize: "mz",
   gatedSources: "gs",
};

// A `storage=` read carries the stored types of its gate columns, as the
// router lists them, and whether its column contract records every one of
// them exact (`gateExact`); exact unless the row's name says otherwise.
const exact = {
   gateColumnTypes: ["BIGINT", "VARCHAR"],
   gateColumnsExactRecorded: true,
};
const inexact = (type: string) => ({
   gateColumnTypes: [type],
   gateColumnsExactRecorded: false,
});
/** The tables each named read of a frozen row read. */
export const GATE_READS: Readonly<Record<string, readonly TableRead[]>> = {
   none: [],
   database: [{ tier: "database" as const }],
   storage: [{ tier: "storage" as const, ...exact }],
   mixed: [
      { tier: "database" as const },
      { tier: "storage" as const, ...exact },
   ],
   // A `storage=` table plan admitted past its own gate (own-gate keeping), alone,
   // beside a database one, and beside one it did not admit.
   own: [{ tier: "storage" as const, ownGateKept: true, ...exact }],
   own_database: [
      { tier: "database" as const },
      { tier: "storage" as const, ownGateKept: true, ...exact },
   ],
   own_storage: [
      { tier: "storage" as const, ownGateKept: true, ...exact },
      { tier: "storage" as const, ...exact },
   ],
   // A gate column stored as a float or a 128-bit
   // integer, or one whose type was not listed, keeps no answer under any
   // switch, on either tier.
   own_inexact: [
      {
         tier: "storage" as const,
         ownGateKept: true,
         ...inexact("DOUBLE"),
      },
   ],
   own_hugeint: [
      {
         tier: "storage" as const,
         ownGateKept: true,
         ...inexact("HUGEINT"),
      },
   ],
   own_unlisted: [{ tier: "storage" as const, ownGateKept: true }],
   // A gate column stored as a DuckDB DOUBLE that its build recorded
   // `gateExact` (a copy of a warehouse double compared as DuckDB compares
   // it): exact, kept as `own` is. One the build recorded not exact (a copy
   // of an exact number, of a 4-byte float, or a column it computed) is not.
   // (What a build records: column_contract.spec.ts and gate_policy.spec.ts.)
   own_binary64: [
      {
         tier: "storage" as const,
         ownGateKept: true,
         gateColumnTypes: ["DOUBLE"],
         gateColumnsExactRecorded: true,
      },
   ],
   own_double_of_decimal: [
      {
         tier: "storage" as const,
         ownGateKept: true,
         gateColumnTypes: ["DOUBLE"],
         gateColumnsExactRecorded: false,
      },
   ],
   own_double_of_float4: [
      {
         tier: "storage" as const,
         ownGateKept: true,
         gateColumnTypes: ["DOUBLE"],
         gateColumnsExactRecorded: false,
      },
   ],
   own_double_computed: [
      {
         tier: "storage" as const,
         ownGateKept: true,
         gateColumnTypes: ["DOUBLE"],
         gateColumnsExactRecorded: false,
      },
   ],
   database_inexact: [{ tier: "database" as const, ...inexact("DOUBLE") }],
   // A `storage=` table proven to hold only ungated rows (the ungated-rows rule),
   // alone, beside a database one, beside one admitted past its own
   // gate, and beside one proven neither way.
   ungated: [
      { tier: "storage" as const, holdsOnlyUngatedRows: true, ...exact },
   ],
   ungated_database: [
      { tier: "database" as const },
      { tier: "storage" as const, holdsOnlyUngatedRows: true, ...exact },
   ],
   ungated_own: [
      { tier: "storage" as const, ownGateKept: true, ...exact },
      { tier: "storage" as const, holdsOnlyUngatedRows: true, ...exact },
   ],
   ungated_storage: [
      { tier: "storage" as const, holdsOnlyUngatedRows: true, ...exact },
      { tier: "storage" as const, ...exact },
   ],
   // The entry point's gates are locks reading no column: alone, beside
   // a database table, beside a `storage=` one without the mark, and
   // with an inexact column listed anyway (never kept).
   locks: [
      {
         tier: "storage" as const,
         gatesReadNoColumn: true,
         gateColumnTypes: [],
         gateColumnsExactRecorded: true,
      },
   ],
   locks_database: [
      { tier: "database" as const },
      {
         tier: "storage" as const,
         gatesReadNoColumn: true,
         gateColumnTypes: [],
         gateColumnsExactRecorded: true,
      },
   ],
   locks_storage: [
      {
         tier: "storage" as const,
         gatesReadNoColumn: true,
         gateColumnTypes: [],
         gateColumnsExactRecorded: true,
      },
      { tier: "storage" as const, ...exact },
   ],
   locks_inexact: [
      {
         tier: "storage" as const,
         gatesReadNoColumn: true,
         ...inexact("DOUBLE"),
      },
   ],
   locks_unlisted: [{ tier: "storage" as const, gatesReadNoColumn: true }],
};

// prettier-ignore
export const FROZEN_LEG_ROWS: readonly string[] = [
   // Own-gate keeping for a database rollup: the rollup leg past a gate with the switch
   // off, kept only for an answer that read a database rollup.
   "may rollup mz=0 gs=0 g3=1",
   "keeps rollup read=database mz=0 gs=0 g3=1",
   "may rollup mz=1 gs=0 g3=1",
   "keeps rollup read=database mz=1 gs=0 g3=1",
   "may view mz=0 gs=1 g3=0",
   "keeps view read=none mz=0 gs=1 g3=0",
   "keeps view read=database mz=0 gs=1 g3=0",
   "keeps view read=storage mz=0 gs=1 g3=0",
   "keeps view read=mixed mz=0 gs=1 g3=0",
   "keeps view read=own mz=0 gs=1 g3=0",
   "keeps view read=own_database mz=0 gs=1 g3=0",
   "keeps view read=own_storage mz=0 gs=1 g3=0",
   "may view mz=0 gs=1 g3=1",
   "keeps view read=none mz=0 gs=1 g3=1",
   "keeps view read=database mz=0 gs=1 g3=1",
   "keeps view read=storage mz=0 gs=1 g3=1",
   "keeps view read=mixed mz=0 gs=1 g3=1",
   "keeps view read=own mz=0 gs=1 g3=1",
   "keeps view read=own_database mz=0 gs=1 g3=1",
   "keeps view read=own_storage mz=0 gs=1 g3=1",
   "may view mz=1 gs=1 g3=0",
   "keeps view read=none mz=1 gs=1 g3=0",
   "keeps view read=database mz=1 gs=1 g3=0",
   "keeps view read=storage mz=1 gs=1 g3=0",
   "keeps view read=mixed mz=1 gs=1 g3=0",
   "keeps view read=own mz=1 gs=1 g3=0",
   "keeps view read=own_database mz=1 gs=1 g3=0",
   "keeps view read=own_storage mz=1 gs=1 g3=0",
   "may view mz=1 gs=1 g3=1",
   "keeps view read=none mz=1 gs=1 g3=1",
   "keeps view read=database mz=1 gs=1 g3=1",
   "keeps view read=storage mz=1 gs=1 g3=1",
   "keeps view read=mixed mz=1 gs=1 g3=1",
   "keeps view read=own mz=1 gs=1 g3=1",
   "keeps view read=own_database mz=1 gs=1 g3=1",
   "keeps view read=own_storage mz=1 gs=1 g3=1",
   "may rollup mz=0 gs=1 g3=0",
   "keeps rollup read=none mz=0 gs=1 g3=0",
   "keeps rollup read=database mz=0 gs=1 g3=0",
   "may rollup mz=0 gs=1 g3=1",
   "keeps rollup read=none mz=0 gs=1 g3=1",
   "keeps rollup read=database mz=0 gs=1 g3=1",
   "may rollup mz=1 gs=1 g3=0",
   "keeps rollup read=none mz=1 gs=1 g3=0",
   "keeps rollup read=database mz=1 gs=1 g3=0",
   "may rollup mz=1 gs=1 g3=1",
   "keeps rollup read=none mz=1 gs=1 g3=1",
   "keeps rollup read=database mz=1 gs=1 g3=1",
   "may served mz=0 gs=0 g3=1",
   "keeps served read=database mz=0 gs=0 g3=1",
   "keeps served read=own mz=0 gs=0 g3=1",
   "keeps served read=own_database mz=0 gs=0 g3=1",
   "may served mz=1 gs=0 g3=1",
   "keeps served read=database mz=1 gs=0 g3=1",
   "keeps served read=own mz=1 gs=0 g3=1",
   "keeps served read=own_database mz=1 gs=0 g3=1",
   "may served mz=0 gs=1 g3=0",
   "keeps served read=none mz=0 gs=1 g3=0",
   "keeps served read=database mz=0 gs=1 g3=0",
   "keeps served read=storage mz=0 gs=1 g3=0",
   "keeps served read=mixed mz=0 gs=1 g3=0",
   "keeps served read=own mz=0 gs=1 g3=0",
   "keeps served read=own_database mz=0 gs=1 g3=0",
   "keeps served read=own_storage mz=0 gs=1 g3=0",
   "may served mz=0 gs=1 g3=1",
   "keeps served read=none mz=0 gs=1 g3=1",
   "keeps served read=database mz=0 gs=1 g3=1",
   "keeps served read=storage mz=0 gs=1 g3=1",
   "keeps served read=mixed mz=0 gs=1 g3=1",
   "keeps served read=own mz=0 gs=1 g3=1",
   "keeps served read=own_database mz=0 gs=1 g3=1",
   "keeps served read=own_storage mz=0 gs=1 g3=1",
   "may served mz=1 gs=1 g3=0",
   "keeps served read=none mz=1 gs=1 g3=0",
   "keeps served read=database mz=1 gs=1 g3=0",
   "keeps served read=storage mz=1 gs=1 g3=0",
   "keeps served read=mixed mz=1 gs=1 g3=0",
   "keeps served read=own mz=1 gs=1 g3=0",
   "keeps served read=own_database mz=1 gs=1 g3=0",
   "keeps served read=own_storage mz=1 gs=1 g3=0",
   "may served mz=1 gs=1 g3=1",
   "keeps served read=none mz=1 gs=1 g3=1",
   "keeps served read=database mz=1 gs=1 g3=1",
   "keeps served read=storage mz=1 gs=1 g3=1",
   "keeps served read=mixed mz=1 gs=1 g3=1",
   "keeps served read=own mz=1 gs=1 g3=1",
   "keeps served read=own_database mz=1 gs=1 g3=1",
   "keeps served read=own_storage mz=1 gs=1 g3=1",
   // An entry point whose gates are locks reading no column
   // (`gatesReadNoColumn`): kept wherever a `storage=` read already was,
   "keeps view read=locks mz=0 gs=1 g3=0",
   "keeps view read=locks_database mz=0 gs=1 g3=0",
   "keeps view read=locks_storage mz=0 gs=1 g3=0",
   "keeps view read=locks mz=0 gs=1 g3=1",
   "keeps view read=locks_database mz=0 gs=1 g3=1",
   "keeps view read=locks_storage mz=0 gs=1 g3=1",
   "keeps view read=locks mz=1 gs=1 g3=0",
   "keeps view read=locks_database mz=1 gs=1 g3=0",
   "keeps view read=locks_storage mz=1 gs=1 g3=0",
   "keeps view read=locks mz=1 gs=1 g3=1",
   "keeps view read=locks_database mz=1 gs=1 g3=1",
   "keeps view read=locks_storage mz=1 gs=1 g3=1",
   "keeps rollup read=locks mz=0 gs=1 g3=0",
   "keeps rollup read=locks_database mz=0 gs=1 g3=0",
   "keeps rollup read=locks mz=0 gs=1 g3=1",
   "keeps rollup read=locks_database mz=0 gs=1 g3=1",
   "keeps rollup read=locks mz=1 gs=1 g3=0",
   "keeps rollup read=locks_database mz=1 gs=1 g3=0",
   "keeps rollup read=locks mz=1 gs=1 g3=1",
   "keeps rollup read=locks_database mz=1 gs=1 g3=1",
   "keeps served read=locks mz=0 gs=1 g3=0",
   "keeps served read=locks_database mz=0 gs=1 g3=0",
   "keeps served read=locks_storage mz=0 gs=1 g3=0",
   "keeps served read=locks mz=0 gs=1 g3=1",
   "keeps served read=locks_database mz=0 gs=1 g3=1",
   "keeps served read=locks_storage mz=0 gs=1 g3=1",
   "keeps served read=locks mz=1 gs=1 g3=0",
   "keeps served read=locks_database mz=1 gs=1 g3=0",
   "keeps served read=locks_storage mz=1 gs=1 g3=0",
   "keeps served read=locks mz=1 gs=1 g3=1",
   "keeps served read=locks_database mz=1 gs=1 g3=1",
   "keeps served read=locks_storage mz=1 gs=1 g3=1",
   // and, pending the security review's sign-off, on the
   // rollup leg under own-gate keeping alone, on any tier.
   "keeps rollup read=locks mz=0 gs=0 g3=1",
   "keeps rollup read=locks_database mz=0 gs=0 g3=1",
   "keeps rollup read=locks mz=1 gs=0 g3=1",
   "keeps rollup read=locks_database mz=1 gs=0 g3=1",
   // Pending the security review's sign-off: the rollup leg keeps
   // an answer under own-gate keeping that read a `storage=` rollup plan admitted past its
   // base's own gates (read=own, own_database), and, under the switch, any
   // answer whose gate columns are exact, as before an interim
   // tightening; the router's comparison rules refuse the rest.
   "keeps rollup read=own mz=0 gs=0 g3=1",
   "keeps rollup read=own_database mz=0 gs=0 g3=1",
   "keeps rollup read=own mz=1 gs=0 g3=1",
   "keeps rollup read=own_database mz=1 gs=0 g3=1",
   "keeps rollup read=storage mz=0 gs=1 g3=0",
   "keeps rollup read=mixed mz=0 gs=1 g3=0",
   "keeps rollup read=own mz=0 gs=1 g3=0",
   "keeps rollup read=own_database mz=0 gs=1 g3=0",
   "keeps rollup read=own_storage mz=0 gs=1 g3=0",
   "keeps rollup read=locks_storage mz=0 gs=1 g3=0",
   "keeps rollup read=storage mz=0 gs=1 g3=1",
   "keeps rollup read=mixed mz=0 gs=1 g3=1",
   "keeps rollup read=own mz=0 gs=1 g3=1",
   "keeps rollup read=own_database mz=0 gs=1 g3=1",
   "keeps rollup read=own_storage mz=0 gs=1 g3=1",
   "keeps rollup read=locks_storage mz=0 gs=1 g3=1",
   "keeps rollup read=storage mz=1 gs=1 g3=0",
   "keeps rollup read=mixed mz=1 gs=1 g3=0",
   "keeps rollup read=own mz=1 gs=1 g3=0",
   "keeps rollup read=own_database mz=1 gs=1 g3=0",
   "keeps rollup read=own_storage mz=1 gs=1 g3=0",
   "keeps rollup read=locks_storage mz=1 gs=1 g3=0",
   "keeps rollup read=storage mz=1 gs=1 g3=1",
   "keeps rollup read=mixed mz=1 gs=1 g3=1",
   "keeps rollup read=own mz=1 gs=1 g3=1",
   "keeps rollup read=own_database mz=1 gs=1 g3=1",
   "keeps rollup read=own_storage mz=1 gs=1 g3=1",
   "keeps rollup read=locks_storage mz=1 gs=1 g3=1",
   // The `read=ungated*` reads with the gated-sources switch on: no change,
   // the switch keeps every answer a leg past a gate produced.
   "keeps view read=ungated mz=0 gs=1 g3=0",
   "keeps view read=ungated_database mz=0 gs=1 g3=0",
   "keeps view read=ungated_own mz=0 gs=1 g3=0",
   "keeps view read=ungated_storage mz=0 gs=1 g3=0",
   "keeps view read=ungated mz=0 gs=1 g3=1",
   "keeps view read=ungated_database mz=0 gs=1 g3=1",
   "keeps view read=ungated_own mz=0 gs=1 g3=1",
   "keeps view read=ungated_storage mz=0 gs=1 g3=1",
   "keeps view read=ungated mz=1 gs=1 g3=0",
   "keeps view read=ungated_database mz=1 gs=1 g3=0",
   "keeps view read=ungated_own mz=1 gs=1 g3=0",
   "keeps view read=ungated_storage mz=1 gs=1 g3=0",
   "keeps view read=ungated mz=1 gs=1 g3=1",
   "keeps view read=ungated_database mz=1 gs=1 g3=1",
   "keeps view read=ungated_own mz=1 gs=1 g3=1",
   "keeps view read=ungated_storage mz=1 gs=1 g3=1",
   "keeps rollup read=ungated mz=0 gs=1 g3=0",
   "keeps rollup read=ungated_database mz=0 gs=1 g3=0",
   "keeps rollup read=ungated_own mz=0 gs=1 g3=0",
   "keeps rollup read=ungated_storage mz=0 gs=1 g3=0",
   "keeps rollup read=ungated mz=0 gs=1 g3=1",
   "keeps rollup read=ungated_database mz=0 gs=1 g3=1",
   "keeps rollup read=ungated_own mz=0 gs=1 g3=1",
   "keeps rollup read=ungated_storage mz=0 gs=1 g3=1",
   "keeps rollup read=ungated mz=1 gs=1 g3=0",
   "keeps rollup read=ungated_database mz=1 gs=1 g3=0",
   "keeps rollup read=ungated_own mz=1 gs=1 g3=0",
   "keeps rollup read=ungated_storage mz=1 gs=1 g3=0",
   "keeps rollup read=ungated mz=1 gs=1 g3=1",
   "keeps rollup read=ungated_database mz=1 gs=1 g3=1",
   "keeps rollup read=ungated_own mz=1 gs=1 g3=1",
   "keeps rollup read=ungated_storage mz=1 gs=1 g3=1",
   "keeps served read=ungated mz=0 gs=1 g3=0",
   "keeps served read=ungated_database mz=0 gs=1 g3=0",
   "keeps served read=ungated_own mz=0 gs=1 g3=0",
   "keeps served read=ungated_storage mz=0 gs=1 g3=0",
   "keeps served read=ungated mz=0 gs=1 g3=1",
   "keeps served read=ungated_database mz=0 gs=1 g3=1",
   "keeps served read=ungated_own mz=0 gs=1 g3=1",
   "keeps served read=ungated_storage mz=0 gs=1 g3=1",
   "keeps served read=ungated mz=1 gs=1 g3=0",
   "keeps served read=ungated_database mz=1 gs=1 g3=0",
   "keeps served read=ungated_own mz=1 gs=1 g3=0",
   "keeps served read=ungated_storage mz=1 gs=1 g3=0",
   "keeps served read=ungated mz=1 gs=1 g3=1",
   "keeps served read=ungated_database mz=1 gs=1 g3=1",
   "keeps served read=ungated_own mz=1 gs=1 g3=1",
   "keeps served read=ungated_storage mz=1 gs=1 g3=1",
   // The ungated-rows rule: with the switch off, a served answer
   // reading a `storage=` table proven to hold only ungated rows keeps the
   // gates, as one reading a database table does, alone or beside a
   // database table or a table admitted past its own gate. Beside a
   // `storage=` table proven neither way (`read=ungated_storage`) it does
   // not. The rollup leg follows below. Reviewed in the security
   // review, including the lock-gated join widening.
   "keeps served read=ungated mz=0 gs=0 g3=1",
   "keeps served read=ungated_database mz=0 gs=0 g3=1",
   "keeps served read=ungated_own mz=0 gs=0 g3=1",
   "keeps served read=ungated mz=1 gs=0 g3=1",
   "keeps served read=ungated_database mz=1 gs=0 g3=1",
   "keeps served read=ungated_own mz=1 gs=0 g3=1",
   // The ungated-rows rule for a `storage=` rollup: with the switch off, the
   // rollup leg keeps the gates for an answer reading a `storage=` table
   // proven to hold only ungated rows, as the served copy does: a join
   // branch over a rollup of an ungated joined source, beside the query
   // source's own table in that destination (admitted past its own gate, or
   // holding only ungated rows). Beside a `storage=` table proven neither
   // way (`read=ungated_storage`) it does not. Needs the security review's
   // sign-off.
   "keeps rollup read=ungated mz=0 gs=0 g3=1",
   "keeps rollup read=ungated_database mz=0 gs=0 g3=1",
   "keeps rollup read=ungated_own mz=0 gs=0 g3=1",
   "keeps rollup read=ungated mz=1 gs=0 g3=1",
   "keeps rollup read=ungated_database mz=1 gs=0 g3=1",
   "keeps rollup read=ungated_own mz=1 gs=0 g3=1",
   "keeps view read=own_binary64 mz=0 gs=1 g3=0",
   "keeps view read=own_binary64 mz=0 gs=1 g3=1",
   "keeps view read=own_binary64 mz=1 gs=1 g3=0",
   "keeps view read=own_binary64 mz=1 gs=1 g3=1",
   "keeps served read=own_binary64 mz=0 gs=0 g3=1",
   "keeps served read=own_binary64 mz=1 gs=0 g3=1",
   "keeps served read=own_binary64 mz=0 gs=1 g3=0",
   "keeps served read=own_binary64 mz=0 gs=1 g3=1",
   "keeps served read=own_binary64 mz=1 gs=1 g3=0",
   "keeps served read=own_binary64 mz=1 gs=1 g3=1",
   "keeps rollup read=own_binary64 mz=0 gs=0 g3=1",
   "keeps rollup read=own_binary64 mz=1 gs=0 g3=1",
   "keeps rollup read=own_binary64 mz=0 gs=1 g3=0",
   "keeps rollup read=own_binary64 mz=0 gs=1 g3=1",
   "keeps rollup read=own_binary64 mz=1 gs=1 g3=0",
   "keeps rollup read=own_binary64 mz=1 gs=1 g3=1",
];
