// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The plan-time proof that a downstream compiled over its upstream's stored
 * table (a persisted view's or named query's build source, or a rollup)
 * builds the table the plan gives it, recorded on its table plan so a run
 * only reads the decision:
 *
 *  - over a `#@ persist` upstream, the compile keeps the upstream's own
 *    `#@ persist`, which leaves the downstream's fully inlined SQL, and so
 *    its address, unchanged: the two addresses must be equal;
 *  - over a `#(materialize)` upstream, the upstream is re-rooted on its
 *    generated build source, which moves the downstream's SQL, so its own
 *    address cannot match. The build source must instead be at the
 *    upstream's planned address (the same build source, so the same table),
 *    the downstream's SQL rendered over that table alone must read it, and
 *    its columns must be the planned downstream's, by name, type and
 *    numeric subtype.
 *
 * Each check is defensive: a compile made as planned passes them all. Each
 * guards a drift between the second compile and the plan and fails closed:
 * a view or query that fails it is refused at plan (`generated_build_failed`),
 * a rollup builds from the warehouse (build_plan.ts).
 * Addresses are compared over one connection digest for both sources, which
 * share a connection, so the comparison needs no live connection.
 */

import type { PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { outputColumnsOf } from "../../governance/ir/compiled_model";
import { readsTable } from "../domain/table_name";
import type { OverStoredBase } from "../domain/table_plans";
import {
   buildSQLOf,
   compilerBuildId,
   computeSourceEntityId,
   presentAs,
} from "./addressing";

/** The table name the proof renders an upstream's table under. */
const PROOF_TABLE = "publisher_over_stored_proof_table";

/**
 * Whether `variant`, `planned` compiled over its upstream's stored table,
 * builds `planned`'s table. `base` is the upstream a `#(materialize)`
 * variant re-roots (its planned table source); absent for a `#@ persist`
 * upstream. Never throws: a check that cannot be made is false.
 */
export function overStoredProven(
   planned: PersistSource,
   variant: OverStoredBase,
   base?: PersistSource,
): boolean {
   try {
      const address = (s: PersistSource) =>
         computeSourceEntityId(s, { [s.connectionName]: "proof" });
      // Equal addresses are equal SQL: the same table, columns and all.
      if (!variant.baseBuildSource) {
         return address(variant.source) === address(planned);
      }
      if (!sameColumns(variant.source, planned)) return false;
      if (!base) return false;
      const buildSource = presentAs(variant.baseBuildSource, {
         name: base.name,
         sourceID: base.sourceID,
      });
      const baseAddress = address(base);
      if (address(buildSource) !== baseAddress) return false;
      const only = new Manifest();
      only.update(baseAddress, { tableName: PROOF_TABLE });
      // Also under the compiler's own key for the base, which differs from
      // its address when the address folds in a `name=` or projection.
      only.update(compilerBuildId(buildSource), { tableName: PROOF_TABLE });
      return readsTable(
         buildSQLOf(variant.source, only.buildManifest),
         PROOF_TABLE,
      );
   } catch {
      return false;
   }
}

/**
 * Whether two sources output the same columns, by name, type and numeric
 * subtype, in order: an integer column where the plan has a float is drift.
 */
function sameColumns(a: PersistSource, b: PersistSource): boolean {
   return (
      JSON.stringify(outputColumnsOf(a)) === JSON.stringify(outputColumnsOf(b))
   );
}
