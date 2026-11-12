// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The join_one uniqueness check (materialization v2, P8): before a
 * `#(materialize)` source that inlines a join_one is built, its joined source
 * is grouped on the join's key, and any key held twice fails the build. A
 * duplicate would store the parent row once per match, so every count over
 * the table would come up high while live, deduplicating through the join,
 * stayed right.
 *
 * The check's SQL is generated and compiled with the build companion at plan
 * time (build_file_generator.ts); here it only runs. It runs on every build:
 * skipping it when the joined table is unchanged since the last passing check
 * needs a last-modified time from the warehouse, which is not read yet.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import type { MaterializePlan } from "../plan/build_file_generator";

/**
 * A `#(materialize)` source inlines a join_one (`inline=["j"]`) whose joined
 * source holds one key more than once (P8), so its build would store a parent
 * row once per match and every count over the table would come up high. The
 * build fails before writing anything; the message names the join.
 */
export class InlinedJoinNotUniqueError extends Error {
   get kind(): "inlined_join_not_unique" {
      return "inlined_join_not_unique";
   }

   constructor(
      readonly sourceName: string,
      readonly join: string,
      readonly keyColumn: string,
      /**
       * True when the plan carries no compiled check for the join (a plan from
       * a Publisher that did not compile one): refused rather than built
       * unchecked, and said so rather than claimed a duplicate.
       */
      readonly unchecked = false,
   ) {
      super(
         unchecked
            ? `'${sourceName}' is not built: it inlines the join_one '${join}', ` +
                 `and its build plan carries no uniqueness check for it, so a ` +
                 `duplicate '${keyColumn}' could not be ruled out. Reload the ` +
                 `package to plan the check.`
            : `'${sourceName}' is not built: it inlines the join_one '${join}', ` +
                 `but the joined source holds a '${keyColumn}' value more than once, ` +
                 `so a row would be stored once per match. Make '${keyColumn}' ` +
                 `unique in the joined source, or leave '${join}' out of inline=.`,
      );
      this.name = "InlinedJoinNotUniqueError";
   }
}

/**
 * Run every uniqueness check `plan` carries on `connection`, throwing
 * {@link InlinedJoinNotUniqueError} for the first join whose key repeats. A
 * plan with no inlined join checks nothing.
 */
export async function assertInlinedJoinsUnique(
   plan: MaterializePlan | undefined,
   connection: Pick<MalloyConnection, "runSQL">,
): Promise<void> {
   for (const check of plan?.uniquenessChecks ?? []) {
      // A plan from a Publisher that did not compile the check: refuse rather
      // than build unchecked.
      if (!check.sql) {
         throw new InlinedJoinNotUniqueError(
            plan?.sourceName ?? "",
            check.join,
            check.keyColumn,
            true,
         );
      }
      const result = await connection.runSQL(check.sql);
      if (result.rows.length > 0) {
         throw new InlinedJoinNotUniqueError(
            plan?.sourceName ?? "",
            check.join,
            check.keyColumn,
         );
      }
   }
}
