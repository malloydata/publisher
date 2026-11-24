// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The build-time gate of a persisted view or named query, or an extension
 * table, planned over a `#(materialize)` pair table with its join put back
 * (plan/pair_reroot.ts):
 * the re-root counts and sums each level of the path by that level's key, so
 * the build relies on the leaf's key being unique and never null on the leaf
 * source's rows. The build writing the pair table runs that check and
 * records it in the table's contract (`ColumnContract.keys`); a build over the
 * table reads the record and is refused (`schema_incompatible`) when a check
 * failed or is not recorded, never built from raw instead.
 */

import type { ManifestEntry } from "../domain/types";
import { SchemaIncompatibleError } from "../../../errors";
import {
   keyCheckFailed,
   keyCheckPassed,
   parseContract,
} from "../domain/column_contract";

/**
 * Why `downstream` must not build over its pair-table upstream `upstream`,
 * whose current entry is `entry`, given the leaf checks it relies on
 * (`OverStoredBase.pairLeafKeys`, or an extension's `ChainMode.pair`), or
 * undefined when every one passed. Also read at plan, over the upstream's
 * bound contract, so a failure the bind already holds refuses the
 * extension there rather than failing its build again.
 */
export function pairLeafRefusal(
   downstream: string,
   upstream: string,
   leafKeys: readonly string[],
   entry: ManifestEntry | undefined,
): string | undefined {
   if (leafKeys.length === 0) return undefined;
   const contract = parseContract(entry?.contract);
   const failed = leafKeys.filter((k) => keyCheckFailed(contract, k));
   // Not recorded, or recorded as a check that could not run.
   const unrecorded = leafKeys.filter(
      (k) => !keyCheckPassed(contract, k) && !keyCheckFailed(contract, k),
   );
   if (failed.length === 0 && unrecorded.length === 0) return undefined;
   // Named as the model names the path (`items.item_id`), not as recorded.
   const named = (ks: string[]) =>
      ks.map((k) => `'${k.replace(/^leaf:/, "")}'`).join(", ");
   const what =
      failed.length > 0
         ? `found the key ${named(failed)} held more than once or NULL`
         : `recorded no result for the key ${named(unrecorded)}`;
   return (
      `'${downstream}' was not built over its upstream '${upstream}''s pair ` +
      `table: the leaf key check of its inlined join_many, which a build over ` +
      `that table relies on, ${what} when the table was built. It is never ` +
      `built from raw instead; it builds once the upstream's table records ` +
      `the check passed.`
   );
}

/** {@link pairLeafRefusal}, thrown as the SourceFailure `schema_incompatible`. */
export function assertPairLeafKeysPassed(
   ...args: Parameters<typeof pairLeafRefusal>
): void {
   const why = pairLeafRefusal(...args);
   if (why !== undefined) throw new SchemaIncompatibleError(why);
}
