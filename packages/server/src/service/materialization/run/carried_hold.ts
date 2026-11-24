// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The scope of a standalone run's pre-run contract read
 * ({@link deriveHoldingCarried}): which tables it reads (those the run keeps,
 * carried or advanced by a delta), and when the run is derived again (one
 * moved, or an advanced one went unread). Pure over its callbacks apart from
 * one log line: the reads, and the drift handler a moved table goes to, are
 * `hold`'s (PackageMaterialization.holdCarriedToContracts).
 */

import { logger } from "../../../logger";
import type { ManifestEntry } from "../domain/types";

/** What `hold` found of the tables it was handed. */
interface Held {
   /** No longer match their contracts. */
   moved: ReadonlySet<string>;
   /** Carry a contract but were not read (a failed read, the budget, a cancel). */
   unread: ReadonlySet<string>;
}

/**
 * A standalone run's instructions, with each table it keeps held to its
 * contract first: `derive` decides what the run carries, and which
 * incremental tables a delta advances in place (whose rows it keeps too),
 * given the tables to rebuild in full (`incompatible`); `hold` reads only
 * those. A table the run rebuilds, refuses or leaves out (outside a named
 * run's closure) is never read, so one that keeps reading as moved but is
 * never rebuilt does not send the package to the drift handler on every run.
 *
 * The run is derived again, rebuilding in full, for each table that moved,
 * and for each advanced table `hold` did not read: a delta writes into the
 * table as it stands and records its contract from it, and nothing else
 * checks that table, so one not verified is reseeded rather than advanced
 * (`contract_unverified`). A carried table not read stays carried: the bind
 * and each downstream build's upstream check still hold it. Keeping only
 * shrinks on the second pass, so nothing kept goes unread.
 */
export async function deriveHoldingCarried<
   T extends {
      carried: Record<string, ManifestEntry>;
      advanced?: Record<string, ManifestEntry>;
   },
>(
   derive: (incompatible: ReadonlySet<string>) => T,
   incompatible: () => ReadonlySet<string>,
   hold:
      | ((kept: Readonly<Record<string, ManifestEntry>>) => Promise<Held>)
      | undefined,
   packageName?: string,
): Promise<T> {
   const derived = derive(incompatible());
   const kept = { ...derived.advanced, ...derived.carried };
   if (!hold || Object.keys(kept).length === 0) return derived;
   const { moved, unread } = await hold(kept);
   const unverified = [...unread].filter(
      (id) => id in (derived.advanced ?? {}),
   );
   for (const sourceEntityId of unverified) {
      logger.warn(
         "An incremental table a delta would advance was not read before the run, so the run reseeds it rather than write into a table it could not check",
         { packageName, sourceEntityId, reasonCode: "contract_unverified" },
      );
   }
   if (moved.size === 0 && unverified.length === 0) return derived;
   // `moved` directly too: an advanced table whose entry came from an
   // earlier run than `incompatible`'s priors is not named there.
   return derive(new Set([...incompatible(), ...moved, ...unverified]));
}
