// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a table is built and served: the one tier decision, read by plan
 * (the destination an address folds in), the run (the tier a build takes,
 * the refusal of a destination that cannot be written) and its telemetry.
 * Pure: the storage mode is the caller's (`domain/switches.ts`).
 */

import type { StorageMode } from "./switches";
import type { Tier } from "./vocabulary";

/** Whether a `storage=` destination may be written at all under `mode`. */
export function storageWritable(mode: StorageMode): boolean {
   return mode !== "off";
}

/**
 * Whether a query may read a `storage=` table under `mode`: only `on`.
 * `write-only` writes the destination but serves live (or from the
 * database tier), as `off` does.
 */
export function storageServes(mode: StorageMode): boolean {
   return mode === "on";
}

/**
 * The tier an instruction builds into: its destination's, while a
 * destination may be written; else the source's own database. An
 * instruction naming a destination under `off` is refused before it builds
 * ({@link instructsUnwritableDestination}), so its build never runs.
 */
export function instructionTier(
   instruction: { destination?: string },
   mode: StorageMode,
): Tier {
   return instruction.destination && storageWritable(mode)
      ? "storage"
      : "database";
}

/** Whether an instruction names a destination `mode` does not let it write. */
export function instructsUnwritableDestination(
   instruction: { destination?: string },
   mode: StorageMode,
): boolean {
   return !!instruction.destination && !storageWritable(mode);
}

/**
 * The tier a query reads a planned source's table in, as the serve verdict
 * judges it: a storage binding storing the source decides it
 * (`storageLookupOf`), since a host may place a source with no `storage=` in
 * a storage destination; with none, a declared `storage=` does. The served
 * copy (every source but a rollup, persisted view or persisted query, which
 * read their storage binding through their own leg) reads a fresh table over
 * a stale one, so a source whose storage table is stale while its database
 * table is fresh is read from the database.
 */
export function readTier(facts: {
   storageBound: boolean;
   storageFresh: boolean;
   databaseFresh: boolean;
   servedCopy: boolean;
   declaredStorage: boolean;
}): Tier {
   if (!facts.storageBound) {
      return facts.declaredStorage ? "storage" : "database";
   }
   return facts.servedCopy && !facts.storageFresh && facts.databaseFresh
      ? "database"
      : "storage";
}

/**
 * The destination a source's declared `storage=` builds into under `mode`:
 * none under `off`, where the source is not built at all and serves live.
 */
export function declaredDestination(
   declared: string | undefined,
   mode: StorageMode,
): string | undefined {
   return storageWritable(mode) ? declared : undefined;
}
