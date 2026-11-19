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
 * The served copy re-roots a source on one table: of the bindings it may read
 * for that source (fresh, and a storage one only under `on`), the storage
 * table over the database one, as the rollup, persisted view and persisted
 * query legs read a storage binding first (`storageLookupOf`). Whether a
 * `tier` candidate stands beside the others that re-root the same source.
 */
export function servedCopyReadsTier(tier: Tier, others: readonly Tier[]) {
   return tier === "storage" || !others.includes("storage");
}

/**
 * The tier a query reads a planned source's table in, as the serve verdict
 * judges it: a storage binding storing the source decides it
 * (`storageLookupOf`), since a host may place a source with no `storage=` in
 * a storage destination; with none, a declared `storage=` does. The served
 * copy (every source but a rollup, persisted view or persisted query, which
 * read their storage binding through their own leg) reads the storage table
 * when it may ({@link servedCopyReadsTier}): fresh, and under `on`. Else a
 * database table bound beside it: under another mode, whatever its
 * freshness, which the verdict then judges; under `on`, a fresh one beside a
 * stale storage table.
 */
export function readTier(facts: {
   storageBound: boolean;
   storageFresh: boolean;
   /** PERSIST_STORAGE_MODE lets a query read a storage table ({@link storageServes}). */
   storageServed: boolean;
   databaseBound: boolean;
   databaseFresh: boolean;
   servedCopy: boolean;
   declaredStorage: boolean;
}): Tier {
   if (!facts.storageBound) {
      return facts.declaredStorage ? "storage" : "database";
   }
   if (!facts.servedCopy) return "storage";
   if (!facts.storageServed) {
      return facts.databaseBound ? "database" : "storage";
   }
   return !facts.storageFresh && facts.databaseFresh ? "database" : "storage";
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
