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
 * The destination a source's declared `storage=` builds into under `mode`:
 * none under `off`, where the source is not built at all and serves live.
 */
export function declaredDestination(
   declared: string | undefined,
   mode: StorageMode,
): string | undefined {
   return storageWritable(mode) ? declared : undefined;
}
