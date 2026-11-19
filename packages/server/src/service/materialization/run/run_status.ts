// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run's state machine as data: which statuses are terminal, which moves
 * between statuses are admitted, and how a thrown run is classified. Pure;
 * the lifecycle writes the transitions it admits.
 */

import type { MaterializationStatus } from "../domain/types";

/** Classify a thrown build error as cancelled (cooperative abort) or failed. */
export function outcomeFor(
   signal: AbortSignal | undefined,
): "failed" | "cancelled" {
   return signal?.aborted ? "cancelled" : "failed";
}

/** A run that can write nothing more. */
const TERMINAL_RUN_STATUSES: ReadonlySet<string> = new Set([
   "MANIFEST_FILE_READY",
   "FAILED",
   "CANCELLED",
]);

/** Whether a run with `status` has not settled: it may still write. */
export function isActiveRun(status: string): boolean {
   return !TERMINAL_RUN_STATUSES.has(status);
}

/**
 * Allowed status transitions: PENDING → MANIFEST_ROWS_READY (tables built) →
 * MANIFEST_FILE_READY. MANIFEST_FILE_READY, FAILED and CANCELLED are terminal.
 */
export const VALID_TRANSITIONS: Record<
   MaterializationStatus,
   MaterializationStatus[]
> = {
   PENDING: ["MANIFEST_ROWS_READY", "FAILED", "CANCELLED"],
   MANIFEST_ROWS_READY: ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"],
   MANIFEST_FILE_READY: [],
   FAILED: [],
   CANCELLED: [],
};
