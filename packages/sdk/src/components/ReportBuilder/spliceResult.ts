// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** What a writer hands back: the patched file, or why it refused to write one. */

export interface SpliceFailure {
   ok: false;
   reason: string;
}

export type SpliceResult = { ok: true; source: string } | SpliceFailure;

/** Narrow to the failure arm; see `readFailed` in `readDocument.ts` for why a guard. */
export const spliceFailed = (result: SpliceResult): result is SpliceFailure =>
   result.ok === false;
