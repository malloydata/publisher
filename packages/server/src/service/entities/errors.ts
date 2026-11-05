// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The message of a thrown value, whatever was thrown. */
export function errorMessage(err: unknown): string {
   return err instanceof Error ? err.message : String(err);
}
