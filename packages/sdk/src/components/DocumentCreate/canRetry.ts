// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** Statuses a second attempt cannot change: not signed in, not allowed, not there. */
const FINAL_STATUSES = new Set([401, 403, 404]);

/**
 * Whether asking again can succeed after a request failed with `error`: not
 * after a 401, 403 or 404; yes after any other status or no response at all.
 * Reads an axios error's `response.status`, or a `status` on the error itself.
 */
export function canRetryRequest(error: unknown): boolean {
   const failure = error as
      | { response?: { status?: unknown }; status?: unknown }
      | null
      | undefined;
   const status = failure?.response?.status ?? failure?.status;
   return typeof status !== "number" || !FINAL_STATUSES.has(status);
}
