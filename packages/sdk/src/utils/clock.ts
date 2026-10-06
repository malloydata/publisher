// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** A monotonic clock in milliseconds, for durations. */
export const now = (): number =>
   typeof performance !== "undefined" ? performance.now() : Date.now();
