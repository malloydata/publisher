// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useState } from "react";

/**
 * How long a changed parameter has to stay changed before the notebook runs.
 *
 * Aborting the superseded run is not enough on its own: an abort cancels the
 * HTTP request, but the cells already dispatched have reached the server and go
 * on compiling and running against the customer's warehouse, and their answers
 * are then thrown away. So autorun on a text control put one wave of doomed
 * queries on that warehouse per keystroke. `origin/main` bounded the load by
 * refusing to start a second run at all, which dropped the newest values;
 * waiting for the value to settle bounds it without making that trade.
 */
export const GIVEN_SETTLE_MS = 400;

/** `value`, once it has stayed the same for `ms`; the first value is returned at once. */
export function useSettled<T>(value: T, ms: number): T {
   const [settled, setSettled] = useState(value);
   useEffect(() => {
      const timer = setTimeout(() => setSettled(value), ms);
      return () => clearTimeout(timer);
   }, [value, ms]);
   return settled;
}
