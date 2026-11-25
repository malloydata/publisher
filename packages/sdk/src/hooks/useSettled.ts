// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useState } from "react";

/**
 * How long a changed value has to stay changed before it is acted on: a given
 * input commits after it, and the notebook waits it again before running.
 *
 * Aborting a superseded run is not enough on its own: an abort cancels the
 * HTTP request, but cells already dispatched have reached the server and go on
 * running against the customer's warehouse, and their answers are thrown away.
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
