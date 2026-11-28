// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { jsonResource } from "./tool_response";

/** The id a host is assumed to send when the request carries none. */
const ASSUMED_ID = "i".repeat(62);

/** Characters of the JSON-RPC message the host receives for this answer, which echoes the request's own id. */
export function wireChars(
   uri: string,
   payload: unknown,
   id: string | number = ASSUMED_ID,
): number {
   return JSON.stringify({
      jsonrpc: "2.0",
      id,
      result: jsonResource(uri, payload),
   }).length;
}

/** The most entries that fit: all `hi` if that does, else the largest n below it (a cut list carries a longer message, so the full count is checked on its own), or undefined when not even 0 fit. */
export function largest(
   fits: (n: number) => boolean,
   hi: number,
): number | undefined {
   if (fits(hi)) return hi;
   if (hi === 0 || !fits(0)) return undefined;
   let lo = 0;
   let top = hi - 1;
   while (lo < top) {
      const mid = Math.ceil((lo + top) / 2);
      if (fits(mid)) lo = mid;
      else top = mid - 1;
   }
   return lo;
}
