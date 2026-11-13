// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { causeChain } from "../../../driver_errors";
import { MaterializationEligibilityError } from "../../../errors";

// A leaf module, so a strategy or the build session can reduce an error to a
// log code without importing run_telemetry, which imports them.

// A code is a short identifier; anything else (a message, a path) is dropped.
const CODE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$/;

/**
 * An error reduced to a code a log may carry: an eligibility refusal's
 * reason, else Publisher's error kind, else a driver's own code (a SQLSTATE,
 * `ECONNREFUSED`), else the error's class name. Never its message, which can
 * quote SQL or a value. The cause chain is read outermost first: a strategy
 * rewraps a driver's error in a plain `Error` to redact its message.
 */
export function errorCodeOf(err: unknown): string {
   const chain = [...causeChain(err)];
   for (const e of chain) {
      if (e instanceof MaterializationEligibilityError && e.reason) {
         return e.reason;
      }
      const { kind, code } = e as Error & { kind?: unknown; code?: unknown };
      for (const candidate of [kind, code]) {
         if (typeof candidate === "string" && CODE_PATTERN.test(candidate)) {
            return candidate;
         }
      }
      if (typeof code === "number") return String(code);
   }
   // No code anywhere: the most specific class name, outermost first.
   for (const e of chain) {
      if (e.name !== "Error" && CODE_PATTERN.test(e.name)) return e.name;
   }
   return "Error";
}
