// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a spec reads of shadow admission (`serve/admission_parity.ts`): the
 * disagreements its queries produced between read admission's probes and
 * its lookups, and the assertion that there were none but those it allows.
 */

import {
   clearParityDrifts,
   type ParityAllowance,
   type ParityDrift,
   parityDrifts,
} from "../service/materialization/serve/admission_parity";

export {
   clearParityDrifts,
   clearQueryWalks,
   holdParityAllowance,
   queryWalks,
   withAdmissionMode,
   withParityAllowance,
} from "../service/materialization/serve/admission_parity";

/** Whether this run judges admission in shadow (PUBLISHER_ADMISSION_MODE). */
export function admissionInShadow(): boolean {
   return process.env.PUBLISHER_ADMISSION_MODE === "shadow";
}

/**
 * Throw when a disagreement was recorded since the last clear that falls
 * under none of `allow`, then clear. Each is listed with both refusals.
 */
export function expectNoDrift(allow: readonly ParityAllowance[] = []): void {
   const found = parityDrifts();
   clearParityDrifts();
   const unexpected = found.filter(
      (d: ParityDrift) => d.allowed === undefined || !allow.includes(d.allowed),
   );
   if (unexpected.length === 0) return;
   throw new Error(
      `read admission's probe and lookup disagreed ${unexpected.length} time(s):\n` +
         unexpected
            .map(
               (d) =>
                  `  ${d.side}: probe ${d.probe === undefined ? "admits" : `refuses (${d.probe})`}, ` +
                  `lookup ${d.lookup === undefined ? "admits" : `refuses (${d.lookup})`}` +
                  (d.tables.length > 0 ? ` [${d.tables.join(", ")}]` : "") +
                  (d.allowed ? ` (marked ${d.allowed})` : ""),
            )
            .join("\n"),
   );
}
