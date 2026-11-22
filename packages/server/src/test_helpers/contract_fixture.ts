// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Column contracts for specs.
 *
 * - {@link fakeContractReads}: a spec whose builds run over a fake
 *   connection or session that answers no catalog query installs this, so
 *   each build records an empty contract instead of failing its capture
 *   (`contract_capture_failed`). A spec about the capture itself does not.
 */

import { afterEach, beforeEach } from "bun:test";
import { fakeContractReadsForTests } from "../service/materialization/run/column_contract_capture";

/** Fake every build's contract reads for each test of the calling file. */
export function fakeContractReads(): void {
   let restore: (() => void) | undefined;
   beforeEach(() => {
      restore = fakeContractReadsForTests();
   });
   afterEach(() => {
      restore?.();
      restore = undefined;
   });
}
