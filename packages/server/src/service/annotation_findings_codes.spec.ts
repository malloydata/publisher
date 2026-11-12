// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which coded package warnings the annotation-findings metric counts. The
 * incremental advisories only arise on dialects that support incremental
 * refresh (not DuckDB), so the selection is checked on `Package` directly.
 */

import { describe, expect, it } from "bun:test";
import { PERSIST_SOURCE_REFUSED_CODE } from "./materialization/package_materialization";
import {
   INCREMENTAL_NO_MERGE_KEY_CODE,
   INCREMENTAL_SHARED_ADDRESS_CODE,
} from "./materialization/plan/incremental_policy";
import { Package } from "./package";

describe("Package.annotationFindingWarnings", () => {
   it("counts the no-merge_key advisory, and not the shared-address advisory or a gate refusal", () => {
      const warn = (code: string) => ({
         model: "m.malloy",
         message: code,
         code,
         severity: "warn" as const,
      });
      const self = {
         materialization: {
            droppedPersistWarnings: () => [],
            annotationFindingWarnings: () => [],
            rollupsOverStoredBaseWarnings: () => [],
            incrementalPolicyAdvisories: () => [
               warn(INCREMENTAL_NO_MERGE_KEY_CODE),
               warn(INCREMENTAL_SHARED_ADDRESS_CODE),
            ],
            persistencePolicyWarnings: () => [
               warn(PERSIST_SOURCE_REFUSED_CODE),
            ],
         },
         preaggregateAccessWarnings: () => [],
         annotationNearMissWarnings: () => [],
      };
      const codes = Package.prototype.annotationFindingWarnings
         .call(self as unknown as Package)
         .map((w) => w.code);
      expect(codes).toEqual([INCREMENTAL_NO_MERGE_KEY_CODE]);
   });
});
