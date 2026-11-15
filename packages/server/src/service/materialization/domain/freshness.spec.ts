// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";

import type { FreshnessAwareManifestEntry } from "./types";
import { freshnessOf } from "./freshness";

// A fixed "now" so age math is deterministic.
const NOW = new Date("2026-07-07T12:00:00.000Z");
const NOW_MS = NOW.getTime();

/** dataAsOf `ageSeconds` before NOW. */
function asOf(ageSeconds: number): string {
   return new Date(NOW_MS - ageSeconds * 1000).toISOString();
}

describe("freshnessOf", () => {
   it("serves the table when un-gated (no window)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(1_000_000),
      };
      expect(freshnessOf(entry, NOW)).toBe("fresh");
   });

   it("serves the table when a window is declared but no dataAsOf anchor", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(freshnessOf(entry, NOW)).toBe("fresh");
   });

   it("serves the table when fresh (age within window)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(1800),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(freshnessOf(entry, NOW)).toBe("fresh");
   });

   it("serves the table at the exact window boundary (age == window)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(3600),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(freshnessOf(entry, NOW)).toBe("fresh");
   });

   it("drops a stale entry with fallback live (=> serve live)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(freshnessOf(entry, NOW)).toBe("stale");
   });

   it("keeps a stale entry with fallback stale_ok (=> serve the stale table)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "stale_ok",
      };
      expect(freshnessOf(entry, NOW)).toBe("stale_ok");
   });

   it("drops a stale entry with fallback fail (interim: serve live in v1)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "fail",
      };
      expect(freshnessOf(entry, NOW)).toBe("stale");
   });

   it("drops a stale entry with no declared fallback (=> serve live)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
      };
      expect(freshnessOf(entry, NOW)).toBe("stale");
   });
});
