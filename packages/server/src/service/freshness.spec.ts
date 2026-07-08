import { describe, expect, it } from "bun:test";

import type {
   FreshnessAwareManifestEntry,
   FreshnessManifest,
} from "../storage/DatabaseInterface";
import { evaluateManifestFreshness, filterFreshManifest } from "./freshness";

// A fixed "now" so age math is deterministic.
const NOW = new Date("2026-07-07T12:00:00.000Z");
const NOW_MS = NOW.getTime();

/** dataAsOf `ageSeconds` before NOW. */
function asOf(ageSeconds: number): string {
   return new Date(NOW_MS - ageSeconds * 1000).toISOString();
}

describe("evaluateManifestFreshness", () => {
   it("serves the table when un-gated (no window)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(1_000_000),
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_table");
   });

   it("serves the table when a window is declared but no dataAsOf anchor", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_table");
   });

   it("serves the table when fresh (age within window)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(1800),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_table");
   });

   it("serves the table at the exact window boundary (age == window)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(3600),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_table");
   });

   it("drops a stale entry with fallback live (=> serve live)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "live",
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_live");
   });

   it("keeps a stale entry with fallback stale_ok (=> serve the stale table)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "stale_ok",
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_table");
   });

   it("drops a stale entry with fallback fail (interim: serve live in v1)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
         freshnessFallback: "fail",
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_live");
   });

   it("drops a stale entry with no declared fallback (=> serve live)", () => {
      const entry: FreshnessAwareManifestEntry = {
         tableName: "t",
         dataAsOf: asOf(7200),
         freshnessWindowSeconds: 3600,
      };
      expect(evaluateManifestFreshness(entry, NOW)).toBe("serve_live");
   });
});

describe("filterFreshManifest", () => {
   it("includes fresh/un-gated/stale_ok and drops stale live/fail", () => {
      const entries: FreshnessManifest = {
         fresh: {
            tableName: "fresh_t",
            dataAsOf: asOf(1800),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "live",
         },
         ungated: { tableName: "ungated_t", dataAsOf: asOf(999999) },
         staleLive: {
            tableName: "stale_live_t",
            dataAsOf: asOf(7200),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "live",
         },
         staleOk: {
            tableName: "stale_ok_t",
            dataAsOf: asOf(7200),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "stale_ok",
         },
         staleFail: {
            tableName: "stale_fail_t",
            dataAsOf: asOf(7200),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "fail",
         },
      };

      const { manifest } = filterFreshManifest(entries, NOW);
      expect(Object.keys(manifest).sort()).toEqual([
         "fresh",
         "staleOk",
         "ungated",
      ]);
      expect(manifest.fresh).toEqual({ tableName: "fresh_t" });
      expect(manifest.staleOk).toEqual({ tableName: "stale_ok_t" });
      expect(manifest.ungated).toEqual({ tableName: "ungated_t" });
   });

   it("reports the earliest future window-crossing as nextStaleSince", () => {
      const entries: FreshnessManifest = {
         a: {
            tableName: "a_t",
            dataAsOf: asOf(1800), // window 3600 => flips at NOW + 1800s
            freshnessWindowSeconds: 3600,
            freshnessFallback: "live",
         },
         b: {
            tableName: "b_t",
            dataAsOf: asOf(1000), // window 3600 => flips at NOW + 2600s (later)
            freshnessWindowSeconds: 3600,
            freshnessFallback: "live",
         },
      };

      const { nextStaleSince } = filterFreshManifest(entries, NOW);
      // Earliest flip is entry a: dataAsOf(a) + window.
      expect(nextStaleSince).toBe(NOW_MS - 1800 * 1000 + 3600 * 1000);
   });

   it("kept stale_ok entries do not set a future nextStaleSince", () => {
      const entries: FreshnessManifest = {
         staleOk: {
            tableName: "stale_ok_t",
            dataAsOf: asOf(7200),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "stale_ok",
         },
      };
      const { manifest, nextStaleSince } = filterFreshManifest(entries, NOW);
      expect(Object.keys(manifest)).toEqual(["staleOk"]);
      // Its window crossing is already in the past, so nothing schedules a
      // future recompute.
      expect(nextStaleSince).toBeNull();
   });

   it("returns null nextStaleSince when nothing is gated", () => {
      const entries: FreshnessManifest = {
         a: { tableName: "a_t" },
         b: { tableName: "b_t", dataAsOf: asOf(10) },
      };
      const { manifest, nextStaleSince } = filterFreshManifest(entries, NOW);
      expect(Object.keys(manifest).sort()).toEqual(["a", "b"]);
      expect(nextStaleSince).toBeNull();
   });
});
