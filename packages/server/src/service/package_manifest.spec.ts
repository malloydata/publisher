import { describe, expect, it } from "bun:test";
import { parsePackageMaterialization } from "./package_manifest";

describe("service/package_manifest", () => {
   describe("parsePackageMaterialization", () => {
      it("extracts a string schedule", () => {
         expect(parsePackageMaterialization({ schedule: "0 6 * * *" })).toEqual(
            { schedule: "0 6 * * *", freshness: null },
         );
      });

      it("returns null when the block is absent", () => {
         expect(parsePackageMaterialization(undefined)).toBeNull();
         expect(parsePackageMaterialization(null)).toBeNull();
      });

      it("ignores extra/unknown fields", () => {
         expect(
            parsePackageMaterialization({
               schedule: "*/15 * * * *",
               retries: 3,
            }),
         ).toEqual({ schedule: "*/15 * * * *", freshness: null });
      });

      it("degrades a non-string schedule to null", () => {
         expect(parsePackageMaterialization({ schedule: 42 })).toEqual({
            schedule: null,
            freshness: null,
         });
         expect(parsePackageMaterialization({})).toEqual({
            schedule: null,
            freshness: null,
         });
      });

      it("returns null for non-object input", () => {
         expect(parsePackageMaterialization("0 6 * * *")).toBeNull();
         expect(parsePackageMaterialization(7)).toBeNull();
      });

      it("extracts a full freshness block verbatim", () => {
         expect(
            parsePackageMaterialization({
               freshness: { window: "24h", fallback: "stale_ok" },
            }),
         ).toEqual({
            schedule: null,
            freshness: { window: "24h", fallback: "stale_ok" },
         });
      });

      it("keeps a partial freshness block (window only)", () => {
         expect(
            parsePackageMaterialization({ freshness: { window: "1h" } }),
         ).toEqual({ schedule: null, freshness: { window: "1h" } });
      });

      it("drops invalid freshness fields rather than defaulting them", () => {
         // A bad value is reported as absent — never substituted — so the
         // control plane sees exactly what was (validly) declared.
         expect(
            parsePackageMaterialization({
               freshness: { window: 24, fallback: "retry" },
            }),
         ).toEqual({ schedule: null, freshness: {} });
      });

      it("degrades a non-object freshness to null", () => {
         expect(parsePackageMaterialization({ freshness: "24h" })).toEqual({
            schedule: null,
            freshness: null,
         });
      });
   });
});
