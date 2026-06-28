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

      it("extracts the freshness window and fallback", () => {
         expect(
            parsePackageMaterialization({
               schedule: "*/15 * * * *",
               freshness: { window: "24h", fallback: "stale_ok" },
            }),
         ).toEqual({
            schedule: "*/15 * * * *",
            freshness: { window: "24h", fallback: "stale_ok" },
         });
      });

      it("defaults the freshness fallback to null when absent", () => {
         expect(
            parsePackageMaterialization({ freshness: { window: "7d" } }),
         ).toEqual({
            schedule: null,
            freshness: { window: "7d", fallback: null },
         });
      });

      it("ignores extra/unknown fields", () => {
         expect(
            parsePackageMaterialization({
               schedule: "*/15 * * * *",
               somethingElse: true,
            }),
         ).toEqual({ schedule: "*/15 * * * *", freshness: null });
      });

      it("degrades non-string leaf values to null", () => {
         expect(parsePackageMaterialization({ schedule: 42 })).toEqual({
            schedule: null,
            freshness: null,
         });
         expect(parsePackageMaterialization({})).toEqual({
            schedule: null,
            freshness: null,
         });
         expect(
            parsePackageMaterialization({
               freshness: { window: 1, fallback: 2 },
            }),
         ).toEqual({
            schedule: null,
            freshness: { window: null, fallback: null },
         });
      });

      it("returns null for non-object input", () => {
         expect(parsePackageMaterialization("0 6 * * *")).toBeNull();
         expect(parsePackageMaterialization(7)).toBeNull();
      });
   });
});
