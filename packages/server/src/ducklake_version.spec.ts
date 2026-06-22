import { describe, expect, it } from "bun:test";
import {
   isCatalogVersionSupported,
   SUPPORTED_CATALOG_VERSIONS,
} from "./ducklake_version";

describe("ducklake_version", () => {
   describe("SUPPORTED_CATALOG_VERSIONS", () => {
      it("matches the baked DuckLake extension's internal list", () => {
         expect([...SUPPORTED_CATALOG_VERSIONS]).toEqual([
            "0.1",
            "0.2",
            "0.3-dev1",
            "0.3",
            "1.0",
         ]);
      });
   });

   describe("isCatalogVersionSupported", () => {
      it("accepts each documented version", () => {
         for (const v of SUPPORTED_CATALOG_VERSIONS) {
            expect(isCatalogVersionSupported(v)).toBe(true);
         }
      });

      it("accepts the 1.0 catalog format produced by the 1.5.x engine", () => {
         expect(isCatalogVersionSupported("1.0")).toBe(true);
      });

      it("rejects 1.0-line versions not in the supported list", () => {
         expect(isCatalogVersionSupported("1.0.0")).toBe(false);
         expect(isCatalogVersionSupported("1.1.0")).toBe(false);
      });

      it("rejects newer versions not in the supported list", () => {
         expect(isCatalogVersionSupported("0.4")).toBe(false);
         expect(isCatalogVersionSupported("0.3-dev2")).toBe(false);
         expect(isCatalogVersionSupported("1.1")).toBe(false);
      });

      it("rejects empty and malformed input without throwing", () => {
         expect(isCatalogVersionSupported("")).toBe(false);
         expect(isCatalogVersionSupported("not a version")).toBe(false);
         expect(isCatalogVersionSupported("0.3 ")).toBe(false);
      });
   });
});
