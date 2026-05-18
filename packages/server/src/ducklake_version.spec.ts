import { describe, expect, it } from "bun:test";
import {
   isCatalogVersionSupported,
   SUPPORTED_CATALOG_VERSIONS,
} from "./ducklake_version";

describe("ducklake_version", () => {
   describe("SUPPORTED_CATALOG_VERSIONS", () => {
      it("matches the 0.3-line DuckLake extension's internal list", () => {
         expect([...SUPPORTED_CATALOG_VERSIONS]).toEqual([
            "0.1",
            "0.2",
            "0.3-dev1",
            "0.3",
         ]);
      });
   });

   describe("isCatalogVersionSupported", () => {
      it("accepts each documented 0.3-line version", () => {
         for (const v of SUPPORTED_CATALOG_VERSIONS) {
            expect(isCatalogVersionSupported(v)).toBe(true);
         }
      });

      it("rejects 1.0-line versions", () => {
         expect(isCatalogVersionSupported("1.0")).toBe(false);
         expect(isCatalogVersionSupported("1.0.0")).toBe(false);
         expect(isCatalogVersionSupported("1.1.0")).toBe(false);
      });

      it("rejects newer pre-1.0 versions not in the supported list", () => {
         expect(isCatalogVersionSupported("0.4")).toBe(false);
         expect(isCatalogVersionSupported("0.3-dev2")).toBe(false);
      });

      it("rejects empty and malformed input without throwing", () => {
         expect(isCatalogVersionSupported("")).toBe(false);
         expect(isCatalogVersionSupported("not a version")).toBe(false);
         expect(isCatalogVersionSupported("0.3 ")).toBe(false);
      });
   });
});
