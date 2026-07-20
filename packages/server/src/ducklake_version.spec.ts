import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import {
   catalogFormatRangeForEngine,
   compareCatalogFormat,
   ENGINE_MAX_FORMAT,
   isCatalogFormatInRange,
   isCatalogFormatSupportedByEngine,
   MIN_CATALOG_FORMAT,
   parseCatalogFormat,
   parseEngineMajorMinor,
} from "./ducklake_version";

/**
 * Resolve the pinned `@duckdb/node-api` engine version from bun.lock, the same
 * source `scripts/duckdb-version.js` reads (mirrored here rather than imported
 * because the server tsconfig has `allowJs: false`). Prefers the version nested
 * under `@malloydata/db-duckdb` — the engine Malloy actually runs on — and
 * falls back to the deduped top-level entry.
 */
function resolvePinnedDuckDBVersion(): string {
   const lock = readFileSync(
      fileURLToPath(new URL("../../../bun.lock", import.meta.url)),
      "utf8",
   );
   const nested = lock.match(
      /"@malloydata\/db-duckdb\/@duckdb\/node-api":\s*\["@duckdb\/node-api@(\d+\.\d+\.\d+[^"]*)"/,
   );
   const plain = lock.match(
      /"@duckdb\/node-api":\s*\["@duckdb\/node-api@(\d+\.\d+\.\d+[^"]*)"/,
   );
   const match = nested ?? plain;
   if (!match) throw new Error("@duckdb/node-api not found in bun.lock");
   return match[1];
}

describe("ducklake_version", () => {
   describe("parseCatalogFormat", () => {
      it("parses two-part and pre-release formats", () => {
         expect(parseCatalogFormat("1.0")).toEqual({ major: 1, minor: 0 });
         expect(parseCatalogFormat("0.3-dev1")).toEqual({
            major: 0,
            minor: 3,
            prerelease: "dev1",
         });
      });

      it("rejects malformed input without throwing", () => {
         // Three-part strings are malformed for a catalog format, not a newer
         // version -- they must not parse as a bigger number.
         expect(parseCatalogFormat("1.0.0")).toBeNull();
         expect(parseCatalogFormat("1.1.0")).toBeNull();
         expect(parseCatalogFormat("")).toBeNull();
         expect(parseCatalogFormat("not a version")).toBeNull();
         expect(parseCatalogFormat("0.3 ")).toBeNull();
      });
   });

   describe("compareCatalogFormat", () => {
      const p = (s: string) => parseCatalogFormat(s)!;
      it("orders by major, then minor", () => {
         expect(compareCatalogFormat(p("0.3"), p("1.0"))).toBe(-1);
         expect(compareCatalogFormat(p("1.1"), p("1.0"))).toBe(1);
         expect(compareCatalogFormat(p("1.0"), p("1.0"))).toBe(0);
      });
      it("sorts a release above its own pre-releases", () => {
         expect(compareCatalogFormat(p("1.0"), p("1.0-dev1"))).toBe(1);
         expect(compareCatalogFormat(p("0.3-dev1"), p("0.3"))).toBe(-1);
      });
   });

   describe("parseEngineMajorMinor", () => {
      it("extracts the minor line from assorted engine spec forms", () => {
         expect(parseEngineMajorMinor("1.5.3")).toEqual({ major: 1, minor: 5 });
         expect(parseEngineMajorMinor("1.5.3-r.2")).toEqual({
            major: 1,
            minor: 5,
         });
         expect(parseEngineMajorMinor("v1.5.3")).toEqual({
            major: 1,
            minor: 5,
         });
      });
      it("returns null when no version is present", () => {
         expect(parseEngineMajorMinor("nope")).toBeNull();
      });
   });

   describe("catalogFormatRangeForEngine", () => {
      it("derives [1.0, maxFormat] for a covered engine (all patch levels)", () => {
         expect(catalogFormatRangeForEngine("1.5.3")).toEqual({
            min: "1.0",
            max: "1.0",
            engineVersion: "1.5",
         });
         // Any patch of a known minor line resolves to the same range.
         expect(catalogFormatRangeForEngine("1.5.9")).toEqual({
            min: "1.0",
            max: "1.0",
            engineVersion: "1.5",
         });
      });
      it("returns null (drift signal) for an engine with no matrix row", () => {
         expect(catalogFormatRangeForEngine("1.6.0")).toBeNull();
         expect(catalogFormatRangeForEngine("2.0.0")).toBeNull();
         expect(catalogFormatRangeForEngine("garbage")).toBeNull();
      });
   });

   // These reproduce the accept/reject set the prior enumerated-list work
   // asserted, now expressed as a RANGE derived from the 1.5.x engine.
   describe("isCatalogFormatSupportedByEngine (engine 1.5.x)", () => {
      const engine = "1.5.3";
      it("accepts the 1.0 catalog format produced by the 1.5.x engine", () => {
         expect(isCatalogFormatSupportedByEngine("1.0", engine)).toBe(true);
      });
      it("rejects the 0.3-line formats (attach only via migration)", () => {
         for (const v of ["0.1", "0.2", "0.3-dev1", "0.3"]) {
            expect(isCatalogFormatSupportedByEngine(v, engine)).toBe(false);
         }
      });
      it("rejects formats newer than the supported max", () => {
         for (const v of ["1.1", "2.0", "0.4", "0.3-dev2"]) {
            expect(isCatalogFormatSupportedByEngine(v, engine)).toBe(false);
         }
      });
      it("rejects malformed / three-part input without throwing", () => {
         for (const v of ["1.0.0", "1.1.0", "", "not a version", "0.3 "]) {
            expect(isCatalogFormatSupportedByEngine(v, engine)).toBe(false);
         }
      });
      it("rejects everything when the engine is unknown (no matrix row)", () => {
         expect(isCatalogFormatSupportedByEngine("1.0", "1.6.0")).toBe(false);
      });
   });

   describe("isCatalogFormatInRange", () => {
      const range = catalogFormatRangeForEngine("1.5.3")!;
      it("is inclusive at both ends", () => {
         expect(isCatalogFormatInRange("1.0", range)).toBe(true);
      });
      it("rejects below min and above max", () => {
         expect(isCatalogFormatInRange("0.9", range)).toBe(false);
         expect(isCatalogFormatInRange("1.1", range)).toBe(false);
      });
   });

   // The version-contract: the ENGINE_MAX_FORMAT matrix MUST cover the DuckDB
   // engine this repo is pinned to. This is the same drift guard the CI script
   // enforces (scripts/validate-ducklake-catalog-range.ts); asserting it here
   // fails `bun test` the moment a Malloy bump moves the engine past the matrix.
   describe("pinned-engine drift contract", () => {
      it("has a matrix row covering the pinned @duckdb/node-api engine", () => {
         const engineVersion = resolvePinnedDuckDBVersion();
         const range = catalogFormatRangeForEngine(engineVersion);
         expect(range).not.toBeNull();
         expect(range!.min).toBe(MIN_CATALOG_FORMAT);
         // The matrix must not be empty and its rows must be well-formed.
         expect(ENGINE_MAX_FORMAT.length).toBeGreaterThan(0);
      });
   });
});
