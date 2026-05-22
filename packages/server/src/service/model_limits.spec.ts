import { describe, expect, it } from "bun:test";

import { PayloadTooLargeError } from "../errors";
import {
   assertWithinModelResponseLimits,
   resolveModelQueryRowLimit,
} from "./model_limits";

describe("resolveModelQueryRowLimit", () => {
   it("uses the user's LIMIT when set and below the maxRows ceiling", () => {
      expect(
         resolveModelQueryRowLimit(500, {
            defaultLimit: 1000,
            maxRows: 100_000,
         }),
      ).toBe(500);
   });

   it("falls back to defaultLimit when the user's LIMIT is undefined", () => {
      expect(
         resolveModelQueryRowLimit(undefined, {
            defaultLimit: 1000,
            maxRows: 100_000,
         }),
      ).toBe(1000);
   });

   it("falls back to defaultLimit when the user's LIMIT is 0 (Malloy returns 0 for 'no limit')", () => {
      // Malloy's PreparedResult returns 0 from `resultExplore.limit` when the
      // query has no LIMIT clause; treat that as "no user limit".
      expect(
         resolveModelQueryRowLimit(0, {
            defaultLimit: 1000,
            maxRows: 100_000,
         }),
      ).toBe(1000);
   });

   it("clamps a too-high user LIMIT to the maxRows + 1 sentinel", () => {
      expect(
         resolveModelQueryRowLimit(1_000_000, {
            defaultLimit: 1000,
            maxRows: 100_000,
         }),
      ).toBe(100_001);
   });

   it("clamps a too-high defaultLimit to the maxRows + 1 sentinel", () => {
      // Operator misconfigured DEFAULT > MAX; the hard cap still wins.
      expect(
         resolveModelQueryRowLimit(undefined, {
            defaultLimit: 1_000_000,
            maxRows: 50,
         }),
      ).toBe(51);
   });

   it("returns the requested limit unchanged when maxRows is 0 (cap disabled)", () => {
      expect(
         resolveModelQueryRowLimit(1_000_000, {
            defaultLimit: 1000,
            maxRows: 0,
         }),
      ).toBe(1_000_000);
   });

   it("returns the default unchanged when maxRows is 0 and no user limit", () => {
      expect(
         resolveModelQueryRowLimit(undefined, {
            defaultLimit: 1000,
            maxRows: 0,
         }),
      ).toBe(1000);
   });

   it("rejects negative user limits by treating them as 'no limit'", () => {
      // Defensive — shouldn't happen in practice, but a -1 from a malformed
      // PreparedResult shouldn't propagate as a negative rowLimit to the driver.
      expect(
         resolveModelQueryRowLimit(-1, {
            defaultLimit: 1000,
            maxRows: 100_000,
         }),
      ).toBe(1000);
   });
});

describe("assertWithinModelResponseLimits", () => {
   it("does not throw when both counts are below their caps", () => {
      expect(() =>
         assertWithinModelResponseLimits(500, 1_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).not.toThrow();
   });

   it("does not throw when row count equals the cap exactly (sentinel hasn't fired)", () => {
      expect(() =>
         assertWithinModelResponseLimits(1000, 1_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).not.toThrow();
   });

   it("throws PayloadTooLargeError with the row-cap message on row overflow", () => {
      expect(() =>
         assertWithinModelResponseLimits(1001, 1_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).toThrow(PayloadTooLargeError);
      expect(() =>
         assertWithinModelResponseLimits(1001, 1_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).toThrow("more than 1000 rows");
   });

   it("throws PayloadTooLargeError with the byte-cap message on byte overflow", () => {
      expect(() =>
         assertWithinModelResponseLimits(10, 50_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).toThrow(PayloadTooLargeError);
      expect(() =>
         assertWithinModelResponseLimits(10, 50_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).toThrow("exceeded 10000 bytes");
   });

   it("prefers the row-cap message when both caps would have fired (row check runs first)", () => {
      expect(() =>
         assertWithinModelResponseLimits(2000, 50_000, {
            maxRows: 1000,
            maxBytes: 10_000,
         }),
      ).toThrow("more than 1000 rows");
   });

   it("disables row cap when maxRows is 0", () => {
      expect(() =>
         assertWithinModelResponseLimits(1_000_000, 1_000, {
            maxRows: 0,
            maxBytes: 10_000,
         }),
      ).not.toThrow();
   });

   it("disables byte cap when maxBytes is 0", () => {
      expect(() =>
         assertWithinModelResponseLimits(10, 1_000_000_000, {
            maxRows: 1000,
            maxBytes: 0,
         }),
      ).not.toThrow();
   });
});
