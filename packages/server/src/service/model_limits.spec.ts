import { describe, expect, it } from "bun:test";

import { PayloadTooLargeError, ResponseUnserializableError } from "../errors";
import {
   assertWithinModelResponseLimits,
   queryRowLimitSource,
   resolveModelQueryRowLimit,
   stringifyQueryResponse,
} from "./model_limits";

/**
 * A value whose serialization fails the way an oversized one does. Throwing
 * from `toJSON` reproduces the real failure without allocating the ~512 MB a
 * genuine overflow needs, and the messages are the ones the two engines
 * actually produce (verified on node v24 and bun 1.3).
 */
function failingToSerialize(error: Error) {
   return {
      toJSON() {
         throw error;
      },
   };
}

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
         assertWithinModelResponseLimits(
            500,
            1_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).not.toThrow();
   });

   it("does not throw when row count equals the cap exactly (sentinel hasn't fired)", () => {
      expect(() =>
         assertWithinModelResponseLimits(
            1000,
            1_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).not.toThrow();
   });

   it("throws PayloadTooLargeError with the row-cap message on row overflow", () => {
      expect(() =>
         assertWithinModelResponseLimits(
            1001,
            1_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).toThrow(PayloadTooLargeError);
      expect(() =>
         assertWithinModelResponseLimits(
            1001,
            1_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).toThrow("more than 1000 rows");
   });

   it("throws PayloadTooLargeError with the byte-cap message on byte overflow", () => {
      expect(() =>
         assertWithinModelResponseLimits(
            10,
            50_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).toThrow(PayloadTooLargeError);
      expect(() =>
         assertWithinModelResponseLimits(
            10,
            50_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).toThrow("exceeded 10000 bytes");
   });

   it("prefers the row-cap message when both caps would have fired (row check runs first)", () => {
      expect(() =>
         assertWithinModelResponseLimits(
            2000,
            50_000,
            { maxRows: 1000, maxBytes: 10_000 },
            "model_query",
         ),
      ).toThrow("more than 1000 rows");
   });

   it("disables row cap when maxRows is 0", () => {
      expect(() =>
         assertWithinModelResponseLimits(
            1_000_000,
            1_000,
            { maxRows: 0, maxBytes: 10_000 },
            "model_query",
         ),
      ).not.toThrow();
   });

   it("disables byte cap when maxBytes is 0", () => {
      expect(() =>
         assertWithinModelResponseLimits(
            10,
            1_000_000_000,
            { maxRows: 1000, maxBytes: 0 },
            "model_query",
         ),
      ).not.toThrow();
   });
});

describe("stringifyQueryResponse", () => {
   it("returns the same JSON as JSON.stringify for a serializable response", () => {
      const response = { data: [{ id: 1, name: "a" }], schema: {} };
      expect(stringifyQueryResponse(response, 1, 10_000, "model_query")).toBe(
         JSON.stringify(response),
      );
   });

   it("converts V8's overflow (node: 'Invalid string length') into a 413", () => {
      const tooBig = failingToSerialize(
         new RangeError("Invalid string length"),
      );
      expect(() =>
         stringifyQueryResponse(tooBig, 100_000, 50_000_000, "model_query"),
      ).toThrow(PayloadTooLargeError);
   });

   it("throws the subclass, so REST still 413s and MCP can drop the cap advice", () => {
      const tooBig = failingToSerialize(
         new RangeError("Invalid string length"),
      );
      try {
         stringifyQueryResponse(tooBig, 100_000, 50_000_000, "model_query");
         throw new Error("expected a throw");
      } catch (error) {
         expect(error).toBeInstanceOf(ResponseUnserializableError);
         expect(error).toBeInstanceOf(PayloadTooLargeError);
      }
   });

   it("converts JSC's overflow (bun: 'Out of memory') into a 413", () => {
      // Bun runs the Docker image, and its engine reports the same condition
      // with a different message, so the RangeError class is what we match on.
      const tooBig = failingToSerialize(new RangeError("Out of memory"));
      expect(() =>
         stringifyQueryResponse(tooBig, 100_000, 50_000_000, "model_query"),
      ).toThrow(PayloadTooLargeError);
   });

   it("names the cap, the row count, and remedies that can actually help", () => {
      const tooBig = failingToSerialize(
         new RangeError("Invalid string length"),
      );
      let message = "";
      try {
         stringifyQueryResponse(tooBig, 100_000, 50_000_000, "model_query");
      } catch (error) {
         message = (error as Error).message;
      }
      // Reports the cap as context, not as the limit that fired: on node the
      // engine gives up at ~512 MB whatever the cap is set to, so "exceeded
      // <cap> bytes" would be false for any cap above that.
      expect(message).toContain("could not be serialized");
      expect(message).toContain("byte cap: 50000000");
      expect(message).not.toContain("exceeded 50000000 bytes");
      expect(message).toContain("100000-row");
      expect(message).toContain("Project fewer columns");
      expect(message).toContain("add a LIMIT");
      expect(message).toContain("filter wide values");
      // Raising the cap cannot help a response that will not serialize at any
      // cap, so the byte-cap message's third remedy must not be repeated here.
      expect(message).not.toContain("PUBLISHER_MAX_RESPONSE_BYTES");
   });

   it("omits the cap from the message when the byte cap is disabled", () => {
      const tooBig = failingToSerialize(
         new RangeError("Invalid string length"),
      );
      let message = "";
      try {
         stringifyQueryResponse(tooBig, 42, 0, "notebook_cell");
      } catch (error) {
         message = (error as Error).message;
      }
      expect(message).not.toContain("byte cap");
      expect(message).toContain("could not be serialized");
      expect(message).toContain("42-row");
   });

   it("leaves a stack overflow alone: deep nesting is not a size cap", () => {
      const deep = failingToSerialize(
         new RangeError("Maximum call stack size exceeded"),
      );
      expect(() =>
         stringifyQueryResponse(deep, 1, 50_000_000, "model_query"),
      ).toThrow("Maximum call stack size exceeded");
      expect(() =>
         stringifyQueryResponse(deep, 1, 50_000_000, "model_query"),
      ).not.toThrow(PayloadTooLargeError);
   });

   it("leaves a non-RangeError alone: an unserializable type is not too large", () => {
      // What a BigInt in the payload throws. Reporting it as 413 would send the
      // caller off shrinking a query that is not too big.
      const unserializable = failingToSerialize(
         new TypeError("Do not know how to serialize a BigInt"),
      );
      expect(() =>
         stringifyQueryResponse(unserializable, 1, 50_000_000, "model_query"),
      ).toThrow(TypeError);
   });
});

describe("queryRowLimitSource", () => {
   /**
    * Must mirror resolveModelQueryRowLimit's own `requested` condition: if the
    * two ever disagree, the reported source describes a different limit than
    * the one actually pushed into the SQL.
    */
   it("reports the query when it carried its own positive limit", () => {
      expect(queryRowLimitSource(10)).toBe("query");
      expect(queryRowLimitSource(1)).toBe("query");
   });

   it("reports the server default when the query carried none", () => {
      expect(queryRowLimitSource(undefined)).toBe("server_default");
      expect(queryRowLimitSource(0)).toBe("server_default");
      expect(queryRowLimitSource(-1)).toBe("server_default");
   });

   it("agrees with which limit resolveModelQueryRowLimit actually used", () => {
      const config = { defaultLimit: 1000, maxRows: 0 };
      for (const userLimit of [undefined, 0, -1, 1, 10, 5000]) {
         const used = resolveModelQueryRowLimit(userLimit, config);
         const cameFromQuery = queryRowLimitSource(userLimit) === "query";
         expect(cameFromQuery).toBe(used !== config.defaultLimit);
      }
   });
});
