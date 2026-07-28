import { describe, expect, it } from "bun:test";
import type { GivenValue } from "../../hooks/givenValue";
import {
   givensToParams,
   givensToRequest,
   givenToParam,
   paramsToGivens,
   paramToGiven,
} from "./paramCodec";

describe("givenToParam", () => {
   it("omits what the server should fill from the model default", () => {
      expect(givenToParam(undefined)).toBe(undefined);
      expect(givenToParam(null)).toBe(undefined);
   });

   it("keeps an empty string, which is a real value", () => {
      // For a filter this is the empty filter — "All" — and is meaningfully
      // different from leaving the declaration's default in place.
      expect(givenToParam("")).toBe("");
   });

   it("renders each value type as one parameter", () => {
      expect(givenToParam("Nike")).toBe("Nike");
      expect(givenToParam(42)).toBe("42");
      expect(givenToParam(false)).toBe("false");
      expect(givenToParam(new Date("2024-03-01T00:00:00Z"))).toBe("2024-03-01");
      expect(givenToParam(["a", "b"])).toBe("a,b");
   });
});

describe("paramToGiven", () => {
   it("leaves a filter as the filter syntax it is", () => {
      // `filter<number>` values are filter expressions, not numbers, so the
      // inner type must not pull them through a numeric coercion.
      expect(paramToGiven("filter<number>", ">= 100")).toBe(">= 100");
      expect(paramToGiven("filter<string>", "us-east, us-west")).toBe(
         "us-east, us-west",
      );
      expect(paramToGiven("filter<date>", "2024-03-01")).toBe("2024-03-01");
   });

   it("reads plain types back as their type", () => {
      expect(paramToGiven("number", "42")).toBe(42);
      expect(paramToGiven("boolean", "true")).toBe(true);
      expect(paramToGiven("boolean", "false")).toBe(false);
      expect(paramToGiven("string", "Nike")).toBe("Nike");
      expect(paramToGiven("array<string>", "a,b")).toEqual(["a", "b"]);
      expect(paramToGiven("date", "2024-03-01")).toBeInstanceOf(Date);
   });

   it("passes a nonsense value through for the server to reject", () => {
      // A URL is hand-editable. A NaN or an Invalid Date would reach the server
      // as null and read as "unset"; the raw text gets an error naming the
      // given instead, which is the more useful failure.
      expect(paramToGiven("number", "abc")).toBe("abc");
      expect(paramToGiven("date", "not-a-date")).toBe("not-a-date");
   });
});

describe("paramsToGivens", () => {
   const declared = new Map<string, string | undefined>([
      ["REGION", "filter<string>"],
      ["LIMIT", "number"],
   ]);

   it("reads the declared givens out of a URL", () => {
      expect(
         paramsToGivens({ REGION: "us-east", LIMIT: "10" }, declared),
      ).toEqual(
         new Map<string, GivenValue>([
            ["REGION", "us-east"],
            ["LIMIT", 10],
         ]),
      );
   });

   it("ignores a parameter the dashboard does not declare", () => {
      // Binding an undeclared given fails the query, and an unrelated query
      // parameter on the page URL must not be able to break the dashboard.
      expect(
         paramsToGivens({ REGION: "us-east", utm_source: "email" }, declared),
      ).toEqual(new Map<string, GivenValue>([["REGION", "us-east"]]));
   });
});

describe("givensToParams", () => {
   it("drops unset givens so a default view has a clean URL", () => {
      const values = new Map<string, GivenValue>([
         ["REGION", "us-east"],
         ["LIMIT", null],
      ]);
      expect(givensToParams(values)).toEqual({ REGION: "us-east" });
   });
});

describe("givensToRequest", () => {
   const values = new Map<string, GivenValue>([
      ["REGION", "us-east"],
      ["LIMIT", 10],
      ["SINCE", new Date("2024-03-01T04:05:06.007Z")],
   ]);
   const types = new Map<string, string | undefined>([
      ["REGION", "filter<string>"],
      ["LIMIT", "number"],
      ["SINCE", "date"],
   ]);

   it("keeps types, unlike the URL form", () => {
      expect(givensToRequest(values, types)).toEqual({
         REGION: "us-east",
         LIMIT: 10,
         SINCE: "2024-03-01",
      });
   });

   it("narrows to a tile's own givens", () => {
      // A composite tile must be run with only the givens it references.
      expect(givensToRequest(values, types, ["REGION"])).toEqual({
         REGION: "us-east",
      });
      expect(givensToRequest(values, types, [])).toEqual({});
   });

   // The server takes a different spelling for each of the three time types and
   // rejects the other two, so one blanket ISO string fails two of them.
   it.each([
      ["date", "2024-03-01"],
      ["timestamp", "2024-03-01T04:05:06.007"],
      ["timestamptz", "2024-03-01T04:05:06.007Z"],
   ])("encodes a Date for a %s given", (type, expected) => {
      const request = givensToRequest(
         new Map<string, GivenValue>([
            ["SINCE", new Date("2024-03-01T04:05:06.007Z")],
         ]),
         new Map([["SINCE", type]]),
      );
      expect(request).toEqual({ SINCE: expected });
   });
});
