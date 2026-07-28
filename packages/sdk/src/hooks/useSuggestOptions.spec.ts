import { describe, expect, it } from "bun:test";
import { readOptionValues } from "./useSuggestOptions";

describe("readOptionValues", () => {
   it("takes the named dimension's column", () => {
      const rows = JSON.stringify([
         { region: "us-east", orders: 12 },
         { region: "us-west", orders: 8 },
      ]);
      expect(readOptionValues(rows, "region")).toEqual(["us-east", "us-west"]);
   });

   it("falls back to the first column", () => {
      // A `suggest { query=… }` names no dimension, and its column can be
      // called anything.
      const rows = JSON.stringify([{ name: "Nike" }, { name: "Levi's" }]);
      expect(readOptionValues(rows, undefined)).toEqual(["Nike", "Levi's"]);
   });

   it("drops nulls and duplicates", () => {
      // "No value" is expressed by clearing the control, not by picking an
      // empty option.
      const rows = JSON.stringify([
         { region: "us-east" },
         { region: null },
         { region: "us-east" },
      ]);
      expect(readOptionValues(rows, "region")).toEqual(["us-east"]);
   });

   it("renders a non-string value as text", () => {
      const rows = JSON.stringify([{ year: 2024 }, { year: 2025 }]);
      expect(readOptionValues(rows, "year")).toEqual(["2024", "2025"]);
   });

   it("is empty rather than throwing on anything unexpected", () => {
      expect(readOptionValues(undefined, undefined)).toEqual([]);
      expect(readOptionValues("not json", undefined)).toEqual([]);
      expect(readOptionValues('{"not":"an array"}', undefined)).toEqual([]);
      expect(readOptionValues("[]", undefined)).toEqual([]);
   });
});
