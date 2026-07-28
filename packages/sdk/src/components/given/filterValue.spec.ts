import { describe, expect, it } from "bun:test";
import {
   decodeAtLeast,
   decodeFilterList,
   encodeAtLeast,
   encodeFilterList,
   filterInnerType,
   isFilterType,
} from "./filterValue";

describe("filter type helpers", () => {
   it("recognizes the filter family and its inner type", () => {
      expect(isFilterType("filter<string>")).toBe(true);
      expect(isFilterType("string")).toBe(false);
      expect(isFilterType(undefined)).toBe(false);
      expect(filterInnerType("filter<timestamp>")).toBe("timestamp");
      expect(filterInnerType("number")).toBe(undefined);
   });
});

describe("encodeFilterList", () => {
   it("joins values the way a string filter reads them", () => {
      expect(encodeFilterList(["us-east", "us-west"])).toBe("us-east, us-west");
   });

   it("leaves an apostrophe bare", () => {
      // Ordinary inside a filter word; quoting every one would be noise.
      expect(encodeFilterList(["Levi's"])).toBe("Levi's");
   });

   it("quotes what would otherwise change the parse", () => {
      expect(encodeFilterList(["Ben & Jerry, Inc"])).toBe('"Ben & Jerry, Inc"');
      expect(encodeFilterList(['say "hi"'])).toBe('"say \\"hi\\""');
      expect(encodeFilterList(["back\\slash"])).toBe('"back\\\\slash"');
      expect(encodeFilterList([" padded "])).toBe('" padded "');
   });

   it("is empty for no selection, which is the natural All", () => {
      expect(encodeFilterList([])).toBe("");
   });
});

describe("decodeFilterList", () => {
   it("round-trips the list form, quoting included", () => {
      for (const values of [
         ["us-east", "us-west"],
         ["Levi's"],
         ["Ben & Jerry, Inc", "Nike"],
         ['say "hi"'],
         ["back\\slash"],
         [" padded "],
      ]) {
         expect(decodeFilterList(encodeFilterList(values))).toEqual(values);
      }
   });

   it("reads an empty filter as no selection", () => {
      expect(decodeFilterList("")).toEqual([]);
      expect(decodeFilterList("   ")).toEqual([]);
   });

   it("keeps a filter it cannot represent in one piece", () => {
      // A control that cannot express `-Nike` should preserve it, not split or
      // reinterpret it into something that means something else.
      expect(decodeFilterList("-Nike")).toEqual(["-Nike"]);
      expect(decodeFilterList("%foo%")).toEqual(["%foo%"]);
   });
});

describe("encodeAtLeast", () => {
   it("round-trips a slider bound", () => {
      expect(encodeAtLeast(100)).toBe(">= 100");
      expect(decodeAtLeast(">= 100")).toBe(100);
      expect(decodeAtLeast(">=2.5")).toBe(2.5);
      expect(decodeAtLeast(">= -5")).toBe(-5);
   });

   it("declines to read a filter that is not a lower bound", () => {
      // The slider then leaves the value alone rather than showing a position
      // that misrepresents it.
      expect(decodeAtLeast("100 to 500")).toBe(undefined);
      expect(decodeAtLeast("> 100")).toBe(undefined);
      expect(decodeAtLeast("")).toBe(undefined);
   });
});
