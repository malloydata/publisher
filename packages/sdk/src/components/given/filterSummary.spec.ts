// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Given } from "../../client";
import { summarizeGivenValue } from "./filterSummary";
import {
   encodeAtLeast,
   encodeBetween,
   encodeDayRange,
   encodeFilterList,
   encodeTimePreset,
} from "./filterValue";

const given = (over: Partial<Given>): Given => ({ name: "g", ...over });

describe("summarizeGivenValue", () => {
   it("reads an unset picker as All, and an unset range as Any", () => {
      expect(
         summarizeGivenValue(
            given({ type: "filter<string>", control: "multiselect" }),
            null,
         ),
      ).toBe("All");
      expect(summarizeGivenValue(given({ type: "filter<number>" }), null)).toBe(
         "Any",
      );
      expect(summarizeGivenValue(given({ type: "boolean" }), undefined)).toBe(
         "Any",
      );
   });

   it("shows an unset given's default as its value, read the way its type reads", () => {
      expect(
         summarizeGivenValue(
            given({ type: "date", default: "@2023-01-01" }),
            null,
         ),
      ).toBe("Jan 1, 2023");
      expect(
         summarizeGivenValue(given({ type: "string", default: "'WN'" }), null),
      ).toBe("WN");
   });

   it("lists picked values, shortening a long list to the first and a count", () => {
      const type = "filter<string>";
      expect(
         summarizeGivenValue(
            given({ type }),
            encodeFilterList(["Outerwear", "Footwear"]),
         ),
      ).toBe("Outerwear, Footwear");
      expect(
         summarizeGivenValue(
            given({ type }),
            encodeFilterList(["Outerwear", "Footwear", "Dresses", "Swimwear"]),
         ),
      ).toBe("Outerwear +3");
   });

   it("reads number ranges and time presets", () => {
      expect(
         summarizeGivenValue(
            given({ type: "filter<number>" }),
            encodeAtLeast(50),
         ),
      ).toBe("≥ 50");
      expect(
         summarizeGivenValue(
            given({ type: "filter<number>" }),
            encodeBetween(10, 90),
         ),
      ).toBe("10–90");
      expect(
         summarizeGivenValue(
            given({ type: "filter<date>" }),
            encodeTimePreset("30d"),
         ),
      ).toBe("Last 30 days");
      expect(
         summarizeGivenValue(
            given({ type: "filter<date>" }),
            encodeDayRange("2024-01-01", "2024-03-31"),
         ),
      ).toBe("Jan 1, 2024 – Mar 31, 2024");
   });

   it("reads scalars plainly, and an explicit empty string as (empty)", () => {
      expect(summarizeGivenValue(given({ type: "boolean" }), true)).toBe("Yes");
      expect(summarizeGivenValue(given({ type: "number" }), 7)).toBe("7");
      expect(
         summarizeGivenValue(
            given({ type: "date" }),
            new Date("2023-01-01T00:00:00Z"),
         ),
      ).toBe("Jan 1, 2023");
      expect(summarizeGivenValue(given({ type: "string" }), "")).toBe(
         "(empty)",
      );
   });

   it("truncates a long free-text value", () => {
      const text = "a".repeat(60);
      const shown = summarizeGivenValue(given({ type: "string" }), text);
      expect(shown.length).toBe(28);
      expect(shown.endsWith("…")).toBe(true);
   });
});
