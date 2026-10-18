// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Given } from "../../client";
import {
   missingRequiredGivens,
   requiredPrompt,
   waitingOn,
   waitingText,
} from "./required";

const YEAR: Given = {
   name: "YEAR",
   label: "Recall Year",
   type: "number",
   required: true,
};
const MAKER: Given = {
   name: "MAKER",
   label: "Maker",
   type: "filter<string>",
   required: true,
};
const REGION: Given = { name: "REGION", type: "filter<string>", default: "" };

describe("required givens", () => {
   it("are missing until set; an optional one never is", () => {
      const missing = missingRequiredGivens([YEAR, MAKER, REGION], new Map());
      expect(missing.map((g) => g.name)).toEqual(["YEAR", "MAKER"]);
   });

   it("count an empty filter as a value (All), but not an empty number", () => {
      const missing = missingRequiredGivens(
         [YEAR, MAKER],
         new Map<string, string>([
            ["YEAR", ""],
            ["MAKER", ""],
         ]),
      );
      expect(missing.map((g) => g.name)).toEqual(["YEAR"]);
   });

   it("count a value the host sets", () => {
      expect(missingRequiredGivens([YEAR], new Map(), { YEAR: 2020 })).toEqual(
         [],
      );
   });

   it("say what to set, and which tiles wait", () => {
      expect(requiredPrompt([YEAR, MAKER])).toBe(
         "Pick a value for Recall Year and Maker to see this report.",
      );
      expect(waitingOn([YEAR, MAKER], ["MAKER"]).map((g) => g.name)).toEqual([
         "MAKER",
      ]);
      expect(waitingOn([YEAR], undefined)).toEqual([]);
      expect(waitingText([YEAR])).toBe("Waiting for Recall Year.");
   });
});
