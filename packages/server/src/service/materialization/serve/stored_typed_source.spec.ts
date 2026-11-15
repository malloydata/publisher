// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { storedTypedSource } from "./stored_typed_source";

const binding = {
   destinationName: "lake",
   virtualHandle: "t_handle",
   schema: [
      { name: "carrier", type: "string" },
      { name: "m", type: "timestamp" },
      { name: "n", type: "number" },
   ],
};

describe("storedTypedSource", () => {
   it("types each stored column, keeping a plain dimension's notes on its field", () => {
      const { typeText, timeframes } = storedTypedSource({
         columns: [
            { name: "carrier", notes: ["# label"], kind: "dimension" },
            { name: "n", notes: ["# currency"], kind: "measure" },
         ],
         casts: new Map(),
         binding,
         typeName: "q__type",
         stored: "q__stored",
      });
      expect(typeText).toContain("# label");
      // A measure's notes go on its re-aggregation, not its stored field.
      expect(typeText).not.toContain("# currency");
      expect(typeText).toContain(".virtual('t_handle')::q__type");
      expect(timeframes).toBe("");
   });

   it("restates a timeframe column and a cast one, with their notes there", () => {
      const { typeText, timeframes } = storedTypedSource({
         columns: [
            {
               name: "m",
               timeframe: "month",
               notes: ["# tag"],
               kind: "dimension",
            },
            { name: "carrier", notes: ["# label"], kind: "dimension" },
         ],
         casts: new Map([["carrier", { castTo: "BOOLEAN" as const }]]),
         binding,
         typeName: "q__type",
         stored: "q__stored",
      });
      expect(typeText).not.toContain("# tag");
      expect(typeText).not.toContain("# label");
      expect(timeframes).toContain("m");
      expect(timeframes).toContain("# tag");
      expect(timeframes).toContain("carrier");
   });
});
