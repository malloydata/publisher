// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { extractCompiledModelGivens } from "./modelGivens";
import { runGate } from "./runGate";

const given = (name: string, dflt?: string) => ({
   name,
   type: "string",
   modelFile: "m.malloy",
   default: dflt,
});

describe("runGate", () => {
   it("runs plainly when the model declares nothing the explorer can't supply", () => {
      expect(runGate(undefined, undefined)).toEqual({ kind: "ok" });
      expect(runGate([], [])).toEqual({ kind: "ok" });
   });

   it("runs at the defaults, and names them", () => {
      expect(
         runGate([given("ROLE", "'analyst'"), given("REGION", "'us'")], []),
      ).toEqual({
         kind: "defaults",
         note: "Ran with defaults ROLE = 'analyst', REGION = 'us'",
      });
   });

   it("a value the reader set is neither missing nor a default", () => {
      expect(runGate([given("ROLE")], [], { ROLE: "'admin'" })).toEqual({
         kind: "ok",
      });
      expect(
         runGate([given("ROLE", "'analyst'"), given("REGION", "'us'")], [], {
            ROLE: "'admin'",
         }),
      ).toEqual({
         kind: "defaults",
         note: "Ran with the default REGION = 'us'",
      });
      // Absent is "use the default"; an empty string is a value, as on a
      // dashboard (the control's × is what unsets it).
      expect(runGate([given("ROLE")], [], {}).kind).toBe("blocked");
      expect(runGate([given("ROLE")], [], { ROLE: "" }).kind).toBe("ok");
   });

   it("refuses when a given has no default or a filter is required, and says what is missing", () => {
      expect(runGate([given("ROLE")], [])).toEqual({
         kind: "blocked",
         reason:
            "This needs a value for the given ROLE. Set it in the Givens section above.",
      });
      expect(runGate([], ["Customer ID"])).toEqual({
         kind: "blocked",
         reason:
            "This needs the required filter Customer ID, which this page can't set; open the source in a dashboard or notebook.",
      });
      expect(runGate([given("A"), given("B", "1")], ["F1", "F2"])).toEqual({
         kind: "blocked",
         reason:
            "This needs a value for the given A (set it in the Givens section above) and the required filters F1, F2, which this page can't set; open the source in a dashboard or notebook.",
      });
   });
});

describe("givens the host fills", () => {
   it("are left out of the model's givens, so a run never blocks on one", () => {
      // A gate's GROUPS is declared with no default (a gate may not read a
      // defaulted given), and the host supplies it on every query.
      const givens = extractCompiledModelGivens(
         "m.malloy",
         { givens: [given("GROUPS"), given("MIN", "100")] },
         new Set(["GROUPS"]),
      );
      expect(givens.map((g) => g.name)).toEqual(["MIN"]);
      expect(runGate(givens, [])).toEqual({
         kind: "defaults",
         note: "Ran with the default MIN = 100",
      });
   });
});
