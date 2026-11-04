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
         kind: "noted",
         note: "Ran with the defaults ROLE: analyst, REGION: us.",
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
         kind: "noted",
         note: "Ran with the default REGION: us.",
      });
      // Absent is "use the default"; an empty string is a value, as on a
      // dashboard (the control's × is what unsets it).
      expect(runGate([given("ROLE")], [], {}).kind).toBe("blocked");
      expect(runGate([given("ROLE")], [], { ROLE: "" }).kind).toBe("ok");
   });

   it("refuses when a given has no default or a filter is required, and says what is missing", () => {
      expect(runGate([given("ROLE")], [])).toEqual({
         kind: "blocked",
         canRunAnyway: false,
         reason:
            "This needs a value for the given ROLE. Set it in the Givens section above.",
      });
      expect(runGate([], ["Customer ID"])).toEqual({
         kind: "blocked",
         canRunAnyway: false,
         reason:
            "This needs the required filter Customer ID, which this page can't set; open the source in a dashboard or notebook.",
      });
      expect(runGate([given("A"), given("B", "1")], ["F1", "F2"])).toEqual({
         kind: "blocked",
         canRunAnyway: false,
         reason:
            "This needs a value for the given A (set it in the Givens section above) and the required filters F1, F2, which this page can't set; open the source in a dashboard or notebook.",
      });
   });

   // The storefront's overview givens: each named as its control words it.
   const category = {
      ...given("CATEGORY", "f''"),
      type: "filter<string>",
      label: "Category",
   };
   const since = {
      ...given("SINCE", "@2023-01-01"),
      type: "date",
      label: "Ordered since",
   };
   const minSale = {
      ...given("MIN_SALE", "f''"),
      type: "filter<number>",
      label: "Minimum line total",
   };
   const region = { ...given("REGION"), label: "Region" };

   it("names a default the way its control does, never as a Malloy literal", () => {
      expect(runGate([category, since, minSale], [])).toEqual({
         kind: "noted",
         note: "Ran with the defaults Category: All, Ordered since: Jan 1, 2023, Minimum line total: Any.",
      });
      expect(runGate([region], [])).toEqual({
         kind: "blocked",
         canRunAnyway: false,
         reason:
            "This needs a value for the given Region. Set it in the Givens section above.",
      });
   });

   it("says it can't tell which givens a source reads, and what the run had for each", () => {
      const all = new Set(["CATEGORY", "REGION", "SINCE"]);
      expect(
         runGate([category, region, since], [], { REGION: "West" }, all),
      ).toEqual({
         kind: "noted",
         note: "This page can't tell which givens this source reads. Where it reads one, it ran with Region: West and the defaults Category: All, Ordered since: Jan 1, 2023.",
      });
      expect(runGate([region], [], { REGION: "West" }, all)).toEqual({
         kind: "noted",
         note: "This page can't tell which givens this source reads. Where it reads one, it ran with Region: West.",
      });
      // A given with no value still holds the run: if the source reads it, a
      // run without it fails with no control to fix it.
      expect(runGate([region], [], undefined, all)).toEqual({
         kind: "blocked",
         canRunAnyway: true,
         reason:
            "This needs a value for the given Region. Set it in the Givens section above. This page can't tell whether this source reads it.",
      });
      expect(runGate([], [], undefined, all)).toEqual({ kind: "ok" });
   });

   it("names apart only the givens it can't tie to the source", () => {
      // The source reads REGION in its own text and is built on an import that
      // may read CATEGORY and SINCE.
      const maybe = new Set(["CATEGORY", "SINCE"]);
      expect(
         runGate([category, region, since], [], { REGION: "West" }, maybe),
      ).toEqual({
         kind: "noted",
         note: "This page can't tell whether this source reads Category or Ordered since. Where it does, it ran with the defaults Category: All, Ordered since: Jan 1, 2023.",
      });
      expect(runGate([category, region, since], [], undefined, maybe)).toEqual({
         kind: "blocked",
         canRunAnyway: false,
         reason:
            "This needs a value for the given Region. Set it in the Givens section above.",
      });
      expect(
         runGate([category, { ...minSale, default: undefined }], [], {}, maybe),
      ).toEqual({
         kind: "blocked",
         canRunAnyway: false,
         reason:
            "This needs a value for the given Minimum line total. Set it in the Givens section above.",
      });
      expect(runGate([category, region], [], {}, new Set(["REGION"]))).toEqual({
         kind: "blocked",
         canRunAnyway: true,
         reason:
            "This needs a value for the given Region. Set it in the Givens section above. This page can't tell whether this source reads it.",
      });
   });

   it("runs anyway, at the reader's word, without givens it can't tie to the source", () => {
      const maybe = new Set(["REGION", "CATEGORY"]);
      expect(runGate([region, category], [], {}, maybe, true)).toEqual({
         kind: "noted",
         note: "Ran without a value for the given Region, which this page can't tell this source reads. This page can't tell which givens this source reads. Where it reads one, it ran with the default Category: All.",
      });
      // Never for a given the source is known to read, nor past a required
      // filter: those runs fail, with nothing the reader can do.
      expect(runGate([region], [], {}, new Set(), true)).toMatchObject({
         kind: "blocked",
         canRunAnyway: false,
      });
      expect(
         runGate([region], ["Customer ID"], {}, new Set(["REGION"]), true),
      ).toMatchObject({ kind: "blocked", canRunAnyway: false });
   });

   it("names a named query with no source as the query, not a source", () => {
      expect(
         runGate(
            [region],
            [],
            { REGION: "West" },
            new Set(["REGION"]),
            false,
            "query",
         ),
      ).toEqual({
         kind: "noted",
         note: "This page can't tell which givens this query reads. Where it reads one, it ran with Region: West.",
      });
   });

   it("names every value a run had, never cut to a button's width", () => {
      const picked = "Outerwear,Footwear,Dresses,Swimwear";
      expect(
         runGate(
            [{ ...category, control: "multiselect" as const }],
            [],
            { CATEGORY: picked },
            new Set(["CATEGORY"]),
         ),
      ).toEqual({
         kind: "noted",
         note: "This page can't tell which givens this source reads. Where it reads one, it ran with Category: Outerwear, Footwear, Dresses, Swimwear.",
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
         kind: "noted",
         note: "Ran with the default MIN: 100.",
      });
   });
});
