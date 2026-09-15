// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   applyMapping,
   controlsOf,
   declareControl,
   defaultOperator,
   givenNameFor,
   mappingOf,
   newLocalGiven,
   removeControl,
} from "./controls";
import type { DashboardDocument } from "./document";

const doc = (): DashboardDocument => ({
   title: "T",
   imports: [{ kind: "names", names: ["order_items"], from: "../m.malloy" }],
   sources: [{ name: "a", base: "order_items" }],
   localGivens: [
      {
         name: "CATEGORY",
         type: "filter<string>",
         default: "f''",
         label: "Category",
         control: "select",
         suggest: { source: "products", dimension: "category" },
      },
   ],
   tiles: [
      {
         name: "by_cat",
         source: "a",
         declaration: { kind: "reference", from: "by_category" },
         filters: [{ field: "category", given: "CATEGORY" }],
      },
      {
         name: "by_brand",
         source: "a",
         declaration: { kind: "reference", from: "top_brands" },
      },
      { name: "kpis", source: "a", declaration: { kind: "inline" } },
   ],
});

describe("controlsOf", () => {
   it("lists the dashboard's own controls first, then the model's", () => {
      const out = controlsOf(doc(), [
         { name: "SINCE", type: "date", label: "Since" },
         // Same name as a local one: the local declaration is the one that
         // binds, so the model's copy is not a second control.
         { name: "CATEGORY", type: "filter<string>" },
      ]);
      expect(out.map((c) => [c.name, c.origin, c.boundTiles])).toEqual([
         ["CATEGORY", "dashboard", 1],
         ["SINCE", "model", 0],
      ]);
      expect(out[0].field).toBe("category");
   });

   // A dashboard over a bare `import '../givens.malloy'` binds names nobody
   // handed the builder. They are still controls, and can still be unbound.
   it("shows a binding to a given nobody declared", () => {
      const d = doc();
      delete d.localGivens;
      expect(controlsOf(d).map((c) => [c.name, c.origin])).toEqual([
         ["CATEGORY", "model"],
      ]);
   });
});

describe("defaultOperator", () => {
   it("leaves a filter implicit and compares a value", () => {
      expect(defaultOperator("filter<string>")).toBeUndefined();
      expect(defaultOperator("filter<number>")).toBeUndefined();
      expect(defaultOperator("date")).toBe(">=");
      expect(defaultOperator("number")).toBe(">=");
      expect(defaultOperator("string")).toBe("=");
      expect(defaultOperator(undefined)).toBeUndefined();
   });
});

describe("givenNameFor", () => {
   it("names a given from a field and keeps it distinct", () => {
      expect(givenNameFor("products.category", [])).toBe("CATEGORY");
      expect(givenNameFor("created_at", [])).toBe("CREATED_AT");
      expect(givenNameFor("category", ["CATEGORY"])).toBe("CATEGORY_2");
      expect(givenNameFor("2nd-tier", [])).toBe("F_2ND_TIER");
   });
});

describe("newLocalGiven", () => {
   it("declares a picker that suggests over the field's own source", () => {
      expect(
         newLocalGiven({
            name: "REGION",
            label: "Region",
            kind: "select",
            field: "regions.region",
            source: "order_items",
         }),
      ).toEqual({
         name: "REGION",
         type: "filter<string>",
         default: "f''",
         label: "Region",
         control: "select",
         suggest: { source: "order_items", dimension: "region" },
      });
   });

   it("declares a date with the default it was given", () => {
      expect(
         newLocalGiven({
            name: "SINCE",
            label: "",
            kind: "date",
            field: "created_at",
            dateDefault: "2023-01-01",
         }),
      ).toEqual({
         name: "SINCE",
         type: "date",
         default: "@2023-01-01",
         label: "SINCE",
      });
   });
});

describe("mappings", () => {
   it("opens on what is true, with the comparison the type implies", () => {
      const rows = mappingOf(doc(), { name: "CATEGORY", field: "category" });
      expect(rows.map((r) => r.include)).toEqual([true, false, false]);
      const since = mappingOf(doc(), {
         name: "SINCE",
         field: "created_at",
         type: "date",
      });
      expect(since[1]).toEqual({
         include: false,
         field: "created_at",
         op: ">=",
      });
   });

   it("applies a mapping as a diff and skips tiles that cannot bind", () => {
      const d = doc();
      applyMapping(d, "SINCE", [
         { include: true, field: "created_at", op: ">=" },
         { include: true, field: "created_at", op: ">=" },
         // Inline: cannot take a refinement, so ticking it does nothing.
         { include: true, field: "created_at", op: ">=" },
      ]);
      expect(d.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
         { field: "created_at", given: "SINCE", op: ">=" },
      ]);
      expect(d.tiles[1].filters).toEqual([
         { field: "created_at", given: "SINCE", op: ">=" },
      ]);
      expect(d.tiles[2].filters).toBeUndefined();

      // Unticking removes, and a `~` is left implicit.
      applyMapping(d, "SINCE", [
         { include: false, field: "created_at" },
         { include: true, field: "created_at", op: "~" },
         { include: false, field: "created_at" },
      ]);
      expect(d.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
      expect(d.tiles[1].filters).toEqual([
         { field: "created_at", given: "SINCE" },
      ]);
   });
});

describe("declareControl and removeControl", () => {
   it("declares in place, and removes the declaration with every binding", () => {
      const d = doc();
      declareControl(d, {
         name: "CATEGORY",
         type: "filter<string>",
         default: "f''",
         label: "Cat",
      });
      expect(d.localGivens?.map((g) => g.label)).toEqual(["Cat"]);
      declareControl(d, {
         name: "SINCE",
         type: "date",
         default: "@2023-01-01",
      });
      expect(d.localGivens?.map((g) => g.name)).toEqual(["CATEGORY", "SINCE"]);

      removeControl(d, "CATEGORY");
      expect(d.tiles[0].filters).toBeUndefined();
      expect(d.localGivens?.map((g) => g.name)).toEqual(["SINCE"]);
      removeControl(d, "SINCE");
      expect(d.localGivens).toBeUndefined();
   });
});
