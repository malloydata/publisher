// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   acceptsField,
   kindForFieldType,
   typeLabel,
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

describe("acceptsField", () => {
   it("matches the given's scalar to the field's type, filter or value alike", () => {
      expect(acceptsField("filter<string>", "string_type")).toBe(true);
      expect(acceptsField("string", "string_type")).toBe(true);
      expect(acceptsField("filter<string>", "number_type")).toBe(false);
      expect(acceptsField("filter<number>", "number_type")).toBe(true);
      expect(acceptsField("number", "string_type")).toBe(false);
   });

   it("lets dates and timestamps compare with each other, and nothing else", () => {
      expect(acceptsField("date", "timestamp_type")).toBe(true);
      expect(acceptsField("filter<timestamp>", "date_type")).toBe(true);
      expect(acceptsField("date", "string_type")).toBe(false);
      expect(acceptsField("filter<string>", "date_type")).toBe(false);
   });

   it("accepts what it cannot judge", () => {
      // A catalog with no type, or a given of a type this does not know.
      expect(acceptsField("filter<string>", undefined)).toBe(true);
      expect(acceptsField(undefined, "number_type")).toBe(true);
   });
});

describe("kindForFieldType and typeLabel", () => {
   it("gives a field the control its type wants", () => {
      expect(kindForFieldType("number_type")).toBe("number");
      expect(kindForFieldType("date_type")).toBe("date");
      expect(kindForFieldType("timestamp_type")).toBe("date");
      expect(kindForFieldType("string_type")).toBe("select");
      expect(kindForFieldType(undefined)).toBe("select");
   });

   it("says a type the way a message would", () => {
      expect(typeLabel("number_type")).toBe("a number");
      expect(typeLabel("string_type")).toBe("text");
   });
});
