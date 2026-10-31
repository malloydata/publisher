// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   pickDefaultSource,
   type ExplorerField,
   type ExplorerSource,
} from "./schemaModel";
import {
   exportOrder,
   inExportOrder,
   orderPickerSources,
   pickerDescription,
   sourceMetaLine,
} from "./sourcePicker";

const field = (name: string, kind: ExplorerField["kind"]): ExplorerField => ({
   name,
   kind,
});

/** A source with `views` views (named per source, so none looks like another's extension). */
function src(
   name: string,
   views: number,
   extra: {
      dimensions?: number;
      measures?: number;
      joins?: number;
      doc?: string;
      viewNames?: string[];
   } = {},
): ExplorerSource {
   const viewNames =
      extra.viewNames ??
      Array.from({ length: views }, (_, i) => `${name}_v${i}`);
   return {
      name,
      doc: extra.doc,
      accent: "steel",
      calculations: [],
      dimensions: Array.from({ length: extra.dimensions ?? 0 }, (_, i) =>
         field(`d${i}`, "dimension"),
      ),
      measures: Array.from({ length: extra.measures ?? 0 }, (_, i) =>
         field(`m${i}`, "measure"),
      ),
      views: viewNames.map((n) => field(n, "view")),
      joins: Array.from({ length: extra.joins ?? 0 }, (_, i) => ({
         name: `j${i}`,
         relationship: "one" as never,
         dimensions: [],
         measures: [],
      })),
   };
}

const names = (sources: ExplorerSource[]) => sources.map((s) => s.name);

describe("orderPickerSources", () => {
   it("puts the source the model opens on first, then the rest by view count, most first", () => {
      // flights.malloy declares its lookups first and its subject last.
      const sources = [
         src("carriers", 0),
         src("airports", 1),
         src("aircraft", 0),
         src("flights", 6),
         src("routes", 2),
      ];
      const order = orderPickerSources(
         sources,
         pickDefaultSource(sources, "flights.malloy"),
      );
      expect(names(order.sources)).toEqual([
         "flights",
         "routes",
         "airports",
         "carriers",
         "aircraft",
      ]);
      expect(order.curated).toBe(false);
      expect(order.exportOrdered).toBe(false);
      expect(order.mostViews).toBe("flights");
   });

   it("keeps declaration order among equal view counts", () => {
      const sources = [src("b", 1), src("a", 1), src("top", 3)];
      const order = orderPickerSources(
         sources,
         pickDefaultSource(sources, "m.malloy"),
      );
      expect(names(order.sources)).toEqual(["top", "b", "a"]);
   });

   it("keeps the root index.malloy's compiled order when its text is unknown, and claims no order or Most views", () => {
      const sources = [src("users", 0), src("orders", 9), src("products", 2)];
      const order = orderPickerSources(
         sources,
         pickDefaultSource(sources, "index.malloy"),
         { exportOrder: [] },
      );
      expect(names(order.sources)).toEqual(["users", "orders", "products"]);
      expect(order.curated).toBe(true);
      expect(order.exportOrdered).toBe(false);
      expect(order.mostViews).toBeUndefined();
   });

   it("lists the root index.malloy's sources in the order its export statement names them", () => {
      // The compiled model lists them where ecommerce.malloy declares them; the author's
      // `export { … }` is the order they chose.
      const sources = [
         src("users", 0),
         src("product", 0),
         src("order_items", 16),
      ];
      const text =
         'import {order_items, users, product} from "ecommerce.malloy"\n\nexport { order_items, users, product }\n';
      const order = orderPickerSources(
         sources,
         pickDefaultSource(sources, "index.malloy"),
         {
            exportOrder: exportOrder(text),
         },
      );
      expect(names(order.sources)).toEqual(["order_items", "users", "product"]);
      expect(order.exportOrdered).toBe(true);
   });

   it("claims no export order when the statement names none of the listed sources", () => {
      const sources = [src("users", 0), src("orders", 1)];
      const order = orderPickerSources(sources, null, {
         exportOrder: ["gone"],
      });
      expect(order.exportOrdered).toBe(false);
   });

   it("orders a model off the surface as an ordinary one: no curated set, no export order", () => {
      // The caller passes no `publicSurface` for an index.malloy in a folder, or for the
      // root one when the server marks it hidden.
      const sources = [src("users", 0), src("orders", 9)];
      const order = orderPickerSources(
         sources,
         pickDefaultSource(sources, "sub/index.malloy"),
      );
      expect(names(order.sources)).toEqual(["orders", "users"]);
      expect(order.curated).toBe(false);
      expect(order.exportOrdered).toBe(false);
   });

   it("never claims Most views for a default that is set aside for fewer views (a base its extension outranks)", () => {
      // purchase_events extends events, so it carries events' view plus its own; the model
      // opens on the base, which does not have the most views.
      const events = src("events", 1, { viewNames: ["by_day"] });
      const purchases = src("purchase_events", 3, {
         viewNames: ["by_day", "p1", "p2"],
      });
      const sources = [events, purchases];
      const def = pickDefaultSource(sources, "ga4.malloy");
      expect(def?.name).toBe("events");
      const order = orderPickerSources(sources, def);
      expect(names(order.sources)).toEqual(["events", "purchase_events"]);
      expect(order.mostViews).toBeUndefined();
   });

   it("claims Most views on a tie (the default has as many as any), never on zero", () => {
      const tied = [src("a", 2), src("b", 2)];
      expect(
         orderPickerSources(tied, pickDefaultSource(tied, "b.malloy"))
            .mostViews,
      ).toBe("b");
      const none = [src("a", 0), src("b", 0)];
      expect(
         orderPickerSources(none, pickDefaultSource(none, "m.malloy"))
            .mostViews,
      ).toBeUndefined();
   });
});

describe("pickerDescription", () => {
   it("prefers the author's #(doc)", () => {
      expect(
         pickerDescription("Flights, one row each.", "Generated text"),
      ).toEqual({
         text: "Flights, one row each.",
         generated: false,
      });
   });

   it("falls back to the index's summary, marked generated", () => {
      expect(
         pickerDescription(undefined, "Airline flights with delays"),
      ).toEqual({
         text: "Airline flights with delays",
         generated: true,
      });
      expect(pickerDescription("   ", "Summary")).toEqual({
         text: "Summary",
         generated: true,
      });
   });

   it("is nothing when there is neither", () => {
      expect(pickerDescription(undefined, undefined)).toBeUndefined();
      expect(pickerDescription("", " ")).toBeUndefined();
   });
});

describe("sourceMetaLine", () => {
   it("counts views, the header's fields and joins", () => {
      expect(
         sourceMetaLine(
            src("flights", 9, { dimensions: 20, measures: 8, joins: 4 }),
         ),
      ).toBe("9 views · 37 fields · 4 joins");
   });

   it("leaves joins out when there are none, and speaks in the singular for one", () => {
      expect(
         sourceMetaLine(src("carriers", 0, { dimensions: 3, measures: 1 })),
      ).toBe("0 views · 4 fields");
      expect(sourceMetaLine(src("one", 1, { joins: 1 }))).toBe(
         "1 view · 1 field · 1 join",
      );
   });
});

describe("exportOrder", () => {
   it("reads every export statement in order, once per name, across lines", () => {
      const text = [
         "##! experimental.access_modifiers",
         'import "a.malloy"',
         "// export { commented_out }",
         "export {",
         "  orders,   -- the fact table",
         "  users",
         "}",
         "export { products, orders }",
      ].join("\n");
      expect(exportOrder(text)).toEqual(["orders", "users", "products"]);
   });

   it("skips block comments, as well as line ones", () => {
      expect(
         exportOrder("/* export { old } */ export { a, /* b, */ c }"),
      ).toEqual(["a", "c"]);
      expect(exportOrder("/*\n export { gone }\n*/\nexport { kept }")).toEqual([
         "kept",
      ]);
   });

   it("matches the keyword without case, as Malloy does", () => {
      expect(exportOrder("EXPORT { a, b }\nExport { c }")).toEqual([
         "a",
         "b",
         "c",
      ]);
   });

   it("reads a backquoted name without its backquotes", () => {
      expect(exportOrder("export { `order items`, users }")).toEqual([
         "order items",
         "users",
      ]);
   });

   it("is empty with no text or no export statement", () => {
      expect(exportOrder(undefined)).toEqual([]);
      expect(exportOrder("source: a is duckdb.table('a.parquet')")).toEqual([]);
   });

   it("leaves anything the statement does not name after the rest, in its own order", () => {
      expect(
         inExportOrder(
            ["a", "b", "c", "d"],
            (n) => n,
            exportOrder("export { c, a }"),
         ),
      ).toEqual(["c", "a", "b", "d"]);
      expect(inExportOrder(["b", "a"], (n) => n, [])).toEqual(["b", "a"]);
   });
});
