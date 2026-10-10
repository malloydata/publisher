// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { GivenValue } from "../../hooks/givenValue";
import type { DashboardDocument, QueryTile } from "./document";
import { queryTile } from "./testing/fixtures";
import { previewGivens, previewTileQuery, tileExpressionKey } from "./preview";

const tile = (
   name: string,
   from: string,
   filters?: QueryTile["filters"],
): QueryTile => ({
   name,
   source: "overview",
   declaration: { kind: "reference", from },
   ...(filters ? { filters } : {}),
});

const document: DashboardDocument = {
   title: "Overview",
   imports: [],
   sources: [{ name: "overview", base: "order_items" }],
   localGivens: [
      {
         name: "CATEGORY",
         type: "filter<string>",
         default: "f''",
         label: "Category",
         control: "select",
         suggest: { source: "products", dimension: "category" },
      },
      { name: "SINCE", type: "date", default: "@2023-01-01", label: "Since" },
      { name: "UNBOUND", type: "filter<string>", default: "f''" },
   ],
   tiles: [
      tile("kpis", "key_figures", [
         { field: "category", given: "CATEGORY" },
         { field: "created_at", given: "SINCE", op: ">=" },
      ]),
      tile("trend", "sales_by_month", [
         { field: "category", given: "CATEGORY" },
      ]),
      tile("plain", "by_state"),
   ],
};

describe("previewGivens", () => {
   it("shows the givens some tile binds, this file's first, in declaration order", () => {
      const specs = previewGivens(document, [
         { name: "REGION", type: "filter<string>" },
         { name: "CATEGORY", type: "filter<string>", label: "From the model" },
      ]);
      // UNBOUND is declared and bound by nothing, so it is not a control; the
      // model's REGION is bound by nothing either. The model's CATEGORY shares
      // a name with the local one, and the local declaration is the control.
      expect(specs.map((s) => s.name)).toEqual(["CATEGORY", "SINCE"]);
      expect(specs[0].label).toBe("Category");
      expect(specs[0].control).toBe("select");
      expect(specs[0].suggest).toEqual({
         source: "products",
         dimension: "category",
      });
   });

   it("shows a filter default as the server publishes it: unwrapped, and none when empty", () => {
      const filtered: DashboardDocument = {
         ...document,
         localGivens: [
            { name: "EMPTY", type: "filter<string>", default: "f''" },
            { name: "STATE", type: "filter<string>", default: "f'WN'" },
         ],
         tiles: [
            tile("t", "v", [
               { field: "a", given: "EMPTY" },
               { field: "b", given: "STATE" },
            ]),
         ],
      };
      const [empty, state] = previewGivens(filtered, []);
      expect(empty.default).toBeUndefined();
      expect(state.default).toBe("WN");
      // The document keeps the literal the file is written from.
      expect(filtered.localGivens?.[1].default).toBe("f'WN'");
   });

   it("uses the model's own spec for a model given a tile binds", () => {
      const withModel: DashboardDocument = {
         ...document,
         localGivens: [],
         tiles: [tile("t", "v", [{ field: "brand", given: "BRAND" }])],
      };
      const spec = {
         name: "BRAND",
         type: "filter<string>",
         suggest: {
            query: "brand_suggest",
            dimension: "brand",
            givenNames: ["REGION"],
         },
      };
      expect(previewGivens(withModel, [spec])).toEqual([spec]);
   });
});

describe("previewTileQuery", () => {
   const runnable = new Set(["CATEGORY", "SINCE"]);

   it("runs the view on the source the extension extends, with the document's bindings", () => {
      const q = previewTileQuery(document, queryTile(document, 0), runnable);
      expect(q.expression).toBe(
         "overview -> key_figures + { where: category ~ $CATEGORY, where: created_at >= $SINCE }",
      );
      expect(q.givenNames).toEqual(["CATEGORY", "SINCE"]);
   });

   it("carries the wrapper's chart line above the run, and nothing for the view's own", () => {
      const base = queryTile(document, 2);
      const withChart = (chart: QueryTile["chart"]) =>
         previewTileQuery(
            document,
            {
               ...base,
               chart,
               chartLines: ["# bar_chart { size=spark }"],
            } as QueryTile,
            runnable,
         ).annotation;
      expect(withChart("bar_chart")).toBe(
         "# -line_chart -big_value -scatter_chart -shape_map -segment_map -viz bar_chart",
      );
      expect(withChart("none")).toBe(
         "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz",
      );
      expect(withChart("custom")).toBe("# bar_chart { size=spark }");
      expect(withChart("default")).toBeUndefined();
      expect(withChart(undefined)).toBeUndefined();
   });

   it("clears the saved view's own chart line for an inline or opaque tile on Default", () => {
      const NONE =
         "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz";
      const annotationOf = (
         declaration: QueryTile["declaration"],
         chart: QueryTile["chart"],
      ) =>
         previewTileQuery(
            document,
            { name: "x", source: "overview", declaration, chart },
            runnable,
         ).annotation;
      expect(annotationOf({ kind: "inline" }, "default")).toBe(NONE);
      for (const why of [
         "a `{ … } + { … }` compound refinement",
         "a `->` pipeline from a named view",
      ])
         expect(annotationOf({ kind: "opaque", why }, "default")).toBe(NONE);
      // These start from a named view, whose own chart the saved dashboard shows.
      for (const why of [
         "a chained refinement",
         "a parenthesized expression",
         "an unreadable refinement",
      ])
         expect(
            annotationOf({ kind: "opaque", why }, "default"),
         ).toBeUndefined();
      expect(annotationOf({ kind: "inline" }, undefined)).toBeUndefined();
      expect(
         annotationOf({ kind: "reference", from: "v" }, "default"),
      ).toBeUndefined();
   });

   it("sends a tile only the givens it binds", () => {
      // `trend` binds CATEGORY alone: SINCE moving must not re-run it, and
      // unbinding a tile is what takes a control's effect off it.
      const q = previewTileQuery(document, queryTile(document, 1), runnable);
      expect(q.givenNames).toEqual(["CATEGORY"]);
      const bare = previewTileQuery(document, queryTile(document, 2), runnable);
      expect(bare.expression).toBe("overview -> by_state");
      expect(bare.givenNames).toEqual([]);
   });

   it("writes the value of a given the server does not know yet as a literal", () => {
      // SINCE is declared here and not compiled on the server: it cannot be
      // sent by name, so its value goes into the query, and a new filter works
      // before the file is saved. Only CATEGORY travels with the request.
      const q = previewTileQuery(
         document,
         queryTile(document, 0),
         new Set(["CATEGORY"]),
         new Map<string, GivenValue>([
            ["CATEGORY", "Shoes"],
            ["SINCE", new Date("2024-01-31T00:00:00Z")],
         ]),
      );
      expect(q.expression).toBe(
         "overview -> key_figures + { where: category ~ $CATEGORY, where: created_at >= @2024-01-31 }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("leaves out an unsent given with no value yet, and one nobody declares", () => {
      const q = previewTileQuery(document, queryTile(document, 0), new Set());
      expect(q.expression).toBe("overview -> key_figures");
      expect(q.givenNames).toEqual([]);
      const stray: QueryTile = tile("t", "v", [
         { field: "x", given: "NOBODY" },
      ]);
      expect(
         previewTileQuery(document, stray, new Set(), new Map([["NOBODY", 1]]))
            .expression,
      ).toBe("overview -> v");
   });

   it("runs an inline tile the same as a reference, on its own name", () => {
      // An inline body still names a view of the extension — `view: x is {
      // … }` is as much a named view as `view: x is base_view` — so a
      // binding takes the same `source -> x + { where: … }` path a reference
      // tile does, rather than being sent unrefined.
      const inline: QueryTile = {
         name: "kpis",
         source: "overview",
         declaration: { kind: "inline" },
         filters: [{ field: "category", given: "CATEGORY" }],
      };
      const q = previewTileQuery(document, inline, runnable);
      expect(q.expression).toBe(
         "overview -> kpis + { where: category ~ $CATEGORY }",
      );
      expect(q.givenNames).toEqual(["CATEGORY"]);
   });

   it("runs an inherited tile as the model has it, sending the whole row", () => {
      const inherited: QueryTile = {
         name: "by_brand",
         source: "orders",
         declaration: { kind: "inherited" },
      };
      expect(previewTileQuery(document, inherited, runnable)).toEqual({
         expression: "orders -> by_brand",
         givenNames: undefined,
         reads: undefined,
      });
   });
});

describe("previewTileQuery reads", () => {
   const runnable = new Set(["CATEGORY", "REGION"]);

   it("counts a literal-bound local given the server cannot be sent", () => {
      const q = previewTileQuery(document, queryTile(document, 0), runnable);
      expect(q.givenNames).toEqual(["CATEGORY"]);
      expect(q.reads).toEqual(["CATEGORY", "SINCE"]);
   });

   it("counts, and sends, what the extension's own where: reads", () => {
      const scoped: DashboardDocument = {
         ...document,
         sources: [
            { name: "overview", base: "order_items", scopedBy: ["REGION"] },
         ],
      };
      const q = previewTileQuery(scoped, queryTile(scoped, 2), runnable);
      expect(q.expression).toBe("overview -> by_state");
      expect(q.givenNames).toEqual(["REGION"]);
      expect(q.reads).toEqual(["REGION"]);
   });

   it("adds what the served tile reads, which the document cannot see, and sends the runnable part", () => {
      // A model source's own `where: region ~ $REGION`: the reader moves the
      // tile with REGION, so the builder must send it too.
      const q = previewTileQuery(
         document,
         queryTile(document, 2),
         runnable,
         new Map(),
         ["REGION", "BRAND"],
      );
      expect(q.reads).toEqual(["REGION", "BRAND"]);
      expect(q.givenNames).toEqual(["REGION"]);
   });

   it("says nothing for a tile the served file lacks and the document scopes by nothing", () => {
      // A tile not saved yet over a model-scoped source: no served entry, so
      // an empty reads would chip every control the model reads.
      expect(
         previewTileQuery(document, queryTile(document, 2), runnable).reads,
      ).toBeUndefined();
      expect(
         previewTileQuery(
            document,
            queryTile(document, 2),
            runnable,
            new Map(),
            [],
         ).reads,
      ).toEqual([]);
   });

   it("takes an inherited tile's reads from the served file alone", () => {
      const inherited: QueryTile = {
         name: "by_brand",
         source: "orders",
         declaration: { kind: "inherited" },
      };
      expect(
         previewTileQuery(document, inherited, runnable, new Map(), ["REGION"])
            .reads,
      ).toEqual(["REGION"]);
      expect(
         previewTileQuery(document, inherited, runnable).reads,
      ).toBeUndefined();
   });
});

describe("tileExpressionKey", () => {
   it("keys a tile expression as the server does", () => {
      expect(tileExpressionKey("orders->by_x")).toBe("orders -> by_x");
      expect(tileExpressionKey("  orders  ->   by_x ")).toBe("orders -> by_x");
   });
});

describe("previewTileQuery: a filter on a reserved field name", () => {
   it("back-quotes the field in the preview's where clause", () => {
      const q = previewTileQuery(
         document,
         tile("t", "v", [{ field: "date", given: "CATEGORY" }]),
         new Set(["CATEGORY"]),
      );
      expect(q.expression).toBe(
         "overview -> v + { where: `date` ~ $CATEGORY }",
      );
   });
});
