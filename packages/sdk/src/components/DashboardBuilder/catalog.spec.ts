// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { CompiledModel } from "../../client";
import {
   buildCatalog,
   chartOf,
   docOf,
   filterableFields,
   isDashboardModel,
   missingTiles,
} from "./catalog";

/**
 * Shaped like the real response, including the two details that are easy to get
 * wrong from the type alone: annotations arrive RAW with their trailing newline,
 * and `sourceInfos` is an array of JSON STRINGS rather than objects.
 */
const MODEL: CompiledModel = {
   modelPath: "data_app.malloy",
   sources: [
      {
         name: "scoped_orders",
         annotations: ["#(doc) Order lines narrowed by the control row\n"],
         views: [
            {
               name: "by_category",
               annotations: [
                  "#(doc) Revenue by product category\n",
                  "# bar_chart\n",
               ],
            },
            { name: "top_products", annotations: ["#(doc) Top products\n"] },
         ],
         givens: [{ name: "CATEGORY" }, { name: "BRAND" }],
      },
   ],
   sourceInfos: [
      JSON.stringify({
         name: "scoped_orders",
         schema: {
            fields: [
               {
                  name: "category",
                  kind: "dimension",
                  type: { kind: "string_type" },
               },
               {
                  name: "total_sales",
                  kind: "measure",
                  type: { kind: "number_type" },
               },
               { name: "ignored", kind: "something_else" },
            ],
         },
      }),
   ],
} as CompiledModel;

describe("reading raw annotations", () => {
   it("takes the doc text without its marker or newline", () => {
      expect(docOf(["#(doc) Revenue by product category\n"])).toBe(
         "Revenue by product category",
      );
      expect(docOf(["# bar_chart\n"])).toBeUndefined();
      expect(docOf(undefined)).toBeUndefined();
   });

   // Only the renderer's own chart tags, so a `# colspan` or a `# label` is not
   // mistaken for a visualization.
   it("takes a chart tag and ignores every other tag", () => {
      expect(chartOf(["# bar_chart\n"])).toBe("bar_chart");
      expect(chartOf(["# shape_map\n"])).toBe("shape_map");
      expect(chartOf(["# colspan=6\n", '# label="x"\n'])).toBeUndefined();
   });
});

describe("buildCatalog", () => {
   it("shapes a source with its views, givens and fields", () => {
      const catalog = buildCatalog([MODEL]);
      expect(catalog.sources).toHaveLength(1);
      const source = catalog.sources[0];
      expect(source).toMatchObject({
         name: "scoped_orders",
         modelPath: "data_app.malloy",
         description: "Order lines narrowed by the control row",
         givens: ["CATEGORY", "BRAND"],
      });
      expect(source.views).toEqual([
         {
            name: "by_category",
            description: "Revenue by product category",
            chart: "bar_chart",
         },
         { name: "top_products", description: "Top products" },
      ]);
   });

   // `sourceInfos` is JSON strings, and a field whose kind is neither a
   // dimension nor a measure is not something a drill can be declared on.
   it("reads fields out of the JSON-string sourceInfos", () => {
      const [source] = buildCatalog([MODEL]).sources;
      expect(source.fields).toEqual([
         { name: "category", kind: "dimension", type: "string_type" },
         { name: "total_sales", kind: "measure", type: "number_type" },
      ]);
   });

   it("survives a malformed sourceInfos entry rather than failing", () => {
      const catalog = buildCatalog([
         { ...MODEL, sourceInfos: ["{not json"] } as CompiledModel,
      ]);
      expect(catalog.sources[0].fields).toEqual([]);
      expect(catalog.sources[0].views).toHaveLength(2);
   });

   // Offering a dashboard's own tiles as things to put on a dashboard would be
   // circular, and the endpoint lists dashboards because they are models too.
   it("leaves dashboards out", () => {
      expect(isDashboardModel("dashboards/overview.malloy")).toBe(true);
      expect(isDashboardModel("data_app.malloy")).toBe(false);
      const catalog = buildCatalog([
         { ...MODEL, modelPath: "dashboards/overview.malloy" } as CompiledModel,
      ]);
      expect(catalog.sources).toEqual([]);
   });

   // An imported source appears in every model that imports it. The first one
   // to declare it wins, so a preview runs against the file that defines it.
   it("keeps one entry for a source that several models carry", () => {
      const catalog = buildCatalog([
         MODEL,
         { ...MODEL, modelPath: "storefront.malloy" } as CompiledModel,
      ]);
      expect(catalog.sources).toHaveLength(1);
      expect(catalog.sources[0].modelPath).toBe("data_app.malloy");
   });
});

describe("missingTiles", () => {
   const catalog = buildCatalog([MODEL]);
   const declared = [{ name: "overview", base: "scoped_orders" }];
   const ref = (name: string, from: string) => ({
      source: "overview",
      name,
      declaration: { kind: "reference" as const, from },
   });

   // A tile declared in the dashboard file needs its BASE view, not its own
   // name: `overview -> kpis` is declared right there. Getting this wrong
   // reported every healthy tile in the bundled dashboard as missing.
   it("checks the base view a tile was built from, not the tile's own name", () => {
      expect(
         missingTiles(catalog, [ref("revenue_trend", "by_category")], declared),
      ).toEqual([]);
   });

   it("names a base view the model no longer has", () => {
      expect(
         missingTiles(
            catalog,
            [ref("revenue_trend", "deleted_view")],
            declared,
         ),
      ).toEqual([
         { source: "overview", name: "revenue_trend", needs: "deleted_view" },
      ]);
   });

   // An inline tile carries its own query, so there is nothing to go stale.
   it("asks nothing of an inline tile", () => {
      expect(
         missingTiles(
            catalog,
            [
               {
                  source: "overview",
                  name: "kpis",
                  declaration: { kind: "inline" },
               },
            ],
            declared,
         ),
      ).toEqual([]);
   });

   // An inherited tile is the one case where the view really does live on the
   // source, so its own name is what has to exist.
   it("checks an inherited tile's own name against its source", () => {
      expect(
         missingTiles(
            catalog,
            [
               {
                  source: "scoped_orders",
                  name: "top_products",
                  declaration: { kind: "inherited" },
               },
            ],
            [],
         ),
      ).toEqual([]);
      expect(
         missingTiles(
            catalog,
            [
               {
                  source: "scoped_orders",
                  name: "gone",
                  declaration: { kind: "inherited" },
               },
            ],
            [],
         ),
      ).toHaveLength(1);
   });

   it("names a tile whose source is gone entirely", () => {
      expect(
         missingTiles(
            catalog,
            [
               {
                  source: "ghost",
                  name: "x",
                  declaration: { kind: "inherited" },
               },
            ],
            [],
         ),
      ).toEqual([{ source: "ghost", name: "x", needs: "ghost" }]);
   });
});

describe("filterableFields", () => {
   // Captured from the storefront package's `order_items`: a join's fields are
   // what people filter on, and a view is not a field at all.
   const model: CompiledModel = {
      modelPath: "storefront.malloy",
      sources: [{ name: "order_items" }],
      sourceInfos: [
         JSON.stringify({
            name: "order_items",
            schema: {
               fields: [
                  {
                     name: "status",
                     kind: "dimension",
                     type: { kind: "string_type" },
                  },
                  {
                     name: "total_sales",
                     kind: "measure",
                     type: { kind: "number_type" },
                  },
                  {
                     name: "products",
                     kind: "join",
                     schema: {
                        fields: [
                           {
                              name: "category",
                              kind: "dimension",
                              type: { kind: "string_type" },
                           },
                           {
                              name: "supplier",
                              kind: "join",
                              schema: {
                                 fields: [
                                    { name: "region", kind: "dimension" },
                                 ],
                              },
                           },
                        ],
                     },
                  },
                  {
                     name: "by_status",
                     kind: "view",
                     schema: {
                        fields: [{ name: "status", kind: "dimension" }],
                     },
                  },
               ],
            },
         }),
      ],
   } as CompiledModel;

   it("offers dimensions, with a join's fields as paths, and no measures or views", () => {
      const fields = filterableFields(buildCatalog([model]), "order_items");
      expect(fields?.map((f) => f.name)).toEqual([
         "status",
         "products.category",
      ]);
      expect(fields?.[1]).toEqual({
         name: "products.category",
         kind: "dimension",
         type: "string_type",
      });
   });

   it("has no list for a source the catalog does not know, or no catalog", () => {
      expect(
         filterableFields(buildCatalog([model]), "elsewhere"),
      ).toBeUndefined();
      expect(filterableFields(undefined, "order_items")).toBeUndefined();
   });
});
