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
} from "./catalog";

/** A `modelInfo` exporting these sources, and a named query beside them. */
const infoOf = (...sources: string[]) =>
   JSON.stringify({
      entries: [
         ...sources.map((name) => ({ kind: "source", name })),
         { kind: "query", name: "a_named_query" },
      ],
   });

/**
 * Shaped like the real response, including the two details that are easy to get
 * wrong from the type alone: annotations arrive RAW with their trailing newline,
 * and `sourceInfos` is an array of JSON STRINGS rather than objects.
 */
const MODEL: CompiledModel = {
   modelPath: "data_app.malloy",
   modelInfo: infoOf("scoped_orders"),
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
      expect(chartOf(["# -bar_chart\n"])).toBeUndefined();
      expect(chartOf(["# -bar_chart -viz line_chart\n"])).toBe("line_chart");
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

   // `sources` also lists names a model only imports; an import of one of
   // those from the importer fails to compile.
   it("credits a source to the model that exports it, not an importer listed first", () => {
      const importer = {
         ...MODEL,
         modelPath: "data_app.malloy",
         modelInfo: infoOf("scoped_orders"),
         sources: [
            ...MODEL.sources!,
            { name: "order_items", views: [{ name: "by_category" }] },
         ],
      } as CompiledModel;
      const exporter = {
         modelPath: "storefront.malloy",
         modelInfo: infoOf("order_items"),
         sources: [{ name: "order_items", views: [{ name: "by_category" }] }],
      } as CompiledModel;
      const catalog = buildCatalog([importer, exporter]);
      expect(catalog.sources.map((s) => [s.name, s.modelPath]).sort()).toEqual([
         ["order_items", "storefront.malloy"],
         ["scoped_orders", "data_app.malloy"],
      ]);
   });

   it("records every model that lists a source, imported or not, as where it is visible", () => {
      const catalog = buildCatalog([
         {
            ...MODEL,
            modelInfo: infoOf(),
            modelPath: "importer.malloy",
         } as CompiledModel,
         MODEL,
      ]);
      expect(catalog.sources[0].exporters).toEqual(["data_app.malloy"]);
      expect(catalog.sources[0].visibleIn).toEqual([
         "importer.malloy",
         "data_app.malloy",
      ]);
   });

   it("lists every model that exports a source, the first as its modelPath", () => {
      const catalog = buildCatalog([
         MODEL,
         { ...MODEL, modelPath: "storefront.malloy" } as CompiledModel,
      ]);
      expect(catalog.sources).toHaveLength(1);
      expect(catalog.sources[0].modelPath).toBe("data_app.malloy");
      expect(catalog.sources[0].exporters).toEqual([
         "data_app.malloy",
         "storefront.malloy",
      ]);
   });

   // Falling back to the first model that lists a source would bring the
   // unreachable import back.
   it("offers nothing from a model with no modelInfo, or one that does not parse", () => {
      expect(
         buildCatalog([{ ...MODEL, modelInfo: undefined } as CompiledModel])
            .sources,
      ).toEqual([]);
      expect(
         buildCatalog([{ ...MODEL, modelInfo: "{not json" } as CompiledModel])
            .sources,
      ).toEqual([]);
   });

   it("does not make a source of a named query", () => {
      const catalog = buildCatalog([
         {
            ...MODEL,
            sources: [...MODEL.sources!, { name: "a_named_query" }],
         } as CompiledModel,
      ]);
      expect(catalog.sources.map((s) => s.name)).toEqual(["scoped_orders"]);
   });
});

describe("filterableFields", () => {
   // Captured from the storefront package's `order_items`: a join's fields are
   // what people filter on, and a view is not a field at all.
   const model: CompiledModel = {
      modelPath: "storefront.malloy",
      modelInfo: infoOf("order_items"),
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
