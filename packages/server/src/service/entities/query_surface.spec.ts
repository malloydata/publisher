// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";
import type { Model } from "@malloydata/malloy";
import {
   buildPackageQuerySurface,
   type PackageManifest,
   nonQueryableSources,
   servedFrom,
   servedReports,
   type SurfaceModelFile,
} from "./query_surface";

/**
 * A plain (non-dashboard) file declaring `declared` and exporting `exported`.
 * Only the fields the surface reads: explores, exportedExplores, and each
 * declaration's location.
 */
const file = (
   relativePath: string,
   declared: string[],
   exported: string[],
): SurfaceModelFile => ({
   relativePath,
   text: "",
   model: {
      explores: declared.map((name) => ({ name })),
      exportedExplores: exported.map((name) => ({ name })),
      _modelDef: {
         contents: Object.fromEntries(
            declared.map((name, line) => [
               name,
               {
                  location: {
                     url: `file:///pkg/${relativePath}`,
                     range: { start: { line, character: 0 } },
                  },
               },
            ]),
         ),
      },
   } as unknown as Model,
});

const notQueryable = (
   manifest: PackageManifest | undefined,
   target: SurfaceModelFile,
   others: SurfaceModelFile[] = [],
) =>
   [
      ...nonQueryableSources(
         target,
         buildPackageQuerySurface(manifest, [target, ...others]),
      ),
   ].sort();

describe("the query surface outside a compile", () => {
   const orders = file(
      "orders.malloy",
      ["orders", "orders_staging"],
      ["orders"],
   );

   test.each([
      ["no manifest", undefined],
      ["no explores", { queryableSources: "declared" }],
      ["empty explores", { explores: [] }],
      [
         "queryableSources all",
         { explores: ["index.malloy"], queryableSources: "all" },
      ],
   ] as [string, PackageManifest | undefined][])(
      "%s: every source is queryable, listed or not",
      (_label, manifest) => {
         assert.deepStrictEqual(notQueryable(manifest, orders), []);
      },
   );

   test.each([
      ["./orders.malloy", "orders.malloy"],
      ["orders.malloy", "/orders.malloy"],
   ])("explores %s matches a file at %s", (listed, relativePath) => {
      const target = file(
         relativePath,
         ["orders", "orders_staging"],
         ["orders"],
      );
      assert.deepStrictEqual(notQueryable({ explores: [listed] }, target), [
         "orders_staging",
      ]);
   });

   test("a file explores does not list refuses every source", () => {
      assert.deepStrictEqual(
         notQueryable({ explores: ["products.malloy"] }, orders),
         ["orders", "orders_staging"],
      );
   });
});

/** A file whose only model-level note is a bare `## artifact`. */
// A linear report's tag: since Publisher 0.9.11 a tag must list its tiles to make a report.
const LINEAR_TAG =
   '## artifact { layout=linear tiles=["orders -> by_status"] }\n';

const taggedDocument = (relativePath: string): SurfaceModelFile => ({
   relativePath,
   text: LINEAR_TAG,
   model: {
      explores: [],
      exportedExplores: [],
      _modelDef: {
         modelID: "file:///pkg/doc.malloy",
         modelAnnotations: {
            "file:///pkg/doc.malloy": {
               inheritsFrom: [],
               ownNotes: { notes: [{ text: LINEAR_TAG }] },
            },
         },
         contents: {},
      },
   } as unknown as Model,
});

// A legacy archive lists its files as `./notebooks/x.malloy`. The served
// report is keyed by the normalized path either way, so `explores` and the
// report set compare equal.
test.each(["notebooks/review.malloy", "./notebooks/review.malloy"])(
   "a linear artifact tag at %s is a served linear report",
   (relativePath) => {
      assert.deepStrictEqual(
         [...servedReports([taggedDocument(relativePath)])],
         [["notebooks/review.malloy", "linear"]],
      );
   },
);

// A tag that lists no tiles makes no report (Publisher 0.9.11's tileless rule), so a
// bare `## artifact` file is a plain model file.
test("a bare artifact tag, listing no tiles, is no report", () => {
   const bare = taggedDocument("notebooks/review.malloy");
   const text = "## artifact\n";
   const doc = {
      ...bare,
      text,
      model: {
         ...bare.model,
         _modelDef: {
            ...(bare.model as unknown as { _modelDef: object })._modelDef,
            modelAnnotations: {
               "file:///pkg/doc.malloy": {
                  inheritsFrom: [],
                  ownNotes: { notes: [{ text }] },
               },
            },
         },
      } as unknown as Model,
   };
   assert.deepStrictEqual([...servedReports([doc])], []);
});

// `public/` and the root `index.malloy` are never reports, in any spelling
// `isReportEligiblePath` (../report.ts) accepts.
test.each([
   "public/review.malloy",
   "./public/review.malloy",
   "Public/review.malloy",
   "index.malloy",
   "./Index.malloy",
])("an artifact tag at %s is no report", (relativePath) => {
   assert.deepStrictEqual(
      [...servedReports([taggedDocument(relativePath)])],
      [],
   );
});

/**
 * A linear report at `reports/overview.malloy` that imports `orders` from the
 * file at `ordersUrl`: its `orders` is that file's declaration.
 */
const reportImportingOrders = (ordersUrl: string): SurfaceModelFile => ({
   relativePath: "reports/overview.malloy",
   text: LINEAR_TAG,
   model: {
      explores: [{ name: "orders" }],
      exportedExplores: [],
      _modelDef: {
         modelID: "file:///pkg/reports/overview.malloy",
         modelAnnotations: {
            "file:///pkg/reports/overview.malloy": {
               inheritsFrom: [],
               ownNotes: { notes: [{ text: LINEAR_TAG }] },
            },
         },
         contents: {
            orders: {
               location: {
                  url: ordersUrl,
                  range: { start: { line: 0, character: 0 } },
               },
            },
         },
      },
   } as unknown as Model,
});

describe("a report's import of a served source", () => {
   test("names the serving file as the package lists it", () => {
      const orders = file("./orders.malloy", ["orders"], ["orders"]);
      const report = reportImportingOrders("file:///pkg/./orders.malloy");
      const surface = buildPackageQuerySurface(
         { explores: ["orders.malloy"] },
         [orders, report],
      );
      assert.deepStrictEqual(
         [...servedFrom(report, surface)],
         [
            [
               "orders",
               { modelFilePath: "./orders.malloy", sourceName: "orders" },
            ],
         ],
      );
   });

   test("is not served by a listed file whose report check throws", () => {
      const orders = file("orders.malloy", ["orders"], ["orders"]);
      // Reading the file's model-level notes throws, as the tag parser can.
      // The file is then ERRORed and contributes no rows.
      Object.defineProperty(orders.model._modelDef, "modelAnnotations", {
         get() {
            throw new Error("tag parser exploded");
         },
      });
      const report = reportImportingOrders("file:///pkg/orders.malloy");
      const failed: string[] = [];
      const surface = buildPackageQuerySurface(
         { explores: ["orders.malloy"] },
         [orders, report],
         (f) => failed.push(f.relativePath),
      );
      assert.deepStrictEqual(failed, ["orders.malloy"]);
      assert.ok(!surface.inert);
      assert.deepStrictEqual([...surface.exportedDeclarations], []);
      assert.deepStrictEqual([...servedFrom(report, surface)], []);
      // Nothing admits the report's `orders` any more, so Publisher refuses it.
      assert.deepStrictEqual(
         [...nonQueryableSources(report, surface)],
         ["orders"],
      );
   });
});
