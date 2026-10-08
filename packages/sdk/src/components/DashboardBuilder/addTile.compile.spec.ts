// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import type { CompiledModel } from "../../client";
import { buildCatalog } from "./catalog";
import { addTileToDocument } from "./addTileToDocument";
import { spliceDashboardDocument, spliceFailed } from "./spliceDocument";
import { openDocument } from "./testing/fixtures";

/**
 * The file an add-tile writes, compiled by Malloy against the real storefront
 * package. The catalog is built the way the server lists models: every source
 * in `modelDef.contents`, imported ones included.
 */
const PACKAGE = path.resolve(
   import.meta.dir,
   "../../../../../examples/storefront",
);
const DOCUMENT = "dashboards/x.malloy";

async function loadPackage() {
   const { Runtime, isSourceDef, modelDefToModelInfo } = await import(
      "@malloydata/malloy"
   );
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const connection = new DuckDBConnection("duckdb", ":memory:", PACKAGE);
   const overlay = new Map<string, string>();
   const runtime = new Runtime({
      urlReader: {
         readURL: async (at: URL) =>
            overlay.get(at.href) ??
            fs.readFileSync(fileURLToPath(at.href), "utf8"),
      },
      connections: { lookupConnection: async () => connection },
   } as never);
   const urlOf = (file: string) => pathToFileURL(path.join(PACKAGE, file)).href;

   const models: CompiledModel[] = [];
   for (const file of ["data_app.malloy", "storefront.malloy"]) {
      const { _modelDef: def } = await runtime
         .loadModel(new URL(urlOf(file)))
         .getModel();
      const listed = Object.entries(def.contents)
         .filter(([, entry]) => isSourceDef(entry as never))
         .map(([name]) => ({
            name,
            views: [{ name: "by_category" }],
         }));
      models.push({
         modelPath: file,
         modelInfo: JSON.stringify(modelDefToModelInfo(def)),
         sources: listed,
      } as unknown as CompiledModel);
   }

   return {
      models,
      /** Compile `text` as the dashboard file; the errors, or none. */
      problems: async (text: string) => {
         overlay.set(urlOf(DOCUMENT), text);
         try {
            await runtime.loadModel(new URL(urlOf(DOCUMENT))).getModel();
            return [];
         } catch (error) {
            return [String(error)];
         }
      },
      close: () => connection.close(),
   };
}

/** What the builder writes for a new tile on `base`, through the same function the add-tile hook calls. */
async function addTile(
   text: string,
   base: string,
   catalog: ReturnType<typeof buildCatalog>,
   { textHeld = false } = {},
) {
   const source = catalog.sources.find((s) => s.name === base)!;
   const document = structuredClone(await openDocument(text));
   addTileToDocument(
      document,
      {
         base,
         modelPath: source.modelPath,
         exporters: source.exporters,
         view: "by_category",
         colspan: 6,
      },
      { modelPath: DOCUMENT, textHeld, notebook: false },
   );
   const result = await spliceDashboardDocument(text, document, {
      modelPath: DOCUMENT,
   });
   if (spliceFailed(result)) throw new Error(result.reason);
   return result.source;
}

const FILE = (imports: string) => `##! experimental.givens
## artifact { title="X" tiles=[] } dashboard { columns=12 }
${imports}
`;

describe("the file an add-tile writes compiles", () => {
   it("lists the imported source under the importer, as the server does", async () => {
      const pkg = await loadPackage();
      try {
         const dataApp = pkg.models[0];
         expect(dataApp.sources?.map((s) => s.name)).toContain("order_items");
      } finally {
         await pkg.close();
      }
   });

   it("imports a source from its exporter, not from the model listed first", async () => {
      const pkg = await loadPackage();
      try {
         const catalog = buildCatalog(pkg.models);
         const text = await addTile(
            FILE('import { products } from "../storefront.malloy"'),
            "order_items",
            catalog,
         );
         expect(await pkg.problems(text)).toEqual([]);
         expect(
            catalog.sources.find((s) => s.name === "order_items")?.modelPath,
         ).toBe("storefront.malloy");
         expect(text).toContain(
            'import { products, order_items } from "../storefront.malloy"',
         );
      } finally {
         await pkg.close();
      }
   });

   it("writes no import for a source a whole-file import already carries", async () => {
      const pkg = await loadPackage();
      try {
         const text = await addTile(
            FILE('import "../storefront.malloy"'),
            "order_items",
            buildCatalog(pkg.models),
         );
         expect(await pkg.problems(text)).toEqual([]);
         expect(text.match(/^import /gm)).toHaveLength(1);
      } finally {
         await pkg.close();
      }
   });

   it("writes no import for a text-held document, whatever the source", async () => {
      const pkg = await loadPackage();
      try {
         const text = await addTile(
            FILE('import "../storefront.malloy"'),
            "order_items",
            buildCatalog(pkg.models),
            { textHeld: true },
         );
         expect(text.match(/^import /gm)).toHaveLength(1);
      } finally {
         await pkg.close();
      }
   });

   it("accepts a tile on the source a whole-file import of its model brings", async () => {
      const pkg = await loadPackage();
      try {
         const text = await addTile(
            FILE('import "../data_app.malloy"'),
            "scoped_orders",
            buildCatalog(pkg.models),
         );
         expect(text.match(/^import /gm)).toHaveLength(1);
         expect(await pkg.problems(text)).toEqual([]);
      } finally {
         await pkg.close();
      }
   });
});
