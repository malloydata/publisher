// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, setDefaultTimeout } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { addTileToDocument } from "./addTileToDocument";
import { isQueryTile, type QueryTile } from "./document";
import { previewTileQuery, unsavedSources } from "./preview";
import { spliceDashboardDocument, spliceFailed } from "./spliceDocument";
import { openDocument } from "./testing/fixtures";

/**
 * A tile added on a source the file has no extension of goes on a NEW
 * extension, which the served model does not have until the file is saved. Its
 * preview is run here by Malloy against the served file, as the server runs it,
 * and its rows are checked against the saved file's own tile.
 */
const PACKAGE = path.resolve(
   import.meta.dir,
   "../../../../../examples/storefront",
);
const DOCUMENT = "dashboards/x.malloy";

// DuckDB from cold, as in addTile.compile.spec.ts.
setDefaultTimeout(30_000);

async function loadPackage() {
   const { Runtime } = await import("@malloydata/malloy");
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
   return {
      /** Serve `text` as the report file. */
      serve: (text: string) => overlay.set(urlOf(DOCUMENT), text),
      /** Run `query` on the served `model`: its rows, or the error. */
      rows: async (model: string, query: string) => {
         try {
            const result = await runtime
               .loadModel(new URL(urlOf(model)))
               .loadQuery(query)
               .run();
            return { rows: result.data.toObject() };
         } catch (error) {
            return { error: String(error) };
         }
      },
      close: () => connection.close(),
   };
}

const FILE = (imports: string) => `##! experimental.givens
## artifact { title="X" tiles=[] } dashboard { columns=12 }
${imports}
`;

/** The served file, the document after an add-tile on `base`, and the file that add saves. */
async function addOn(served: string, base: string) {
   const opened = await openDocument(served);
   const document = structuredClone(opened);
   addTileToDocument(
      document,
      {
         base,
         modelPath: "storefront.malloy",
         exporters: ["storefront.malloy"],
         view: "by_category",
         colspan: 6,
      },
      { modelPath: DOCUMENT, textHeld: false, widths: true },
   );
   const saved = await spliceDashboardDocument(served, document);
   if (spliceFailed(saved)) throw new Error(saved.reason);
   const tile = document.tiles.filter(isQueryTile).at(-1) as QueryTile;
   return { opened, document, tile, saved: saved.source };
}

describe("a tile on an extension the served model does not have yet", () => {
   it("follows a copy of a new tile to the view it shows, and keeps a filtering extension", () => {
      const tile = (name: string, source: string, from: string): QueryTile => ({
         name,
         source,
         declaration: { kind: "reference", from },
      });
      const document = {
         layout: "grid" as const,
         title: "T",
         imports: [],
         sources: [
            { name: "a_tiles", base: "a" },
            { name: "a_tiles_tiles", base: "a_tiles" },
            { name: "scoped", base: "a", scopedBy: ["X"] },
         ],
         tiles: [
            tile("v_tile", "a_tiles", "v"),
            tile("v_tile_tile", "a_tiles_tiles", "v_tile"),
            tile("w_tile", "scoped", "w"),
         ],
      };
      const unsaved = {
         sources: new Set(["a_tiles", "a_tiles_tiles", "scoped"]),
      };
      const run = (t: QueryTile) =>
         previewTileQuery(document, t, new Set(), new Map(), undefined, unsaved)
            .expression;
      expect(run(document.tiles[0])).toBe("a -> v");
      expect(run(document.tiles[1])).toBe("a -> v");
      // Its base would show rows its `where:` leaves out: run as before.
      expect(run(document.tiles[2])).toBe("scoped -> w");
   });

   it("fails as a reference to an undefined object when run on that extension", async () => {
      const pkg = await loadPackage();
      try {
         const served = FILE(
            'import { order_items } from "../storefront.malloy"',
         );
         const { document, tile } = await addOn(served, "order_items");
         const q = previewTileQuery(document, tile, new Set());
         pkg.serve(served);
         const run = await pkg.rows(DOCUMENT, `run: ${q.expression}`);
         expect(q.expression).toBe("order_items_tiles -> by_category");
         expect(run.error).toContain("order_items_tiles");
      } finally {
         await pkg.close();
      }
   });

   for (const [why, imports, model] of [
      [
         "a source the file imports",
         'import { order_items } from "../storefront.malloy"',
         undefined,
      ],
      [
         "a source whose import is unsaved too",
         'import { products } from "../storefront.malloy"',
         "storefront.malloy",
      ],
   ] as const)
      it(`previews what the saved page shows, on ${why}`, async () => {
         const pkg = await loadPackage();
         try {
            const served = FILE(imports);
            const { opened, document, tile, saved } = await addOn(
               served,
               "order_items",
            );
            expect(tile.source).toBe("order_items_tiles");

            const q = previewTileQuery(
               document,
               tile,
               new Set(),
               new Map(),
               undefined,
               unsavedSources(document, opened, DOCUMENT, false),
            );
            expect(q.expression).toBe("order_items -> by_category");
            expect(q.modelPath).toBe(model);
            pkg.serve(served);
            const preview = await pkg.rows(
               q.modelPath ?? DOCUMENT,
               `${q.annotation ? `${q.annotation}\n` : ""}run: ${q.expression}`,
            );
            expect(preview.error).toBeUndefined();

            pkg.serve(saved);
            const page = await pkg.rows(
               DOCUMENT,
               `run: ${tile.source} -> ${tile.name}`,
            );
            expect(page.error).toBeUndefined();
            expect(page.rows?.length).toBeGreaterThan(0);
            expect(preview.rows).toEqual(page.rows);
         } finally {
            await pkg.close();
         }
      });
});
