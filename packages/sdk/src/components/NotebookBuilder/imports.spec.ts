// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { CompiledModel } from "../../client";
import type { CatalogSource } from "../DashboardBuilder/catalog";
import { importedCatalog, mergeSources, notebookImports } from "./imports";
import {
   notebookSourceRefused,
   readNotebookSource,
} from "../DashboardBuilder/legacyNotebook";

const importsOf = async (body: string, modelPath = "notebooks/n.malloy") => {
   const read = await readNotebookSource(
      `## artifact { kind=notebook }\n${body}\n\nsource: s is duckdb.table('t')\n`,
   );
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   return notebookImports(read.source, modelPath);
};

/** A model that exports `exported` (default: every source) and lists `sources`, each with one view. */
const modelOf = (
   sources: Record<string, string>,
   exported: string[] = Object.keys(sources),
) =>
   ({
      sources: Object.entries(sources).map(([name, view]) => ({
         name,
         views: [{ name: view }],
      })),
      modelInfo: JSON.stringify({
         entries: exported.map((name) => ({ kind: "source", name })),
      }),
   }) as unknown as CompiledModel;
const model = (...sources: string[]) =>
   modelOf(Object.fromEntries(sources.map((name) => [name, "v"])));

const source = (name: string): CatalogSource => ({
   name,
   modelPath: "m.malloy",
   views: [],
   givens: [],
   fields: [],
});

describe("notebookImports", () => {
   it("reads a named import, resolved against the notebook's directory", async () => {
      expect(
         await importsOf(`import { orders, items } from "../shop.malloy"`),
      ).toEqual([
         {
            kind: "names",
            names: [
               { from: "orders", as: "orders" },
               { from: "items", as: "items" },
            ],
            path: "shop.malloy",
         },
      ]);
   });

   it("reads a whole-file import", async () => {
      expect(await importsOf(`import "../shop.malloy"`)).toEqual([
         { kind: "all", path: "shop.malloy" },
      ]);
      expect(await importsOf(`import 'sibling.malloy'`)).toEqual([
         { kind: "all", path: "notebooks/sibling.malloy" },
      ]);
   });

   it("keeps a rename as a pair: `x is orders` brings x from orders", async () => {
      expect(
         await importsOf(
            "import { x is orders, `y z` is items } from './a.malloy'",
         ),
      ).toEqual([
         {
            kind: "names",
            names: [
               { from: "orders", as: "x" },
               { from: "items", as: "y z" },
            ],
            path: "notebooks/a.malloy",
         },
      ]);
   });

   it("reads an import split over lines, with CRLF endings", async () => {
      expect(
         await importsOf(
            'import {\r\n  orders,\r\n  items\r\n} from\r\n  "../shop.malloy"',
         ),
      ).toEqual([
         {
            kind: "names",
            names: [
               { from: "orders", as: "orders" },
               { from: "items", as: "items" },
            ],
            path: "shop.malloy",
         },
      ]);
   });

   it("ignores an import that is only in a comment", async () => {
      expect(
         await importsOf(
            [
               '// import "../a.malloy"',
               '/* import { x } from "../b.malloy" */',
               '/* spans\n   import "../c.malloy"\n   lines */',
               'import "../real.malloy" // import "../d.malloy"',
               'import "../real2.malloy" -- import "../e.malloy"',
            ].join("\n"),
         ),
      ).toEqual([
         { kind: "all", path: "real.malloy" },
         { kind: "all", path: "real2.malloy" },
      ]);
   });

   it("does not treat a comment marker inside a path as a comment", async () => {
      expect(await importsOf(`import "../a--b.malloy"`)).toEqual([
         { kind: "all", path: "a--b.malloy" },
      ]);
   });

   it("decodes a percent-encoded name, and refuses an encoded traversal", async () => {
      expect(await importsOf(`import "../my%20model.malloy"`)).toEqual([
         { kind: "all", path: "my model.malloy" },
      ]);
      expect(await importsOf(`import "..%2f..%2fx.malloy"`)).toEqual([]);
      expect(await importsOf(`import "a%5c..%5cx.malloy"`)).toEqual([]);
      expect(await importsOf(`import "../%e0%a4%a.malloy"`)).toEqual([]);
   });

   it("leaves a path outside the package alone", async () => {
      expect(
         await importsOf(`import "https://elsewhere.test/x.malloy"`),
      ).toEqual([]);
   });
});

describe("importedCatalog", () => {
   const models = new Map([["shop.malloy", model("orders", "items", "other")]]);

   it("offers only the named sources of a named import, and all of a whole import", () => {
      expect(
         importedCatalog(
            [
               {
                  kind: "names",
                  names: [{ from: "orders", as: "orders" }],
                  path: "shop.malloy",
               },
            ],
            models,
         ).map((s) => s.name),
      ).toEqual(["orders"]);
      expect(
         importedCatalog([{ kind: "all", path: "shop.malloy" }], models).map(
            (s) => s.name,
         ),
      ).toEqual(["orders", "items", "other"]);
   });

   it("offers a renamed source under its new name, matched on the original", () => {
      const [renamed] = importedCatalog(
         [
            {
               kind: "names",
               names: [{ from: "orders", as: "x" }],
               path: "shop.malloy",
            },
         ],
         models,
      );
      expect(renamed.name).toBe("x");
      expect(renamed.views.map((v) => v.name)).toEqual(["v"]);
   });

   it("does not offer the file's own source of the same name when it is renamed away", () => {
      const [only, ...rest] = importedCatalog(
         [
            {
               kind: "names",
               names: [{ from: "raw_orders", as: "orders" }],
               path: "shop.malloy",
            },
         ],
         new Map([
            [
               "shop.malloy",
               modelOf({ orders: "own_view", raw_orders: "raw_view" }),
            ],
         ]),
      );
      expect(rest).toEqual([]);
      // It is raw_orders' catalog entry, renamed, not the file's `orders`.
      expect(only.name).toBe("orders");
      expect(only.views.map((v) => v.name)).toEqual(["raw_view"]);
   });

   it("offers only what a whole-file import can reach: the sources the model exports", () => {
      const middle = new Map([
         ["middle.malloy", modelOf({ s: "v", own: "v" }, ["own"])],
      ]);
      expect(
         importedCatalog([{ kind: "all", path: "middle.malloy" }], middle).map(
            (s) => s.name,
         ),
      ).toEqual(["own"]);
      // No `modelInfo` to say what is exported offers nothing.
      const bare = {
         sources: [{ name: "s", views: [] }],
      } as unknown as CompiledModel;
      expect(
         importedCatalog(
            [{ kind: "all", path: "bare.malloy" }],
            new Map([["bare.malloy", bare]]),
         ),
      ).toEqual([]);
   });

   it("brings nothing from a model that could not be read, and counts each name once", () => {
      expect(
         importedCatalog([{ kind: "all", path: "gone.malloy" }], models),
      ).toEqual([]);
      expect(
         importedCatalog(
            [
               { kind: "all", path: "shop.malloy" },
               {
                  kind: "names",
                  names: [{ from: "orders", as: "orders" }],
                  path: "shop.malloy",
               },
            ],
            models,
         ).map((s) => s.name),
      ).toEqual(["orders", "items", "other"]);
   });
});

describe("mergeSources", () => {
   it("keeps the notebook's own sources first and adds only imports it does not list", () => {
      expect(
         mergeSources(
            [source("a"), source("b")],
            [source("b"), source("c")],
         ).map((s) => s.name),
      ).toEqual(["a", "b", "c"]);
      expect(mergeSources([source("a")], undefined).map((s) => s.name)).toEqual(
         ["a"],
      );
   });
});
