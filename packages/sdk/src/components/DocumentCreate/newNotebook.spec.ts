// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { lintNotebookText } from "../../../../server/src/service/notebook_lint";
import { isQueryTile, isTextTile } from "../DashboardBuilder/document";
import {
   readDashboardDocument,
   readFailed,
} from "../DashboardBuilder/readDocument";
import { documentPathFor, documentPathForTitle, slugFor } from "./documentPath";
import { newDocumentProblem } from "./guards";
import { newNotebookSource, newReportSource } from "./newNotebook";

const INPUT = {
   title: 'Sales "West"',
   modelPath: "models/storefront.malloy",
   source: "order_items",
   view: "by_category",
};

describe("newNotebookSource", () => {
   it("lints clean as a notebook", () => {
      const text = newNotebookSource(INPUT);
      expect(lintNotebookText("notebooks/sales.malloy", text)).toEqual([]);
   });

   it("lints clean for a back-quoted source and a title with a heading hash", () => {
      const text = newNotebookSource({
         ...INPUT,
         title: "# Q3 | review",
         source: "order items",
      });
      expect(text).toContain("import { `order items` }");
      expect(lintNotebookText("notebooks/q3.malloy", text)).toEqual([]);
   });

   it("reads back as a layout notebook: an intro text tile, then one query tile, and no run cell", async () => {
      const text = newNotebookSource(INPUT);
      const result = await readDashboardDocument(text);
      if (readFailed(result)) throw new Error(result.reason);
      const { document } = result;
      expect(document.kind).toBe("notebook");
      expect(
         document.tiles.map((t) => (isTextTile(t) ? "text" : "query")),
      ).toEqual(["text", "query"]);
      const query = document.tiles.find(isQueryTile);
      expect(query?.name).toBe("by_category_tile");
      expect(text).toContain(
         "source: order_items_tiles is order_items extend {",
      );
      expect(text).not.toMatch(/^run:/m);
   });

   it("writes the artifact tag as a block with a tile on each line, named for its view", () => {
      expect(newNotebookSource(INPUT)).toContain(
         '##| artifact { layout=linear title="Sales \\"West\\""\n  tiles=[\n    intro { kind=text },\n    "order_items_tiles -> by_category_tile"\n  ]\n}\n|##\n',
      );
   });

   it("reads back for a back-quoted source too", async () => {
      const result = await readDashboardDocument(
         newNotebookSource({ ...INPUT, source: "order items" }),
      );
      if (readFailed(result)) throw new Error(result.reason);
      expect(result.document.tiles.filter(isQueryTile)).toHaveLength(1);
   });

   it("imports the one source by name, relative to the package root by default", () => {
      expect(newNotebookSource(INPUT)).toContain(
         'import { order_items } from "models/storefront.malloy"',
      );
   });

   it("imports relative to where the report is written", () => {
      expect(newNotebookSource(INPUT, "team/sales.malloy")).toContain(
         'import { order_items } from "../models/storefront.malloy"',
      );
      expect(newNotebookSource(INPUT, "models/sales.malloy")).toContain(
         'import { order_items } from "storefront.malloy"',
      );
   });

   it("throws on what the guard refuses", () => {
      expect(() => newNotebookSource({ ...INPUT, title: "a\r\nb" })).toThrow();
      expect(() => newNotebookSource({ ...INPUT, title: "a |## b" })).toThrow();
   });
});

describe("newReportSource", () => {
   it("writes the two layouts from one template, differing only in layout= and the grid tag", () => {
      const grid = newReportSource(INPUT, "grid").split("\n");
      const linear = newReportSource(INPUT, "linear").split("\n");
      expect(grid).toHaveLength(linear.length);
      const differ = grid
         .map((line, i) => [line, linear[i]])
         .filter(([a, b]) => a !== b);
      expect(differ).toEqual([
         [
            '##| artifact { layout=grid title="Sales \\"West\\""',
            '##| artifact { layout=linear title="Sales \\"West\\""',
         ],
         ["} dashboard { columns=12 }", "}"],
      ]);
   });

   it("never writes kind= for the layout", () => {
      for (const layout of ["grid", "linear"] as const)
         expect(newReportSource(INPUT, layout)).not.toMatch(
            /kind=(dashboard|notebook)/,
         );
   });

   it("reads back by its tag: grid as a dashboard, linear as a notebook", async () => {
      const grid = await readDashboardDocument(newReportSource(INPUT, "grid"));
      if (readFailed(grid)) throw new Error(grid.reason);
      expect(grid.document.kind).toBeUndefined();
      expect(grid.document.columns).toBe(12);
      expect(
         grid.document.tiles.map((t) => (isTextTile(t) ? "text" : "query")),
      ).toEqual(["text", "query"]);
      const linear = await readDashboardDocument(
         newReportSource(INPUT, "linear"),
      );
      if (readFailed(linear)) throw new Error(linear.reason);
      expect(linear.document.kind).toBe("notebook");
   });

   it("reads the same wherever the file would sit", async () => {
      for (const path of ["dashboards/x.malloy", "notebooks/x.malloy"]) {
         const grid = await readDashboardDocument(
            newReportSource(INPUT, "grid"),
            path,
         );
         if (readFailed(grid)) throw new Error(grid.reason);
         expect(grid.document.kind).toBeUndefined();
         const linear = await readDashboardDocument(
            newReportSource(INPUT, "linear"),
            path,
         );
         if (readFailed(linear)) throw new Error(linear.reason);
         expect(linear.document.kind).toBe("notebook");
      }
   });
});

describe("newDocumentProblem", () => {
   it("takes a name either layout back-quotes", () => {
      for (const kind of ["dashboard", "notebook"] as const)
         expect(
            newDocumentProblem(kind, { ...INPUT, source: "order items" }),
         ).toBeUndefined();
   });

   it("back-quotes a source named like a keyword", () => {
      const text = newNotebookSource({
         ...INPUT,
         source: "source",
         view: "is",
      });
      expect(text).toContain("import { `source` }");
      expect(text).toContain("view: is_tile is `is`");
      expect(lintNotebookText("notebooks/sales.malloy", text)).toEqual([]);
      expect(
         newDocumentProblem("dashboard", { ...INPUT, source: "date" }),
      ).toBeUndefined();
   });

   it("trims the title once, for the tag and the heading", () => {
      const text = newNotebookSource({ ...INPUT, title: "  Q3  " });
      expect(text).toContain('title="Q3"');
      expect(text).toContain("# Q3\n");
   });

   it("refuses names that cannot be written", () => {
      expect(
         newDocumentProblem("dashboard", { ...INPUT, view: "a`b" }),
      ).toMatch(/view name/);
      expect(
         newDocumentProblem("dashboard", { ...INPUT, modelPath: 'a".malloy' }),
      ).toMatch(/model path/);
      expect(newDocumentProblem("dashboard", INPUT)).toBeUndefined();
      // Both layouts write the tag as a `##|` block, which `|##` would close.
      expect(
         newDocumentProblem("dashboard", { ...INPUT, title: "a |## b" }),
      ).toMatch(/\|##/);
   });
});

describe("document paths", () => {
   it("always satisfy the server's write shape", () => {
      let seed = 7;
      const next = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);
      const alphabet = "aZ09 -_/\\.\"'`#%\n\té中\u{1f600}*()[]";
      const shape = /^[^/]+\.malloy$/;
      for (let i = 0; i < 500; i++) {
         const length = next() % 40;
         let title = "";
         for (let j = 0; j < length; j++)
            title += [...alphabet][next() % [...alphabet].length];
         for (const suffix of [1, 2, 19]) {
            expect(documentPathForTitle(title, suffix)).toMatch(shape);
            expect(documentPathForTitle(title, suffix, "team")).toMatch(
               /^team\/[^/]+\.malloy$/,
            );
         }
      }
   });

   it("spells suffixes -2, -3 at the package root, or in a directory asked for", () => {
      expect(documentPathForTitle("Sales")).toBe("sales.malloy");
      expect(documentPathForTitle("Sales", 3)).toBe("sales-3.malloy");
      expect(documentPathForTitle("***")).toBe("untitled.malloy");
      expect(documentPathFor(slugFor("A b"))).toBe("a-b.malloy");
      expect(documentPathFor("a-b", "/team//q3/")).toBe("team/q3/a-b.malloy");
      expect(documentPathFor("a-b", "")).toBe("a-b.malloy");
   });
});
