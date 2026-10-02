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
import { newNotebookSource } from "./newNotebook";

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
      expect(query?.name).toBe("cell_1");
      expect(text).toContain(
         "source: order_items_tiles is order_items extend {",
      );
      expect(text).not.toMatch(/^run:/m);
   });

   it("reads back for a back-quoted source too", async () => {
      const result = await readDashboardDocument(
         newNotebookSource({ ...INPUT, source: "order items" }),
      );
      if (readFailed(result)) throw new Error(result.reason);
      expect(result.document.tiles.filter(isQueryTile)).toHaveLength(1);
   });

   it("imports the one source by name", () => {
      expect(newNotebookSource(INPUT)).toContain(
         'import { order_items } from "../models/storefront.malloy"',
      );
   });

   it("throws on what the guard refuses", () => {
      expect(() => newNotebookSource({ ...INPUT, title: "a\r\nb" })).toThrow();
      expect(() => newNotebookSource({ ...INPUT, title: "a |## b" })).toThrow();
   });
});

describe("newDocumentProblem", () => {
   it("holds a dashboard to bare identifiers, as its writer does", () => {
      expect(
         newDocumentProblem("dashboard", { ...INPUT, source: "order items" }),
      ).toMatch(/source name/);
      expect(
         newDocumentProblem("notebook", { ...INPUT, source: "order items" }),
      ).toBeUndefined();
   });

   it("back-quotes a source named like a keyword, and a dashboard refuses it", () => {
      const text = newNotebookSource({
         ...INPUT,
         source: "source",
         view: "is",
      });
      expect(text).toContain("import { `source` }");
      expect(text).toContain("view: cell_1 is `is`");
      expect(lintNotebookText("notebooks/sales.malloy", text)).toEqual([]);
      expect(
         newDocumentProblem("dashboard", { ...INPUT, source: "date" }),
      ).toMatch(/source name/);
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
      expect(
         newDocumentProblem("dashboard", { ...INPUT, title: "a |## b" }),
      ).toBeUndefined();
   });
});

describe("document paths", () => {
   it("always satisfy the server's write shape", () => {
      let seed = 7;
      const next = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);
      const alphabet = "aZ09 -_/\\.\"'`#%\n\té中\u{1f600}*()[]";
      const shape = /^(dashboards|notebooks)\/[^/]+\.malloy$/;
      for (let i = 0; i < 500; i++) {
         const length = next() % 40;
         let title = "";
         for (let j = 0; j < length; j++)
            title += [...alphabet][next() % [...alphabet].length];
         for (const kind of ["dashboard", "notebook"] as const)
            for (const suffix of [1, 2, 19])
               expect(documentPathForTitle(kind, title, suffix)).toMatch(shape);
      }
   });

   it("spells suffixes -2, -3 and names the kind's directory", () => {
      expect(documentPathForTitle("notebook", "Sales")).toBe(
         "notebooks/sales.malloy",
      );
      expect(documentPathForTitle("dashboard", "Sales", 3)).toBe(
         "dashboards/sales-3.malloy",
      );
      expect(documentPathForTitle("dashboard", "***")).toBe(
         "dashboards/untitled.malloy",
      );
      expect(documentPathFor("notebook", slugFor("A b"))).toBe(
         "notebooks/a-b.malloy",
      );
   });
});
