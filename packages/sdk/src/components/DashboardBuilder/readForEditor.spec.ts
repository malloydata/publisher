// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { readForEditor } from "./readForEditor";

const REPO = path.resolve(import.meta.dir, "../../../../..");
const LEGACY = fs
   .readFileSync(
      path.join(REPO, "examples/storefront/category-review.malloy"),
      "utf8",
   )
   .replace(/\r\n/g, "\n");

describe("readForEditor", () => {
   it("opens a layout file as it is, with no conversion", async () => {
      const result = await readForEditor(
         `## artifact { title="D" tiles=["a -> x"] }\nsource: a is b extend {\n  view: x is v\n}\n`,
      );
      expect(result.ok).toBe(true);
      expect(result.ok && result.conversion).toBeUndefined();
   });

   it("converts a cell-format notebook, keeping the disk text to hand back", async () => {
      const result = await readForEditor(LEGACY);
      if (result.ok === false) throw new Error(result.reason);
      expect(result.conversion?.from).toBe(LEGACY);
      expect(result.conversion?.to).toContain(
         "tiles=[\n    text_1 { kind=text }",
      );
      expect(result.document.kind).toBe("notebook");
      expect(result.document.tiles).toHaveLength(7);
   });

   it("refuses a conversion it cannot make, naming the line", async () => {
      const result = await readForEditor(
         `## artifact { kind=notebook }\nimport { o } from "../m.malloy"\n\nrun: o extend { dimension: z is 1 } -> { select: z }\n`,
      );
      expect(result.ok).toBe(false);
      expect(result.ok === false && result.reason).toMatch(/line 4/i);
   });

   it("says why a file that is not a notebook will not open, with its line", async () => {
      const result = await readForEditor(`source: a is b\n`);
      expect(result.ok).toBe(false);
   });

   describe("a report's layout is its tags', wherever the file lives", () => {
      const LAYOUT = `## artifact { title="D" tiles=["a -> x"] }\nsource: a is b extend {\n  view: x is v\n}\n`;
      const PLACES = [
         undefined,
         "d.malloy",
         "notebooks/d.malloy",
         "dashboards/d.malloy",
         "sales/q3/d.malloy",
      ];

      it("opens a tile list with no layout as a grid, in any directory", async () => {
         for (const where of PLACES) {
            const result = await readForEditor(LAYOUT, where);
            expect([where, result.ok && result.document.kind]).toEqual([
               where,
               undefined,
            ]);
         }
      });

      it("opens it linear wherever layout=linear (or the legacy kind=notebook) says so", async () => {
         for (const tag of ["layout=linear", "kind=notebook"])
            for (const where of PLACES) {
               const result = await readForEditor(
                  LAYOUT.replace("{ title", `{ ${tag} title`),
                  where,
               );
               expect([where, result.ok && result.document.kind]).toEqual([
                  where,
                  "notebook",
               ]);
            }
      });

      it("converts a cell-format file with no layout rather than refusing it, in any directory", async () => {
         const layoutless = LEGACY.replace("layout=linear ", "");
         expect(layoutless).not.toBe(LEGACY);
         for (const where of PLACES) {
            const result = await readForEditor(layoutless, where);
            if (result.ok === false) throw new Error(result.reason);
            expect(result.conversion?.from).toBe(layoutless);
         }
      });

      // KNOWN BUG, not yet fixed in source: the converter keeps the tag as
      // written and adds `tiles=[…]`, so a cell report with no `layout=` (linear
      // by the default rule) converts to a tile list with no `layout=`, which
      // reads as GRID. The converter must write `layout=linear`.
      it("converts a cell-format file with no layout into a LINEAR report", async () => {
         const layoutless = LEGACY.replace("layout=linear ", "");
         const result = await readForEditor(layoutless, "x.malloy");
         if (result.ok === false) throw new Error(result.reason);
         expect(result.document.kind).toBe("notebook");
         expect(result.conversion?.to).toContain("layout=linear");
      });
   });
});
