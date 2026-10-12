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
      expect(result.document.layout).toBe("linear");
      expect(result.document.tiles).toHaveLength(7);
   });

   it("refuses a conversion it cannot make, naming the line", async () => {
      const result = await readForEditor(
         `## artifact { layout=linear }\nimport { o } from "../m.malloy"\n\nrun: o extend { dimension: z is 1 } -> { select: z }\n`,
      );
      expect(result.ok).toBe(false);
      expect(result.ok === false && result.reason).toMatch(/line 4/i);
   });

   it("says why a file that is not a notebook will not open, with its line", async () => {
      const result = await readForEditor(`source: a is b\n`);
      expect(result.ok).toBe(false);
   });

   // The reader takes no path: a report's layout is its tag's, wherever it lives.
   describe("a report's layout is its tags'", () => {
      const LAYOUT = `## artifact { title="D" tiles=["a -> x"] }\nsource: a is b extend {\n  view: x is v\n}\n`;

      it("opens a tile list with no layout as a grid", async () => {
         const result = await readForEditor(LAYOUT);
         expect(result.ok && result.document.layout).toBe("grid");
      });

      it("opens it linear when layout=linear says so", async () => {
         const result = await readForEditor(
            LAYOUT.replace("{ title", "{ layout=linear title"),
         );
         expect(result.ok && result.document.layout).toBe("linear");
      });

      it("refuses a tag that sets the removed kind=, naming the layout= to write", async () => {
         const result = await readForEditor(
            LAYOUT.replace("{ title", "{ kind=notebook title"),
         );
         expect(result.ok).toBe(false);
         expect(result.ok === false && result.reason).toContain(
            "`kind=notebook` is no longer read",
         );
         expect(result.ok === false && result.reason).toContain(
            "`layout=grid` or `layout=linear`",
         );
      });

      it("converts a cell-format file with no layout rather than refusing it", async () => {
         const layoutless = LEGACY.replace("layout=linear ", "");
         expect(layoutless).not.toBe(LEGACY);
         const result = await readForEditor(layoutless);
         if (result.ok === false) throw new Error(result.reason);
         expect(result.conversion?.from).toBe(layoutless);
      });

      // A tile list with no `layout=` reads as grid, so the converter writes `layout=linear`.
      it("converts a cell-format file with no layout into a LINEAR report", async () => {
         const layoutless = LEGACY.replace("layout=linear ", "");
         const result = await readForEditor(layoutless);
         if (result.ok === false) throw new Error(result.reason);
         expect(result.document.layout).toBe("linear");
         expect(result.conversion?.to).toContain("layout=linear");
      });
   });
});
