// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readForEditor } from "./readForEditor";

describe("readForEditor", () => {
   it("opens a layout file as it is", async () => {
      const result = await readForEditor(
         `## artifact { title="D" tiles=["a -> x"] }\nsource: a is b extend {\n  view: x is v\n}\n`,
      );
      expect(result.ok).toBe(true);
   });

   it("refuses a file written as run: cells, naming the fix", async () => {
      const result = await readForEditor(
         `## artifact { layout=linear }\nimport { o } from "../m.malloy"\n\nrun: o -> v\n`,
      );
      expect(result.ok === false && result.reason).toContain("no longer read");
      expect(result.ok === false && result.reason).toContain("tiles=[…]");
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
   });
});
