// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { readForEditor } from "./readForEditor";

const REPO = path.resolve(import.meta.dir, "../../../../..");
const LEGACY = fs
   .readFileSync(
      path.join(REPO, "examples/storefront/notebooks/category-review.malloy"),
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
});
