// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { newNotebookSource } from "../DocumentCreate/newNotebook";
import { artifactTag, splitSourceLines } from "./malloyText";

// One case table for this reader and the server's; `artifact_tag_parity.spec.ts` there reads the same file.
const FIXTURE = path.join(import.meta.dir, "testing/artifactTagParity.json");
const { cases } = JSON.parse(fs.readFileSync(FIXTURE, "utf8")) as {
   cases: {
      name: string;
      source: string;
      tag: string[] | null;
      sdk?: string[] | null;
   }[];
};

describe("artifactTag agrees with the server and the lexer", () => {
   for (const { name, source, tag, sdk } of cases)
      it(name, () => {
         const found = artifactTag(splitSourceLines(source));
         expect(
            found ? found.text.split("\n").map((l) => l.trim()) : null,
         ).toEqual(sdk === undefined ? tag : sdk);
      });
});

describe("newNotebookSource", () => {
   it("writes the tag as a block whose closer has a line of its own", () => {
      const lines = splitSourceLines(
         newNotebookSource({
            title: "Sales",
            modelPath: "m.malloy",
            source: "orders",
            view: "by_brand",
         }),
      );
      const tag = artifactTag(lines);
      expect(tag?.block).toBe(true);
      expect(lines[tag!.from]).not.toContain("|##");
      expect(lines[tag!.to]).toBe("|##");
   });
});
