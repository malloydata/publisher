// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE: delete with `.malloynb` notebooks.

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { malloynbCellQuery } from "./malloynb";

// One case table for this copy and the SDK's `malloynbCellQuery`, whose spec
// reads the same file: the load-time lint must check the text Report sends.
const TABLE = path.resolve(
   __dirname,
   "../../../../sdk/src/components/Report/malloynb/cellQueryParity.json",
);
const SDK_SOURCE = path.resolve(
   __dirname,
   "../../../../sdk/src/components/Report/malloynb/cellKind.ts",
);

/** A file's text from the function's first statement to its end: the body and the tokenizer it calls. */
const bodyOf = (file: string) => {
   const text = fs.readFileSync(file, "utf8");
   const start = text.indexOf("   const tokens = topLevelTokens(text);");
   expect(start).toBeGreaterThan(0);
   return text.slice(start);
};
const { cases } = JSON.parse(fs.readFileSync(TABLE, "utf8")) as {
   cases: { name: string; text: string; query: string | null }[];
};

describe("malloynbCellQuery, as the SDK's Report sends a cell", () => {
   it("is the SDK's function, statement for statement", () => {
      // Only the signature differs: the SDK takes a cell, this takes its text.
      expect(bodyOf(path.resolve(__dirname, "malloynb.ts"))).toBe(
         bodyOf(SDK_SOURCE),
      );
   });

   it("reads a table with cases in it", () => {
      expect(cases.length).toBeGreaterThan(10);
   });
   for (const { name, text, query } of cases) {
      it(name, () => {
         expect(malloynbCellQuery(text) ?? null).toBe(query);
      });
   }
});
