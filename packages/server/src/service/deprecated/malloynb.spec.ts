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
const { cases } = JSON.parse(fs.readFileSync(TABLE, "utf8")) as {
   cases: { name: string; text: string; query: string | null }[];
};

describe("malloynbCellQuery, as the SDK's Report sends a cell", () => {
   it("reads a table with cases in it", () => {
      expect(cases.length).toBeGreaterThan(10);
   });
   for (const { name, text, query } of cases) {
      it(name, () => {
         expect(malloynbCellQuery(text) ?? null).toBe(query);
      });
   }
});
