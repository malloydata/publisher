// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../errors";
import { assertSingleSelect, classifySelect } from "./single_select";

describe("classifySelect", () => {
   it.each([
      ["a SELECT", "SELECT 1"],
      ["a CTE", "WITH a AS (SELECT 1 AS x) SELECT * FROM a"],
      ["a trailing semicolon", "SELECT 1;"],
      [
         "a catalog-qualified table it never binds",
         "SELECT * FROM lake.main.orders LIMIT 5",
      ],
   ])("counts %s as one SELECT", async (_label, sql) => {
      expect(await classifySelect(sql)).toEqual({ selects: 1 });
   });

   it("counts each statement of a script", async () => {
      expect(await classifySelect("SELECT 1; SELECT 2")).toEqual({
         selects: 2,
      });
   });

   it.each([
      ["a CREATE SECRET", "CREATE SECRET s (TYPE s3, PROVIDER config)"],
      ["a SET", "SET threads = 1"],
      ["an ATTACH", "ATTACH ':memory:' AS other"],
      ["a COPY", "COPY (SELECT 1) TO 'out.csv'"],
      ["a SELECT followed by a statement", "SELECT 1; SET threads = 1"],
      ["text that does not parse", "SELEC 1"],
   ])("refuses %s", async (_label, sql) => {
      expect(await classifySelect(sql)).toHaveProperty("refused");
   });
});

describe("assertSingleSelect", () => {
   it("lets one SELECT through", async () => {
      await assertSingleSelect("sales", "SELECT 1");
   });

   it.each([
      ["SET threads = 1", "it is not a SELECT"],
      ["SELECT 1; SELECT 2", "it holds 2 statements"],
      ["", "it holds no statement"],
   ])("refuses %p with a 400 naming the database and why", async (sql, why) => {
      const refused = assertSingleSelect("sales", sql);
      await expect(refused).rejects.toBeInstanceOf(BadRequestError);
      await expect(refused).rejects.toThrow(`'sales' is a managed database`);
      await expect(refused).rejects.toThrow(why);
   });
});
