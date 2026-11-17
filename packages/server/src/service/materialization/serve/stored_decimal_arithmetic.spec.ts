// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { isStoredDecimalArithmeticError } from "./leg";

/**
 * A stored Postgres `numeric` is the exact DECIMAL(38, s) that holds it, and
 * DuckDB's DECIMAL arithmetic over it can run out of digits that live's
 * arbitrary precision has. Each such refusal, as DuckDB words it, degrades the
 * answer to live; any other error does not.
 */
describe("isStoredDecimalArithmeticError", () => {
   let db: DuckDBConnection;
   beforeAll(() => {
      db = new DuckDBConnection("stored-decimal-spec", ":memory:");
   });
   afterAll(async () => {
      await db.close();
   });

   async function errorOf(sql: string): Promise<unknown> {
      try {
         await db.runSQL(sql);
      } catch (err) {
         return err;
      }
      throw new Error(`expected ${sql} to fail`);
   }

   it("reads DuckDB's DECIMAL scale and overflow refusals", async () => {
      for (const sql of [
         // A product's scale past 38.
         `SELECT sum(x * x) FROM (SELECT 0.33333333333333333333::DECIMAL(38,20) AS x)`,
         // A product past 38 digits at run time.
         `SELECT x * x FROM (SELECT 1234567.5::DECIMAL(38,16) AS x)`,
         // An addition past 38 digits.
         `SELECT x + x FROM (SELECT 99999999999999999999999999999999999999::DECIMAL(38,0) AS x)`,
      ]) {
         expect(isStoredDecimalArithmeticError(await errorOf(sql))).toBe(true);
      }
   });

   it("does not read any other error as one", async () => {
      expect(
         isStoredDecimalArithmeticError(
            await errorOf(`SELECT CAST('x' AS INTEGER)`),
         ),
      ).toBe(false);
      expect(
         isStoredDecimalArithmeticError(await errorOf(`SELECT nope FROM t`)),
      ).toBe(false);
   });
});
