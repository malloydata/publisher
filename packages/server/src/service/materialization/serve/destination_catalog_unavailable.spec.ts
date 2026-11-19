// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DuckLakeCatalogUnavailableError } from "../../ducklake_catalog_unavailable";
import { isDestinationCatalogUnavailableError } from "./leg";

/**
 * A stored read whose destination catalog the worker lost degrades to live
 * whatever the binding's fallback says. Only the typed error says so: the
 * lookup raises it, and the connection raises it for a binder error once the
 * instance confirms the catalog is gone (connection.ts). A bare binder error
 * naming a catalog, which a misbinding also produces, is not one.
 */
describe("isDestinationCatalogUnavailableError", () => {
   let db: DuckDBConnection;
   beforeAll(() => {
      db = new DuckDBConnection("catalog-unavailable-spec", ":memory:");
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

   it("reads the typed error, and an error it caused", async () => {
      const typed = new DuckLakeCatalogUnavailableError("credible", {
         cause: await errorOf(`SELECT count(*) FROM credible.main.t`),
      });
      expect(isDestinationCatalogUnavailableError(typed)).toBe(true);
      expect(
         isDestinationCatalogUnavailableError(
            new Error("query failed", { cause: typed }),
         ),
      ).toBe(true);
   });

   it("does not read a bare binder error, a missing table or any other error as one", async () => {
      for (const sql of [
         `SELECT count(*) FROM credible.main.t`,
         `SELECT count(*) FROM memory.main.no_such_table`,
         `SELECT CAST('x' AS INTEGER)`,
      ]) {
         expect(isDestinationCatalogUnavailableError(await errorOf(sql))).toBe(
            false,
         );
      }
   });
});
