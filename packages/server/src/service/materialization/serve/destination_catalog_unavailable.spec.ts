// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DuckLakeCatalogUnavailableError } from "../../ducklake_catalog_unavailable";
import { isDestinationCatalogUnavailableError } from "./leg";

/**
 * A stored read whose destination catalog the worker lost degrades to live
 * whatever the binding's fallback says: the lookup's own typed error, or
 * DuckDB binding the read on an instance without the catalog. A missing table
 * in a catalog that is there does not.
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

   it("reads the lookup's typed error", () => {
      expect(
         isDestinationCatalogUnavailableError(
            new DuckLakeCatalogUnavailableError("credible"),
         ),
      ).toBe(true);
   });

   it("reads DuckDB's missing-catalog binder error", async () => {
      expect(
         isDestinationCatalogUnavailableError(
            await errorOf(`SELECT count(*) FROM credible.main.t`),
         ),
      ).toBe(true);
   });

   it("does not read a missing table or any other error as one", async () => {
      expect(
         isDestinationCatalogUnavailableError(
            await errorOf(`SELECT count(*) FROM memory.main.no_such_table`),
         ),
      ).toBe(false);
      expect(
         isDestinationCatalogUnavailableError(
            await errorOf(`SELECT CAST('x' AS INTEGER)`),
         ),
      ).toBe(false);
   });
});
