// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A storage destination's session `default_collation` is held to the
 * column contract: the build records it (`contract.defaultCollation`), read
 * by the same catalog batch a bind reads its tables with, in a real DuckDB
 * session; a bind that finds another one reports `collation_changed`
 * (`incompatible`), and a table built under a non-default setting passes its
 * own contract at the next bind, recorded as compared unlike live.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, describe, expect, it } from "bun:test";
import type { TableCollations } from "../domain/collation";
import { compatible, deriveContract } from "../domain/column_contract";
import { collationCatalogBatches } from "./warehouse_sql";

const connection = new DuckDBConnection("d", ":memory:");

afterAll(async () => {
   await connection.close();
});

/** The table's collations as the bind's (and the build's) catalog read finds them. */
async function readCollations(): Promise<TableCollations | undefined> {
   const [batch] = collationCatalogBatches(
      [{ key: "t", parts: ["main", "t"] }],
      "duckdb",
   );
   const result = await connection.runSQL(batch.sql);
   return batch.collationsOf(result.rows as Record<string, unknown>[]).get("t");
}

/** The contract a build records for `t`, from what it reads now. */
function contractOf(collations: TableCollations) {
   return deriveContract({
      tier: "storage",
      stored: {
         dialect: "duckdb",
         columns: [
            { name: "id", nativeType: "INTEGER", isString: false },
            { name: "name", nativeType: "VARCHAR", isString: true },
         ],
         collations,
         ...(collations.defaultCollation
            ? { defaultCollation: collations.defaultCollation }
            : {}),
      },
      live: new Map([["name", "binary"]]),
   });
}

/** `t` as a bind observes it now. */
async function observed() {
   const collations = await readCollations();
   return {
      columns: [
         { name: "id", nativeType: "INTEGER" },
         { name: "name", nativeType: "VARCHAR" },
      ],
      ...(collations ? { collations } : {}),
   };
}

describe("a destination's default_collation, held to the contract", () => {
   it("reads the session's setting beside the table's collations", async () => {
      await connection.runSQL("CREATE OR REPLACE TABLE t (id INTEGER, name VARCHAR)");
      expect((await readCollations())?.defaultCollation).toBe("binary");
      await connection.runSQL("SET default_collation = 'nocase'");
      try {
         expect((await readCollations())?.defaultCollation).toBe(
            "duckdb:nocase",
         );
      } finally {
         await connection.runSQL("SET default_collation = ''");
      }
   });

   it("finds a table incompatible when the setting changes after its build", async () => {
      const contract = contractOf((await readCollations())!);
      expect(contract.defaultCollation).toBe("binary");
      expect(compatible(contract, await observed())).toEqual({ ok: true });
      await connection.runSQL("SET default_collation = 'nocase'");
      try {
         expect(compatible(contract, await observed())).toEqual({
            ok: false,
            code: "collation_changed",
            columns: ["name"],
         });
      } finally {
         await connection.runSQL("SET default_collation = ''");
      }
   });

   it("passes a table built under a non-default setting at the next bind, compared unlike live", async () => {
      await connection.runSQL("SET default_collation = 'nocase'");
      try {
         const contract = contractOf((await readCollations())!);
         expect(contract.defaultCollation).toBe("duckdb:nocase");
         expect(contract.columns.find((c) => c.name === "name")?.compare).toBe(
            "none",
         );
         expect(compatible(contract, await observed())).toEqual({ ok: true });
      } finally {
         await connection.runSQL("SET default_collation = ''");
      }
   });
});
