// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import type { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { equalityOnlyExpr } from "./gate_reads";
import { duckdbCollation, postgresCollation } from "./gate_collation";

describe("a DuckDB source's string comparison", () => {
   it("is binary with no collation set or written, and unknown with one", async () => {
      const db = new DuckDBConnection("duckdb", ":memory:");
      try {
         const c = db as unknown as Connection;
         expect(await duckdbCollation(c, "SELECT 'a' AS probe_value")).toBe(
            "binary",
         );
         // A COLLATE in the source's own SQL.
         expect(
            await duckdbCollation(
               c,
               "SELECT 'a' COLLATE NOCASE AS probe_value",
            ),
         ).toBe("unknown");
         // A COLLATE in any table the instance holds.
         await db.runSQL("CREATE TABLE ci_t (r VARCHAR COLLATE NOCASE)");
         expect(await duckdbCollation(c, "SELECT 'a' AS probe_value")).toBe(
            "unknown",
         );
      } finally {
         await db.close();
      }
   });

   it("is unknown under an instance default_collation", async () => {
      const db = new DuckDBConnection("duckdb", ":memory:");
      try {
         await db.runSQL("SET default_collation = 'nocase'");
         expect(
            await duckdbCollation(
               db as unknown as Connection,
               "SELECT 'a' AS probe_value",
            ),
         ).toBe("unknown");
      } finally {
         await db.close();
      }
   });
});

describe("an equality-only comparison", () => {
   it("is =, !=, in and is null over fields, givens and literals, and nothing else", () => {
      const f = { node: "field", path: ["r"] };
      const g = { node: "given", id: "X" };
      const eq = (node: string, r: unknown = g) => ({
         node,
         kids: { left: f, right: r },
      });
      expect(equalityOnlyExpr(eq("="))).toBe(true);
      expect(equalityOnlyExpr(eq("!="))).toBe(true);
      expect(
         equalityOnlyExpr({
            node: "and",
            kids: { left: eq("="), right: { node: "is-not-null", e: f } },
         }),
      ).toBe(true);
      expect(equalityOnlyExpr(eq("<"))).toBe(false);
      expect(equalityOnlyExpr(eq("like", { node: "stringLiteral" }))).toBe(
         false,
      );
      expect(
         equalityOnlyExpr({
            node: "=",
            kids: { left: { node: "function_call", kids: {} }, right: g },
         }),
      ).toBe(false);
   });
});

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

describe.skipIf(!url)("a Postgres column's collation", () => {
   let pg: PostgresConnection;
   beforeAll(async () => {
      pg = new PostgresConnection(
         "pg",
         () => ({}),
         async () => ({
            connectionString: url,
         }),
      );
      await pg.runSQL(`DROP TABLE IF EXISTS gate_coll_t`);
      await pg.runSQL(
         `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS gate_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); EXCEPTION WHEN others THEN NULL; END $$`,
      );
      await pg.runSQL(
         `CREATE TABLE gate_coll_t (c_bin text COLLATE "C", c_def text, c_ci text COLLATE gate_ci)`,
      );
      await pg.runSQL(`INSERT INTO gate_coll_t VALUES ('a', 'a', 'a')`);
   });
   afterAll(async () => {
      await pg.runSQL(`DROP TABLE IF EXISTS gate_coll_t`);
      await pg.close();
   });

   const of = (col: string) =>
      postgresCollation(
         pg as unknown as Connection,
         `SELECT ${col} AS probe_value FROM gate_coll_t`,
      );

   it("is binary under C, deterministic under the default, and unknown under a nondeterministic collation", async () => {
      expect(await of("c_bin")).toBe("binary");
      expect(["deterministic", "binary"]).toContain(await of("c_def"));
      expect(await of("c_ci")).toBe("unknown");
   });

   it("reads an empty source's column too", async () => {
      expect(
         await postgresCollation(
            pg as unknown as Connection,
            `SELECT c_bin AS probe_value FROM gate_coll_t WHERE false`,
         ),
      ).toBe("binary");
   });
});
