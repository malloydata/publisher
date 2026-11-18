// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { type Connection, SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { equalityOnlyExpr, storedTableCollationRefusal } from "./gate_reads";
import {
   duckdbCollation,
   duckdbTableCollation,
   postgresCollation,
   postgresTableCollation,
   unfinalizedPostgresSQL,
} from "./gate_collation";

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
         // In-memory DuckDB instances can be shared within a process.
         await db.runSQL("SET default_collation = ''").catch(() => {});
         await db
            .runSQL(
               "DROP TABLE IF EXISTS ci_t; DROP TABLE IF EXISTS t_plain; DROP TABLE IF EXISTS t_alter",
            )
            .catch(() => {});
         await db.close();
      }
   });

   it("is unknown when a macro returns a collated value", async () => {
      const db = new DuckDBConnection("duckdb", ":memory:");
      try {
         await db.runSQL(
            "CREATE MACRO gate_ci(x) AS CAST(x AS VARCHAR) COLLATE NOCASE",
         );
         expect(
            await duckdbCollation(
               db as unknown as Connection,
               "SELECT gate_ci('a') AS probe_value",
            ),
         ).toBe("unknown");
      } finally {
         await db.runSQL("DROP MACRO IF EXISTS gate_ci").catch(() => {});
         await db.close();
      }
   });

   it("reads a default_collation set before the instance's configuration was locked", async () => {
      // As the lockdown leaves an operator's connection: its setupSQL ran,
      // then the configuration was locked. Reading the setting still works;
      // only a SET is refused.
      const db = new DuckDBConnection({
         name: "duckdb",
         databasePath: ":memory:",
         setupSQL: "SET default_collation = 'nocase'",
         lockConfiguration: true,
      } as never);
      try {
         await expect(db.runSQL("SET default_collation = ''")).rejects.toThrow(
            /locked/,
         );
         const [row] = (
            await db.runSQL("SELECT current_setting('default_collation') AS c")
         ).rows;
         expect(row.c).toBe("nocase");
         expect(
            await duckdbCollation(
               db as unknown as Connection,
               "SELECT 'a' AS probe_value",
            ),
         ).toBe("unknown");
         expect(
            await duckdbTableCollation(
               db as unknown as Connection,
               "t_missing",
            ),
         ).toBe("unknown");
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
         // In-memory DuckDB instances can be shared within a process.
         await db.runSQL("SET default_collation = ''").catch(() => {});
         await db
            .runSQL(
               "DROP TABLE IF EXISTS ci_t; DROP TABLE IF EXISTS t_plain; DROP TABLE IF EXISTS t_alter",
            )
            .catch(() => {});
         await db.close();
      }
   });
});

describe("a stored table's own collation", () => {
   it("is binary for a plain DuckDB table, and unknown once a column is altered to NOCASE, under a default_collation, or for a table the catalog does not list", async () => {
      const db = new DuckDBConnection("duckdb", ":memory:");
      try {
         const c = db as unknown as Connection;
         await db.runSQL("CREATE TABLE t_plain (r VARCHAR)");
         await db.runSQL("CREATE TABLE t_alter (r VARCHAR)");
         expect(await duckdbTableCollation(c, '"main"."t_plain"')).toBe(
            "binary",
         );
         await db.runSQL(
            "ALTER TABLE t_alter ALTER COLUMN r SET DATA TYPE VARCHAR COLLATE NOCASE",
         );
         expect(await duckdbTableCollation(c, "t_alter")).toBe("unknown");
         expect(await duckdbTableCollation(c, "t_missing")).toBe("unknown");
         await db.runSQL("SET default_collation = 'nocase'");
         expect(await duckdbTableCollation(c, "t_plain")).toBe("unknown");
      } finally {
         // In-memory DuckDB instances can be shared within a process.
         await db.runSQL("SET default_collation = ''").catch(() => {});
         await db
            .runSQL(
               "DROP TABLE IF EXISTS ci_t; DROP TABLE IF EXISTS t_plain; DROP TABLE IF EXISTS t_alter",
            )
            .catch(() => {});
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

describe("a Postgres probe's SQL", () => {
   it("reads the stage Malloy's final stage wraps, and leaves other SQL alone", () => {
      const stage = `WITH __stage0 AS (\n  SELECT \n     (base."c") as "probe_value"\n  FROM t as base\n  LIMIT 1\n)\n`;
      expect(
         unfinalizedPostgresSQL(
            `${stage}SELECT row_to_json(finalStage) as row FROM __stage0 AS finalStage`,
         ),
      ).toBe(`${stage}SELECT * FROM __stage0`);
      const plain = "SELECT c AS probe_value FROM t";
      expect(unfinalizedPostgresSQL(plain)).toBe(plain);
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

   it("reads a table's columns: unknown once one is nondeterministic", async () => {
      await pg.runSQL(`DROP TABLE IF EXISTS gate_coll_c`);
      await pg.runSQL(
         `CREATE TABLE gate_coll_c (c_bin text COLLATE "C", n int)`,
      );
      expect(
         await postgresTableCollation(
            pg as unknown as Connection,
            "gate_coll_c",
         ),
      ).toBe("binary");
      await pg.runSQL(
         `ALTER TABLE gate_coll_c ALTER COLUMN c_bin TYPE text COLLATE gate_ci`,
      );
      expect(
         await postgresTableCollation(
            pg as unknown as Connection,
            "gate_coll_c",
         ),
      ).toBe("unknown");
      await pg.runSQL(`DROP TABLE gate_coll_c`);
   });

   it("judges a database created with an ICU default deterministic, never binary", async () => {
      const icu = new PostgresConnection(
         "pg_icu",
         () => ({}),
         async () => ({
            connectionString: url!.replace(/\/[^/]*$/, "/gate_icu"),
         }),
      );
      await pg
         .runSQL(
            `CREATE DATABASE gate_icu LOCALE_PROVIDER icu ICU_LOCALE 'und' LOCALE 'C' TEMPLATE template0`,
         )
         .catch(() => {});
      try {
         await icu.runSQL(`DROP TABLE IF EXISTS t_icu`);
         await icu.runSQL(`CREATE TABLE t_icu (c text)`);
         expect(
            await postgresCollation(
               icu as unknown as Connection,
               `SELECT c AS probe_value FROM t_icu`,
            ),
         ).toBe("deterministic");
         expect(
            await postgresTableCollation(icu as unknown as Connection, "t_icu"),
         ).toBe("deterministic");
      } finally {
         await icu.close();
      }
   });

   it("judges a database table with no path unknown, never binary", async () => {
      const postgresAuthor = new SingleConnectionRuntime({ connection: pg })
         .loadModel(`source: s is pg.sql("SELECT 'a' AS c")`)
         .loadQuery("run: s -> { select: c }");
      expect(
         await storedTableCollationRefusal({
            tables: [{ tier: "database" }],
            answerRunnable: {
               runtime: {
                  connections: { lookupConnection: async () => pg },
               },
            } as never,
            virtualMap: undefined,
            authorRunnable: postgresAuthor as never,
            reads: undefined,
         }),
      ).toBeDefined();
   });

   it("reads a column through a query Malloy compiled, as the probe sends it", async () => {
      const { sql } = await new SingleConnectionRuntime({ connection: pg })
         .loadModel(`source: s is pg.table('public.gate_coll_t')`)
         .loadQuery("run: s -> { select: probe_value is c_bin; limit: 1 }")
         .getPreparedResult();
      // Malloy wraps a Postgres query's rows as one JSON column, which has
      // no `probe_value` to read.
      await expect(
         postgresCollation(pg as unknown as Connection, sql),
      ).rejects.toThrow();
      expect(
         await postgresCollation(
            pg as unknown as Connection,
            unfinalizedPostgresSQL(sql),
         ),
      ).toBe("binary");
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
