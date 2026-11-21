// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { runInternalSQL } from "../../duckdb_lockdown";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { type Connection, SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   storedTableCollationRefusal,
   stringGateCollationRefusal,
} from "./gate_collation";
import { equalityOnlyExpr, gateReadsOfQuery } from "./gate_reads";
import {
   postgresCollation as postgresCollationOf,
   postgresTableCollation as postgresTableCollationOf,
   probeIO,
   unfinalizedPostgresSQL,
} from "./gate_collation";
import { DUCKDB_COLLATION } from "./collation/duckdb";

/** The reads over a connection, through the I/O `gate_collation.ts` builds. */
const duckdbCollation = (c: Connection, sql: string) =>
   DUCKDB_COLLATION.sourceColumns(probeIO(c), sql);
const duckdbTableCollation = (c: Connection, path: string) =>
   DUCKDB_COLLATION.storedTable(probeIO(c), path);
const postgresCollation = (c: Connection, sql: string) =>
   postgresCollationOf(probeIO(c), sql);
const postgresTableCollation = (c: Connection, path: string) =>
   postgresTableCollationOf(probeIO(c), path);

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
            await runInternalSQL(
               db,
               "SELECT current_setting('default_collation') AS c",
            )
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

/**
 * A gate over a dimension computed from a string column compares that
 * column, so its collation is probed though the gate names only the
 * dimension: through a rename of it, a nested dimension, and a join.
 */
describe.skipIf(!url)(
   "a Postgres gate over a dimension computed from a string",
   () => {
      const MODEL = (
         table: string,
      ) => `##! experimental { givens access_modifiers composite_sources }
given: FLAG :: boolean
source: rows is pg.table('public.${table}') extend {
  primary_key: id
  dimension:
    late is region > 'M'
    is_n is region = 'n'
    is_n2 is is_n
}
#(access_filter) late = $FLAG
source: g_late is rows extend {}
#(access_filter) is_n = $FLAG
source: g_is_n is rows extend {}
#(access_filter) is_n2 = $FLAG
source: g_nested is rows extend {}
#(access_filter) r.is_n = $FLAG
source: g_join is pg.table('public.${table}') extend {
  primary_key: id
  join_one: r is rows on id = r.id
}
source: comp is compose(rows, rows extend { except: id })
source: c_late is comp extend { where: late }
source: c_is_n is comp extend { where: is_n }
source: c_nested is comp extend {
  dimension: l2 is late and id > 0
  where: l2
}
`;
      const conns: PostgresConnection[] = [];
      const connect = (db: string | undefined) => {
         const c = new PostgresConnection(
            "pg",
            () => ({}),
            async () => ({
               connectionString:
                  db === undefined ? url : url!.replace(/\/[^/]*$/, `/${db}`),
            }),
         );
         conns.push(c);
         return c;
      };
      let def: PostgresConnection;
      let cdb: PostgresConnection;

      beforeAll(async () => {
         def = connect(undefined);
         await def
            .runSQL(
               `CREATE DATABASE gate_cdef LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
            )
            .catch(() => {});
         cdb = connect("gate_cdef");
         for (const pg of [def, cdb]) {
            await pg.runSQL(
               `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS gate_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); EXCEPTION WHEN others THEN NULL; END $$`,
            );
            for (const [t, collate] of [
               ["gate_der_ci", "gate_ci"],
               ["gate_der_c", '"C"'],
            ]) {
               await pg.runSQL(`DROP TABLE IF EXISTS ${t}`);
               await pg.runSQL(
                  `CREATE TABLE ${t} (id int, region text COLLATE ${collate})`,
               );
               await pg.runSQL(
                  `INSERT INTO ${t} VALUES (1,'a'),(2,'B'),(3,'m'),(4,'N'),(5,'z')`,
               );
            }
         }
      });
      afterAll(async () => {
         for (const c of conns) {
            for (const t of ["gate_der_ci", "gate_der_c"]) {
               await c.runSQL(`DROP TABLE IF EXISTS ${t}`).catch(() => {});
            }
            await c.close();
         }
      });

      const refusal = async (
         pg: PostgresConnection,
         table: string,
         source: string,
      ) => {
         const q = new SingleConnectionRuntime({ connection: pg })
            .loadModel(MODEL(table))
            .loadQuery(`run: ${source} -> { group_by: id; order_by: id }`);
         const reads = await gateReadsOfQuery(q as never);
         return stringGateCollationRefusal({
            authorRunnable: q as never,
            reads,
            givens: { FLAG: true },
         });
      };

      it("refuses an equality through a dimension over a nondeterministic column", async () => {
         for (const s of ["g_is_n", "g_nested", "g_join"]) {
            expect(await refusal(def, "gate_der_ci", s), s).toBeDefined();
         }
      });

      it("refuses an ordering through a dimension over a nondeterministic column, under a C default", async () => {
         for (const s of ["g_late", "g_nested", "g_join"]) {
            expect(await refusal(cdb, "gate_der_ci", s), s).toBeDefined();
         }
      });

      it("refuses a scope through a composite's dimension, under a C default", async () => {
         // A composite's field is resolved per member when the query compiles,

         // so its expression names no column a probe could read.

         for (const s of ["c_late", "c_is_n", "c_nested"]) {
            expect(await refusal(cdb, "gate_der_ci", s), s).toBeDefined();
         }
      });

      it("still admits every one over a C column under a C default", async () => {
         for (const s of ["g_late", "g_is_n", "g_nested", "g_join"]) {
            expect(await refusal(cdb, "gate_der_c", s), s).toBeUndefined();
         }
      });
   },
);

/**
 * A database table in Postgres sits in its source's own warehouse: a scope
 * the absolute rule declines (an ordering under a non-C collation) is still
 * kept when every stored column it reads has its source's own collation, by
 * every name the column goes by, and the table's connection has the
 * source's default. A column altered after the build, or a connection to
 * another database whose same-named collation is defined otherwise, refuses.
 */
describe.skipIf(!url)(
   "a Postgres database table under its source's own collation",
   () => {
      const MODEL = `
source: s is pg.table('public.dsc_src') extend { where: region > 'M' }
source: s_eq is pg.table('public.dsc_src') extend { where: region = 'n' }
source: s_ren is pg.table('public.dsc_src') extend {
  rename: reg is region
  where: reg > 'M'
}
source: s_join is pg.table('public.dsc_base') extend {
  join_one: u is pg.table('public.dsc_src') on u.id = id
  where: u.region > 'M'
}
`;
      const conns: PostgresConnection[] = [];
      const connect = (db?: string) => {
         const c = new PostgresConnection(
            "pg",
            () => ({}),
            async () => ({
               connectionString:
                  db === undefined ? url : url!.replace(/\/[^/]*$/, `/${db}`),
            }),
         );
         conns.push(c);
         return c;
      };
      let pg: PostgresConnection;
      let other: PostgresConnection;
      const ci = (name: string, deterministic: boolean, locale: string) =>
         `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS ${name} (provider = icu, locale = '${locale}', deterministic = ${deterministic}); EXCEPTION WHEN others THEN NULL; END $$`;

      beforeAll(async () => {
         pg = connect();
         await pg.runSQL(`CREATE DATABASE dsc_other`).catch(() => {});
         other = connect("dsc_other");
         await pg.runSQL(ci("gate_ci", false, "und-u-ks-level2"));
         // The same name in another database, defined otherwise.
         await other.runSQL(ci("gate_ci", true, "und"));
         for (const c of [pg, other]) {
            for (const t of [
               "dsc_src",
               "dsc_base",
               "dsc_st",
               "dsc_alt",
               "dsc_join",
               "dsc_none",
            ]) {
               await c.runSQL(`DROP TABLE IF EXISTS ${t}`);
            }
         }
         await pg.runSQL(
            `CREATE TABLE dsc_src (id int, region text COLLATE gate_ci)`,
         );
         await pg.runSQL(
            `INSERT INTO dsc_src VALUES (1,'a'),(2,'B'),(3,'m'),(4,'N'),(5,'z')`,
         );
         await pg.runSQL(`CREATE TABLE dsc_base AS SELECT id FROM dsc_src`);
         await pg.runSQL(`CREATE TABLE dsc_none (id int, other text)`);
         // As a build writes them: CREATE TABLE AS keeps each column's collation.
         await pg.runSQL(`CREATE TABLE dsc_st AS SELECT * FROM dsc_src`);
         await pg.runSQL(`CREATE TABLE dsc_alt AS SELECT * FROM dsc_src`);
         await pg.runSQL(
            `ALTER TABLE dsc_alt ALTER COLUMN region TYPE text COLLATE "C"`,
         );
         await pg.runSQL(
            `CREATE TABLE dsc_join AS SELECT b.id, u.region AS u__region FROM dsc_base b JOIN dsc_src u ON u.id = b.id`,
         );
         await other.runSQL(
            `CREATE TABLE dsc_st (id int, region text COLLATE gate_ci)`,
         );
      });
      afterAll(async () => {
         for (const t of [
            "dsc_src",
            "dsc_base",
            "dsc_st",
            "dsc_alt",
            "dsc_join",
            "dsc_none",
         ]) {
            await pg.runSQL(`DROP TABLE IF EXISTS ${t}`).catch(() => {});
         }
         for (const c of conns) await c.close();
         const admin = connect();
         await admin
            .runSQL(`DROP DATABASE IF EXISTS dsc_other WITH (FORCE)`)
            .catch(() => {});
         await admin.close();
      });

      const refusal = async (
         source: string,
         tablePath: string,
         destinationName?: string,
         kind?: string,
      ) => {
         const author = new SingleConnectionRuntime({ connection: pg })
            .loadModel(MODEL)
            .loadQuery(`run: ${source} -> { group_by: id }`);
         return storedTableCollationRefusal({
            tables: [
               {
                  tier: "database",
                  tablePath,
                  ...(destinationName ? { destinationName } : {}),
                  ...(kind ? { kind } : {}),
               },
            ],
            answerRunnable: {
               runtime: {
                  connections: {
                     lookupConnection: async (name?: string) =>
                        name === "other" ? other : pg,
                  },
               },
            } as never,
            virtualMap: undefined,
            authorRunnable: author as never,
            reads: await gateReadsOfQuery(author as never),
         });
      };

      it("keeps an ordering and an equality over a column built under the source's own collation", async () => {
         expect(await refusal("s", "dsc_st")).toBeUndefined();
         expect(await refusal("s_eq", "dsc_st")).toBeUndefined();
      });

      it("maps a renamed column and a flattened join column to the stored ones", async () => {
         expect(await refusal("s_ren", "dsc_st")).toBeUndefined();
         expect(await refusal("s_join", "dsc_join")).toBeUndefined();
      });

      it("refuses a column altered to another collation after the build", async () => {
         expect(await refusal("s", "dsc_alt")).toBeDefined();
         expect(await refusal("s_ren", "dsc_alt")).toBeDefined();
      });

      it("refuses a table on another database whose same-named collation differs", async () => {
         expect(await refusal("s", "dsc_st", "other")).toBeDefined();
      });

      it("refuses a table that does not hold a column the scope reads", async () => {
         expect(await refusal("s_join", "dsc_none")).toBeDefined();
         expect(await refusal("s", "dsc_none")).toBeDefined();
      });

      it("passes over a rollup's base's own where: column, which its build applied", async () => {
         // `s`'s own `where: region > 'M'`: a rollup of `s` is built with it
         // applied, so its table holds no `region`.
         expect(
            await refusal("s", "dsc_none", undefined, "rollup"),
         ).toBeUndefined();
      });

      it("judges an inline extend that adds no where: or gate as the source it extends", async () => {
         expect(
            await refusal("s extend { dimension: z is 1 }", "dsc_st"),
         ).toBeUndefined();
         expect(
            await refusal("s extend { dimension: z is 1 }", "dsc_alt"),
         ).toBeDefined();
         // A `where:` of its own: no named source reads its rows.
         expect(
            await refusal("s extend { where: id > 0 }", "dsc_st"),
         ).toBeDefined();
      });
   },
);
