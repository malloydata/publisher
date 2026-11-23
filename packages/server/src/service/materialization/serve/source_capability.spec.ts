// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import {
   type Connection,
   type ModelDef,
   SingleConnectionRuntime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fieldKey,
   MAX_JOIN_DEPTH,
   readStringCapabilities,
   stringLeaves,
} from "./source_capability";

/** A model over `connection`, and what reading its capabilities needs. */
async function modelOver(connection: Connection, text: string) {
   const materializer = new SingleConnectionRuntime({ connection }).loadModel(
      text,
   );
   const modelDef = (await materializer.getModel())._modelDef as ModelDef;
   return {
      modelDef,
      load: (q: string) => materializer.loadQuery(q),
      connection: async () => connection,
   };
}

describe("a source's string fields", () => {
   it("are read through its joins to the join bound, and a measure is not selectable", () => {
      const join = (depth: number): Record<string, unknown> => ({
         name: `j${depth}`,
         join: "one",
         type: "table",
         fields: [
            { name: "s", type: "string" },
            ...(depth < MAX_JOIN_DEPTH + 2 ? [join(depth + 1)] : []),
         ],
      });
      const leaves = stringLeaves({
         fields: [
            { name: "a", type: "string" },
            { name: "n", type: "number" },
            { name: "m", type: "string", expressionType: "aggregate" },
            join(1),
         ],
      });
      expect(leaves.map((l) => l.path.join("."))).toEqual([
         "a",
         "m",
         "j1.s",
         "j1.j2.s",
         "j1.j2.j3.s",
         "j1.j2.j3.j4.s",
      ]);
      expect(leaves.find((l) => l.path[0] === "m")?.selectable).toBe(false);
   });
});

describe("a DuckDB connection's capability", () => {
   it("records the instance's default collation and its COLLATE definitions", async () => {
      const db = new DuckDBConnection("duckdb", ":memory:");
      try {
         const m = await modelOver(
            db as unknown as Connection,
            "source: t is duckdb.sql(\"SELECT 'a' AS r\")",
         );
         const before = await readStringCapabilities(m);
         expect(before.connections.get("duckdb")).toEqual({
            dialect: "duckdb",
            noDefaultCollation: true,
            noCollateDefinitions: true,
         });
         await db.runSQL("CREATE TABLE cap_ci (r VARCHAR COLLATE NOCASE)");
         const after = await readStringCapabilities(m);
         expect(after.connections.get("duckdb")?.noCollateDefinitions).toBe(
            false,
         );
         expect(after.sources.get("t")?.connectionName).toBe("duckdb");
      } finally {
         await db.runSQL("DROP TABLE IF EXISTS cap_ci").catch(() => {});
         await db.close();
      }
   });
});

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

describe.skipIf(!url)("a Postgres source's capability", () => {
   let pg: PostgresConnection;

   beforeAll(async () => {
      pg = new PostgresConnection(
         "pg",
         () => ({}),
         async () => ({ connectionString: url }),
      );
      await pg.runSQL(
         `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS cap_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); EXCEPTION WHEN others THEN NULL; END $$`,
      );
      await pg.runSQL("DROP TABLE IF EXISTS cap_t");
      await pg.runSQL(
         `CREATE TABLE cap_t (id int, c_bin text COLLATE "C", c_def text, c_ci text COLLATE cap_ci)`,
      );
   });
   afterAll(async () => {
      await pg.runSQL("DROP TABLE IF EXISTS cap_t").catch(() => {});
      await pg.close();
   });

   const MODEL = `source: t is pg.table('public.cap_t') extend {
  primary_key: id
  dimension: up_ci is upper(c_ci)
  measure: top_bin is max(c_bin)
  join_one: o is pg.table('public.cap_t') on id = o.id
}`;

   it("judges and identifies each string field, through a join and a computed dimension, from one compile", async () => {
      const caps = await readStringCapabilities(
         await modelOver(pg as unknown as Connection, MODEL),
      );
      const fields = caps.sources.get("t")!.fields;
      const of = (...path: string[]) => fields.get(fieldKey(path));
      expect(of("c_bin")?.collation).toBe("binary");
      expect(of("c_def")?.collation).toBe("deterministic");
      expect(of("c_ci")?.collation).toBe("unknown");
      expect(of("up_ci")?.identity).toBe(of("c_ci")?.identity);
      expect(of("o", "c_bin")?.identity).toBe(of("c_bin")?.identity);
      expect(of("c_def")?.identity).not.toBe(of("c_bin")?.identity);
      // A measure has no collation of a row to read: unknown.
      expect(of("top_bin")).toBeUndefined();
      const conn = caps.connections.get("pg");
      expect(conn?.defaultCollation).toBe("deterministic");
      expect(conn?.defaultIdentity).toBe(of("c_def")?.identity);
   });

   it("reads the catalog again on every read, so an ALTER is seen at the next bind", async () => {
      const m = await modelOver(pg as unknown as Connection, MODEL);
      expect(
         (await readStringCapabilities(m)).sources
            .get("t")!
            .fields.get(fieldKey(["c_def"]))?.collation,
      ).toBe("deterministic");
      await pg.runSQL(
         `ALTER TABLE cap_t ALTER COLUMN c_def TYPE text COLLATE "C"`,
      );
      try {
         expect(
            (await readStringCapabilities(m)).sources
               .get("t")!
               .fields.get(fieldKey(["c_def"]))?.collation,
         ).toBe("binary");
      } finally {
         await pg.runSQL(
            `ALTER TABLE cap_t ALTER COLUMN c_def TYPE text COLLATE "default"`,
         );
      }
   });
});
