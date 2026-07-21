// Unit contract for the build-scoped credential primitives: the SQL a build
// session issues to (a) RW-attach a destination DuckLake and (b) federate a
// source warehouse for a native query-passthrough. `runSQL` is stubbed so no
// live warehouse / catalog is touched — this pins the SQL *shape* (handles,
// secret scoping, read-only-ness, escaping); live behavior is the measured
// spike's job.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import type { components } from "../api";
import {
   attachDuckLakeReadWrite,
   federateSourceForPassthrough,
} from "./connection";

/** A real in-memory DuckDB connection with runSQL stubbed to capture SQL. */
function stubbedConnection(): { conn: DuckDBConnection; sql: string[] } {
   const conn = new DuckDBConnection("build", ":memory:");
   const sql: string[] = [];
   sinon.stub(conn, "runSQL").callsFake(async (q: string) => {
      sql.push(q);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return { rows: [], totalRows: 0, runStats: {} } as any;
   });
   return { conn, sql };
}

const SERVICE_ACCOUNT = JSON.stringify({
   type: "service_account",
   project_id: "sa-project",
   private_key: "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
   client_email: "svc@sa-project.iam.gserviceaccount.com",
});

describe("federateSourceForPassthrough", () => {
   it("bigquery: creates a project-scoped SECRET and returns the project id as handle", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "bigquery", {
         name: "src_bq",
         bigqueryConnection: {
            defaultProjectId: "cfg-project",
            serviceAccountKeyJson: SERVICE_ACCOUNT,
         } as components["schemas"]["BigqueryConnection"],
      });
      expect(result).toEqual({ handle: "sa-project", sourceType: "bigquery" });
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"));
      expect(secret).toBeDefined();
      expect(secret).toContain("TYPE BIGQUERY");
      expect(secret).toContain("SCOPE 'bq://sa-project'");
      expect(secret).toContain("SERVICE_ACCOUNT_JSON");
      // Never ATTACH for a bigquery passthrough.
      expect(sql.some((s) => s.includes("ATTACH"))).toBe(false);
   });

   it("snowflake: creates a SECRET and returns the secret name as handle (no ATTACH)", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "snowflake", {
         name: "src_sf",
         snowflakeConnection: {
            account: "acct",
            username: "user",
            password: "p'wd",
            database: "DB",
            warehouse: "WH",
         } as components["schemas"]["SnowflakeConnection"],
      });
      expect(result.sourceType).toBe("snowflake");
      expect(result.handle).toBe("secret_snowflake_src_sf");
      const secret = sql.find((s) => s.includes("CREATE OR REPLACE SECRET"));
      expect(secret).toContain("TYPE snowflake");
      expect(secret).toContain("ACCOUNT 'acct'");
      // Single quotes in a value are doubled (escapeSQL).
      expect(secret).toContain("PASSWORD 'p''wd'");
      expect(sql.some((s) => s.includes("ATTACH"))).toBe(false);
   });

   it("postgres: ATTACHes READ_ONLY and returns the alias as handle", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "postgres", {
         name: "src_pg",
         postgresConnection: {
            host: "h",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "pw",
         } as components["schemas"]["PostgresConnection"],
      });
      expect(result).toEqual({ handle: "src_pg", sourceType: "postgres" });
      const attach = sql.find((s) => s.startsWith("ATTACH"));
      expect(attach).toBeDefined();
      // The ATTACH alias is dialect-quoted; the handle stays the raw name (it
      // becomes the postgres_query string literal, which matches the catalog).
      expect(attach).toContain('AS "src_pg" (TYPE postgres, READ_ONLY)');
   });

   it("postgres: dialect-quotes an alias that needs quoting (e.g. a hyphen)", async () => {
      const { conn, sql } = stubbedConnection();
      const result = await federateSourceForPassthrough(conn, "postgres", {
         name: "my-pg",
         postgresConnection: {
            host: "h",
            port: 5432,
            databaseName: "d",
            userName: "u",
            password: "pw",
         } as components["schemas"]["PostgresConnection"],
      });
      expect(result.handle).toBe("my-pg");
      const attach = sql.find((s) => s.startsWith("ATTACH"));
      expect(attach).toContain('AS "my-pg" (TYPE postgres, READ_ONLY)');
   });

   it("rejects a source type with no native passthrough", async () => {
      const { conn } = stubbedConnection();
      await expect(
         federateSourceForPassthrough(conn, "mysql" as unknown as "postgres", {
            name: "x",
         }),
      ).rejects.toThrow(/no native query-passthrough/i);
   });

   it("fails loud when the configured credentials are missing", async () => {
      const { conn } = stubbedConnection();
      await expect(
         federateSourceForPassthrough(conn, "bigquery", { name: "x" }),
      ).rejects.toThrow(/BigQuery connection configuration missing/i);
   });
});

describe("attachDuckLakeReadWrite", () => {
   const ducklakeConfig = {
      catalog: {
         postgresConnection: {
            host: "cat-host",
            port: 5432,
            databaseName: "ducklake",
            userName: "ducklake",
            password: "secret",
         },
      },
      storage: { bucketUrl: "gs://org-env-ducklake/prefix" },
   } as unknown as components["schemas"]["DucklakeConnection"];

   it("attaches read-write: no READ_ONLY, no AUTOMATIC_MIGRATION, keeps OVERRIDE_DATA_PATH", async () => {
      const { conn, sql } = stubbedConnection();
      await attachDuckLakeReadWrite(conn, "lake", ducklakeConfig);
      const attach = sql.find((s) => s.includes("ATTACH OR REPLACE"));
      expect(attach).toBeDefined();
      expect(attach).toContain("OVERRIDE_DATA_PATH true");
      expect(attach).not.toContain("READ_ONLY");
      expect(attach).not.toContain("AUTOMATIC_MIGRATION");
      expect(attach).toContain("AS lake");
   });
});
