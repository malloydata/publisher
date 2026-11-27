// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// F-03: whether a database table's container exists, read per dialect: a
// BigQuery dataset in the project the container names (else the
// connection's) and in the connection's location; a Postgres, Snowflake or
// DuckDB schema. A read that fails refuses nothing.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   type TargetContainerConnection,
   targetContainerKey,
} from "../domain/target_containers";
import {
   readTargetContainer,
   readTargetContainers,
} from "./target_container_read";

/** A BigQuery connection over a fake client, recording each dataset read. */
function bigQuery(opts: {
   datasets: Record<string, { location: string }>;
   /** The billing project, the client's own (db-bigquery's billingProjectId). */
   billingProjectId?: string;
   /** The connection's configured default project; else billing. */
   defaultProjectId?: string;
   location?: string;
   fail?: Error;
}) {
   const reads: { id: string; projectId?: string }[] = [];
   const billing = opts.billingProjectId ?? "home";
   const connection = {
      dialectName: "standardsql",
      projectId: opts.defaultProjectId ?? billing,
      billingProjectId: billing,
      location: opts.location,
      runSQL: async () => {
         throw new Error("no SQL expected");
      },
      bigQuery: {
         projectId: billing,
         dataset(id: string, options?: { projectId?: string }) {
            reads.push({ id, projectId: options?.projectId });
            return {
               async getMetadata() {
                  if (opts.fail) throw opts.fail;
                  const found =
                     opts.datasets[`${options?.projectId ?? billing}.${id}`];
                  if (!found) {
                     throw Object.assign(new Error("Not found: Dataset"), {
                        code: 404,
                     });
                  }
                  return [found];
               },
            };
         },
      },
   };
   return {
      connection: connection as unknown as TargetContainerConnection,
      reads,
   };
}

describe("BigQuery: a dataset's metadata", () => {
   it("reads a one-part container in the connection's project", async () => {
      const { connection, reads } = bigQuery({
         datasets: { "home.analytics": { location: "US" } },
      });
      expect(await readTargetContainer(connection, "analytics")).toEqual({
         found: true,
      });
      expect(reads).toEqual([{ id: "analytics", projectId: "home" }]);
   });

   it("reads a project-qualified container in the project it names", async () => {
      const { connection, reads } = bigQuery({
         datasets: { "other.reports": { location: "US" } },
      });
      expect(await readTargetContainer(connection, "other.reports")).toEqual({
         found: true,
      });
      expect(reads).toEqual([{ id: "reports", projectId: "other" }]);
   });

   it("keeps a domain-scoped project id whole", async () => {
      const { connection, reads } = bigQuery({ datasets: {} });
      await readTargetContainer(connection, "example.com:proj.reports");
      expect(reads).toEqual([{ id: "reports", projectId: "example.com:proj" }]);
   });

   it("reads a missing dataset as not found, naming the project it read", async () => {
      const { connection } = bigQuery({ datasets: {} });
      expect(await readTargetContainer(connection, "no_such_dataset")).toEqual({
         notFound: true,
         project: "home",
      });
      expect(await readTargetContainer(connection, "p2.ds")).toEqual({
         notFound: true,
      });
   });

   // db-bigquery runs DDL through createQueryJob with no projectId and no
   // defaultDataset, so `CREATE TABLE ds.t` lands in the billing project,
   // not the connection's default projectId.
   describe("a default project other than the billing project", () => {
      const projects = {
         billingProjectId: "billing",
         defaultProjectId: "data",
      };

      it("reads a two-part name's dataset in the billing project", async () => {
         const { connection, reads } = bigQuery({
            ...projects,
            datasets: { "billing.analytics": { location: "US" } },
         });
         expect(await readTargetContainer(connection, "analytics")).toEqual({
            found: true,
         });
         expect(reads).toEqual([{ id: "analytics", projectId: "billing" }]);
      });

      it("refuses a dataset only the default project holds, naming the billing project", async () => {
         const { connection } = bigQuery({
            ...projects,
            datasets: { "data.analytics": { location: "US" } },
         });
         expect(await readTargetContainer(connection, "analytics")).toEqual({
            notFound: true,
            project: "billing",
         });
      });

      it("reads a three-part name's dataset in the project it names", async () => {
         const { connection, reads } = bigQuery({
            ...projects,
            datasets: { "data.analytics": { location: "US" } },
         });
         expect(
            await readTargetContainer(connection, "data.analytics"),
         ).toEqual({ found: true });
         expect(reads).toEqual([{ id: "analytics", projectId: "data" }]);
      });
   });

   it("reads a dataset in another location than the connection's as a mismatch", async () => {
      const { connection } = bigQuery({
         datasets: { "home.eu_ds": { location: "EU" } },
         location: "US",
      });
      expect(await readTargetContainer(connection, "eu_ds")).toEqual({
         otherLocation: { location: "EU", expected: "US" },
      });
   });

   it("compares locations whatever their case, and not at all when the connection declares none", async () => {
      const { connection } = bigQuery({
         datasets: { "home.ds": { location: "us" } },
         location: "US",
      });
      expect(await readTargetContainer(connection, "ds")).toEqual({
         found: true,
      });
      const undeclared = bigQuery({
         datasets: { "home.ds": { location: "EU" } },
      });
      expect(await readTargetContainer(undeclared.connection, "ds")).toEqual({
         found: true,
      });
   });
});

describe("readTargetContainers", () => {
   it("reads each (connection, container) once", async () => {
      const { connection, reads } = bigQuery({
         datasets: { "home.analytics": { location: "US" } },
      });
      const containers = [
         { connectionName: "bq", container: "analytics" },
         { connectionName: "bq", container: "analytics" },
      ];
      const readings = await readTargetContainers({
         containers,
         connection: async () => connection,
      });
      expect(reads).toHaveLength(1);
      expect(readings[targetContainerKey(containers[0])]).toEqual({
         found: true,
      });
   });

   it("reads a failed read, or a connection that does not resolve, as unread (fail open)", async () => {
      const { connection } = bigQuery({
         datasets: {},
         fail: Object.assign(new Error("Access Denied"), { code: 403 }),
      });
      const a = { connectionName: "bq", container: "analytics" };
      const b = { connectionName: "gone", container: "analytics" };
      const readings = await readTargetContainers({
         containers: [a, b],
         connection: async (name) => {
            if (name === "gone") throw new Error("no such connection");
            return connection;
         },
      });
      expect(readings[targetContainerKey(a)]).toEqual({ unread: true });
      expect(readings[targetContainerKey(b)]).toEqual({ unread: true });
   });

   it("reads a read that outlives its timeout as unread", async () => {
      const slow = {
         dialectName: "postgres",
         runSQL: () => new Promise(() => {}),
      } as unknown as TargetContainerConnection;
      const target = { connectionName: "pg", container: "s" };
      const readings = await readTargetContainers({
         containers: [target],
         connection: async () => slow,
         timeoutMs: 20,
      });
      expect(readings[targetContainerKey(target)]).toEqual({ unread: true });
   });
});

/** A SQL connection answering every statement with `rows`, recording each. */
function sqlConnection(dialectName: string, rows: Record<string, unknown>[]) {
   const statements: string[] = [];
   const connection = {
      dialectName,
      runSQL: async (sql: string) => {
         statements.push(sql);
         return { rows };
      },
   } as unknown as TargetContainerConnection;
   return { connection, statements };
}

describe("Postgres and Snowflake: the catalog's schemas", () => {
   it("Postgres: counts pg_namespace in the connection's database", async () => {
      const none = sqlConnection("postgres", [{ db: "app", n: 0 }]);
      expect(await readTargetContainer(none.connection, "anal'ytics")).toEqual({
         notFound: true,
      });
      expect(none.statements[0]).toContain("pg_catalog.pg_namespace");
      expect(none.statements[0]).toContain("nspname = 'anal''ytics'");
      const one = sqlConnection("postgres", [{ db: "app", n: 1 }]);
      expect(await readTargetContainer(one.connection, "app.s")).toEqual({
         found: true,
      });
   });

   it("Postgres: leaves a container in another database unchecked", async () => {
      const other = sqlConnection("postgres", [{ db: "app", n: 0 }]);
      expect(
         await readTargetContainer(other.connection, "elsewhere.s"),
      ).toEqual({ unread: true });
   });

   it("Snowflake: reads the named database's information_schema, its column in any case", async () => {
      const none = sqlConnection("snowflake", [{ N: 0 }]);
      expect(await readTargetContainer(none.connection, "DB.REPORTS")).toEqual({
         notFound: true,
      });
      expect(none.statements[0]).toContain('"DB".information_schema.schemata');
      expect(none.statements[0]).toContain("schema_name = 'REPORTS'");
      const one = sqlConnection("snowflake", [{ N: 1 }]);
      expect(await readTargetContainer(one.connection, "REPORTS")).toEqual({
         found: true,
      });
   });

   it("reads an answer with no row as unread, and leaves other dialects unchecked", async () => {
      const empty = sqlConnection("postgres", []);
      expect(await readTargetContainer(empty.connection, "s")).toEqual({
         unread: true,
      });
      const trino = sqlConnection("trino", [{ n: 0 }]);
      expect(await readTargetContainer(trino.connection, "s")).toEqual({
         unread: true,
      });
      expect(trino.statements).toEqual([]);
   });
});

describe("DuckDB: duckdb_schemas()", () => {
   let duckdb: DuckDBConnection;
   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL("CREATE SCHEMA analytics");
      await duckdb.runSQL("ATTACH ':memory:' AS side");
   });
   afterAll(async () => {
      await duckdb.close();
   });

   it("finds a schema, case-insensitively, and not one that does not exist", async () => {
      expect(await readTargetContainer(duckdb, "Analytics")).toEqual({
         found: true,
      });
      expect(await readTargetContainer(duckdb, "nope")).toEqual({
         notFound: true,
      });
   });

   it("reads a one-part container as an attached database too, as CREATE TABLE db.t does", async () => {
      expect(await readTargetContainer(duckdb, "side")).toEqual({
         found: true,
      });
      expect(await readTargetContainer(duckdb, "side.main")).toEqual({
         found: true,
      });
      expect(await readTargetContainer(duckdb, "side.nope")).toEqual({
         notFound: true,
      });
   });
});
