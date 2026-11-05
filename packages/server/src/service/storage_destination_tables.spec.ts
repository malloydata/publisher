// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import sinon from "sinon";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import { attachDuckLakeForCreate, attachDuckLakeReadOnly } from "./connection";
import { storageDestinationRoot } from "./connection_config";
import {
   cleanupStorageFiles,
   dropStorageTable,
   listStorageTables,
} from "./materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];

const LAKE: ApiConnection = { name: "lake", type: "duckdb" } as ApiConnection;

/**
 * A plain DuckDB destination: the same list/drop code path a DuckLake one takes
 * once attached, with a database file standing in for the catalog. DuckLake's
 * own maintenance calls need a real catalog and are exercised end to end by the
 * hammer scenario instead.
 */
describe("storage destination tables (plain DuckDB destination)", () => {
   let envPath: string;
   let dbPath: string;

   beforeEach(() => {
      envPath = mkdtempSync(join(tmpdir(), "dest-tables-"));
      mkdirSync(storageDestinationRoot(envPath), { recursive: true });
      dbPath = join(storageDestinationRoot(envPath), "lake.duckdb");
   });
   afterEach(() => rmSync(envPath, { recursive: true, force: true }));

   async function seed(sql: string): Promise<void> {
      const instance = await DuckDBInstance.create(dbPath);
      const connection = await instance.connect();
      try {
         await connection.run(sql);
      } finally {
         connection.closeSync();
         instance.closeSync();
      }
   }

   const list = (schemaName = "main") =>
      listStorageTables({
         destinationName: "lake",
         destinationConnection: LAKE,
         schemaName,
         environmentPath: envPath,
      });
   const drop = (physicalTableName: string) =>
      dropStorageTable({
         destinationName: "lake",
         destinationConnection: LAKE,
         physicalTableName,
         environmentPath: envPath,
      });

   it("reports a destination that was never written as empty, without creating it", async () => {
      expect(await list()).toEqual([]);
      expect(await Bun.file(dbPath).exists()).toBe(false);
   });

   it("lists one schema's tables in name order", async () => {
      await seed(
         "CREATE TABLE b_daily__g001 AS SELECT 1 AS x; " +
            "CREATE TABLE a_daily__g001 AS SELECT 1 AS x; " +
            "CREATE SCHEMA analytics; " +
            "CREATE TABLE analytics.other AS SELECT 1 AS x;",
      );
      expect(await list()).toEqual(["a_daily__g001", "b_daily__g001"]);
      expect(await list("analytics")).toEqual(["other"]);
      expect(await list("absent")).toEqual([]);
   });

   it("drops exactly the named table, and a repeated drop is a no-op", async () => {
      await seed(
         "CREATE TABLE daily__g001 AS SELECT 1 AS x; " +
            "CREATE TABLE daily__g002 AS SELECT 2 AS x;",
      );
      await drop("daily__g001");
      expect(await list()).toEqual(["daily__g002"]);
      await drop("daily__g001");
      expect(await list()).toEqual(["daily__g002"]);
   });

   it("drops a table in a named schema by its qualified name", async () => {
      await seed(
         "CREATE SCHEMA analytics; " +
            "CREATE TABLE analytics.daily AS SELECT 1 AS x; " +
            "CREATE TABLE daily AS SELECT 1 AS x;",
      );
      await drop("analytics.daily");
      expect(await list("analytics")).toEqual([]);
      expect(await list()).toEqual(["daily"]);
   });

   it("refuses a file cleanup for a destination that is not DuckLake", async () => {
      const cleanup = cleanupStorageFiles({
         destinationName: "lake",
         destinationConnection: LAKE,
         snapshotsOlderThan: new Date(),
         filesOlderThan: new Date(),
      });
      await expect(cleanup).rejects.toBeInstanceOf(BadRequestError);
      await expect(cleanup).rejects.toThrow(/only to 'ducklake'/);
   });
});

// A DuckLake destination reaches S3 through the host's credential chain, which
// `S3_CREDENTIAL_CHAIN_POLICY=destinations-only` permits for the operator's
// destinations and refuses for tenant connections. These sessions attach the
// destination directly, so each must declare which of the two it is. The catalog
// is unreachable, so the DuckLake ATTACH is answered as a success and each
// operation then fails on its first read of the catalog, after the storage
// secret is created -- the secret statement is the proof the policy gate was
// passed.
describe("storage destination attach under destinations-only (DuckLake)", () => {
   const ENV_KEYS = [
      "S3_CREDENTIAL_CHAIN_POLICY",
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
   ];
   const saved: Record<string, string | undefined> = {};
   let secretStatements: string[];
   let allStatements: string[];
   let workDir: string;

   const CHAIN_LAKE: ApiConnection = {
      name: "lake",
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "127.0.0.1",
               port: 1,
               databaseName: "d",
               userName: "u",
               password: "p",
            },
         },
         storage: {
            bucketUrl: "s3://bucket/prefix",
            s3Connection: {
               provider: "credential_chain",
               chain: "env",
               region: "us-east-1",
            },
         },
      },
   };

   beforeEach(() => {
      for (const key of ENV_KEYS) saved[key] = process.env[key];
      process.env.S3_CREDENTIAL_CHAIN_POLICY = "destinations-only";
      process.env.AWS_ACCESS_KEY_ID = "AKIAFAKE";
      process.env.AWS_SECRET_ACCESS_KEY = "fakesecret";
      secretStatements = [];
      allStatements = [];
      workDir = mkdtempSync(join(tmpdir(), "dest-attach-"));
      const runSQL = DuckDBConnection.prototype.runSQL;
      sinon
         .stub(DuckDBConnection.prototype, "runSQL")
         .callsFake(async function (this: DuckDBConnection, sql: string) {
            allStatements.push(String(sql));
            if (/CREATE OR REPLACE SECRET/i.test(String(sql))) {
               secretStatements.push(String(sql));
            }
            if (/ATTACH OR REPLACE 'ducklake:/.test(String(sql))) {
               return [] as never;
            }
            return runSQL.call(this, sql);
         });
   });
   afterEach(() => {
      sinon.restore();
      rmSync(workDir, { recursive: true, force: true });
      for (const key of ENV_KEYS) {
         if (saved[key] === undefined) delete process.env[key];
         else process.env[key] = saved[key];
      }
   });

   async function expectAttachPastPolicy(attach: Promise<unknown>) {
      let message = "";
      try {
         await attach;
      } catch (error) {
         message = (error as Error).message;
      }
      expect(message).not.toBe("");
      expect(message).not.toMatch(/not permitted/);
      expect(secretStatements).toHaveLength(1);
      expect(secretStatements[0]).toContain("PROVIDER credential_chain");
   }

   it("lists a chain-auth destination past the policy gate", async () => {
      await expectAttachPastPolicy(
         listStorageTables({
            destinationName: "lake",
            destinationConnection: CHAIN_LAKE,
            schemaName: "main",
            environmentPath: tmpdir(),
         }),
      );
   });

   it("cleans up a chain-auth destination past the policy gate", async () => {
      await expectAttachPastPolicy(
         cleanupStorageFiles({
            destinationName: "lake",
            destinationConnection: CHAIN_LAKE,
            snapshotsOlderThan: new Date(),
            filesOlderThan: new Date(),
         }),
      );
   });

   // The admin API's catalogs are the operator's too: its create and its reads
   // declare the destination origin, so a chain credential passes the gate. (The
   // chain secret itself cannot validate without real credentials, so each attach
   // fails there, after the gate, and the statement is the proof.)
   it("creates a chain-auth catalog past the policy gate", async () => {
      const session = new DuckDBConnection("lake", ":memory:", workDir);
      try {
         await expectAttachPastPolicy(
            attachDuckLakeForCreate(
               session,
               "lake",
               CHAIN_LAKE.ducklakeConnection!,
               { encrypted: true },
            ),
         );
      } finally {
         await session.close();
      }
      expect(secretStatements[0]).toContain("SCOPE 's3://bucket/prefix/'");
   });

   it("reads a chain-auth catalog past the policy gate", async () => {
      const session = new DuckDBConnection("lake", ":memory:", workDir);
      try {
         await expectAttachPastPolicy(
            attachDuckLakeReadOnly(
               session,
               "lake",
               CHAIN_LAKE.ducklakeConnection!,
            ),
         );
      } finally {
         await session.close();
      }
   });

   // The scope is what keeps a modeler's raw SQL on this session from reading
   // another tenant's prefix of the shared bucket with the destination's credential.
   describe("storage secret scope", () => {
      const lakeAt = (bucketUrl: string, gcs = false): ApiConnection => ({
         ...CHAIN_LAKE,
         ducklakeConnection: {
            ...CHAIN_LAKE.ducklakeConnection!,
            storage: gcs
               ? { bucketUrl, gcsConnection: { keyId: "k", secret: "s" } }
               : {
                    bucketUrl,
                    s3Connection:
                       CHAIN_LAKE.ducklakeConnection!.storage!.s3Connection,
                 },
         },
      });
      const attach = (lake: ApiConnection) =>
         listStorageTables({
            destinationName: "lake",
            destinationConnection: lake,
            schemaName: "main",
            environmentPath: tmpdir(),
         }).catch(() => undefined);

      it("scopes the S3 secret to the prefix with exactly one trailing slash", async () => {
         for (const [url, scope] of [
            ["s3://bucket", "s3://bucket/"],
            ["s3://bucket/prefix", "s3://bucket/prefix/"],
            ["s3://bucket/prefix/", "s3://bucket/prefix/"],
            ["s3://bucket/prefix//", "s3://bucket/prefix/"],
         ]) {
            secretStatements.length = 0;
            await attach(lakeAt(url));
            expect(secretStatements).toHaveLength(1);
            expect(secretStatements[0]).toContain(`SCOPE '${scope}'`);
         }
      });

      it("scopes the GCS secret too", async () => {
         await attach(lakeAt("gs://bucket/prefix", true));
         expect(secretStatements).toHaveLength(1);
         expect(secretStatements[0]).toContain("TYPE gcs");
         expect(secretStatements[0]).toContain("SCOPE 'gs://bucket/prefix/'");
      });

      it("turns off the env-seeded global S3 config before anything reaches S3", async () => {
         await attach(lakeAt("s3://bucket/prefix"));
         const set = allStatements.findIndex((sql) =>
            /SET GLOBAL enable_global_s3_configuration=false/.test(sql),
         );
         const ducklake = allStatements.findIndex((sql) =>
            /ATTACH OR REPLACE 'ducklake:/.test(sql),
         );
         const secret = allStatements.findIndex((sql) =>
            /CREATE OR REPLACE SECRET/.test(sql),
         );
         expect(set).toBeGreaterThanOrEqual(0);
         expect(set).toBeLessThan(ducklake);
         expect(set).toBeLessThan(secret);
      });

      it("turns the global S3 config off for every connection object on the instance", async () => {
         // The attach and the secret live on the DuckDB instance, which later
         // connection objects (a destination rebuild, a connection add) share; a
         // session-local SET would leave those with the env-key fallback.
         const dbFile = join(workDir, "lake.duckdb");
         const attached = new DuckDBConnection("lake", dbFile, workDir);
         let later: DuckDBConnection | undefined;
         try {
            // Fails at the chain secret's validation, after the SET.
            await attachDuckLakeReadOnly(
               attached,
               "lake",
               lakeAt("s3://bucket/prefix").ducklakeConnection!,
            ).catch(() => undefined);
            // Created once the first is open, as a later connection object is,
            // so it joins the pooled instance rather than racing it for one.
            later = new DuckDBConnection("lake", dbFile, workDir);
            const result = (await later.runSQL(
               "SELECT value FROM duckdb_settings() WHERE name = 'enable_global_s3_configuration'",
            )) as unknown as
               | { rows: { value: string }[] }
               | { value: string }[];
            const rows = Array.isArray(result) ? result : result.rows;
            expect(rows).toEqual([{ value: "false" }]);
         } finally {
            await attached.close();
            await later?.close();
         }
      });

      it("does not touch the global S3 config for GCS storage", async () => {
         await attach(lakeAt("gs://bucket/prefix", true));
         expect(allStatements.join("\n")).not.toContain(
            "enable_global_s3_configuration",
         );
      });

      it("re-emits the scope when the expired-credential renewer rebuilds the secret", async () => {
         const proto = DuckDBConnection.prototype as unknown as {
            setStorageSecretRenewer?: (renew: () => Promise<void>) => void;
         };
         let renew: (() => Promise<void>) | undefined;
         // A chain secret cannot validate without real credentials, and the renewer
         // is only registered once creation succeeds, so the statement is faked.
         sinon.restore();
         const runSQL = DuckDBConnection.prototype.runSQL;
         sinon
            .stub(DuckDBConnection.prototype, "runSQL")
            .callsFake(async function (this: DuckDBConnection, sql: string) {
               if (/CREATE OR REPLACE SECRET/i.test(String(sql))) {
                  secretStatements.push(String(sql));
                  return { rows: [] } as never;
               }
               if (/ATTACH OR REPLACE 'ducklake:/.test(String(sql))) {
                  return [] as never;
               }
               return runSQL.call(this, sql);
            });
         proto.setStorageSecretRenewer = (fn) => {
            renew = fn;
         };
         try {
            await attach(lakeAt("s3://bucket/prefix"));
            expect(renew).toBeDefined();
            secretStatements.length = 0;
            await renew!();
         } finally {
            delete proto.setStorageSecretRenewer;
         }
         expect(secretStatements).toHaveLength(1);
         expect(secretStatements[0]).toContain("SCOPE 's3://bucket/prefix/'");
      });
   });
});
