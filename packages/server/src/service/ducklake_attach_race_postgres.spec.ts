// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Against a real DuckLake catalog in Postgres: overlapping first lookups of
// one storage destination with an S3 secret must leave the catalog attached,
// and a destination whose locked instance lost its catalog must reopen on the
// next lookup. Before the fix, an overlapping lookup's refused storage secret
// detached the catalog from the locked instance, and every later read failed
// with `Catalog "credible" does not exist` until the process restarted.
//
// Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a Postgres the test may create
// and drop schemas in.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import sinon from "sinon";
import type { components } from "../api";
import { buildEnvironmentMalloyConfig } from "./connection";
import { DuckLakeCatalogUnavailableError } from "./ducklake_catalog_unavailable";
import { isInstanceLocked } from "./duckdb_lockdown";

type ApiConnection = components["schemas"]["Connection"];

const PG_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const LOOKUPS = 4;

describe.skipIf(!PG_URL)(
   "a DuckLake storage destination over a real Postgres catalog",
   () => {
      let pg: Client;
      let pgConn: {
         host: string;
         port: number;
         databaseName: string;
         userName: string;
         password: string;
      };
      let round = 0;

      beforeAll(async () => {
         const u = new URL(PG_URL!);
         pgConn = {
            host: u.hostname,
            port: Number(u.port || 5432),
            databaseName: u.pathname.slice(1),
            userName: decodeURIComponent(u.username),
            password: decodeURIComponent(u.password),
         };
         pg = new Client({ connectionString: PG_URL });
         await pg.connect();
      });

      afterAll(async () => {
         await pg?.end();
      });

      /**
       * A fresh catalog (one empty table, created by a plain unlocked
       * session) and an environment holding it as destination `credible`.
       */
      async function freshDestination(): Promise<{
         env: ReturnType<typeof buildEnvironmentMalloyConfig>;
         reopened: string[];
         dispose: () => Promise<void>;
      }> {
         const schema = `attach_race_${process.pid}_${round++}`;
         await pg.query(`CREATE SCHEMA "${schema}"`);
         const bucketUrl = `s3://attach-race-bucket/${schema}/`;
         const setupDir = mkdtempSync(join(tmpdir(), "attach-race-setup-"));
         const setup = new DuckDBConnection("setup", ":memory:", setupDir);
         try {
            await setup.runSQL(
               "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
            );
            await setup.runSQL(
               `ATTACH 'ducklake:postgres:host=${pgConn.host} port=${pgConn.port} dbname=${pgConn.databaseName} user=${pgConn.userName} password=${pgConn.password}' AS c (DATA_PATH '${bucketUrl}', METADATA_SCHEMA '${schema}')`,
            );
            await setup.runSQL("CREATE TABLE c.main.t (x INTEGER)");
         } finally {
            await setup.close();
            rmSync(setupDir, { recursive: true, force: true });
         }
         const root = mkdtempSync(join(tmpdir(), "attach-race-env-"));
         const destinationRoot = join(root, ".storage-destinations");
         mkdirSync(destinationRoot, { recursive: true });
         const destination = {
            name: "credible",
            type: "ducklake",
            ducklakeConnection: {
               catalog: { postgresConnection: pgConn, metadataSchema: schema },
               storage: {
                  bucketUrl,
                  s3Connection: {
                     accessKeyId: "AKIAATTACHRACE",
                     secretAccessKey: "fake",
                     region: "us-east-1",
                  },
               },
            },
         } as ApiConnection;
         const reopened: string[] = [];
         const env = buildEnvironmentMalloyConfig(
            [destination],
            destinationRoot,
            false,
            {
               origin: "storage-destination",
               onDuckLakeReopened: (name) => reopened.push(name),
            },
         );
         return {
            env,
            reopened,
            dispose: async () => {
               await env.releaseConnections().catch(() => {});
               rmSync(root, { recursive: true, force: true });
               await pg.query(`DROP SCHEMA "${schema}" CASCADE`);
            },
         };
      }

      async function readCount(
         env: ReturnType<typeof buildEnvironmentMalloyConfig>,
      ): Promise<unknown> {
         const c = (await env.malloyConfig.connections.lookupConnection(
            "credible",
         )) as DuckDBConnection;
         const result = await c.runSQL(
            "SELECT count(*) AS n FROM credible.main.t",
         );
         return Number(result.rows[0]?.n);
      }

      it("keeps the catalog attached through overlapping first lookups", async () => {
         // Hold each storage secret so the lookups overlap the way a slow
         // catalog round trip makes them overlap at boot: before the fix the
         // first lookup locked the instance while a later one held its secret.
         const proto = Object.getPrototypeOf(DuckDBConnection.prototype) as {
            runSQL: (sql: string, opts?: unknown) => Promise<unknown>;
         };
         const runSQL = proto.runSQL;
         const held = sinon.stub(proto, "runSQL").callsFake(async function (
            this: unknown,
            sql: string,
            opts,
         ) {
            if (/^\s*CREATE OR REPLACE SECRET/i.test(sql)) {
               await new Promise((r) => setTimeout(r, 300));
            }
            return runSQL.call(this, sql, opts);
         });
         try {
            for (let i = 0; i < 3; i++) {
               const { env, dispose } = await freshDestination();
               try {
                  const lookups = await Promise.allSettled(
                     Array.from({ length: LOOKUPS }, async (_, n) => {
                        await new Promise((r) => setTimeout(r, n * 100));
                        return env.malloyConfig.connections.lookupConnection(
                           "credible",
                        );
                     }),
                  );
                  expect(
                     lookups
                        .filter((l) => l.status === "rejected")
                        .map((l) =>
                           String((l as PromiseRejectedResult).reason),
                        ),
                  ).toEqual([]);
                  expect(await readCount(env)).toBe(0);
                  expect(await readCount(env)).toBe(0);
               } finally {
                  await dispose();
               }
            }
         } finally {
            held.restore();
         }
      });

      it("reopens a destination whose locked instance lost its catalog", async () => {
         const { env, reopened, dispose } = await freshDestination();
         try {
            const first = (await env.malloyConfig.connections.lookupConnection(
               "credible",
            )) as DuckDBConnection;
            expect(isInstanceLocked(first)).toBe(true);
            // The state the race used to leave: locked, catalog detached.
            await first.runSQL(`DETACH "credible"`);
            await expect(readCountOn(first)).rejects.toThrow(
               /Catalog "credible" does not exist/,
            );

            const lost = await env.malloyConfig.connections
               .lookupConnection("credible")
               .then(
                  () => undefined,
                  (error: unknown) => error,
               );
            expect(lost).toBeInstanceOf(DuckLakeCatalogUnavailableError);
            expect(reopened).toEqual(["credible"]);

            // No restart: the next lookup opens a fresh instance and reads.
            expect(await readCount(env)).toBe(0);
         } finally {
            await dispose();
         }
      });
   },
);

async function readCountOn(c: DuckDBConnection): Promise<unknown> {
   return c.runSQL("SELECT count(*) AS n FROM credible.main.t");
}
