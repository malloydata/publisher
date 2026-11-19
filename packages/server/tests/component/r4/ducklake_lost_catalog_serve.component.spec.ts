// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A served copy over a real DuckLake storage destination whose catalog is
// lost from the worker's locked instance after its check was cached. The
// query that meets the loss answers live (READ_FAILED), the destination
// reopens with no restart, and the next query reads the stored table again.
// Both table-path shapes: three parts (`<dest>.main.<table>`), where DuckDB
// says `Binder Error: Catalog "<dest>" does not exist`, and two parts
// (`<dest>.<table>`, a destination with no schema), where it says only
// `Catalog Error: Table with name "<dest>.<table>" does not exist`.
//
// Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a Postgres the test may create
// and drop schemas in.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import { buildEnvironmentMalloyConfig } from "../../../src/service/connection";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { buildScenario, disposeScenarios, type R4Fixture } from "./harness";

const PG = process.env.PUBLISHER_TEST_POSTGRES_URL;
const QUERY = "run: daily -> { group_by: region; aggregate: t is total.sum() }";

let restore: () => void;
beforeAll(() => {
   restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } }).restore;
});
afterAll(async () => {
   restore();
   await disposeScenarios();
});

for (const parts of [2, 3] as const) {
   describe.skipIf(!PG)(
      `a DuckLake destination that loses its catalog (${parts}-part table paths)`,
      () => {
         let fixture: R4Fixture;
         let destination: ReturnType<typeof buildEnvironmentMalloyConfig>;
         let reopened = 0;
         let work: string;
         const schema = `lostcat_${process.pid}_${parts}`;

         beforeAll(async () => {
            fixture = await buildScenario({
               files: {
                  "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
}
`,
               },
               seedSql: [
                  `CREATE TABLE orders AS SELECT * FROM (VALUES
                     (1, DATE '2026-01-01', 'east', 100.5),
                     (2, DATE '2026-01-02', 'west', 3.5)
                   ) AS t(id, order_date, region, amount)`,
               ],
            });
            // Copy each stored table into a DuckLake catalog in Postgres.
            const u = new URL(PG!);
            const pg = new Client({ connectionString: PG });
            await pg.connect();
            await pg.query(`CREATE SCHEMA "${schema}"`);
            await pg.end();
            work = mkdtempSync(join(tmpdir(), "lostcat-"));
            const dataPath = join(work, "data");
            mkdirSync(dataPath);
            const setup = new DuckDBConnection("setup", ":memory:", work);
            const pgConn = {
               host: u.hostname,
               port: Number(u.port || 5432),
               databaseName: u.pathname.slice(1),
               userName: decodeURIComponent(u.username),
               password: decodeURIComponent(u.password),
            };
            await setup.runSQL(
               "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
            );
            await setup.runSQL(
               `ATTACH 'ducklake:postgres:host=${pgConn.host} port=${pgConn.port} dbname=${pgConn.databaseName} user=${pgConn.userName} password=${pgConn.password}' AS c (DATA_PATH '${dataPath}/', METADATA_SCHEMA '${schema}')`,
            );
            const bindings: R4Fixture["bindings"] = [];
            for (const b of fixture.bindings) {
               const physical = b.tablePath!.split(".").slice(1).join(".");
               const parquet = join(work, `${physical}.parquet`);
               await fixture.duckdb.runSQL(
                  `COPY (SELECT * FROM ${b.tablePath}) TO '${parquet}' (FORMAT parquet)`,
               );
               await setup.runSQL(
                  `CREATE TABLE c.main."${physical}" AS SELECT * FROM '${parquet}'`,
               );
               bindings.push({
                  ...b,
                  destinationName: "lakedest",
                  tablePath:
                     parts === 3
                        ? `lakedest.main.${physical}`
                        : `lakedest.${physical}`,
               });
            }
            await setup.close();
            const destinationRoot = join(work, ".storage-destinations");
            mkdirSync(destinationRoot);
            destination = buildEnvironmentMalloyConfig(
               [
                  {
                     name: "lakedest",
                     type: "ducklake",
                     ducklakeConnection: {
                        catalog: {
                           postgresConnection: pgConn,
                           metadataSchema: schema,
                        },
                        storage: { bucketUrl: `${dataPath}/` },
                     },
                  } as never,
               ],
               destinationRoot,
               false,
               {
                  origin: "storage-destination",
                  // As the environment wires it.
                  onDuckLakeReopened: () => {
                     reopened++;
                     fixture.model.invalidateCompiledLegs();
                  },
               },
            );
            fixture.model.setServeDestinationConfig(
               () => destination.malloyConfig,
            );
            fixture.setBindings(bindings);
         });

         afterAll(async () => {
            await destination?.releaseConnections().catch(() => {});
            if (work) rmSync(work, { recursive: true, force: true });
            const pg = new Client({ connectionString: PG });
            await pg.connect();
            await pg.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
            await pg.end();
         });

         async function servedFrom(): Promise<string> {
            const { servedFrom, rows } = await fixture.runAsIs(QUERY);
            expect(rows.length).toBe(2);
            return servedFrom ?? "live";
         }

         /** Lose the catalog from the destination's locked instance. */
         async function loseCatalog(): Promise<void> {
            const c =
               (await destination.malloyConfig.connections.lookupConnection(
                  "lakedest",
               )) as DuckDBConnection;
            await c.runSQL(`DETACH "lakedest"`);
         }

         it("answers the query that meets the loss live, then reads the table again with no restart", async () => {
            expect(await servedFrom()).toBe("storage");
            const before = reopened;
            await loseCatalog();
            const { servedFrom: first, path } = await fixture.runAsIs(QUERY);
            expect(first).toBe("live_fallback");
            expect(path?.considered?.[0]?.reason).toBe("READ_FAILED");
            for (let i = 0; i < 3; i++) {
               expect(await servedFrom()).toBe("storage");
            }
            expect(reopened).toBe(before + 1);
         });

         it("heals when a compile is the first to meet the loss", async () => {
            expect(await servedFrom()).toBe("storage");
            const before = reopened;
            await loseCatalog();
            // A rebind or build drops the compiled legs: the next query
            // compiles the served copy again.
            fixture.model.invalidateCompiledLegs();
            const answers: string[] = [];
            for (let i = 0; i < 4; i++) answers.push(await servedFrom());
            // At most the first answers live; none fails, and the rest read
            // the table again.
            expect(answers.slice(1)).toEqual(["storage", "storage", "storage"]);
            expect(["storage", "live_fallback", "live"]).toContain(answers[0]);
            expect(reopened).toBe(before + 1);
         });
      },
   );
}
