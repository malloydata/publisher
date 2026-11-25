// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` extension on another tier or in another `storage=`
 * destination than its parent is built over the parent's table, never by
 * recomputing the parent from raw: the drop-raw proof. The parent is built
 * alone; its raw rows are then deleted out of band; the extension, built
 * next, still builds, reports `upstreamReuse: "reused"`, names the parent's
 * table in `builtFrom`, and serves what live answered before the delete.
 *
 *  - a `storage=` extension of a database parent: built in the warehouse
 *    over the parent's table, then copied into its destination;
 *  - a `storage=` extension of a parent in another destination: built in its
 *    own destination's session with the parent's attached read-only.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse (a DuckDB
 * warehouse is not read through to a `storage=` build) and the DuckLake
 * catalogs; the build in a destination runs on DuckDB.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const RAW_ROWS = `SELECT * FROM (VALUES
    ('a', CAST(9.0 AS DECIMAL(10,1))), ('a', CAST(10.5 AS DECIMAL(10,1))),
    ('b', CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(cat, amt)`;

/** No string is grouped, filtered or ordered: every answer compares as live. */
const QUERY =
   "run: ext -> { aggregate: n is count(), big_n is count() { where: big }, total_text is amt.sum()::string }";

interface Mechanism {
   label: string;
   model: (raw: string) => string;
   /** The destination the extension's table is in. */
   destination: string;
}

const MECHANISMS: Mechanism[] = [
   {
      label: "a storage= extension of a database parent",
      model: (raw) => `##! experimental { persistence }
source: raw is warehouse.table('${raw}')
#(materialize)
source: a is raw extend { dimension: cat2 is cat }
#(materialize) storage="lake"
source: ext is a extend { dimension: big is amt > 5 }
`,
      destination: "lake",
   },
   {
      label: "a storage= extension of a parent in another destination",
      model: (raw) => `##! experimental { persistence }
source: raw is warehouse.table('${raw}')
#(materialize) storage="lake"
source: a is raw extend { dimension: cat2 is cat }
#(materialize) storage="other"
source: ext is a extend { dimension: big is amt > 5 }
`,
      destination: "other",
   },
];

interface Tier {
   label: string;
   raw: string;
   /** The warehouse and the two destinations, and how to drop what they made. */
   connections: (root: string) => Promise<{
      warehouse: unknown;
      destinations: unknown[];
      drop: () => Promise<void>;
   }>;
}

/**
 * A Postgres warehouse (a DuckDB connection cannot be read through to a
 * `storage=` build), with the two destinations as DuckLake catalogs on the
 * same server.
 */
function postgresTier(label: string): Tier {
   return {
      label,
      raw: "public.ect_raw",
      connections: async (root: string) => {
         const token = `${process.pid}_${Date.now()}`;
         const dbs = [
            `ect_wh_${token}`,
            `ect_lake_${token}`,
            `ect_other_${token}`,
         ];
         const admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         for (const db of dbs) await admin.runSQL(`CREATE DATABASE ${db}`);
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const destination = (name: string, db: string) => {
            const bucket = path.join(root, `bucket_${name}`);
            fs.mkdirSync(bucket);
            return {
               name,
               type: "ducklake",
               ducklakeConnection: {
                  catalog: { postgresConnection: { ...pg, databaseName: db } },
                  storage: { bucketUrl: bucket },
               },
            };
         };
         return {
            warehouse: {
               name: "warehouse",
               type: "postgres",
               postgresConnection: { ...pg, databaseName: dbs[0] },
            },
            destinations: [
               destination("lake", dbs[1]),
               destination("other", dbs[2]),
            ],
            drop: async () => {
               for (const db of dbs) {
                  await admin
                     .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                     .catch(() => undefined);
               }
               await admin.close().catch(() => undefined);
            },
         };
      },
   };
}

/** DuckLake is the one `storage=` destination type a deployment configures. */
const TIERS: Tier[] = POSTGRES_URL
   ? [postgresTier("Postgres, DuckLake destinations")]
   : [];

interface Entry {
   sourceName?: string;
   sourceEntityId: string;
   physicalTableName: string;
   storageDestinationName?: string;
   upstreamReuse?: string;
   builtAt?: string;
   builtFrom?: {
      sourceEntityId: string;
      physicalTableName: string;
      builtAt?: string;
   }[];
}

const entriesOf = (m: Materialization): Entry[] =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> }).entries ?? {},
   );

for (const tier of TIERS) {
   for (const mechanism of MECHANISMS) {
      describe(`an extension builds from its parent's table with its raw rows gone (${mechanism.label}, ${tier.label})`, () => {
         let restore: () => void;
         let root: string;
         let db: DuckDBConnection;
         let repo: ResourceRepository;
         let env: Environment;
         let service: MaterializationService;
         let drop: () => Promise<void> = async () => {};
         let first: Materialization;
         let second: Materialization;
         let liveBefore: unknown;
         let served: { rows: unknown; kind?: string };

         const ask = async (query: string) => {
            const pkg = await env.getPackage(parsePackageName(PKG), false);
            const result = await pkg
               .getModel("m.malloy")!
               .runQuery(
                  unrecordedAudit(),
                  { query } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
            return {
               rows: JSON.parse(
                  JSON.stringify(result.compactResult),
               ) as unknown,
               kind: result.path?.servedBy?.kind,
            };
         };
         const build = async (opts: object): Promise<Materialization> => {
            const created = await service.createMaterialization(ENV, PKG, opts);
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            const deadline = Date.now() + 120_000;
            for (;;) {
               const m = await repo.getMaterializationById(created.id);
               if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
                  while (running.has(created.id)) {
                     await new Promise((r) => setTimeout(r, 10));
                  }
                  return (await repo.getMaterializationById(created.id)) ?? m;
               }
               if (Date.now() > deadline) throw new Error("run never settled");
               await new Promise((r) => setTimeout(r, 20));
            }
         };

         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
               },
            }).restore;
            let envPath: string;
            ({ root, envPath, db, repo } = await openTempStorage("ect-", {
               id: "env-1",
               name: ENV,
            }));
            writePackageFiles(
               path.join(envPath, PKG),
               { "m.malloy": mechanism.model(tier.raw) },
               { manifest: { name: PKG, description: "fixture" } },
            );
            const made = await tier.connections(root);
            drop = made.drop;
            env = await Environment.create(
               ENV,
               envPath,
               [made.warehouse as never],
               made.destinations as never[],
            );
            const warehouse = await env.getMalloyConnection("warehouse");
            await warehouse.runSQL(`CREATE TABLE ${tier.raw} AS ${RAW_ROWS}`);
            await env.addPackage(PKG);
            service = serviceOverEnvironment(
               () => repo,
               () => env,
            );
            liveBefore = (await ask(QUERY)).rows;
            first = await build({ sourceNames: ["a"] });
            // Out of band: the raw rows are gone. An extension that recomputed
            // its parent from raw would hold no rows.
            await warehouse.runSQL(`DELETE FROM ${tier.raw}`);
            second = await build({});
            served = await ask(QUERY);
         }, 300_000);
         afterAll(async () => {
            await env?.closeAllConnections().catch(() => undefined);
            await db?.close();
            await drop();
            if (root) fs.rmSync(root, { recursive: true, force: true });
            restore?.();
         });

         it("builds the extension into its destination, reusing its parent's table, named in builtFrom", () => {
            expect([first.status, first.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            expect([second.status, second.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            const parent = entriesOf(first).find((e) => e.sourceName === "a")!;
            const ext = entriesOf(second).find((e) => e.sourceName === "ext")!;
            expect(ext.storageDestinationName).toBe(mechanism.destination);
            expect(ext.upstreamReuse).toBe("reused");
            expect(
               (ext.builtFrom ?? []).map((b) => [
                  b.sourceEntityId,
                  b.physicalTableName,
                  b.builtAt,
               ]),
            ).toEqual([
               [
                  parent.sourceEntityId,
                  parent.physicalTableName,
                  parent.builtAt,
               ],
            ]);
         });

         it("serves its table, equal to live before the delete", () => {
            expect(served.kind).not.toBe("live");
            expect(served.rows).toEqual(liveBefore);
            expect(served.rows).not.toEqual([
               { n: 0, big_n: 0, total_text: null },
            ]);
         });
      });
   }
}
