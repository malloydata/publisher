// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` downstream is built from its upstream's stored table, never by
 * recomputing the upstream from raw: the drop-raw proof, for a downstream in
 * a DuckLake destination. The upstream is built alone; its raw rows are then
 * deleted out of band; the downstream, built next, still builds, reports
 * `upstreamReuse: "reused"`, names the upstream's table in `builtFrom`, and
 * answers as live answered before the delete.
 *
 * Each query orders by a number: an order by a string the warehouse compares
 * under its collation is answered live, by design.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse and the
 * DuckLake catalog.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

const RAW_ROWS = `SELECT * FROM (VALUES
    ('a', 'x', 9.0::DOUBLE PRECISION, 1.5::NUMERIC(10,1)),
    ('a', 'y', 10.5::DOUBLE PRECISION, 2.5::NUMERIC(10,1)),
    ('b', 'x', 2.5::DOUBLE PRECISION, 4.0::NUMERIC(10,1))
  ) AS t(cat, sub, amt, price)`;

const HEAD = `##! experimental.persistence
source: raw is warehouse.table('public.sdr_raw')
`;

interface Mechanism {
   label: string;
   model: string;
   /** Built alone first. */
   upstream: string;
   /** The downstream's entry's `sourceName`. */
   downstream: RegExp;
   query: string;
}

const DATABASE_BASE = `#@ persist name="public.sdr_base"
source: base is raw -> { select: cat, sub, amt }
`;

const STORAGE_BASE = `#@ persist storage="lake" name="sdr_lake_base"
source: base is raw -> { select: cat, sub, amt }
`;

const MECHANISMS: Mechanism[] = [
   {
      label: "storage= view over a #@ persist in its destination",
      model: `${HEAD}${STORAGE_BASE}
source: s is base extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      upstream: "base",
      downstream: /^s__by_cat__view$/,
      query: "run: s -> by_cat -> { select: *; order_by: total }",
   },
   {
      label: "storage= query over a #@ persist in its destination",
      model: `${HEAD}${STORAGE_BASE}
#@ persist storage="lake"
query: q is base -> { group_by: sub; aggregate: total is amt.sum() }
`,
      upstream: "base",
      downstream: /^q__query$/,
      query: "run: q -> { select: *; order_by: total }",
   },
   {
      label: "storage= view over a database #@ persist",
      model: `${HEAD}${DATABASE_BASE}
source: s is base extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      upstream: "base",
      downstream: /^s__by_cat__view$/,
      query: "run: s -> by_cat -> { select: *; order_by: total }",
   },
   {
      label: "storage= query over a database #@ persist",
      model: `${HEAD}${DATABASE_BASE}
#@ persist storage="lake"
query: q is base -> { group_by: sub; aggregate: total is amt.sum() }
`,
      upstream: "base",
      downstream: /^q__query$/,
      query: "run: q -> { select: *; order_by: total }",
   },
   {
      label: "storage= rollup over a database #@ persist",
      model: `${HEAD}${DATABASE_BASE}
source: rolled is base extend {
  #@ preaggregate grain="cat" storage=lake
  measure: total is amt.sum()
}
`,
      upstream: "base",
      downstream: /^rolled__rollup__/,
      query: "run: rolled -> { group_by: cat; aggregate: total; order_by: total }",
   },
];

interface Entry {
   sourceName?: string;
   sourceEntityId: string;
   physicalTableName: string;
   storageDestinationName?: string;
   upstreamReuse?: string;
   error?: string;
   builtFrom?: { sourceEntityId: string; physicalTableName: string }[];
}

const entriesOf = (m: Materialization): Entry[] =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> }).entries ?? {},
   );

/**
 * A package over the warehouse and a DuckLake destination, both databases on
 * the Postgres server, made in `setUp` and dropped in `tearDown`.
 */
function lakeFixture(model: string) {
   const token = Math.random().toString(36).slice(2, 8);
   const catalog = `sdr_lake_${token}`;
   const warehouseDb = `sdr_wh_${token}`;
   const f = {
      admin: undefined as PostgresConnection | undefined,
      root: "",
      db: undefined as DuckDBConnection | undefined,
      repo: undefined as unknown as ResourceRepository,
      env: undefined as unknown as Environment,
      service: undefined as unknown as MaterializationService,
      restore: (() => {}) as () => void,
      async ask(query: string) {
         const pkg = await f.env.getPackage(parsePackageName(PKG), false);
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: JSON.parse(JSON.stringify(result.compactResult)) as unknown,
            kind: result.path?.servedBy?.kind,
         };
      },
      async build(opts: object): Promise<Materialization> {
         const created = await f.service.createMaterialization(ENV, PKG, opts);
         const running = (
            f.service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await f.repo.getMaterializationById(created.id);
            if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
               while (running.has(created.id)) {
                  await new Promise((r) => setTimeout(r, 10));
               }
               return (await f.repo.getMaterializationById(created.id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 20));
         }
      },
      warehouse: () => f.env.getMalloyConnection("warehouse"),
      async setUp() {
         f.restore = installSettings({
            env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
         }).restore;
         let envPath: string;
         let db: DuckDBConnection;
         ({
            root: f.root,
            envPath,
            db,
            repo: f.repo,
         } = await openTempStorage("sdr-", { id: ENV_ID, name: ENV }));
         f.db = db;
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": model },
            { manifest: { name: PKG, description: "fixture" } },
         );
         f.admin = new PostgresConnection({
            name: "admin",
            connectionString: url!,
         });
         await f.admin.runSQL(`CREATE DATABASE ${catalog}`);
         await f.admin.runSQL(`CREATE DATABASE ${warehouseDb}`);
         const parsed = new URL(url!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const bucket = path.join(f.root, "bucket");
         fs.mkdirSync(bucket);
         f.env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "postgres",
                  postgresConnection: { ...pg, databaseName: warehouseDb },
               } as never,
            ],
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: { ...pg, databaseName: catalog },
                     },
                     storage: { bucketUrl: bucket },
                  },
               } as never,
            ],
         );
         await (
            await f.warehouse()
         ).runSQL(`CREATE TABLE public.sdr_raw AS ${RAW_ROWS}`);
         await f.env.addPackage(PKG);
         f.service = serviceOverEnvironment(
            () => f.repo,
            () => f.env,
         );
      },
      async tearDown() {
         await f.env?.closeAllConnections().catch(() => undefined);
         await f.db?.close();
         for (const name of [catalog, warehouseDb]) {
            await f.admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await f.admin?.close().catch(() => undefined);
         if (f.root) fs.rmSync(f.root, { recursive: true, force: true });
         f.restore();
      },
   };
   return f;
}

for (const mechanism of MECHANISMS) {
   describe.skipIf(!url)(
      `a storage= downstream builds from its upstream's table with its raw rows gone (${mechanism.label})`,
      () => {
         const f = lakeFixture(mechanism.model);
         let first: Materialization;
         let second: Materialization;
         let liveBefore: unknown;
         let served: { rows: unknown; kind?: string };

         beforeAll(async () => {
            await f.setUp();
            liveBefore = (await f.ask(mechanism.query)).rows;
            first = await f.build({ sourceNames: [mechanism.upstream] });
            // Out of band: the raw rows are gone. A downstream that
            // recomputed its upstream from raw would hold no rows.
            await (await f.warehouse()).runSQL("DELETE FROM public.sdr_raw");
            second = await f.build({});
            served = await f.ask(mechanism.query);
         }, 300_000);
         afterAll(() => f.tearDown());

         it("builds the downstream into the destination, reusing its upstream's table, named in builtFrom", () => {
            expect([first.status, first.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            expect(second.status).toBe("MANIFEST_FILE_READY");
            const upstream = entriesOf(first).find(
               (e) => e.sourceName === mechanism.upstream,
            )!;
            const downstream = entriesOf(second).find((e) =>
               mechanism.downstream.test(e.sourceName ?? ""),
            )!;
            expect(downstream.error).toBeUndefined();
            expect(downstream.storageDestinationName).toBe("lake");
            expect(downstream.upstreamReuse).toBe("reused");
            expect(
               (downstream.builtFrom ?? []).map((b) => b.sourceEntityId),
            ).toEqual([upstream.sourceEntityId]);
         });

         it("serves its table, equal to live before the delete", () => {
            expect(served.kind).not.toBe("live");
            expect(served.rows).toEqual(liveBefore);
         });
      },
   );
}

// The plan stands in for an upstream's table the types its build writes by
// default (a number as DOUBLE); this one's is DECIMAL(10,1). The first run
// builds the upstream and finds it is not what the view's plan was proven
// over: `schema_incompatible`, and the plan is derived again from the bound
// table. The next run proves the view over the real type and builds it: never
// `schema_incompatible` twice for one table generation.
describe.skipIf(!url)(
   "a storage= view over an upstream whose stored type is not the plan's default",
   () => {
      const QUERY = "run: s -> by_cat -> { select: *; order_by: total }";
      const f = lakeFixture(`${HEAD}#@ persist storage="lake" name="sdr_priced"
source: base is raw -> { select: cat, price }
source: s is base extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total is price.sum() }
}
`);
      let first: Materialization;
      let second: Materialization;
      let live: unknown;
      beforeAll(async () => {
         await f.setUp();
         live = (await f.ask(QUERY)).rows;
         first = await f.build({});
         second = await f.build({});
      }, 300_000);
      afterAll(() => f.tearDown());

      const failureOf = (m: Materialization) =>
         Object.values(
            (
               m.manifest as {
                  failures?: Record<
                     string,
                     { sourceName?: string; code?: string; retryable?: boolean }
                  >;
               }
            ).failures ?? {},
         ).find((x) => x.sourceName === "s__by_cat__view");

      it("fails the view as schema_incompatible, non-terminal, in the run that builds its upstream", () => {
         expect(failureOf(first)).toMatchObject({
            code: "schema_incompatible",
            retryable: true,
         });
      });

      it("builds it in the next run, over the upstream's table, and serves it equal to live", async () => {
         expect(failureOf(second)).toBeUndefined();
         const view = entriesOf(second).find(
            (e) => e.sourceName === "s__by_cat__view",
         )!;
         expect(view.error).toBeUndefined();
         expect(view.upstreamReuse).toBe("reused");
         const served = await f.ask(QUERY);
         expect(served.kind).not.toBe("live");
         expect(served.rows).toEqual(live);
      });
   },
);
