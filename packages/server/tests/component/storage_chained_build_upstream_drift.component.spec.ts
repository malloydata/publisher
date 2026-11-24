// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `storage=` variants of chained_build_upstream_drift: a chained build
 * over a stored upstream whose table was altered in its destination after
 * its build (its `amt` column dropped and added back as text, out of band)
 * never builds from that table's new shape, and never recomputes the
 * upstream from raw.
 *
 * - A host-instructed build of the downstream alone, with the upstream by
 *   reference, is refused before it reaches the destination's engine:
 *   SourceFailure code `schema_incompatible`, naming the column, never the
 *   engine's own binder error.
 * - The next standalone run (on a package loaded again) holds the
 *   upstream's table to its contract, rebuilds it rather than carry it, and
 *   builds the downstream over the rebuilt table, served equal to live; the
 *   run after it carries both.
 *
 * - With every raw row then deleted, a forced rebuild of the downstream
 *   alone builds it over the carried upstream table, still equal to what
 *   live answered before.
 *
 * `#@ persist` over `#@ persist`, a `#(materialize)` extension of a
 * `#(materialize)` parent, a `#(rollup)` of a `#(materialize)` base, and a
 * `#(rollup)` on an `extend` of a `#@ persist` query source (`->`), each in
 * one DuckLake destination. Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the
 * warehouse and the DuckLake catalog.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
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
    (1, CAST(9.0 AS DECIMAL(10,1))), (1, CAST(10.0 AS DECIMAL(10,1))),
    (2, CAST(2.4 AS DECIMAL(10,1))), (2, CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(k, amt)`;

const HEAD = `##! experimental { persistence }
source: raw is warehouse.table('public.cs_raw')

source: b_live is raw -> { group_by: k; aggregate: total is amt.sum() }
`;

/**
 * Grouped by a number, never a string: a Postgres warehouse compares strings
 * under its database's collation and a destination byte-exact, so a query
 * ordering a string answers live and would prove nothing here.
 */
const AS_TEXT = "-> { group_by: k, total_text is total::string; order_by: k }";
const LIVE = `run: b_live ${AS_TEXT}`;

const MECHANISMS = [
   {
      label: "#@ persist over #@ persist",
      cell: "l-drift-build ps",
      model: `${HEAD}
#@ persist storage="lake"
source: a is raw -> { select: k, amt }

#@ persist storage="lake"
source: b is a -> { group_by: k; aggregate: total is amt.sum() }
`,
      downstream: /^b$/,
      query: `run: b ${AS_TEXT}`,
   },
   {
      label: "#(materialize) extension of a #(materialize) parent",
      cell: "l-drift-build ms",
      model: `${HEAD}
#(materialize) storage="lake"
source: a is raw extend { dimension: k2 is k }

#(materialize) storage="lake"
source: ext is a extend { dimension: big is amt > 5 }
`,
      downstream: /^ext$/,
      query: `run: ext -> { group_by: k; aggregate: total is amt.sum() } ${AS_TEXT}`,
   },
   {
      label: "#(rollup) over a #(materialize) base",
      cell: "l-drift-build rs",
      model: `${HEAD}
#(materialize) storage="lake"
source: a is raw extend {
  dimension: k2 is k
  #(rollup) grain=["k"] storage="lake"
  measure: total is amt.sum()
}
`,
      downstream: /__rollup__/,
      query: `run: a -> { group_by: k; aggregate: total } ${AS_TEXT}`,
   },
   {
      label: "#(rollup) on an extend of a #@ persist query source",
      cell: "l-drift-build rs",
      model: `${HEAD}
#@ persist storage="lake"
source: a is raw -> { select: k, amt }

source: rolled is a extend {
  #(rollup) grain=["k"] storage="lake"
  measure: total is amt.sum()
}
`,
      downstream: /^rolled__rollup__/,
      query: `run: rolled -> { group_by: k; aggregate: total } ${AS_TEXT}`,
   },
];

interface Entry {
   sourceName?: string;
   sourceEntityId: string;
   physicalTableName: string;
   upstreamReuse?: string;
   builtAt?: string;
   nativeSchema?: { name: string; type: string }[];
   builtFrom?: { physicalTableName: string; builtAt?: string }[];
}

const entriesOf = (m: Materialization): Entry[] =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> } | null)?.entries ?? {},
   );

const failuresOf = (
   m: Materialization,
): { sourceName?: string; code?: string; reason?: string }[] =>
   Object.values(
      (
         m.manifest as {
            failures?: Record<
               string,
               { sourceName?: string; code?: string; reason?: string }
            >;
         } | null
      )?.failures ?? {},
   );

const EXPECTED = [
   { k: 1, total_text: "19.0" },
   { k: 2, total_text: "4.8" },
];

for (const mechanism of POSTGRES_URL ? MECHANISMS : []) {
   describe(`a storage= chained build over an upstream table altered since its build is refused, and a standalone run rebuilds the upstream rather than carry it (${mechanism.label}, ${mechanism.cell}; Postgres, DuckLake)`, () => {
      let restore: () => void;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      const dbs: string[] = [];
      let admin: PostgresConnection;
      let first: Materialization;
      let instructed: Materialization;
      let afterAlter: Materialization;
      let second: Materialization;
      let betweenServed: { rows: unknown; kind?: string };
      let served: { rows: unknown; kind?: string };
      let live: { rows: unknown };
      let rawless: Materialization;
      let rawlessServed: { rows: unknown; kind?: string };

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
            rows: JSON.parse(JSON.stringify(result.compactResult)) as unknown,
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
         ({ root, envPath, db, repo } = await openTempStorage("csd-", {
            id: "env-1",
            name: ENV,
         }));
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": mechanism.model },
            { manifest: { name: PKG, description: "fixture" } },
         );
         const token = `${process.pid}_${Date.now()}`;
         dbs.push(`csd_wh_${token}`, `csd_lake_${token}`);
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         for (const name of dbs) await admin.runSQL(`CREATE DATABASE ${name}`);
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const bucket = path.join(root, "bucket_lake");
         fs.mkdirSync(bucket);
         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "postgres",
                  postgresConnection: { ...pg, databaseName: dbs[0] },
               } as never,
            ],
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: { ...pg, databaseName: dbs[1] },
                     },
                     storage: { bucketUrl: bucket },
                  },
               } as never,
            ],
         );
         const warehouse = await env.getMalloyConnection("warehouse");
         await warehouse.runSQL(`CREATE TABLE public.cs_raw AS ${RAW_ROWS}`);
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         // `a` alone first, so every downstream builds after the alter.
         first = await build({ sourceNames: ["a"] });
         // Out of band: the destination's own ALTER of `a`'s table, its
         // `amt` dropped and added back as text (DuckLake narrows no type).
         const lake = new MalloyDuckDBConnection(
            "alter",
            path.join(root, "alter.duckdb"),
         );
         try {
            await lake.runSQL("INSTALL ducklake");
            await lake.runSQL("LOAD ducklake");
            await lake.runSQL("INSTALL postgres");
            await lake.runSQL("LOAD postgres");
            await lake.runSQL(
               `ATTACH 'ducklake:postgres:host=${pg.host} port=${pg.port} dbname=${dbs[1]} user=${pg.userName} password=${pg.password}' AS lk (DATA_PATH '${bucket}')`,
            );
            for (const e of entriesOf(first)) {
               const t = `lk.main."${e.physicalTableName}"`;
               await lake.runSQL(`ALTER TABLE ${t} DROP COLUMN amt`);
               await lake.runSQL(`ALTER TABLE ${t} ADD COLUMN amt VARCHAR`);
               await lake.runSQL(`UPDATE ${t} SET amt = '1'`);
            }
            await lake.runSQL("DETACH lk");
         } finally {
            await lake.close();
         }
         let pkg = await env.getPackage(parsePackageName(PKG), true, {
            refreshSchemas: true,
         });
         // A host instructs the downstream alone, with `a` by reference as
         // its build recorded it.
         const downstream = Object.values(
            pkg.getBuildPlan()?.sources ?? {},
         ).find((s) => mechanism.downstream.test(s.name))!;
         const a = entriesOf(first)[0] as unknown as Record<string, unknown>;
         instructed = await build({
            buildInstructions: [
               {
                  sourceEntityId: downstream.sourceEntityId,
                  sourceID: downstream.sourceID,
                  materializedTableId: "mt-instructed",
                  physicalTableName: "csd_instructed",
                  realization: "COPY",
                  destination: "lake",
               },
            ],
            referenceManifest: [
               {
                  sourceEntityId: a.sourceEntityId,
                  physicalTableName: a.physicalTableName,
                  connectionName: "warehouse",
                  storageDestinationName: a.storageDestinationName,
                  schema: a.schema,
                  contract: a.contract,
                  builtAt: a.builtAt,
               },
            ],
         });
         // Loaded again: what that refusal reported to this package's drift
         // handler is gone, so the run below decides from the table alone.
         pkg = await env.getPackage(parsePackageName(PKG), true, {
            refreshSchemas: true,
         });
         void pkg;
         afterAlter = await build({});
         betweenServed = await ask(mechanism.query);
         second = await build({});
         served = await ask(mechanism.query);
         live = await ask(LIVE);
         // Out of band: every raw row gone. A downstream recomputed from raw
         // would hold none.
         await warehouse.runSQL("DELETE FROM public.cs_raw");
         rawless = await build({
            sourceNames: entriesOf(second)
               .filter((e) => mechanism.downstream.test(e.sourceName ?? ""))
               .map((e) => e.sourceName!),
            forceRefresh: true,
         });
         rawlessServed = await ask(mechanism.query);
      }, 300_000);
      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         for (const name of dbs) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      it("built only the upstream before the alter", () => {
         expect([first.status, first.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(entriesOf(first).map((e) => e.sourceName)).toEqual(["a"]);
      });

      it("refuses a host-instructed downstream over the altered table as schema_incompatible, naming the column", () => {
         expect(instructed.status).toBe("FAILED");
         expect(
            entriesOf(instructed).filter((e) =>
               mechanism.downstream.test(e.sourceName ?? ""),
            ),
         ).toEqual([]);
         const failures = failuresOf(instructed);
         expect(failures).toHaveLength(1);
         expect(failures[0].code).toBe("schema_incompatible");
         expect(failures[0].reason).toContain(
            "was not read from its table, since its column 'amt' was built as DECIMAL(10,1) and is now VARCHAR",
         );
      });

      it("rebuilds the upstream in the next standalone run rather than carry it, and builds the downstream over its new table", () => {
         expect([afterAlter.status, afterAlter.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const before = entriesOf(first)[0];
         const upstream = entriesOf(afterAlter).find(
            (e) => e.sourceName === "a",
         );
         expect(upstream?.builtAt).toBeDefined();
         expect(upstream?.builtAt).not.toBe(before.builtAt);
         const downstream = entriesOf(afterAlter).filter((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         );
         expect(downstream).toHaveLength(1);
         expect(downstream[0].upstreamReuse).toBe("reused");
         expect(
            (downstream[0].builtFrom ?? []).map((b) => [
               b.physicalTableName,
               b.builtAt,
            ]),
         ).toEqual([[upstream!.physicalTableName, upstream!.builtAt]]);
      });

      it("carries both in the run after", () => {
         expect([second.status, second.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
      });

      it("serves its table after each run, equal to live", () => {
         expect(betweenServed.kind).not.toBe("live");
         expect(served.kind).not.toBe("live");
         expect(betweenServed.rows).toEqual(live.rows);
         expect(served.rows).toEqual(live.rows);
         expect(live.rows).toEqual(EXPECTED);
      });

      it("rebuilds the downstream over the carried upstream table with every raw row deleted, and serves it equal to live before", () => {
         expect([rawless.status, rawless.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const was = entriesOf(second).find((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         )!;
         const now = entriesOf(rawless).find((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         )!;
         expect(now.builtAt).not.toBe(was.builtAt);
         expect(now.upstreamReuse).toBe("reused");
         expect(rawlessServed.kind).not.toBe("live");
         expect(rawlessServed.rows).toEqual(EXPECTED);
      });
   });
}
