// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A chained database build over a stored upstream whose table was altered in
 * its warehouse after its build (`ALTER TABLE … TYPE DECIMAL(10,0)`, which
 * rounds `2.4` to `2`) never reads that table, and never recomputes the
 * upstream from raw either. The first run's downstream (a `#@ persist` over
 * it, a `#(rollup)` over it) fails with SourceFailure code
 * `schema_incompatible`, naming the column, and reports the table to the
 * drift handler; the next run rebuilds the upstream rather than carry that
 * generation again, and builds the downstream over the rebuilt table
 * (`upstreamReuse: reused`, `builtFrom` naming its new build), served equal
 * to live as text. DuckDB, and Postgres when `PUBLISHER_TEST_POSTGRES_URL`
 * names one.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const RAW = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(9.0 AS DECIMAL(10,1))), ('a', CAST(10.0 AS DECIMAL(10,1))),
    ('b', CAST(2.4 AS DECIMAL(10,1))), ('b', CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(cat, amt)
""")

#@ persist name="cd_a"
source: a is raw -> { select: cat, amt }

source: b_live is raw -> { group_by: cat; aggregate: total is amt.sum() }
`;

/** Each total as the warehouse renders it, so rows compare as text. */
const AS_TEXT =
   "-> { group_by: cat, total_text is total::string; order_by: cat }";

/** The same answer from a source that reads no table. */
const LIVE = `run: b_live ${AS_TEXT}`;

/** Each downstream of \`a\`, one package each, and its query. */
const MECHANISMS = [
   {
      label: "#@ persist",
      model: `${RAW}
#@ persist name="cd_b"
source: b is a -> { group_by: cat; aggregate: total is amt.sum() }
`,
      query: `run: b ${AS_TEXT}`,
      servedBy: "materialized",
   },
   {
      label: "#(rollup)",
      model: `${RAW}
source: rolled is a extend {
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      query: `run: rolled -> { group_by: cat; aggregate: total } ${AS_TEXT}`,
      servedBy: "rollup",
   },
] as const;

interface Answer {
   rows: unknown;
   kind?: string;
}

const tiers = [
   {
      label: "DuckDB",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         } as unknown,
         drop: undefined as undefined | (() => Promise<void>),
      }),
      alter: (table: string) =>
         `ALTER TABLE ${table} ALTER amt TYPE DECIMAL(10,0)`,
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `cd_drift_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 await admin.runSQL(`CREATE DATABASE ${db}`);
                 const parsed = new URL(POSTGRES_URL);
                 return {
                    config: {
                       name: "warehouse",
                       type: "postgres",
                       postgresConnection: {
                          host: parsed.hostname,
                          port: Number(parsed.port || 5432),
                          userName: decodeURIComponent(parsed.username),
                          password: decodeURIComponent(parsed.password),
                          databaseName: db,
                       },
                    } as unknown,
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
              alter: (table: string) =>
                 `ALTER TABLE ${table} ALTER COLUMN amt TYPE NUMERIC(10,0)`,
           },
        ]
      : []),
];

for (const [tier, mechanism] of tiers.flatMap((t) =>
   MECHANISMS.map((m) => [t, m] as const),
)) {
   describe(`a chained build over an upstream table altered since its build fails once, then builds over the rebuilt table (${mechanism.label}, ${tier.label})`, () => {
      let restore: () => void;
      let root: string;
      let env: Environment;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let service: MaterializationService;
      let pkg: Package;
      let drop: (() => Promise<void>) | undefined;
      let first: Materialization;
      let failing: Materialization;
      let second: Materialization;
      let served: Answer;
      let live: Answer;

      const ask = async (query: string): Promise<Answer> => {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: JSON.parse(JSON.stringify(result.compactResult)),
            kind: (result.path as { servedBy?: { kind?: string } } | undefined)
               ?.servedBy?.kind,
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
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "cd-")));
         const envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": mechanism.model },
            { manifest: { name: PKG, description: "fixture" } },
         );
         db = new DuckDBConnection(":memory:");
         await db.initialize();
         await initializeSchema(db);
         await db.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            ["env-1", ENV, envPath],
         );
         repo = createDuckDBRepository(db);
         const w = await tier.warehouse();
         drop = w.drop;
         env = await Environment.create(ENV, envPath, [w.config as never], []);
         await env.addPackage(PKG);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         // `a` alone first, so every downstream builds after the alter.
         first = await build({ sourceNames: ["a"] });
         const entries = Object.values(
            (
               first.manifest as {
                  entries: Record<string, { physicalTableName: string }>;
               }
            ).entries,
         );
         // Out of band: the warehouse's own ALTER of `a`'s table.
         const warehouse = await env.getMalloyConnection("warehouse");
         for (const e of entries) {
            await warehouse.runSQL(tier.alter(e.physicalTableName));
         }
         pkg = await env.getPackage(parsePackageName(PKG), true, {
            refreshSchemas: true,
         });
         failing = await build({});
         second = await build({});

         served = await ask(mechanism.query);
         live = await ask(LIVE);
      }, 300_000);
      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         await drop?.();
         fs.rmSync(root, { recursive: true, force: true });
         restore();
         await freshLoadPool();
      });
      it("built only the upstream before the alter", () => {
         expect(first.status).toBe("MANIFEST_FILE_READY");
         expect(
            Object.values(
               (first.manifest as { entries: Record<string, unknown> }).entries,
            ),
         ).toHaveLength(1);
      });

      it("fails the downstream that would read the altered table, naming the column", () => {
         expect(failing.status).toBe("FAILED");
         const failures = Object.values(
            (
               failing.manifest as {
                  failures?: Record<string, { reason: string; code?: string }>;
               }
            ).failures ?? {},
         );
         expect(failures).toHaveLength(1);
         expect(failures[0].code).toBe("schema_incompatible");
         expect(failures[0].reason).toMatch(
            /persisted upstream '\w+' was not read from its table, since its column 'amt' was built as (DECIMAL|numeric)\(10,1\) and is now (DECIMAL|numeric)\(10,0\)/,
         );
      });

      it("rebuilds the upstream next, and builds the downstream over its new table", () => {
         expect(second.status).toBe("MANIFEST_FILE_READY");
         type Entry = {
            sourceName?: string;
            builtAt?: string;
            physicalTableName: string;
            upstreamReuse?: string;
            builtFrom?: { physicalTableName: string; builtAt?: string }[];
         };
         const entries = Object.values(
            (second.manifest as { entries: Record<string, Entry> }).entries,
         );
         const before = Object.values(
            (first.manifest as { entries: Record<string, Entry> }).entries,
         )[0];
         const upstream = entries.find(
            (e) => e.physicalTableName === before.physicalTableName,
         );
         expect(upstream?.builtAt).toBeDefined();
         expect(upstream?.builtAt).not.toBe(before.builtAt);
         const downstream = entries.filter((e) => e !== upstream);
         expect(downstream).toHaveLength(1);
         expect(downstream[0].upstreamReuse).toBe("reused");
         expect(downstream[0].builtFrom).toEqual([
            expect.objectContaining({
               physicalTableName: upstream!.physicalTableName,
               builtAt: upstream!.builtAt,
            }),
         ]);
      });

      it("serves its table, equal to live as text", () => {
         // A Postgres rollup's table at an unqualified name captures no
         // columns after its build, so the bind answers it live either way.
         if (!(mechanism.servedBy === "rollup" && tier.label === "Postgres")) {
            expect(served.kind).toBe(mechanism.servedBy);
         }
         expect(served.rows).toEqual(live.rows);
         expect(served.rows).toEqual([
            { cat: "a", total_text: "19.0" },
            { cat: "b", total_text: "4.8" },
         ]);
      });
   });
}
