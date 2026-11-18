// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rollup conformance on a Postgres origin, on both of a `#(rollup)`'s
 * tiers: database (the rollup built in Postgres, beside its base) and
 * `storage=` (built into a DuckLake destination on a Postgres catalog). The
 * shapes are conformance/rollup_postgres_shapes.ts, rendered per tier by the
 * conformance fixture's `renderFiles`; the package goes through the real
 * production path (Environment, Package, MaterializationService's build and
 * bind), as database_table_alter_at_bind does.
 *
 * Each query is answered live before the build, then served after it. The
 * served answer must equal live's, by R4's comparison and the strict one
 * (`answer_difference.ts`), and must have been answered as the shape says:
 * from a rollup, in the tier's `servedFrom`, or declined and answered live.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import {
   type Row,
   rowDifference,
   strictRowDifference,
} from "../../../src/test_helpers/conformance/answer_difference";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../src/test_helpers/temp_storage";
import {
   PG_RAW_MODEL,
   PG_SEED,
   ROLLUP_POSTGRES_SHAPES,
} from "./conformance/rollup_postgres_shapes";
import {
   type ConformanceQuery,
   type Expect,
   renderFiles,
   type Variant,
} from "./conformance/shapes";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

setDefaultTimeout(600_000);

/** The two tiers, as the conformance fixture's variants name them. */
const TIERS: { label: string; variant: Variant; servedFrom: string }[] = [
   {
      label: "database",
      variant: "materialize_database",
      servedFrom: "database",
   },
   {
      label: "storage= (DuckLake)",
      variant: "materialize",
      servedFrom: "storage",
   },
];

interface Answer {
   rows: Row[];
   servedBy?: string;
   servedFrom: string | null;
   /** Why each rollup the router considered was not read. */
   declined: string[];
}

type Outcome = { answer: Answer } | { error: string };

function expectedOf(q: ConformanceQuery, variant: Variant): Expect {
   return typeof q.expect === "string"
      ? q.expect
      : (q.expect?.[variant] ?? "rollup");
}

describe.skipIf(!POSTGRES_URL)("rollup parity on a Postgres origin", () => {
   for (const tier of TIERS) {
      const shapes = ROLLUP_POSTGRES_SHAPES.filter(
         (s) => !s.variants || s.variants.includes(tier.variant),
      );
      describe(`[${tier.label}]`, () => {
         const token = Math.random().toString(36).slice(2, 8);
         const warehouse = `rollup_pg_wh_${token}`;
         const catalog = `rollup_pg_lake_${token}`;
         const storage = tier.variant === "materialize";
         let admin: PostgresConnection;
         let root: string;
         let db: DuckDBConnection;
         let repo: ResourceRepository;
         let env: Environment | undefined;
         let service: MaterializationService;
         let restore = () => {};
         let run: Materialization | undefined;
         let warnings: { code?: string; message?: string }[] = [];
         const live = new Map<ConformanceQuery, Outcome>();
         const served = new Map<ConformanceQuery, Outcome>();

         async function ask(q: ConformanceQuery): Promise<Outcome> {
            const pkg = await env!.getPackage(parsePackageName(PKG), false);
            const model = pkg.getModel(q.model!);
            if (!model) return { error: `${q.model} did not load` };
            try {
               const result = await model.runQuery(
                  unrecordedAudit(),
                  { query: q.malloy } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
               return {
                  answer: {
                     rows: result.compactResult as unknown as Row[],
                     servedBy: result.path?.servedBy?.kind,
                     servedFrom: result.servedFrom ?? null,
                     declined: (result.path?.considered ?? [])
                        .filter((c) => c.kind === "rollup" && c.reason)
                        .map((c) => String(c.reason)),
                  },
               };
            } catch (error) {
               return {
                  error: error instanceof Error ? error.message : String(error),
               };
            }
         }

         /** One run of the package, to its settled record. */
         async function build(): Promise<Materialization> {
            const created = await service.createMaterialization(ENV, PKG, {});
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            const deadline = Date.now() + 300_000;
            for (;;) {
               const m = await repo.getMaterializationById(created.id);
               if (
                  m &&
                  ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(
                     m.status,
                  )
               ) {
                  while (running.has(created.id)) {
                     await new Promise((resolve) => setTimeout(resolve, 10));
                  }
                  return (await repo.getMaterializationById(created.id)) ?? m;
               }
               if (Date.now() > deadline) throw new Error("run never settled");
               await new Promise((resolve) => setTimeout(resolve, 50));
            }
         }

         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            }).restore;
            await freshLoadPool();
            ({ root, db, repo } = await openTempStorage("rollup-pg-", {
               id: ENV_ID,
               name: ENV,
            }));
            admin = new PostgresConnection({
               name: "admin",
               connectionString: POSTGRES_URL!,
            });
            await admin.runSQL(`CREATE DATABASE ${warehouse}`);
            if (storage) await admin.runSQL(`CREATE DATABASE ${catalog}`);
            const parsed = new URL(POSTGRES_URL!);
            const pg = {
               host: parsed.hostname,
               port: Number(parsed.port || 5432),
               userName: decodeURIComponent(parsed.username),
               password: decodeURIComponent(parsed.password),
            };
            const seed = new PostgresConnection({
               name: "seed",
               connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
            });
            for (const sql of PG_SEED) await seed.runSQL(sql);
            await seed.close();

            const files: Record<string, string> = {};
            for (const shape of shapes) {
               Object.assign(files, renderFiles(shape, tier.variant));
            }
            files["raw.malloy"] = PG_RAW_MODEL;
            const envPath = path.join(root, "env");
            writePackageFiles(path.join(envPath, PKG), files, {
               manifest: { name: PKG, description: "rollup parity" },
            });
            const bucket = path.join(root, "bucket");
            fs.mkdirSync(bucket);
            env = await Environment.create(
               ENV,
               envPath,
               [
                  {
                     name: "pg",
                     type: "postgres",
                     postgresConnection: { ...pg, databaseName: warehouse },
                  } as never,
               ],
               storage
                  ? [
                       {
                          name: "lake",
                          type: "ducklake",
                          ducklakeConnection: {
                             catalog: {
                                postgresConnection: {
                                   ...pg,
                                   databaseName: catalog,
                                },
                             },
                             storage: { bucketUrl: bucket },
                          },
                       } as never,
                    ]
                  : [],
            );
            await env.addPackage(PKG);
            service = serviceOverEnvironment(
               () => repo,
               () => env!,
            );
            for (const shape of shapes) {
               for (const q of shape.queries) live.set(q, await ask(q));
            }
            run = await build();
            const pkg = await env.getPackage(parsePackageName(PKG), false);
            warnings = (pkg.getPackageMetadata().warnings ?? []).map((w) => ({
               code: (w as { code?: string }).code,
               message: (w as { message?: string }).message,
            }));
            for (const shape of shapes) {
               for (const q of shape.queries) served.set(q, await ask(q));
            }
         });

         afterAll(async () => {
            await env?.closeAllConnections().catch(() => undefined);
            if (db) await disposeTempStorage({ db, root });
            for (const name of [warehouse, catalog]) {
               await admin
                  ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
                  .catch(() => undefined);
            }
            await admin?.close().catch(() => undefined);
            restore();
            await freshLoadPool();
         });

         it("builds every rollup, with no failure and no rollup refused", () => {
            expect([run?.status, run?.error ?? null]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            const manifest = run?.manifest as {
               entries: Record<string, { origin?: string }>;
               failures?: Record<string, unknown>;
            };
            expect(manifest.failures ?? {}).toEqual({});
            expect(
               Object.values(manifest.entries).filter(
                  (e) => e.origin === "rollup",
               ).length,
            ).toBeGreaterThan(0);
            expect(
               warnings.filter((w) => w.code?.startsWith("rollup-")),
            ).toEqual([]);
         });

         for (const shape of shapes) {
            describe(shape.name, () => {
               for (const q of shape.queries) {
                  const want = expectedOf(q, tier.variant);
                  it(`${q.name}: ${want === "rollup" ? "served from the rollup" : "declined, answered live"}, equal to live`, () => {
                     const l = live.get(q)!;
                     const s = served.get(q)!;
                     if ("error" in l)
                        throw new Error(`live failed: ${l.error}`);
                     if ("error" in s) {
                        throw new Error(`served failed: ${s.error}`);
                     }
                     expect(l.answer.servedBy ?? "live").toBe("live");
                     expect(l.answer.rows.length).toBeGreaterThan(0);
                     expect(
                        rowDifference(l.answer.rows, s.answer.rows),
                     ).toBeUndefined();
                     expect(
                        strictRowDifference(l.answer.rows, s.answer.rows),
                     ).toBeUndefined();
                     if (want === "rollup") {
                        expect(s.answer.servedBy).toBe("rollup");
                        expect(s.answer.servedFrom).toBe(tier.servedFrom);
                     } else {
                        expect(s.answer.servedBy ?? "live").toBe("live");
                        expect(s.answer.declined).toContain(q.reason!);
                     }
                  });
               }
            });
         }
      });
   }
});
