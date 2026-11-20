// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Campaign P2-22 (control-plane companion R-02): a `#(materialize)` extension
 * declared in a file that imports its base's file is answered from its OWN
 * table, not from its base's with its `where:` applied again.
 *
 * The fixture is the Credible e2e shape on DuckDB: `by_cat` a query-shaped
 * `#(materialize)` source in base.malloy, `by_cat_done is by_cat extend {
 * where: … }` a `#(materialize)` source in other.malloy. It runs twice: with
 * `orders_raw` declared in base.malloy, and with base.malloy importing it from
 * raw.malloy. On dev (0.16.1, BigQuery) the base's file imported another file,
 * and the extension was answered from its base's table every time, its own
 * table bound and never read (serve verdict `served_copy`).
 *
 * The path report can name one table while the run reads another, so each
 * answer is checked three ways: the path report's table, the SQL the
 * connection actually ran, and the rows after each stored table is tampered
 * with a marker row of its own (`zz_by_cat` in by_cat's, `zz_done` in
 * by_cat_done's; both `completed`, so either survives the `where:`).
 *
 * Built twice: through the real MaterializationService (a standalone run that
 * binds its own manifest), and as a host builds and binds (each planned table
 * built from the plan's SQL, then bound from a manifest of `{ tableName,
 * connectionName }` entries), where the host may also bind only some tables.
 */

import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
   spyOn,
} from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import type { ResourceRepository } from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection as StorageDuckDB } from "../../../src/storage/duckdb/DuckDBConnection";
import { __setPackageLoadPoolForTests } from "../../../src/package_load/package_load_pool";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../src/test_helpers/temp_storage";

setDefaultTimeout(120_000);

const ORDERS =
   "id,category,status,amount\n1,a,completed,10.5\n2,a,open,5.25\n3,b,completed,7.5\n4,b,completed,3.25\n5,c,open,1.5\n";
const RAW = `source: orders_raw is duckdb.table('orders.csv') extend {
  measure: revenue is amount.sum()
}
`;
const BY_CAT = `#(materialize)
source: by_cat is orders_raw -> { group_by: category, status; aggregate: revenue }
`;
const OTHER = `##! experimental.persistence
import "base.malloy"
#(materialize)
source: by_cat_done is by_cat extend { where: status = 'completed' }
`;

/** orders_raw declared in base.malloy, or imported into it from raw.malloy. */
const VARIANTS: { label: string; files: Record<string, string> }[] = [
   {
      label: "the base's file imports nothing",
      files: {
         "orders.csv": ORDERS,
         "base.malloy": `##! experimental.persistence\n${RAW}${BY_CAT}`,
         "other.malloy": OTHER,
      },
   },
   {
      label: "the base's file imports another file",
      files: {
         "orders.csv": ORDERS,
         "raw.malloy": RAW,
         "base.malloy": `##! experimental.persistence\nimport "raw.malloy"\n${BY_CAT}`,
         "other.malloy": OTHER,
      },
   },
];

const DONE_QUERY =
   "run: by_cat_done -> { group_by: category; aggregate: n is count(); order_by: category }";
const BASE_QUERY =
   "run: by_cat -> { group_by: category; aggregate: n is count(); order_by: category }";

interface Answer {
   rows: { category: string; n: number }[];
   servedBy?: { kind?: string; tableId?: string; via?: string };
   /** Every statement the package's DuckDB connections ran for the query. */
   sql: string[];
}

/** Run `query` through the model's query path, capturing the SQL executed. */
async function ask(
   pkg: Package,
   modelPath: string,
   query: string,
): Promise<Answer> {
   const spy = spyOn(DuckDBConnection.prototype, "runSQL");
   try {
      const result = await pkg
         .getModel(modelPath)!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: result.compactResult as unknown as Answer["rows"],
         servedBy: result.path?.servedBy as Answer["servedBy"],
         sql: (spy.mock.calls as unknown[][]).map((c) => String(c[0])),
      };
   } finally {
      spy.mockRestore();
   }
}

/** The statements that read `table` (quoted, as Malloy writes it). */
function reads(answer: Answer, table: string): boolean {
   return answer.sql.some(
      (s) => /\bFROM\b/.test(s) && s.includes(`"${table}"`),
   );
}

/** Each planned source's sourceEntityId, by name. */
function entityIds(pkg: Package): Record<string, string> {
   return Object.fromEntries(
      Object.values(pkg.getBuildPlan()?.sources ?? {}).map((e) => [
         e.name,
         e.sourceEntityId,
      ]),
   );
}

async function settle(pkg: Package): Promise<void> {
   for (const modelPath of pkg.getModelPaths()) {
      await pkg.getModel(modelPath)?.prepareServedCopy();
   }
}

/** One marker row in each stored table, so an answer names the table it read. */
async function tamper(
   conn: { runSQL(sql: string): Promise<unknown> },
   byCat: string,
   done: string,
): Promise<void> {
   await conn.runSQL(
      `INSERT INTO "${byCat}" (category, status, revenue) VALUES ('zz_by_cat', 'completed', 0)`,
   );
   await conn.runSQL(
      `INSERT INTO "${done}" (category, status, revenue) VALUES ('zz_done', 'completed', 0)`,
   );
}

function categories(a: Answer): string[] {
   return a.rows.map((r) => r.category);
}

/**
 * Service criteria (1)–(3), and the tamper: by_cat_done is read from its own
 * table, which the SQL names (not by_cat's), the path report names (not
 * by_cat's tableId), and only its own marker row shows.
 */
function expectOwnTable(
   ctx: {
      live: Answer["rows"];
      ids: Record<string, string>;
      tables: Record<string, string>;
   },
   untampered: Answer,
   base: Answer,
   tampered: Answer,
): void {
   const doneTable = ctx.tables[ctx.ids.by_cat_done];
   const baseTable = ctx.tables[ctx.ids.by_cat];
   // (3) rows equal live.
   expect(untampered.rows).toEqual(ctx.live);
   // (1) the SQL executed names by_cat_done's table, not by_cat's.
   expect([reads(untampered, doneTable), reads(untampered, baseTable)]).toEqual(
      [true, false],
   );
   // (2) the path report's table differs from by_cat's own.
   expect(untampered.servedBy).toMatchObject({
      kind: "materialized",
      tableId: ctx.ids.by_cat_done,
   });
   expect(base.servedBy?.tableId).toBe(ctx.ids.by_cat);
   expect(untampered.servedBy?.tableId).not.toBe(base.servedBy?.tableId);
   // The tamper: only by_cat_done's marker.
   expect(categories(tampered)).toContain("zz_done");
   expect(categories(tampered)).not.toContain("zz_by_cat");
}

for (const variant of VARIANTS) {
   describe(`P2-22, host-style bind (${variant.label})`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let live: Answer["rows"];
      let ids: Record<string, string>;
      let tables: Record<string, string>;

      /** Bind the named sources' tables as a host's manifest binds them. */
      async function hostBind(names: string[]): Promise<void> {
         const mz = loader.pkg.materialization();
         const database = Object.fromEntries(
            names.map((n) => [
               ids[n],
               { tableName: tables[ids[n]], connectionName: "duckdb" },
            ]),
         );
         mz.bind({ database: {} });
         mz.bind({
            database: database as never,
            observed: await mz.readDatabaseColumns(database as never),
         });
         await settle(loader.pkg);
      }

      beforeAll(async () => {
         restore = withSwitch("on");
         await freshLoadPool();
         loader = await loadFixture(variant.files);
         live = (await ask(loader.pkg, "other.malloy", DONE_QUERY)).rows;
         const fx = await loader.bind();
         expect(fx.buildErrors).toEqual({});
         ids = entityIds(loader.pkg);
         tables = fx.tables;
      });

      afterAll(async () => {
         restore?.();
         await loader?.dispose();
         await freshLoadPool();
      });

      it("(a) both tables bound: by_cat_done is read from its own table", async () => {
         await hostBind(["by_cat", "by_cat_done"]);
         expect(
            loader.pkg
               .materialization()
               .serveVerdicts()
               .filter((v) => v.sourceName === "by_cat_done")
               .map((v) => v.verdict),
         ).toEqual(["serves"]);
         const untampered = await ask(loader.pkg, "other.malloy", DONE_QUERY);
         const base = await ask(loader.pkg, "base.malloy", BASE_QUERY);
         const sandbox = await loader.pkg.getMalloyConnection("duckdb");
         await tamper(
            sandbox as never,
            tables[ids.by_cat],
            tables[ids.by_cat_done],
         );
         try {
            const tampered = await ask(loader.pkg, "other.malloy", DONE_QUERY);
            expectOwnTable({ live, ids, tables }, untampered, base, tampered);
         } finally {
            await sandbox.runSQL(
               `DELETE FROM "${tables[ids.by_cat]}" WHERE category = 'zz_by_cat'`,
            );
            await sandbox.runSQL(
               `DELETE FROM "${tables[ids.by_cat_done]}" WHERE category = 'zz_done'`,
            );
         }
      });

      it("(b) only by_cat bound: by_cat_done is read from by_cat's table with its where: applied, equal to live, and reported so", async () => {
         await hostBind(["by_cat"]);
         const untampered = await ask(loader.pkg, "other.malloy", DONE_QUERY);
         expect(untampered.rows).toEqual(live);
         expect(untampered.servedBy).toMatchObject({
            kind: "materialized",
            tableId: ids.by_cat,
         });
         expect([
            reads(untampered, tables[ids.by_cat]),
            reads(untampered, tables[ids.by_cat_done]),
         ]).toEqual([true, false]);
         const sandbox = await loader.pkg.getMalloyConnection("duckdb");
         await tamper(
            sandbox as never,
            tables[ids.by_cat],
            tables[ids.by_cat_done],
         );
         const tampered = await ask(loader.pkg, "other.malloy", DONE_QUERY);
         expect(categories(tampered)).toContain("zz_by_cat");
         expect(categories(tampered)).not.toContain("zz_done");
         expect(
            loader.pkg
               .materialization()
               .serveVerdicts()
               .filter((v) => v.sourceName === "by_cat_done")
               .map((v) => v.verdict),
         ).toEqual(["awaiting_build"]);
      });
   });

   describe(`P2-22, standalone MaterializationService run (${variant.label})`, () => {
      let root: string;
      let db: StorageDuckDB;
      let repo: ResourceRepository;
      let env: Environment;
      let restore: (() => void) | undefined;
      let pkg: Package;
      let live: Answer["rows"];
      let ids: Record<string, string>;
      const tables: Record<string, string> = {};

      beforeAll(async () => {
         let envPath: string;
         ({ root, envPath, db, repo } = await openTempStorage(
            "cross-file-own-",
            { id: "env-1", name: "e1" },
         ));
         restore = installSettings({
            env: { PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         await __setPackageLoadPoolForTests(null);
         writePackageFiles(path.join(envPath, "pkg"), variant.files, {
            manifest: { name: "pkg", description: "fixture" },
         });
         env = await Environment.create("e1", envPath, []);
         await env.addPackage("pkg");
         pkg = await env.getPackage(parsePackageName("pkg"), false);
         live = (await ask(pkg, "other.malloy", DONE_QUERY)).rows;
         const service: MaterializationService = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         const created = await service.createMaterialization("e1", "pkg", {});
         const deadline = Date.now() + 60_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               while (running.has(created.id)) {
                  await new Promise((r) => setTimeout(r, 10));
               }
               const built = (await repo.getMaterializationById(created.id))!;
               expect([built.status, built.error]).toEqual([
                  "MANIFEST_FILE_READY",
                  null,
               ]);
               for (const [id, e] of Object.entries(
                  (
                     built.manifest as {
                        entries: Record<string, { physicalTableName: string }>;
                     }
                  ).entries,
               )) {
                  tables[id] = e.physicalTableName;
               }
               break;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 20));
         }
         pkg = await env.getPackage(parsePackageName("pkg"), false);
         await settle(pkg);
         ids = entityIds(pkg);
      });

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await disposeTempStorage({ db, root });
         restore?.();
         await __setPackageLoadPoolForTests(null);
      });

      it("(a) both tables built and bound: by_cat_done is read from its own table", async () => {
         expect(Object.keys(tables).sort()).toEqual(
            [ids.by_cat, ids.by_cat_done].sort(),
         );
         const untampered = await ask(pkg, "other.malloy", DONE_QUERY);
         const base = await ask(pkg, "base.malloy", BASE_QUERY);
         const sandbox = await pkg.getMalloyConnection("duckdb");
         await tamper(
            sandbox as never,
            tables[ids.by_cat],
            tables[ids.by_cat_done],
         );
         const tampered = await ask(pkg, "other.malloy", DONE_QUERY);
         expectOwnTable({ live, ids, tables }, untampered, base, tampered);
      });
   });
}
