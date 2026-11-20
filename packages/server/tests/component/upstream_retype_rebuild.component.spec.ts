// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R-13: a column retyped upstream of a persisted source, whose SQL does not
 * change, re-addresses the source on the next explicit reload, so the next
 * run rebuilds its table instead of carrying the one built from the old
 * column; until then the query answers live. Two retypes: INTEGER to DOUBLE
 * with the values moved (the dev repro: INT64 to FLOAT64, +0.5), and
 * DECIMAL(10,2) to DECIMAL(12,4), which Malloy's own types (both `number`)
 * do not tell apart. A reload over an unchanged table keeps every address.
 */

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

const MODEL = `##! experimental { persistence }
#@ persist
source: by_cat is warehouse.table('t') -> { group_by: cat; aggregate: total is amt.sum() }
#@ persist
source: by_cat2 is warehouse.table('t2') -> { group_by: cat; aggregate: total is amt.sum() }

#@ -persist
source: by_cat__live is by_cat extend {}
#@ -persist
source: by_cat2__live is by_cat2 extend {}
`;

/** Each total as the warehouse renders it, so rows compare as text, never through a double. */
const AS_TEXT =
   "-> { group_by: cat, total_text is total::string; order_by: cat }";
const STORED = {
   by_cat: `run: by_cat ${AS_TEXT}`,
   by_cat2: `run: by_cat2 ${AS_TEXT}`,
} as const;
/** The same queries over a reader that never reads a table. */
const LIVE = {
   by_cat: `run: by_cat__live ${AS_TEXT}`,
   by_cat2: `run: by_cat2__live ${AS_TEXT}`,
} as const;

interface Answer {
   rows: unknown;
   kind?: string;
}

describe("R-13: a column retyped upstream re-addresses and rebuilds its sources", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;

   const ask = async (query: string): Promise<Answer> => {
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      const report = result.path as
         | { servedBy?: { kind?: string } }
         | undefined;
      return {
         rows: JSON.parse(JSON.stringify(result.compactResult)),
         kind: report?.servedBy?.kind,
      };
   };

   const build = async (): Promise<Materialization> => {
      const created = await service.createMaterialization(ENV, PKG, {});
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   };

   /** Each source's address in the plan in force, by name. */
   const addresses = (): Record<string, string> =>
      Object.fromEntries(
         Object.values(pkg.getBuildPlan()?.sources ?? {}).map((s) => [
            s.name,
            s.sourceEntityId,
         ]),
      );

   /** An explicit reload, as `?reload=true` and a publish make one. */
   const reload = async () => {
      pkg = await env.getPackage(parsePackageName(PKG), true, {
         refreshSchemas: true,
      });
   };

   let first: Record<string, string>;
   let afterUnchangedReload: Record<string, string>;
   let afterRetype: Record<string, string>;
   let firstRun: Materialization;
   let retypedRun: Materialization;
   const servedBefore: Record<string, Answer> = {};
   const liveBefore: Record<string, unknown> = {};
   const liveKinds: (string | undefined)[] = [];
   const beforeRebuild: Record<string, Answer> = {};
   const afterRebuild: Record<string, Answer> = {};
   const live: Record<string, unknown> = {};

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "upstream-retype-rebuild-")),
      );
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": MODEL },
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
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [],
      );
      const warehouse = await env.getMalloyConnection("warehouse");
      await warehouse.runSQL(
         "CREATE TABLE t AS SELECT cat, amt::INTEGER AS amt FROM (VALUES ('a', 1), ('a', 2), ('b', 5)) AS x(cat, amt)",
      );
      await warehouse.runSQL(
         "CREATE TABLE t2 AS SELECT cat, amt::DECIMAL(10,2) AS amt FROM (VALUES ('a', 1.25), ('b', 2.5)) AS x(cat, amt)",
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      first = addresses();
      firstRun = await build();
      for (const [name, query] of Object.entries(STORED)) {
         servedBefore[name] = await ask(query);
         const answer = await ask(LIVE[name as keyof typeof LIVE]);
         liveBefore[name] = answer.rows;
         liveKinds.push(answer.kind);
      }

      await reload();
      afterUnchangedReload = addresses();

      // Upstream, out of band: one statement per call (the connection is
      // locked down).
      await warehouse.runSQL("ALTER TABLE t ALTER amt TYPE DOUBLE");
      await warehouse.runSQL("UPDATE t SET amt = amt + 0.5");
      await warehouse.runSQL("ALTER TABLE t2 ALTER amt TYPE DECIMAL(12,4)");
      await reload();
      afterRetype = addresses();
      for (const [name, query] of Object.entries(STORED)) {
         beforeRebuild[name] = await ask(query);
         const answer = await ask(LIVE[name as keyof typeof LIVE]);
         live[name] = answer.rows;
         liveKinds.push(answer.kind);
      }
      retypedRun = await build();
      for (const [name, query] of Object.entries(STORED)) {
         afterRebuild[name] = await ask(query);
      }
   }, 300_000);

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("built and served each source from its table", () => {
      expect(firstRun.status).toBe("MANIFEST_FILE_READY");
      expect(Object.keys(first).sort()).toEqual(["by_cat", "by_cat2"]);
      for (const name of Object.keys(STORED)) {
         expect({ name, kind: servedBefore[name].kind }).toEqual({
            name,
            kind: "materialized",
         });
      }
   });

   it("keeps every address across a reload of an unchanged upstream", () => {
      expect(afterUnchangedReload).toEqual(first);
   });

   it("re-addresses each source whose upstream column was retyped, a precision change included", () => {
      expect(afterRetype.by_cat).not.toBe(first.by_cat);
      expect(afterRetype.by_cat2).not.toBe(first.by_cat2);
   });

   it("answers live, equal to live, until the table is rebuilt", () => {
      for (const name of Object.keys(STORED)) {
         expect({ name, kind: beforeRebuild[name].kind }).toEqual({
            name,
            kind: "live",
         });
         expect(beforeRebuild[name].rows).toEqual(live[name]);
      }
   });

   it("rebuilds the retyped sources' tables, and serves them equal to live", () => {
      expect(retypedRun.status).toBe("MANIFEST_FILE_READY");
      const entries = retypedRun.manifest?.entries ?? {};
      expect(Object.keys(entries).sort()).toEqual(
         [afterRetype.by_cat, afterRetype.by_cat2].sort(),
      );
      for (const name of Object.keys(STORED)) {
         expect({ name, kind: afterRebuild[name].kind }).toEqual({
            name,
            kind: "materialized",
         });
         expect(afterRebuild[name].rows).toEqual(live[name]);
      }
      // The values moved with the type: 1.5 + 2.5 and 5.5, not the
      // pre-retype table's 3 and 5.
      expect(live.by_cat).toEqual([
         { cat: "a", total_text: "4.0" },
         { cat: "b", total_text: "5.5" },
      ]);
   });

   it("reads live on the live side, never a table", () => {
      expect(liveKinds).toEqual(["live", "live", "live", "live"]);
   });

   it("answered from the old table before the retype, equal to live then", () => {
      expect(servedBefore.by_cat.rows).toEqual(liveBefore.by_cat);
      expect(servedBefore.by_cat2.rows).toEqual(liveBefore.by_cat2);
   });

   it("records the upstream column types each table was built from", () => {
      const entries = retypedRun.manifest?.entries ?? {};
      expect(entries[afterRetype.by_cat]?.upstreamSchemas).toEqual([
         {
            connectionName: "warehouse",
            table: expect.any(String),
            columns: [
               { name: "cat", nativeType: "VARCHAR" },
               { name: "amt", nativeType: "DOUBLE" },
            ],
            dialect: "duckdb",
         },
      ]);
      expect(
         entries[afterRetype.by_cat2]?.upstreamSchemas?.[0]?.columns,
      ).toEqual([
         { name: "cat", nativeType: "VARCHAR" },
         { name: "amt", nativeType: "DECIMAL(12,4)" },
      ]);
      const plan = pkg.getBuildPlan()?.sources ?? {};
      const planned = Object.values(plan).find((s) => s.name === "by_cat");
      expect(planned?.upstreamSchemas).toEqual(
         entries[afterRetype.by_cat]?.upstreamSchemas,
      );
   });
});
