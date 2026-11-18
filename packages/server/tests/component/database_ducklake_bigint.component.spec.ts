// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-13 on the database tier: a DuckDB connection whose `setupSQL` attaches a
 * DuckLake (row inlining off, so every row is in Parquet) and USEs it builds
 * its database tables in the lake, where a 128-bit integer column is stored
 * as a DOUBLE while the catalog reports HUGEINT. A sum of BIGINTs past 2^53
 * must never be served rounded: every mechanism answers live's digits.
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
const ENV_ID = "env-1";
const PKG = "pkg";

/** A BIGINT past 2^53, negated, with -1 added: what F-13 served rounded. */
const BIG = -9007199254740992n;
const SUM = `${BIG - 1n}`;

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT k, v::BIGINT AS v FROM (VALUES
    ('a', ${BIG}), ('a', -1), ('b', ${BIG}), ('b', -1)
  ) AS t(k, v)
""") extend {
  #(rollup) grain=["k"]
  measure: v_total is v.sum()
  #@ persist name="f13_by_k"
  view: by_k is { group_by: k; aggregate: total is v.sum() }
}

#@ persist name="f13_persist"
source: p_sum is raw -> { group_by: k; aggregate: total is v.sum() }

#(materialize) name="f13_mat"
source: m_sum is raw -> { group_by: k; aggregate: total is v.sum() } extend {
  primary_key: k
}
`;

const QUERIES: Record<string, string> = {
   rollup:
      "run: raw -> { group_by: k; aggregate: total is v_total; order_by: k }",
   view: "run: raw -> by_k",
   persist: "run: p_sum -> { select: k, total; order_by: k }",
   materialize: "run: m_sum -> { select: k, total; order_by: k }",
};

interface Fixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
}

async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 120_000;
   for (;;) {
      const m = await f.repo.getMaterializationById(created.id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
      ) {
         while (running.has(created.id)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await f.repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

async function ask(
   f: Fixture,
   query: string,
): Promise<{ rows: Record<string, unknown>[]; kind?: string; path: unknown }> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      rows: (result.compactResult as Record<string, unknown>[])
         .slice()
         .sort((x, y) => String(x.k).localeCompare(String(y.k))),
      kind: result.path?.servedBy?.kind,
      path: result.path,
   };
}

describe.skipIf(process.platform === "win32")(
   "a database table in a DuckLake a DuckDB connection attached and USEd never serves a 128-bit integer rounded",
   () => {
      let restore: () => void;
      let f: Fixture;
      let run: Materialization;
      const live: Record<string, Record<string, unknown>[]> = {};

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         const root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "database-ducklake-")),
         );
         const lakeDir = path.join(root, "lake");
         fs.mkdirSync(path.join(lakeDir, "data"), { recursive: true });
         const envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL },
            { manifest: { name: PKG, description: "fixture" } },
         );
         const db = new DuckDBConnection(":memory:");
         await db.initialize();
         await initializeSchema(db);
         await db.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            [ENV_ID, ENV, envPath],
         );
         const repo = createDuckDBRepository(db);
         const setupSQL = [
            "INSTALL ducklake",
            "LOAD ducklake",
            "SET ducklake_default_data_inlining_row_limit = 0",
            `ATTACH IF NOT EXISTS 'ducklake:${path.join(lakeDir, "origin.ducklake")}' AS origin_lake (DATA_PATH '${path.join(lakeDir, "data")}/')`,
            "USE origin_lake",
         ].join(";\n");
         const env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "duckdb",
                  duckdbConnection: { setupSQL },
               } as never,
            ],
            [],
         );
         await env.addPackage(PKG);
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         f = { root, env, pkg, repo, db, service };
         for (const [name, query] of Object.entries(QUERIES)) {
            const answer = await ask(f, query);
            expect(answer.kind).toBe("live");
            live[name] = answer.rows;
         }
         run = await build(f);
      }, 240_000);

      afterAll(async () => {
         await f?.env.closeAllConnections().catch(() => undefined);
         await f?.db.close();
         if (f) fs.rmSync(f.root, { recursive: true, force: true });
         restore?.();
         await freshLoadPool();
      });

      it("live keeps every digit", () => {
         for (const rows of Object.values(live)) {
            expect(rows.map((r) => String(r.total))).toEqual([SUM, SUM]);
         }
      });

      it("builds every table into the lake", async () => {
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<
                     string,
                     {
                        sourceName: string;
                        physicalTableName: string;
                        schema?: unknown;
                     }
                  >;
               }
            ).entries,
         );
         expect(entries.length).toBe(4);
      });

      for (const [name, query] of Object.entries(QUERIES)) {
         it(`${name}: equals live, digit for digit`, async () => {
            const answer = await ask(f, query);
            expect(answer.rows.map((r) => String(r.total))).toEqual([SUM, SUM]);
         });
      }

      for (const [name, query] of Object.entries(QUERIES)) {
         it(`${name}: equals live after a rebind`, async () => {
            await f.pkg.reloadAllModels(
               f.pkg.materialization().boundDatabaseEntries() ?? {},
            );
            const answer = await ask(f, query);
            expect(answer.rows.map((r) => String(r.total))).toEqual([SUM, SUM]);
         });
      }
   },
);
