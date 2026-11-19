// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A relation that reads the current time, through real auto-runs on DuckDB
 * and (with PUBLISHER_TEST_POSTGRES_URL) Postgres. An access decision over
 * it (a gated extension, a gated `->` derivation, a gated join, an ungated
 * source that reads a given through a join, a given-scoped extension) has it
 * refused (`current_time`) and answers live: once a grant expires, it is
 * gone from every answer. An ungated relation no access decision reads is
 * stored as of its build: it keeps the expired grant until the next run,
 * which rebuilds it although nothing changed, and then drops it.
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
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
/** How far after the load the moving grant expires. */
const LEAD_MS = 30_000;
let MOVING = "";
const modelText = () => `##! experimental { persistence givens }
given: USER :: number
given: ORG :: number
source: graw is warehouse.sql("""SELECT * FROM (VALUES (10, 1, TIMESTAMP '2100-01-01 00:00:00'), (20, 1, TIMESTAMP '${MOVING}'), (30, 1, TIMESTAMP '2001-01-01 00:00:00')) AS t(doc_id, user_id, expires_at)""")
source: draw is warehouse.sql("""SELECT * FROM (VALUES (10, 1), (20, 1), (30, 1)) AS t(id, org)""")

#@ persist name="vis5"
source: vis5 is draw extend { join_one: g is graw on g.doc_id = id } -> { where: g.expires_at > now; select: id, user_id is g.user_id }
#(access_filter) user_id = $USER
#@ persist name="d5"
source: d5 is vis5 extend {}

#@ persist name="g6"
source: g6 is graw -> { where: expires_at > now; select: doc_id, user_id }
source: d6 is draw extend { where: org = $ORG; join_one: g is g6 on g.doc_id = id; dimension: visible is g.user_id = $USER }

#(access_filter) g.user_id = $USER
source: d2 is draw extend { join_one: g is g6 on g.doc_id = id }
#(access_filter) user_id = $USER
source: d11 is vis5 -> { select: * }
source: d7 is g6 extend { where: user_id = $USER }

#@ persist name="down6"
source: down6 is g6 -> { select: doc_id }

#@ persist name="plainp"
source: plainp is draw -> { select: id }
`;
/** The relations an access decision reads, refused; the one none reads, stored. */
const REFUSED = ["vis5", "d5", "g6", "d7"];
const QUERIES: Record<string, string> = {
   d5: "run: d5 -> { group_by: id; order_by: id }",
   d6: "run: d6 -> { where: visible; group_by: id; order_by: id }",
   d2: "run: d2 -> { group_by: id; order_by: id }",
   d11: "run: d11 -> { group_by: id; order_by: id }",
   d7: "run: d7 -> { group_by: doc_id; order_by: doc_id }",
   down6: "run: down6 -> { group_by: doc_id; order_by: doc_id }",
};
const G = { USER: "1", ORG: "1" };
interface Fixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
   cleanup: () => Promise<void>;
}

async function fixture(
   warehouse: unknown,
   model = modelText(),
): Promise<Fixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "current-time-access-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": model },
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
   const env = await Environment.create(ENV, envPath, [warehouse as never], []);
   await env.addPackage(PKG);
   const pkg = await env.getPackage(parsePackageName(PKG), false);
   const service = serviceOverEnvironment(
      () => repo,
      () => env,
   );
   return {
      root,
      env,
      pkg,
      repo,
      db,
      service,
      cleanup: async () => {
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
      },
   };
}

/** One auto-run, to its settled record. */
async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 60_000;
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

/** The answer to `query` through the package, with what served it. */
async function ask(
   f: Fixture,
   query: string,
   givens?: Record<string, string>,
): Promise<{
   rows: Record<string, unknown>[];
   kind?: string;
   considered?: unknown;
}> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query, ...(givens ? { givens } : {}) } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      rows: result.compactResult as Record<string, unknown>[],
      kind: result.path?.servedBy?.kind,
      considered: (result.path as { considered?: unknown } | undefined)
         ?.considered,
   };
}

const tiers: {
   label: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
}[] = [
   {
      label: "DuckDB",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `current_time_access_${process.pid}_${Date.now()}`;
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
                    },
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
           },
        ]
      : []),
];

const fmt = (r: { rows: Record<string, unknown>[]; kind?: string }) =>
   `${r.rows.map((x) => Number(Object.values(x)[0])).join(",")} via ${r.kind ?? "live"}`;
for (const tier of tiers) {
   describe(`a relation that reads the current time, through runs (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      let movingAt = 0;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         const w = await tier.warehouse();
         drop = w.drop;
         movingAt = Math.ceil((Date.now() + LEAD_MS) / 1000) * 1000;
         MOVING = new Date(movingAt)
            .toISOString()
            .replace("T", " ")
            .slice(0, 19);
         f = await fixture(w.config);
      }, 300_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore?.();
         await freshLoadPool();
      });
      it("refuses what an access decision reads, stores the rest as of its build, and rebuilds it every run", async () => {
         const warnings = (f.pkg.getPackageMetadata().warnings ?? []).map(
            (w) => w as { code?: string; subject?: string; reason?: string },
         );
         expect(
            REFUSED.filter(
               (name) =>
                  warnings.find(
                     (w) =>
                        w.subject === name && w.code === "persist-not-built",
                  )?.reason !== "current_time",
            ),
         ).toEqual([]);
         expect(
            warnings.some(
               (w) =>
                  w.subject === "down6" &&
                  w.code === "persist-reads-current-time",
            ),
         ).toBe(true);
         const run1 = await build(f);
         expect(run1.status).toBe("MANIFEST_FILE_READY");
         const before: Record<string, string> = {};
         for (const [k, q] of Object.entries(QUERIES))
            before[k] = fmt(await ask(f, q, G));
         expect(Date.now()).toBeLessThan(movingAt);
         await Bun.sleep(Math.max(0, movingAt - Date.now()) + 2_000);
         const after: Record<string, string> = {};
         for (const [k, q] of Object.entries(QUERIES))
            after[k] = fmt(await ask(f, q, G));
         const run2 = await build(f);
         expect(run2.status).toBe("MANIFEST_FILE_READY");
         const after2: Record<string, string> = {};
         for (const [k, q] of Object.entries(QUERIES))
            after2[k] = fmt(await ask(f, q, G));
         // Every access decision at once, so a failure names each one.
         const wrong = ["d5", "d6", "d2", "d11", "d7"].flatMap((k) =>
            before[k] === "10,20 via live" &&
            after[k] === "10 via live" &&
            after2[k] === "10 via live"
               ? []
               : [
                    `${k}: before=${before[k]} after=${after[k]} after2=${after2[k]}`,
                 ],
         );
         expect(wrong).toEqual([]);
         // As of its build: the expired grant stays until the next run, which
         // rebuilds the unchanged table rather than carry it forward.
         expect(before.down6).toBe("10,20 via materialized");
         expect(after.down6).toBe("10,20 via materialized");
         expect(after2.down6).toBe("10 via materialized");
         const meta = run2.metadata as {
            sourcesBuilt?: number;
            sourcesReused?: number;
         };
         expect(meta.sourcesBuilt).toBe(1);
         expect(meta.sourcesReused).toBe(1);
      }, 200_000);
   });
}
