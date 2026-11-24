// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental table the last run did not record is still held to its
 * contract before a delta advances it. Run 1 builds `a` (incremental) and
 * `b`; run 2 is named `b` only, so its record names no `a`; `a` is retyped
 * out of band (`numeric(10,1)` to `numeric(10,0)`, still a number) and a row
 * lands. Run 3, standalone, finds `a`'s entry on run 1's record: it reads the
 * table, finds it moved, and reseeds it rather than advance it, so the delta
 * never writes into the retyped column nor records a contract from it. The
 * served answer equals live's.
 *
 * Postgres only, when `PUBLISHER_TEST_POSTGRES_URL` names one: incremental
 * refresh reads its range out of Postgres, Snowflake or BigQuery only (a
 * DuckDB source's is refused at plan).
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as path from "path";
import { logger } from "../../src/logger";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const DAY = 86_400_000;
const NOW = Date.now();
const at = (ms: number) =>
   new Date(NOW - ms).toISOString().slice(0, 19).replace("T", " ");

const rows = (r: [number, number, number][]) =>
   r
      .map(
         ([id, ago, amt]) =>
            `(${id}, CAST('${at(ago)}' AS TIMESTAMP), CAST(${amt} AS NUMERIC(10,1)))`,
      )
      .join(", ");

const MODEL = `##! experimental { persistence }
source: raw is warehouse.table('public.hn_raw')

#(materialize) refresh="incremental" watermark="ts" merge_key="id"
source: a is raw

#(materialize)
source: b is raw extend { dimension: big is amt > 5 }
`;

const QUERY = "run: a -> { group_by: id, amt; order_by: id }";

interface Entry {
   sourceName?: string;
   physicalTableName: string;
   ledger?: { seededBy?: string; advancedBy?: string };
}

const entryNamed = (m: Materialization, name: string) =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> }).entries ?? {},
   ).find((e) => e.sourceName === name);

const describePg = POSTGRES_URL ? describe : describe.skip;

describePg("an incremental table left out of the last run (Postgres)", () => {
   let env: Environment;
   let pkg: Package;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let cleanup: () => Promise<void> = async () => undefined;
   const moved: string[] = [];
   const runs: Record<string, Materialization> = {};
   let live: unknown;
   let served: { rows: unknown; kind?: string };

   async function sql(text: string): Promise<void> {
      await (await env.getMalloyConnection("warehouse")).runSQL(text);
   }

   async function build(opts: object): Promise<Materialization> {
      const { id } = await service.createMaterialization(ENV, PKG, opts);
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      const deadline = Date.now() + 180_000;
      for (;;) {
         const m = await repo.getMaterializationById(id);
         if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
            while (running.has(id)) await new Promise((r) => setTimeout(r, 10));
            return (await repo.getMaterializationById(id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
   }

   async function ask(): Promise<{ rows: unknown; kind?: string }> {
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query: QUERY } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: JSON.parse(JSON.stringify(result.compactResult)),
         kind: (result.path as { servedBy?: { kind?: string } } | undefined)
            ?.servedBy?.kind,
      };
   }

   beforeAll(async () => {
      const { restore } = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "off" },
      });
      await freshLoadPool();
      const storage = await openTempStorage("hn-", { id: "env-1", name: ENV });
      repo = storage.repo;
      writePackageFiles(
         path.join(storage.envPath, PKG),
         { "m.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      const db = `hn_${process.pid}_${Date.now()}`;
      const admin = new PostgresConnection({
         name: "admin",
         connectionString: POSTGRES_URL!,
      });
      await admin.runSQL(`CREATE DATABASE ${db}`);
      const parsed = new URL(POSTGRES_URL!);
      env = await Environment.create(
         ENV,
         storage.envPath,
         [
            {
               name: "warehouse",
               type: "postgres",
               postgresConnection: {
                  host: parsed.hostname,
                  port: Number(parsed.port || 5432),
                  userName: decodeURIComponent(parsed.username),
                  password: decodeURIComponent(parsed.password),
                  databaseName: db,
               },
            } as never,
         ],
         [],
      );
      const warn = logger.warn.bind(logger);
      (logger as { warn: unknown }).warn = (
         message: string,
         meta?: Record<string, unknown>,
      ) => {
         if (
            message.startsWith("A table a run would carry no longer matches")
         ) {
            moved.push(String(meta?.sourceEntityId));
         }
         return warn(message, meta as never);
      };
      cleanup = async () => {
         (logger as { warn: unknown }).warn = warn;
         await env.closeAllConnections().catch(() => undefined);
         await storage.db.close();
         await admin
            .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
            .catch(() => undefined);
         await admin.close();
         restore();
         await freshLoadPool();
      };
      await sql(
         `CREATE TABLE public.hn_raw AS SELECT * FROM (VALUES ${rows([
            [1, 5 * DAY, 2.4],
            [2, 3 * DAY, 7.6],
         ])}) AS t(id, ts, amt)`,
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );

      runs.first = await build({});
      runs.second = await build({ sourceNames: ["b"] });
      const table = entryNamed(runs.first, "a")!.physicalTableName;
      await sql(
         `ALTER TABLE ${table} ALTER COLUMN amt TYPE numeric(10,0) USING amt::numeric(10,0)`,
      );
      await sql(
         `INSERT INTO public.hn_raw SELECT * FROM (VALUES ${rows([
            [3, 3_600_000, 4.4],
         ])}) AS t(id, ts, amt)`,
      );
      runs.third = await build({});
      await pkg.reloadAllModels(
         pkg.materialization().boundDatabaseEntries() ?? {},
      );
      served = await ask();
      live = (
         await (
            await env.getMalloyConnection("warehouse")
         ).runSQL(
            "SELECT row_to_json(t) AS row FROM (SELECT id, amt FROM public.hn_raw ORDER BY id) t",
         )
      ).rows.map((r) => {
         const row = (r as { row?: unknown }).row ?? r;
         const { id, amt } = row as { id: unknown; amt: unknown };
         return { id: Number(id), amt: Number(amt) };
      });
   }, 400_000);

   afterAll(() => cleanup());

   it("records no entry for `a` in the run of `b` alone", () => {
      for (const run of Object.values(runs)) {
         expect(run.status).toBe("MANIFEST_FILE_READY");
      }
      expect(entryNamed(runs.second, "a")).toBeUndefined();
      expect(entryNamed(runs.second, "b")).toBeDefined();
   });

   it("reads `a` from the earlier run's entry, finds it moved, and reseeds it", () => {
      const a = entryNamed(runs.third, "a")!;
      expect(a.physicalTableName).toBe(
         entryNamed(runs.first, "a")!.physicalTableName,
      );
      expect(moved.length).toBe(1);
      expect(a.ledger?.seededBy).toBe(runs.third.id);
   });

   it("serves `a` from its table, equal to live", () => {
      expect(served.kind).not.toBe("live");
      expect(
         (served.rows as { id: unknown; amt: unknown }[]).map((r) => ({
            id: Number(r.id),
            amt: Number(r.amt),
         })),
      ).toEqual(live as never);
   });
});
