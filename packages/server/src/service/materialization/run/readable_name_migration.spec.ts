// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../access_record";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { packageTableSuffix } from "../../versions/materialization_scope";
import { MaterializationService } from "./materialization_service";
import { ownedTableComment } from "./table_ownership";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";

/**
 * The move to readable table names, end to end on a DuckDB warehouse: a
 * table an earlier release built under the name Publisher gave it then
 * (`<source>__p<hash>`) is rebuilt once under its readable name, and the old
 * table dropped after the run commits, only when Publisher can prove it is
 * this package's and nothing still serves it. Otherwise the old table stays.
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";
const LEGACY = `summary${packageTableSuffix({ environmentName: ENV, packageName: PKG })}`;

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const MODEL = [
   "##! experimental.persistence",
   `source: base is warehouse.sql("SELECT 'fresh' AS who")`,
   "#@ persist",
   "source: summary is base -> { group_by: who }",
   "",
].join("\n");

describe("a table named before readable names", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      ({ root, envPath, db, repo } = await openTempStorage(
         "readable-name-migration-",
         {
            id: ENV_ID,
            name: ENV,
         },
      ));
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restoreSettings();
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
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
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   async function sql(text: string): Promise<Record<string, unknown>[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (await connection.runSQL(text)).rows as Record<string, unknown>[];
   }

   async function tables(): Promise<string[]> {
      return (
         await sql(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY 1",
         )
      ).map((r) => String(r.table_name));
   }

   async function who(): Promise<string> {
      const pkg = await env.getPackage(parsePackageName(PKG), false);
      const result = await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), {
            query: "run: summary -> { select: who }",
         });
      return (result as unknown as { compactResult: { who: string }[] })
         .compactResult[0].who;
   }

   /**
    * As an earlier release left it: `summary` built at its old name, and a
    * committed run of `version` (null: the package's own) naming it.
    */
   async function earlierRelease(
      options: { version?: string; metadata?: Record<string, unknown> } = {},
   ): Promise<void> {
      await sql(`CREATE TABLE ${LEGACY} AS SELECT 'old' AS who`);
      const plan = (
         await env.getPackage(parsePackageName(PKG), false)
      ).getBuildPlan()!;
      const entries = Object.fromEntries(
         Object.values(plan.sources).map((s) => [
            s.sourceEntityId,
            {
               sourceEntityId: s.sourceEntityId,
               sourceName: s.name,
               physicalTableName: LEGACY,
               connectionName: "warehouse",
               realization: "COPY",
            },
         ]),
      );
      const run = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "PENDING",
         options.metadata ?? { mode: "auto" },
         options.version ? { version: options.version } : undefined,
      );
      await repo.updateMaterialization(run.id, {
         status: "MANIFEST_FILE_READY",
         manifest: { builtAt: new Date().toISOString(), entries } as never,
         completedAt: new Date(),
      } as never);
   }

   const readableNames = (m: Materialization) =>
      Object.values(m.manifest?.entries ?? {}).map((e) => e.physicalTableName);

   it("is rebuilt under its readable name, and the old table dropped once the run commits", async () => {
      await earlierRelease();
      const m = await build();
      expect(m.status).toBe("MANIFEST_FILE_READY");
      const [name] = readableNames(m);
      expect(name).toMatch(/^pa__model__summary__p[0-9a-f]{8}$/);
      expect(await tables()).toEqual([name]);
      expect(await who()).toBe("fresh");
      // Built once: the next run carries it.
      const again = await build();
      expect(readableNames(again)).toEqual([name]);
      expect(await tables()).toEqual([name]);
   });

   it("is kept when another package has marked it as its own since", async () => {
      await earlierRelease();
      await sql(
         `COMMENT ON TABLE ${LEGACY} IS '${ownedTableComment(`${ENV}/pb`)}'`,
      );
      const m = await build();
      expect(m.status).toBe("MANIFEST_FILE_READY");
      const [name] = readableNames(m);
      expect((await tables()).sort()).toEqual([LEGACY, name].sort());
      expect(await who()).toBe("fresh");
   });

   it("is kept while a run the package does not supersede still names it", async () => {
      await earlierRelease();
      // Another version's own run, which this package's runs never replace,
      // names the same table.
      const plan = (
         await env.getPackage(parsePackageName(PKG), false)
      ).getBuildPlan()!;
      const [source] = Object.values(plan.sources);
      const other = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "PENDING",
         { mode: "auto", scope: "version", versionId: "9.0.0" },
         { version: "9.0.0" },
      );
      await repo.updateMaterialization(other.id, {
         status: "MANIFEST_FILE_READY",
         manifest: {
            builtAt: new Date().toISOString(),
            entries: {
               [source.sourceEntityId]: {
                  sourceEntityId: source.sourceEntityId,
                  sourceName: source.name,
                  physicalTableName: LEGACY,
                  connectionName: "warehouse",
                  realization: "COPY",
               },
            },
         } as never,
         completedAt: new Date(),
      } as never);
      const m = await build();
      expect(m.status).toBe("MANIFEST_FILE_READY");
      expect(await tables()).toContain(LEGACY);
   });

   it("names a table, and plans its source, by the model that declares it, whatever imports it", async () => {
      const importer = (name: string) =>
         fs.writeFileSync(
            path.join(envPath, PKG, name),
            [
               "##! experimental.persistence",
               'import "model.malloy"',
               "query: everyone is summary -> { select: who }",
               "",
            ].join("\n"),
         );
      const plannedPaths = async () =>
         Object.values(
            (await env.getPackage(parsePackageName(PKG), true)).getBuildPlan()!
               .sources,
         ).map((source) => source.modelPath);

      // An importer that sorts after the declaring model.
      importer("z_imports.malloy");
      expect(await plannedPaths()).toEqual(["model.malloy"]);
      const first = await build();
      const [name] = readableNames(first);
      expect(name).toMatch(/^pa__model__summary__p[0-9a-f]{8}$/);

      // Another that sorts before it, and the first one gone.
      fs.rmSync(path.join(envPath, PKG, "z_imports.malloy"));
      importer("0_imports.malloy");
      expect(await plannedPaths()).toEqual(["model.malloy"]);
      const second = await build();
      expect(readableNames(second)).toEqual([name]);
      expect(await tables()).toEqual([name]);
   });

   it("keeps the old table while the package did not load the run, and drops it once a later run loads", async () => {
      await earlierRelease();
      // This run's load fails: the package still serves the old table.
      const load = env.loadRunManifest.bind(env);
      env.loadRunManifest = async (params) => {
         const outcome = {
            loaded: false as const,
            step: "reload" as const,
            error: "boom",
         };
         await params.commit(outcome);
         return outcome;
      };
      const failed = await build();
      env.loadRunManifest = load;
      expect(failed.metadata?.autoLoad).toBe("failed");
      const [name] = readableNames(failed);
      expect((await tables()).sort()).toEqual([LEGACY, name].sort());
      expect(failed.metadata?.renamedTables).toHaveLength(1);

      // The next run carries the table, loads, and finishes the reclaim.
      const next = await build();
      expect(next.metadata?.autoLoad).toBe("loaded");
      expect(await tables()).toEqual([name]);
      expect(
         (await repo.getMaterializationById(failed.id))?.metadata
            ?.renamedTables,
      ).toBeUndefined();
   });
});
