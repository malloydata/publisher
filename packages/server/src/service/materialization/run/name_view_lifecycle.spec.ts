// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { __setPackageLoadPoolForTests } from "../../../package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../storage/duckdb/schema";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import type { EnvironmentStore } from "../../environment_store";
import { parsePackageName, VersionRef } from "../../identity";
import {
   storageBindingResolverFor,
   versionTableSuffix,
} from "../../versions/materialization_scope";
import { MaterializationService } from "./materialization_service";
import { NAME_VIEWS_KEY, nameViewsIn } from "./name_view";
import { nameViewComment, nameViewOwner } from "./name_view_sql";

/**
 * The `name=` view through real builds (materialization v2, phase 3, table
 * behaviours): a `#(materialize)` source whose table a run names by its
 * generation gets a view at its `name=`, repointed by each later build,
 * repointed again when the table it reads is reclaimed while another
 * generation stands, and dropped with its last generation. Builds are real,
 * into a DuckDB file every version reaches (the environment's `warehouse`).
 */

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "sales";
const mark = (target: string) =>
   nameViewComment({ target, owner: nameViewOwner(ENV, PKG) });

type Scope = "version" | "package";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

/** The table a `scope: version` version builds `summary` into. */
const versionTable = (version: string) =>
   `summary${versionTableSuffix({ environmentName: ENV, packageName: PKG, version })}`;

function materializeModel(answer: number): string {
   return [
      `source: base is warehouse.sql("SELECT ${answer} AS answer")`,
      "",
      '#(materialize) name="summary"',
      "source: summary is base extend {",
      "  dimension: doubled is answer * 2",
      "}",
      "",
   ].join("\n");
}

function persistModel(answer: number): string {
   return [
      "##! experimental.persistence",
      `source: base is warehouse.sql("SELECT ${answer} AS answer")`,
      '#@ persist name="summary"',
      "source: summary is base -> { group_by: answer }",
      "",
   ].join("\n");
}

async function settledRun(
   repository: Pick<ResourceRepository, "getMaterializationById">,
   service: MaterializationService,
   id: string,
): Promise<Materialization> {
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await repository.getMaterializationById(id);
      if (
         m &&
         (m.status === "MANIFEST_FILE_READY" ||
            m.status === "FAILED" ||
            m.status === "CANCELLED")
      ) {
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         while (running.has(id)) {
            if (Date.now() > deadline) throw new Error(`run ${id} never ended`);
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await repository.getMaterializationById(id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

let restoreSettings: () => void;

/**
 * These builds put a colocated `#(materialize)` table and its view in the
 * source's own warehouse, which is not built until the served copy can serve
 * it (`materialize-not-built`, decided 2026-11-12): the view's colocated site
 * returns with colocated serving, and these run again then. A storage
 * destination is a DuckLake, which a unit spec cannot attach.
 */
const colocatedIt = it.skip;

describe("the name= view through real builds", () => {
   let root: string;
   let envPath: string;
   let sourcesPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let reclaims: Promise<void>[] = [];

   async function newEnvironment(): Promise<Environment> {
      const created = await Environment.create(ENV, envPath, [
         WAREHOUSE as never,
      ]);
      created.bindVersions(repo, ENV_ID, {
         downloaderFor: (_packageName, location) => async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         ensurePackageRecord: async (packageName, description) => {
            if (await repo.getPackageByName(ENV_ID, packageName)) return false;
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               description,
               manifestPath: "",
            });
            return true;
         },
         removePackageRecord: async (packageName) => {
            const row = await repo.getPackageByName(ENV_ID, packageName);
            if (row) await repo.deletePackage(row.id);
         },
      });
      created.setStorageBindingResolver(
         storageBindingResolverFor(repo, ENV_ID),
      );
      return created;
   }

   function newService(environment: Environment): MaterializationService {
      const store = {
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => environment,
         getLoadedEnvironments: () => [environment],
      } as unknown as EnvironmentStore;
      const created = new MaterializationService(store);
      environment.setVersionArchivedHook((packageName, versionId) => {
         reclaims.push(
            created.reclaimVersionTables(ENV, packageName, versionId),
         );
      });
      environment.setOwedReclaimHook((packageName, versionId) =>
         created.reclaimOwedNow(environment, packageName, versionId),
      );
      return created;
   }

   async function publish(
      version: string,
      answer: number,
      scope: Scope,
      model: (answer: number) => string = materializeModel,
   ) {
      const location = path.join(
         sourcesPath,
         `${PKG}-${version}-${answer}-${scope}`,
      );
      writePackageFiles(
         location,
         { "model.malloy": model(answer) },
         { manifest: { name: PKG, version, materialization: { scope } } },
      );
      return env.getVersionService()!.publish(
         PKG,
         async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         { sourceLocation: location, promotion: "on-publish" },
      );
   }

   async function build(
      options: Parameters<
         MaterializationService["createMaterialization"]
      >[2] = {},
   ): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, options);
      return settledRun(repo, service, created.id);
   }

   /** Build `summary` of a version into `table`, as a host instructs it. */
   async function buildInstructed(
      versionId: string,
      table: string,
   ): Promise<Materialization> {
      const plan = (
         await env.getPackage(parsePackageName(PKG), false, {
            versionId: VersionRef.parse(versionId),
         })
      ).getBuildPlan()!;
      const source = Object.values(plan.sources).find(
         (s) => s.name === "summary",
      );
      expect(source).toBeDefined();
      return build({
         versionId: VersionRef.parse(versionId),
         buildInstructions: [
            {
               sourceID: source!.sourceID,
               sourceEntityId: source!.sourceEntityId,
               physicalTableName: table,
               realization: "COPY",
            },
         ] as never,
      });
   }

   async function archive(version: string) {
      await env.getVersionService()!.setArchiveStatus(PKG, version, "archive");
      await Promise.all(reclaims);
   }

   async function sql(text: string): Promise<Record<string, unknown>[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (await connection.runSQL(text)).rows as Record<string, unknown>[];
   }

   const tables = async () =>
      (
         await sql("SELECT table_name FROM duckdb_tables() ORDER BY table_name")
      ).map((r) => String(r.table_name));

   /** The view named `summary`: what it answers and the table its mark names. */
   async function summaryView(): Promise<
      { answer: number; comment: string } | undefined
   > {
      const views = await sql(
         "SELECT comment FROM duckdb_views() WHERE view_name = 'summary'",
      );
      if (views.length === 0) return undefined;
      const [row] = await sql('SELECT answer FROM "summary"');
      return {
         answer: Number(row.answer),
         comment: String(views[0].comment),
      };
   }

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: {
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
      // A load worker reads its settings when it spawns: a fresh pool, so
      // the package's build plan sees PUBLISHER_MATERIALIZE.
      await __setPackageLoadPoolForTests(null);
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "name-view-")),
      );
      envPath = path.join(root, "env");
      sourcesPath = path.join(root, "sources");
      fs.mkdirSync(envPath);
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      reclaims = [];
      env = await newEnvironment();
      service = newService(env);
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
      restoreSettings();
      // Nor does a later spec's worker inherit them.
      await __setPackageLoadPoolForTests(null);
   });

   colocatedIt(
      "scope: version: the view follows each version's build, falls back to the standing generation, and goes with the last",
      async () => {
         await publish("1.0.0", 1, "version");
         await publish("2.0.0", 2, "version");

         const v1 = await build({ versionId: VersionRef.parse("1.0.0") });
         expect(v1.status).toBe("MANIFEST_FILE_READY");
         expect(await tables()).toEqual([versionTable("1.0.0")]);
         expect(await summaryView()).toEqual({
            answer: 1,
            comment: mark(versionTable("1.0.0")),
         });
         expect(nameViewsIn(v1.metadata)).toEqual([
            expect.objectContaining({
               viewName: "summary",
               physicalTableName: versionTable("1.0.0"),
               connectionName: "warehouse",
               outcome: "created",
            }),
         ]);

         // A later version's build repoints it.
         const v2 = await build({ versionId: VersionRef.parse("2.0.0") });
         expect(v2.status).toBe("MANIFEST_FILE_READY");
         expect((await summaryView())?.answer).toBe(2);
         expect(nameViewsIn(v2.metadata)[0].outcome).toBe("repointed");

         // A rebuild of the table the view reads keeps it (DuckDB: the swap
         // drops and renames under a view that reads by name).
         const again = await build({
            versionId: VersionRef.parse("2.0.0"),
            forceRefresh: true,
         });
         expect(again.status).toBe("MANIFEST_FILE_READY");
         expect((await summaryView())?.answer).toBe(2);
         expect(nameViewsIn(again.metadata)[0].outcome).toBe("unchanged");

         // Reclaiming the generation it reads repoints it to the one that
         // stands. (A later version, never built, is latest so both can be
         // archived.)
         await publish("3.0.0", 3, "version");
         await archive("2.0.0");
         expect(await tables()).toEqual([versionTable("1.0.0")]);
         expect(await summaryView()).toEqual({
            answer: 1,
            comment: mark(versionTable("1.0.0")),
         });

         // And it goes with the last generation.
         await archive("1.0.0");
         expect(await tables()).toEqual([]);
         expect(await summaryView()).toBeUndefined();
      },
      { timeout: 60_000 },
   );

   colocatedIt(
      "orchestrated: the view follows the host's generations, and a deleted run's drop repoints or drops it",
      async () => {
         await publish("1.0.0", 1, "package");
         const g1 = await buildInstructed("1.0.0", "summary_g1");
         expect(g1.status).toBe("MANIFEST_FILE_READY");
         expect(await summaryView()).toEqual({
            answer: 1,
            comment: mark("summary_g1"),
         });
         const g2 = await buildInstructed("1.0.0", "summary_g2");
         expect(g2.status).toBe("MANIFEST_FILE_READY");
         expect((await summaryView())?.comment).toBe(mark("summary_g2"));

         // Deleting the run the view reads, with its tables, repoints it to
         // the standing generation.
         await service.deleteMaterialization(ENV, PKG, g2.id, {
            dropTables: true,
            versionId: VersionRef.parse("1.0.0"),
         });
         expect(await tables()).toEqual(["summary_g1"]);
         expect((await summaryView())?.comment).toBe(mark("summary_g1"));

         // Deleting the last drops it.
         await service.deleteMaterialization(ENV, PKG, g1.id, {
            dropTables: true,
            versionId: VersionRef.parse("1.0.0"),
         });
         expect(await tables()).toEqual([]);
         expect(await summaryView()).toBeUndefined();
      },
      { timeout: 60_000 },
   );

   colocatedIt(
      "a run whose commit fails puts the view back on the table it read",
      async () => {
         await publish("1.0.0", 1, "package");
         await buildInstructed("1.0.0", "summary_g1");
         const committing = service as unknown as {
            commitManifest: (...args: unknown[]) => Promise<void>;
         };
         const commit = committing.commitManifest;
         committing.commitManifest = async () => {
            throw new Error("commit lost");
         };
         try {
            const g2 = await buildInstructed("1.0.0", "summary_g2");
            expect(g2.status).toBe("FAILED");
         } finally {
            committing.commitManifest = commit;
         }
         expect(await summaryView()).toEqual({
            answer: 1,
            comment: mark("summary_g1"),
         });
      },
      { timeout: 60_000 },
   );

   colocatedIt(
      "a deleted run whose view another run repointed leaves the view alone",
      async () => {
         await publish("1.0.0", 1, "package");
         const g1 = await buildInstructed("1.0.0", "summary_g1");
         await buildInstructed("1.0.0", "summary_g2");
         await service.deleteMaterialization(ENV, PKG, g1.id, {
            dropTables: true,
            versionId: VersionRef.parse("1.0.0"),
         });
         expect(await tables()).toEqual(["summary_g2"]);
         expect((await summaryView())?.comment).toBe(mark("summary_g2"));
      },
      { timeout: 60_000 },
   );

   colocatedIt(
      "refuses the build when name= is held by a table Publisher did not create, and writes nothing",
      async () => {
         await sql('CREATE TABLE "summary" AS SELECT 42 AS answer');
         await publish("1.0.0", 1, "package");
         const run = await buildInstructed("1.0.0", "summary_g1");
         expect(run.status).toBe("FAILED");
         expect(run.error).toContain(
            "held by a table Publisher did not create",
         );
         expect(await tables()).toEqual(["summary"]);
         expect(await sql('SELECT answer FROM "summary"')).toEqual([
            { answer: 42 },
         ]);
      },
      { timeout: 60_000 },
   );

   colocatedIt(
      "never replaces or drops a view Publisher did not mark",
      async () => {
         await publish("1.0.0", 1, "package");
         await sql("CREATE TABLE theirs AS SELECT 7 AS answer");
         await sql('CREATE VIEW "summary" AS SELECT * FROM theirs');
         const run = await buildInstructed("1.0.0", "summary_g1");
         expect(run.status).toBe("FAILED");
         expect(await sql('SELECT answer FROM "summary"')).toEqual([
            { answer: 7 },
         ]);
      },
      { timeout: 60_000 },
   );

   it(
      "a colocated #(materialize) builds nothing and makes no view, until it can be served",
      async () => {
         await publish("1.0.0", 1, "version");
         const plan = (
            await env.getPackage(parsePackageName(PKG), false, {
               versionId: VersionRef.parse("1.0.0"),
            })
         ).getBuildPlan();
         expect(
            Object.values(plan?.sources ?? {}).map((s) => s.name),
         ).not.toContain("summary");
         const run = await build({ versionId: VersionRef.parse("1.0.0") });
         expect(run.status).not.toBe("FAILED");
         expect(await tables()).toEqual([]);
         expect(await summaryView()).toBeUndefined();
      },
      { timeout: 60_000 },
   );

   it(
      "a v1 #@ persist name= makes no view (only #(materialize) does)",
      async () => {
         await publish("1.0.0", 1, "version", persistModel);
         const run = await build({ versionId: VersionRef.parse("1.0.0") });
         expect(run.status).toBe("MANIFEST_FILE_READY");
         expect(await tables()).toEqual([versionTable("1.0.0")]);
         expect(await summaryView()).toBeUndefined();
         expect(run.metadata?.[NAME_VIEWS_KEY]).toBeUndefined();
      },
      { timeout: 60_000 },
   );
});
