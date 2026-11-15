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
import { VersionRef } from "../../identity";
import {
   storageBindingResolverFor,
   versionTableSuffix,
} from "../../versions/materialization_scope";
import { DuckDBConnection as WarehouseConnection } from "@malloydata/db-duckdb";
import { MaterializationService } from "./materialization_service";
import {
   dropLeftoverNameViews,
   heldByOwnNameView,
   nameViewsIn,
   ownNameViewTarget,
   provenOwnView,
   replaceOwnViewStatements,
} from "./leftover_name_views";
import { swapIntoPlace } from "./colocated_build_strategy";
import { createTableAndDescribe } from "./materialization_build_session";
import { formatPublishRejections } from "../../publish_checks";
import { packageOwner } from "./table_mark_sql";
import {
   colocatedTableSite,
   storageTableSite,
   type TableSiteContext,
} from "./table_site";

/**
 * The `name=` views earlier releases left (leftover_name_views.ts). Publisher
 * makes none now: a build makes no view and records none; a reclaim drops
 * this package's own marked view over a table it drops, and nothing else; a
 * v1 `#@ persist name=` build replaces this package's own view at its name
 * with the table, and is refused by anyone else's. The views are made here
 * by hand, as an earlier release left them, in a DuckDB file every version
 * reaches (the environment's `warehouse`).
 */

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "sales";
const OWNER = packageOwner(ENV, PKG);
/** The comment an earlier release marked a `name=` view with. */
const markOf = (target: string, owner = OWNER) =>
   `malloy-publisher:name-view owner=${encodeURIComponent(owner)} ` +
   `target=${encodeURIComponent(target)}`;

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
      "#(materialize)",
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

describe("leftover name= views through real builds", () => {
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
         {
            sourceLocation: location,
            promotion: "on-publish",
            // The publish route's gate (package.controller.ts), which an
            // orchestrator's publish passes through as well.
            validate: (pkg) => formatPublishRejections(pkg),
         },
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
         fs.mkdtempSync(path.join(os.tmpdir(), "leftover-name-view-")),
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

   /** Leave a view `name` reading `target`, marked as `owner`'s (or unmarked). */
   async function leaveView(
      name: string,
      target: string,
      owner: string | undefined,
   ): Promise<void> {
      await sql(`CREATE VIEW "${name}" AS SELECT * FROM "${target}"`);
      if (owner !== undefined) {
         await sql(`COMMENT ON VIEW "${name}" IS '${markOf(target, owner)}'`);
      }
   }

   /** Record views on a run, as an earlier release's run recorded them. */
   async function recordViews(
      run: Materialization,
      views: { viewName: string; owner: string; target: string }[],
   ): Promise<void> {
      await repo.updateMaterialization(run.id, {
         metadata: {
            ...(run.metadata ?? {}),
            nameViews: views.map((v) => ({
               viewName: v.viewName,
               owner: v.owner,
               physicalTableName: v.target,
               connectionName: "warehouse",
               sourceName: "summary",
               sourceEntityId: "x",
               outcome: "created",
            })),
         },
      });
   }

   const views = async () =>
      (
         await sql(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY view_name",
         )
      ).map((r) => String(r.view_name));

   it("a build makes no view and records none", async () => {
      await publish("1.0.0", 1, "version");
      const run = await build({ versionId: VersionRef.parse("1.0.0") });
      expect(run.status).toBe("MANIFEST_FILE_READY");
      expect(await tables()).toEqual([versionTable("1.0.0")]);
      expect(await views()).toEqual([]);
      expect(run.metadata?.nameViews).toBeUndefined();
      expect(nameViewsIn(run.metadata)).toEqual([]);
   });

   it("#(materialize) name= names the table, unversioned or scope: version, and makes no view", async () => {
      const named = (answer: number) =>
         materializeModel(answer).replace(
            "#(materialize)",
            '#(materialize) name="summary_t"',
         );
      await publish("1.0.0", 1, "package", named);
      const unversioned = await build({ versionId: VersionRef.parse("1.0.0") });
      expect(unversioned.status).toBe("MANIFEST_FILE_READY");
      expect(await tables()).toEqual(["summary_t"]);
      await publish("2.0.0", 2, "version", named);
      const versioned = await build({ versionId: VersionRef.parse("2.0.0") });
      expect(versioned.status).toBe("MANIFEST_FILE_READY");
      // The version's own table: name='s leaf with the version's suffix.
      expect(await tables()).toEqual([
         "summary_t",
         `summary_t${versionTableSuffix({ environmentName: ENV, packageName: PKG, version: "2.0.0" })}`,
      ]);
      expect(await views()).toEqual([]);
      expect(nameViewsIn(versioned.metadata)).toEqual([]);
   });

   it("a version's reclaim drops this package's own leftover view over a table it drops, and no other view", async () => {
      await publish("1.0.0", 1, "version");
      const run = await build({ versionId: VersionRef.parse("1.0.0") });
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const table = versionTable("1.0.0");
      await sql("CREATE TABLE elsewhere AS SELECT 7 AS answer");
      // This package's own view over the table the reclaim drops: dropped.
      await leaveView("summary", table, OWNER);
      // Recorded as this package's, but marked by another: left.
      await leaveView("theirs", table, "testEnv/other");
      // Recorded, but unmarked: left.
      await leaveView("unmarked", table, undefined);
      // This package's own, but repointed to a table that stays: left.
      await leaveView("moved", "elsewhere", OWNER);
      await recordViews(run, [
         { viewName: "summary", owner: OWNER, target: table },
         { viewName: "theirs", owner: OWNER, target: table },
         { viewName: "unmarked", owner: OWNER, target: table },
         { viewName: "moved", owner: OWNER, target: table },
      ]);
      await publish("2.0.0", 2, "version");
      await archive("1.0.0");
      expect(await tables()).toEqual(["elsewhere"]);
      expect(await views()).toEqual(["moved", "theirs", "unmarked"]);
   });

   it("deleting a run with its tables drops this package's own leftover view first, found on another run's record", async () => {
      await publish("1.0.0", 1, "package");
      const first = await build({ versionId: VersionRef.parse("1.0.0") });
      expect(first.status).toBe("MANIFEST_FILE_READY");
      const [table] = await tables();
      await leaveView("summary", table, OWNER);
      // The view was recorded by an earlier run that made it; the run
      // deleted here is the one that names the table, and records none.
      const earlier = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "FAILED",
         null,
         { version: "1.0.0" },
      );
      await recordViews(earlier, [
         { viewName: "summary", owner: OWNER, target: table },
      ]);
      await service.deleteMaterialization(ENV, PKG, first.id, {
         dropTables: true,
         versionId: VersionRef.parse("1.0.0"),
      });
      expect(await tables()).toEqual([]);
      expect(await views()).toEqual([]);
   });

   for (const [label, model] of [
      ["a v1 #@ persist name=", persistModel],
      [
         "a #(materialize) name=",
         (answer: number) =>
            materializeModel(answer).replace(
               "#(materialize)",
               '#(materialize) name="summary"',
            ),
      ],
   ] as const) {
      it(`${label} build replaces this package's own leftover view with its table, in the swap's transaction`, async () => {
         await sql("CREATE TABLE summary__g001 AS SELECT 0 AS answer");
         await leaveView("summary", "summary__g001", OWNER);
         await publish("1.0.0", 5, "package", model);
         const run = await build({ versionId: VersionRef.parse("1.0.0") });
         expect(run.status).toBe("MANIFEST_FILE_READY");
         expect(Object.keys(run.manifest?.failures ?? {})).toEqual([]);
         expect(await views()).toEqual([]);
         expect(await tables()).toEqual(["summary", "summary__g001"]);
         expect(
            Number((await sql('SELECT answer FROM "summary"'))[0].answer),
         ).toBe(5);
      });
   }

   it("a name= build is refused by this package's own view repointed off its marked table, and leaves it", async () => {
      await sql("CREATE TABLE summary__g001 AS SELECT 0 AS answer");
      await sql("CREATE TABLE elsewhere AS SELECT 7 AS answer");
      // Marked as reading summary__g001, but repointed: it reads elsewhere.
      await sql(`CREATE VIEW "summary" AS SELECT * FROM "elsewhere"`);
      await sql(`COMMENT ON VIEW "summary" IS '${markOf("summary__g001")}'`);
      await publish("1.0.0", 5, "package", persistModel);
      const run = await build({ versionId: VersionRef.parse("1.0.0") });
      expect(JSON.stringify(run.manifest?.failures ?? run.error)).toContain(
         "no longer provably reads the table its mark names",
      );
      expect(await views()).toEqual(["summary"]);
   });

   it("a v1 #@ persist name= build is refused by another package's view or an unmarked one, and drops neither", async () => {
      await sql("CREATE TABLE summary__g001 AS SELECT 0 AS answer");
      for (const owner of ["testEnv/other", undefined]) {
         await sql('DROP VIEW IF EXISTS "summary"');
         await leaveView("summary", "summary__g001", owner);
         await publish("1.0.0", 5, "package", persistModel).catch(
            () => undefined,
         );
         const run = await build({
            versionId: VersionRef.parse("1.0.0"),
            forceRefresh: true,
         });
         const reasons = JSON.stringify(run.manifest?.failures ?? run.error);
         expect(reasons).toContain("table-owned-elsewhere");
         expect(await views()).toEqual(["summary"]);
         expect(await tables()).toEqual(["summary__g001"]);
      }
   });
});

describe("leftover name= views, at a site", () => {
   let dir: string;
   let connection: WarehouseConnection;
   let context: TableSiteContext;
   const run = (sql: string) => connection.runSQL(sql);
   const OTHER = "testEnv/other";

   beforeEach(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "leftover-site-"));
      connection = new WarehouseConnection(
         "warehouse",
         path.join(dir, "wh.duckdb"),
         dir,
      );
      context = {
         connectionFor: () => connection,
         destinationFor: () => undefined,
         environmentPath: dir,
      };
      await run("CREATE TABLE g1 AS SELECT 1 AS x");
      await run("CREATE TABLE g2 AS SELECT 2 AS x");
   });
   afterEach(async () => {
      await connection.close().catch(() => undefined);
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const view = async (name: string, target: string, owner?: string) => {
      await run(`CREATE VIEW ${name} AS SELECT * FROM ${target}`);
      if (owner)
         await run(`COMMENT ON VIEW ${name} IS '${markOf(target, owner)}'`);
   };
   const record = (viewName: string, target: string, owner = OWNER) => ({
      viewName,
      owner,
      physicalTableName: target,
      connectionName: "warehouse",
   });
   const viewNames = async () =>
      (
         await run(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY view_name",
         )
      ).rows.map((r) => String(r.view_name));

   it("reads only this package's own mark, on a view", async () => {
      const site = colocatedTableSite(connection);
      await view("mine", "g1", OWNER);
      await view("theirs", "g1", OTHER);
      await view("plain", "g1");
      expect(await ownNameViewTarget(site, "mine", OWNER)).toBe("g1");
      expect(await ownNameViewTarget(site, "theirs", OWNER)).toBeUndefined();
      expect(await ownNameViewTarget(site, "plain", OWNER)).toBeUndefined();
      expect(await ownNameViewTarget(site, "g1", OWNER)).toBeUndefined();
      expect(await ownNameViewTarget(site, "none", OWNER)).toBeUndefined();
      expect(
         await ownNameViewTarget({ ...site, dialect: "mysql" }, "mine", OWNER),
      ).toBeUndefined();
   });

   it("drops with a table only this package's own view that still reads it", async () => {
      await view("mine", "g1", OWNER);
      // Marked as reading g1, repointed in place to g2 (Postgres keeps the
      // comment through CREATE OR REPLACE VIEW): left.
      await run("CREATE VIEW repointed AS SELECT * FROM g2");
      await run(`COMMENT ON VIEW repointed IS '${markOf("g1", OWNER)}'`);
      await view("moved", "g2", OWNER);
      await view("theirs", "g1", OTHER);
      await view("plain", "g1");
      const entry = { physicalTableName: "g1", connectionName: "warehouse" };
      await dropLeftoverNameViews(
         entry,
         [
            record("mine", "g1"),
            record("mine", "g1"),
            record("moved", "g1"),
            record("theirs", "g1"),
            record("plain", "g1"),
            record("repointed", "g1"),
            // Another package's record, even of this package's view: skipped.
            record("mine", "g1", OTHER),
            // A record of another table: skipped.
            record("moved", "g2"),
         ],
         OWNER,
         context,
      );
      expect(await viewNames()).toEqual([
         "moved",
         "plain",
         "repointed",
         "theirs",
      ]);
      // A table at a recorded name is never dropped as a view.
      await dropLeftoverNameViews(
         { physicalTableName: "g2", connectionName: "warehouse" },
         [record("g1", "g2")],
         OWNER,
         context,
      );
      expect((await run("SELECT x FROM g1")).rows).toEqual([{ x: 1 }]);
   });

   it("never throws: a site that fails is logged, and the table's drop goes on", async () => {
      await view("mine", "g1", OWNER);
      const failing: TableSiteContext = {
         ...context,
         connectionFor: () =>
            ({
               dialectName: "duckdb",
               runSQL: async () => {
                  throw new Error("connection lost");
               },
            }) as never,
      };
      await dropLeftoverNameViews(
         { physicalTableName: "g1", connectionName: "warehouse" },
         [record("mine", "g1")],
         OWNER,
         failing,
      );
      expect(await viewNames()).toEqual(["mine"]);
      expect(
         await heldByOwnNameView(
            { connectionName: "warehouse" },
            "mine",
            OWNER,
            failing,
         ),
      ).toBe(false);
   });

   it("tells a table a leftover view replaced: this package's own view holds its name", async () => {
      await view("mine", "g1", OWNER);
      await view("theirs", "g1", OTHER);
      const held = (name: string) =>
         heldByOwnNameView(
            { connectionName: "warehouse" },
            name,
            OWNER,
            context,
         );
      expect(await held("mine")).toBe(true);
      expect(await held("theirs")).toBe(false);
      expect(await held("g1")).toBe(false);
   });

   it("never proves a view that reads another relation beside its marked table, or the same name in another schema", async () => {
      const site = colocatedTableSite(connection);
      await run("CREATE TABLE other AS SELECT 1 AS x, 9 AS y");
      await run("CREATE SCHEMA s2");
      await run("CREATE TABLE s2.g1 AS SELECT 3 AS x");
      const marked = async (name: string, select: string) => {
         await run(`CREATE VIEW ${name} AS ${select}`);
         await run(`COMMENT ON VIEW ${name} IS '${markOf("g1", OWNER)}'`);
      };
      await marked(
         "joined",
         "SELECT g.x, o.y FROM g1 g JOIN other o ON o.x = g.x",
      );
      await marked(
         "subquery",
         "SELECT * FROM other WHERE x IN (SELECT x FROM g1)",
      );
      await marked("elsewhere", "SELECT * FROM s2.g1");
      await marked("exact", "SELECT * FROM main.g1");
      for (const name of ["joined", "subquery", "elsewhere"]) {
         expect([name, await provenOwnView(site, name, OWNER)]).toEqual([
            name,
            undefined,
         ]);
      }
      expect((await provenOwnView(site, "exact", OWNER))?.target).toBe("g1");
   });

   it("drops its own view in the swap's transaction, and leaves it when it was repointed after the proof", async () => {
      const site = colocatedTableSite(connection);
      const swap = (statements: string[] | undefined) =>
         swapIntoPlace({
            connection,
            runOptions: {},
            dialect: "duckdb",
            physicalTableName: "named_t",
            stagingTableName: "named_t_stage",
            backupTableName: "named_t_aside",
            connectionName: "warehouse",
            ...(statements ? { replacesView: statements } : {}),
         });
      await view("named_t", "g1", OWNER);
      await run("CREATE TABLE named_t_stage AS SELECT 5 AS x");
      const stale = replaceOwnViewStatements(
         (await provenOwnView(site, "named_t", OWNER))!,
         site,
      );
      // Repointed after the proof: the guard refuses, nothing is dropped.
      await run("CREATE OR REPLACE VIEW named_t AS SELECT * FROM g2");
      await run(`COMMENT ON VIEW named_t IS '${markOf("g1", OWNER)}'`);
      const failed = await swap(stale).catch((e: Error) => e);
      expect(String(failed?.message)).toContain(
         "changed while the build checked it",
      );
      expect(await viewNames()).toEqual(["named_t"]);
      expect((await run("SELECT x FROM named_t")).rows).toEqual([{ x: 2 }]);
      // Proven as it is now (reading g1 again), it is replaced.
      await run("CREATE OR REPLACE VIEW named_t AS SELECT * FROM g1");
      await run(`COMMENT ON VIEW named_t IS '${markOf("g1", OWNER)}'`);
      await run("CREATE TABLE named_t_stage AS SELECT 5 AS x");
      await swap(
         replaceOwnViewStatements(
            (await provenOwnView(site, "named_t", OWNER))!,
            site,
         ),
      );
      expect(await viewNames()).toEqual([]);
      expect((await run("SELECT x FROM named_t")).rows).toEqual([{ x: 5 }]);
   });

   it("reads a run's recorded views, and nothing malformed", () => {
      expect(nameViewsIn(null)).toEqual([]);
      expect(nameViewsIn({ nameViews: "x" })).toEqual([]);
      expect(
         nameViewsIn({
            nameViews: [record("mine", "g1"), { viewName: "x" }, null, 3],
         }),
      ).toEqual([record("mine", "g1")]);
   });
});

describe("leftover name= views, in a storage destination", () => {
   let dir: string;
   let session: WarehouseConnection;
   const lake = { name: "lake", type: "duckdb" } as never;
   const run = (sql: string) => session.runSQL(sql);

   beforeEach(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "leftover-lake-"));
      session = new WarehouseConnection("session", ":memory:", dir);
      await run(`ATTACH '${path.join(dir, "lake.duckdb")}' AS lake`);
      await run("CREATE TABLE lake.main.g1 AS SELECT 1 AS x");
      await run("CREATE TABLE lake.main.g2 AS SELECT 2 AS x");
      await run("CREATE VIEW lake.main.named_t AS SELECT * FROM main.g1");
      await run(
         `COMMENT ON VIEW lake.main.named_t IS '${markOf("g1", OWNER)}'`,
      );
   });
   afterEach(async () => {
      await session.close().catch(() => undefined);
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const views = async () =>
      (
         await run(
            "SELECT view_name FROM duckdb_views() WHERE database_name = 'lake' AND NOT internal",
         )
      ).rows.map((r) => String(r.view_name));

   it("replaces its own view with the table in the build's transaction, and leaves one repointed after the proof", async () => {
      const site = storageTableSite("lake", lake, session);
      const statements = () =>
         provenOwnView(site, "named_t", OWNER).then((v) =>
            replaceOwnViewStatements(v!, site),
         );
      const stale = await statements();
      await run(
         "CREATE OR REPLACE VIEW lake.main.named_t AS SELECT * FROM main.g2",
      );
      await run(
         `COMMENT ON VIEW lake.main.named_t IS '${markOf("g1", OWNER)}'`,
      );
      const failed = await createTableAndDescribe(
         session,
         '"lake"."main"."named_t"',
         "SELECT 5 AS x",
         { replacesView: stale! },
      ).catch((e: Error) => e);
      expect(String((failed as Error)?.message)).toContain(
         "changed while the build checked it",
      );
      expect(await views()).toEqual(["named_t"]);
      // Proven as it is now, it is replaced.
      await run(
         "CREATE OR REPLACE VIEW lake.main.named_t AS SELECT * FROM main.g1",
      );
      await run(
         `COMMENT ON VIEW lake.main.named_t IS '${markOf("g1", OWNER)}'`,
      );
      await createTableAndDescribe(
         session,
         '"lake"."main"."named_t"',
         "SELECT 5 AS x",
         { replacesView: (await statements())! },
      );
      expect(await views()).toEqual([]);
      expect((await run("SELECT x FROM lake.main.named_t")).rows).toEqual([
         { x: 5 },
      ]);
   });
});
