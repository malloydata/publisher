// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A caller's drop of a materialized table (MaterializationService
 * .dropMaterializedTable), end to end through a real Environment and
 * MaterializationService, on three warehouses: DuckDB, a DuckLake catalog
 * connection (its catalog a Postgres database, its data a local directory)
 * and Postgres, the last two when PUBLISHER_TEST_POSTGRES_URL names a server
 * the spec may create databases on.
 *
 * On each: every table a build writes carries the package's mark, an
 * auto-run's and a host-instructed run's alike; a table a committed run
 * names, or a loaded package binds, is never dropped; once its record is
 * gone it is, and the package answers live; a legacy table with no mark is
 * dropped only with the id of the run that built it; and a table another
 * package marked, one nothing proves is the package's, any table while a
 * run is active, and a connection the package does not reach are refused,
 * the table left exactly as it was.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   ConnectionNotFoundError,
   InvalidArgumentError,
   MaterializationConflictError,
   MaterializedTableDropRefusedError,
   TableNotFoundError,
} from "../../src/errors";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { backupSuffix } from "../../src/service/materialization/run/materialization_run_support";
import { ownedTableComment } from "../../src/service/materialization/run/table_ownership";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";
/** This package's mark on `table` (lower-case names: DuckDB and Postgres agree). */
const OWN = (table: string) => ownedTableComment(`${ENV}/${PKG}`, table);
const OTHER = ownedTableComment(`${ENV}/pb`, "theirs_t");
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const MODEL = [
   "##! experimental.persistence",
   `source: base is warehouse.sql("SELECT 'live' AS who, 1 AS k")`,
   "#@ persist",
   "source: summary is base -> { group_by: who }",
   "#@ persist",
   "source: by_k is base -> { group_by: k }",
   "",
].join("\n");

/** One warehouse the spec runs on. */
interface Warehouse {
   label: string;
   /** The connection's config, given the spec's scratch directory. */
   config(root: string): Promise<Record<string, unknown>>;
   /** Whether `table` exists, and its comment. */
   look(
      sql: (text: string) => Promise<Record<string, unknown>[]>,
      table: string,
   ): Promise<{ exists: boolean; comment: unknown }>;
   /** Plant a table, with a comment or none. */
   plant(
      sql: (text: string) => Promise<Record<string, unknown>[]>,
      table: string,
      comment: string | null,
   ): Promise<void>;
}

const duckLook: Warehouse["look"] = async (sql, table) => {
   const rows = await sql(
      `SELECT comment FROM duckdb_tables() WHERE table_name = '${table}'`,
   );
   return { exists: rows.length > 0, comment: rows[0]?.comment ?? null };
};
const duckPlant: Warehouse["plant"] = async (sql, table, comment) => {
   await sql(`CREATE TABLE ${table} AS SELECT 'planted' AS who`);
   if (comment !== null) {
      await sql(`COMMENT ON TABLE ${table} IS '${comment}'`);
   }
};

/** The Postgres databases the spec created, dropped once it is done. */
const pgDatabases: string[] = [];
let pgAdmin: PostgresConnection | undefined;

/** A fresh Postgres database, and how to reach it. */
async function freshPgDatabase(prefix: string): Promise<{
   name: string;
   url: URL;
}> {
   pgAdmin ??= new PostgresConnection({
      name: "admin",
      connectionString: POSTGRES_URL!,
   });
   const name = `${prefix}_${Math.random().toString(36).slice(2, 8)}`;
   await pgAdmin.runSQL(`CREATE DATABASE ${name}`);
   pgDatabases.push(name);
   const url = new URL(POSTGRES_URL!);
   url.pathname = `/${name}`;
   return { name, url };
}

async function dropPgDatabases(): Promise<void> {
   if (!pgAdmin) return;
   for (const name of pgDatabases.splice(0)) {
      await pgAdmin
         .runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
         .catch(() => undefined);
   }
}

const WAREHOUSES: Warehouse[] = [
   {
      label: "DuckDB",
      config: async () => ({
         name: "warehouse",
         type: "duckdb",
         duckdbConnection: { setupSQL: "SELECT 1" },
      }),
      look: duckLook,
      plant: duckPlant,
   },
   ...(POSTGRES_URL
      ? [
           {
              // The production shape: a DuckLake connection whose catalog is
              // a Postgres database, its data in a local directory.
              label: "DuckLake",
              config: async (root: string) => {
                 const { name, url } = await freshPgDatabase("drop_lake");
                 const data = path.join(root, "lake-data");
                 fs.mkdirSync(data, { recursive: true });
                 // A DuckLake connection attaches read-only to compile, which
                 // never creates a catalog: create it first, at its data path.
                 const creator = new MalloyDuckDBConnection(
                    "creator",
                    path.join(root, "creator.duckdb"),
                 );
                 try {
                    await creator.runSQL("INSTALL ducklake");
                    await creator.runSQL("LOAD ducklake");
                    await creator.runSQL("INSTALL postgres");
                    await creator.runSQL("LOAD postgres");
                    await creator.runSQL(
                       `ATTACH 'ducklake:postgres:host=${url.hostname} port=${url.port || 5432} dbname=${name} user=${decodeURIComponent(url.username)} password=${decodeURIComponent(url.password)}' AS lake (DATA_PATH '${data}/')`,
                    );
                    await creator.runSQL("DETACH lake");
                 } finally {
                    await creator.close();
                 }
                 return {
                    name: "warehouse",
                    type: "ducklake",
                    ducklakeConnection: {
                       catalog: {
                          postgresConnection: {
                             host: url.hostname,
                             port: Number(url.port || 5432),
                             userName: decodeURIComponent(url.username),
                             password: decodeURIComponent(url.password),
                             databaseName: name,
                          },
                       },
                       storage: { bucketUrl: data },
                    },
                 };
              },
              look: duckLook,
              plant: duckPlant,
           } satisfies Warehouse,
           {
              label: "Postgres",
              config: async () => {
                 const { url } = await freshPgDatabase("drop_pg");
                 return {
                    name: "warehouse",
                    type: "postgres",
                    postgresConnection: { connectionString: url.toString() },
                 };
              },
              look: async (
                 sql: (text: string) => Promise<Record<string, unknown>[]>,
                 table: string,
              ) => {
                 // Malloy's Postgres runSQL returns each row's one `row` column.
                 const rows = await sql(
                    `SELECT row_to_json(t) AS "row" FROM (SELECT obj_description(c.oid, 'pg_class') AS comment FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = '${table}' AND c.relkind = 'r') t`,
                 );
                 const row = rows[0] as
                    | { row?: { comment: unknown } }
                    | undefined;
                 const value = (row?.row ?? row) as
                    | { comment?: unknown }
                    | undefined;
                 return {
                    exists: rows.length > 0,
                    comment: value?.comment ?? null,
                 };
              },
              plant: duckPlant,
           } satisfies Warehouse,
        ]
      : []),
];

for (const warehouse of WAREHOUSES) {
   describe(`dropping a materialized table on ${warehouse.label}`, () => {
      let root: string;
      let envPath: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restoreSettings: () => void;

      const sql = async (text: string) => {
         const connection = await env.getMalloyConnection("warehouse");
         return (await connection.runSQL(text)).rows as Record<
            string,
            unknown
         >[];
      };
      const look = (table: string) => warehouse.look(sql, table);

      async function settle(id: string): Promise<Materialization> {
         const deadline = Date.now() + 60_000;
         for (;;) {
            const m = await repo.getMaterializationById(id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               while (running.has(id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               return (await repo.getMaterializationById(id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      }

      async function build(
         options: Parameters<
            MaterializationService["createMaterialization"]
         >[2] = {},
      ): Promise<Materialization> {
         const m = await settle(
            (await service.createMaterialization(ENV, PKG, options)).id,
         );
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         return m;
      }

      /** A host's build of `summary` into `table`. */
      async function buildInstructed(table: string): Promise<Materialization> {
         const plan = (
            await env.getPackage(parsePackageName(PKG), false)
         ).getBuildPlan()!;
         const source = Object.values(plan.sources).find(
            (s) => s.name === "summary",
         )!;
         return build({
            buildInstructions: [
               {
                  sourceID: source.sourceID,
                  sourceEntityId: source.sourceEntityId,
                  physicalTableName: table,
                  realization: "COPY",
               },
            ] as never,
         });
      }

      const tableOf = (m: Materialization, sourceName: string) =>
         Object.values(
            (
               m.manifest as {
                  entries: Record<
                     string,
                     { sourceName: string; physicalTableName: string }
                  >;
               }
            ).entries,
         ).find((e) => e.sourceName === sourceName)!.physicalTableName;

      const drop = (
         table: string,
         options: { materializationId?: string; connectionName?: string } = {},
      ) =>
         service.dropMaterializedTable(
            ENV,
            PKG,
            options.connectionName ?? "warehouse",
            table,
            options.materializationId
               ? { materializationId: options.materializationId }
               : {},
         );

      async function refused(p: Promise<unknown>): Promise<Error> {
         return p.then(
            () => {
               throw new Error("expected the drop to be refused");
            },
            (err: unknown) => err as Error,
         );
      }

      async function expectReason(p: Promise<unknown>, reason: string) {
         const err = await refused(p);
         expect(err).toBeInstanceOf(MaterializedTableDropRefusedError);
         expect(
            (err as MaterializedTableDropRefusedError).reason as string,
         ).toBe(reason);
      }

      async function answer(): Promise<string> {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const result = await pkg
            .getModel("model.malloy")!
            .runQuery(unrecordedAudit(), {
               query: "run: summary -> { select: who }",
            });
         return (result as unknown as { compactResult: { who: string }[] })
            .compactResult[0].who;
      }

      beforeAll(() => {
         restoreSettings = installSettings({
            env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
         }).restore;
      });
      afterAll(async () => {
         restoreSettings();
         await dropPgDatabases();
      });

      beforeEach(async () => {
         ({ root, envPath, db, repo } = await openTempStorage(
            "drop-materialized-table-",
            { id: ENV_ID, name: ENV },
         ));
         writePackageFiles(
            path.join(envPath, PKG),
            { "model.malloy": MODEL },
            { manifest: { name: PKG, description: "fixture" } },
         );
         env = await Environment.create(ENV, envPath, [
            (await warehouse.config(root)) as never,
         ]);
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
      });
      afterEach(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await disposeTempStorage({ db, root });
      });

      it("an auto-run's table: marked, in use while its run names it, dropped once the record is gone", async () => {
         const m = await build();
         const table = tableOf(m, "summary");
         // Publisher named it (no name=), and it carries the mark.
         expect(await look(table)).toEqual({
            exists: true,
            comment: OWN(table),
         });

         // Named by a committed run and bound: never dropped.
         await expectReason(drop(table), "TABLE_IN_USE");
         await expectReason(
            drop(table, { materializationId: m.id }),
            "TABLE_IN_USE",
         );
         await sql(`UPDATE ${table} SET who = 'stored'`);
         expect(await answer()).toBe("stored");

         // The record goes; the package rebinds to live; the drop goes ahead.
         await service.deleteMaterialization(ENV, PKG, m.id);
         expect(await answer()).toBe("live");
         expect(await drop(table)).toBe("dropped");
         expect((await look(table)).exists).toBe(false);
         expect(await answer()).toBe("live");
         // The other source's table is untouched.
         expect((await look(tableOf(m, "by_k"))).exists).toBe(true);

         // Nothing records it any more: nothing proves it was the package's.
         expect(await refused(drop(table))).toBeInstanceOf(TableNotFoundError);
      });

      it("a host-instructed build's table carries the mark, and drops once no record names it", async () => {
         const m = await buildInstructed("cp_gen1");
         expect(await look("cp_gen1")).toEqual({
            exists: true,
            comment: OWN("cp_gen1"),
         });
         await expectReason(drop("cp_gen1"), "TABLE_IN_USE");
         await service.deleteMaterialization(ENV, PKG, m.id);
         expect(await drop("cp_gen1")).toBe("dropped");
         expect((await look("cp_gen1")).exists).toBe(false);
      });

      it("a table with no mark (built before the mark): dropped only with the id of the run that built it", async () => {
         const legacy = await buildInstructed("cp_legacy");
         await sql("COMMENT ON TABLE cp_legacy IS NULL");
         expect(await look("cp_legacy")).toEqual({
            exists: true,
            comment: null,
         });
         // A newer generation is what the package serves now.
         await buildInstructed("cp_gen2");
         await expectReason(drop("cp_legacy"), "TABLE_IN_USE");
         // With its run's id, that run is not counted as using it, and its
         // record is the proof the table is the package's.
         expect(await drop("cp_legacy", { materializationId: legacy.id })).toBe(
            "dropped",
         );
         expect((await look("cp_legacy")).exists).toBe(false);
         // Retried with the record still there: already done.
         expect(await drop("cp_legacy", { materializationId: legacy.id })).toBe(
            "absent",
         );
         // The served generation is never dropped.
         expect((await look("cp_gen2")).exists).toBe(true);
      });

      it("a renamed, moved or copied table carries its mark, which proves nothing: refused, the tables left as they were", async () => {
         const m = await build();
         const table = tableOf(m, "summary");
         await service.deleteMaterialization(ENV, PKG, m.id);
         const mark = (await look(table)).comment;
         expect(mark).toBe(OWN(table));

         // Copied, its comment with it (a clone, a copy that keeps comments).
         await sql(`CREATE TABLE prod_copy AS SELECT * FROM ${table}`);
         await sql(`COMMENT ON TABLE prod_copy IS '${mark}'`);
         await expectReason(drop("prod_copy"), "TABLE_OWNERSHIP_UNVERIFIED");
         expect((await look("prod_copy")).exists).toBe(true);

         // Moved out of the way to keep it: the rename keeps the comment.
         await sql(`ALTER TABLE ${table} RENAME TO keep_me`);
         expect((await look("keep_me")).comment).toBe(mark);
         await expectReason(drop("keep_me"), "TABLE_OWNERSHIP_UNVERIFIED");
         expect((await look("keep_me")).exists).toBe(true);

         if (warehouse.label === "Postgres") {
            // Moved to another schema under its own name.
            await sql("CREATE SCHEMA s2");
            await sql("ALTER TABLE keep_me RENAME TO " + table);
            await sql(`ALTER TABLE ${table} SET SCHEMA s2`);
            await expectReason(
               drop(`s2.${table}`),
               "TABLE_OWNERSHIP_UNVERIFIED",
            );
         }
      });

      it("a table other objects depend on is refused TABLE_IN_USE, and another holder is not named", async () => {
         const m = await build();
         const table = tableOf(m, "summary");
         await service.deleteMaterialization(ENV, PKG, m.id);
         if (warehouse.label === "Postgres") {
            // Postgres binds a view to its table; DuckDB and DuckLake views
            // are bound only when read, so their tables drop under them.
            await sql(`CREATE VIEW dep_v AS SELECT * FROM ${table}`);
            await expectReason(drop(table), "TABLE_IN_USE");
            expect((await look(table)).exists).toBe(true);
         }
         await warehouse.plant(
            sql,
            "secret_t",
            ownedTableComment("otherenv/secretpkg", "secret_t"),
         );
         const err = await refused(drop("secret_t"));
         expect(
            (err as MaterializedTableDropRefusedError).reason as string,
         ).toBe("TABLE_OWNED_ELSEWHERE");
         expect(err.message).not.toContain("secretpkg");
         expect(err.message).not.toContain("otherenv");
      });

      it("a build's backup copy is refused however it is named", async () => {
         const m = await buildInstructed("cp_a");
         const entry = Object.values(
            (
               m.manifest as {
                  entries: Record<string, { sourceEntityId: string }>;
               }
            ).entries,
         )[0];
         const copy = `cp_a${backupSuffix(entry.sourceEntityId)}`;
         await warehouse.plant(sql, copy, OWN(copy));
         const schema = warehouse.label === "Postgres" ? "public" : "main";
         for (const name of [copy, `${schema}.${copy}`, copy.toUpperCase()]) {
            const err = await refused(drop(name));
            expect(err).toBeInstanceOf(InvalidArgumentError);
         }
         expect((await look(copy)).exists).toBe(true);
         expect((await look("cp_a")).exists).toBe(true);
      });

      it("refuses another package's table, an unproven one, any while a run is active, and an unreached connection", async () => {
         await warehouse.plant(sql, "theirs_t", OTHER);
         await warehouse.plant(sql, "users_t", null);

         await expectReason(drop("theirs_t"), "TABLE_OWNED_ELSEWHERE");
         expect(await look("theirs_t")).toEqual({
            exists: true,
            comment: OTHER,
         });
         await expectReason(drop("users_t"), "TABLE_OWNERSHIP_UNVERIFIED");
         expect(await look("users_t")).toEqual({ exists: true, comment: null });

         const active = await repo.createMaterialization(
            ENV_ID,
            PKG,
            "PENDING",
            { mode: "auto" },
         );
         expect(await refused(drop("users_t"))).toBeInstanceOf(
            MaterializationConflictError,
         );
         await repo.updateMaterialization(active.id, { status: "CANCELLED" });

         expect(
            await refused(drop("users_t", { connectionName: "nope" })),
         ).toBeInstanceOf(ConnectionNotFoundError);
         expect((await look("users_t")).exists).toBe(true);
      });
   });
}
