// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import fs from "fs";
import path from "path";
import {
   spawnServer,
   tempDir,
   type SpawnedServer,
} from "../../harness/spawn_server";
import { pollFor, TERMINAL_STATUSES } from "./polling";

/**
 * The one rename rule across places, through a real server on a Postgres
 * warehouse with two DuckLake storage destinations (catalogs on Postgres):
 * when a source's `storage=` changes, its next auto-run builds it in the new
 * place and, once that run commits and is served, drops the table it left in
 * the old one, which Publisher can prove is its own; a table at an author's
 * exact `name=` that changes is kept, listed on the run and warned about.
 * Every drop is checked against the warehouse and the catalogs themselves.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

const model = (persist: string) =>
   [
      "##! experimental.persistence",
      persist,
      `source: summary is pg.sql("SELECT 'pa' AS who") -> { group_by: who }`,
      "",
   ].join("\n");

interface Run {
   id: string;
   status: string;
   error?: string | null;
   metadata?: Record<string, unknown> | null;
   manifest?: {
      entries?: Record<
         string,
         { physicalTableName: string; storageDestinationName?: string }
      >;
   };
}

describe.skipIf(!url)("the one rename rule across places, on Postgres", () => {
   const token = Math.random().toString(36).slice(2, 8);
   const warehouse = `rr_wh_${token}`;
   const catalogs = [`rr_lake_${token}`, `rr_lake2_${token}`];
   let admin: PostgresConnection;
   let wh: PostgresConnection;
   const lakes: PostgresConnection[] = [];
   let server: SpawnedServer;
   const dirs: { cleanup(): void }[] = [];
   /** Start the server, on the same root each time. */
   let boot: () => Promise<SpawnedServer>;
   const api = () => `${server.baseUrl}/api/v0/environments/e1/packages/pa`;

   /**
    * Edit the served copy's model (the package dir, and its published
    * version's), and reload the package.
    */
   async function edit(persist: string): Promise<void> {
      const served = path.join(server.serverRoot, "publisher_data", "e1", "pa");
      for (const dir of [served, path.join(served, "1.0.0")]) {
         if (fs.existsSync(path.join(dir, "model.malloy"))) {
            fs.writeFileSync(path.join(dir, "model.malloy"), model(persist));
         }
      }
      const res = await fetch(`${api()}?reload=true`);
      expect(res.status).toBe(200);
   }

   async function build(): Promise<Run> {
      const created = (await (
         await fetch(`${api()}/materializations`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
         })
      ).json()) as { id?: string; message?: string };
      if (!created.id) throw new Error(`no run: ${created.message}`);
      const run = await pollFor(
         async () =>
            (await (
               await fetch(`${api()}/materializations/${created.id}`)
            ).json()) as Run,
         (m) => TERMINAL_STATUSES.includes(m.status),
         "the build did not settle",
         120_000,
      );
      expect(`${run.status} ${run.error ?? ""}`).toBe("MANIFEST_FILE_READY ");
      // The reclaim runs after the commit: settled once the run's record
      // no longer names a table to decide.
      return pollFor(
         async () =>
            (await (
               await fetch(`${api()}/materializations/${created.id}`)
            ).json()) as Run,
         (m) =>
            m.metadata?.renamedTables === undefined &&
            m.metadata?.autoLoad !== undefined,
         "the reclaim did not settle",
         60_000,
      );
   }

   const tableOf = (run: Run) =>
      Object.values(run.manifest?.entries ?? {})[0].physicalTableName;

   // Malloy's Postgres runSQL returns each row's one `row` column.
   const rows = async (connection: PostgresConnection, sql: string) =>
      (
         await connection.runSQL(
            `SELECT row_to_json(q) AS "row" FROM (${sql}) q`,
         )
      ).rows as Record<string, unknown>[];
   const warehouseTables = async () =>
      (
         await rows(
            wh,
            "SELECT tablename AS name FROM pg_tables WHERE schemaname = 'public'",
         )
      ).map((r) => String(r.name));
   /** The live tables a DuckLake catalog lists. */
   const lakeTables = async (lake: number) =>
      (
         await rows(
            lakes[lake],
            "SELECT table_name AS name FROM ducklake_table WHERE end_snapshot IS NULL",
         )
      ).map((r) => String(r.name));

   async function who(): Promise<string> {
      const res = await fetch(`${api()}/models/model.malloy/query`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            query: "run: summary -> { select: who }",
            compactJson: true,
         }),
      });
      const body = (await res.json()) as { result: string };
      return (JSON.parse(body.result) as { who: string }[])[0].who;
   }

   beforeAll(async () => {
      admin = new PostgresConnection({ name: "admin", connectionString: url! });
      for (const db of [warehouse, ...catalogs]) {
         await admin.runSQL(`CREATE DATABASE ${db}`);
      }
      const parsed = new URL(url!);
      const pg = {
         host: parsed.hostname,
         port: Number(parsed.port || 5432),
         userName: decodeURIComponent(parsed.username),
         password: decodeURIComponent(parsed.password),
      };
      const at = (db: string) =>
         `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${db}`;
      wh = new PostgresConnection({
         name: "wh",
         connectionString: at(warehouse),
      });
      for (const db of catalogs) {
         lakes.push(
            new PostgresConnection({ name: db, connectionString: at(db) }),
         );
      }
      const pkg = tempDir("rr-pa-");
      dirs.push(pkg);
      fs.writeFileSync(
         path.join(pkg.dir, "publisher.json"),
         JSON.stringify({ name: "pa", version: "1.0.0", description: "x" }),
      );
      fs.writeFileSync(
         path.join(pkg.dir, "model.malloy"),
         model('#@ persist storage="lake"'),
      );
      const destination = (name: string, catalog: string) => {
         const data = tempDir(`rr-${name}-`);
         dirs.push(data);
         return {
            name,
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: { ...pg, databaseName: catalog },
               },
               storage: { bucketUrl: data.dir },
            },
         };
      };
      const root = tempDir("rr-root-");
      dirs.push(root);
      boot = () =>
         spawnServer({
            serverRoot: root.dir,
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: "e1",
                     packages: [{ name: "pa", location: pkg.dir }],
                     connections: [
                        {
                           name: "pg",
                           type: "postgres",
                           postgresConnection: {
                              ...pg,
                              databaseName: warehouse,
                           },
                        },
                     ],
                     storageDestinations: [
                        destination("lake", catalogs[0]),
                        destination("lake2", catalogs[1]),
                     ],
                  },
               ],
            },
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
            },
         });
      server = await boot();
      await server.waitUntilServing();
   });

   afterAll(async () => {
      await server?.stop();
      for (const c of [wh, ...lakes]) await c?.close().catch(() => undefined);
      for (const db of [warehouse, ...catalogs]) {
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
            .catch(() => undefined);
      }
      await admin?.close().catch(() => undefined);
      for (const dir of dirs) dir.cleanup();
   });

   it("drops the table a storage= change left behind, wherever it moves", async () => {
      // In `lake`.
      const first = await build();
      const name = tableOf(first);
      expect(name).toMatch(/__p[0-9a-f]{8}$/);
      expect(await lakeTables(0)).toEqual([name]);

      // `lake` -> `lake2`: built there, and dropped from `lake`.
      await edit('#@ persist storage="lake2"');
      const moved = await build();
      expect(Object.values(moved.manifest!.entries!)[0]).toMatchObject({
         storageDestinationName: "lake2",
      });
      expect(await lakeTables(1)).toEqual([tableOf(moved)]);
      expect(await lakeTables(0)).toEqual([]);
      expect(await who()).toBe("pa");

      // `lake2` -> the warehouse itself: dropped from `lake2`.
      await edit("#@ persist");
      const home = await build();
      expect(await warehouseTables()).toEqual([tableOf(home)]);
      expect(await lakeTables(1)).toEqual([]);
      expect(await who()).toBe("pa");

      // The warehouse -> `lake`: dropped from the warehouse.
      await edit('#@ persist storage="lake"');
      const back = await build();
      expect(await lakeTables(0)).toEqual([tableOf(back)]);
      expect(await warehouseTables()).toEqual([]);
      expect(back.metadata?.keptTables).toBeUndefined();
   });

   it("keeps an author's exact name= that changed, lists it, and warns", async () => {
      await edit('#@ persist name="summary_t"');
      await build();
      expect(await warehouseTables()).toContain("summary_t");
      await edit('#@ persist name="summary_v2"');
      const renamed = await build();
      expect(tableOf(renamed)).toBe("summary_v2");
      expect((await warehouseTables()).sort()).toEqual([
         "summary_t",
         "summary_v2",
      ]);
      expect(renamed.metadata?.keptTables).toEqual([
         expect.objectContaining({
            physicalTableName: "summary_t",
            connectionName: "pg",
            reason: "author_name",
         }),
      ]);
      // Set on the package as the reclaim finishes.
      const kept = await pollFor(
         async () =>
            (
               (
                  (await (await fetch(api())).json()) as {
                     warnings?: {
                        code?: string;
                        subject?: string;
                        message: string;
                     }[];
                  }
               ).warnings ?? []
            ).filter((w) => w.code === "table-kept-after-rename"),
         (warnings) => warnings.length > 0,
         "no table-kept-after-rename warning",
         30_000,
      );
      expect(kept).toHaveLength(1);
      expect(kept[0].subject).toBe("summary");
      expect(kept[0].message).toContain("`summary_t`");
      expect(await who()).toBe("pa");

      // Read again from the run record when the package loads after a
      // restart.
      await server.stop();
      server = await boot();
      await server.waitUntilServing();
      const afterRestart = await pollFor(
         async () =>
            (
               (
                  (await (await fetch(api())).json()) as {
                     warnings?: { code?: string; message: string }[];
                  }
               ).warnings ?? []
            ).filter((w) => w.code === "table-kept-after-rename"),
         (warnings) => warnings.length > 0,
         "no table-kept-after-rename warning after a restart",
         60_000,
      );
      expect(afterRestart[0].message).toContain("`summary_t`");
   });
});
