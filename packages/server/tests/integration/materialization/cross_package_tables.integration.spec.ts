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
 * Two environments, two packages each, all on ONE Postgres schema and ONE
 * DuckLake storage destination, every package declaring the same four
 * sources with its own rows: a colocated `#@ persist` named after its
 * source, a `#@ persist name=`, a `#(materialize) storage=` named after its
 * source, and a `#(materialize) storage= name=`. Built and queried through a
 * real server: every package must answer every source with its own rows,
 * from its own table or live. Before package-owned table names, all four
 * packages served whichever package built last (12 of 16 answers wrong).
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

const ENVS = ["e1", "e2"];
const PKGS = ["pa", "pb"];
const SOURCES = ["summary", "named", "stored", "stored_named_src"];

function model(who: string): string {
   return [
      "##! experimental.persistence",
      "#@ persist",
      `source: summary is pg.sql("SELECT '${who}' AS who, 's1' AS k") -> { select: who }`,
      '#@ persist name="named_t"',
      `source: named is pg.sql("SELECT '${who}' AS who, 's2' AS k") -> { select: who }`,
      '#(materialize) storage="lake"',
      `source: stored is pg.sql("SELECT '${who}' AS who, 's3' AS k") extend { primary_key: who }`,
      '#(materialize) storage="lake" name="stored_named"',
      `source: stored_named_src is pg.sql("SELECT '${who}' AS who, 's4' AS k") extend { primary_key: who }`,
      "",
   ].join("\n");
}

describe.skipIf(!url)("tables of packages that share a schema", () => {
   const token = Math.random().toString(36).slice(2, 8);
   const warehouse = `xpkg_wh_${token}`;
   const catalog = `xpkg_lake_${token}`;
   let admin: PostgresConnection;
   let wh: PostgresConnection;
   let server: SpawnedServer;
   const dirs: { cleanup(): void }[] = [];

   const api = (env: string, pkg: string) =>
      `${server.baseUrl}/api/v0/environments/${env}/packages/${pkg}`;

   async function build(env: string, pkg: string) {
      const created = (await (
         await fetch(`${api(env, pkg)}/materializations`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
         })
      ).json()) as { id: string };
      return pollFor(
         async () =>
            (await (
               await fetch(`${api(env, pkg)}/materializations/${created.id}`)
            ).json()) as {
               status: string;
               error?: string | null;
               manifest?: {
                  entries?: Record<string, { physicalTableName: string }>;
                  failures?: Record<string, { reason: string }>;
               };
            },
         (m) => TERMINAL_STATUSES.includes(m.status),
         `build of ${env}/${pkg} did not settle`,
         120_000,
      );
   }

   async function query(env: string, pkg: string, source: string) {
      const res = await fetch(`${api(env, pkg)}/models/model.malloy/query`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            query: `run: ${source} -> { select: who }`,
            compactJson: true,
         }),
      });
      const body = (await res.json()) as {
         result: string;
         path?: { servedBy?: { kind?: string } };
      };
      return {
         who: (JSON.parse(body.result) as { who: string }[])[0].who,
         servedBy: body.path?.servedBy?.kind,
      };
   }

   // Malloy's Postgres runSQL returns each row's one `row` column.
   const pgRows = async (sql: string) =>
      (await wh.runSQL(`SELECT row_to_json(q) AS "row" FROM (${sql}) q`))
         .rows as Record<string, unknown>[];

   beforeAll(async () => {
      admin = new PostgresConnection({ name: "admin", connectionString: url! });
      await admin.runSQL(`CREATE DATABASE ${warehouse}`);
      await admin.runSQL(`CREATE DATABASE ${catalog}`);
      const parsed = new URL(url!);
      const pg = {
         host: parsed.hostname,
         port: Number(parsed.port || 5432),
         userName: decodeURIComponent(parsed.username),
         password: decodeURIComponent(parsed.password),
      };
      wh = new PostgresConnection({
         name: "wh",
         connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
      });
      const lake = tempDir("xpkg-lake-");
      dirs.push(lake);
      const environments = ENVS.map((env) => ({
         name: env,
         packages: PKGS.map((pkg) => {
            const dir = tempDir(`xpkg-${env}-${pkg}-`);
            dirs.push(dir);
            fs.writeFileSync(
               path.join(dir.dir, "publisher.json"),
               JSON.stringify({
                  name: pkg,
                  version: "1.0.0",
                  description: "x",
               }),
            );
            fs.writeFileSync(
               path.join(dir.dir, "model.malloy"),
               model(`${env}/${pkg}`),
            );
            return { name: pkg, location: dir.dir };
         }),
         connections: [
            {
               name: "pg",
               type: "postgres",
               postgresConnection: { ...pg, databaseName: warehouse },
            },
         ],
         storageDestinations: [
            {
               name: "lake",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: { ...pg, databaseName: catalog },
                  },
                  storage: { bucketUrl: lake.dir },
               },
            },
         ],
      }));
      server = await spawnServer({
         config: { frozenConfig: false, environments },
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "on",
         },
      });
      await server.waitUntilServing();
   });

   afterAll(async () => {
      await server?.stop();
      await wh?.close().catch(() => undefined);
      for (const db of [warehouse, catalog]) {
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
            .catch(() => undefined);
      }
      await admin?.close().catch(() => undefined);
      for (const dir of dirs) dir.cleanup();
   });

   it("every package answers every source with its own rows", async () => {
      const builds: Record<string, Awaited<ReturnType<typeof build>>> = {};
      for (const env of ENVS) {
         for (const pkg of PKGS)
            builds[`${env}/${pkg}`] = await build(env, pkg);
      }
      for (const env of ENVS) {
         for (const pkg of PKGS) {
            for (const source of SOURCES) {
               const answer = await query(env, pkg, source);
               expect(`${env}/${pkg} ${source}: ${answer.who}`).toBe(
                  `${env}/${pkg} ${source}: ${env}/${pkg}`,
               );
            }
         }
      }

      // The first package to build holds both name= tables, marked; every
      // other package is refused them, coded, and answers those live.
      for (const key of ["e1/pb", "e2/pa", "e2/pb"]) {
         const reasons = Object.values(builds[key].manifest?.failures ?? {})
            .map((f) => f.reason)
            .join(" ");
         expect(reasons).toContain("table-owned-elsewhere");
         const [env, pkg] = key.split("/");
         expect((await query(env, pkg, "named")).servedBy).not.toBe(
            "materialized",
         );
      }
      expect((await query("e1", "pa", "named")).servedBy).toBe("materialized");
      const marks = await pgRows(
         "SELECT c.relname AS name, obj_description(c.oid, 'pg_class') AS comment " +
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY 1",
      );
      // Four default-named tables, one per package, and one name= table.
      expect(
         marks.filter((r) => String(r.name).startsWith("summary__p")),
      ).toHaveLength(4);
      expect(marks.find((r) => r.name === "named_t")?.comment).toBe(
         "malloy-publisher:table owner=e1%2Fpa",
      );
   });

   it("deleting a refused package's runs with their tables leaves the other package's table", async () => {
      const list = (await (
         await fetch(`${api("e1", "pb")}/materializations`)
      ).json()) as { id: string; status: string }[];
      for (const m of list.filter((r) => r.status !== "PENDING")) {
         await fetch(
            `${api("e1", "pb")}/materializations/${m.id}?dropTables=true`,
            { method: "DELETE" },
         );
      }
      const tables = (
         await pgRows(
            "SELECT tablename AS name FROM pg_tables WHERE schemaname = 'public'",
         )
      ).map((r) => String(r.name));
      expect(tables).toContain("named_t");
      expect(await query("e1", "pa", "named")).toEqual({
         who: "e1/pa",
         servedBy: "materialized",
      });
   });
});
