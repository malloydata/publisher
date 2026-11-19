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

/**
 * `statement_usage` records on a real server, read from its log and checked
 * against the responses it sent: a managed DuckLake connection whose catalog is
 * on a real Postgres. Each metered statement logs exactly one record, and one
 * that answered carries `bytes_out`, the byte length of the result string the
 * caller received.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create and
 * drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

// `é` makes the UTF-8 byte count differ from the character count.
const ROWS = `SELECT 1 AS x, 'é' AS s UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'`;
const MODEL = `source: nums is lk.sql("${ROWS}")
source: plain is duckdb.sql("SELECT 1 AS x")
`;
const NOTEBOOK = `>>>malloy
import "model.malloy"
>>>malloy
run: nums -> { select: x, s }
`;

type UsageRecord = Record<string, unknown>;

/** Every statement_usage record the server has logged, in order. */
function usageRecords(server: SpawnedServer): UsageRecord[] {
   const out: UsageRecord[] = [];
   for (const line of server.output().split("\n")) {
      // eslint-disable-next-line no-control-regex
      const clean = line.replace(/\x1b\[[0-9;]*m/g, "");
      const at = clean.indexOf("statement_usage {");
      if (at < 0) continue;
      out.push(JSON.parse(clean.slice(at + "statement_usage ".length)));
   }
   return out;
}

/** The records logged while `act` ran, once the log has caught up. */
async function recordsDuring(
   server: SpawnedServer,
   act: () => Promise<void>,
): Promise<UsageRecord[]> {
   const before = usageRecords(server).length;
   await act();
   await new Promise((resolve) => setTimeout(resolve, 300));
   return usageRecords(server).slice(before);
}

describe.skipIf(!url)("statement_usage bytes_out on a real server", () => {
   const token = Math.random().toString(36).slice(2, 8);
   const catalog = `usage_lake_${token}`;
   let admin: PostgresConnection;
   let server: SpawnedServer;
   let capped: SpawnedServer;
   const dirs: { cleanup(): void }[] = [];
   const env = (s: SpawnedServer) => `${s.baseUrl}/api/v0/environments/e1`;
   const post = (target: string, body: unknown) =>
      fetch(target, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify(body),
      });

   beforeAll(async () => {
      admin = new PostgresConnection({
         name: "admin",
         connectionString: url!,
      });
      await admin.runSQL(`CREATE DATABASE ${catalog}`);
      const parsed = new URL(url!);
      const pg = {
         host: parsed.hostname,
         port: Number(parsed.port || 5432),
         userName: decodeURIComponent(parsed.username),
         password: decodeURIComponent(parsed.password),
      };
      const lake = tempDir("usage-lake-");
      const pkg = tempDir("usage-pkg-");
      dirs.push(lake, pkg);
      fs.writeFileSync(
         path.join(pkg.dir, "publisher.json"),
         JSON.stringify({ name: "pa", version: "1.0.0", description: "x" }),
      );
      fs.writeFileSync(path.join(pkg.dir, "model.malloy"), MODEL);
      fs.writeFileSync(path.join(pkg.dir, "nb.malloynb"), NOTEBOOK);
      const spawn = (maxRows: string) =>
         spawnServer({
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: "e1",
                     packages: [{ name: "pa", location: pkg.dir }],
                     connections: [
                        {
                           name: "lk",
                           type: "ducklake",
                           managed: true,
                           ducklakeConnection: {
                              catalog: {
                                 postgresConnection: {
                                    ...pg,
                                    databaseName: catalog,
                                 },
                              },
                              storage: { bucketUrl: lake.dir },
                              readOnly: false,
                           },
                        },
                     ],
                  },
               ],
            },
            env: { PUBLISHER_MAX_QUERY_ROWS: maxRows },
         });
      server = await spawn("1000");
      await server.waitUntilServing(120_000);
      capped = await spawn("2");
      await capped.waitUntilServing(120_000);
   });

   afterAll(async () => {
      await server?.stop();
      await capped?.stop();
      await admin
         ?.runSQL(`DROP DATABASE IF EXISTS ${catalog} WITH (FORCE)`)
         .catch(() => undefined);
      for (const dir of dirs) dir.cleanup();
   });

   for (const compactJson of [false, true]) {
      it(`a model query (${compactJson ? "compact" : "full"}) records bytes_out as the bytes of the result it returned`, async () => {
         let result = "";
         const records = await recordsDuring(server, async () => {
            const res = await post(
               `${env(server)}/packages/pa/models/model.malloy/query`,
               { query: "run: nums -> { select: x, s }", compactJson },
            );
            expect(res.status).toBe(200);
            result = ((await res.json()) as { result: string }).result;
         });
         expect(records).toHaveLength(1);
         expect(records[0]).toMatchObject({
            surface: "serve",
            connection: "lk",
            rows: 3,
            status: "ok",
            bytes_out: Buffer.byteLength(result, "utf8"),
         });
      });
   }

   it("a notebook cell records bytes_out as the bytes of the cell's result", async () => {
      let result = "";
      const records = await recordsDuring(server, async () => {
         const res = await fetch(
            `${env(server)}/packages/pa/notebooks/nb.malloynb/cells/1`,
         );
         expect(res.status).toBe(200);
         result = ((await res.json()) as { result: string }).result;
      });
      expect(records).toHaveLength(1);
      expect(records[0]).toMatchObject({
         surface: "serve",
         rows: 3,
         status: "ok",
         bytes_out: Buffer.byteLength(result, "utf8"),
      });
   });

   it("raw SQL records bytes_out as the bytes of the data it returned", async () => {
      let data = "";
      const records = await recordsDuring(server, async () => {
         const res = await post(`${env(server)}/connections/lk/sqlQuery`, {
            sqlStatement: "SELECT 1 AS x, 'é' AS s UNION ALL SELECT 2, 'b'",
         });
         expect(res.status).toBe(200);
         data = ((await res.json()) as { data: string }).data;
      });
      expect(records).toHaveLength(1);
      expect(records[0]).toMatchObject({
         surface: "sql",
         rows: 2,
         status: "ok",
         bytes_out: Buffer.byteLength(data, "utf8"),
      });
   });

   it("a failed statement records one error, without bytes_out", async () => {
      const records = await recordsDuring(server, async () => {
         const res = await post(`${env(server)}/connections/lk/sqlQuery`, {
            sqlStatement: "SELECT * FROM no_such_table",
         });
         expect(res.status).toBeGreaterThanOrEqual(400);
      });
      expect(records).toHaveLength(1);
      expect(records[0]).toMatchObject({ surface: "sql", status: "error" });
      expect(records[0]).not.toHaveProperty("bytes_out");
   });

   it("an unmetered connection records nothing", async () => {
      const records = await recordsDuring(server, async () => {
         const res = await post(
            `${env(server)}/packages/pa/models/model.malloy/query`,
            { query: "run: plain -> { select: x }" },
         );
         expect(res.status).toBe(200);
      });
      expect(records).toEqual([]);
   });

   it("a result the row cap refuses still records the statement once, without bytes_out", async () => {
      const records = await recordsDuring(capped, async () => {
         // The model query ran and its result was refused: an ok record.
         const query = await post(
            `${env(capped)}/packages/pa/models/model.malloy/query`,
            { query: "run: nums -> { select: x }" },
         );
         expect(query.status).toBe(413);
         // DuckLake streams raw SQL, so the cap stops the statement
         // mid-stream: it did not complete, and records an error.
         const sql = await post(`${env(capped)}/connections/lk/sqlQuery`, {
            sqlStatement: ROWS,
         });
         expect(sql.status).toBe(413);
      });
      expect(
         records.map((r) => [r.surface, r.status, "bytes_out" in r]),
      ).toEqual([
         ["serve", "ok", false],
         ["sql", "error", false],
      ]);
   });
});
