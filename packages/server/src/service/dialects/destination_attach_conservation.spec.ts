// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type { components } from "../../api";
import { installSettings } from "../../test_helpers/settings_env";
import {
   attachDuckLakeForCreate,
   attachDuckLakeReadOnly,
   attachDuckLakeReadOnlyIfPresent,
   attachDuckLakeReadWrite,
} from "../connection";
import { connectionDialectOf } from ".";

/**
 * The DuckLake catalog attach the registry took over from connection.ts,
 * run on a recording connection and compared with what the code produced
 * before the move, recorded in destination_attach.golden.json: every
 * statement each attach sent and how it ended, over the four exported
 * attaches and a matrix of catalog configs and session answers (catalog
 * present or absent, a recorded data path, a lost create race, an alias
 * already attached, a failing storage secret), and, for every connection
 * type and types no entry names, what a destination attach of that type
 * does.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_DESTINATION_ATTACH=1 bun test <this file>`.
 */

type DucklakeConnection = components["schemas"]["DucklakeConnection"];
type Rows = Record<string, unknown>[];

const GOLDEN = path.join(__dirname, "destination_attach.golden.json");

const TYPES = [
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
   "constructor",
   "toString",
   "nope",
];

const PG = {
   host: "catalog.test",
   port: 5433,
   databaseName: "lakemeta",
   userName: "lake_user",
   password: "pw",
   sslmode: "disable",
};
const S3 = { accessKeyId: "AK", secretAccessKey: "SK", region: "us-east-1" };
const GCS = { keyId: "GK", secret: "GS" };

const CONFIGS: Record<string, DucklakeConnection> = {
   "s3 path schema": {
      catalog: { postgresConnection: PG, metadataSchema: "meta" },
      storage: { bucketUrl: "s3://b/lake", s3Connection: S3 },
   } as DucklakeConnection,
   "gcs path": {
      catalog: { postgresConnection: PG },
      storage: { bucketUrl: "gs://b/lake/", gcsConnection: GCS },
   } as DucklakeConnection,
   "s3 no path": {
      catalog: { postgresConnection: PG, metadataSchema: "meta" },
      storage: { s3Connection: S3 },
   } as DucklakeConnection,
   local: { catalog: { postgresConnection: PG } } as DucklakeConnection,
   "local path": {
      catalog: { postgresConnection: PG },
      storage: { bucketUrl: "/tmp/lake" },
   } as DucklakeConnection,
   "read-write": {
      catalog: { postgresConnection: PG },
      readOnly: false,
   } as DucklakeConnection,
   "no catalog": { catalog: {} } as DucklakeConnection,
};

const RACE = new Error(
   'Failed to initialize DuckLake: duplicate key value violates unique constraint "pg_type"',
);

/** Session answers: what each read returns, and which statements fail. */
const SESSIONS: Record<string, (sql: string, n: number) => Rows> = {
   present: (sql) => present(sql),
   absent: (sql) =>
      sql.startsWith("SELECT count(*) AS n FROM duckdb_tables()")
         ? [{ n: 0 }]
         : [],
   recorded: (sql) =>
      sql.startsWith("SELECT key, value FROM")
         ? [{ key: "data_path", value: "s3://b/recorded/" }]
         : sql.startsWith("SELECT path, path_is_relative")
           ? [{ path: "main/", path_is_relative: true }]
           : present(sql),
   "race once": (sql, n) => {
      if (sql.startsWith("ATTACH OR REPLACE") && n === 0) throw RACE;
      return present(sql);
   },
   "race always": (sql) => {
      if (sql.startsWith("ATTACH OR REPLACE")) throw RACE;
      return present(sql);
   },
   "already attached": (sql) => {
      if (sql.startsWith("ATTACH OR REPLACE")) {
         throw new Error('Catalog "lake" already exists');
      }
      if (sql.startsWith("SELECT 1 FROM duckdb_databases()")) return [{}];
      return present(sql);
   },
   "secret fails": (sql) => {
      if (sql.startsWith("CREATE OR REPLACE SECRET")) {
         throw new Error("secret refused");
      }
      return present(sql);
   },
   versioned: (sql) =>
      sql.startsWith("SELECT value FROM")
         ? [{ value: "999" }]
         : sql.startsWith("SELECT version()")
           ? [{ v: "v1.5.1" }]
           : present(sql),
};

function present(sql: string): Rows {
   return sql.startsWith("SELECT count(*) AS n FROM duckdb_tables()")
      ? [{ n: 1 }]
      : [];
}

/** A connection that records each statement, answered by `answer`. */
function recorder(answer: (sql: string, n: number) => Rows): {
   conn: DuckDBConnection;
   sql: string[];
} {
   const sql: string[] = [];
   const seen = new Map<string, number>();
   const conn = {
      runSQL: async (q: string) => {
         sql.push(normalize(q));
         const head = q.slice(0, 18);
         const n = seen.get(head) ?? 0;
         seen.set(head, n + 1);
         const rows = answer(q, n);
         return { rows, totalRows: rows.length };
      },
   } as unknown as DuckDBConnection;
   return { conn, sql };
}

function normalize(sql: string): string {
   return sql
      .replace(/\s+/g, " ")
      .trim()
      .replace(/_fmt_preflight_\d+/g, "_fmt_preflight_N");
}

type Run = (conn: DuckDBConnection, cfg: DucklakeConnection) => unknown;

const ENTRY_POINTS: Record<string, Run> = {
   "read-write": (c, cfg) => attachDuckLakeReadWrite(c, "lake", cfg),
   "read-only": (c, cfg) => attachDuckLakeReadOnly(c, "lake", cfg),
   "read-only if present": (c, cfg) =>
      attachDuckLakeReadOnlyIfPresent(c, "lake", cfg),
   create: (c, cfg) =>
      attachDuckLakeForCreate(c, "lake", cfg, {
         encrypted: true,
         dataInliningRowLimit: 10,
      }),
};

async function trace(
   run: Run,
   cfg: DucklakeConnection,
   session: (sql: string, n: number) => Rows,
): Promise<unknown> {
   const r = recorder(session);
   try {
      const value = await run(r.conn, cfg);
      return { value: value ?? null, sql: r.sql };
   } catch (error) {
      return {
         error: error instanceof Error ? error.message : String(error),
         name: error instanceof Error ? error.constructor.name : typeof error,
         sql: r.sql,
      };
   }
}

/**
 * A destination attach of `type`, as the registry dispatches it, as a build
 * attaches its destination: read-write, the operator's config, its result
 * unread (recorded from attachDuckLakeReadWrite, which returns nothing).
 */
function destinationRun(type: string): Run | undefined {
   const destination = connectionDialectOf(type)?.destination;
   return destination === undefined
      ? undefined
      : async (c, cfg) => {
           await destination.attach(
              c,
              "lake",
              { ducklakeConnection: cfg },
              { readOnly: false, origin: "storage-destination" },
           );
        };
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("the DuckLake catalog attach is unchanged by the registry", () => {
   it("attaches every config, mode and session as before the move", async () => {
      const actual: Record<string, unknown> = {};
      for (const [entry, run] of Object.entries(ENTRY_POINTS)) {
         for (const [config, cfg] of Object.entries(CONFIGS)) {
            for (const [session, answer] of Object.entries(SESSIONS)) {
               actual[`${entry} | ${config} | ${session}`] = await trace(
                  run,
                  cfg,
                  answer,
               );
            }
         }
      }
      for (const type of TYPES) {
         const run = destinationRun(type);
         actual[`destination ${type}`] =
            run === undefined
               ? { destination: "none" }
               : await trace(run, CONFIGS["s3 path schema"], SESSIONS.present);
      }
      if (process.env.RECORD_DESTINATION_ATTACH === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   });
});
