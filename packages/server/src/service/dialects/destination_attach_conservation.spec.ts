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
 * The DuckLake catalog attach, run on a recording connection: every
 * statement each of the four exported attaches sends on one config,
 * recorded in destination_attach.golden.json; how each attach ends under
 * the session answers that change its path (catalog absent, a lost create
 * race, an alias already attached, an unsupported catalog version, no
 * catalog config); and, for every connection type, a prototype name and an
 * unknown one, whether a destination attach of that type exists and what it
 * sends. The attach text for each storage and catalog config is pinned in
 * ducklake_attach_text.spec.ts, so it is not repeated here.
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

const S3_CONFIG = {
   catalog: { postgresConnection: PG, metadataSchema: "meta" },
   storage: { bucketUrl: "s3://b/lake", s3Connection: S3 },
} as DucklakeConnection;
const NO_CATALOG = { catalog: {} } as DucklakeConnection;

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

type Outcome = { value?: unknown; error?: string; sql: string[] };

describe("the DuckLake catalog attach", () => {
   it("sends each attach's statements as recorded", async () => {
      const actual: Record<string, unknown> = {};
      for (const [entry, run] of Object.entries(ENTRY_POINTS)) {
         actual[entry] = await trace(run, S3_CONFIG, SESSIONS.present);
      }
      if (process.env.RECORD_DESTINATION_ATTACH === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   });

   it("ends each attach by the session's answers", async () => {
      const outcome = async (entry: string, session: string) => {
         const { value, error } = (await trace(
            ENTRY_POINTS[entry],
            S3_CONFIG,
            SESSIONS[session],
         )) as Outcome;
         return error ?? value;
      };
      const race = RACE.message;
      const versioned =
         "DuckLake catalog 'lake' has format version 999, which is outside the range";
      expect(await outcome("read-write", "absent")).toBeNull();
      expect(await outcome("read-write", "race once")).toBeNull();
      expect(await outcome("read-write", "race always")).toBe(race);
      expect(await outcome("read-write", "already attached")).toBeNull();
      expect(await outcome("read-write", "versioned")).toStartWith(versioned);
      expect(await outcome("read-only", "race once")).toBeNull();
      expect(await outcome("read-only if present", "present")).toBe(true);
      expect(await outcome("read-only if present", "absent")).toBe(false);
      // A create does not retry a lost race: the winner's catalog is not its.
      expect(await outcome("create", "race once")).toBe(race);
      expect(await outcome("create", "versioned")).toStartWith(versioned);
      for (const entry of Object.keys(ENTRY_POINTS)) {
         expect(
            (
               (await trace(
                  ENTRY_POINTS[entry],
                  NO_CATALOG,
                  SESSIONS.present,
               )) as Outcome
            ).error,
         ).toBe(
            "PostgreSQL connection configuration is required for DuckLake catalog: lake",
         );
      }
   });

   it("attaches a destination only for DuckLake, as a read-write attach", async () => {
      for (const type of TYPES) {
         const run = destinationRun(type);
         if (type !== "ducklake") {
            expect([type, run]).toEqual([type, undefined]);
            continue;
         }
         expect(run).toBeDefined();
         expect(await trace(run!, S3_CONFIG, SESSIONS.present)).toEqual(
            await trace(
               ENTRY_POINTS["read-write"],
               S3_CONFIG,
               SESSIONS.present,
            ),
         );
      }
   });
});
