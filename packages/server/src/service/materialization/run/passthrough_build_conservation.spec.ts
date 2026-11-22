// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "path";
import type { components } from "../../../api";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import { storageDestinationRoot } from "../../connection_config";
import type { FederatedSourceType } from "../../dialects";
import type { IncrementalLineage } from "./incremental_apply";
import { storageDeltaTarget } from "./incremental_storage";
import {
   buildSourceIntoStorage,
   type BuildSessionDeps,
   type StorageIncrementalRefresh,
} from "./materialization_build_session";

/**
 * A `storage=` build's run path for each passthrough source type (each
 * warehouse's read preparation, its session tag and cost, the job cancel,
 * the numeric recast, the kept warehouse types and the late-row window
 * read), recorded in passthrough_build.golden.json: the statements the build
 * session sends after its lockdown, what the build returns or the refusal,
 * the BigQuery jobs a cancel lists and cancels, and an incremental delta's
 * reads. One case per behaviour that differs by type; every other
 * connection type, a prototype name and an unknown one are refused before
 * any statement. The session lockdown itself is pinned in
 * duckdb_lockdown.spec.ts and destination_session_lockdown.spec.ts.
 *
 * No case reaches a network: the federation is stubbed, the passthrough
 * functions are macros over the session's own `query()`, and the reads a
 * macro cannot answer (the BigQuery job listings, Snowflake's session
 * settings and history, Postgres's type catalog) are answered here.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_PASSTHROUGH_BUILD=1 bun test <this file>`.
 */

type ApiConnection = components["schemas"]["Connection"];

const GOLDEN = path.join(__dirname, "passthrough_build.golden.json");

/** The types with no passthrough build, refused before any statement. */
const REFUSED = [
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "publisher",
   "constructor",
   "nope",
];

/** A source whose warehouse returns a DOUBLE, a string and a byte string. */
const BUILD_SQL = "SELECT 1.5::DOUBLE AS amount, 'x' AS s, '\\x01'::BLOB AS b";

const DECLARED = [
   { name: "amount", type: "number" },
   { name: "s", type: "string" },
   { name: "b", type: "string" },
];

const METADATA = { team: "Data Eng", Team: "x", job: "nightly" };

/** The passthrough functions, as macros over the session itself. */
const MACROS = [
   "CREATE OR REPLACE TEMP MACRO bigquery_query(p, q) AS TABLE SELECT * FROM query(q)",
   "CREATE OR REPLACE TEMP MACRO snowflake_query(q, s) AS TABLE SELECT * FROM query(q)",
   "CREATE OR REPLACE TEMP MACRO postgres_query(a, q) AS TABLE SELECT * FROM query(q)",
   `CREATE OR REPLACE TEMP MACRO bigquery_scan(p) AS TABLE ${BUILD_SQL}`,
];

interface Case {
   metadata?: Record<string, string>;
   signal?: boolean;
   /** Abort the run when the first marked BigQuery read is sent. */
   abortOnMarkedRead?: boolean;
   /** The BigQuery job listing is refused (no split). */
   noJobListing?: boolean;
   /** Snowflake reports a TIMEZONE other than UTC. */
   wrongTimezone?: boolean;
   /** Offer an incremental plan that drives a delta target, then rebuilds. */
   incremental?: boolean;
}

const TAGGED_INCREMENTAL: Case = {
   metadata: METADATA,
   signal: true,
   incremental: true,
};

/**
 * Per passthrough type, the cases whose path differs. A tagged incremental
 * build covers the tag, the cost read, the numeric recast and the delta
 * reads at once; BigQuery's untagged read, its fallback when the job listing
 * is refused and its cancel, and Snowflake's UTC refusal are their own paths.
 */
const CASES: [string, string, Case][] = [
   ["postgres", "tagged incremental", TAGGED_INCREMENTAL],
   ["bigquery", "plain", {}],
   [
      "bigquery",
      "tagged, no job listing",
      { metadata: METADATA, noJobListing: true },
   ],
   [
      "bigquery",
      "cancelled at its read",
      { signal: true, abortOnMarkedRead: true },
   ],
   ["bigquery", "tagged incremental", TAGGED_INCREMENTAL],
   ["snowflake", "tagged incremental", TAGGED_INCREMENTAL],
   [
      "snowflake",
      "session refuses UTC",
      { metadata: METADATA, wrongTimezone: true },
   ],
];

/** The statements after the session lockdown's last one. */
function afterLockdown(statements: string[]): string[] {
   return statements.slice(
      statements.lastIndexOf("SET enable_external_access=false") + 1,
   );
}

const normalize = (text: string) =>
   text
      .replace(/publisher_build:[0-9a-f-]+/g, "publisher_build:<uuid>")
      .replace(/(\/private)?\/(var|tmp)\/[^'"\s)]*/g, "<tmp>");

/** The answer to a read the macros cannot give, or undefined to run it. */
function cannedAnswer(sql: string, c: Case): unknown {
   if (sql.includes("bigquery_jobs(") && sql.includes("maxResults := 1")) {
      if (c.noJobListing) throw new Error("bigquery.jobs.list denied");
      return { rows: [{ job_id: "probe" }] };
   }
   if (sql.includes("bigquery_execute(")) {
      return { rows: [{ job_id: "parent-1" }] };
   }
   if (sql.includes("bigquery_jobs(") && sql.includes("parentJobId")) {
      return {
         rows: [
            {
               job_id: "child-1",
               project: "proj",
               dataset: "_anon",
               table: "result",
               total_slot_time_ms: 7,
               bytes_processed: "100",
               bytes_billed: "10485760",
               cache_hit: "false",
            },
         ],
      };
   }
   if (sql.includes("snowflake_query(") && sql.includes("SHOW PARAMETERS")) {
      const name = /LIKE ''(\w+)''/.exec(sql)?.[1] ?? "";
      const value =
         name === "TIMEZONE"
            ? c.wrongTimezone
               ? "America/Denver"
               : "UTC"
            : "7";
      return { rows: [{ key: name, value }] };
   }
   if (sql.includes("snowflake_query(") && sql.includes("ALTER SESSION")) {
      return { rows: [] };
   }
   if (sql.includes("snowflake_query(") && sql.includes("QUERY_HISTORY")) {
      return {
         rows: [
            {
               QUERY_TEXT: BUILD_SQL,
               JOB_ID: "sf-1",
               BYTES_SCANNED: 0,
               ROWS_PRODUCED: 1,
               EXECUTION_TIME_MS: 12,
            },
         ],
      };
   }
   if (sql.includes("pg_typeof")) {
      return { rows: [{ t0: "numeric" }] };
   }
   return undefined;
}

function errorOf(error: unknown): { error: string; name: string } {
   return {
      error: normalize(error instanceof Error ? error.message : String(error)),
      name: error instanceof Error ? error.constructor.name : typeof error,
   };
}

/** Each delta-target read, as the late-row check and a refresh make them. */
async function deltaReads(
   session: DuckDBConnection,
   sourceType: FederatedSourceType,
   metadata: Record<string, string> | undefined,
): Promise<Record<string, unknown>> {
   const lineage: IncrementalLineage = {
      physicalTableName: "t",
      connectionName: "wh",
      storageDestinationName: "lake",
      sourceEntityId: "addr-1",
      watermarkName: "amount",
      watermarkType: "number",
      mergeKeys: [],
      scopeColumns: [],
      strategy: "range_replace",
   };
   const delta = storageDeltaTarget({
      session,
      sourceType,
      handle: "h'q",
      destinationName: "lake",
      physicalTableName: "t",
      quotedTablePath: '"lake"."t"',
      lineage,
      persistSource: fakeSource({
         name: "src",
         sourceEntityId: "addr-1",
         sql: BUILD_SQL,
         connectionName: "wh",
         dialectName: sourceType === "bigquery" ? "standardsql" : sourceType,
         columns: ["amount", "s", "b"],
      }),
      buildSQL: BUILD_SQL,
      ...(metadata ? { queryMetadata: metadata } : {}),
   });
   const out: Record<string, unknown> = {
      windowRows: delta.target.windowRows !== undefined,
   };
   const attempt = async (key: string, run: () => Promise<unknown>) => {
      try {
         out[key] = { value: await run() };
      } catch (error) {
         out[key] = errorOf(error);
      }
   };
   const start = { malloyType: "number", value: "1" } as const;
   const end = { malloyType: "number", value: "2" } as const;
   await attempt("frontier", () => delta.target.probeSourceFrontier());
   await attempt("numericStoredAsDouble", () =>
      delta.numericColumnsStoredAsDouble(),
   );
   if (delta.target.windowRows) {
      const windowRows = delta.target.windowRows;
      await attempt("window", () => windowRows(start, end, ["amount"]));
   }
   return out;
}

async function observe(
   type: string,
   c: Case,
): Promise<Record<string, unknown>> {
   const statements: string[] = [];
   const listed: unknown[] = [];
   const cancelled: string[] = [];
   const run = new AbortController();
   let marker: string | undefined;
   let incremental: Record<string, unknown> | undefined;
   const deps: BuildSessionDeps = {
      federate: async (session, sourceType) => {
         for (const sql of MACROS) await session.runSQL(sql);
         const real = session.runSQL.bind(session);
         session.runSQL = (async (sql: string, ...rest: unknown[]) => {
            statements.push(normalize(sql));
            marker ??= /\/\* publisher_build:[0-9a-f-]{36} \*\//.exec(sql)?.[0];
            if (
               c.abortOnMarkedRead &&
               sql.includes("publisher_build:") &&
               !run.signal.aborted
            ) {
               run.abort();
            }
            const canned = cannedAnswer(sql, c);
            if (canned !== undefined) return canned;
            return (real as (...a: unknown[]) => Promise<unknown>)(
               sql,
               ...rest,
            );
         }) as typeof session.runSQL;
         return { handle: "h'q", sourceType };
      },
      bigQueryJobs: (projectId, key) => {
         listed.push({ projectId, key: key ?? null });
         let calls = 0;
         return {
            activeJobs: async () =>
               calls++ === 0
                  ? [
                       {
                          id: "job-1",
                          query: "/* publisher_build:x */ other",
                          cancel: async () => cancelled.push("job-1"),
                       },
                       {
                          id: "job-2",
                          query: undefined,
                          cancel: async () => cancelled.push("job-2"),
                       },
                       ...(marker
                          ? [
                               {
                                  id: "job-3",
                                  query: `${marker}\nSELECT 1`,
                                  cancel: async () => cancelled.push("job-3"),
                               },
                            ]
                          : []),
                    ]
                  : [],
         };
      },
   };
   const plan: StorageIncrementalRefresh | undefined = c.incremental
      ? {
           plan: async ({ session, sourceType }) => {
              incremental = await deltaReads(session, sourceType, c.metadata);
              return undefined;
           },
           afterSeed: async () => undefined,
        }
      : undefined;
   const dir = mkdtempSync(path.join(tmpdir(), "passthrough-build-"));
   mkdirSync(storageDestinationRoot(dir), { recursive: true });
   let outcome: unknown;
   try {
      const result = await buildSourceIntoStorage({
         destinationName: "lake",
         destinationConnection: {
            name: "lake",
            type: "duckdb",
         } as ApiConnection,
         sourceConnection: {
            name: "wh",
            type,
            bigqueryConnection: { serviceAccountKeyJson: "{}" },
            snowflakeConnection: { database: "ANALYTICS" },
         } as unknown as ApiConnection,
         buildSQL: BUILD_SQL,
         physicalTableName: "t",
         declaredColumns: DECLARED,
         environmentPath: dir,
         ...(c.metadata ? { queryMetadata: c.metadata } : {}),
         ...(c.signal || c.abortOnMarkedRead ? { signal: run.signal } : {}),
         ...(plan ? { incremental: plan } : {}),
         deps,
      });
      outcome = {
         schema: result.schema.map((col) => `${col.name} ${col.type}`),
         rowCount: result.rowCount,
         readCost: result.readCost,
      };
   } catch (error) {
      outcome = errorOf(error);
   } finally {
      rmSync(dir, { recursive: true, force: true });
   }
   const observed = {
      outcome,
      statements: afterLockdown(statements.filter((s) => !MACROS.includes(s))),
      ...(listed.length > 0 ? { listed } : {}),
      ...(cancelled.length > 0 ? { cancelled: cancelled.sort() } : {}),
      ...(incremental ? { incremental } : {}),
   };
   return JSON.parse(normalize(JSON.stringify(observed)));
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

describe("a storage= build's run path by source type", () => {
   it("builds each passthrough type as recorded", async () => {
      const actual: Record<string, unknown> = {};
      for (const [type, name, c] of CASES) {
         actual[`${type}: ${name}`] = await observe(type, c);
      }
      if (process.env.RECORD_PASSTHROUGH_BUILD === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   }, 120_000);

   it("refuses every other type before any statement", async () => {
      for (const type of REFUSED) {
         expect(await observe(type, {})).toEqual({
            outcome: {
               error: `Cannot materialize a '${type}' source into a storage destination: the native query-passthrough build supports source connections of type bigquery, snowflake, postgres only.`,
               name: "BadRequestError",
            },
            statements: [],
         });
      }
      expect(await observe("ducklake", {})).toMatchObject({
         outcome: {
            error: expect.stringContaining(
               "Cannot materialize a 'ducklake' source into a storage destination that is a different catalog.",
            ),
            name: "BadRequestError",
         },
         statements: [],
      });
   });
});
