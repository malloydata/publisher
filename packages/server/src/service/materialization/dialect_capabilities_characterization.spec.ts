// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What each warehouse dialect is allowed to do, per dialect and one unknown
 * dialect, read through the public functions that decide it: the incremental
 * refresh's source range, target DML, MERGE, pair delta and Postgres version
 * floor; the transactional swap; the relation probe and ownership mark; a
 * leftover `name=` view's in-transaction drop and its dependency guard; the
 * BigQuery `name=` rule; nested stored columns; big-integer reads; and table
 * name case folding.
 *
 * It pins the answers so that moving these decisions into one capability
 * table can be shown to change none of them. The statement text each
 * dialect is asked in is pinned separately, by
 * `run/dialect_statements.spec.ts`.
 */

import { describe, expect, it } from "bun:test";
import { tableNamesFoldCase } from "./domain/storage_table_path";
import { incrementalPolicyRejections } from "./plan/incremental_policy";
import type { IncrementalDeclaration } from "./plan/incremental_declaration";
import { planModelMaterialize } from "./plan/materialize_plan";
import {
   capturedColocatedSchema,
   swapIntoPlace,
} from "./run/colocated_build_strategy";
import {
   INCREMENTAL_SOURCE_RANGE_DIALECTS,
   INCREMENTAL_TARGET_DML_DIALECTS,
   MERGE_CAPABLE_DIALECTS,
   planIncrementalStep,
   type DeltaTarget,
   type IncrementalLineage,
} from "./run/incremental_apply";
import {
   incrementalLineage,
   planSourceRefresh,
   type IncrementalRunContext,
} from "./run/incremental_build";
import { replaceOwnViewStatements } from "./run/leftover_name_views";
import { relationProbeSupported } from "./run/table_mark_sql";
import { ownedElsewhere } from "./run/table_ownership";
import { v1StoredColumnsOf } from "./serve/bind";
import { bigIntegerReadOf, liveKeepsBigIntegers } from "./stored_column_types";

const DIALECTS = [
   "duckdb",
   "postgres",
   "standardsql",
   "snowflake",
   "databricks",
   "trino",
   "presto",
   "mysql",
   "sqlserver",
   "unknown_dialect",
] as const;

type Dialect = (typeof DIALECTS)[number];

function declaration(
   overrides: Partial<IncrementalDeclaration> = {},
): IncrementalDeclaration {
   return {
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      declaredMergeKey: false,
      watermark: { name: "order_date", kind: "dimension", malloyType: "date" },
      watermarkOrderable: true,
      mergeKeys: [],
      scopeColumns: [],
      scopeIncomplete: false,
      watermarkInMergeKeys: false,
      strategy: "range_replace",
      malformed: [],
      unknownKeys: [],
      calculateFields: [],
      outputColumns: ["id", "order_date", "revenue"],
      ...overrides,
   } as IncrementalDeclaration;
}

const ID_KEY = { name: "id", kind: "dimension", malloyType: "number" };

function lineageFor(
   dialect: string,
   targetDialect: string,
   d: IncrementalDeclaration = declaration(),
): boolean {
   return (
      incrementalLineage({
         declaration: d,
         dialect,
         targetDialect,
         physicalTableName: "orders_v1",
         connectionName: "wh",
         sourceEntityId: "addr-1",
      }) !== undefined
   );
}

const MERGE_LINEAGE = {
   physicalTableName: "analytics.daily",
   connectionName: "wh",
   sourceEntityId: "sid",
   watermarkName: "order_date",
   watermarkType: "date",
   mergeKeys: ["order_id"],
   scopeColumns: [],
   strategy: "merge",
} as unknown as IncrementalLineage;

const MERGE_LEDGER = {
   sourceEntityId: "sid",
   coveredThroughValue: "2024-06-01",
   coveredThroughType: "date",
   watermarkDimension: "order_date",
   mergeKeyDimensions: ["order_id"],
   derivedStrategy: "merge",
   physicalTableName: "analytics.daily",
   connectionName: "wh",
};

function target(dialect: string, statements: string[] = []): DeltaTarget {
   return {
      dialect,
      runner: async (sql: string) => {
         statements.push(sql);
         return { rows: [] };
      },
      quotedTablePath: "analytics.daily",
      logicalTablePath: "analytics.daily",
      deltaRows: async () => "SELECT 1",
   } as unknown as DeltaTarget;
}

/** Whether a merge refresh probes the target's server version. */
async function probesServerVersion(dialect: string): Promise<boolean> {
   const statements: string[] = [];
   const context = {
      environmentId: "env",
      packageName: "pkg",
      materializationId: "m1",
      forceRefresh: false,
      now: new Date("2024-07-01T00:00:00Z"),
      declarations: {},
      ledger: {
         getIncrementalLedgerEntry: async () => MERGE_LEDGER,
      },
   } as unknown as IncrementalRunContext;
   try {
      await planSourceRefresh({
         context,
         lineage: MERGE_LINEAGE,
         target: target(dialect, statements),
         columns: ["order_id", "order_date"],
      });
   } catch {
      // Only the probe is observed.
   }
   return statements.some((s) => s.includes("server_version_num"));
}

/** Whether a merge on a server below the MERGE floor seeds for it. */
async function mergeRefusedBelowFloor(dialect: string): Promise<boolean> {
   try {
      const step = await planIncrementalStep({
         target: target(dialect),
         lineage: MERGE_LINEAGE,
         ledgerEntry: MERGE_LEDGER as never,
         forceRefresh: false,
         now: new Date("2024-07-01T00:00:00Z"),
         columns: ["order_id", "order_date"],
         postgresVersionNum: 140009,
      });
      return step.mode === "seed" && step.reasonCode === "merge_unsupported";
   } catch {
      return false;
   }
}

function policy(dialect: string, d: IncrementalDeclaration): string {
   const [rejection] = incrementalPolicyRejections([
      { sourceName: "orders", dialect, declaration: d },
   ]);
   if (rejection === undefined) return "accepted";
   if (rejection.includes("not supported on dialect")) return "no range";
   if (rejection.includes("needs a MERGE statement")) return "no merge";
   return "other";
}

/** Whether the swap is one call holding the drop and the rename. */
async function swapsInOneCall(dialect: string): Promise<boolean> {
   const calls: string[] = [];
   try {
      await swapIntoPlace({
         connection: {
            runSQL: async (sql: string) => {
               calls.push(sql);
               return { rows: [], totalRows: 0 };
            },
         } as never,
         runOptions: {},
         dialect,
         physicalTableName: "orders",
         stagingTableName: "orders__staging",
         backupTableName: "orders__backup",
         connectionName: "wh",
      });
   } catch {
      // Only the first call is observed.
   }
   return (
      calls.length > 0 &&
      calls[0].includes("DROP TABLE") &&
      calls[0].includes("RENAME")
   );
}

function viewDrop(dialect: string): string {
   const statements = replaceOwnViewStatements(
      {
         name: "v",
         target: "t__m1",
         comment: "malloy-publisher:name-view owner=env%2Fpkg",
         definition: "CREATE VIEW v AS SELECT * FROM t__m1;",
      },
      { dialect, segments: (name) => ["sch", name] },
   );
   if (statements === undefined) return "before the build";
   return statements[0].startsWith("DO $guard$")
      ? "in transaction, dependency guard"
      : "in transaction, definition guard";
}

function materializeRefused(dialect: string): boolean {
   const text = "#(materialize)\n";
   const { plans } = planModelMaterialize({
      orders: {
         sourceID: "orders@file:///pkg/m.malloy",
         dialect,
         annotations: {
            notes: [
               {
                  text,
                  at: {
                     url: "file:///pkg/m.malloy",
                     range: {
                        start: { line: 0, character: 0 },
                        end: { line: 0, character: text.length },
                     },
                  },
               },
            ],
         },
         fields: [{ name: "id", type: "number" }],
      },
   } as never);
   return plans.length === 0;
}

const NESTED_FIELDS = [
   { name: "s", type: "string" },
   {
      name: "tags",
      type: "array",
      elementTypeDef: { type: "string" },
      join: "many",
   },
];

function v1KeepsNest(dialect: string): boolean {
   const columns = v1StoredColumnsOf({ dialect, fields: NESTED_FIELDS }) ?? [];
   return columns.some((c) => c.name === "tags");
}

async function capturedKeepsNest(dialect: string): Promise<boolean> {
   const columns = await capturedColocatedSchema({
      connection: {
         fetchSchemaForTables: async () => ({
            schemas: { orders: { fields: NESTED_FIELDS } },
            errors: {},
         }),
      } as never,
      physicalTableName: "orders",
      dialect,
      sourceName: "orders",
   });
   return (columns ?? []).some((c) => c.name === "tags");
}

function colocatedCast(dialect: string, nativeType: string): string | null {
   const read = bigIntegerReadOf(
      { name: "n", type: "number", numberType: "bigint" },
      { name: "n", type: "BIGINT" },
      { dialect, colocated: true, nativeTypeOf: () => nativeType },
   );
   return read?.castTo ?? null;
}

async function ownershipProbed(dialect: string): Promise<boolean> {
   let ran = false;
   await ownedElsewhere(
      {
         dialect,
         segments: (name) => ["sch", name],
         run: async () => {
            ran = true;
            return [];
         },
      },
      "orders",
      "env/pkg",
   );
   return ran;
}

async function observe(): Promise<Record<string, Record<Dialect, unknown>>> {
   const per = async (
      fn: (d: Dialect) => unknown,
   ): Promise<Record<Dialect, unknown>> => {
      const out = {} as Record<Dialect, unknown>;
      for (const d of DIALECTS) out[d] = await fn(d);
      return out;
   };
   const merge = declaration({
      declaredMergeKey: true,
      mergeKeys: [ID_KEY],
      strategy: "merge",
   } as Partial<IncrementalDeclaration>);
   const pair = declaration({
      declaredMergeKey: true,
      mergeKeys: [ID_KEY],
      strategy: "merge",
      pair: { parentKey: "id", parentRow: "__parent_row" },
   } as Partial<IncrementalDeclaration>);
   return {
      sourceRangeSet: await per((d) =>
         INCREMENTAL_SOURCE_RANGE_DIALECTS.has(d),
      ),
      targetDmlSet: await per((d) => INCREMENTAL_TARGET_DML_DIALECTS.has(d)),
      mergeSet: await per((d) => MERGE_CAPABLE_DIALECTS.has(d)),
      lineageAsSource: await per((d) => lineageFor(d, "duckdb")),
      lineageAsTarget: await per((d) => lineageFor("postgres", d)),
      pairLineageAsTarget: await per((d) => lineageFor("postgres", d, pair)),
      policyRange: await per((d) => policy(d, declaration())),
      policyMerge: await per((d) => policy(d, merge)),
      probesServerVersion: await per(probesServerVersion),
      mergeRefusedBelowFloor: await per(mergeRefusedBelowFloor),
      swapsInOneCall: await per(swapsInOneCall),
      relationProbeSupported: await per((d) => relationProbeSupported(d)),
      ownershipProbed: await per(ownershipProbed),
      viewDrop: await per(viewDrop),
      materializeRefusedWithoutName: await per(materializeRefused),
      v1KeepsNest: await per(v1KeepsNest),
      capturedKeepsNest: await per(capturedKeepsNest),
      liveKeepsBigIntegers: await per((d) => liveKeepsBigIntegers(d)),
      colocatedCastBigint: await per((d) => colocatedCast(d, "bigint")),
      colocatedCastDecimal: await per((d) => colocatedCast(d, "decimal(38,0)")),
      tableNamesFoldCase: await per((d) => tableNamesFoldCase(d, false)),
   };
}

/**
 * Each answer as decided. A MERGE refusal is subsumed today by the range
 * refusal (no dialect has a range and no MERGE), so `policyMerge` reads as
 * `policyRange`.
 */
const DECIDED: Record<string, Record<Dialect, unknown>> = {
   sourceRangeSet: {
      duckdb: false,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   targetDmlSet: {
      duckdb: true,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   mergeSet: {
      duckdb: true,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   lineageAsSource: {
      duckdb: false,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   lineageAsTarget: {
      duckdb: true,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   pairLineageAsTarget: {
      duckdb: true,
      postgres: false,
      standardsql: false,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   policyRange: {
      duckdb: "no range",
      postgres: "accepted",
      standardsql: "accepted",
      snowflake: "accepted",
      databricks: "no range",
      trino: "no range",
      presto: "no range",
      mysql: "no range",
      sqlserver: "no range",
      unknown_dialect: "no range",
   },
   policyMerge: {
      duckdb: "no range",
      postgres: "accepted",
      standardsql: "accepted",
      snowflake: "accepted",
      databricks: "no range",
      trino: "no range",
      presto: "no range",
      mysql: "no range",
      sqlserver: "no range",
      unknown_dialect: "no range",
   },
   probesServerVersion: {
      duckdb: false,
      postgres: true,
      standardsql: false,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   mergeRefusedBelowFloor: {
      duckdb: false,
      postgres: true,
      standardsql: false,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   swapsInOneCall: {
      duckdb: true,
      postgres: true,
      standardsql: false,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   relationProbeSupported: {
      duckdb: true,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: true,
      trino: true,
      presto: false,
      mysql: false,
      sqlserver: true,
      unknown_dialect: false,
   },
   ownershipProbed: {
      duckdb: true,
      postgres: true,
      standardsql: true,
      snowflake: true,
      databricks: true,
      trino: true,
      presto: true,
      mysql: true,
      sqlserver: true,
      unknown_dialect: false,
   },
   viewDrop: {
      duckdb: "in transaction, definition guard",
      postgres: "in transaction, dependency guard",
      standardsql: "before the build",
      snowflake: "before the build",
      databricks: "before the build",
      trino: "before the build",
      presto: "before the build",
      mysql: "before the build",
      sqlserver: "before the build",
      unknown_dialect: "before the build",
   },
   materializeRefusedWithoutName: {
      duckdb: false,
      postgres: false,
      standardsql: true,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   v1KeepsNest: {
      duckdb: true,
      postgres: false,
      standardsql: false,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   capturedKeepsNest: {
      duckdb: true,
      postgres: false,
      standardsql: false,
      snowflake: false,
      databricks: false,
      trino: false,
      presto: false,
      mysql: false,
      sqlserver: false,
      unknown_dialect: false,
   },
   liveKeepsBigIntegers: {
      duckdb: true,
      postgres: false,
      standardsql: true,
      snowflake: true,
      databricks: false,
      trino: false,
      presto: false,
      mysql: true,
      sqlserver: false,
      unknown_dialect: true,
   },
   colocatedCastBigint: {
      duckdb: "bigint",
      postgres: "bigint",
      standardsql: null,
      snowflake: "bigint",
      databricks: "bigint",
      trino: "bigint",
      presto: "bigint",
      mysql: null,
      sqlserver: "bigint",
      unknown_dialect: null,
   },
   colocatedCastDecimal: {
      duckdb: null,
      postgres: null,
      standardsql: null,
      snowflake: "decimal(38,0)",
      databricks: null,
      trino: null,
      presto: null,
      mysql: "decimal(38,0)",
      sqlserver: "decimal(38,0)",
      unknown_dialect: null,
   },
   tableNamesFoldCase: {
      duckdb: true,
      postgres: false,
      standardsql: false,
      snowflake: false,
      databricks: true,
      trino: true,
      presto: true,
      mysql: false,
      sqlserver: true,
      unknown_dialect: false,
   },
};

describe("dialect capabilities", () => {
   it("answer per dialect as decided", async () => {
      expect(await observe()).toEqual(DECIDED);
   });
});
