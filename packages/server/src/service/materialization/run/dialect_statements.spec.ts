// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every statement whose text depends on the warehouse dialect, per dialect,
 * frozen in `dialect_statements.golden.json`: the ownership probe and mark,
 * the relation and view probes and the view drop, the incremental refresh's
 * probes, bounds, range replace, merge and pair delta, the colocated swap
 * and drop, the native-type catalog read, the read-cost and query-tag
 * statements, and the leftover `name=` view's in-transaction drop. A statement a dialect
 * refuses is recorded as the refusal's message.
 *
 * It pins the text so that moving these emitters behind one dialect table
 * can be shown to leave every statement byte-identical. Change the golden
 * only in a commit that says why; to regenerate:
 *
 *    UPDATE_GOLDENS=1 bun test src/service/materialization/run/dialect_statements.spec.ts
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   bigQueryQueryLabelValue,
   snowflakeSetQueryTagSQL,
} from "./build_query_tag";
import { ColocatedTier, swapIntoPlace } from "./colocated_build_strategy";
import { capturedNativeSchema } from "./colocated_native_types";
import { describeDialects } from "../domain/dialect";
import {
   decodeTablePathSegments,
   deltaScript,
   deltaSelect,
   deltaStatements,
   INCREMENTAL_SOURCE_RANGE_DIALECTS,
   INCREMENTAL_TARGET_DML_DIALECTS,
   MERGE_CAPABLE_DIALECTS,
   pairDeltaStatements,
   probeMaxWatermark,
   probePostgresVersion,
   probeTargetColumns,
   renderSqlBound,
   type WatermarkBound,
} from "./incremental_apply";
import { replaceOwnViewStatements } from "./leftover_name_views";
import {
   BIGQUERY_COST_COLUMNS,
   dropViewSql,
   ownedTableProbeSql,
   probeSelect,
   relationProbeSql,
   relationProbeSupported,
   snowflakeCostSQL,
   snowflakeScriptingBlock,
   sqlString,
   viewReadsSql,
} from "./warehouse_sql";
import { ownedTableMarkSql } from "./table_ownership";

const GOLDEN = path.join(__dirname, "dialect_statements.golden.json");

/** Every dialect a statement branches on, and one none of them names. */
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

/** A table path of one, two and three segments, with a quote and a space. */
const PATHS: Record<string, string[]> = {
   one: ["orders"],
   two: ["Sales Data", "orders'x"],
   three: ["cat", "sch", "Orders"],
};

/** A statement, or the refusal a dialect answers it with. */
function attempt(fn: () => unknown): unknown {
   try {
      return fn();
   } catch (err) {
      return { refused: err instanceof Error ? err.message : String(err) };
   }
}

async function attemptAsync(fn: () => Promise<unknown>): Promise<unknown> {
   try {
      return await fn();
   } catch (err) {
      return { refused: err instanceof Error ? err.message : String(err) };
   }
}

/** Each dialect's answer to `fn`. */
function perDialect(fn: (dialect: string) => unknown): Record<string, unknown> {
   return Object.fromEntries(DIALECTS.map((d) => [d, attempt(() => fn(d))]));
}

async function perDialectAsync(
   fn: (dialect: string) => Promise<unknown>,
): Promise<Record<string, unknown>> {
   const out: Record<string, unknown> = {};
   for (const d of DIALECTS) out[d] = await attemptAsync(() => fn(d));
   return out;
}

/** A runner that records each statement and answers with no row. */
function recorder(): {
   statements: string[];
   run: (sql: string) => Promise<{ rows: unknown[] }>;
} {
   const statements: string[] = [];
   return {
      statements,
      run: async (sql: string) => {
         statements.push(sql);
         return { rows: [] };
      },
   };
}

const START: WatermarkBound = {
   malloyType: "timestamp",
   value: "2026-01-01T00:00:00",
};
const END: WatermarkBound = {
   malloyType: "timestamp",
   value: "2026-02-01T00:00:00",
};
const BOUNDS: Record<string, WatermarkBound> = {
   date: { malloyType: "date", value: "2026-01-02" },
   timestamp: START,
   number: { malloyType: "number", value: "42.5" },
   string: { malloyType: "string", value: "it's a \\ test" },
};

async function statements(): Promise<Record<string, unknown>> {
   const out: Record<string, unknown> = {};

   // The relation, ownership and view probes, the mark and the view drop.
   out.relationProbeSupported = perDialect((d) => relationProbeSupported(d));
   out.sqlString = perDialect((d) => sqlString("it's \\ here", d));
   for (const [shape, segments] of Object.entries(PATHS)) {
      out[`relationProbeSql.${shape}`] = perDialect((d) =>
         relationProbeSql(segments, d),
      );
      out[`ownedTableProbeSql.${shape}`] = perDialect((d) =>
         ownedTableProbeSql(segments, d),
      );
      out[`ownedTableMarkSql.${shape}`] = perDialect((d) =>
         ownedTableMarkSql(segments, "env/pkg's", d),
      );
      out[`viewReadsSql.${shape}`] = perDialect((d) =>
         viewReadsSql(segments, [...segments.slice(0, -1), "target"], d),
      );
      out[`dropViewSql.${shape}`] = perDialect((d) => dropViewSql(segments, d));
   }
   out.replaceOwnViewStatements = perDialect((d) =>
      replaceOwnViewStatements(
         {
            name: "v",
            target: "t__m1",
            comment: "malloy-publisher:name-view owner=env%2Fpkg",
            definition: "CREATE VIEW v AS SELECT * FROM t__m1;",
         },
         { dialect: d, segments: (name) => ["sch", name] },
      ),
   );

   // The incremental refresh: dialect sets, paths, probes, bounds and DML.
   out.incrementalDialects = {
      sourceRange: describeDialects(INCREMENTAL_SOURCE_RANGE_DIALECTS),
      targetDml: describeDialects(INCREMENTAL_TARGET_DML_DIALECTS),
      merge: describeDialects(MERGE_CAPABLE_DIALECTS),
   };
   out.decodeTablePathSegments = perDialect((d) => ({
      bare: decodeTablePathSegments(d, "Cat.Sch.Orders") ?? null,
      quoted: decodeTablePathSegments(d, '"Cat"."sch"."Or ders"') ?? null,
      backtick: decodeTablePathSegments(d, "`p-1`.ds.t") ?? null,
   }));
   out.probeSelect = perDialect((d) => probeSelect(d, "SELECT 1 AS n"));
   out.probeTargetColumns = await perDialectAsync(async (d) => {
      const r = recorder();
      const answer = await probeTargetColumns(r.run, d, "lake.sch.t__m1");
      return { statements: r.statements, error: answer.error ?? null };
   });
   out.probeMaxWatermark = await perDialectAsync(async (d) => {
      const r = recorder();
      await probeMaxWatermark(
         r.run,
         d,
         "SELECT * FROM src",
         "updated_at",
         "timestamp",
      );
      return r.statements;
   });
   {
      const r = recorder();
      await probePostgresVersion(r.run);
      out.probePostgresVersion = r.statements;
   }
   out.renderSqlBound = perDialect((d) =>
      Object.fromEntries(
         Object.entries(BOUNDS).map(([type, bound]) => [
            type,
            renderSqlBound(bound, d),
         ]),
      ),
   );
   out.deltaSelect = perDialect((d) =>
      deltaSelect({
         dialect: d,
         sourceSQL: "SELECT * FROM src",
         watermarkName: "updated_at",
         start: START,
         end: END,
      }),
   );
   const delta = (dialect: string) => ({
      dialect,
      quotedTablePath: "tgt",
      deltaSQL: "SELECT * FROM src",
      columns: ["id", "amount", "updated_at"],
      watermarkName: "updated_at",
      start: START,
      end: END,
   });
   out["deltaStatements.range"] = perDialect((d) =>
      deltaStatements({ ...delta(d), mergeKeys: [] }),
   );
   out["deltaStatements.rangeWithWindow"] = perDialect((d) =>
      deltaStatements({
         ...delta(d),
         mergeKeys: [],
         windowDeletes: [{ column: "updated_at", before: START }],
      }),
   );
   out["deltaStatements.merge"] = perDialect((d) =>
      deltaStatements({
         ...delta(d),
         mergeKeys: ["id"],
         scopeColumns: ["org"],
      }),
   );
   out["deltaStatements.mergeWithWindow"] = perDialect((d) =>
      deltaStatements({
         ...delta(d),
         mergeKeys: ["id"],
         windowDeletes: [{ column: "updated_at", before: START }],
      }),
   );
   out["deltaStatements.mergeAllKeys"] = perDialect((d) =>
      deltaStatements({ ...delta(d), columns: ["id"], mergeKeys: ["id"] }),
   );
   out.pairDeltaStatements = perDialect((d) =>
      pairDeltaStatements({
         dialect: d,
         quotedTablePath: "tgt",
         deltaSQL: "SELECT * FROM src",
         columns: ["id", "child_id", "__parent_row"],
         pair: {
            parentKey: "id",
            parentRow: "__parent_row",
            stagingTable: "tgt__stage",
            checks: [
               { flag: "__parent_row", keys: ["id"], lane: 0 },
               { flag: "__level_1_row", keys: ["id", "child_id"], lane: 1 },
            ],
            laneColumn: "__lane",
            lanes: 2,
         },
      }),
   );
   out.snowflakeScriptingBlock = snowflakeScriptingBlock([
      "DELETE FROM t",
      "INSERT INTO t SELECT 1",
   ]);
   out.deltaScript = deltaScript(["BEGIN", "DELETE FROM t", "COMMIT"]);

   // The colocated swap, as the statements it sends on a clean run.
   out.swapIntoPlace = await perDialectAsync(async (d) => {
      const r = recorder();
      await swapIntoPlace({
         connection: { runSQL: r.run } as never,
         runOptions: {},
         dialect: d,
         physicalTableName: "sch.orders__m1",
         stagingTableName: "sch.orders__m1__staging",
         backupTableName: "sch.orders__m1__backup",
         connectionName: "wh",
      });
      return r.statements;
   });
   out.swapIntoPlaceReplacingAView = await perDialectAsync(async (d) => {
      const r = recorder();
      await swapIntoPlace({
         connection: { runSQL: r.run } as never,
         runOptions: {},
         dialect: d,
         physicalTableName: "sch.orders",
         stagingTableName: "sch.orders__staging",
         backupTableName: "sch.orders__backup",
         connectionName: "wh",
         replacesView: ["DROP VIEW IF EXISTS sch.orders"],
      });
      return r.statements;
   });

   // The swap's failure paths: the statement at `failAt` (1-based) fails
   // once, as a warehouse error that is not a missing table, and every
   // statement sent is recorded with the refusal the swap answers with.
   const failingSwap = async (
      dialect: string,
      failAt: number,
      error = "permission denied",
   ) => {
      const statements: string[] = [];
      let n = 0;
      const runSQL = async (sql: string) => {
         statements.push(sql);
         n++;
         if (n === failAt) throw new Error(error);
         return { rows: [] };
      };
      let refused: string | null = null;
      try {
         await swapIntoPlace({
            connection: { runSQL } as never,
            runOptions: {},
            dialect,
            physicalTableName: "sch.orders__m1",
            stagingTableName: "sch.orders__m1__staging",
            backupTableName: "sch.orders__m1__backup",
            connectionName: "wh",
         });
      } catch (err) {
         refused = err instanceof Error ? err.message : String(err);
      }
      return { statements, refused };
   };
   // The first statement: the transaction, or the move aside.
   out["swapIntoPlace.failsFirst"] = await perDialectAsync((d) =>
      failingSwap(d, 1),
   );
   // The second: the rename onto the served name, where the move aside
   // came first.
   out["swapIntoPlace.failsSecond"] = await perDialectAsync((d) =>
      failingSwap(d, 2),
   );
   // The move aside answers a missing table: the first build of the name,
   // where no probe runs.
   out["swapIntoPlace.firstBuild"] = await perDialectAsync((d) =>
      failingSwap(d, 1, "Table sch.orders__m1 does not exist"),
   );
   // The third: the backup's drop once the new table holds the name.
   out["swapIntoPlace.failsThird"] = await perDialectAsync((d) =>
      failingSwap(d, 3),
   );

   // The colocated drop: the table, then the staging and backup names a
   // crash can leave beside it when the table names its source.
   const colocatedDrop = (leftoversOf?: string) =>
      perDialectAsync(async (d) => {
         const r = recorder();
         await new ColocatedTier().drop(
            {
               connection: { runSQL: r.run, dialectName: d } as never,
               runOptions: {},
            },
            { physicalTableName: "sch.Orders x__m1", leftoversOf },
         );
         return r.statements;
      });
   out["ColocatedTier.drop"] = await colocatedDrop();
   out["ColocatedTier.drop.leftoversOf"] = await colocatedDrop("orders");

   // The native-type catalog read.
   for (const [shape, segments] of Object.entries(PATHS)) {
      out[`capturedNativeSchema.${shape}`] = await perDialectAsync(
         async (d) => {
            const r = recorder();
            await capturedNativeSchema({
               run: (sql) => r.run(sql),
               physicalTableName: segments.join("."),
               dialect: d,
               sourceName: "orders",
            });
            return r.statements;
         },
      );
   }

   // Read cost and the query tag.
   out.bigQueryCostColumns = BIGQUERY_COST_COLUMNS;
   out.snowflakeCostSQL = {
      database: snowflakeCostSQL('{"run":"r\'1"}', "My DB"),
      noDatabase: snowflakeCostSQL('{"run":"r\'1"}', null),
   };
   const metadata = { team: "a\\b's", run: "r1" } as never;
   out.snowflakeSetQueryTagSQL = snowflakeSetQueryTagSQL(metadata, "sec'ret");
   out.bigQueryQueryLabelValue = bigQueryQueryLabelValue(metadata) ?? null;
   return out;
}

describe("every dialect-dependent statement", () => {
   it("matches its golden, per dialect", async () => {
      const text = `${JSON.stringify(await statements(), null, 2)}\n`;
      if (process.env.UPDATE_GOLDENS === "1") {
         fs.writeFileSync(GOLDEN, text);
      }
      expect(text).toBe(fs.readFileSync(GOLDEN, "utf8"));
   });
});
