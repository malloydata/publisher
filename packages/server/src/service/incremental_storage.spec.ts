import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";

import {
   planIncrementalStep,
   probeTargetColumns,
   type IncrementalLineage,
   type RecordedBoundary,
} from "./incremental_apply";
import { storageDeltaTarget } from "./incremental_storage";
import { fakeSource } from "./materialization_test_fixtures";

/**
 * The delta a `storage=` source gets, where the refresh spans two engines: the
 * source warehouse computes the rows, the destination engine applies the DML.
 *
 * These tests are about WHICH SIDE each piece of SQL ends up on. Every mistake
 * available here produces SQL that runs and rows that are plausible — a predicate
 * applied outside the passthrough streams the whole source and then filters it,
 * a probe wrapped for Postgres reads `undefined` off a DuckDB session forever —
 * so the assertions are on the shape of the statements, not only their effect.
 */

const NOW = new Date("2024-06-15T00:00:00Z");
const SOURCE_SQL = "SELECT order_date, region, revenue FROM base";

const LINEAGE: IncrementalLineage = {
   physicalTableName: "orders__g000__abc",
   connectionName: "orders_pg",
   storageDestinationName: "lake",
   sourceEntityId: "addr-1",
   watermarkName: "order_date",
   watermarkType: "date",
   mergeKeys: [],
   strategy: "range_replace",
};

const BOUNDARY: RecordedBoundary = {
   sourceEntityId: "addr-1",
   coveredThroughValue: "2024-06-01",
   coveredThroughType: "date",
   watermarkDimension: "order_date",
   mergeKeyDimensions: [],
   derivedStrategy: "range_replace",
   physicalTableName: "orders__g000__abc",
   connectionName: "orders_pg",
   storageDestinationName: "lake",
};

/**
 * A build session that records every statement and answers the probes the planner
 * makes, so a test can assert on what was sent as well as on what came back.
 */
function fakeSession(answers: { columns?: string[]; nonEmpty?: boolean } = {}) {
   const seen: string[] = [];
   const session = {
      runSQL: async (sql: string) => {
         seen.push(sql);
         if (sql.includes("duckdb_columns()")) {
            return {
               rows: (
                  answers.columns ?? ["order_date", "region", "revenue"]
               ).map((column_name) => ({ column_name })),
            };
         }
         if (sql.includes("LIMIT 1")) {
            return { rows: answers.nonEmpty === false ? [] : [{ present: 1 }] };
         }
         if (sql.includes("MAX(")) {
            return { rows: [{ watermark_max: "2024-06-10" }] };
         }
         return { rows: [] };
      },
   } as unknown as DuckDBConnection;
   return { session, seen };
}

function targetFor(
   opts: {
      lineage?: IncrementalLineage;
      sourceDialect?: string;
      answers?: Parameters<typeof fakeSession>[0];
   } = {},
) {
   const { session, seen } = fakeSession(opts.answers);
   const { target, readCost } = storageDeltaTarget({
      session,
      sourceType: "postgres",
      handle: "orders_pg_db",
      destinationName: "lake",
      physicalTableName: "orders__g000__abc",
      lineage: opts.lineage ?? LINEAGE,
      persistSource: fakeSource({
         name: "orders",
         sourceEntityId: "addr-1",
         sql: SOURCE_SQL,
         connectionName: "orders_pg",
         dialectName: opts.sourceDialect ?? "postgres",
         columns: ["order_date", "region", "revenue"],
      }),
      buildSQL: SOURCE_SQL,
   });
   return { target, readCost, seen };
}

describe("storageDeltaTarget", () => {
   it("names the destination-qualified table, quoted for DuckDB", () => {
      const { target } = targetFor();
      expect(target.dialect).toBe("duckdb");
      expect(target.quotedTablePath).toBe(`"lake"."orders__g000__abc"`);
      // The metadata read needs the logical path, and the destination catalog is
      // part of it — the lineage's physicalTableName alone does not name it.
      expect(target.logicalTablePath).toBe("lake.orders__g000__abc");
   });

   it("pushes the range predicate INSIDE the passthrough", async () => {
      const { target } = targetFor();
      const sql = await target.deltaRows(
         { malloyType: "date", value: "2024-06-01" },
         { malloyType: "date", value: "2024-06-15" },
      );
      // One passthrough call, with the whole bounded query as its SQL literal.
      expect(sql).toContain("postgres_query('orders_pg_db'");
      const literal = sql.slice(sql.indexOf("postgres_query"));
      expect(literal).toContain("order_date");
      expect(literal).toContain("DATE ''2024-06-01''");
      expect(literal).toContain("DATE ''2024-06-15''");
      // And NOT the other way round: a predicate outside the passthrough would
      // stream the whole source across the egress boundary before filtering it.
      expect(sql.split("postgres_query")[0]).not.toContain("order_date >=");
   });

   it("projects the public columns OUTSIDE the range predicate", async () => {
      const { target } = targetFor();
      const sql = await target.deltaRows(
         { malloyType: "date", value: "2024-06-01" },
         { malloyType: "date", value: "2024-06-15" },
      );
      // Nested the same way the seed nests them, so the delta writes the seed's
      // column list by construction: the projection is outermost, the range
      // predicate inside it, the source's own SQL innermost. Read off the aliases,
      // which close inside-out — `AS __d` (the filter) before `AS __public` (the
      // projection).
      expect(sql.indexOf("AS __d")).toBeLessThan(sql.indexOf("AS __public"));
      // The projection names the columns explicitly, and it opens before the
      // predicate it wraps.
      expect(
         sql.indexOf(`"order_date", "region", "revenue" FROM`),
      ).toBeLessThan(sql.indexOf("WHERE"));
   });

   it("composes the frontier probe in the SOURCE dialect, inside the passthrough", async () => {
      const numeric: IncrementalLineage = {
         ...LINEAGE,
         watermarkName: "seq",
         watermarkType: "number",
      };
      const { target, seen } = targetFor({ lineage: numeric });
      await target.probeSourceFrontier();
      expect(seen).toHaveLength(1);
      // The warehouse aggregates and one row crosses; computing MAX() on the
      // DuckDB side instead would stream every row to get it.
      expect(seen[0]).toContain("postgres_query");
      expect(seen[0]).toContain("MAX(");
      // NOT wrapped in row_to_json. That wrapper exists for Malloy's Postgres
      // connector, which rewrites rows[i] = rows[i].row; this runner is a DuckDB
      // session, where wrapping would make every probe read undefined.
      expect(seen[0]).not.toContain("row_to_json");
   });

   it("reports no read cost until a read has actually been issued", async () => {
      const { target, readCost } = targetFor();
      expect(readCost()).toBeNull();
      await target.deltaRows(
         { malloyType: "date", value: "2024-06-01" },
         { malloyType: "date", value: "2024-06-15" },
      );
      // A rows-returning passthrough call reports no job, so this stays null —
      // which is not the same as free, and is why the getter exists rather than a
      // zero.
      expect(readCost()).toBeNull();
   });
});

describe("probeTargetColumns on a storage destination", () => {
   it("reads duckdb_columns(), scoped to the catalog and schema", async () => {
      const { session, seen } = fakeSession();
      const probe = await probeTargetColumns(
         (sql) => session.runSQL(sql) as Promise<{ rows: unknown[] }>,
         "duckdb",
         "lake.orders__g000__abc",
      );
      expect(probe.columns).toEqual(["order_date", "region", "revenue"]);
      // An attached DuckLake catalog exposes no information_schema at all.
      expect(seen[0]).not.toContain("information_schema");
      expect(seen[0]).toContain("duckdb_columns()");
      expect(seen[0]).toContain("database_name = 'lake'");
      // A two-segment path lands in the catalog's default schema, named rather
      // than left unconstrained: two schemas can hold the same table name.
      expect(seen[0]).toContain("schema_name = 'main'");
      expect(seen[0]).toContain("table_name = 'orders__g000__abc'");
   });

   it("scopes to a schema-qualified persist name when the path carries one", async () => {
      const { session, seen } = fakeSession();
      await probeTargetColumns(
         (sql) => session.runSQL(sql) as Promise<{ rows: unknown[] }>,
         "duckdb",
         "lake.analytics.orders",
      );
      expect(seen[0]).toContain("database_name = 'lake'");
      expect(seen[0]).toContain("schema_name = 'analytics'");
      expect(seen[0]).toContain("table_name = 'orders'");
   });

   it("reports an absent table as no columns, which is the SEED signal", async () => {
      const { session } = fakeSession({ columns: [] });
      const probe = await probeTargetColumns(
         (sql) => session.runSQL(sql) as Promise<{ rows: unknown[] }>,
         "duckdb",
         "lake.gone",
      );
      // Emptily, not by throwing: DESCRIBE would throw here, and the planner
      // would report `table_unreadable` where it means "there is no table yet".
      expect(probe.columns).toEqual([]);
      expect(probe.error).toBeUndefined();
   });
});

describe("planIncrementalStep against a storage destination", () => {
   const plan = (
      opts: {
         lineage?: IncrementalLineage;
         ledgerEntry?: RecordedBoundary | null;
         answers?: Parameters<typeof fakeSession>[0];
      } = {},
   ) => {
      const { target, seen } = targetFor({
         lineage: opts.lineage,
         answers: opts.answers,
      });
      return {
         seen,
         step: planIncrementalStep({
            target,
            lineage: opts.lineage ?? LINEAGE,
            ledgerEntry:
               opts.ledgerEntry === undefined ? BOUNDARY : opts.ledgerEntry,
            forceRefresh: false,
            now: NOW,
            columns: ["order_date", "region", "revenue"],
         }),
      };
   };

   it("range-replaces inside a DuckDB transaction", async () => {
      const step = await plan().step;
      expect(step.mode).toBe("delta");
      if (step.mode !== "delta") return;
      // Four statements in one script: the DELETE and the INSERT have to commit
      // or roll back together, or a failure between them leaves the range empty
      // in a table that is already serving.
      expect(step.statements[0]).toBe("BEGIN");
      expect(step.statements[3]).toBe("COMMIT");
      expect(step.statements[1]).toContain(
         `DELETE FROM "lake"."orders__g000__abc"`,
      );
      expect(step.statements[1]).toContain(`"order_date" >= DATE '2024-06-01'`);
      expect(step.statements[2]).toContain(
         `INSERT INTO "lake"."orders__g000__abc"`,
      );
      // The rows come through the passthrough, so the warehouse computed them.
      expect(step.statements[2]).toContain("postgres_query");
   });

   it("merges by row identity when the source declares merge_key=", async () => {
      const keyed: IncrementalLineage = {
         ...LINEAGE,
         mergeKeys: ["region"],
         strategy: "merge",
      };
      const step = await plan({
         lineage: keyed,
         ledgerEntry: {
            ...BOUNDARY,
            mergeKeyDimensions: ["region"],
            derivedStrategy: "merge",
         },
      }).step;
      expect(step.mode).toBe("delta");
      if (step.mode !== "delta") return;
      // One statement, so no transaction is needed around it.
      expect(step.statements).toHaveLength(1);
      expect(step.statements[0]).toContain(
         `MERGE INTO "lake"."orders__g000__abc" AS __t`,
      );
      expect(step.statements[0]).toContain("WHEN MATCHED THEN UPDATE SET");
   });

   it("seeds when the boundary was measured somewhere else", async () => {
      // The same table name and the same content address, in the source's own
      // warehouse rather than the destination — which is exactly what a source
      // that just gained or lost `storage=` looks like, since an annotation
      // enters neither the address nor the name.
      const step = await plan({
         ledgerEntry: { ...BOUNDARY, storageDestinationName: undefined },
      }).step;
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reasonCode).toBe("table_renamed");
      expect(step.reason).toContain("the source warehouse");
   });

   it("seeds when the destination holds no such table yet", async () => {
      const step = await plan({ answers: { columns: [] } }).step;
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reasonCode).toBe("table_unreadable");
   });
});
