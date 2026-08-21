// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The delta SELECT composer and its literal rendering. The compiler behaviors
// these rest on (getSQL() returns the source's unfinalized SQL; a rename diverges
// the table's columns from the source's schema) are pinned in
// incremental_compiler_contract.spec.ts; here we check the SQL we compose, and
// that it really runs.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { FixedConnectionMap, ModelMaterializer } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";
import {
   canonicalBoundValue,
   compareBounds,
   decodeTablePathSegments,
   deltaScript,
   deltaStatements,
   ledgerLineageMismatch,
   deltaSelect,
   planIncrementalStep,
   probeMaxWatermark,
   probeSelect,
   probeTargetColumns,
   renderSqlBound,
   seedCoveredThrough,
   shapeMismatch,
   snapshotBound,
   snowflakeScriptingBlock,
   type SqlRunner,
} from "./incremental_apply";

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;

beforeAll(() => {
   ({ duckdb, connections } = duckdbTestConnections());
});

function materialize(model: string): ModelMaterializer {
   return loadTestModel(connections, model);
}

const MODEL = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT 1 AS amount, DATE '2024-01-01' AS order_date,
         TIMESTAMP '2024-01-01 00:00:00' AS ts, 'US' AS region
""")

#@ persist name="t" refresh="incremental" watermark="order_date"
source: daily is raw -> {
  group_by: order_date, ts, region
  aggregate: revenue is amount.sum()
}`;

describe("renderSqlBound", () => {
   it("spells each type as its own SQL literal", () => {
      expect(renderSqlBound({ malloyType: "date", value: "2024-06-01" })).toBe(
         "DATE '2024-06-01'",
      );
      expect(
         renderSqlBound({
            malloyType: "timestamp",
            value: "2024-06-01T12:30:00.000Z",
         }),
      ).toBe("TIMESTAMP '2024-06-01 12:30:00'");
      expect(renderSqlBound({ malloyType: "number", value: "42" })).toBe("42");
   });

   it("doubles a backslash only where it is an escape character", () => {
      // Snowflake and BigQuery read `\` in a single-quoted string as an escape,
      // so a value ending in one would swallow the closing quote. Postgres
      // reads it literally, so doubling there would change the value.
      const bound = { malloyType: "string", value: "acme\\" };
      expect(renderSqlBound(bound, "snowflake")).toBe(`'acme\\\\'`);
      expect(renderSqlBound(bound, "standardsql")).toBe(`'acme\\\\'`);
      expect(renderSqlBound(bound, "postgres")).toBe(`'acme\\'`);
   });

   it("doubles a quote in a string bound rather than letting it break out", () => {
      expect(renderSqlBound({ malloyType: "string", value: "o'brien" })).toBe(
         "'o''brien'",
      );
   });

   it("truncates a timestamp to whole seconds", () => {
      // Safe because the range is half-open and the ledger records the value
      // used: the trailing sub-second rows are picked up by the next run, which
      // starts exactly where this one ended.
      expect(
         renderSqlBound({
            malloyType: "timestamp",
            value: "2024-06-01T12:30:00.512Z",
         }),
      ).toBe("TIMESTAMP '2024-06-01 12:30:00'");
   });

   it("refuses a type it cannot render, instead of guessing", () => {
      expect(() =>
         renderSqlBound({ malloyType: "boolean", value: "true" }),
      ).toThrow(/no literal rendering/);
   });

   it("refuses a malformed value rather than pasting it into a statement", () => {
      expect(() =>
         renderSqlBound({ malloyType: "date", value: "yesterday" }),
      ).toThrow(/ISO-8601/);
      expect(() =>
         renderSqlBound({ malloyType: "number", value: "1; DROP TABLE t" }),
      ).toThrow(/finite number/);
   });
});

describe("canonicalBoundValue", () => {
   it("collapses the shapes a driver hands back", () => {
      const date = new Date("2024-06-01T12:30:00.512Z");
      expect(canonicalBoundValue("date", date)).toEqual({
         malloyType: "date",
         value: "2024-06-01",
      });
      expect(canonicalBoundValue("timestamp", date)).toEqual({
         malloyType: "timestamp",
         value: "2024-06-01T12:30:00",
      });
      // BigQuery wraps temporal values.
      expect(canonicalBoundValue("date", { value: "2024-06-01" })).toEqual({
         malloyType: "date",
         value: "2024-06-01",
      });
      expect(canonicalBoundValue("number", 17)).toEqual({
         malloyType: "number",
         value: "17",
      });
      expect(canonicalBoundValue("string", "us-east")).toEqual({
         malloyType: "string",
         value: "us-east",
      });
   });

   it("refuses a null watermark instead of producing an unbounded range", () => {
      expect(() => canonicalBoundValue("date", null)).toThrow(/null/);
   });

   it("round-trips a snapshot time through the literal renderer", () => {
      const bound = snapshotBound(
         "timestamp",
         new Date("2024-06-01T12:30:00Z"),
      );
      expect(renderSqlBound(bound)).toBe("TIMESTAMP '2024-06-01 12:30:00'");
   });
});

describe("deltaSelect", () => {
   it("wraps the source's own SQL in a half-open range, quoted for the dialect", () => {
      expect(
         deltaSelect({
            dialect: "postgres",
            sourceSQL: "SELECT 1 AS amount, DATE '2024-01-01' AS order_date",
            watermarkName: "order_date",
            start: { malloyType: "date", value: "2024-06-01" },
            end: { malloyType: "date", value: "2024-07-01" },
         }),
      ).toBe(
         `SELECT * FROM (SELECT 1 AS amount, DATE '2024-01-01' AS order_date) ` +
            `AS __d WHERE "order_date" >= DATE '2024-06-01' ` +
            `AND "order_date" < DATE '2024-07-01'`,
      );
   });

   it("filters the source's OUTPUT rows, and returns real columns", async () => {
      // The regression this composition exists for, end to end on a dialect that
      // can run it. The delta is built from PersistSource.getSQL(), so it selects
      // the seed's own columns; a delta built from a compiled QUERY's SQL would
      // return a single json `row` column on Postgres (Dialect.hasFinalStage) and
      // the INSERT would fail on the first column it named. Pinned in
      // incremental_compiler_contract.spec.ts.
      const compiled = await materialize(MODEL).getModel();
      const source = Object.values(compiled.getBuildPlan().sources).find(
         (s) => s.name === "daily",
      );
      const sql = deltaSelect({
         dialect: "duckdb",
         sourceSQL: source!.getSQL({ buildManifest: { entries: {} } }),
         watermarkName: "order_date",
         start: { malloyType: "date", value: "2023-12-31" },
         end: { malloyType: "date", value: "2024-01-02" },
      });
      const result = await duckdb.runSQL(sql);
      expect(result.rows).toHaveLength(1);
      // The columns the seed's CTAS materializes, each on its own — the shape the
      // DML names. A finalized query's SQL would return one column here.
      expect(Object.keys(result.rows[0])).toEqual([
         "order_date",
         "ts",
         "region",
         "revenue",
      ]);
      expect(result.rows[0].region).toBe("US");
      // Not vacuous: the range is what excluded everything else, and it applied
      // ABOVE the source's GROUP BY, to its aggregated output rows.
      expect(sql.indexOf("WHERE")).toBeGreaterThan(sql.indexOf("GROUP BY"));
   });

   it("selects nothing for a range the source has no rows in", async () => {
      const compiled = await materialize(MODEL).getModel();
      const source = Object.values(compiled.getBuildPlan().sources).find(
         (s) => s.name === "daily",
      );
      const result = await duckdb.runSQL(
         deltaSelect({
            dialect: "duckdb",
            sourceSQL: source!.getSQL({ buildManifest: { entries: {} } }),
            watermarkName: "order_date",
            start: { malloyType: "date", value: "2030-01-01" },
            end: { malloyType: "date", value: "2030-02-01" },
         }),
      );
      expect(result.rows).toEqual([]);
   });
});

// ── The probes ────────────────────────────────────────────────────────────

describe("probeSelect", () => {
   it("wraps a Postgres probe in row_to_json, and leaves BigQuery alone", () => {
      // Not cosmetic: PostgresConnection.runSQL rewrites every result row as
      // `row.row`, so an unwrapped `SELECT max(x) AS m` comes back as
      // [undefined]. See the comment on probeSelect.
      expect(probeSelect("postgres", "SELECT max(a) AS m FROM t")).toBe(
         `SELECT row_to_json(__p) AS "row" FROM (SELECT max(a) AS m FROM t) AS __p`,
      );
      expect(probeSelect("standardsql", "SELECT max(a) AS m FROM t")).toBe(
         "SELECT max(a) AS m FROM t",
      );
   });
});

describe("decodeTablePathSegments", () => {
   it("case-folds a bare Postgres segment and preserves a quoted one", () => {
      expect(decodeTablePathSegments("postgres", "MySchema.MyTable")).toEqual([
         "myschema",
         "mytable",
      ]);
      expect(
         decodeTablePathSegments("postgres", '"MySchema"."MyTable"'),
      ).toEqual(["MySchema", "MyTable"]);
   });

   it("leaves a BigQuery segment's case alone", () => {
      // BigQuery dataset and table names are case-sensitive, so folding here
      // would probe a table that does not exist.
      expect(
         decodeTablePathSegments("standardsql", "proj-x.MyDataset.MyTable"),
      ).toEqual(["proj-x", "MyDataset", "MyTable"]);
   });

   it("folds a bare Snowflake segment to UPPER case and preserves a quoted one", () => {
      // Snowflake folds the opposite way from Postgres, and information_schema
      // stores the folded form — lowercase folding here would probe a table
      // that does not exist.
      expect(decodeTablePathSegments("snowflake", "MySchema.MyTable")).toEqual([
         "MYSCHEMA",
         "MYTABLE",
      ]);
      expect(
         decodeTablePathSegments("snowflake", '"MySchema"."MyTable"'),
      ).toEqual(["MySchema", "MyTable"]);
   });

   it("returns undefined for a path it cannot decode, rather than guessing", () => {
      expect(
         decodeTablePathSegments("postgres", "proj-x.ds.tbl"),
      ).toBeUndefined();
      expect(decodeTablePathSegments("duckdb", "t")).toBeUndefined();
   });
});

describe("probeTargetColumns", () => {
   it("qualifies a Postgres probe with the named schema", async () => {
      const seen: string[] = [];
      const result = await probeTargetColumns(
         async (sql) => {
            seen.push(sql);
            return { rows: [{ column_name: "a" }, { column_name: "b" }] };
         },
         "postgres",
         "analytics.daily",
      );
      expect(result.columns).toEqual(["a", "b"]);
      expect(seen[0]).toContain("information_schema.columns");
      expect(seen[0]).toContain("table_schema = 'analytics'");
      expect(seen[0]).toContain("table_name = 'daily'");
      expect(seen[0]).toContain("row_to_json");
   });

   it("falls back to current_schema() for an unqualified Postgres name", async () => {
      // An unqualified persist target is created in the session's default
      // schema, so that is the only schema whose table this build wrote.
      let sql = "";
      await probeTargetColumns(
         async (s) => {
            sql = s;
            return { rows: [] };
         },
         "postgres",
         "daily",
      );
      expect(sql).toContain("table_schema = current_schema()");
   });

   it("reads BigQuery's per-dataset INFORMATION_SCHEMA", async () => {
      let sql = "";
      await probeTargetColumns(
         async (s) => {
            sql = s;
            return { rows: [{ column_name: "a" }] };
         },
         "standardsql",
         "proj-x.ds.daily",
      );
      expect(sql).toContain("`proj-x`.`ds`.INFORMATION_SCHEMA.COLUMNS");
      expect(sql).toContain("table_name = 'daily'");
      expect(sql).not.toContain("row_to_json");
   });

   it("probes Snowflake with a quoted alias, in the named database", async () => {
      // The quoted alias is what keeps the read key exact: Snowflake folds a
      // bare `AS column_name` to COLUMN_NAME, and the probe would then read
      // undefined from every row. The database qualifier matters because
      // Snowflake's information_schema is per-database.
      const seen: string[] = [];
      const result = await probeTargetColumns(
         async (sql) => {
            seen.push(sql);
            return { rows: [{ column_name: "a" }] };
         },
         "snowflake",
         "analytics.reporting.daily",
      );
      expect(result.columns).toEqual(["a"]);
      expect(seen[0]).toContain(`"ANALYTICS".information_schema.columns`);
      expect(seen[0]).toContain(`AS "column_name"`);
      expect(seen[0]).toContain("table_schema = 'REPORTING'");
      expect(seen[0]).toContain("table_name = 'DAILY'");
      expect(seen[0]).not.toContain("row_to_json");
   });

   it("falls back to current_schema() for an unqualified Snowflake name", async () => {
      let sql = "";
      await probeTargetColumns(
         async (s) => {
            sql = s;
            return { rows: [] };
         },
         "snowflake",
         "daily",
      );
      expect(sql).toContain("information_schema.columns");
      expect(sql).toContain("table_schema = current_schema()");
      expect(sql).toContain("table_name = 'DAILY'");
   });

   it("refuses an unqualified BigQuery name, which has no dataset to look in", async () => {
      const result = await probeTargetColumns(
         async () => ({ rows: [] }),
         "standardsql",
         "daily",
      );
      expect(result.columns).toEqual([]);
      expect(result.error).toContain("dataset-qualified");
   });

   it("reports an empty column list for an absent table, not a throw", async () => {
      // information_schema answers "no such table" with no rows, which is
      // exactly the seed signal.
      const result = await probeTargetColumns(
         async () => ({ rows: [] }),
         "postgres",
         "analytics.gone",
      );
      expect(result).toEqual({ columns: [] });
   });

   it("turns a failed probe into a reason, not an exception", async () => {
      const result = await probeTargetColumns(
         async () => {
            throw new Error("permission denied");
         },
         "postgres",
         "analytics.daily",
      );
      expect(result.columns).toEqual([]);
      expect(result.error).toContain("permission denied");
   });
});

describe("probeMaxWatermark", () => {
   it("reads the frontier over a SELECT and canonicalizes it", async () => {
      let sql = "";
      const result = await probeMaxWatermark(
         async (s) => {
            sql = s;
            return {
               rows: [{ watermark_max: new Date("2024-06-01T12:34:56Z") }],
            };
         },
         "postgres",
         "SELECT * FROM base",
         "order_date",
         "date",
      );
      expect(result.bound).toEqual({ malloyType: "date", value: "2024-06-01" });
      expect(sql).toContain(`MAX("order_date")`);
      expect(sql).toContain("FROM (SELECT * FROM base) AS __w");
      // Quoted so the read key stays exact on a case-folding dialect: Snowflake
      // returns a bare `AS watermark_max` as WATERMARK_MAX.
      expect(sql).toContain(`AS "watermark_max"`);
   });

   it("reports no bound for a NULL maximum, which is an empty input", async () => {
      const result = await probeMaxWatermark(
         async () => ({ rows: [{ watermark_max: null }] }),
         "standardsql",
         "SELECT * FROM base",
         "seq",
         "number",
      );
      expect(result).toEqual({});
   });
});

describe("compareBounds", () => {
   it("orders numbers numerically, not as text", () => {
      const a = { malloyType: "number", value: "9" };
      const b = { malloyType: "number", value: "10" };
      expect(compareBounds(a, b)).toBeLessThan(0);
   });

   it("orders fixed-width ISO text lexically", () => {
      const a = { malloyType: "timestamp", value: "2024-01-01 00:00:00" };
      const b = { malloyType: "timestamp", value: "2024-01-01 00:00:01" };
      expect(compareBounds(a, b)).toBeLessThan(0);
      expect(compareBounds(a, a)).toBe(0);
   });

   it("refuses to compare two different types", () => {
      expect(() =>
         compareBounds(
            { malloyType: "date", value: "2024-01-01" },
            { malloyType: "number", value: "1" },
         ),
      ).toThrow(/cannot compare/);
   });
});

describe("shapeMismatch", () => {
   it("ignores order but not membership", () => {
      expect(shapeMismatch(["a", "b"], ["b", "a"])).toBeUndefined();
      expect(shapeMismatch(["a", "order_date"], ["a", "ord_dt"])).toContain(
         `absent from the table: "order_date"`,
      );
      expect(shapeMismatch(["a", "order_date"], ["a", "ord_dt"])).toContain(
         `present only in the table: "ord_dt"`,
      );
   });
});

// ── The DML ───────────────────────────────────────────────────────────────

const RANGE = {
   start: { malloyType: "date", value: "2024-06-01" },
   end: { malloyType: "date", value: "2024-07-01" },
};

describe("deltaStatements: range replace", () => {
   const statements = (dialect: string) =>
      deltaStatements({
         dialect,
         quotedTablePath: `"analytics"."daily"`,
         deltaSQL: "SELECT 1",
         columns: ["order_date", "region", "revenue"],
         mergeKeys: [],
         watermarkName: "order_date",
         ...RANGE,
      });

   it("deletes exactly the half-open range, then inserts the delta", () => {
      const [begin, del, ins, commit] = statements("postgres");
      expect(begin).toBe("BEGIN");
      expect(del).toBe(
         `DELETE FROM "analytics"."daily" WHERE "order_date" >= DATE '2024-06-01' AND "order_date" < DATE '2024-07-01'`,
      );
      expect(ins).toContain(
         `INSERT INTO "analytics"."daily" ("order_date", "region", "revenue")`,
      );
      // Named on both sides, so the table's column ORDER never has to match the
      // delta's projection order.
      expect(ins).toContain(
         `SELECT "order_date", "region", "revenue" FROM (SELECT 1) AS __s`,
      );
      expect(commit).toBe("COMMIT");
   });

   it("uses each dialect's own transaction syntax", () => {
      expect(statements("postgres")[0]).toBe("BEGIN");
      expect(statements("standardsql")[0]).toBe("BEGIN TRANSACTION");
      expect(statements("standardsql")[3]).toBe("COMMIT TRANSACTION");
   });

   it("is one script, because the two statements must commit together", () => {
      // A per-statement loop would put BEGIN and COMMIT in different sessions on
      // a pooled connection, leaving the DELETE autocommitted on its own — the
      // range emptied in a table that is already serving.
      const script = deltaScript(statements("postgres"));
      expect(script.split(";\n")).toHaveLength(4);
      expect(script.endsWith("COMMIT;")).toBe(true);
   });

   it("is ONE Snowflake Scripting block on Snowflake, not a script", () => {
      // The Snowflake driver executes exactly one statement per call, each on a
      // possibly different pooled session — a multi-statement script is refused
      // outright and a statement-per-call loop would autocommit the DELETE on
      // its own. The block is the one form that carries the transaction.
      const all = statements("snowflake");
      expect(all).toHaveLength(1);
      const [block] = all;
      expect(block.startsWith("EXECUTE IMMEDIATE $$")).toBe(true);
      expect(block).toContain("BEGIN TRANSACTION;");
      expect(block).toContain(
         `DELETE FROM "analytics"."daily" WHERE "order_date" >= DATE '2024-06-01' AND "order_date" < DATE '2024-07-01';`,
      );
      expect(block).toContain(
         `INSERT INTO "analytics"."daily" ("order_date", "region", "revenue") SELECT "order_date", "region", "revenue" FROM (SELECT 1) AS __s;`,
      );
      expect(block).toContain("COMMIT;");
      // The rollback lives INSIDE the statement: a separate ROLLBACK call would
      // go to whichever pooled session the driver picks next, not the one
      // holding the open transaction.
      expect(block).toContain("EXCEPTION");
      expect(block).toContain("ROLLBACK;");
      expect(block).toContain("RAISE;");
   });

   it("refuses delta SQL containing the block's own $$ delimiter", () => {
      // Loud, not escaped: the throw lands in planSourceRefresh's catch and the
      // source seeds. No Malloy-generated SQL contains $$ (string literals are
      // backslash-escaped on Snowflake), so this only fires on something that
      // is not the SQL we meant to run anyway.
      expect(() => snowflakeScriptingBlock(["SELECT '$$'"])).toThrow(
         "Snowflake Scripting block",
      );
   });
});

describe("deltaStatements: merge", () => {
   const merge = (columns: string[], mergeKeys: string[]) =>
      deltaStatements({
         dialect: "postgres",
         quotedTablePath: `"daily"`,
         deltaSQL: "SELECT 1",
         columns,
         mergeKeys,
         watermarkName: "order_date",
         ...RANGE,
      });

   it("is a single statement: MERGE is atomic on its own", () => {
      expect(merge(["order_id", "revenue"], ["order_id"])).toHaveLength(1);
   });

   it("matches on the declared identity, NULL-safely", () => {
      // SQL equality is not NULL-safe, so a NULL identity value would fail to
      // match its own copy and the row would be INSERTED again on every run.
      const [sql] = merge(["order_id", "revenue"], ["order_id"]);
      expect(sql).toContain(
         `ON (__t."order_id" = __s."order_id" OR (__t."order_id" IS NULL AND __s."order_id" IS NULL))`,
      );
   });

   it("updates every non-key column and inserts the whole row", () => {
      const [sql] = merge(["order_id", "region", "revenue"], ["order_id"]);
      expect(sql).toContain(
         `WHEN MATCHED THEN UPDATE SET "region" = __s."region", "revenue" = __s."revenue"`,
      );
      expect(sql).toContain(
         `WHEN NOT MATCHED THEN INSERT ("order_id", "region", "revenue") VALUES (__s."order_id", __s."region", __s."revenue")`,
      );
      // The identity columns are never SET: the ON clause already proved them
      // equal, so writing them back is work with no effect.
      expect(sql).not.toContain(`SET "order_id"`);
   });

   it("omits WHEN MATCHED when every column is an identity column", () => {
      // An empty SET list is a syntax error, and a matched row is already
      // identical to its replacement — so there is nothing to update.
      const [sql] = merge(["a", "b"], ["a", "b"]);
      expect(sql).not.toContain("WHEN MATCHED");
      expect(sql).toContain("WHEN NOT MATCHED THEN INSERT");
   });

   it("does not delete the range: a merge is the strategy that need not", () => {
      const [sql] = merge(["order_id", "revenue"], ["order_id"]);
      expect(sql).not.toContain("DELETE");
   });

   it("stays a bare MERGE on Snowflake: one statement needs no block", () => {
      const [sql] = deltaStatements({
         dialect: "snowflake",
         quotedTablePath: `"daily"`,
         deltaSQL: "SELECT 1",
         columns: ["order_id", "revenue"],
         mergeKeys: ["order_id"],
         watermarkName: "order_date",
         ...RANGE,
      });
      expect(sql.startsWith("MERGE INTO")).toBe(true);
      expect(sql).not.toContain("EXECUTE IMMEDIATE");
   });
});

// ── The dispatch ──────────────────────────────────────────────────────────

const LINEAGE = {
   physicalTableName: "analytics.daily",
   connectionName: "wh",
   sourceEntityId: "sid",
   watermarkName: "order_date",
   watermarkType: "date",
   mergeKeys: [] as string[],
   strategy: "range_replace" as const,
};

const LEDGER = {
   environmentId: "env",
   packageName: "pkg",
   sourceEntityId: "sid",
   coveredThroughValue: "2024-06-01",
   coveredThroughType: "date",
   watermarkDimension: "order_date",
   mergeKeyDimensions: [] as string[],
   derivedStrategy: "range_replace" as const,
   physicalTableName: "analytics.daily",
   connectionName: "wh",
   advancedByMaterializationId: "run-1",
   advancedAt: new Date("2024-06-01T00:00:00Z"),
   createdAt: new Date("2024-05-01T00:00:00Z"),
};

describe("ledgerLineageMismatch", () => {
   it("accepts a boundary that describes the same thing", () => {
      expect(ledgerLineageMismatch(LEDGER, LINEAGE)).toBeUndefined();
   });

   it("reports a renamed table under its OWN reason code", () => {
      // Distinct from lineage_changed because the cause and the remedy differ: a
      // host assigning a generational physical name per run re-seeds forever, and
      // that has to be tellable from an author changing the model.
      expect(
         ledgerLineageMismatch(
            { ...LEDGER, physicalTableName: "analytics.other" },
            LINEAGE,
         ),
      ).toEqual({
         reasonCode: "table_renamed",
         reason: expect.stringContaining(
            "analytics.other",
         ) as unknown as string,
      });
   });

   it("rejects a boundary measured over different build SQL", () => {
      // Load-bearing since the ledger stopped being keyed on the content address:
      // a standalone publisher's table names carry no content token, so an edited
      // model keeps its table name and the read HITS. Without this check the
      // delta would be applied to a table its own definition no longer describes.
      const mismatch = ledgerLineageMismatch(
         { ...LEDGER, sourceEntityId: "sid-before-the-edit" },
         LINEAGE,
      );
      expect(mismatch?.reasonCode).toBe("lineage_changed");
      expect(mismatch?.reason).toContain("content address");
   });

   it("rejects a boundary advanced on another connection", () => {
      const mismatch = ledgerLineageMismatch(
         { ...LEDGER, connectionName: "other" },
         LINEAGE,
      );
      expect(mismatch?.reasonCode).toBe("lineage_changed");
      expect(mismatch?.reason).toContain("connection");
   });

   it("rejects a changed watermark, type or strategy", () => {
      expect(
         ledgerLineageMismatch({ ...LEDGER, watermarkDimension: "ts" }, LINEAGE)
            ?.reason,
      ).toContain("different column");
      expect(
         ledgerLineageMismatch(
            { ...LEDGER, coveredThroughType: "timestamp" },
            LINEAGE,
         )?.reason,
      ).toContain("type changed");
      expect(
         ledgerLineageMismatch({ ...LEDGER, derivedStrategy: "merge" }, LINEAGE)
            ?.reason,
      ).toContain("strategy changed");
   });

   it("rejects any change to merge_key=, including a reorder", () => {
      // Order-sensitive because a NARROWED identity has to force a rebuild: rows
      // the old key set kept apart would collide under the new one.
      expect(
         ledgerLineageMismatch(
            { ...LEDGER, mergeKeyDimensions: ["a"] },
            LINEAGE,
         )?.reason,
      ).toContain("merge_key=");
      expect(
         ledgerLineageMismatch(
            { ...LEDGER, mergeKeyDimensions: ["a", "b"] },
            { ...LINEAGE, mergeKeys: ["b", "a"] },
         )?.reason,
      ).toContain("merge_key=");
   });

   it("reports every non-rename mismatch as lineage_changed", () => {
      for (const entry of [
         { ...LEDGER, connectionName: "other" },
         { ...LEDGER, watermarkDimension: "ts" },
         { ...LEDGER, coveredThroughType: "timestamp" },
         { ...LEDGER, derivedStrategy: "merge" as const },
         { ...LEDGER, mergeKeyDimensions: ["a"] },
      ]) {
         expect(ledgerLineageMismatch(entry, LINEAGE)?.reasonCode).toBe(
            "lineage_changed",
         );
      }
   });
});

describe("planIncrementalStep", () => {
   const NOW = new Date("2024-07-01T00:00:00Z");

   /** A runner that answers each probe by matching on the SQL it receives. */
   function fakeRunner(
      answers: {
         columns?: string[];
         nonEmpty?: boolean;
         maxWatermark?: unknown;
      } = {},
   ): { runner: SqlRunner; seen: string[] } {
      const seen: string[] = [];
      const runner: SqlRunner = async (sql) => {
         seen.push(sql);
         if (
            sql.includes("INFORMATION_SCHEMA") ||
            sql.includes("information_schema")
         ) {
            return {
               rows: (
                  answers.columns ?? ["order_date", "region", "revenue"]
               ).map((c) => ({ column_name: c })),
            };
         }
         if (sql.includes("LIMIT 1")) {
            return { rows: answers.nonEmpty === false ? [] : [{ present: 1 }] };
         }
         if (sql.includes("watermark_max")) {
            return { rows: [{ watermark_max: answers.maxWatermark ?? null }] };
         }
         throw new Error(`unexpected probe: ${sql}`);
      };
      return { runner, seen };
   }

   const plan = (
      overrides: Partial<Parameters<typeof planIncrementalStep>[0]> = {},
      answers?: Parameters<typeof fakeRunner>[0],
   ) => {
      const { runner } = fakeRunner(answers);
      return planIncrementalStep({
         runner,
         dialect: "postgres",
         quotedTablePath: `"analytics"."daily"`,
         lineage: LINEAGE,
         ledgerEntry: LEDGER,
         forceRefresh: false,
         now: NOW,
         sourceSQL: "SELECT * FROM base",
         columns: ["order_date", "region", "revenue"],
         ...overrides,
      });
   };

   it("applies a delta over [covered_through, snapshot) for a date watermark", async () => {
      const step = await plan();
      expect(step.mode).toBe("delta");
      if (step.mode !== "delta") return;
      expect(step.start.value).toBe("2024-06-01");
      // The run's own snapshot, so a time watermark needs no warehouse probe for
      // its frontier.
      expect(step.end.value).toBe("2024-07-01");
      expect(step.coveredThrough).toEqual(step.end);
      const script = step.statements.join(";\n");
      expect(script).toContain("DELETE FROM");
      expect(script).toContain("INSERT INTO");
      // The delta filters the source's OWN build SQL at the planned range.
      expect(script).toContain("FROM (SELECT * FROM base)");
      expect(script).toContain("2024-06-01");
      expect(script).toContain("2024-07-01");
   });

   it("seeds on forceRefresh, before touching the warehouse", async () => {
      const { runner, seen } = fakeRunner();
      const step = await planIncrementalStep({
         runner,
         dialect: "postgres",
         quotedTablePath: `"analytics"."daily"`,
         lineage: LINEAGE,
         ledgerEntry: LEDGER,
         forceRefresh: true,
         now: NOW,
         sourceSQL: "SELECT * FROM base",
         columns: ["order_date", "region", "revenue"],
      });
      expect(step.mode).toBe("seed");
      expect(seen).toEqual([]);
   });

   it("seeds when no boundary is recorded", async () => {
      const step = await plan({ ledgerEntry: null });
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reasonCode).toBe("no_boundary");
      expect(step.reason).toContain("no covered_through boundary");
   });

   it("seeds when the recorded boundary describes another table", async () => {
      const step = await plan({
         ledgerEntry: { ...LEDGER, physicalTableName: "analytics.older" },
      });
      expect(step.mode).toBe("seed");
   });

   it("seeds when the target cannot be described", async () => {
      const step = await plan({}, { columns: [] });
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reason).toContain("could not be described");
   });

   it("seeds when the target is empty but a boundary claims coverage", async () => {
      // Someone truncated the table; a delta would rebuild only the tail and the
      // history would be silently gone.
      const step = await plan({}, { nonEmpty: false });
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reason).toContain("removed outside this ledger");
   });

   it("seeds when the delta's shape does not match the table's", async () => {
      const step = await plan({
         // What a `rename:` on a table-backed source produces: the source's
         // schema names the logical column, the table holds the physical one.
         columns: ["order_date", "region", "revenue_total"],
      });
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reasonCode).toBe("shape_mismatch");
      expect(step.reason).toContain(`absent from the table: "revenue_total"`);
   });

   it("seeds rather than silently downgrading a merge on Postgres 14", async () => {
      // merge_key= asks for the stronger guarantee; range replace is not a
      // substitute for it.
      const step = await plan({
         lineage: { ...LINEAGE, strategy: "merge", mergeKeys: ["order_id"] },
         ledgerEntry: {
            ...LEDGER,
            derivedStrategy: "merge",
            mergeKeyDimensions: ["order_id"],
         },
         postgresVersionNum: 140009,
      });
      expect(step.mode).toBe("seed");
      if (step.mode !== "seed") return;
      expect(step.reason).toContain("Postgres 15");
   });

   it("skips when the watermark has not advanced", async () => {
      const step = await plan({
         ledgerEntry: { ...LEDGER, coveredThroughValue: "2024-07-01" },
      });
      expect(step.mode).toBe("skip");
      if (step.mode !== "skip") return;
      expect(step.reasonCode).toBe("not_advanced");
      expect(step.reason).toContain("has not advanced");
   });

   it("reads the frontier from the SOURCE for a number watermark", async () => {
      const numeric = {
         ...LINEAGE,
         watermarkName: "seq",
         watermarkType: "number",
      };
      const { runner, seen } = fakeRunner({
         columns: ["seq", "region"],
         maxWatermark: 500,
      });
      const step = await planIncrementalStep({
         runner,
         dialect: "postgres",
         quotedTablePath: `"analytics"."daily"`,
         lineage: numeric,
         ledgerEntry: {
            ...LEDGER,
            coveredThroughValue: "100",
            coveredThroughType: "number",
            watermarkDimension: "seq",
         },
         forceRefresh: false,
         now: NOW,
         sourceSQL: "SELECT * FROM base",
         columns: ["seq", "region"],
      });
      expect(step.mode).toBe("delta");
      if (step.mode !== "delta") return;
      expect(step.end).toEqual({ malloyType: "number", value: "500" });
      // The frontier probe runs over the SOURCE query, not the target: the
      // target by definition stops at the last boundary.
      expect(
         seen.some((s) => s.includes("FROM (SELECT * FROM base) AS __w")),
      ).toBe(true);
   });

   it("skips a number watermark whose frontier is unreadable, rather than guessing", async () => {
      const step = await plan(
         {
            lineage: {
               ...LINEAGE,
               watermarkName: "seq",
               watermarkType: "number",
            },
            ledgerEntry: {
               ...LEDGER,
               coveredThroughValue: "100",
               coveredThroughType: "number",
               watermarkDimension: "seq",
            },
         },
         { columns: ["seq", "region"], maxWatermark: null },
      );
      expect(step.mode).toBe("skip");
   });
});

describe("seedCoveredThrough", () => {
   const runner =
      (max: unknown): SqlRunner =>
      async (sql) => {
         if (!sql.includes("watermark_max"))
            throw new Error(`unexpected: ${sql}`);
         return { rows: [{ watermark_max: max }] };
      };
   const NOW = new Date("2024-07-01T12:00:00Z");

   it("records the run's snapshot for a time watermark, not the table's max", async () => {
      // A table may hold future-dated rows; a boundary must not claim coverage
      // the clock has not reached. Those rows sit above it and are recomputed by
      // the next delta.
      const result = await seedCoveredThrough(
         runner(new Date("2025-01-01T00:00:00Z")),
         "postgres",
         LINEAGE,
         `"analytics"."daily"`,
         NOW,
      );
      expect(result.bound).toEqual({ malloyType: "date", value: "2024-07-01" });
   });

   it("records the table's max for a number watermark, which has no clock", async () => {
      const result = await seedCoveredThrough(
         runner(42),
         "postgres",
         { ...LINEAGE, watermarkName: "seq", watermarkType: "number" },
         `"analytics"."daily"`,
         NOW,
      );
      expect(result.bound).toEqual({ malloyType: "number", value: "42" });
   });

   it("records nothing after seeding an empty source", async () => {
      // A boundary set from an empty seed would exclude every historical row
      // that arrives afterwards, since they would all land below it. Recording
      // nothing leaves the next run to seed again, which is cheap for a table
      // that has no rows.
      const result = await seedCoveredThrough(
         runner(null),
         "postgres",
         LINEAGE,
         `"analytics"."daily"`,
         NOW,
      );
      expect(result.bound).toBeUndefined();
   });
});
