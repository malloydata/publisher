// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { nowComparison } from "../../malloy_text";
import {
   applyDeltaScript,
   canonicalBoundValue,
   deltaStatements,
   planIncrementalStep,
   type SqlRunner,
   warehouseDeltaTarget,
} from "../run/incremental_apply";
import {
   materializeColumns,
   planModelMaterialize,
} from "./build_file_generator";
import {
   classifyRollingWindow,
   currentTimeFields,
   readRollingWindowTerm,
   windowDeleteBefore,
} from "./rolling_window";

const URL_ = "file:///rw/m.malloy";
let duckdb: DuckDBConnection;
let contents: Record<string, unknown>;

const model = `source: events is duckdb.table('events')
#(materialize) storage="lake"
source: lower_w is events extend { where: ts >= now - 30 days }
source: lower_bare is events extend { where: ts > now }
source: mirrored is events extend { where: now - 2 hours <= ts }
source: inherited is lower_w extend { where: amount > 1 }
#(materialize) storage="lake"
source: upper_w is events extend { where: ts < now - 30 days }
source: two_terms is events extend { where: ts >= now - 30 days and amount > 1 }
source: shifted is events extend { where: ts >= now.day - 1 day }
source: on_date is events extend { where: d >= now::date - 3 days }
source: via_dim is events extend {
  dimension: is_recent is ts >= now - 30 days
  where: is_recent
}
source: via_age is events extend {
  dimension: age_h is hours(ts to now)
  where: age_h > 720
}
source: via_join is events extend {
  join_one: e2 is events extend { where: ts >= now - 1 day } on amount = e2.amount
  join_one: e3 is events on amount = e3.amount
  dimension: e2_amount is e2.amount
  dimension: e3_amount is e3.amount
  where: e2.amount > 0
}
source: aged is events extend {
  dimension: age is hours(ts to now)
  dimension: old is age > 24
  dimension: plain is amount * 2
}
`;

beforeAll(async () => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   await duckdb.runSQL(
      `CREATE TABLE events AS SELECT * FROM (VALUES
         (1, TIMESTAMP '2026-01-01 00:00:00', DATE '2026-01-01')) t(amount, ts, d)`,
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[URL_, model]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const compiled = await runtime.loadModel(new URL(URL_)).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   contents = (compiled as any)._modelDef.contents;
});

afterAll(async () => {
   await duckdb.close();
});

const stored = new Map([
   ["ts", "timestamp"],
   ["d", "date"],
   ["amount", "number"],
]);

function classify(name: string) {
   return classifyRollingWindow(
      contents[name] as { filterList?: unknown },
      stored,
   );
}

describe("classifyRollingWindow, over compiled sources", () => {
   it("reads a lower bound with an offset", () => {
      const out = classify("lower_w");
      expect(out).toEqual({
         ok: true,
         terms: [
            {
               field: "ts",
               op: ">=",
               offset: { sign: -1, amount: 30, unit: "day" },
               code: "ts >= now - 30 days",
            },
         ],
      });
   });

   it("reads a bare now, and a mirrored comparison with the field on the right", () => {
      const bare = classify("lower_bare");
      expect(bare.ok && bare.terms[0]).toMatchObject({
         field: "ts",
         op: ">",
      });
      expect(bare.ok && bare.terms[0].offset).toBeUndefined();
      const mirrored = classify("mirrored");
      expect(mirrored.ok && mirrored.terms[0]).toMatchObject({
         field: "ts",
         op: ">=",
         offset: { sign: -1, amount: 2, unit: "hour" },
      });
   });

   it("reads an inherited term from the extension's filter list", () => {
      const out = classify("inherited");
      expect(out.ok && out.terms.map((t) => t.code)).toEqual([
         "ts >= now - 30 days",
      ]);
   });

   it("refuses an upper bound (question 2)", () => {
      expect(classify("upper_w")).toMatchObject({
         ok: false,
         reason: "current_time_upper_bound",
      });
   });

   it("refuses a term that reads now in any other shape", () => {
      for (const name of ["two_terms", "shifted", "on_date"]) {
         expect(classify(name)).toMatchObject({
            ok: false,
            reason: "current_time_shape",
         });
      }
   });

   it("refuses a term that reads now through a dimension or a join", () => {
      for (const name of ["via_dim", "via_age", "via_join"]) {
         expect(classify(name)).toMatchObject({
            ok: false,
            reason: "current_time_shape",
         });
      }
   });

   it("refuses a field that is not a stored timestamp", () => {
      expect(
         classifyRollingWindow(
            contents.lower_w as { filterList?: unknown },
            new Map([["amount", "number"]]),
         ),
      ).toMatchObject({ ok: false, reason: "current_time_field" });
   });

   it("accepts a source with no current-time term", () => {
      expect(classify("events")).toEqual({ ok: true, terms: [] });
   });

   it("reads nothing from a filter that is not a comparison with now", () => {
      expect(
         readRollingWindowTerm({
            e: {
               node: ">=",
               kids: { left: { node: "now" }, right: { node: "now" } },
            },
         }),
      ).toBeUndefined();
   });
});

describe("dimensions that depend on the current time", () => {
   it("are found directly and through another dimension", () => {
      const fields = (contents.aged as { fields: unknown[] }).fields;
      expect([...currentTimeFields(fields)].sort()).toEqual(["age", "old"]);
   });

   it("are found through a join whose own where: reads now", () => {
      const fields = (contents.via_join as { fields: unknown[] }).fields;
      const found = currentTimeFields(fields);
      expect(found.has("e2")).toBe(true);
      expect(found.has("e2_amount")).toBe(true);
      expect(found.has("e3")).toBe(false);
      expect(found.has("e3_amount")).toBe(false);
   });

   it("are not stored", () => {
      const columns = materializeColumns(
         contents.aged as Parameters<typeof materializeColumns>[0],
      ).map((c) => c.dimension);
      expect(columns).toContain("plain");
      expect(columns).not.toContain("age");
      expect(columns).not.toContain("old");
   });
});

describe("planModelMaterialize with a rolling window", () => {
   it("records the window on the plan, and refuses an upper bound", () => {
      const { plans, refused } = planModelMaterialize(contents);
      // `inherited` extends `lower_w`, so it carries the annotation and the
      // window both.
      expect(plans.map((p) => [p.sourceName, p.rollingWindow?.length])).toEqual(
         [
            ["inherited", 1],
            ["lower_w", 1],
         ],
      );
      expect(refused.map((r) => r.sourceName)).toEqual(["upper_w"]);
      expect(refused[0].reason).toContain("OLDER than a moving cut-off");
   });
});

describe("nowComparison", () => {
   it("writes each part from a closed set", () => {
      expect(
         nowComparison("ts", ">=", { sign: -1, amount: 30, unit: "day" }),
      ).toBe("ts >= now - 30 days");
      expect(nowComparison("count", "<", undefined)).toBe("`count` < now");
      expect(() =>
         nowComparison("ts", ">=", {
            sign: -1,
            amount: 1.5,
            unit: "day",
         }),
      ).toThrow();
      expect(() =>
         nowComparison("ts", "= 1 or 1" as ">=", undefined),
      ).toThrow();
   });
});

describe("windowDeleteBefore", () => {
   const runStart = new Date("2026-11-10T00:00:00Z");

   it("is the term's cut-off at the run's start, less a day's margin", () => {
      expect(
         windowDeleteBefore(
            { op: ">=", offset: { sign: -1, amount: 30, unit: "day" } },
            runStart,
         ).toISOString(),
      ).toBe("2026-10-10T00:00:00.000Z");
      expect(windowDeleteBefore({ op: ">" }, runStart).toISOString()).toBe(
         "2026-11-09T00:00:00.000Z",
      );
   });

   it("takes calendar units at their longest when subtracted, shortest when added", () => {
      expect(
         windowDeleteBefore(
            { op: ">=", offset: { sign: -1, amount: 1, unit: "month" } },
            runStart,
         ).toISOString(),
      ).toBe("2026-10-09T00:00:00.000Z");
      expect(
         windowDeleteBefore(
            { op: ">=", offset: { sign: 1, amount: 1, unit: "month" } },
            runStart,
         ).toISOString(),
      ).toBe("2026-12-07T00:00:00.000Z");
   });

   it("is refused for an upper bound, which never deletes", () => {
      expect(() => windowDeleteBefore({ op: "<" }, runStart)).toThrow();
   });
});

describe("the incremental delete, run on DuckDB", () => {
   const quoted = `"win"`;
   let runner: SqlRunner;
   beforeAll(async () => {
      runner = async (sql) => ({
         rows: (await duckdb.runSQL(sql)).rows as unknown[],
      });
      // The table as the last refresh left it: hourly rows from 2026-10-01.
      await duckdb.runSQL(`CREATE TABLE win AS
         SELECT ts, ts AS loaded FROM range(TIMESTAMP '2026-10-01 00:00:00',
            TIMESTAMP '2026-11-09 00:00:00', INTERVAL 1 HOUR) t(ts)`);
      await duckdb.runSQL(`CREATE TABLE incoming AS
         SELECT ts, ts AS loaded FROM range(TIMESTAMP '2026-11-09 00:00:00',
            TIMESTAMP '2026-11-10 00:00:00', INTERVAL 1 HOUR) t(ts)`);
   });

   it("appends the delta and deletes below the new bound in one transaction", async () => {
      const term = {
         op: ">=" as const,
         offset: { sign: -1 as const, amount: 30, unit: "day" as const },
      };
      const before = canonicalBoundValue(
         "timestamp",
         windowDeleteBefore(term, new Date("2026-11-10T00:00:00Z")),
      );
      const statements = deltaStatements({
         dialect: "duckdb",
         quotedTablePath: quoted,
         deltaSQL: "SELECT * FROM incoming",
         columns: ["ts", "loaded"],
         mergeKeys: [],
         watermarkName: "ts",
         start: { malloyType: "timestamp", value: "2026-11-09T00:00:00" },
         end: { malloyType: "timestamp", value: "2026-11-10T00:00:00" },
         windowDeletes: [{ column: "ts", before }],
      });
      expect(statements[0]).toBe("BEGIN");
      expect(statements.at(-1)).toBe("COMMIT");
      expect(statements.at(-2)).toBe(
         `DELETE FROM "win" WHERE "ts" < TIMESTAMP '2026-10-10 00:00:00'`,
      );
      await applyDeltaScript(runner, "duckdb", statements);
      const [row] = (
         await duckdb.runSQL(
            "SELECT count(*) AS n, min(ts) AS first, max(ts) AS last FROM win",
         )
      ).rows as { n: unknown; first: unknown; last: unknown }[];
      // Every row from the bound on, the appended day included: nothing a read
      // from 2026-11-10 on (cut-off 2026-10-11 or later) needs is gone.
      expect(new Date(String(row.first)).toISOString()).toBe(
         "2026-10-10T00:00:00.000Z",
      );
      expect(new Date(String(row.last)).toISOString()).toBe(
         "2026-11-09T23:00:00.000Z",
      );
      expect(Number(row.n)).toBe(31 * 24);
   });

   it("deletes after a merge too, inside its transaction", () => {
      const statements = deltaStatements({
         dialect: "duckdb",
         quotedTablePath: quoted,
         deltaSQL: "SELECT * FROM incoming",
         columns: ["ts", "loaded"],
         mergeKeys: ["ts"],
         watermarkName: "ts",
         start: { malloyType: "timestamp", value: "2026-11-09T00:00:00" },
         end: { malloyType: "timestamp", value: "2026-11-10T00:00:00" },
         windowDeletes: [
            {
               column: "ts",
               before: {
                  malloyType: "timestamp",
                  value: "2026-10-10T00:00:00",
               },
            },
         ],
      });
      expect(statements[0]).toBe("BEGIN");
      expect(statements[1]).toStartWith("MERGE INTO");
      expect(statements[2]).toStartWith(`DELETE FROM "win" WHERE "ts" <`);
      expect(statements[3]).toBe("COMMIT");
   });

   it("is planned by planIncrementalStep from the lineage's window", async () => {
      const lineage = {
         physicalTableName: "win",
         connectionName: "wh",
         sourceEntityId: "sid",
         watermarkName: "ts",
         watermarkType: "timestamp",
         mergeKeys: [] as string[],
         scopeColumns: [] as string[],
         strategy: "range_replace" as const,
         rollingWindow: [
            {
               field: "ts",
               op: ">=" as const,
               offset: { sign: -1 as const, amount: 7, unit: "day" as const },
               code: "ts >= now - 7 days",
            },
         ],
      };
      const fake: SqlRunner = async (sql) => {
         if (/information_schema/i.test(sql)) {
            return { rows: [{ column_name: "ts" }, { column_name: "loaded" }] };
         }
         if (sql.includes("LIMIT 1")) return { rows: [{ present: 1 }] };
         throw new Error(`unexpected probe: ${sql}`);
      };
      const step = await planIncrementalStep({
         target: warehouseDeltaTarget({
            dialect: "postgres",
            runner: fake,
            quotedTablePath: `"analytics"."win"`,
            lineage,
            sourceSQL: "SELECT * FROM base",
         }),
         lineage,
         ledgerEntry: {
            sourceEntityId: "sid",
            coveredThroughValue: "2026-11-09T00:00:00",
            coveredThroughType: "timestamp",
            watermarkDimension: "ts",
            mergeKeyDimensions: [],
            derivedStrategy: "range_replace",
            physicalTableName: "win",
            connectionName: "wh",
         },
         forceRefresh: false,
         now: new Date("2026-11-10T00:00:00Z"),
         columns: ["ts", "loaded"],
      });
      expect(step.mode).toBe("delta");
      if (step.mode !== "delta") return;
      expect(step.statements).toContain(
         `DELETE FROM "analytics"."win" WHERE "ts" < TIMESTAMP '2026-11-02 00:00:00'`,
      );
   });
});
