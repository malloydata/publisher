// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../logger";
import { components } from "../../../api";
import { BadRequestError } from "../../../errors";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   COUNT_QUERY_METHOD,
   countTimeoutMs,
   MAX_SOURCES_PER_REQUEST,
   estimatePackageRowCounts,
   estimateRefusal,
   estimateRowCounts,
   parseEstimateRequest,
   readRowCount,
   rowCountSQL,
   selectSources,
   type EstimatablePackage,
} from "./row_count_estimate";

type BuildPlan = components["schemas"]["BuildPlan"];
type PersistSourcePlan = components["schemas"]["PersistSourcePlan"];

function planSource(
   name: string,
   extra: Partial<PersistSourcePlan> = {},
): PersistSourcePlan {
   return {
      name,
      sourceID: `${name}@file:///m.malloy`,
      connectionName: "duckdb",
      dialect: "duckdb",
      origin: "persist",
      sourceEntityId: `id-${name}`,
      sql: `SELECT * FROM ${name}_raw`,
      columns: [],
      modelPath: "m.malloy",
      ...extra,
   };
}

function planOf(...sources: PersistSourcePlan[]): BuildPlan {
   return {
      graphs: [],
      sources: Object.fromEntries(sources.map((s) => [s.sourceID, s])),
   };
}

const ALL_ELIGIBLE = (names: string[]) => ({ eligible: names, refused: {} });

describe("rowCountSQL", () => {
   it("wraps the build query in a count, dropping a trailing semicolon", () => {
      expect(rowCountSQL("  SELECT 1 AS a;  \n")).toBe(
         "SELECT COUNT(*) AS row_count FROM (\nSELECT 1 AS a\n) AS publisher_row_estimate",
      );
   });
});

describe("readRowCount", () => {
   it("reads a number, a bigint or a numeric string, under any case", () => {
      expect(readRowCount([{ row_count: 3 }]).rows).toBe(3);
      expect(readRowCount([{ ROW_COUNT: 7n }]).rows).toBe(7);
      expect(readRowCount([{ Row_Count: " 12 " }]).rows).toBe(12);
   });

   it("refuses an answer that is not a row count, including an empty string", () => {
      expect(() => readRowCount([])).toThrow("no row");
      expect(() => readRowCount([{ other: 1 }])).toThrow("not a row count");
      expect(() => readRowCount([{ row_count: -1 }])).toThrow();
      expect(() => readRowCount([{ row_count: 1.5 }])).toThrow();
      expect(() => readRowCount([{ row_count: "" }])).toThrow(
         "not a row count",
      );
      expect(() => readRowCount([{ row_count: "  " }])).toThrow(
         "not a row count",
      );
   });

   it("accepts a count past 2^53, marked rounded", () => {
      const big = readRowCount([{ row_count: 9007199254740993n }]);
      expect(big).toEqual({ rows: 9007199254740992, rounded: true });
      expect(
         readRowCount([{ row_count: "18000000000000000000" }]).rounded,
      ).toBe(true);
      expect(readRowCount([{ row_count: 5 }]).rounded).toBe(false);
   });
});

describe("estimateRefusal", () => {
   const open = planSource("open");

   it("admits a source the storage gate admits and the build does not scope", () => {
      expect(
         estimateRefusal(
            open.sourceID,
            open,
            undefined,
            ALL_ELIGIBLE(["open"]),
         ),
      ).toBeUndefined();
   });

   it("reports a plan refusal that keeps its plan entry (an authorize-refused rollup) as gated", () => {
      const refusal = estimateRefusal(
         open.sourceID,
         open,
         {
            name: "open",
            sourceID: open.sourceID,
            tier: "preaggregate",
            reason: "authorize",
            message: "gated base",
         },
         ALL_ELIGIBLE(["open"]),
      );
      expect(refusal?.reason).toBe("gated");
      expect(refusal?.message).toContain("gated base");
   });

   it("refuses a source the storage gate does not admit, or admits under a refused name", () => {
      expect(
         estimateRefusal(open.sourceID, open, undefined, {
            eligible: [],
            refused: { open: "it is protected by a gate" },
         })?.reason,
      ).toBe("gated");
      // Two models' same-named sources: one refusal refuses the name.
      expect(
         estimateRefusal(open.sourceID, open, undefined, {
            eligible: ["open"],
            refused: { open: "it is protected by a gate" },
         })?.reason,
      ).toBe("gated");
   });

   it("refuses a source whose build strips a caller-scoped term", () => {
      const stripped = planSource("scoped", {
         strippedTerms: [{ code: "org = $ORG", givens: ["ORG"] }],
      });
      const joined = planSource("joined", {
         joinedTerms: [{ alias: "g", source: "grants", terms: [] }],
      });
      for (const source of [stripped, joined]) {
         expect(
            estimateRefusal(
               source.sourceID,
               source,
               undefined,
               ALL_ELIGIBLE([source.name]),
            )?.reason,
         ).toBe("caller_scoped");
      }
   });

   it("refuses everything when eligibility was never computed", () => {
      expect(
         estimateRefusal(open.sourceID, open, undefined, undefined)?.reason,
      ).toBe("not_examined");
   });
});

describe("selectSources and parseEstimateRequest", () => {
   const plan: BuildPlan = {
      ...planOf(planSource("a"), planSource("b")),
      refusedSources: {
         "r@file:///m.malloy": {
            name: "r",
            sourceID: "r@file:///m.malloy",
            tier: "storage",
            reason: "authorize",
            message: "gated",
         },
      },
   };

   it("selects every planned and refused source by default, by name or by sourceID", () => {
      expect(selectSources(plan, undefined)).toEqual([
         "a@file:///m.malloy",
         "b@file:///m.malloy",
         "r@file:///m.malloy",
      ]);
      expect(selectSources(plan, ["b", "r@file:///m.malloy"])).toEqual([
         "b@file:///m.malloy",
         "r@file:///m.malloy",
      ]);
   });

   it("refuses a name the plan does not hold", () => {
      expect(() => selectSources(plan, ["nope"])).toThrow(BadRequestError);
   });

   it("reads a body and refuses a malformed one", () => {
      expect(parseEstimateRequest(undefined)).toEqual({});
      expect(parseEstimateRequest({ sources: ["a"], maxRows: 10 })).toEqual({
         sources: ["a"],
         maxRows: 10,
      });
      for (const bad of [
         [],
         { sources: "a" },
         { sources: [""] },
         { maxRows: -1 },
         { maxRows: 1.5 },
         { maxRows: "10" },
      ]) {
         expect(() => parseEstimateRequest(bad)).toThrow(BadRequestError);
      }
   });
});

describe("estimateRowCounts", () => {
   const persisted = planSource("orders_flat");
   const rollup = planSource("orders__preagg_1", {
      origin: "preaggregate",
      preaggregate: {
         baseSourceName: "orders",
         grainDimensions: ["category", "order_day"],
         measures: ["total"],
      },
      sql: "SELECT category, order_day, SUM(amount) FROM orders GROUP BY 1, 2;",
   });
   const gated = planSource("gated");

   it("names the method and a zero-wide band for an exact count, and the grain for a rollup", async () => {
      const statements: string[] = [];
      const counts: Record<string, number> = {
         orders_flat: 1000,
         orders__preagg_1: 40,
      };
      let tick = 0;
      const result = await estimateRowCounts({
         plan: planOf(persisted, rollup, gated),
         eligibility: {
            eligible: ["orders_flat", "orders__preagg_1"],
            refused: { gated: "it is protected by a gate" },
         },
         request: {},
         countRows: async (source, sql) => {
            statements.push(sql);
            return [{ row_count: counts[source.name] }];
         },
         now: () => new Date("2026-11-11T00:00:00Z"),
         clock: () => (tick += 5),
      });

      expect(result.measuredAt).toBe("2026-11-11T00:00:00.000Z");
      expect(result.maxRows).toBeUndefined();
      expect(result.estimates[persisted.sourceID]).toMatchObject({
         status: "estimated",
         rows: 1000,
         band: { low: 1000, high: 1000 },
         method: COUNT_QUERY_METHOD,
         origin: "persist",
         modelPath: "m.malloy",
         elapsedMs: 5,
      });
      expect(
         result.estimates[persisted.sourceID].exceedsMaxRows,
      ).toBeUndefined();
      const rolled = result.estimates[rollup.sourceID];
      expect(rolled).toMatchObject({
         status: "estimated",
         rows: 40,
         band: { low: 40, high: 40 },
         method: COUNT_QUERY_METHOD,
         origin: "preaggregate",
         baseSourceName: "orders",
         grainDimensions: ["category", "order_day"],
      });
      expect(rolled.methodDetail).toContain("distinct grain tuples");
      // The rollup is counted over its own GROUP BY, the trailing `;` dropped.
      expect(statements).toEqual([
         rowCountSQL(persisted.sql),
         rowCountSQL(rollup.sql),
      ]);
      expect(statements[1]).not.toContain(";");

      // The gated source issued no statement and carries no number.
      const refused = result.estimates[gated.sourceID];
      expect(refused.status).toBe("refused");
      expect(refused.reason).toBe("gated");
      expect(refused.rows).toBeUndefined();
      expect(refused.band).toBeUndefined();
      expect(refused.method).toBeUndefined();
   });

   it("flags an estimate above maxRows, and only that one", async () => {
      const big = planSource("big");
      const small = planSource("small");
      const result = await estimateRowCounts({
         plan: planOf(big, small),
         eligibility: ALL_ELIGIBLE(["big", "small"]),
         request: { maxRows: 100 },
         countRows: async (source) => [
            { row_count: source.name === "big" ? 101 : 100 },
         ],
      });
      expect(result.maxRows).toBe(100);
      expect(result.estimates[big.sourceID].exceedsMaxRows).toBe(true);
      expect(result.estimates[small.sourceID].exceedsMaxRows).toBe(false);
   });

   it("reports a failed count and still runs the rest", async () => {
      const broken = planSource("broken");
      const fine = planSource("fine");
      const result = await estimateRowCounts({
         plan: planOf(broken, fine),
         eligibility: ALL_ELIGIBLE(["broken", "fine"]),
         request: {},
         countRows: async (source) => {
            if (source.name === "broken") throw new Error("table not found");
            return [{ row_count: 2 }];
         },
      });
      expect(result.estimates[broken.sourceID]).toMatchObject({
         status: "failed",
         message: "table not found",
      });
      expect(result.estimates[broken.sourceID].rows).toBeUndefined();
      expect(result.estimates[fine.sourceID].rows).toBe(2);
   });

   it("issues no further statement once the client has gone", async () => {
      const controller = new AbortController();
      controller.abort(new Error("client disconnected"));
      let ran = 0;
      await expect(
         estimateRowCounts({
            plan: planOf(planSource("a")),
            eligibility: ALL_ELIGIBLE(["a"]),
            request: {},
            countRows: async () => {
               ran++;
               return [{ row_count: 1 }];
            },
            signal: controller.signal,
         }),
      ).rejects.toThrow("client disconnected");
      expect(ran).toBe(0);
   });
});

describe("estimatePackageRowCounts", () => {
   function fakePackage(
      overrides: Partial<EstimatablePackage> = {},
   ): EstimatablePackage {
      return {
         getPackageName: () => "pkg",
         getVersionId: () => undefined,
         getSourceEligibility: () => ({ eligible: [], refused: {} }),
         getMalloyConnection: async () => {
            throw new Error("no statement may run");
         },
         ...overrides,
      };
   }

   it("answers an empty map for a package with no plan", async () => {
      const result = await estimatePackageRowCounts({
         pkg: fakePackage(),
         plan: { graphs: [], sources: {} },
         environmentName: "env",
         body: {},
      });
      expect(result.estimates).toEqual({});
   });

   it("tags each count like a build of the source, with trigger estimate", async () => {
      // Query metadata ships dark; turned on for this test only.
      const { restore } = installSettings({
         env: { PUBLISHER_QUERY_METADATA: "on" },
      });
      const source = planSource("orders_flat", {
         queryMetadata: { team: "growth" },
      });
      const seen: { sql: string; options: unknown }[] = [];
      const result = await estimatePackageRowCounts({
         pkg: fakePackage({
            getVersionId: () => "1.2.0",
            getSourceEligibility: () => ALL_ELIGIBLE(["orders_flat"]),
            getMalloyConnection: async () =>
               ({
                  runSQL: async (sql: string, options: unknown) => {
                     seen.push({ sql, options });
                     return { rows: [{ row_count: 9 }], totalRows: 1 };
                  },
               }) as never,
         }),
         plan: planOf(source),
         environmentName: "env",
         body: {},
         connectionMetadata: () => ({ enforced: { tenant: "t1" } }),
      });
      expect(result.estimates[source.sourceID].rows).toBe(9);
      expect(seen).toHaveLength(1);
      expect(seen[0].sql).toBe(rowCountSQL(source.sql));
      const options = seen[0].options as {
         queryMetadata?: Record<string, string>;
         abortSignal?: AbortSignal;
      };
      expect(options.abortSignal).toBeDefined();
      expect(options.queryMetadata).toMatchObject({
         class: "materialize",
         trigger: "estimate",
         environment: "env",
         package: "pkg",
         source: "orders_flat",
         version: "1.2.0",
         team: "growth",
         tenant: "t1",
      });
      expect(options.queryMetadata?.run_id).toBeDefined();
      restore();
   });

   const countingPackage = (seen: string[]) =>
      fakePackage({
         getSourceEligibility: () => ALL_ELIGIBLE(["orders_flat"]),
         getMalloyConnection: async () =>
            ({
               runSQL: async (sql: string) => {
                  seen.push(sql);
                  return { rows: [{ row_count: 4 }], totalRows: 1 };
               },
            }) as never,
      });

   it("meters a count on a metered connection, and not on another", async () => {
      const spy = spyOn(logger, "info");
      try {
         const source = planSource("orders_flat");
         const usage = () =>
            (spy.mock.calls as unknown as [unknown, Record<string, unknown>][])
               .filter((c) => c[0] === "statement_usage")
               .map((c) => c[1]);
         await estimatePackageRowCounts({
            pkg: countingPackage([]),
            plan: planOf(source),
            environmentName: "env",
            body: {},
            meteredConnection: () => true,
         });
         expect(usage()).toMatchObject([
            {
               event: "statement_usage",
               environment: "env",
               connection: "duckdb",
               surface: "build",
               package: "pkg",
               status: "ok",
            },
         ]);
         spy.mockClear();
         await estimatePackageRowCounts({
            pkg: countingPackage([]),
            plan: planOf(source),
            environmentName: "env",
            body: {},
            meteredConnection: () => false,
         });
         expect(usage()).toEqual([]);
      } finally {
         spy.mockRestore();
      }
   });
});

describe("the review's edge cases", () => {
   it("refuses a SQL Server source with reason dialect, issuing no statement", () => {
      const source = planSource("mssql_orders", {
         dialect: "sqlserver",
         sql: "WITH __stage0 AS (SELECT 1 AS a) SELECT a FROM __stage0 ORDER BY 1",
      });
      expect(
         estimateRefusal(
            source.sourceID,
            source,
            undefined,
            ALL_ELIGIBLE(["mssql_orders"]),
         ),
      ).toMatchObject({ reason: "dialect" });
   });

   it("reads a refusal for a source named constructor by own key only", () => {
      const source = planSource("constructor");
      expect(
         estimateRefusal(
            source.sourceID,
            source,
            undefined,
            ALL_ELIGIBLE(["constructor"]),
         ),
      ).toBeUndefined();
   });

   it("reports an authorize-refused rollup that keeps its plan entry as gated", () => {
      const rollup = planSource("orders__preagg", { origin: "preaggregate" });
      expect(
         estimateRefusal(
            rollup.sourceID,
            rollup,
            {
               name: "orders__preagg",
               sourceID: rollup.sourceID,
               tier: "preaggregate",
               reason: "authorize",
               message: "the base is gated",
            },
            ALL_ELIGIBLE(["orders__preagg"]),
         ),
      ).toMatchObject({ reason: "gated" });
   });

   it("counts at most maxSources, and reports the rest as not attempted", async () => {
      const names = ["a", "b", "c"];
      let ran = 0;
      const result = await estimateRowCounts({
         plan: planOf(...names.map((n) => planSource(n))),
         eligibility: ALL_ELIGIBLE(names),
         request: parseEstimateRequest({ maxSources: 2 }),
         countRows: async () => {
            ran++;
            return [{ row_count: 1 }];
         },
      });
      expect(ran).toBe(2);
      expect(
         Object.values(result.estimates).map((e) => e.reason ?? e.status),
      ).toEqual(["estimated", "estimated", "limit"]);
      expect(() => parseEstimateRequest({ maxSources: 0 })).toThrow(
         BadRequestError,
      );
      expect(() =>
         parseEstimateRequest({ maxSources: MAX_SOURCES_PER_REQUEST + 1 }),
      ).toThrow(BadRequestError);
   });

   it("stops counting when the request's time budget runs out", async () => {
      let t = 0;
      let ran = 0;
      const result = await estimateRowCounts({
         plan: planOf(planSource("a"), planSource("b")),
         eligibility: ALL_ELIGIBLE(["a", "b"]),
         request: {},
         budgetMs: 100,
         clock: () => t,
         countRows: async () => {
            ran++;
            t += 150; // the first count uses the whole budget
            return [{ row_count: 1 }];
         },
      });
      expect(ran).toBe(1);
      expect(
         Object.values(result.estimates).map((e) => e.reason ?? e.status),
      ).toEqual(["estimated", "deadline"]);
   });

   it("cuts each count's timeout to the remaining budget, and keeps no timeout as none", () => {
      expect(countTimeoutMs(60_000, 1_500.2)).toBe(1_501);
      expect(countTimeoutMs(1_000, 60_000)).toBe(1_000);
      expect(countTimeoutMs(0, 60_000)).toBe(0);
   });

   it("rethrows a client disconnect during a count instead of recording a failure", async () => {
      const controller = new AbortController();
      const warn = spyOn(logger, "warn");
      try {
         await expect(
            estimateRowCounts({
               plan: planOf(planSource("a")),
               eligibility: ALL_ELIGIBLE(["a"]),
               request: { maxRows: 0 },
               signal: controller.signal,
               countRows: async () => {
                  controller.abort(new Error("client disconnected"));
                  throw new Error("client disconnected");
               },
            }),
         ).rejects.toThrow("client disconnected");
         expect(warn).not.toHaveBeenCalled();
      } finally {
         warn.mockRestore();
      }
   });
});
