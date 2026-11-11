// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { components } from "../../../api";
import { BadRequestError, BuildPlanUnavailableError } from "../../../errors";
import { installSettings } from "../../../test_helpers/settings_env";
import {
   COUNT_QUERY_METHOD,
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
      expect(readRowCount([{ row_count: 3 }])).toBe(3);
      expect(readRowCount([{ ROW_COUNT: 7n }])).toBe(7);
      expect(readRowCount([{ Row_Count: "12" }])).toBe(12);
   });

   it("refuses an answer that is not a row count", () => {
      expect(() => readRowCount([])).toThrow("no row");
      expect(() => readRowCount([{ other: 1 }])).toThrow("not a row count");
      expect(() => readRowCount([{ row_count: -1 }])).toThrow();
      expect(() => readRowCount([{ row_count: 1.5 }])).toThrow();
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

   it("refuses a source the plan refuses, even one that keeps a plan entry", () => {
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
      expect(refusal?.reason).toBe("not_buildable");
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
         getBuildPlan: () => null,
         isBuildPlanUnavailable: () => false,
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
         environmentName: "env",
         body: {},
      });
      expect(result.estimates).toEqual({});
   });

   it("refuses with BUILD_PLAN_UNAVAILABLE when the plan was not derived", async () => {
      await expect(
         estimatePackageRowCounts({
            pkg: fakePackage({ isBuildPlanUnavailable: () => true }),
            environmentName: "env",
            body: {},
         }),
      ).rejects.toThrow(BuildPlanUnavailableError);
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
            getBuildPlan: () => planOf(source),
            getSourceEligibility: () => ALL_ELIGIBLE(["orders_flat"]),
            getMalloyConnection: async () =>
               ({
                  runSQL: async (sql: string, options: unknown) => {
                     seen.push({ sql, options });
                     return { rows: [{ row_count: 9 }], totalRows: 1 };
                  },
               }) as never,
         }),
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
         team: "growth",
         tenant: "t1",
      });
      expect(options.queryMetadata?.run_id).toBeDefined();
      restore();
   });
});
