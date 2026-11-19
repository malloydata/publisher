// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import type { BuildPlan, ManifestEntry } from "../domain/types";
import {
   declaredFreshnessIndex,
   pastDeclaredWindow,
   stampDeclaredFreshness,
} from "./freshness_stamp";
import { deriveSelfInstructions } from "./run_instructions";

/**
 * A standalone run's freshness stamping: each table's declared window and
 * fallback on its entry, anchored at the instant its data is as of, so a
 * standalone server ages a table as a host's manifest would have it aged.
 */

const STARTED = new Date("2026-11-16T12:00:00.000Z");

/** A plan whose sources declare `freshness`, with `dependsOn` edges. */
function planOf(
   sources: Record<
      string,
      {
         address: string;
         freshness?: {
            window?: string;
            fallback?: "live" | "stale_ok" | "fail";
         };
         dependsOn?: string[];
      }
   >,
): Pick<BuildPlan, "sources" | "graphs"> {
   return {
      sources: Object.fromEntries(
         Object.entries(sources).map(([sourceID, s]) => [
            sourceID,
            {
               name: sourceID,
               sourceID,
               sourceEntityId: s.address,
               freshness: s.freshness ?? null,
            },
         ]),
      ) as unknown as BuildPlan["sources"],
      graphs: [
         {
            connectionName: "duckdb",
            nodes: [
               Object.entries(sources).map(([sourceID, s]) => ({
                  sourceID,
                  dependsOn: s.dependsOn ?? [],
               })),
            ],
         },
      ],
   };
}

function entry(address: string, extra: Partial<ManifestEntry> = {}) {
   return {
      sourceEntityId: address,
      physicalTableName: `t_${address}`,
      connectionName: "duckdb",
      ...extra,
   } as ManifestEntry;
}

describe("declaredFreshnessIndex", () => {
   it("reads each table's declared window in seconds and its fallback", () => {
      const index = declaredFreshnessIndex(
         planOf({
            a: {
               address: "A",
               freshness: { window: "2h", fallback: "stale_ok" },
            },
            b: { address: "B" },
         }),
      );
      expect(index.factsAt("A")).toEqual({
         freshnessWindowSeconds: 7200,
         freshnessFallback: "stale_ok",
      });
      expect(index.factsAt("B")).toBeUndefined();
   });

   it("gives a table two sources share the narrowest window, and stale_ok only when every one says it", () => {
      const index = declaredFreshnessIndex(
         planOf({
            a: {
               address: "A",
               freshness: { window: "1d", fallback: "stale_ok" },
            },
            b: { address: "A", freshness: { window: "30m", fallback: "live" } },
         }),
      );
      expect(index.factsAt("A")).toEqual({
         freshnessWindowSeconds: 1800,
         freshnessFallback: "live",
      });
   });

   it("names the tables a table is built from, by content address", () => {
      const index = declaredFreshnessIndex(
         planOf({
            base: { address: "A" },
            down: { address: "B", dependsOn: ["base"] },
         }),
      );
      expect(index.upstreamsOf("B")).toEqual(["A"]);
      expect(index.upstreamsOf("A")).toEqual([]);
   });
});

describe("pastDeclaredWindow", () => {
   const facts = { freshnessWindowSeconds: 3600 };
   it("is past only when the anchor is older than the window", () => {
      const inside = new Date(STARTED.getTime() - 1800_000).toISOString();
      const outside = new Date(STARTED.getTime() - 7200_000).toISOString();
      expect(pastDeclaredWindow({ dataAsOf: inside }, facts, STARTED)).toBe(
         false,
      );
      expect(pastDeclaredWindow({ dataAsOf: outside }, facts, STARTED)).toBe(
         true,
      );
   });

   it("is past for a windowed table with no anchor, and never without a window", () => {
      expect(pastDeclaredWindow({}, facts, STARTED)).toBe(true);
      expect(pastDeclaredWindow({}, undefined, STARTED)).toBe(false);
      expect(
         pastDeclaredWindow({}, { freshnessFallback: "live" }, STARTED),
      ).toBe(false);
   });
});

describe("stampDeclaredFreshness", () => {
   it("stamps a built table with its declaration, anchored at the run's start", () => {
      const index = declaredFreshnessIndex(
         planOf({ a: { address: "A", freshness: { window: "24h" } } }),
      );
      const stamped = stampDeclaredFreshness(
         { A: entry("A") },
         index,
         new Set(),
         STARTED,
      );
      expect(stamped.A).toEqual({
         ...entry("A"),
         dataAsOf: STARTED.toISOString(),
         freshnessWindowSeconds: 86_400,
      });
   });

   it("keeps a carried table's anchor and gives it the window declared now", () => {
      const earlier = "2026-11-16T10:00:00.000Z";
      const index = declaredFreshnessIndex(
         planOf({
            a: {
               address: "A",
               freshness: { window: "3h", fallback: "stale_ok" },
            },
         }),
      );
      const stamped = stampDeclaredFreshness(
         {
            A: entry("A", {
               dataAsOf: earlier,
               freshnessWindowSeconds: 60,
               freshnessFallback: "live",
            }),
         },
         index,
         new Set(["A"]),
         STARTED,
      );
      expect(stamped.A).toMatchObject({
         dataAsOf: earlier,
         freshnessWindowSeconds: 10_800,
         freshnessFallback: "stale_ok",
      });
   });

   it("anchors a table built over a carried one at the older anchor", () => {
      const earlier = "2026-11-16T09:30:00.000Z";
      const index = declaredFreshnessIndex(
         planOf({
            base: { address: "A", freshness: { window: "24h" } },
            mid: { address: "B", dependsOn: ["base"] },
            down: {
               address: "C",
               freshness: { window: "24h" },
               dependsOn: ["mid"],
            },
         }),
      );
      const stamped = stampDeclaredFreshness(
         {
            A: entry("A", {
               dataAsOf: earlier,
               freshnessWindowSeconds: 86_400,
            }),
            B: entry("B"),
            C: entry("C"),
         },
         index,
         new Set(["A"]),
         STARTED,
      );
      // Through an undeclared table, which itself carries no facts.
      expect(stamped.B).toEqual(entry("B"));
      expect(stamped.C.dataAsOf).toBe(earlier);
   });

   it("anchors a windowed table built over a carried table of unknown age at the epoch, never at the run's start", () => {
      const index = declaredFreshnessIndex(
         planOf({
            base: { address: "A" },
            down: {
               address: "B",
               freshness: { window: "1h" },
               dependsOn: ["base"],
            },
         }),
      );
      const stamped = stampDeclaredFreshness(
         { A: entry("A"), B: entry("B") },
         index,
         new Set(["A"]),
         STARTED,
      );
      expect(stamped.B.dataAsOf).toBe(new Date(0).toISOString());
   });

   it("anchors a rollup at its base's anchor when it read the base's table", () => {
      const earlier = "2026-11-16T11:00:00.000Z";
      const index = declaredFreshnessIndex(
         planOf({
            base: { address: "A", freshness: { window: "1d" } },
            rollup: { address: "R", freshness: { window: "2h" } },
         }),
      );
      const stamped = stampDeclaredFreshness(
         { A: entry("A", { dataAsOf: earlier }), R: entry("R") },
         index,
         new Set(["A"]),
         STARTED,
         new Map([["R", ["A"]]]),
      );
      expect(stamped.R.dataAsOf).toBe(earlier);
   });

   it("clears the facts of a table that no longer declares any", () => {
      const index = declaredFreshnessIndex(planOf({ a: { address: "A" } }));
      const stamped = stampDeclaredFreshness(
         {
            A: entry("A", {
               dataAsOf: "2026-11-16T10:00:00.000Z",
               freshnessWindowSeconds: 60,
               freshnessFallback: "live",
            }),
         },
         index,
         new Set(["A"]),
         STARTED,
      );
      expect(stamped.A).toEqual(entry("A"));
   });
});

describe("a standalone run's carry-forward", () => {
   const compiled = compiledWith(
      {
         s1: fakeSource({ name: "s1", sourceEntityId: "b1aaaaaaaaaaaaaa" }),
         s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
      },
      [["s1", "s2"]],
   );
   const prior = (address: string, name: string, dataAsOf?: string) => ({
      sourceEntityId: address,
      physicalTableName: name,
      connectionName: "duckdb",
      ...(dataAsOf ? { dataAsOf } : {}),
   });

   it("rebuilds a table past its declared window and carries the rest", () => {
      const old = new Date(STARTED.getTime() - 7200_000).toISOString();
      const recent = new Date(STARTED.getTime() - 60_000).toISOString();
      const index = declaredFreshnessIndex(
         planOf({
            s1: { address: "b1aaaaaaaaaaaaaa", freshness: { window: "1h" } },
            s2: { address: "b2bbbbbbbbbbbbbb", freshness: { window: "1h" } },
         }),
      );
      const { instructions, carried } = deriveSelfInstructions(
         compiled,
         undefined,
         {
            b1aaaaaaaaaaaaaa: prior("b1aaaaaaaaaaaaaa", "s1", old),
            b2bbbbbbbbbbbbbb: prior("b2bbbbbbbbbbbbbb", "s2", recent),
         },
         undefined,
         undefined,
         {
            pastWindow: (address, p) =>
               pastDeclaredWindow(p, index.factsAt(address), STARTED),
         },
      );
      expect(instructions.map((i) => i.sourceEntityId)).toEqual([
         "b1aaaaaaaaaaaaaa",
      ]);
      expect(Object.keys(carried)).toEqual(["b2bbbbbbbbbbbbbb"]);
   });

   it("carries every unchanged table when none declares a window", () => {
      const { instructions, carried } = deriveSelfInstructions(
         compiled,
         undefined,
         {
            b1aaaaaaaaaaaaaa: prior("b1aaaaaaaaaaaaaa", "s1"),
            b2bbbbbbbbbbbbbb: prior("b2bbbbbbbbbbbbbb", "s2"),
         },
         undefined,
         undefined,
         {
            pastWindow: (address, p) =>
               pastDeclaredWindow(
                  p,
                  declaredFreshnessIndex(
                     planOf({
                        s1: { address: "b1aaaaaaaaaaaaaa" },
                        s2: { address: "b2bbbbbbbbbbbbbb" },
                     }),
                  ).factsAt(address),
                  STARTED,
               ),
         },
      );
      expect(instructions).toEqual([]);
      expect(Object.keys(carried).sort()).toEqual([
         "b1aaaaaaaaaaaaaa",
         "b2bbbbbbbbbbbbbb",
      ]);
   });

   it("never carries a table stored as of its build: its build reads the current time", () => {
      const nowReading = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               sourceDef: {
                  type: "query_source",
                  query: {
                     structRef: { type: "table", fields: [] },
                     pipeline: [
                        {
                           type: "project",
                           filterList: [
                              {
                                 node: "filterCondition",
                                 code: "ts > now",
                                 e: {
                                    node: ">",
                                    kids: {
                                       left: { node: "field", path: ["ts"] },
                                       right: { node: "now" },
                                    },
                                 },
                              },
                           ],
                        },
                     ],
                  },
               },
            }),
            s2: fakeSource({ name: "s2", sourceEntityId: "b2bbbbbbbbbbbbbb" }),
         },
         [["s1", "s2"]],
      );
      const { instructions, carried } = deriveSelfInstructions(
         { ...nowReading, asOfBuild: ["s1"] },
         undefined,
         {
            b1aaaaaaaaaaaaaa: prior("b1aaaaaaaaaaaaaa", "s1"),
            b2bbbbbbbbbbbbbb: prior("b2bbbbbbbbbbbbbb", "s2"),
         },
      );
      expect(instructions.map((i) => i.sourceEntityId)).toEqual([
         "b1aaaaaaaaaaaaaa",
      ]);
      expect(Object.keys(carried)).toEqual(["b2bbbbbbbbbbbbbb"]);
   });
});
