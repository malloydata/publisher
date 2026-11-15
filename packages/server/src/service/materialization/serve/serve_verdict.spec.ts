// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../../../api";
import { computeServeVerdicts, type ServeVerdictFacts } from "./serve_verdict";
import { reachableSourceIDs } from "../text/source_map";

type PersistSourcePlan = components["schemas"]["PersistSourcePlan"];

function plan(
   name: string,
   extra: Partial<PersistSourcePlan> = {},
): [string, PersistSourcePlan] {
   const sourceID = `${name}@file:///pkg/m.malloy`;
   return [
      sourceID,
      {
         name,
         sourceID,
         connectionName: "duckdb",
         sourceEntityId: `entity-${name}`,
         sql: "SELECT 1",
         columns: [],
         modelPath: "m.malloy",
         ...extra,
      } as PersistSourcePlan,
   ];
}

const STORAGE = { annotationFields: { storage: "lake" } };

function facts(
   sources: [string, PersistSourcePlan][],
   overrides: Partial<ServeVerdictFacts> = {},
): ServeVerdictFacts {
   return {
      plan: { graphs: [], sources: Object.fromEntries(sources) },
      storageMode: "on",
      reachable: () => true,
      gated: () => false,
      boundEntityIds: new Set(),
      boundStorageNames: new Set(),
      servedStorageNames: new Set(),
      ...overrides,
   };
}

describe("computeServeVerdicts", () => {
   it("says a colocated source serves once bound, and will serve before", () => {
      const [unbound] = computeServeVerdicts(facts([plan("orders")]));
      expect(unbound).toMatchObject({
         sourceName: "orders",
         tier: "colocated",
         origin: "persist",
         verdict: "not_built",
      });
      const [bound] = computeServeVerdicts(
         facts([plan("orders")], {
            boundEntityIds: new Set(["entity-orders"]),
         }),
      );
      expect(bound.verdict).toBe("serves");
      expect(bound.reason).toBeUndefined();
   });

   it("says a stored source serves only when some model kept its binding", () => {
      const bound = { boundStorageNames: new Set(["orders"]) };
      const [shadowed] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], bound),
      );
      expect(shadowed).toMatchObject({
         tier: "storage",
         verdict: "live",
         reason: "shadowed",
      });
      expect(shadowed.fixes?.length).toBeGreaterThan(0);
      const [serves] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], {
            ...bound,
            servedStorageNames: new Set(["orders"]),
         }),
      );
      expect(serves.verdict).toBe("serves");
   });

   it("serves nothing from storage unless PERSIST_STORAGE_MODE is on", () => {
      for (const storageMode of ["off", "write-only"]) {
         const [verdict] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               storageMode,
               boundStorageNames: new Set(["orders"]),
               servedStorageNames: new Set(["orders"]),
            }),
         );
         expect(verdict).toMatchObject({
            verdict: "live",
            reason: "storage_mode",
         });
         expect(verdict.message).toContain(
            storageMode === "off" ? "off" : storageMode,
         );
      }
      // The mode says nothing about a colocated source.
      const [colocated] = computeServeVerdicts(
         facts([plan("orders")], { storageMode: "off" }),
      );
      expect(colocated.verdict).toBe("not_built");
   });

   it("names a source no published query reaches, with the fix", () => {
      const [verdict] = computeServeVerdicts(
         facts([plan("staging")], {
            reachable: (_id, name) => name !== "staging",
         }),
      );
      expect(verdict).toMatchObject({ verdict: "live", reason: "unreachable" });
      expect(verdict.fixes?.[0]).toContain("index.malloy");
   });

   it("sends a gated stored source live, and keeps a gated colocated one", () => {
      const gated = { gated: () => true };
      const [stored] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], gated),
      );
      expect(stored).toMatchObject({
         verdict: "live",
         reason: "row_level_gate",
      });
      const [colocated] = computeServeVerdicts(facts([plan("orders")], gated));
      expect(colocated.verdict).toBe("not_built");
   });

   it("serves a gated stored source when the served copy keeps its gates (R3), never a gated rollup", () => {
      const keeps = {
         gated: () => true,
         servedCopyKeepsGates: true,
         boundStorageNames: new Set(["orders"]),
         servedStorageNames: new Set(["orders"]),
      };
      const [stored] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], keeps),
      );
      expect(stored.verdict).toBe("serves");
      const rollup = plan("orders_rollup_1", {
         origin: "preaggregate",
         preaggregate: {
            baseSourceName: "orders",
            grainDimensions: ["region"],
            measures: [],
         } as unknown as PersistSourcePlan["preaggregate"],
      });
      const [rolled] = computeServeVerdicts(facts([rollup], keeps));
      expect(rolled).toMatchObject({
         verdict: "live",
         reason: "row_level_gate",
      });
   });

   it("does not call a gated #(preaggregate) rollup gated out when its variant keeps the gates (row 1)", () => {
      const rollup = plan("orders_rollup_1", {
         origin: "preaggregate",
         preaggregate: {
            baseSourceName: "orders",
            grainDimensions: ["region"],
            measures: [],
         } as unknown as PersistSourcePlan["preaggregate"],
      });
      const keeping = (keeps: boolean) =>
         computeServeVerdicts(
            facts([rollup], {
               gated: () => true,
               servedCopyKeepsGates: true,
               rollupKeepsGates: () => keeps,
            }),
         )[0];
      expect(keeping(true)?.reason).not.toBe("row_level_gate");
      expect(keeping(false)).toMatchObject({
         verdict: "live",
         reason: "row_level_gate",
      });
   });

   it("reads a rollup's reach and gate through its base, by name", () => {
      const asked: [string | undefined, string, string | undefined][] = [];
      const rollup = plan("orders_rollup_1", {
         origin: "preaggregate",
         preaggregate: {
            baseSourceName: "orders",
            grainDimensions: ["region"],
            measures: [],
         } as unknown as PersistSourcePlan["preaggregate"],
      });
      const [verdict] = computeServeVerdicts(
         facts([rollup], {
            reachable: (id, name) => {
               asked.push([id, name, undefined]);
               return true;
            },
            gated: (id, name, modelPath) => {
               asked.push([id, name, modelPath]);
               return true;
            },
         }),
      );
      expect(asked).toEqual([
         [undefined, "orders", undefined],
         [undefined, "orders", "m.malloy"],
      ]);
      expect(verdict).toMatchObject({
         origin: "preaggregate",
         verdict: "live",
         reason: "row_level_gate",
      });
      expect(verdict.message).toContain("rollup of `orders` at (region)");
   });

   it("reports refused sources and rollups left out of the plan", () => {
      const verdicts = computeServeVerdicts({
         ...facts([plan("orders")]),
         plan: {
            graphs: [],
            sources: Object.fromEntries([plan("orders")]),
            refusedSources: {
               "gated@file:///pkg/m.malloy": {
                  name: "gated",
                  sourceID: "gated@file:///pkg/m.malloy",
                  modelPath: "m.malloy",
                  tier: "storage",
                  reason: "authorize",
                  message: "it is gated by #(authorize)",
               },
               // A preaggregate refusal beside a planned base adds nothing.
               "orders@file:///pkg/m.malloy": {
                  name: "orders",
                  sourceID: "orders@file:///pkg/m.malloy",
                  tier: "preaggregate",
                  reason: "authorize",
                  message: "x",
               },
            },
         } as unknown as components["schemas"]["BuildPlan"],
         rollupsOverStoredBase: [{ baseName: "events", modelPath: "m.malloy" }],
      });
      expect(verdicts.map((v) => [v.sourceName, v.verdict, v.reason])).toEqual([
         ["orders", "not_built", undefined],
         ["gated", "live", "refused"],
         ["events", "live", "rollup_over_stored_base"],
      ]);
      expect(verdicts[1].message).toContain("gated by #(authorize)");
   });

   it("carries a stored source's view verdicts", () => {
      const views = new Map([
         [
            "orders@file:///pkg/m.malloy",
            [
               { view: "by_region", verdict: "serves" as const },
               {
                  view: "odd",
                  verdict: "live" as const,
                  detail: "'x' is not defined",
               },
            ],
         ],
      ]);
      const [verdict] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], { views }),
      );
      expect(verdict.verdict).toBe("not_built");
      expect(verdict.views).toEqual(views.get("orders@file:///pkg/m.malloy"));
   });

   it("keys view verdicts by sourceID, so two models' same-named sources keep their own", () => {
      const [aID, a] = plan("orders", STORAGE);
      const bID = "orders@file:///pkg/other.malloy";
      const b = { ...a, sourceID: bID, modelPath: "other.malloy" };
      const views = new Map([
         [aID, [{ view: "by_region", verdict: "serves" as const }]],
         [bID, [{ view: "by_day", verdict: "live" as const, detail: "x" }]],
      ]);
      const verdicts = computeServeVerdicts(
         facts(
            [
               [aID, a],
               [bID, b],
            ],
            { views },
         ),
      );
      expect(verdicts.map((v) => v.views?.[0]?.view)).toEqual([
         "by_region",
         "by_day",
      ]);
   });

   it("sends a planned source the bind refuses live, naming why", () => {
      const [verdict] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], {
            refusedAtBind: () => "two models' rollups share this name",
         }),
      );
      expect(verdict).toMatchObject({ verdict: "live", reason: "refused" });
      expect(verdict.message).toContain("two models' rollups share this name");
   });

   it("says a bound table past its freshness window is not served", () => {
      const verdicts = computeServeVerdicts(
         facts([plan("orders"), plan("events", STORAGE)], {
            boundEntityIds: new Set(["entity-orders"]),
            freshEntityIds: new Set(),
            boundStorageNames: new Set(["events"]),
            servedStorageNames: new Set(["events"]),
            freshStorageNames: new Set(),
         }),
      );
      expect(verdicts.map((v) => [v.sourceName, v.verdict, v.reason])).toEqual([
         ["orders", "live", "stale"],
         ["events", "live", "stale"],
      ]);
   });

   // P13: a deployment the source cannot be written into is a publish
   // warning, and the source serves live.
   describe("deployment facts", () => {
      it("a storage= destination the environment does not configure is live, with the destination and the fixes", () => {
         const [v] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               destinationConfigured: (name) => name !== "lake",
            }),
         );
         expect(v).toMatchObject({
            tier: "storage",
            verdict: "live",
            reason: "destination_missing",
         });
         expect(v.message).toContain('storage="lake"');
         expect(v.fixes?.join(" ")).toContain('named "lake"');
      });

      it("names the missing destination ahead of write-only, whose build would fail on it", () => {
         const [v] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               storageMode: "write-only",
               destinationConfigured: () => false,
            }),
         );
         expect(v.reason).toBe("destination_missing");
      });

      it("never reads a destination while PERSIST_STORAGE_MODE=off ignores storage=", () => {
         const [v] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               storageMode: "off",
               destinationConfigured: () => false,
            }),
         );
         expect(v.reason).toBe("storage_mode");
      });

      it("does not warn when the destination is configured, or the list is not known", () => {
         for (const answer of [true, undefined]) {
            const [v] = computeServeVerdicts(
               facts([plan("orders", STORAGE)], {
                  destinationConfigured: () => answer,
               }),
            );
            expect(v.verdict).toBe("not_built");
         }
      });

      it("never calls a bound table unbuildable: what serves is reported as serving", () => {
         const [stored] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               boundStorageNames: new Set(["orders"]),
               servedStorageNames: new Set(["orders"]),
               destinationConfigured: () => false,
            }),
         );
         expect(stored.verdict).toBe("serves");
         const [colocated] = computeServeVerdicts(
            facts([plan("orders")], {
               boundEntityIds: new Set(["entity-orders"]),
               connectionCanHoldTable: () => false,
            }),
         );
         expect(colocated.verdict).toBe("serves");
      });

      it("a colocated source on a connection that cannot hold a table is live, naming the connection", () => {
         const [v] = computeServeVerdicts(
            facts([plan("orders")], {
               connectionCanHoldTable: (name) => name !== "duckdb",
            }),
         );
         expect(v).toMatchObject({
            tier: "colocated",
            verdict: "live",
            reason: "connection_cannot_hold_table",
         });
         expect(v.message).toContain("'duckdb'");
         expect(v.fixes?.[0]).toContain("storage=");
      });

      it("reads the connection's capability only for a colocated source, and never warns on an unknown one", () => {
         // A storage= source only reads its own connection.
         const [stored] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               connectionCanHoldTable: () => false,
            }),
         );
         expect(stored.verdict).toBe("not_built");
         const [unknown] = computeServeVerdicts(
            facts([plan("orders")], {
               connectionCanHoldTable: () => undefined,
            }),
         );
         expect(unknown.verdict).toBe("not_built");
      });
   });

   it("is empty for a package that materializes nothing", () => {
      expect(computeServeVerdicts({ ...facts([]), plan: null })).toEqual([]);
   });
});

describe("reachableSourceIDs", () => {
   it("collects every sourceID and referenceID in a structure, joins included, once", () => {
      const joined = { type: "table", sourceID: "dim@b", fields: [] };
      const root = {
         type: "query_source",
         sourceID: "fact@a",
         referenceID: "base@a",
         fields: [{ ...joined, join: "one" }],
      };
      // A cycle, as compiled structures can share nodes.
      (root as Record<string, unknown>).self = root;
      expect([...reachableSourceIDs([root])].sort()).toEqual([
         "base@a",
         "dim@b",
         "fact@a",
      ]);
   });

   it("judges a #(materialize) source by the served copy, not the serve shape", () => {
      const bound = {
         boundStorageNames: new Set(["orders"]),
         // The serve shape never keeps a #(materialize) binding.
         servedStorageNames: new Set<string>(),
      };
      const verdictWith = (
         state: "serves" | "off" | "not_served",
         extra: Partial<PersistSourcePlan> = STORAGE,
      ) =>
         computeServeVerdicts(
            facts([plan("orders", extra)], {
               ...bound,
               materialize: () => state,
            }),
         )[0];
      expect(verdictWith("serves")).toMatchObject({ verdict: "serves" });
      expect(verdictWith("serves").message).toContain("served copy");
      expect(verdictWith("not_served")).toMatchObject({
         verdict: "live",
         reason: "served_copy",
      });
      expect(verdictWith("off")).toMatchObject({
         verdict: "live",
         reason: "served_copy",
      });
      // A colocated #(materialize) table is served by the served copy too,
      // on the source's own connection (phase 3 pull request 6).
      const colocatedWith = (
         state: "serves" | "off" | "not_served",
         bound = new Set(["entity-orders"]),
      ) =>
         computeServeVerdicts(
            facts([plan("orders")], {
               boundEntityIds: bound,
               materialize: () => state,
            }),
         )[0];
      expect(colocatedWith("serves")).toMatchObject({ verdict: "serves" });
      expect(colocatedWith("serves").message).toContain("served copy");
      expect(colocatedWith("not_served")).toMatchObject({
         verdict: "live",
         reason: "served_copy",
      });
      expect(colocatedWith("serves", new Set())).toMatchObject({
         verdict: "not_built",
      });
      // Off: the fix names only the materialize switch, not the storage tier.
      expect(colocatedWith("off")).toMatchObject({
         verdict: "live",
         reason: "served_copy",
      });
      expect(JSON.stringify(colocatedWith("off"))).not.toContain(
         "PERSIST_STORAGE_MODE",
      );
      // A #@ persist source keeps today's serve-shape rule.
      expect(
         computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               ...bound,
               materialize: () => undefined,
            }),
         )[0],
      ).toMatchObject({ verdict: "live", reason: "shadowed" });
   });
});
