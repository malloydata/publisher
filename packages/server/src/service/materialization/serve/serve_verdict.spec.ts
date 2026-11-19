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
      keepsGate: () => false,
      boundEntityIds: new Set(),
      boundStorageNames: new Set(),
      servedStorageNames: new Set(),
      // The bind keeps every source, none is a #(materialize) source or a
      // persisted result, and every deployment fact is unknown.
      refusedAtBind: () => undefined,
      materialize: () => undefined,
      persistedViewOf: () => undefined,
      destinationConfigured: () => undefined,
      connectionCanHoldTable: () => undefined,
      ...overrides,
   };
}

describe("computeServeVerdicts", () => {
   it("names each verdict's origin by the mechanism that declared it", () => {
      const view = `ops__by_carrier__view@file:///pkg/m.malloy.persist-view.malloy`;
      const query = `top__query@file:///pkg/m.malloy.persist-view.malloy`;
      const sources: [string, PersistSourcePlan][] = [
         plan("orders"),
         plan("mat"),
         [view, { ...plan("ops__by_carrier__view")[1], sourceID: view }],
         [query, { ...plan("top__query")[1], sourceID: query }],
         plan("roll", { origin: "rollup" }),
      ];
      const verdicts = computeServeVerdicts({
         ...facts(sources, {
            materialize: (id) => (id.startsWith("mat@") ? "serves" : undefined),
            declaredMaterialize: (id) => id.startsWith("refused_mat@"),
         }),
         plan: {
            graphs: [],
            sources: Object.fromEntries(sources),
            refusedSources: {
               "refused_mat@file:///pkg/m.malloy": {
                  name: "refused_mat",
                  sourceID: "refused_mat@file:///pkg/m.malloy",
                  tier: "database",
                  reason: "access_filter",
                  message: "gated",
               },
            },
         } as never,
      });
      expect(verdicts.map((v) => [v.sourceName, v.origin])).toEqual([
         ["orders", "persist"],
         ["mat", "materialize"],
         ["ops__by_carrier__view", "view"],
         ["top__query", "query"],
         ["roll", "rollup"],
         ["refused_mat", "materialize"],
      ]);
   });

   it("says a database source serves once bound, and will serve before", () => {
      const [unbound] = computeServeVerdicts(facts([plan("orders")]));
      expect(unbound).toMatchObject({
         sourceName: "orders",
         tier: "database",
         origin: "persist",
         verdict: "awaiting_build",
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

   // F-45: a host may place a source with no `storage=` in a storage
   // destination, and queries read it from that storage binding
   // (`storageLookupOf`). The verdict's tier is the binding's, as theirs is.
   it("judges a source a host placed in storage by its storage binding, not its annotation", () => {
      const sources = [
         plan("orders"),
         plan("orders_rollup_1", { origin: "rollup" }),
         plan("ops__by_carrier__view"),
      ];
      const names = new Set(sources.map(([, s]) => s.name));
      const asked: string[] = [];
      const verdicts = computeServeVerdicts(
         facts(sources, {
            boundStorageNames: names,
            servedStorageNames: names,
            keepsGate: (_id, tier) => {
               asked.push(tier);
               return true;
            },
            refusedAtBind: (_id, _source, tier) => {
               asked.push(tier);
               return undefined;
            },
            gated: () => true,
         }),
      );
      expect(verdicts.map((v) => [v.tier, v.verdict])).toEqual([
         ["storage", "serves"],
         ["storage", "serves"],
         ["storage", "serves"],
      ]);
      // The gate and the bind are asked of the storage tier too.
      expect(new Set(asked)).toEqual(new Set(["storage"]));
      // With no storage binding, the annotation still decides.
      const [unbound] = computeServeVerdicts(facts([plan("orders")]));
      expect(unbound).toMatchObject({
         tier: "database",
         verdict: "awaiting_build",
      });
   });

   it("names no storage= to drop for a host's storage binding while the storage mode is off", () => {
      const [verdict] = computeServeVerdicts(
         facts([plan("orders")], {
            storageMode: "off",
            boundStorageNames: new Set(["orders"]),
         }),
      );
      expect(verdict).toMatchObject({
         tier: "storage",
         verdict: "live",
         reason: "storage_mode",
      });
      expect(verdict.message).not.toContain("storage=");
      expect(verdict.fixes).toEqual([
         "Set PERSIST_STORAGE_MODE=on on the server.",
      ]);
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
      // The mode says nothing about a database source.
      const [database] = computeServeVerdicts(
         facts([plan("orders")], { storageMode: "off" }),
      );
      expect(database.verdict).toBe("awaiting_build");
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

   it("sends a gated stored source live, and keeps a gated database one", () => {
      // G3 with gated sources off: the policy keeps a database table's gate.
      const gated = {
         gated: () => true,
         keepsGate: (_id: string, tier: string) => tier === "database",
      };
      const [stored] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], gated),
      );
      expect(stored).toMatchObject({
         verdict: "live",
         reason: "row_level_gate",
      });
      const [database] = computeServeVerdicts(facts([plan("orders")], gated));
      expect(database.verdict).toBe("awaiting_build");
   });

   it("serves a gated stored source plan admitted past its own gate (G3) as a database one, and no other stored source", () => {
      const bound = {
         gated: () => true,
         boundStorageNames: new Set(["orders", "other"]),
         servedStorageNames: new Set(["orders", "other"]),
         keepsGate: (id: string, tier: string) =>
            tier === "database" || id.startsWith("orders@"),
      };
      const verdicts = computeServeVerdicts(
         facts([plan("orders", STORAGE), plan("other", STORAGE)], bound),
      );
      expect(verdicts.map((v) => [v.sourceName, v.verdict, v.reason])).toEqual([
         ["orders", "serves", undefined],
         ["other", "live", "row_level_gate"],
      ]);
   });

   it("serves a gated stored source when the served copy keeps its gates (R3), never a gated rollup", () => {
      // Gated sources on: the served copy keeps every table's gate, and no
      // rollup here was proven at plan.
      const keeps = {
         gated: () => true,
         keepsGate: (id: string) => !id.startsWith("orders_rollup"),
         boundStorageNames: new Set(["orders"]),
         servedStorageNames: new Set(["orders"]),
      };
      const [stored] = computeServeVerdicts(
         facts([plan("orders", STORAGE)], keeps),
      );
      expect(stored.verdict).toBe("serves");
      const rollup = plan("orders_rollup_1", {
         origin: "rollup",
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

   it("does not call a gated #(rollup) rollup gated out when its variant keeps the gates (row 1)", () => {
      const rollup = plan("orders_rollup_1", {
         origin: "rollup",
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
               keepsGate: () => keeps,
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
         origin: "rollup",
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
         origin: "rollup",
         verdict: "live",
         reason: "row_level_gate",
      });
      expect(verdict.message).toContain("rollup of `orders` at (region)");
   });

   it("reports refused sources", () => {
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
               // A rollup's refusal beside its planned entry adds nothing.
               "orders@file:///pkg/m.malloy": {
                  name: "orders",
                  sourceID: "orders@file:///pkg/m.malloy",
                  origin: "rollup",
                  tier: "database",
                  reason: "authorize",
                  message: "x",
               },
            },
         } as unknown as components["schemas"]["BuildPlan"],
      });
      expect(verdicts.map((v) => [v.sourceName, v.verdict, v.reason])).toEqual([
         ["orders", "awaiting_build", undefined],
         ["gated", "live", "refused"],
      ]);
      expect(verdicts[1].message).toContain("gated by #(authorize)");
   });

   it("gives a refused rollup the tier its record gives, and reads a record before this release as a rollup", () => {
      const refused = (name: string, tier: string, origin?: string) => ({
         name,
         sourceID: `${name}@file:///pkg/m.preaggregate.malloy`,
         modelPath: "m.malloy",
         tier,
         ...(origin ? { origin } : {}),
         reason: "hidden_field",
         message: "x",
      });
      const verdicts = computeServeVerdicts({
         ...facts([]),
         plan: {
            graphs: [],
            sources: {},
            refusedSources: Object.fromEntries(
               [
                  refused("in_lake", "storage", "rollup"),
                  refused("in_db", "database", "rollup"),
                  // As a release before this one wrote it.
                  refused("legacy", "preaggregate"),
               ].map((r) => [r.sourceID, r]),
            ),
         } as unknown as components["schemas"]["BuildPlan"],
      });
      expect(verdicts.map((v) => [v.sourceName, v.origin, v.tier])).toEqual([
         ["in_lake", "rollup", "storage"],
         ["in_db", "rollup", "database"],
         ["legacy", "rollup", "database"],
      ]);
   });

   it("gives a rollup over a storage= base its own verdict, beside its base's", () => {
      // A `#@ preaggregate` line is planned as the `#(rollup)` it reads
      // as, over the author's rows, so a stored base no longer leaves its
      // rollups out: each is a planned source, bound and served on its own.
      const base = plan("daily", STORAGE);
      const rollup = plan("daily__rollup__category__1a2b3c4d", {
         ...STORAGE,
         origin: "rollup",
         preaggregate: {
            baseSourceName: "daily",
            grainDimensions: ["category"],
            measures: ["total"],
         } as unknown as PersistSourcePlan["preaggregate"],
      });
      const names = new Set(["daily", "daily__rollup__category__1a2b3c4d"]);
      const verdicts = computeServeVerdicts(
         facts([base, rollup], {
            boundStorageNames: names,
            servedStorageNames: names,
         }),
      );
      expect(
         verdicts.map((v) => [v.sourceName, v.origin, v.verdict, v.reason]),
      ).toEqual([
         ["daily", "persist", "serves", undefined],
         ["daily__rollup__category__1a2b3c4d", "rollup", "serves", undefined],
      ]);
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
      expect(verdict.verdict).toBe("awaiting_build");
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
            expect(v.verdict).toBe("awaiting_build");
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
         const [database] = computeServeVerdicts(
            facts([plan("orders")], {
               boundEntityIds: new Set(["entity-orders"]),
               connectionCanHoldTable: () => false,
            }),
         );
         expect(database.verdict).toBe("serves");
      });

      it("a database source on a connection that cannot hold a table is live, naming the connection", () => {
         const [v] = computeServeVerdicts(
            facts([plan("orders")], {
               connectionCanHoldTable: (name) => name !== "duckdb",
            }),
         );
         expect(v).toMatchObject({
            tier: "database",
            verdict: "live",
            reason: "connection_cannot_hold_table",
         });
         expect(v.message).toContain("'duckdb'");
         expect(v.fixes?.[0]).toContain("storage=");
      });

      it("reads the connection's capability only for a database source, and never warns on an unknown one", () => {
         // A storage= source only reads its own connection.
         const [stored] = computeServeVerdicts(
            facts([plan("orders", STORAGE)], {
               connectionCanHoldTable: () => false,
            }),
         );
         expect(stored.verdict).toBe("awaiting_build");
         const [unknown] = computeServeVerdicts(
            facts([plan("orders")], {
               connectionCanHoldTable: () => undefined,
            }),
         );
         expect(unknown.verdict).toBe("awaiting_build");
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
      // Two causes, two names: the served copy is off, against a bound
      // table no served copy re-roots.
      expect(verdictWith("off")).toMatchObject({
         verdict: "live",
         reason: "materialization_off",
      });
      // A database #(materialize) table is served by the served copy too,
      // on the source's own connection (phase 3 pull request 6).
      const databaseWith = (
         state: "serves" | "off" | "not_served",
         bound = new Set(["entity-orders"]),
      ) =>
         computeServeVerdicts(
            facts([plan("orders")], {
               boundEntityIds: bound,
               materialize: () => state,
            }),
         )[0];
      expect(databaseWith("serves")).toMatchObject({ verdict: "serves" });
      expect(databaseWith("serves").message).toContain("served copy");
      expect(databaseWith("not_served")).toMatchObject({
         verdict: "live",
         reason: "served_copy",
      });
      expect(databaseWith("serves", new Set())).toMatchObject({
         verdict: "awaiting_build",
      });
      // Off: the fix names only the materialize switch, not the storage tier.
      expect(databaseWith("off")).toMatchObject({
         verdict: "live",
         reason: "materialization_off",
      });
      expect(JSON.stringify(databaseWith("off"))).not.toContain(
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
