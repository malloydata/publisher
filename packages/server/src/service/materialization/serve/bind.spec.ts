// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../domain/table_plans";
import { storedColumnsOfOutput } from "../stored_column_types";
import {
   applyObservedColumns,
   bindDatabaseEntries,
   bindStorageEntries,
   deriveDatabaseServeBindings,
   kindFromPlan,
   v1StoredColumnsOf,
} from "./bind";
import type { BuildPlan, ManifestEntry } from "../domain/types";
import { boundTablePath, buildVirtualMap } from "./bindings";
import type { Binding } from "../domain/binding";
import { databaseDriftFindings } from "./storage_bindings";

const ID = "orders@file:///pkg/m.malloy";

function plan(over: Partial<MaterializePlan> = {}): MaterializePlan {
   return {
      sourceName: "orders",
      sourceID: ID,
      buildSourceName: "orders__mat",
      columns: [],
      persist: { name: "orders_all" },
      storedColumns: [
         { name: "id", type: "BIGINT" },
         { name: "region", type: "VARCHAR" },
      ],
      ...over,
   };
}

const SOURCE = {
   name: "orders",
   sourceEntityId: "addr1",
   connectionName: "pg",
   dialect: "postgres",
};

function derive(
   over: {
      plan?: Partial<MaterializePlan>;
      source?: Partial<typeof SOURCE> & {
         annotationFields?: Record<string, string>;
      };
      entry?: Record<string, unknown>;
      eligible?: boolean;
   } = {},
) {
   return deriveDatabaseServeBindings({
      entries: {
         addr1: {
            tableName: "analytics.orders_all",
            connectionName: "pg",
            dataAsOf: "2026-11-12T00:00:00Z",
            ...over.entry,
         },
      },
      planSources: { [ID]: { ...SOURCE, ...over.source } },
      materializePlans: { [ID]: plan(over.plan) },
      eligible: () => over.eligible ?? true,
   });
}

describe("deriveDatabaseServeBindings", () => {
   it("binds a database #(materialize) table on its own connection, keyed by its content address", () => {
      expect(derive()).toEqual([
         {
            kind: "materialize",
            tier: "database",
            sourceName: "orders",
            sourceIDs: [ID],
            destinationName: "pg",
            virtualHandle: "addr1",
            // Canonical for Postgres, as Malloy's manifest pastes it.
            tablePath: '"analytics"."orders_all"',
            schema: [
               { name: "id", type: "BIGINT" },
               { name: "region", type: "VARCHAR" },
            ],
            freshAsOf: "2026-11-12T00:00:00Z",
         },
      ]);
   });

   it("binds nothing for a storage= source, which the storage manifest binds", () => {
      expect(
         derive({ plan: { persist: { name: "x", storage: "lake" } } }),
      ).toEqual([]);
      expect(
         derive({ source: { annotationFields: { storage: "lake" } } }),
      ).toEqual([]);
   });

   it("binds nothing the bind's eligibility refuses", () => {
      expect(derive({ eligible: false })).toEqual([]);
   });

   it("binds nothing for a table the manifest records on another connection", () => {
      expect(derive({ entry: { connectionName: "other" } })).toEqual([]);
   });

   it("declares the plan's columns as typed, and adds only a zoned timestamp the build captured", () => {
      const [binding] = derive({
         entry: {
            schema: [
               // The warehouse holds a truncation as a timestamp: the plan's
               // type stands.
               { name: "id", type: "BIGINT" },
               { name: "region", type: "TIMESTAMP" },
               { name: "seen", type: "TIMESTAMP WITH TIME ZONE" },
               { name: "blob", type: "BLOB" },
            ],
         },
      });
      expect(binding?.schema).toEqual([
         { name: "id", type: "BIGINT" },
         { name: "region", type: "VARCHAR" },
         { name: "seen", type: "TIMESTAMP WITH TIME ZONE" },
      ]);
   });

   it("reads a zoned timestamp from the captured warehouse types when the build captured no columns (a Postgres table, whose name its schema reader refuses)", () => {
      const [binding] = derive({
         entry: {
            nativeSchema: [
               { name: "id", type: "bigint" },
               { name: "region", type: "text" },
               { name: "seen", type: "timestamp with time zone" },
               { name: "tags", type: "text[]" },
            ],
         },
      });
      expect(binding?.schema).toEqual([
         { name: "id", type: "BIGINT" },
         { name: "region", type: "VARCHAR" },
         { name: "seen", type: "TIMESTAMP WITH TIME ZONE" },
      ]);
   });

   it("binds nothing when the build's columns are unknown, so the source serves live", () => {
      expect(derive({ plan: { storedColumns: undefined } })).toEqual([]);
      expect(derive({ plan: { storedColumns: [] } })).toEqual([]);
   });

   it("binds nothing for a v1 #@ persist source (no #(materialize) plan) or a manifest without its address", () => {
      expect(
         deriveDatabaseServeBindings({
            entries: { addr1: { tableName: "t" } },
            planSources: { [ID]: SOURCE },
            materializePlans: {},
            eligible: () => true,
         }),
      ).toEqual([]);
      expect(
         deriveDatabaseServeBindings({
            entries: { other: { tableName: "t" } },
            planSources: { [ID]: SOURCE },
            materializePlans: { [ID]: plan() },
            eligible: () => true,
         }),
      ).toEqual([]);
      expect(
         deriveDatabaseServeBindings({
            entries: undefined,
            planSources: { [ID]: SOURCE },
            materializePlans: { [ID]: plan() },
            eligible: () => true,
         }),
      ).toEqual([]);
   });

   it("puts a database table's path in the virtual map as bound, a storage table's quoted for DuckDB", () => {
      const database = {
         ...derive()[0],
         destinationName: "bq",
         tablePath: "proj.ds.orders_all",
      };
      const storage: Binding = {
         kind: "persist",
         tier: "storage",
         sourceName: "s",
         destinationName: "lake",
         virtualHandle: "addr2",
         tablePath: "main.s",
         schema: [],
      };
      expect(boundTablePath(database)).toBe("proj.ds.orders_all");
      expect(boundTablePath(storage)).toBe('"main"."s"');
      expect(buildVirtualMap([database, storage])).toEqual(
         new Map([
            ["bq", new Map([["addr1", "proj.ds.orders_all"]])],
            ["lake", new Map([["addr2", '"main"."s"']])],
         ]),
      );
   });
});

describe("storedColumnsOfOutput", () => {
   it("spells each build output column's type as DuckDB does, and leaves out what it cannot declare", () => {
      const fields = [
         { name: "s", type: "string" },
         { name: "i", type: "number", numberType: "integer" },
         { name: "f", type: "number", numberType: "float" },
         { name: "b", type: "boolean" },
         { name: "d", type: "date" },
         { name: "t", type: "timestamp" },
         { name: "renamed", as: "shown", type: "string" },
         // No faithful declaration: left out, so the source serves live.
         { name: "tz", type: "timestamptz" },
         { name: "j", type: "json" },
         { name: "n", type: "sql native" },
         {
            name: "rec",
            type: "record",
            fields: [{ name: "a", type: "string" }],
         },
         {
            name: "arr",
            type: "array",
            elementTypeDef: { type: "number", numberType: "integer" },
         },
         {
            name: "rows",
            type: "array",
            elementTypeDef: { type: "record_element" },
            fields: [{ name: "x", type: "date" }],
         },
      ];
      expect(storedColumnsOfOutput(fields, { nested: true })).toEqual([
         { name: "s", type: "VARCHAR" },
         { name: "i", type: "BIGINT" },
         { name: "f", type: "DOUBLE" },
         { name: "b", type: "BOOLEAN" },
         { name: "d", type: "DATE" },
         { name: "t", type: "TIMESTAMP" },
         { name: "shown", type: "VARCHAR" },
         { name: "rec", type: 'STRUCT("a" VARCHAR)' },
         { name: "arr", type: "BIGINT[]" },
         { name: "rows", type: 'STRUCT("x" DATE)[]' },
      ]);
      // Off DuckDB a nest is stored as JSON: never declared as a record.
      expect(
         storedColumnsOfOutput(fields, { nested: false }).map((c) => c.name),
      ).toEqual(["s", "i", "f", "b", "d", "t", "shown"]);
   });
});

describe("deriveDatabaseServeBindings: v1 database #@ persist (the v1 cutover, G1)", () => {
   const V1 = "daily@file:///pkg/m.malloy";
   const VIEW = "ov__by_region__view@file:///pkg/m.malloy";
   const sources = {
      [V1]: {
         name: "daily",
         sourceEntityId: "addr-v1",
         connectionName: "duckdb",
         dialect: "duckdb",
      },
      [VIEW]: {
         name: "ov__by_region__view",
         sourceEntityId: "addr-view",
         connectionName: "duckdb",
         dialect: "duckdb",
      },
   };
   const derived = [{ name: "region", type: "VARCHAR" }];
   const derive = (args: {
      schema?: { name: string; type: string }[];
      v1?: boolean;
      storage?: boolean;
      eligible?: boolean;
   }) =>
      deriveDatabaseServeBindings({
         entries: {
            "addr-v1": {
               tableName: "daily",
               connectionName: "duckdb",
               ...(args.schema ? { schema: args.schema } : {}),
            },
            "addr-view": {
               tableName: "ov_by_region",
               connectionName: "duckdb",
            },
         },
         planSources: args.storage
            ? {
                 [V1]: {
                    ...sources[V1],
                    annotationFields: { storage: "lake" },
                 },
              }
            : sources,
         materializePlans: {},
         eligible: () => args.eligible ?? true,
         ...(args.v1 === false
            ? {}
            : {
                 v1Schema: () => derived,
                 notV1: (id: string) => id === VIEW,
              }),
      });

   it("binds nothing for a v1 source without the switch (no v1Schema)", () => {
      expect(derive({ v1: false })).toEqual([]);
   });

   it("binds a v1 table with the schema its build captured, over the derived one", () => {
      const captured = [
         { name: "region", type: "VARCHAR" },
         { name: "total", type: "DOUBLE" },
      ];
      expect(derive({ schema: captured })).toEqual([
         {
            kind: "persist",
            tier: "database",
            sourceName: "daily",
            sourceIDs: [V1],
            destinationName: "duckdb",
            virtualHandle: "addr-v1",
            // Canonical for DuckDB, as Malloy's manifest pastes it.
            tablePath: '"daily"',
            schema: captured,
         },
      ]);
   });

   it("derives the schema for a table built before the capture", () => {
      expect(derive({}).map((b) => b.schema)).toEqual([derived]);
   });

   it("never binds a persisted view's build source, a storage= source, or one the bind does not admit", () => {
      expect(derive({}).map((b) => b.sourceName)).toEqual(["daily"]);
      expect(derive({ storage: true })).toEqual([]);
      expect(derive({ eligible: false })).toEqual([]);
   });
});

describe("v1StoredColumnsOf", () => {
   it("reads a query-shaped source's output columns, with the two corrections a v1 table needs", () => {
      const def = {
         dialect: "duckdb",
         fields: [
            { name: "region", type: "string" },
            // Renamed in the extend block: stored under its name before it.
            { name: "total", as: "revenue", type: "number" },
            // A nest is a column, though Malloy gives it a join.
            {
               name: "by_status",
               type: "array",
               join: "many",
               elementTypeDef: { type: "record_element" },
               fields: [{ name: "status", type: "string" }],
            },
            // Not stored: a computed dimension, a view, a join.
            { name: "big", type: "boolean", e: { node: "true" } },
            { name: "by_region", type: "turtle" },
            { name: "c", type: "table", join: "one", fields: [] },
         ],
      };
      expect(v1StoredColumnsOf(def)).toEqual([
         { name: "region", type: "VARCHAR" },
         { name: "total", type: "DOUBLE" },
         { name: "by_status", type: 'STRUCT("status" VARCHAR)[]' },
      ]);
      // Off DuckDB a nest is stored as JSON, which no type: declares.
      expect(
         v1StoredColumnsOf({ ...def, dialect: "postgres" })?.map((c) => c.name),
      ).toEqual(["region", "total"]);
      expect(v1StoredColumnsOf(undefined)).toBeUndefined();
   });
});

describe("bindStorageEntries", () => {
   const plan = {
      sources: {
         "orders@m": {
            name: "orders",
            sourceEntityId: "addr_orders",
            connectionName: "duckdb",
         },
         "gated@m": {
            name: "gated",
            sourceEntityId: "addr_gated",
            connectionName: "duckdb",
         },
      },
   } as unknown as BuildPlan;
   const entry = (sourceName: string, sourceEntityId: string): ManifestEntry =>
      ({
         sourceName,
         sourceEntityId,
         storageDestinationName: "lake",
         physicalTableName: `${sourceName}_t`,
         schema: [{ name: "x", type: "INTEGER" }],
      }) as ManifestEntry;
   const entries = {
      addr_orders: entry("orders", "addr_orders"),
      addr_gated: entry("gated", "addr_gated"),
      // Built from another definition of `orders`: another address.
      addr_old: entry("orders", "addr_old"),
   };
   const bind = (
      eligibility: Parameters<typeof bindStorageEntries>[0]["eligibility"],
   ) =>
      bindStorageEntries({
         entries,
         plan,
         eligibility,
         kindOf: (b) => b.kind,
      });

   it("binds the eligible sources' tables, at their plan addresses, as storage tables", () => {
      const bound = bind({
         eligible: ["orders@m"],
         refused: { "gated@m": "it reads a given" },
      });
      expect(
         bound.bindings.map((b) => [b.sourceName, b.sourceIDs, b.kind, b.tier]),
      ).toEqual([["orders", ["orders@m"], "persist", "storage"]]);
      expect(bound.otherDefinition).toEqual(["orders"]);
      expect(bound.declined).toEqual([
         {
            reason: "refused",
            sourceName: "gated",
            rollup: false,
            detail: "it reads a given",
         },
      ]);
   });

   it("declines everything when the plan's eligibility is unknown", () => {
      const bound = bind(undefined);
      expect(bound.bindings).toEqual([]);
      expect(bound.declined.map((d) => [d.sourceName, d.reason])).toEqual([
         ["orders", "build_plan_unavailable"],
         ["gated", "build_plan_unavailable"],
      ]);
   });

   it("reads each binding's kind through kindOf", () => {
      const bound = bindStorageEntries({
         entries,
         plan,
         eligibility: { eligible: ["orders@m"], refused: {} },
         kindOf: () => "materialize",
      });
      expect(bound.bindings.map((b) => b.kind)).toEqual(["materialize"]);
   });

   it("marks a source's or a rollup's table kept past its own gate only as plan decided (G3), and never a view's", () => {
      const eligibility = {
         eligible: ["orders@m", "gated@m"],
         refused: {},
         gateKept: ["gated@m"],
      };
      const kept = (kind: Binding["kind"]) =>
         bindStorageEntries({
            entries,
            plan,
            eligibility,
            kindOf: () => kind,
         }).bindings.map((b) => [b.sourceName, b.ownGateKept === true]);
      for (const kind of ["persist", "materialize", "rollup"] as const) {
         expect(kept(kind)).toEqual([
            ["orders", false],
            ["gated", true],
         ]);
      }
      for (const kind of ["view"] as const) {
         expect(kept(kind)).toEqual([
            ["orders", false],
            ["gated", false],
         ]);
      }
      // An extension sharing the gated source's table (one address, two
      // names): the table is a G3 table under both names, since the served
      // copy reads it re-rooting the base.
      const shared = bindStorageEntries({
         entries: { addr_gated: entry("gated", "addr_gated") },
         plan: {
            sources: {
               "gated@m": {
                  name: "gated",
                  sourceEntityId: "addr_gated",
                  connectionName: "duckdb",
               },
               "gated_ext@m": {
                  name: "gated_ext",
                  sourceEntityId: "addr_gated",
                  connectionName: "duckdb",
               },
            },
         } as unknown as BuildPlan,
         eligibility: {
            eligible: ["gated@m", "gated_ext@m"],
            refused: {},
            gateKept: ["gated_ext@m"],
         },
         kindOf: (b) => b.kind,
      });
      expect(
         shared.bindings.map((b) => [b.sourceName, b.ownGateKept === true]),
      ).toEqual([
         ["gated", true],
         ["gated_ext", true],
      ]);
      // No record of it (an older plan's): none is kept, failing closed.
      expect(
         bind({ eligible: ["orders@m", "gated@m"], refused: {} }).bindings.map(
            (b) => b.ownGateKept === true,
         ),
      ).toEqual([false, false]);
   });

   it("marks a table as holding only ungated rows only when every source on its address is proven ungated (g-mixed)", () => {
      const ungated = (
         kind: Binding["kind"],
         eligibility: Parameters<typeof bindStorageEntries>[0]["eligibility"],
      ) =>
         bindStorageEntries({
            entries,
            plan,
            eligibility,
            kindOf: () => kind,
         }).bindings.map((b) => [
            b.sourceName,
            b.holdsOnlyUngatedRows === true,
         ]);
      const proven = {
         eligible: ["orders@m", "gated@m"],
         refused: {},
         ungated: ["orders@m"],
      };
      for (const kind of ["persist", "materialize", "rollup"] as const) {
         expect(ungated(kind, proven)).toEqual([
            ["orders", true],
            ["gated", false],
         ]);
      }
      // A persisted view or query decides a gate on its own leg.
      for (const kind of ["view", "query"] as const) {
         expect(ungated(kind, proven)).toEqual([
            ["orders", false],
            ["gated", false],
         ]);
      }
      // No record of it (an older plan's): none, failing closed.
      expect(
         ungated("persist", { eligible: ["orders@m", "gated@m"], refused: {} }),
      ).toEqual([
         ["orders", false],
         ["gated", false],
      ]);
      // A gated source on the same address, whether the plan admitted it
      // past its own gate or refused it: the table holds its rows, so no
      // name of it is marked.
      const sharing = (eligibility: {
         eligible: string[];
         refused: Record<string, string>;
         gateKept?: string[];
         ungated?: string[];
      }) =>
         bindStorageEntries({
            entries: { addr_orders: entry("orders", "addr_orders") },
            plan: {
               sources: {
                  "orders@m": {
                     name: "orders",
                     sourceEntityId: "addr_orders",
                     connectionName: "duckdb",
                  },
                  "orders_gated@m": {
                     name: "orders_gated",
                     sourceEntityId: "addr_orders",
                     connectionName: "duckdb",
                  },
               },
            } as unknown as BuildPlan,
            eligibility,
            kindOf: () => "persist",
         }).bindings.map((b) => b.holdsOnlyUngatedRows === true);
      expect(
         sharing({
            eligible: ["orders@m", "orders_gated@m"],
            refused: {},
            gateKept: ["orders_gated@m"],
            ungated: ["orders@m"],
         }),
      ).toEqual([false, false]);
      expect(
         sharing({
            eligible: ["orders@m"],
            refused: { "orders_gated@m": "an inherited row filter" },
            ungated: ["orders@m"],
         }),
      ).toEqual([false]);
      // Both proven ungated: the one table is marked under both names.
      expect(
         sharing({
            eligible: ["orders@m", "orders_gated@m"],
            refused: {},
            ungated: ["orders@m", "orders_gated@m"],
         }),
      ).toEqual([true, true]);
   });
});

describe("bindDatabaseEntries", () => {
   it("keeps the eligible entries, quoted for their dialect, and declines the rest", () => {
      const bound = bindDatabaseEntries({
         entries: {
            a: { tableName: "order" },
            b: { tableName: "kept" },
            c: { tableName: "refused" },
         },
         eligibility: {
            eligibleEntityIds: new Set(["a", "b"]),
            refused: { c: "a gate the table cannot keep" },
         },
         dialects: new Map([["a", "postgres"]]),
      });
      expect(bound.entries).toEqual({
         a: { tableName: '"order"' },
      });
      // `b` has no dialect to quote its name in: declined, never bound as
      // given.
      expect(bound.declined).toEqual([
         {
            reason: "unquotable_name",
            sourceEntityId: "b",
            detail: "no dialect is known for its table, to quote its name in",
         },
         {
            reason: "refused",
            sourceEntityId: "c",
            detail: "a gate the table cannot keep",
         },
      ]);
   });

   it("declines only an entry whose name its dialect cannot read or quote, and binds the rest", () => {
      const bound = bindDatabaseEntries({
         entries: {
            bad: { tableName: "`ds`.`a``b`" },
            good: { tableName: "`ds`.`a\\`b`" },
            plain: { tableName: "ds.t" },
         },
         eligibility: {
            eligibleEntityIds: new Set(["bad", "good", "plain"]),
            refused: {},
         },
         dialects: new Map([
            ["bad", "standardsql"],
            ["good", "standardsql"],
            ["plain", "standardsql"],
         ]),
      });
      expect(bound.entries).toEqual({
         good: { tableName: "`ds`.`a\\`b`" },
         plain: { tableName: "`ds`.`t`" },
      });
      expect(bound.declined).toHaveLength(1);
      expect(bound.declined[0]).toMatchObject({
         reason: "unquotable_name",
         sourceEntityId: "bad",
      });
   });

   it("declines every entry when the plan's eligibility is unknown", () => {
      const bound = bindDatabaseEntries({
         entries: { a: { tableName: "t" } },
         eligibility: undefined,
         dialects: new Map(),
      });
      expect(bound.entries).toEqual({});
      expect(bound.declined.map((d) => d.reason)).toEqual([
         "build_plan_unavailable",
      ]);
   });
});

describe("kindFromPlan", () => {
   const plan = {
      materializeSourceIDs: new Set(["m@x"]),
      viewBuildSourceIDs: new Set(["v@x"]),
   };
   it("reads a binding's kind off the plan, never its name", () => {
      expect(kindFromPlan({ kind: "rollup", sourceIDs: ["m@x"] }, plan)).toBe(
         "rollup",
      );
      expect(kindFromPlan({ kind: "persist", sourceIDs: ["v@x"] }, plan)).toBe(
         "view",
      );
      expect(kindFromPlan({ kind: "persist", sourceIDs: ["m@x"] }, plan)).toBe(
         "materialize",
      );
      expect(kindFromPlan({ kind: "persist", sourceIDs: ["p@x"] }, plan)).toBe(
         "persist",
      );
      expect(kindFromPlan({ kind: "persist" }, plan)).toBe("persist");
   });
});

describe("the bind functions at their edges", () => {
   const entry = (sourceName: string, sourceEntityId: string): ManifestEntry =>
      ({
         sourceName,
         sourceEntityId,
         storageDestinationName: "lake",
         physicalTableName: `${sourceName}_t`,
         schema: [{ name: "x", type: "INTEGER" }],
      }) as ManifestEntry;
   /** Two models declaring `orders` with one SQL: one table, two sourceIDs. */
   const twoModels = {
      sources: {
         "orders@a": {
            name: "orders",
            sourceEntityId: "addr_orders",
            connectionName: "duckdb",
         },
         "orders@b": {
            name: "orders",
            sourceEntityId: "addr_orders",
            connectionName: "duckdb",
         },
      },
   } as unknown as BuildPlan;

   it("narrows a shared table's binding to its eligible sourceIDs", () => {
      const bound = bindStorageEntries({
         entries: { addr_orders: entry("orders", "addr_orders") },
         plan: twoModels,
         eligibility: {
            eligible: ["orders@a"],
            refused: { "orders@b": "gated" },
         },
         kindOf: (b) => b.kind,
      });
      expect(bound.bindings.map((b) => b.sourceIDs)).toEqual([["orders@a"]]);
      expect(bound.declined).toEqual([]);
   });

   it("keeps a binding whole when every sourceID is eligible", () => {
      const bound = bindStorageEntries({
         entries: { addr_orders: entry("orders", "addr_orders") },
         plan: twoModels,
         eligibility: { eligible: ["orders@a", "orders@b"], refused: {} },
         kindOf: (b) => b.kind,
      });
      expect(bound.bindings.map((b) => b.sourceIDs)).toEqual([
         ["orders@a", "orders@b"],
      ]);
   });

   it("says a source never examined is not in the plan, and marks a declined rollup", () => {
      const bound = bindStorageEntries({
         entries: { addr_orders: entry("orders", "addr_orders") },
         plan: twoModels,
         eligibility: { eligible: [], refused: {} },
         kindOf: () => "rollup",
      });
      expect(bound.bindings).toEqual([]);
      expect(bound.declined).toHaveLength(1);
      expect(bound.declined[0].reason).toBe("refused");
      expect(bound.declined[0].rollup).toBe(true);
      expect(bound.declined[0].detail).toContain(
         "not in the package build plan",
      );
   });

   it("with no plan, calls no entry another definition's and binds none", () => {
      const bound = bindStorageEntries({
         entries: { addr_orders: entry("orders", "addr_orders") },
         plan: null,
         eligibility: undefined,
         kindOf: (b) => b.kind,
      });
      expect(bound.otherDefinition).toEqual([]);
      expect(bound.bindings).toEqual([]);
      expect(bound.declined.map((d) => d.reason)).toEqual([
         "build_plan_unavailable",
      ]);
   });

   it("passes a database name already quoted, and says why an unrefused miss declines", () => {
      const bound = bindDatabaseEntries({
         entries: {
            a: { tableName: '"order"' },
            b: { tableName: "missed" },
         },
         eligibility: { eligibleEntityIds: new Set(["a"]), refused: {} },
         dialects: new Map([["a", "postgres"]]),
      });
      expect(bound.entries).toEqual({ a: { tableName: '"order"' } });
      expect(bound.declined).toHaveLength(1);
      expect(bound.declined[0].sourceEntityId).toBe("b");
      expect(bound.declined[0].detail).toContain(
         "not in the package build plan",
      );
   });

   it("reads a view's build source as a view before a #(materialize) table", () => {
      expect(
         kindFromPlan(
            { kind: "persist", sourceIDs: ["m@x", "v@x"] },
            {
               materializeSourceIDs: new Set(["m@x"]),
               viewBuildSourceIDs: new Set(["v@x"]),
            },
         ),
      ).toBe("view");
   });
});

describe("applyObservedColumns", () => {
   const built = [
      { name: "id", type: "BIGINT" },
      { name: "amt", type: "DOUBLE" },
      { name: "d", type: "DATE" },
   ];
   const binding: Binding = {
      kind: "materialize",
      tier: "database",
      sourceName: "orders",
      sourceIDs: [ID],
      destinationName: "pg",
      virtualHandle: "addr1",
      tablePath: "orders_t",
      schema: built,
   };
   const entries = {
      addr1: { tableName: "orders_t", schema: built },
      rollup1: {
         tableName: "orders_by_d",
         schema: [{ name: "total__partial", type: "DOUBLE" }],
      },
   };

   it("with no observation, returns everything as bound", () => {
      const out = applyObservedColumns({
         entries,
         bindings: [binding],
         observed: undefined,
      });
      expect(out.entries).toBe(entries);
      expect(out.bindings).toEqual([binding]);
      expect(out.drifted).toEqual([]);
   });

   it("keeps a table whose columns read as their built types, a wider number included", () => {
      const out = applyObservedColumns({
         entries,
         bindings: [binding],
         observed: new Map([
            [
               "addr1",
               {
                  columns: [
                     // A wider integer: a widening that loses nothing.
                     { name: "id", type: "HUGEINT" },
                     { name: "amt", type: "DOUBLE" },
                     { name: "d", type: "DATE" },
                  ],
               },
            ],
            ["rollup1", { columns: entries.rollup1.schema }],
         ]),
      });
      expect(out.bindings).toEqual([binding]);
      expect(out.entries).toEqual(entries);
      expect(out.drifted).toEqual([]);
   });

   it("declares a served-copy table's drifted column with its observed type, and leaves a gone one out", () => {
      const out = applyObservedColumns({
         entries,
         bindings: [binding],
         observed: new Map([
            [
               "addr1",
               {
                  columns: [
                     { name: "ID", type: "BIGINT" },
                     { name: "amt", type: "VARCHAR" },
                  ],
               },
            ],
            ["rollup1", { columns: entries.rollup1.schema }],
         ]),
      });
      expect(out.bindings[0].schema).toEqual([
         { name: "id", type: "BIGINT" },
         { name: "amt", type: "VARCHAR" },
      ]);
      // A served-copy table stays bound: its read plan casts or leaves out.
      expect(Object.keys(out.entries).sort()).toEqual(["addr1", "rollup1"]);
      expect(out.drifted).toEqual([
         {
            sourceEntityId: "addr1",
            columns: [
               { column: "amt", builtType: "DOUBLE", observedType: "VARCHAR" },
               // The bind's decision rides on the report.
               { column: "d", builtType: "DATE", gone: true, leftOut: true },
            ],
         },
      ]);
   });

   // P4-01: a DuckLake table's 128-bit integer holds a DOUBLE's digits
   // (the bind's read marks it `storedAsDouble`), whatever its build wrote.
   it("P4-01: reads a DuckLake 128-bit integer as the DOUBLE it holds, and leaves it out of a rollup that stores one", () => {
      const huge = [
         { name: "id", type: "HUGEINT" },
         { name: "amt", type: "DOUBLE" },
         { name: "d", type: "DATE" },
      ];
      const lake: Binding = {
         ...binding,
         schema: huge,
         nativeSchema: [{ name: "id", type: "HUGEINT" }],
      };
      const asDouble = {
         name: "id",
         type: "DOUBLE",
         nativeType: "DOUBLE",
         storedAsDouble: true as const,
      };
      const out = applyObservedColumns({
         entries: {
            addr1: { tableName: "orders_t", schema: huge },
            rollup1: {
               tableName: "orders_by_d",
               connectionName: "lake",
               schema: [{ name: "total__partial", type: "HUGEINT" }],
            },
         },
         bindings: [lake],
         observed: new Map([
            ["addr1", { columns: [asDouble, ...huge.slice(1)] }],
            ["rollup1", { columns: [{ ...asDouble, name: "total__partial" }] }],
         ]),
      });
      expect(out.bindings[0].schema[0]).toEqual({ name: "id", type: "DOUBLE" });
      expect(out.bindings[0].nativeSchema).toEqual([
         { name: "id", type: "DOUBLE" },
      ]);
      // The rollup keeps its table, its 128-bit partial left out: a query
      // reading it answers live, never the DOUBLE's rounded digits.
      expect(out.entries.rollup1).toEqual({
         tableName: "orders_by_d",
         connectionName: "lake",
         schema: [{ name: "total__partial", type: "HUGEINT" }],
         heldRead: { connectionName: "lake", columns: [] },
         leftOutColumns: ["total__partial"],
      });
      expect(out.drifted.map((d) => d.columns)).toEqual([
         [
            {
               column: "id",
               builtType: "HUGEINT",
               observedType: "DOUBLE",
               storedAsDouble: true,
            },
         ],
         [
            {
               column: "total__partial",
               builtType: "HUGEINT",
               observedType: "DOUBLE",
               storedAsDouble: true,
               leftOut: true,
            },
         ],
      ]);
   });

   it("keeps a substituted table (a rollup) whose column no cast reads, with that column left out", () => {
      const out = applyObservedColumns({
         entries,
         bindings: [binding],
         observed: new Map([
            ["addr1", { columns: built }],
            [
               "rollup1",
               { columns: [{ name: "total__partial", type: "VARCHAR" }] },
            ],
         ]),
         declared: () => ({ columns: [], connectionName: "pg" }),
      });
      // Held as it is now, for the read plan, which reads no text as a
      // number: the warning's column is left out.
      expect(out.entries).toEqual({
         addr1: entries.addr1,
         rollup1: {
            ...entries.rollup1,
            heldRead: {
               connectionName: "pg",
               columns: [
                  { name: "total__partial", type: "VARCHAR", moved: true },
               ],
            },
         },
      });
      expect(out.bindings).toEqual([binding]);
      expect(out.drifted[0].columns).toEqual([
         {
            column: "total__partial",
            builtType: "DOUBLE",
            observedType: "VARCHAR",
            leftOut: true,
         },
      ]);
   });

   describe("a substituted table (a rollup, a persisted view or query) whose columns moved", () => {
      const view = {
         tableName: '"s"."by_day"',
         schema: [
            { name: "flag", type: "BOOLEAN" },
            { name: "d2", type: "DATE" },
            { name: "n", type: "BIGINT" },
            { name: "amt", type: "DOUBLE" },
            { name: "gone", type: "VARCHAR" },
         ],
      };
      const observed = (dialect: string) =>
         new Map([
            [
               "v1",
               {
                  dialect,
                  columns: [
                     { name: "flag", type: "INTEGER" },
                     { name: "d2", type: "TIMESTAMP" },
                     { name: "n", type: "DECIMAL(22,2)" },
                     { name: "amt", type: "VARCHAR" },
                     { name: "other", type: "VARCHAR" },
                  ],
               },
            ],
         ]);

      it("records the columns it holds now for a typed read, leaving out those no read gives live's values", () => {
         const out = applyObservedColumns({
            entries: { v1: view },
            bindings: [],
            observed: observed("postgres"),
            declared: () => ({ columns: [], connectionName: "pg" }),
         });
         // A boolean now an integer, and a date now a timestamp, are held
         // for the read plan's faithful cast; text where a number was, and a
         // gone column, are left out.
         expect(out.entries.v1).toEqual({
            ...view,
            heldRead: {
               connectionName: "pg",
               columns: [
                  { name: "flag", type: "INTEGER", moved: true },
                  { name: "d2", type: "TIMESTAMP", moved: true },
                  { name: "n", type: "DECIMAL(22,2)" },
                  { name: "amt", type: "VARCHAR", moved: true },
                  { name: "other", type: "VARCHAR" },
               ],
            },
            leftOutColumns: ["gone"],
         });
         // `n` still a number: not drift. `amt` is held, but no cast reads
         // it as declared: the warning's and the read plan's left out.
         expect(
            out.drifted[0].columns.map((c) => [c.column, c.leftOut ?? false]),
         ).toEqual([
            ["flag", false],
            ["d2", false],
            ["amt", true],
            ["gone", true],
         ]);
      });

      it("answers live, whole, a table whose connection is not known (fail safe)", () => {
         const out = applyObservedColumns({
            entries: { v1: view },
            bindings: [],
            observed: observed("duckdb"),
         });
         expect(out.entries).toEqual({});
         expect(out.drifted).toHaveLength(1);
      });

      it("takes each column's declared type and reader's time zone from its build source", () => {
         const out = applyObservedColumns({
            entries: {
               v1: {
                  tableName: "t",
                  schema: [{ name: "d", type: "DATE" }],
               },
            },
            bindings: [],
            observed: new Map([
               [
                  "v1",
                  {
                     dialect: "duckdb",
                     columns: [{ name: "d", type: "TIMESTAMP WITH TIME ZONE" }],
                  },
               ],
            ]),
            // An instant's date in a query time zone is not UTC's: no cast.
            declared: () => ({
               columns: [{ name: "d", type: "date" }],
               connectionName: "duckdb",
               queryTimezone: "America/New_York",
            }),
         });
         expect(out.drifted[0].columns).toEqual([
            {
               column: "d",
               builtType: "DATE",
               observedType: "TIMESTAMP WITH TIME ZONE",
               leftOut: true,
            },
         ]);
      });

      // F-40: the table is as built, but its source now declares another
      // type (a warehouse column retyped upstream since the build).
      it("treats a column its source now declares as another type than the table holds as moved", () => {
         const run = (declaredType: string) =>
            applyObservedColumns({
               entries: {
                  v1: {
                     tableName: "t",
                     connectionName: "duckdb",
                     schema: [
                        { name: "cat", type: "VARCHAR" },
                        { name: "total", type: "BIGINT" },
                     ],
                  },
               },
               bindings: [],
               observed: new Map([
                  [
                     "v1",
                     {
                        dialect: "duckdb",
                        columns: [
                           { name: "cat", type: "VARCHAR" },
                           { name: "total", type: "BIGINT" },
                        ],
                     },
                  ],
               ]),
               declared: () => ({
                  columns: [
                     { name: "cat", type: "string" },
                     { name: "total", type: declaredType },
                  ],
                  connectionName: "duckdb",
               }),
            });
         const moved = run("string");
         expect(moved.drifted).toEqual([
            {
               sourceEntityId: "v1",
               columns: [
                  {
                     column: "total",
                     builtType: "BIGINT",
                     observedType: "BIGINT",
                     declaredMoved: true,
                     leftOut: true,
                  },
               ],
            },
         ]);
         // Read through the table's held types, the column marked moved, so
         // the leg checks it against live's type.
         expect(moved.entries.v1?.heldRead?.columns).toContainEqual({
            name: "total",
            type: "BIGINT",
            moved: true,
         });
         // Declared as built: nothing moved, the entry read as it is.
         const unchanged = run("number");
         expect(unchanged.drifted).toEqual([]);
         expect(unchanged.entries.v1?.heldRead).toBeUndefined();
      });

      it("reads a column the build writes as another type by design (a date truncation as a timestamp) through its cast, unreported", () => {
         const out = applyObservedColumns({
            entries: {
               v1: {
                  tableName: "t",
                  connectionName: "duckdb",
                  schema: [
                     { name: "m", type: "TIMESTAMP" },
                     { name: "flag", type: "INTEGER" },
                  ],
               },
            },
            bindings: [],
            observed: new Map([
               [
                  "v1",
                  {
                     dialect: "duckdb",
                     columns: [
                        { name: "m", type: "TIMESTAMP" },
                        { name: "flag", type: "INTEGER" },
                     ],
                  },
               ],
            ]),
            declared: () => ({
               columns: [
                  { name: "m", type: "date" },
                  { name: "flag", type: "boolean" },
               ],
               connectionName: "duckdb",
            }),
         });
         // Moved, so the leg reads each through its faithful cast (a
         // retype upstream into the same pair reads correctly), but marked
         // cast-only, so no warning or count reports it.
         expect(out.drifted).toEqual([
            {
               sourceEntityId: "v1",
               columns: [
                  {
                     column: "m",
                     builtType: "TIMESTAMP",
                     observedType: "TIMESTAMP",
                     declaredMoved: true,
                     castOnly: true,
                  },
                  {
                     column: "flag",
                     builtType: "INTEGER",
                     observedType: "INTEGER",
                     declaredMoved: true,
                     castOnly: true,
                  },
               ],
            },
         ]);
         expect(
            out.entries.v1?.heldRead?.columns.filter((c) => c.moved),
         ).toHaveLength(2);
         // Declared in the plan as the source declares them now: no warning.
         const plan = {
            sources: {
               "v@file:///m.malloy": {
                  name: "v",
                  sourceEntityId: "v1",
                  columns: [
                     { name: "m", type: "date" },
                     { name: "flag", type: "boolean" },
                  ],
               },
            },
         } as unknown as BuildPlan;
         expect(databaseDriftFindings(out.drifted, plan, {})).toEqual([]);
      });

      // C1/H1: a move inside one Malloy type that may round a value is left
      // out; only a widening that loses nothing is read as it is.
      for (const [built, now, kept] of [
         ["INTEGER", "BIGINT", true],
         ["DECIMAL(10,1)", "DECIMAL(12,2)", true],
         ["FLOAT", "DOUBLE", true],
         ["INTEGER", "DECIMAL(10,0)", true],
         ["DECIMAL(10,1)", "DECIMAL(10,0)", false],
         ["DECIMAL(20,0)", "DOUBLE", false],
         ["BIGINT", "INTEGER", false],
         ["DOUBLE", "FLOAT", false],
         ["BIGINT", "DECIMAL(18,0)", false],
      ] as const) {
         it(`reads ${built} now ${now} ${kept ? "as it is" : "left out"}`, () => {
            const out = applyObservedColumns({
               entries: {
                  v1: {
                     tableName: "t",
                     connectionName: "duckdb",
                     schema: [{ name: "x", type: built }],
                     nativeSchema: [{ name: "x", type: built }],
                  },
               },
               bindings: [],
               observed: new Map([
                  [
                     "v1",
                     {
                        dialect: "duckdb",
                        columns: [{ name: "x", type: now, nativeType: now }],
                     },
                  ],
               ]),
            });
            expect(out.entries.v1?.leftOutColumns ?? []).toEqual(
               kept ? [] : ["x"],
            );
            expect(
               out.entries.v1?.heldRead?.columns.map((c) => c.name) ?? [],
            ).toEqual(kept ? ["x"] : []);
         });
      }

      // Fixed-length text pads with blanks: a move to or from it, or to
      // another fixed length, is never read as it is ('ab' against 'ab ').
      for (const [built, now, kept] of [
         ["character(2)", "character varying(2)", false],
         ["character varying(2)", "character(2)", false],
         ["bpchar", "varchar", false],
         ["character(2)", "character(3)", false],
         ["character varying(2)", "character varying(3)", true],
         ["character varying(3)", "character varying(2)", false],
      ] as const) {
         it(`reads ${built} now ${now} ${kept ? "as it is" : "left out"} (Postgres)`, () => {
            const out = applyObservedColumns({
               entries: {
                  v1: {
                     tableName: "t",
                     connectionName: "pg",
                     schema: [{ name: "x", type: "VARCHAR" }],
                     nativeSchema: [{ name: "x", type: built }],
                  },
               },
               bindings: [],
               observed: new Map([
                  [
                     "v1",
                     {
                        dialect: "postgres",
                        columns: [
                           { name: "x", type: "VARCHAR", nativeType: now },
                        ],
                     },
                  ],
               ]),
            });
            expect(out.entries.v1?.leftOutColumns ?? []).toEqual(
               kept ? [] : ["x"],
            );
         });
      }

      it("keeps the bare table when every moved column still reads as declared", () => {
         const out = applyObservedColumns({
            entries: {
               v1: { tableName: "t", schema: [{ name: "n", type: "INTEGER" }] },
            },
            bindings: [],
            observed: new Map([
               [
                  "v1",
                  {
                     dialect: "duckdb",
                     columns: [{ name: "n", type: "BIGINT" }],
                  },
               ],
            ]),
         });
         expect(out.entries.v1).toEqual({
            tableName: "t",
            schema: [{ name: "n", type: "INTEGER" }],
         });
         expect(out.drifted).toEqual([]);
      });
   });

   it("leaves out a table that was not read, binding and entry alike (fail safe)", () => {
      const out = applyObservedColumns({
         entries,
         bindings: [binding],
         observed: new Map([
            [
               "addr1",
               {
                  unread: "connection_failed" as const,
                  detail: "connection refused",
               },
            ],
         ]),
      });
      expect(out.entries).toEqual({});
      expect(out.bindings).toEqual([]);
      expect(out.unread).toEqual([
         {
            sourceEntityId: "addr1",
            reason: "connection_failed",
            detail: "connection refused",
         },
         { sourceEntityId: "rollup1", reason: "not_read" },
      ]);
   });

   // A big integer altered to a decimal is read with its new type only when
   // the decimal holds all its digits; a narrower one may have rounded it.
   for (const [native, served] of [
      ["numeric(22,2)", true],
      ["numeric(10,2)", false],
   ] as const) {
      it(`a big integer altered to ${native}: ${served ? "retyped, and cast to the type it holds now" : "left out"}`, () => {
         const big: Binding = {
            ...binding,
            schema: [{ name: "n", type: "BIGINT" }],
            nativeSchema: [{ name: "n", type: "bigint" }],
         };
         const out = applyObservedColumns({
            entries: {
               addr1: {
                  tableName: "orders_t",
                  schema: [{ name: "n", type: "BIGINT" }],
                  nativeSchema: [{ name: "n", type: "bigint" }],
               },
            },
            bindings: [big],
            observed: new Map([
               [
                  "addr1",
                  {
                     dialect: "postgres",
                     columns: [
                        { name: "n", type: "DOUBLE", nativeType: native },
                     ],
                  },
               ],
            ]),
         });
         if (served) {
            // Never cast to the bigint it was built as: that truncates a
            // fraction.
            expect(out.bindings[0].schema).toEqual([
               { name: "n", type: "DOUBLE" },
            ]);
            expect(out.bindings[0].nativeSchema).toEqual([
               { name: "n", type: native },
            ]);
         } else {
            expect(out.bindings[0].schema).toEqual([]);
         }
         expect(out.drifted.map((d) => d.columns.map((c) => c.column))).toEqual(
            [["n"]],
         );
         expect(out.drifted[0].columns[0].leftOut).toBe(
            served ? undefined : true,
         );
      });
   }

   it("leaves out a timestamp whose zone changed, unless the session is DuckDB's UTC", () => {
      const ts: Binding = {
         ...binding,
         schema: [{ name: "at", type: "TIMESTAMP" }],
      };
      const read = (dialect: string) =>
         applyObservedColumns({
            entries: {
               addr1: {
                  tableName: "orders_t",
                  schema: [{ name: "at", type: "TIMESTAMP" }],
               },
            },
            bindings: [ts],
            observed: new Map([
               [
                  "addr1",
                  {
                     dialect,
                     columns: [
                        {
                           name: "at",
                           type: "TIMESTAMP WITH TIME ZONE",
                           nativeType: "timestamp with time zone",
                        },
                     ],
                  },
               ],
            ]),
         });
      expect(read("postgres").bindings[0].schema).toEqual([]);
      expect(read("duckdb").bindings[0].schema).toEqual(ts.schema);
   });

   it("leaves a rollup's big integer now held as a float out of its reads", () => {
      const out = applyObservedColumns({
         entries: {
            rollup1: {
               tableName: "orders_by_d",
               connectionName: "pg",
               schema: [{ name: "total__partial", type: "BIGINT" }],
               nativeSchema: [{ name: "total__partial", type: "bigint" }],
            },
         },
         bindings: [],
         observed: new Map([
            [
               "rollup1",
               {
                  dialect: "postgres",
                  columns: [
                     {
                        name: "total__partial",
                        type: "DOUBLE",
                        nativeType: "double precision",
                     },
                  ],
               },
            ],
         ]),
      });
      // Its digits past 2^53 are lost: left out, never read as exact.
      expect(out.entries.rollup1?.leftOutColumns).toEqual(["total__partial"]);
   });

   // A build whose schema read failed (an unqualified Postgres table) records
   // only native types, so the binding's declared `date` stands in for what
   // it wrote: a date truncation the warehouse returns as a timestamp.
   for (const { dialect, nativeType, observedType } of [
      {
         dialect: "postgres",
         nativeType: "timestamp with time zone",
         observedType: "TIMESTAMP WITH TIME ZONE",
      },
      {
         dialect: "postgres",
         nativeType: "timestamp without time zone",
         observedType: "TIMESTAMP",
      },
      { dialect: "duckdb", nativeType: "TIMESTAMP", observedType: "TIMESTAMP" },
   ]) {
      const truncation = (now: string) =>
         applyObservedColumns({
            entries: {
               addr1: {
                  tableName: "orders_t",
                  nativeSchema: [{ name: "d_month", type: nativeType }],
               },
            },
            bindings: [
               {
                  ...binding,
                  schema: [{ name: "d_month", type: "DATE" }],
                  nativeSchema: [{ name: "d_month", type: nativeType }],
               },
            ],
            observed: new Map([
               [
                  "addr1",
                  {
                     dialect,
                     columns: [
                        {
                           name: "d_month",
                           type: now === nativeType ? observedType : "VARCHAR",
                           nativeType: now,
                        },
                     ],
                  },
               ],
            ]),
         });

      it(`a date truncation stored as its build wrote it (${dialect} ${nativeType}) is read as it holds, and is not drift`, () => {
         const out = truncation(nativeType);
         expect(out.drifted).toEqual([]);
         expect(Object.keys(out.entries)).toEqual(["addr1"]);
         // Still read with the type it holds, so the read plan casts it.
         expect(out.bindings[0].schema).toEqual([
            { name: "d_month", type: observedType },
         ]);
      });

      it(`a date truncation altered after its build (${dialect} ${nativeType} to text) is still drift`, () => {
         const out = truncation(dialect === "postgres" ? "text" : "VARCHAR");
         expect(out.bindings[0].schema).toEqual([
            { name: "d_month", type: "VARCHAR" },
         ]);
         // Reported against the native type its build captured.
         expect(out.drifted.map((d) => d.columns.map((c) => c.column))).toEqual(
            [["d_month"]],
         );
      });
   }

   it("with no native type captured, a column that reads as another type than declared is still drift", () => {
      const out = applyObservedColumns({
         entries: { addr1: { tableName: "orders_t" } },
         bindings: [{ ...binding, schema: [{ name: "d", type: "DATE" }] }],
         observed: new Map([
            [
               "addr1",
               {
                  dialect: "postgres",
                  columns: [
                     { name: "d", type: "TIMESTAMP", nativeType: "timestamp" },
                  ],
               },
            ],
         ]),
      });
      expect(out.drifted).toEqual([
         {
            sourceEntityId: "addr1",
            columns: [
               { column: "d", builtType: "DATE", observedType: "TIMESTAMP" },
            ],
         },
      ]);
   });

   it("passes a storage binding through untouched", () => {
      const storage: Binding = {
         ...binding,
         tier: "storage",
         virtualHandle: "s1",
      };
      const out = applyObservedColumns({
         entries: {},
         bindings: [storage],
         observed: new Map(),
      });
      expect(out.bindings).toEqual([storage]);
   });
});

/** One database served-copy binding over `x`, with what its build captured. */
function bindSpelled(args: {
   declared: string;
   builtNative?: string;
   builtSchema?: string;
   now: string;
   nowNative: string;
}) {
   const entry = {
      tableName: "t",
      connectionName: "c",
      ...(args.builtSchema
         ? { schema: [{ name: "x", type: args.builtSchema }] }
         : {}),
      ...(args.builtNative
         ? { nativeSchema: [{ name: "x", type: args.builtNative }] }
         : {}),
   };
   const binding = {
      kind: "materialize",
      tier: "database",
      sourceName: "s",
      destinationName: "c",
      virtualHandle: "e",
      tablePath: "t",
      schema: [{ name: "x", type: args.declared }],
   };
   const out = applyObservedColumns({
      entries: { e: entry } as never,
      bindings: [binding] as never,
      observed: new Map([
         [
            "e",
            {
               dialect: "postgres",
               columns: [
                  { name: "x", type: args.now, nativeType: args.nowNative },
               ],
            },
         ],
      ]) as never,
   });
   return {
      column: out.bindings[0]?.schema.find((c) => c.name === "x"),
      drifted: out.drifted.length > 0,
   };
}

// The bind compares spelled types for a narrowing only when the build
// captured no native type: a binding's declared type may stand in for a
// captured one, and a column whose native type is as built has not moved.
describe("the spelled-type narrowing check runs only with no native capture", () => {
   it("a column natively as built is read, not left out, when its declared type stands in", () => {
      const r = bindSpelled({
         declared: "BIGINT",
         builtNative: "integer",
         now: "INTEGER",
         nowNative: "integer",
      });
      expect(r.column).toBeDefined();
      expect(r.drifted).toBe(false);
   });
   it("with no native capture, a narrowing seen only in the spelled types is left out", () => {
      expect(
         bindSpelled({
            declared: "DECIMAL(10,1)",
            builtSchema: "DECIMAL(10,1)",
            now: "DECIMAL(10,0)",
            nowNative: "numeric(10,0)",
         }).column,
      ).toBeUndefined();
      // A gate column: two tenants either side of 2^53 would read alike.
      expect(
         bindSpelled({
            declared: "BIGINT",
            builtSchema: "BIGINT",
            now: "DOUBLE",
            nowNative: "double precision",
         }).column,
      ).toBeUndefined();
   });
});
