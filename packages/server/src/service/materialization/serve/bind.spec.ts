// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../domain/table_plans";
import { storedColumnsOfOutput } from "../stored_column_types";
import {
   applyObservedColumns,
   bindColocatedEntries,
   bindStorageEntries,
   deriveColocatedServeBindings,
   kindFromPlan,
   v1StoredColumnsOf,
} from "./bind";
import type { BuildPlan, ManifestEntry } from "../domain/types";
import { boundTablePath, buildVirtualMap } from "./bindings";
import type { Binding } from "../domain/binding";

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
   return deriveColocatedServeBindings({
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

describe("deriveColocatedServeBindings", () => {
   it("binds a colocated #(materialize) table on its own connection, keyed by its content address", () => {
      expect(derive()).toEqual([
         {
            kind: "materialize",
            tier: "colocated",
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

   it("binds nothing when the build's columns are unknown, so the source serves live", () => {
      expect(derive({ plan: { storedColumns: undefined } })).toEqual([]);
      expect(derive({ plan: { storedColumns: [] } })).toEqual([]);
   });

   it("binds nothing for a v1 #@ persist source (no #(materialize) plan) or a manifest without its address", () => {
      expect(
         deriveColocatedServeBindings({
            entries: { addr1: { tableName: "t" } },
            planSources: { [ID]: SOURCE },
            materializePlans: {},
            eligible: () => true,
         }),
      ).toEqual([]);
      expect(
         deriveColocatedServeBindings({
            entries: { other: { tableName: "t" } },
            planSources: { [ID]: SOURCE },
            materializePlans: { [ID]: plan() },
            eligible: () => true,
         }),
      ).toEqual([]);
      expect(
         deriveColocatedServeBindings({
            entries: undefined,
            planSources: { [ID]: SOURCE },
            materializePlans: { [ID]: plan() },
            eligible: () => true,
         }),
      ).toEqual([]);
   });

   it("puts a colocated table's path in the virtual map as bound, a storage table's quoted for DuckDB", () => {
      const colocated = {
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
      expect(boundTablePath(colocated)).toBe("proj.ds.orders_all");
      expect(boundTablePath(storage)).toBe('"main"."s"');
      expect(buildVirtualMap([colocated, storage])).toEqual(
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

describe("deriveColocatedServeBindings: v1 colocated #@ persist (the v1 cutover, G1)", () => {
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
      deriveColocatedServeBindings({
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
            tier: "colocated",
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

   it("marks a source's own table kept past its own gate only as plan decided (G3), and never a rollup's or a view's", () => {
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
      for (const kind of ["persist", "materialize"] as const) {
         expect(kept(kind)).toEqual([
            ["orders", false],
            ["gated", true],
         ]);
      }
      for (const kind of ["rollup", "view"] as const) {
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
});

describe("bindColocatedEntries", () => {
   it("keeps the eligible entries, quoted for their dialect, and declines the rest", () => {
      const bound = bindColocatedEntries({
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
      const bound = bindColocatedEntries({
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
      const bound = bindColocatedEntries({
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

   it("passes a colocated name already quoted, and says why an unrefused miss declines", () => {
      const bound = bindColocatedEntries({
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
      tier: "colocated",
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
                     { name: "id", type: "BIGINT" },
                     // A number of another width: the same Malloy type.
                     { name: "amt", type: "BIGINT" },
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
               { column: "d", builtType: "DATE" },
            ],
         },
      ]);
   });

   // P4-01: a DuckLake table's 128-bit integer holds a DOUBLE's digits
   // (the bind's read marks it `storedAsDouble`), whatever its build wrote.
   it("P4-01: reads a DuckLake 128-bit integer as the DOUBLE it holds, and leaves out a rollup that stores one", () => {
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
      expect(Object.keys(out.entries)).toEqual(["addr1"]);
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
            },
         ],
      ]);
   });

   it("leaves out a substituted table (a rollup) whose column drifted: no cast reaches it", () => {
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
      });
      expect(Object.keys(out.entries)).toEqual(["addr1"]);
      expect(out.bindings).toEqual([binding]);
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

   it("re-reads a big integer altered to a decimal: retyped, and cast to the type it holds now", () => {
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
                     { name: "n", type: "DOUBLE", nativeType: "numeric(10,2)" },
                  ],
               },
            ],
         ]),
      });
      // Never cast to the bigint it was built as: that truncates a fraction.
      expect(out.bindings[0].schema).toEqual([{ name: "n", type: "DOUBLE" }]);
      expect(out.bindings[0].nativeSchema).toEqual([
         { name: "n", type: "numeric(10,2)" },
      ]);
      expect(out.drifted.map((d) => d.columns.map((c) => c.column))).toEqual([
         ["n"],
      ]);
   });

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

   it("leaves out a rollup whose big integer's native type moved", () => {
      const out = applyObservedColumns({
         entries: {
            rollup1: {
               tableName: "orders_by_d",
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
      expect(out.entries).toEqual({});
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
