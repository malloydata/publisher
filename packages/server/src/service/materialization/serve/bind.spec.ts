// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../domain/table_plans";
import { storedColumnsOfOutput } from "../stored_column_types";
import {
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
         b: { tableName: "kept" },
      });
      expect(bound.declined).toEqual([
         {
            reason: "refused",
            sourceEntityId: "c",
            detail: "a gate the table cannot keep",
         },
      ]);
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
