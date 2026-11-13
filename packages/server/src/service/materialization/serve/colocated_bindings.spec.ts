// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../plan/build_file_generator";
import { storedColumnsOfOutput } from "../stored_column_types";
import {
   deriveColocatedServeBindings,
   v1StoredColumnsOf,
} from "./colocated_bindings";
import {
   boundTablePath,
   buildVirtualMap,
} from "./materialization_serve_transform";

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
            sourceName: "orders",
            sourceIDs: [ID],
            origin: "persist",
            destinationName: "pg",
            colocated: true,
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
      const storage = {
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
            sourceName: "daily",
            sourceIDs: [V1],
            origin: "persist",
            destinationName: "duckdb",
            colocated: true,
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
