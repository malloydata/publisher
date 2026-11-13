// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { MaterializePlan } from "../plan/build_file_generator";
import { storedColumnsOfOutput } from "../stored_column_types";
import { deriveColocatedServeBindings } from "./colocated_bindings";
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
