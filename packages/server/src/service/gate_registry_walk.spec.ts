// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// gateExprsForRoute against the real compiler: which gate groups are in force
// on a source, own versus inherited, and the query-source exception where
// derivation ADDS the base's gate instead of replacing it.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { gateExprsForRoute } from "./gate_registry_walk";

const ROOT = "file:///gate-walk/";
const MODEL = `##! experimental.givens
given: ROLE :: string
given: GROUPS :: number[]

#(authorize) 'admin' = $ROLE
source: base is duckdb.sql("SELECT 1 AS org_id")

source: inherits is base extend { }

#(authorize) 'analyst' = $ROLE
source: replaces is base extend { }

#(authorize) 'analyst' = $ROLE
source: derived is base -> { group_by: org_id }

#(access_filter) org_id in $GROUPS
source: scoped is duckdb.sql("SELECT 1 AS org_id")

source: open is duckdb.sql("SELECT 1 AS org_id")
`;

let modelDef: ModelDef;

beforeAll(async () => {
   const connections = new FixedConnectionMap(
      new Map([["duckdb", new DuckDBConnection("duckdb", ":memory:")]]),
      "duckdb",
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[`${ROOT}m.malloy`, MODEL]])),
      connections,
   });
   const model = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   modelDef = model._modelDef;
});

const gates = (name: string, route = AUTHORIZE_ROUTE) =>
   gateExprsForRoute(modelDef.contents[name] as SourceDef, modelDef, route).map(
      (group) => group.map((expr) => expr.replace(/\s+/g, " ").trim()),
   );

describe("gateExprsForRoute", () => {
   it("returns a source's own gate", () => {
      expect(gates("base")).toEqual([["'admin' = $ROLE"]]);
   });

   it("inherits the base's gate when an extension declares none", () => {
      expect(gates("inherits")).toEqual([["'admin' = $ROLE"]]);
   });

   it("lets an extension's own gate replace the inherited one", () => {
      expect(gates("replaces")).toEqual([["'analyst' = $ROLE"]]);
   });

   it("keeps the base's gate beside a query source's own, as separate groups", () => {
      expect(gates("derived")).toEqual([
         ["'analyst' = $ROLE"],
         ["'admin' = $ROLE"],
      ]);
   });

   it("reads only the route asked for", () => {
      expect(gates("scoped", ACCESS_FILTER_ROUTE)).toEqual([
         ["org_id in $GROUPS"],
      ]);
      expect(gates("scoped", AUTHORIZE_ROUTE)).toEqual([]);
      expect(gates("base", ACCESS_FILTER_ROUTE)).toEqual([]);
   });

   it("finds no gate on an ungated source", () => {
      expect(gates("open")).toEqual([]);
   });
});
