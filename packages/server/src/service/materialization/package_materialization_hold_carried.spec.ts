// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A standalone run holds each table it may carry to the column contract its
 * build recorded, as the table is now (`holdCarriedToContracts`): one
 * altered since the bind last read it (an out-of-band `ALTER … TYPE
 * DECIMAL(10,0)`) goes to the drift handler, so `incompatibleTables` names
 * it and the run rebuilds it in full rather than carry it. Before, the run
 * consulted only what the bind had seen, and a package loaded again after
 * the alter, which had bound nothing, carried the altered table.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import { contractFor } from "../../test_helpers/contract_fixture";
import type { ManifestEntry } from "./domain/types";
import { PackageMaterialization } from "./package_materialization";

let warehouse: DuckDBConnection;

beforeAll(async () => {
   warehouse = new DuckDBConnection("warehouse", ":memory:");
   await warehouse.runSQL(
      "CREATE TABLE altered AS SELECT 'a' AS cat, CAST(2.4 AS DECIMAL(10,1)) AS amt",
   );
   await warehouse.runSQL("ALTER TABLE altered ALTER amt TYPE DECIMAL(10,0)");
   await warehouse.runSQL(
      "CREATE TABLE intact AS SELECT 'a' AS cat, CAST(2.4 AS DECIMAL(10,1)) AS amt",
   );
});

afterAll(async () => {
   await warehouse?.close();
});

async function facade(): Promise<PackageMaterialization> {
   const m = new PackageMaterialization({
      packageName: () => "pkg",
      versionId: () => undefined,
      metadata: () => ({}) as never,
      models: () => new Map(),
      modelPolicy: () => ({ setServingResolvers: () => {} }) as never,
      packagePath: () => "/pkg",
      malloyConfig: () => ({}) as never,
      connection: async () => warehouse as never,
      buildPlanPackage: {} as never,
      publishedSurface: () => undefined,
   });
   await m.applyLoadBuildPlan(okWorkerPlan(makePlanOutcome({})));
   return m;
}

/** A database entry as its build recorded it: `amt` built as DECIMAL(10,1). */
const built = (table: string): ManifestEntry =>
   ({
      sourceEntityId: table,
      physicalTableName: table,
      connectionName: "warehouse",
      builtAt: "2026-11-01T00:00:00.000Z",
      contract: contractFor([
         { name: "cat", type: "VARCHAR" },
         { name: "amt", type: "DECIMAL(10,1)" },
      ]),
   }) as ManifestEntry;

describe("a standalone run holds the tables it would carry to their contracts", () => {
   it("names a table retyped since its build, so the run rebuilds it rather than carry it", async () => {
      const m = await facade();
      const prior = { altered: built("altered"), intact: built("intact") };
      // Nothing bound: the bind never saw the alter.
      expect([...m.incompatibleTables(prior)]).toEqual([]);
      const found = await m.holdCarriedToContracts(prior);
      expect([...found]).toEqual(["altered"]);
      expect([...m.incompatibleTables(prior)]).toEqual(["altered"]);
      m.dispose();
   });

   it("leaves a table it cannot read, or one with no contract, to the bind", async () => {
      const m = await facade();
      const gone = built("no_such_table");
      const uncontracted = { ...built("altered"), contract: undefined };
      const found = await m.holdCarriedToContracts({
         no_such_table: gone,
         altered: uncontracted as ManifestEntry,
      });
      expect([...found]).toEqual([]);
      m.dispose();
   });
});
