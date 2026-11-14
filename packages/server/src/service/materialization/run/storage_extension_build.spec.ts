// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { ChainedUpstreamMissingError } from "../../../errors";
import { buildEnvironmentStub } from "../../../test_helpers/materialization_service_harness";
import type { MaterializePlan } from "../plan/build_file_generator";
import type { ManifestEntry } from "../domain/types";
import {
   NO_PLAN_FACTS,
   presentedSourceName,
   type ChainedPlanFacts,
} from "./materialization_run_support";
import { StorageBuildStrategy } from "./storage_build_strategy";

/**
 * A `storage=` extension table (P10) builds over its parent's stored table
 * in the same destination, from the chain its plan carries. These pin what it
 * does when that table is not there to read: it names the author's parent,
 * never the generated build source, and says where the table is instead, so
 * the ladder above it recomputes, or refuses under `strictUpstreams`.
 */

const PARENT_ID = "orders@file:///pkg/m.malloy";
const EXT_ID = "east@file:///pkg/m.malloy";

const plan: MaterializePlan = {
   sourceName: "east",
   sourceID: EXT_ID,
   buildSourceName: "east__mat",
   columns: [{ dimension: "id", column: "id", computed: false }],
   persist: { storage: "lake" },
   buildsOver: {
      sourceID: PARENT_ID,
      sourceName: "orders",
      declaration: {
         url: "file:///pkg/m.malloy",
         range: {
            start: { line: 0, character: 0 },
            end: { line: 0, character: 1 },
         },
      },
      path: [],
      overStored: {
         parentBuildSourceName: "orders__mat",
         flags: [],
         declarations: "source: orders is orders__mat",
      },
   },
};

function facts(over: Partial<ChainedPlanFacts> = {}): ChainedPlanFacts {
   return {
      ...NO_PLAN_FACTS,
      addressBySourceId: { [PARENT_ID]: "a-parent", [EXT_ID]: "a-ext" },
      materializePlans: { [EXT_ID]: plan },
      ...over,
   };
}

async function attempt(
   builtEntries: Record<string, ManifestEntry>,
   planFacts: ChainedPlanFacts = facts(),
): Promise<ChainedUpstreamMissingError> {
   const strategy = new StorageBuildStrategy({
      buildDownstreamViaParents: () => {
         throw new Error("an extension table never lifts author text");
      },
   });
   const environment = buildEnvironmentStub({
      destinations: { lake: { name: "lake", type: "duckdb" } },
   });
   try {
      await strategy.buildExtensionOverStoredParent(
         { name: "east", sourceID: EXT_ID } as unknown as PersistSource,
         {
            plan,
            parent: plan.buildsOver!,
            chain: plan.buildsOver!.overStored!,
         },
         "lake",
         { name: "lake", type: "duckdb" },
         builtEntries,
         environment,
         "east",
         planFacts,
      );
   } catch (err) {
      expect(err).toBeInstanceOf(ChainedUpstreamMissingError);
      return err as ChainedUpstreamMissingError;
   }
   throw new Error("expected the build to be refused");
}

function entry(over: Partial<ManifestEntry>): ManifestEntry {
   return {
      sourceEntityId: "a-parent",
      sourceName: "orders",
      physicalTableName: "orders",
      connectionName: "duckdb",
      realization: "COPY",
      rowCount: null,
      schema: [{ name: "id", type: "INTEGER" }],
      ...over,
   } as ManifestEntry;
}

describe("a storage= extension table without its parent's stored table", () => {
   it("names the author's parent when the build has no table of it", async () => {
      const err = await attempt({});
      expect(err.missing).toEqual(["orders"]);
      expect(err.message).toContain(
         "'orders' is not materialized in destination 'lake'",
      );
      expect(err.message).not.toContain("__mat");
   });

   it("says the parent is in another destination", async () => {
      const err = await attempt({
         "a-parent": entry({ storageDestinationName: "other" }),
      });
      expect(err.message).toContain(
         "'orders' is materialized in destination 'other', not 'lake'",
      );
   });

   it("says the parent's table is a warehouse table", async () => {
      const err = await attempt({ "a-parent": entry({}) });
      expect(err.message).toContain("materialized outside destination 'lake'");
   });

   it("refuses a parent supplied without a column list", async () => {
      const err = await attempt({
         "a-parent": entry({ storageDestinationName: "lake", schema: [] }),
      });
      expect(err.message).toContain("carries no column list");
   });
});

describe("presentedSourceName", () => {
   it("gives a generated name the author's source it stands for, and any other its own", () => {
      const planFacts = facts();
      expect(presentedSourceName("east__mat", planFacts)).toBe("east");
      // The alias an extension's build reads its base by is on its parent's
      // table.
      expect(presentedSourceName("east__mat__base", planFacts)).toBe("orders");
      expect(presentedSourceName("daily", planFacts)).toBe("daily");
   });
});
