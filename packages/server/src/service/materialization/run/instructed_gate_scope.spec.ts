// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A host-instructed run judges only what the host instructed, and a refusal
// fails only its own source. `gated_entry` (`open_storage extend {}` with its
// own `#(access_filter)`) shares `open_storage`'s content address. A host that
// instructs `open_storage` by sourceID once handed the extension the same
// instruction through the address fallback; a storage re-assert without the
// plan's own-gate rule then refused it, outside the per-source refusal path,
// and failed the whole run after every instructed build had succeeded.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import { type BuildEnvironment } from "./build_upstreams";
import { InstructedBuild } from "./instructed_build";
import {
   indexInstructions,
   resolveInstruction,
} from "./materialization_instructed_run";
import { compileRunPlan, type RunPlan } from "./run_plan";

// A host's model with gated sources, its BigQuery tables swapped for DuckDB.
const MODEL = `##! experimental { givens persistence }
source: trips is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'Subscriber', 10, TIMESTAMP '2017-01-01 08:00:00'),
    (2, 'Customer', 11, TIMESTAMP '2017-01-02 09:00:00')
  ) AS t(trip_id, subscriber_type, start_station_id, start_date)
""") extend {
  dimension: start_day is start_date.day
  measure: trip_count is count()
}
source: stations is duckdb.sql("""
  SELECT * FROM (VALUES (10, 1), (11, 2)) AS t(station_key, region_id)
""")
given:
  SUB :: string
  REGION :: number

#(access_filter) subscriber_type = $SUB
#@ persist name="gated_persist"
source: gated_persist is trips -> { group_by: subscriber_type, start_station_id; aggregate: trip_count }

#(access_filter) subscriber_type = $SUB
#(materialize) name="gated_mat"
source: gated_mat is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'Subscriber', TIMESTAMP '2017-01-01 08:00:00'),
    (2, 'Customer', TIMESTAMP '2018-01-02 09:00:00')
  ) AS t(trip_id, subscriber_type, start_date)
""") extend {
  where: start_date ? @2017
  measure: trip_count is count()
}

#(access_filter) subscriber_type = $SUB
source: gated_base is trips extend {}
#@ persist name="inherits"
source: inherits is gated_base -> { group_by: subscriber_type; aggregate: trip_count }

#(access_filter) region_id = $REGION
source: gated_stations is stations extend {}
#@ persist name="via_join"
source: via_join is trips extend {
  join_one: st is gated_stations on start_station_id = st.station_key
} -> { group_by: st.region_id; aggregate: trip_count }

#(access_filter) subscriber_type = $SUB
#@ persist storage="lake"
source: gated_storage is trips -> { group_by: subscriber_type, start_station_id; aggregate: trip_count }

#@ persist storage="lake"
source: open_storage is trips -> { group_by: subscriber_type, start_day; aggregate: trip_count }
#(access_filter) subscriber_type = $SUB
source: gated_entry is open_storage extend {}
`;

const packages = tempBuildPlanPackages();
let restore: () => void = () => {};
beforeEach(() => {
   restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } }).restore;
});
afterEach(async () => {
   restore();
   await packages.cleanup();
});

const environment = {
   getApiConnection: () => {
      throw new Error("unused");
   },
   getEnvironmentName: () => "env",
   getEnvironmentPath: () => "/tmp/env",
} as unknown as BuildEnvironment;

async function plan(): Promise<{
   runPlan: RunPlan;
   idOf: (name: string) => string;
   addressOf: (name: string) => string;
}> {
   const { runPlan, packagePlan } = await compileRunPlan(
      await packages.create(MODEL),
   );
   const idOf = (name: string) => {
      const id = Object.keys(runPlan.tables).find(
         (sourceID) => runPlan.tables[sourceID].source.name === name,
      );
      if (!id) throw new Error(`no planned source '${name}'`);
      return id;
   };
   const addressOf = (name: string) => {
      const address = packagePlan.addresses[idOf(name)];
      if (typeof address !== "string") {
         throw new Error(`'${name}' has no address`);
      }
      return address;
   };
   return { runPlan, idOf, addressOf };
}

/** A host's instruction for one source, keyed by its sourceID. */
function instruct(
   p: Awaited<ReturnType<typeof plan>>,
   name: string,
   destination?: string,
): BuildInstruction {
   return {
      sourceID: p.idOf(name),
      sourceEntityId: p.addressOf(name),
      materializedTableId: `mt-${name}`,
      physicalTableName: `host_${name}`,
      realization: "COPY",
      ...(destination ? { destination } : {}),
   };
}

/** Run `instructions` as a host would, with builds that always succeed. */
async function runHost(
   p: Awaited<ReturnType<typeof plan>>,
   instructions: BuildInstruction[],
) {
   const built: string[] = [];
   const build = new InstructedBuild({
      buildOneSource: (async ({
         persistSource,
         instruction,
         sourceEntityId,
      }: {
         persistSource: { name: string };
         instruction: BuildInstruction;
         sourceEntityId: string;
      }): Promise<ManifestEntry> => {
         built.push(persistSource.name);
         return {
            sourceEntityId,
            sourceName: persistSource.name,
            physicalTableName: instruction.physicalTableName,
            materializedTableId: instruction.materializedTableId,
            connectionName: "duckdb",
            realization: "COPY",
            ...(instruction.destination
               ? { storageDestinationName: instruction.destination }
               : {}),
         } as ManifestEntry;
      }) as never,
      tableNotOwned: async () => undefined,
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const result = await build.run({
      compiled: p.runPlan,
      environment,
      instructions,
      seedEntries: {},
      signal: new AbortController().signal,
      hostInstructed: true,
   });
   return { built, result };
}

describe("a host-instructed run with an uninstructed gated extension", () => {
   it("shares one address between open_storage and gated_entry", async () => {
      const p = await plan();
      expect(p.addressOf("gated_entry")).toBe(p.addressOf("open_storage"));
   });

   it("resolves no instruction for gated_entry when the host keyed open_storage's by sourceID", async () => {
      const p = await plan();
      const index = indexInstructions(
         [instruct(p, "open_storage", "lake")],
         p.runPlan.tables,
      );
      const resolve = (name: string) =>
         resolveInstruction(
            index,
            p.runPlan.tables[p.idOf(name)].source,
            p.runPlan.connectionDigests,
            p.runPlan.tableFolds,
         );
      expect(resolve("open_storage")?.orchestrated).toBe(true);
      expect(resolve("gated_entry")).toBeUndefined();
   });

   it("builds the four instructed sources and neither builds nor judges gated_entry", async () => {
      const p = await plan();
      const { built, result } = await runHost(p, [
         instruct(p, "gated_mat"),
         instruct(p, "gated_persist"),
         instruct(p, "gated_storage", "lake"),
         instruct(p, "open_storage", "lake"),
      ]);
      expect(built.sort()).toEqual([
         "gated_mat",
         "gated_persist",
         "gated_storage",
         "open_storage",
      ]);
      expect(result.sourcesRefused).toBe(0);
      expect(result.failures).toEqual({});
      // gated_entry reads open_storage's table, through its own gate.
      expect(result.entries[p.addressOf("gated_entry")]).toMatchObject({
         sourceName: "open_storage",
         physicalTableName: "host_open_storage",
      });
   });

   it("records a refused instructed source against itself and builds the rest", async () => {
      const p = await plan();
      const { built, result } = await runHost(p, [
         instruct(p, "inherits"),
         instruct(p, "gated_storage", "lake"),
         instruct(p, "open_storage", "lake"),
      ]);
      expect(built.sort()).toEqual(["gated_storage", "open_storage"]);
      expect(result.sourcesRefused).toBe(1);
      const failure = result.failures[p.addressOf("inherits")];
      expect(failure).toBeDefined();
      expect(JSON.stringify(failure)).toContain("access_filter");
      // No table: only the refusal's error entry.
      expect(result.entries[p.addressOf("inherits")]?.error).toBeDefined();
      expect(
         result.entries[p.addressOf("inherits")]?.storageDestinationName,
      ).toBeUndefined();
   });

   it("admits gated_entry through its own gate when a host instructs its address without a sourceID", async () => {
      const p = await plan();
      const { built, result } = await runHost(p, [
         instruct(p, "gated_storage", "lake"),
         {
            sourceEntityId: p.addressOf("open_storage"),
            materializedTableId: "mt-open",
            physicalTableName: "host_open_storage",
            realization: "COPY",
            destination: "lake",
         },
      ]);
      // open_storage and gated_entry both resolve by address and both pass
      // the plan's storage gate (gated_entry declares its own filter, which
      // is applied at read); the table is written once.
      expect(built.filter((n) => n !== "gated_storage")).toHaveLength(1);
      expect(result.entries[p.addressOf("open_storage")]).toBeDefined();
      expect(result.failures).toEqual({});
      expect(result.sourcesRefused).toBe(0);
   });

   it("records a storage refusal against its own source when the instruction carries no sourceID", async () => {
      const p = await plan();
      const { built, result } = await runHost(p, [
         instruct(p, "open_storage", "lake"),
         {
            sourceEntityId: p.addressOf("via_join"),
            materializedTableId: "mt-via_join",
            physicalTableName: "host_via_join",
            realization: "COPY",
            destination: "lake",
         },
      ]);
      expect(built).toEqual(["open_storage"]);
      expect(result.sourcesRefused).toBe(1);
      expect(result.failures[p.addressOf("via_join")]).toMatchObject({
         sourceName: "via_join",
         refused: true,
         refusalReason: "access_filter",
      });
   });
});
