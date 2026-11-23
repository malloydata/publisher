// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a `storage=` build writes, through the real build of each source: a
 * `#@ persist storage=` source stores every column it declares, the hidden
 * ones (`internal:`, `private:`) included, so its served copy can answer a
 * hidden measure from the table; a renamed column under the name its query
 * gives it; and not a column its own `extend` drops with `except:`. A
 * `#(materialize)` build source stores its hidden fields the same way.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { useMaterializationOn } from "../../../test_helpers/settings_env";
import { computeSourceEntityId } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import { InstructedBuild } from "./instructed_build";
import { type BuildEnvironment } from "./build_upstreams";
import { buildSource } from "./source_build";

const packages = tempBuildPlanPackages();
useMaterializationOn({ each: true });
afterEach(async () => {
   await packages.cleanup();
});

const SEED =
   "CREATE OR REPLACE TABLE customers_src AS SELECT * FROM (VALUES " +
   "(1, 'a', 'west', 'x'), (2, 'b', 'east', 'y'), (3, 'a', 'west', 'z')" +
   ") AS t(id, segment, home_region, status)";

const MODEL = `##! experimental { persistence access_modifiers }
source: customers is duckdb.table('customers_src')

#@ persist storage="lake"
source: stored_dim is customers -> {
  select: id, segment, home_region, status
} include { internal: home_region; private: status; public: * } extend {
  private measure: west_rows is count() { where: home_region = 'west' }
  measure: west_n is west_rows + 0
}

#@ persist storage="lake"
source: dropped is customers -> {
  select: id, segment, status
} extend {
  except: status
  measure: n is count()
}

#@ persist storage="lake"
source: rehidden is customers -> {
  select: id, segment, home_region
} extend {
  rename: region_raw is home_region
} include { internal: region_raw; public: * } extend {
  measure: west_n is count() { where: region_raw = 'west' }
}

#@ persist storage="lake"
source: renamed is customers -> {
  select: id, segment
} extend {
  rename: seg is segment
}

#(materialize) storage="lake"
source: materialized_dim is customers include {
  internal: home_region
  public: *
} extend {
  measure: west_n is count() { where: home_region = 'west' }
}
`;

async function scenario() {
   const pkg = await packages.create(MODEL);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   await duckdb.runSQL(SEED);
   const compiled = await compilePackageBuildPlan(pkg);
   const byName = (name: string) => {
      const hit = Object.entries(compiled.tables).find(
         ([, t]) => t.source.name === name,
      );
      if (!hit) throw new Error(`no planned source ${name}`);
      return { id: hit[0], source: hit[1].source };
   };
   const environment = {
      getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
      getStorageDestination: () => {
         throw new Error("no storage in this spec");
      },
      getEnvironmentName: () => "env",
      getEnvironmentPath: () => pkg.getPackagePath(),
   } as unknown as BuildEnvironment;
   // The storage tier, stood in for by the scenario's own database: the SQL
   // it is handed for the table is written there, so its columns can be read.
   const storage: Parameters<typeof buildSource>[1] = async (request) => {
      const table = request.instruction.physicalTableName;
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${table} AS ${request.storedBuildSQL}`,
      );
      return {
         sourceEntityId: request.contentSourceEntityId ?? "",
         sourceName: request.persistSource.name,
         physicalTableName: table,
         connectionName: "duckdb",
         storageDestinationName: "lake",
      } as ManifestEntry;
   };
   const build = new InstructedBuild({
      buildOneSource: (options) => buildSource(options, storage),
      tableNotOwned: async () => undefined,
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const addressOf = (name: string) => {
      const { id, source } = byName(name);
      return computeSourceEntityId(
         source,
         compiled.connectionDigests,
         compiled.tables[id]?.target,
      );
   };
   const instruction = (name: string, destination?: string): BuildInstruction =>
      ({
         sourceEntityId: addressOf(name),
         materializedTableId: `mt-${name}`,
         physicalTableName: `${name}_t`,
         realization: "COPY",
         ...(destination ? { destination } : {}),
      }) as BuildInstruction;
   const run = (instructions: BuildInstruction[]) =>
      build.run({
         compiled,
         environment,
         instructions,
         seedEntries: {},
         signal: new AbortController().signal,
         strict: false,
         priorRefusals: [],
         hostInstructed: false,
      });
   const columns = async (table: string) =>
      (
         (await duckdb.runSQL(`DESCRIBE ${table}`)).rows as {
            column_name: string;
         }[]
      ).map((r) => r.column_name);
   return { compiled, byName, addressOf, instruction, run, columns };
}

describe("a v1 storage= table stores its hidden columns", () => {
   it("stores every column the source declares, hidden ones included", async () => {
      const s = await scenario();
      const result = await s.run([s.instruction("stored_dim", "lake")]);
      expect(result.failures).toEqual({});
      expect(await s.columns("stored_dim_t")).toEqual([
         "id",
         "segment",
         "home_region",
         "status",
      ]);
   });

   it("leaves out a column the source's own extend drops with except:", async () => {
      const s = await scenario();
      const result = await s.run([s.instruction("dropped", "lake")]);
      expect(result.failures).toEqual({});
      expect(await s.columns("dropped_t")).toEqual(["id", "segment"]);
   });

   it("stores a renamed hidden column under the name its query gives it", async () => {
      const s = await scenario();
      const result = await s.run([s.instruction("rehidden", "lake")]);
      expect(result.failures).toEqual({});
      expect(await s.columns("rehidden_t")).toEqual([
         "id",
         "segment",
         "home_region",
      ]);
   });

   it("stores a renamed public column under the name its query gives it", async () => {
      const s = await scenario();
      const result = await s.run([s.instruction("renamed", "lake")]);
      expect(result.failures).toEqual({});
      expect(await s.columns("renamed_t")).toEqual(["id", "segment"]);
   });

   it("stores a #(materialize) build source's hidden field under its own name", async () => {
      const s = await scenario();
      const result = await s.run([s.instruction("materialized_dim", "lake")]);
      expect(result.failures).toEqual({});
      expect(await s.columns("materialized_dim_t")).toEqual([
         "id",
         "segment",
         "home_region",
         "status",
      ]);
   });
});
