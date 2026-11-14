// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// ms2data/service#7078, storage tier: a base and its `extend` share one
// content address, and the eligibility gate decides per source. A storage
// binding is per source (the bind's eligibility is by name), so a refused
// extension must not fail the table its eligible base built: the address
// keeps its good entry and no failure, in either run order, and the refusal
// is reported per source.

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import { InstructedBuild } from "./instructed_build";
import type { BuildEnvironment } from "./materialization_run_support";

const ADDRESS = "bshared7078storage";

let restore: () => void;
beforeEach(() => {
   restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } }).restore;
});
afterEach(() => restore());

function harness(order: string[][]) {
   const connection = {} as unknown as MalloyConnection;
   const base = fakeSource({ name: "base", sourceEntityId: ADDRESS });
   const ext = fakeSource({
      name: "ext",
      sourceEntityId: ADDRESS,
      sourceDef: { blockNotes: ["#(authorize) true"] },
   });
   const compiled = compiledWith(
      { base, ext },
      order,
      new Map([["duckdb", connection]]),
   );
   const built: string[] = [];
   const buildOneSource = sinon.fake(
      async (
         source: { name: string },
         instruction: BuildInstruction,
      ): Promise<ManifestEntry> => {
         built.push(source.name);
         return {
            sourceEntityId: ADDRESS,
            sourceName: source.name,
            physicalTableName: instruction.physicalTableName,
            materializedTableId: instruction.materializedTableId,
            storageDestinationName: instruction.destination,
            connectionName: "duckdb",
            realization: "COPY",
         } as ManifestEntry;
      },
   );
   const build = new InstructedBuild({
      buildOneSource: buildOneSource as never,
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const environment = {
      getApiConnection: () => {
         throw new Error("unused");
      },
      getEnvironmentPath: () => "/tmp/env",
   } as unknown as BuildEnvironment;
   const run = () =>
      build.run(
         compiled,
         environment,
         [
            {
               sourceEntityId: ADDRESS,
               materializedTableId: "mt-shared",
               physicalTableName: "shared_v1",
               realization: "COPY",
               destination: "lake",
            },
         ],
         {},
         new AbortController().signal,
      );
   return { run, built };
}

describe("a refused sibling on a storage address (ms2data/service#7078)", () => {
   for (const [label, order] of [
      ["the eligible source comes first", [["base"], ["ext"]]],
      ["the refused source comes first", [["ext"], ["base"]]],
   ] as const) {
      it(`leaves the table its eligible sibling built standing when ${label}`, async () => {
         const { run, built } = harness(order.map((o) => [...o]));
         const result = await run();
         expect(built).toEqual(["base"]);
         expect(result.entries[ADDRESS]).toMatchObject({
            sourceName: "base",
            physicalTableName: "shared_v1",
         });
         expect(
            (result.entries[ADDRESS] as { error?: string }).error,
         ).toBeUndefined();
         expect(result.failures[ADDRESS]).toBeUndefined();
         expect(result.sourcesRefused).toBe(1);
         expect(result.refusalsInFailures).toBe(0);
         expect(
            Object.values(result.refusedBesideBuilt).map((r) => r.name),
         ).toEqual(["ext"]);
      });
   }
});
