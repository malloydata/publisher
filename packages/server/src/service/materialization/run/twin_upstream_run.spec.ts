// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// P2-21: `col_a` (database) and `twin_a` (`storage="lake"`) compile to one
// build, so they once shared one address: the run's manifest kept whichever
// built last, and `col_b`, a database source over `col_a`, was handed the
// stored twin's entry (which a database build drops) and missed strictly.
// The storage twin now folds its destination into its address
// (foldDistinctTableNames), so one run builds both, and `col_b` builds over
// `col_a`'s database entry, whether `twin_a` builds or fails.

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import type {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
} from "../domain/types";
import { foldDistinctTableNames, planAddresses } from "../plan/addressing";
import { InstructedBuild } from "./instructed_build";
import { type BuildEnvironment } from "./build_upstreams";

const SHARED = "btwinshared0address";
const DOWNSTREAM = "btwindownstream0col";

let restore: (() => void) | undefined;
afterEach(() => {
   restore?.();
   restore = undefined;
});

function compiled() {
   const col_a = fakeSource({ name: "col_a", sourceEntityId: SHARED });
   const twin_a = fakeSource({
      name: "twin_a",
      sourceEntityId: SHARED,
      annotationFields: { storage: "lake" },
   });
   const col_b = fakeSource({ name: "col_b", sourceEntityId: DOWNSTREAM });
   const plan = compiledWith(
      { col_a, twin_a, col_b },
      [["col_a", "twin_a"], ["col_b"]],
      new Map([["duckdb", {} as unknown as MalloyConnection]]),
   );
   plan.builds[0].dependsOn = { col_b: ["col_a"], col_a: [], twin_a: [] };
   // As collectModelBuildPlan decides them, over the whole plan.
   const tableFolds = foldDistinctTableNames(plan, () => []);
   return {
      ...plan,
      tableFolds,
      addresses: planAddresses({ ...plan, tableFolds }),
   };
}

function selfInstructions(plan: ReturnType<typeof compiled>) {
   const { service } = createMocks();
   return (
      service.collaborators as unknown as {
         deriveSelfInstructions: (
            c: unknown,
            n: string[] | undefined,
            p: unknown,
         ) => {
            instructions: BuildInstruction[];
            refused: Record<string, RefusedSource>;
         };
      }
   ).deriveSelfInstructions(plan, undefined, {});
}

describe("P2-21: twins in two tiers and the database chain over one", () => {
   it("instructs each twin under its own address", () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      const { instructions, refused } = selfInstructions(compiled());
      expect(refused).toEqual({});
      const ids = instructions.map((i) => i.sourceEntityId);
      expect(new Set(ids).size).toBe(3);
      // The database twin and the downstream keep their plain addresses.
      expect(ids).toContain(SHARED);
      expect(ids).toContain(DOWNSTREAM);
   });

   for (const twinFails of [false, true]) {
      it(`builds col_b over col_a's database entry when twin_a ${twinFails ? "fails" : "builds"}`, async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
         const plan = compiled();
         const { instructions } = selfInstructions(plan);
         const seenByColB: Record<string, unknown>[] = [];
         const build = new InstructedBuild({
            buildOneSource: (async ({
               persistSource: source,
               instruction,
               builtEntries,
            }: {
               persistSource: { name: string };
               instruction: BuildInstruction;
               builtEntries: Record<string, ManifestEntry>;
            }): Promise<ManifestEntry> => {
               if (source.name === "twin_a" && twinFails) {
                  throw new Error("lake unreachable");
               }
               if (source.name === "col_b") {
                  seenByColB.push({ ...builtEntries });
               }
               return {
                  sourceEntityId: instruction.sourceEntityId,
                  sourceName: source.name,
                  physicalTableName: instruction.physicalTableName,
                  materializedTableId: instruction.materializedTableId,
                  connectionName: "duckdb",
                  realization: "COPY",
                  ...(source.name === "twin_a"
                     ? { storageDestinationName: "lake" }
                     : {}),
               } as ManifestEntry;
            }) as never,
            tableNotOwned: async () => undefined,
            reclaimStorageTablesFromFailedRun: async () => {},
         });
         const environment = {
            getApiConnection: () => {
               throw new Error("unused");
            },
            getEnvironmentName: () => "env",
            getEnvironmentPath: () => "/tmp/env",
         } as unknown as BuildEnvironment;
         const result = await build.run({
            compiled: plan,
            environment,
            instructions,
            seedEntries: {},
            signal: new AbortController().signal,
         });
         // col_a's database entry is its own; the twin never replaces it.
         expect(result.entries[SHARED]).toMatchObject({ sourceName: "col_a" });
         expect(result.entries[SHARED].storageDestinationName).toBeUndefined();
         expect(result.entries[DOWNSTREAM]).toMatchObject({
            sourceName: "col_b",
         });
         expect(result.failures[DOWNSTREAM]).toBeUndefined();
         expect(seenByColB).toHaveLength(1);
         expect(seenByColB[0][SHARED]).toMatchObject({ sourceName: "col_a" });
         const twinAddress = instructions.find(
            (i) =>
               i.sourceEntityId !== SHARED && i.sourceEntityId !== DOWNSTREAM,
         )!.sourceEntityId;
         if (twinFails) {
            // Its own failure, under its own address: col_a's entry stands.
            expect(result.failures[twinAddress]).toBeDefined();
            expect(result.entries[twinAddress]).toMatchObject({
               sourceName: "twin_a",
               error: "lake unreachable",
            });
         } else {
            expect(result.entries[twinAddress]).toMatchObject({
               sourceName: "twin_a",
               storageDestinationName: "lake",
            });
         }
      });
   }
});
