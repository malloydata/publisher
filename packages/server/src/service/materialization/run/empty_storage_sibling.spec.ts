// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A `#@ persist` whose `storage=` names nothing is refused for itself
// (assertPlacementNamed), never through the colocated sibling rule: that rule
// keeps a GATED source's rows from being served past its gate through a table
// it shares, and a blank `storage=` is one source's own mistake, not a gate. A
// colocated sibling with the same SQL, so the same address, still builds, at
// auto-run and on a host's instruction, in either order.

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import type {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
} from "../domain/types";
import { InstructedBuild } from "./instructed_build";
import type { BuildEnvironment } from "./materialization_run_support";

const ADDRESS = "bshared0emptystorage";

function compiled(order: string[][]) {
   const a = fakeSource({
      name: "a",
      sourceEntityId: ADDRESS,
      annotationFields: { storage: "", name: "a_tbl" },
   });
   const b = fakeSource({
      name: "b",
      sourceEntityId: ADDRESS,
      annotationFields: { name: "b_tbl" },
   });
   return compiledWith(
      { a, b },
      order,
      new Map([["duckdb", {} as unknown as MalloyConnection]]),
   );
}

const ORDERS = [
   ["the refused source comes first", [["a"], ["b"]]],
   ["the sound source comes first", [["b"], ["a"]]],
] as const;

describe("an empty storage= beside a colocated sibling on one address", () => {
   for (const [label, order] of ORDERS) {
      it(`auto-run refuses that source alone and instructs its sibling when ${label}`, () => {
         const { service } = createMocks();
         const { instructions, refused } = (
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
         ).deriveSelfInstructions(
            compiled(order.map((o) => [...o])),
            undefined,
            {},
         );
         expect(Object.values(refused).map((r) => [r.name, r.reason])).toEqual([
            ["a", "empty_storage"],
         ]);
         // Each `name=` its own address (foldDistinctTableNames): b's table.
         expect(instructions.map((i) => i.physicalTableName)).toEqual([
            "b_tbl",
         ]);
         expect(instructions[0].destination).toBeUndefined();
      });

      it(`a host's colocated instruction builds the sibling, not the refused source, when ${label}`, async () => {
         const built: string[] = [];
         const buildOneSource = sinon.fake(
            async ({
               persistSource: source,
               instruction,
            }: {
               persistSource: { name: string };
               instruction: BuildInstruction;
            }): Promise<ManifestEntry> => {
               built.push(source.name);
               return {
                  sourceEntityId: ADDRESS,
                  sourceName: source.name,
                  physicalTableName: instruction.physicalTableName,
                  materializedTableId: instruction.materializedTableId,
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
         const result = await build.run({
            compiled: compiled(order.map((o) => [...o])),
            environment,
            instructions: [
               {
                  sourceEntityId: ADDRESS,
                  materializedTableId: "mt-shared",
                  physicalTableName: "shared_v1",
                  realization: "COPY",
               },
            ],
            seedEntries: {},
            signal: new AbortController().signal,
         });
         expect(built).toEqual(["b"]);
         expect(result.entries[ADDRESS]).toMatchObject({
            sourceName: "b",
            physicalTableName: "shared_v1",
         });
         expect(result.failures[ADDRESS]).toBeUndefined();
         expect(result.sourcesRefused).toBe(1);
      });
   }
});
