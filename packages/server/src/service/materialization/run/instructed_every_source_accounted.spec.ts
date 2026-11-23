// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Every instructed source ends in exactly one of `entries` or `failures`. An
// instruction the run's own plan holds no source for (the plan refused it
// since the host read the served plan, judged against tables the package had
// bound, or it names nothing) is a failure with a reason, never dropped: a
// run that reported success for a source it neither built nor failed told
// the host its table exists.

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import type {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
} from "../domain/types";
import {
   InstructedBuild,
   INSTRUCTION_UNRESOLVED_CODE,
} from "./instructed_build";
import { type BuildEnvironment } from "./build_upstreams";

let restore: () => void;
beforeEach(() => {
   restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } }).restore;
});
afterEach(() => restore());

const instruction = (
   sourceEntityId: string,
   physicalTableName: string,
): BuildInstruction => ({
   sourceEntityId,
   materializedTableId: `mt-${physicalTableName}`,
   physicalTableName,
   realization: "COPY",
   destination: "lake",
});

/** A run over a plan holding only `daily`; `rollup` is not in it. */
function harness() {
   const connection = {} as unknown as MalloyConnection;
   const daily = fakeSource({ name: "daily", sourceEntityId: "addr-daily" });
   const compiled = compiledWith(
      { daily },
      [["daily"]],
      new Map([["duckdb", connection]]),
   );
   const built: string[] = [];
   const buildOneSource = sinon.fake(
      async ({
         persistSource: source,
         instruction: i,
      }: {
         persistSource: { name: string };
         instruction: BuildInstruction;
      }): Promise<ManifestEntry> => {
         built.push(source.name);
         return {
            sourceEntityId: i.sourceEntityId,
            sourceName: source.name,
            physicalTableName: i.physicalTableName,
            materializedTableId: i.materializedTableId,
            storageDestinationName: i.destination,
            connectionName: "duckdb",
            realization: "COPY",
         } as ManifestEntry;
      },
   );
   const build = new InstructedBuild({
      buildOneSource: buildOneSource as never,
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
   const run = (
      instructions: BuildInstruction[],
      refusedAtPlan?: (i: BuildInstruction) => RefusedSource | undefined,
   ) =>
      build.run({
         compiled,
         environment,
         instructions,
         seedEntries: {},
         signal: new AbortController().signal,
         ...(refusedAtPlan ? { refusedAtPlan } : {}),
      });
   return { run, built };
}

const rollupRefusal: RefusedSource = {
   sourceID: "rollup@file:///pkg/m.malloy",
   name: "rollup",
   tier: "storage",
   reason: "upstream_table_unusable",
   message:
      "it reads the source warehouse (regions) through 'daily_regional', " +
      "which no stored table can stand in for",
};

/** Each instruction's outcome: exactly one of built or failed. */
function expectEveryInstructionAccounted(
   instructions: BuildInstruction[],
   result: {
      entries: Record<string, ManifestEntry>;
      failures: Record<string, unknown>;
   },
): void {
   for (const i of instructions) {
      const failed = result.failures[i.sourceEntityId] !== undefined;
      const entry = result.entries[i.sourceEntityId] as
         | (ManifestEntry & { error?: string })
         | undefined;
      const built = entry !== undefined && entry.error === undefined;
      expect({
         id: i.sourceEntityId,
         outcome: Number(failed) + Number(built),
      }).toEqual({ id: i.sourceEntityId, outcome: 1 });
   }
}

describe("every instructed source ends in entries or failures", () => {
   it("fails an instruction the plan refused, with the plan's reason, beside a source that builds", async () => {
      const { run, built } = harness();
      const instructions = [
         instruction("addr-daily", "daily_g2"),
         instruction("addr-rollup", "rollup_g2"),
      ];
      const result = await run(instructions, (i) =>
         i.sourceEntityId === "addr-rollup" ? rollupRefusal : undefined,
      );
      expect(built).toEqual(["daily"]);
      expectEveryInstructionAccounted(instructions, result);
      expect(result.failures["addr-rollup"]).toMatchObject({
         sourceName: "rollup",
         physicalTableName: "rollup_g2",
         storageDestinationName: "lake",
         refused: true,
         refusalReason: "upstream_table_unusable",
         retryable: false,
      });
      expect(result.failures["addr-rollup"].reason).toContain("daily_regional");
      expect(result.sourcesRefused).toBe(1);
   });

   it("fails a run whose only instruction the plan refused, rather than reporting success", async () => {
      const { run, built } = harness();
      const instructions = [instruction("addr-rollup", "rollup_g2")];
      let thrown: unknown;
      try {
         await run(instructions, () => rollupRefusal);
      } catch (err) {
         thrown = err;
      }
      expect(built).toEqual([]);
      expect(thrown).toBeDefined();
      const failures = (thrown as { runFailures?: Record<string, unknown> })
         .runFailures;
      expect(failures?.["addr-rollup"]).toMatchObject({
         refused: true,
         refusalReason: "upstream_table_unusable",
      });
   });

   it("fails an instruction no planned source resolves to, coded instruction_unresolved", async () => {
      const { run, built } = harness();
      const instructions = [
         instruction("addr-daily", "daily_g2"),
         instruction("addr-gone", "gone_g2"),
      ];
      const result = await run(instructions);
      expect(built).toEqual(["daily"]);
      expectEveryInstructionAccounted(instructions, result);
      expect(result.failures["addr-gone"]).toMatchObject({
         physicalTableName: "gone_g2",
         code: INSTRUCTION_UNRESOLVED_CODE,
         retryable: false,
      });
      expect(result.failures["addr-gone"].refused).toBeUndefined();
   });
});
