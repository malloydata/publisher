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
import type {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
   SourceFailure,
} from "../domain/types";
import { failedRunManifestOf } from "./build_failure_class";
import { logger } from "../../../logger";
import { InstructedBuild } from "./instructed_build";
import { RunTelemetry, withRunTelemetry } from "./run_telemetry";
import { type BuildEnvironment } from "./build_upstreams";

const ADDRESS = "bshared7078storage";

let restore: () => void;
beforeEach(() => {
   restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } }).restore;
});
afterEach(() => restore());

function harness(
   order: string[][],
   opts: {
      /** The base is gated too, so both sources are refused. */
      baseGated?: boolean;
      /** The base's build fails. */
      baseFails?: boolean;
      refusedAtPlan?: (i: BuildInstruction) => RefusedSource | undefined;
      instructions?: BuildInstruction[];
   } = {},
) {
   const connection = {} as unknown as MalloyConnection;
   const base = fakeSource({
      name: "base",
      sourceEntityId: ADDRESS,
      ...(opts.baseGated
         ? { sourceDef: { blockNotes: ["#(authorize) false"] } }
         : {}),
   });
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
      async ({
         persistSource: source,
         instruction,
      }: {
         persistSource: { name: string };
         instruction: BuildInstruction;
      }): Promise<ManifestEntry> => {
         built.push(source.name);
         if (opts.baseFails && source.name === "base") {
            throw new Error("the warehouse went away");
         }
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
   const run = () =>
      build.run({
         compiled,
         environment,
         instructions: opts.instructions ?? [
            {
               sourceEntityId: ADDRESS,
               materializedTableId: "mt-shared",
               physicalTableName: "shared_v1",
               realization: "COPY",
               destination: "lake",
            },
         ],
         seedEntries: {},
         signal: new AbortController().signal,
         ...(opts.refusedAtPlan ? { refusedAtPlan: opts.refusedAtPlan } : {}),
      });
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
            Object.values(result.refusedSources).map((r) => r.name),
         ).toEqual(["ext"]);
      });
   }
});

/** The run's outcome, whether it returned or threw a run that built nothing. */
async function outcome(run: () => Promise<unknown>): Promise<{
   failures: Record<string, SourceFailure>;
   refusedSources: Record<string, RefusedSource>;
}> {
   try {
      const result = (await run()) as {
         failures: Record<string, SourceFailure>;
         refusedSources: Record<string, RefusedSource>;
      };
      return result;
   } catch (err) {
      const failed = failedRunManifestOf(err);
      if (!failed) throw err;
      return {
         failures: failed.failures,
         refusedSources: failed.refusedSources ?? {},
      };
   }
}

describe("every refused source on a shared address is reported with its own reason", () => {
   for (const [label, order] of [
      ["the base first", [["base"], ["ext"]]],
      ["the extension first", [["ext"], ["base"]]],
   ] as const) {
      it(`reports both of two refusals on an address nothing built, ${label}`, async () => {
         const { run, built } = harness(
            order.map((o) => [...o]),
            { baseGated: true },
         );
         const { failures, refusedSources } = await outcome(run);
         expect(built).toEqual([]);
         // One is the address's failure, in sourceID order; the other is
         // reported per source, never dropped.
         expect(failures[ADDRESS]).toMatchObject({
            sourceName: "base",
            refused: true,
         });
         expect(Object.values(refusedSources).map((r) => r.name)).toEqual([
            "ext",
         ]);
         expect(failures[ADDRESS].reason).toContain("'base'");
         expect(Object.values(refusedSources)[0].message).toContain("'ext'");
      });

      it(`keeps the base's build failure and reports the extension's gate refusal beside it, ${label}`, async () => {
         const { run, built } = harness(
            order.map((o) => [...o]),
            { baseFails: true },
         );
         const { failures, refusedSources } = await outcome(run);
         expect(built).toEqual(["base"]);
         expect(failures[ADDRESS].reason).toContain("the warehouse went away");
         expect(failures[ADDRESS].refused).toBeUndefined();
         expect(Object.values(refusedSources).map((r) => r.name)).toEqual([
            "ext",
         ]);
      });
   }

   it("reports an extension's plan refusal beside its base's build failure, never folded into it", async () => {
      const extension: RefusedSource = {
         sourceID: "ext_planned@file:///pkg/m.malloy",
         name: "ext_planned",
         tier: "storage",
         reason: "upstream_table_unusable",
         message: "it reads a table no stored table can stand in for",
      };
      const shared = {
         sourceEntityId: ADDRESS,
         materializedTableId: "mt-shared",
         physicalTableName: "shared_v1",
         realization: "COPY",
         destination: "lake",
      } as const;
      const { run, built } = harness([["base"]], {
         baseFails: true,
         instructions: [
            { ...shared, sourceID: "base" },
            { ...shared, sourceID: extension.sourceID },
         ],
         refusedAtPlan: (i) =>
            i.sourceID === extension.sourceID ? extension : undefined,
      });
      const { failures, refusedSources } = await outcome(run);
      expect(built).toEqual(["base"]);
      expect(failures[ADDRESS].reason).toContain("the warehouse went away");
      expect(refusedSources).toEqual({ [extension.sourceID]: extension });
   });
});

describe("materialization.build.refused", () => {
   it("logs the refusal with its run, source and reason code, and no prose", async () => {
      const { run } = harness([["base"], ["ext"]]);
      const warn = sinon.stub(logger, "warn");
      try {
         await withRunTelemetry(
            new RunTelemetry("run-7", "orchestrated", "pkg"),
            run,
         );
         const refused = warn
            .getCalls()
            .map((c) => c.args as unknown[])
            .filter((args) => args[0] === "materialization.build.refused");
         expect(refused).toHaveLength(1);
         expect(refused[0][1]).toEqual({
            runId: "run-7",
            packageName: "pkg",
            sourceEntityId: ADDRESS,
            sourceName: "ext",
            kind: "persist",
            tier: "storage",
            reason: "authorize",
         });
      } finally {
         warn.restore();
      }
   });
});
