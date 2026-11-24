// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The facade's re-derivation when a string capability read lands
 * (`PackageMaterialization.replanOnCapabilityChange`): a plan that judged an
 * unbound `#(materialize)` upstream's compared strings by a capability
 * (`PackagePlan.stringCapabilityReads`) is marked stale and derived again
 * when a later read renders otherwise, so a refusal for a read not yet
 * landed never sticks; it is left alone when the read is unchanged, and the
 * plan derived again, which records what it read, is not derived again
 * until something moves (no loop). A connection edit's re-derivation runs
 * in the load worker, which reads nothing, so the plan it derives is checked
 * the same way on this thread. Driven with fake models, whose reads
 * record what a spec sets, and a fake package whose derivation is counted.
 */

import { describe, expect, it } from "bun:test";
import type { Model } from "../model";
import { stringCapabilityKey } from "./domain/compiled_build_plan";
import { capabilityFingerprint } from "./domain/string_reads";
import { PackageMaterialization } from "./package_materialization";
import {
   recordStringCapabilities,
   type StringCapabilities,
} from "./serve/source_capability";
import {
   makeBuildPlan,
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";

const SOURCE_ID = "m@file:///pkg/m.malloy";
const KEY = stringCapabilityKey(SOURCE_ID, "lake");

/** What one model's read records for its source `m`, by field. */
function capabilities(fields: Record<string, string>): StringCapabilities {
   return {
      connections: new Map(),
      sources: new Map([
         [
            "m",
            {
               connectionName: "pg",
               fields: new Map(
                  Object.entries(fields).map(([name, collation]) => [
                     name,
                     { collation: collation as never },
                  ]),
               ),
            },
         ],
      ]),
   };
}

/**
 * A facade over one model, whose capability reads record `state.fields`
 * (nothing while it is undefined), and whose plan judged `m` by `judged`.
 */
async function facade(judged: string) {
   const state = {
      fields: undefined as Record<string, string> | undefined,
      derived: 0,
   };
   const runtime = {};
   const model = {
      stringCapabilityRuntime: () => runtime,
      // What a publish of a derived plan hands each model; nothing here.
      persistedViewBuilds: () => [],
      setStringCapabilityReader: () => {},
      setScopeFactsRecorder: () => {},
      setServedCopyContext: () => {},
      prepareServedCopy: async () => {},
      prepareServedCopyOver: async () => {},
      refreshStringCapabilities: async () => {
         if (state.fields) {
            recordStringCapabilities(runtime, capabilities(state.fields));
         }
      },
   } as unknown as Model;
   const m = new PackageMaterialization({
      packageName: () => "pkg",
      versionId: () => undefined,
      metadata: () => ({}) as never,
      models: () => new Map([["m.malloy", model]]),
      modelPolicy: () => ({ setServingResolvers: () => {} }) as never,
      packagePath: () => "/pkg",
      malloyConfig: () => ({}) as never,
      connection: async () => {
         throw new Error("no connection");
      },
      // A derivation here plans nothing, and so judges nothing.
      buildPlanPackage: {
         getModelPaths: () => {
            state.derived++;
            return [];
         },
         getPackagePath: () => "/pkg",
         getMalloyConfig: () => ({}) as never,
         getMalloyConnection: async () => {
            throw new Error("no connection");
         },
         getMaterializationConfig: () => null,
         getPackageMetadata: () => ({}),
         getVersionId: () => undefined,
      },
      publishedSurface: () => undefined,
   });
   await m.applyLoadBuildPlan(
      okWorkerPlan(
         makePlanOutcome({
            plan: makeBuildPlan(),
            stringCapabilityReads: { [KEY]: judged },
         }),
      ),
   );
   return { m, state };
}

/** Let a re-derivation started by a read run to the end. */
async function settle(m: PackageMaterialization): Promise<void> {
   for (let i = 0; i < 50; i++) await Promise.resolve();
   await m.whenBuildPlanCurrent();
}

describe("re-deriving the plan when a string capability read lands", () => {
   it("derives it again once a read the plan judged as unread lands", async () => {
      const { m, state } = await facade(capabilityFingerprint("unread"));
      state.fields = { cat: "deterministic", cc: "binary" };
      await m.recordStringCapabilitiesAtLoad();
      await settle(m);
      expect(state.derived).toBe(1);
      expect(m.isBuildPlanStale()).toBe(false);
   });

   it("leaves it when the read renders what the plan judged by", async () => {
      const fields = { cat: "deterministic", cc: "binary" };
      const { m, state } = await facade(
         capabilityFingerprint({
            fields: new Map(Object.entries(fields)) as never,
         }),
      );
      state.fields = fields;
      await m.recordStringCapabilitiesAtLoad();
      await settle(m);
      expect(state.derived).toBe(0);
   });

   it("leaves it while the read still records nothing", async () => {
      const { m, state } = await facade(capabilityFingerprint("unread"));
      await m.recordStringCapabilitiesAtLoad();
      await settle(m);
      expect(state.derived).toBe(0);
   });

   it("checks a plan a connection edit re-derived in the worker against the reads on this thread", async () => {
      const fields = { cat: "deterministic", cc: "binary" };
      const read = capabilityFingerprint({
         fields: new Map(Object.entries(fields)) as never,
      });
      const { m, state } = await facade(read);
      state.fields = fields;
      await m.recordStringCapabilitiesAtLoad();
      await settle(m);
      expect(state.derived).toBe(0);
      // The worker reads no capability, so it judges the upstream unread.
      m.markBuildPlanStale(["pg"]);
      // As this thread's derivation here, it plans nothing.
      const empty = makeBuildPlan({ graphs: [], sources: {} });
      const worker = async () =>
         okWorkerPlan(
            makePlanOutcome({
               plan: empty,
               stringCapabilityReads: {
                  [KEY]: capabilityFingerprint("unread"),
               },
            }),
         );
      expect(await m.refreshBuildPlan(worker)).toBe(true);
      await settle(m);
      // Derived again on this thread, from the read it has.
      expect(state.derived).toBe(1);
      expect(m.isBuildPlanStale()).toBe(false);
      // A worker plan that judged by the same read is left alone.
      m.markBuildPlanStale(["pg"]);
      expect(
         await m.refreshBuildPlan(async () =>
            okWorkerPlan(
               makePlanOutcome({
                  plan: empty,
                  stringCapabilityReads: { [KEY]: read },
               }),
            ),
         ),
      ).toBe(true);
      await settle(m);
      expect(state.derived).toBe(1);
   });

   it("derives it again when the warehouse moved a collation, then never again for the same read", async () => {
      const { m, state } = await facade(
         capabilityFingerprint({
            fields: new Map([["cat", "binary"]]) as never,
         }),
      );
      state.fields = { cat: "deterministic" };
      await m.recordStringCapabilitiesAtLoad();
      await settle(m);
      expect(state.derived).toBe(1);
      // The plan derived again records what it judged by (here nothing), so
      // later reads, the same or another, do not derive it again.
      for (let i = 0; i < 3; i++) {
         await m.recordStringCapabilitiesAtLoad();
         await settle(m);
      }
      state.fields = { cat: "binary" };
      await m.recordStringCapabilitiesAtLoad();
      await settle(m);
      expect(state.derived).toBe(1);
   });
});
