// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import { makeInstruction } from "../../../test_helpers/materialization_test_fixtures";
import {
   createMocks,
   serviceWith,
} from "../../../test_helpers/materialization_service_harness";
import {
   type MetricsHarness,
   startMetricsHarness,
} from "../../../test_helpers/metrics_harness";
import type {
   RunBuildOptions,
   RunCollaborators,
} from "./materialization_service";

/**
 * How a run ends, on every terminal path: exactly one run metric
 * (`publisher_materialization_runs_total` and the duration, by mode and
 * outcome) and exactly one `materialization.run.finished` event, whatever
 * the path. Driven through the service's run with its build steps stubbed.
 */

const ENTRY = {
   "build-orders": {
      sourceEntityId: "build-orders",
      physicalTableName: '"orders_v1"',
      connectionName: "duckdb",
   },
};

describe("a run's outcome is recorded once, on every terminal path", () => {
   let ctx: ReturnType<typeof createMocks>;
   let harness: MetricsHarness;
   let info: sinon.SinonStub;
   let warn: sinon.SinonStub;
   let error: sinon.SinonStub;

   beforeEach(async () => {
      ctx = createMocks();
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      info = sinon.stub(logger, "info");
      warn = sinon.stub(logger, "warn");
      error = sinon.stub(logger, "error");
   });

   afterEach(async () => {
      info.restore();
      warn.restore();
      error.restore();
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   function run(
      collaborators: Partial<RunCollaborators>,
      signal: AbortSignal = new AbortController().signal,
      opts: Partial<RunBuildOptions> = {},
   ): Promise<void> {
      const service = serviceWith(ctx, {
         commitManifest: sinon.stub().resolves(),
         autoLoadManifest: sinon.stub().resolves(),
         ...collaborators,
      });
      return service.collaborators.runBuild(
         "mat-1",
         "my-env",
         "pkg",
         {
            sourceNames: undefined,
            forceRefresh: false,
            buildInstructions: [makeInstruction()],
            ...opts,
         } as RunBuildOptions,
         signal,
      );
   }

   async function outcomes(): Promise<Record<string, unknown>[]> {
      return harness.collectAttributes("publisher_materialization_runs_total");
   }

   function finishedEvents(): unknown[][] {
      return [...info.getCalls(), ...warn.getCalls(), ...error.getCalls()]
         .map((c) => c.args as unknown[])
         .filter((a) => a[0] === "materialization.run.finished");
   }

   it("success", async () => {
      await run({
         executeInstructedBuild: sinon
            .stub()
            .resolves({ entries: ENTRY, failures: {}, sourcesRefused: 0 }),
      });
      expect(await outcomes()).toEqual([
         { mode: "orchestrated", outcome: "success" },
      ]);
      expect(
         await harness.collectCounter("publisher_materialization_runs_total"),
      ).toBe(1);
      expect(finishedEvents()).toHaveLength(1);
      expect(finishedEvents()[0][1]).toMatchObject({
         runId: "mat-1",
         outcome: "success",
      });
   });

   it("partial: a source failed beside one that built", async () => {
      await run({
         executeInstructedBuild: sinon.stub().resolves({
            entries: ENTRY,
            failures: {
               other: {
                  sourceEntityId: "other",
                  sourceName: "other",
                  reason: "boom",
               },
            },
            sourcesRefused: 0,
         }),
      });
      expect(await outcomes()).toEqual([
         { mode: "orchestrated", outcome: "partial" },
      ]);
      expect(finishedEvents()).toHaveLength(1);
      expect(finishedEvents()[0][1]).toMatchObject({ outcome: "partial" });
   });

   it("failed: the build throws", async () => {
      await expect(
         run({
            executeInstructedBuild: sinon.stub().rejects(new Error("boom")),
         }),
      ).rejects.toThrow("boom");
      expect(await outcomes()).toEqual([
         { mode: "orchestrated", outcome: "failed" },
      ]);
      expect(
         await harness.collectCounter("publisher_materialization_runs_total"),
      ).toBe(1);
      expect(finishedEvents()).toHaveLength(1);
      expect(finishedEvents()[0][1]).toMatchObject({ outcome: "failed" });
   });

   it("cancelled: the build throws once the run is aborted", async () => {
      const abort = new AbortController();
      await expect(
         run(
            {
               executeInstructedBuild: sinon.stub().callsFake(async () => {
                  abort.abort();
                  throw new Error("stopped");
               }),
            },
            abort.signal,
         ),
      ).rejects.toThrow("stopped");
      expect(await outcomes()).toEqual([
         { mode: "orchestrated", outcome: "cancelled" },
      ]);
      expect(finishedEvents()).toHaveLength(1);
      expect(finishedEvents()[0][1]).toMatchObject({ outcome: "cancelled" });
   });

   it("failed before the build: the commit throws, auto mode", async () => {
      await expect(
         run(
            {
               deriveSelfInstructions: sinon.stub().returns({
                  instructions: [makeInstruction()],
                  carried: {},
                  refused: {},
               }),
               executeInstructedBuild: sinon.stub().resolves({
                  entries: ENTRY,
                  failures: {},
                  sourcesRefused: 0,
               }),
               commitManifest: sinon.stub().rejects(new Error("store down")),
            },
            undefined,
            { buildInstructions: undefined },
         ),
      ).rejects.toThrow("store down");
      expect(await outcomes()).toEqual([{ mode: "auto", outcome: "failed" }]);
      expect(finishedEvents()).toHaveLength(1);
   });
});
